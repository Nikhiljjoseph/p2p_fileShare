import {signalingService} from './signaling.service';

const RTC_CONFIG: RTCConfiguration = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
    ],
};

const CHUNK_SIZE = 16 * 1024; // 16KB chunk size
const BUFFER_THRESHOLD = 64 * 1024; // 64KB untill the buffer is full
const CHAT_CHANNEL_LABEL = 'chat-channel'; // must match on both ends of the router
const FILE_CHANNEL_PREFIX = 'file-';
const CHANNEL_OPEN_TIMEOUT = 15000; // give a fresh file channel this long to open

export interface FileMetadata {
    type: 'file-meta';
    fileId: string;
    fileName: string;
    fileSize: number;
    totalChunks: number;
}

export interface FileCancelMessage {
    type: 'file-cancel';
    fileId: string;
}

interface IncomingTransfer {
    meta: FileMetadata;
    recievedChunks: ArrayBuffer[];
    receivedBytes: number;
}

interface PeerSession{
    peerConnection: RTCPeerConnection;
    dataChannel: RTCDataChannel|null;
    // ICE can arrive before the offer/answer is applied; hold them until then
    pendingCandidates: RTCIceCandidateInit[];
    hasRemoteDescription: boolean;
}

export class WebRTCService {
    private peers = new Map<string, PeerSession>();
    private activeIncoming = new Map<string, IncomingTransfer>();
    private activeUploads = new Map<string, AbortController>();
    public onPeersChange?: (connectedPeerIds: string[]) => void;
    public onMessageReceived?: (senderId:string, message:string) => void;
    public onFileProgress?: (senderId:string, fileId:string, fileName:string, progressPercent:number) => void;
    public onFileReceived?: (senderId:string, fileName:string, fileBlob:Blob) => void;
    public onFileCancelled?: (senderId:string, fileId:string, fileName:string) => void;

    constructor() {
        this.setupSignalListeners();
    }

    // Incoming transfers are keyed per sender so two peers can push concurrently
    private transferKey(peerId:string, fileId:string): string {
        return `${peerId}::${fileId}`;
    }

    // Set up listeners for signaling events from the signaling service
    private setupSignalListeners():void {
        // When a new peer joins, we create an offer to initiate the WebRTC connection
        signalingService.onPeerJoined = async (newcomerId:string) => {
            await this.createOffer(newcomerId);
        }

        //if we receive an SDP offer or answer, we handle it accordingly
        signalingService.onSdpReceived = async (senderId, sdp, type) => {
            if(type === 'offer') {
                await this.handleOffer(senderId, sdp);
            } else if(type === 'answer') {
                await this.handleAnswer(senderId, sdp);
            }
        }

        // When we receive an ICE candidate from a peer, we add it to our peer connection
        signalingService.onIceCandidateReceived = async (senderId, candidate, sdpMid, sdpMLineIndex) => {
            const session = this.peers.get(senderId);
            if(!session) {return;}
            const init: RTCIceCandidateInit = { candidate, sdpMid, sdpMLineIndex };
            // addIceCandidate throws if the remote description isn't applied yet
            if(!session.hasRemoteDescription){
                session.pendingCandidates.push(init);
                return;
            }
            try {
                await session.peerConnection.addIceCandidate(new RTCIceCandidate(init));
            } catch (error) {
                console.error("Error adding received ICE candidate", error);
            }
        }

        //cleanup if a peer left the group
        signalingService.onPeerLeft = (peerId: string) => {
            this.removePeer(peerId);
        }
    }

    // Drain candidates that arrived before setRemoteDescription
    private async flushPendingCandidates(session: PeerSession): Promise<void> {
        const queued = session.pendingCandidates.splice(0, session.pendingCandidates.length);
        for(const init of queued){
            try {
                await session.peerConnection.addIceCandidate(new RTCIceCandidate(init));
            } catch (error) {
                console.error("Error adding queued ICE candidate", error);
            }
        }
    }

    // Get or create a peer connection for a specific target peer
    private getOrCreatePeer(peerId:string): PeerSession {
        const existing = this.peers.get(peerId);
        if(existing) return existing;

        const pc = new RTCPeerConnection(RTC_CONFIG);
        const session: PeerSession = {
            peerConnection: pc,
            dataChannel: null,
            pendingCandidates: [],
            hasRemoteDescription: false,
        };
        // Whenever the browser discovers a public IP candidate from STUN, send it to the other peer
        pc.onicecandidate = (event) => {
            if(event.candidate) {
                signalingService.sendIceCandidate(
                    peerId,
                    event.candidate.candidate,
                    event.candidate.sdpMid,
                    event.candidate.sdpMLineIndex
                );
            }
        };

        //habdle peer disconnection
        pc.onconnectionstatechange = () => {
            if(pc.connectionState === 'disconnected' || pc.connectionState === 'failed' || pc.connectionState === 'closed')
                this.removePeer(peerId);
        }

        // Single source of truth for channel routing. Never reassign this handler
        // elsewhere -- doing so makes file channels get bound as the chat channel.
        pc.ondatachannel = (event) => {
            const channel = event.channel;
            if(channel.label.startsWith(FILE_CHANNEL_PREFIX)){
                const fileId = channel.label.slice(FILE_CHANNEL_PREFIX.length);
                this.bindFileRecieverChannel(peerId, fileId, channel);
                return;
            }
            if(channel.label === CHAT_CHANNEL_LABEL){
                this.bindChatDataChannel(peerId, session, channel);
                return;
            }
            console.warn(`[WebRTC] Ignoring data channel with unknown label "${channel.label}"`);
        }
        this.peers.set(peerId, session);
        return session;
    }

    // Create an SDP offer for a specific target peer and send it via the signaling service
    public async createOffer(targetPeerId:string): Promise<void> {
        const session = this.getOrCreatePeer(targetPeerId);
        // Guard against a duplicate PeerJoined re-offering over a live connection
        if(session.dataChannel || session.peerConnection.signalingState !== 'stable'){
            return;
        }
        const dc = session.peerConnection.createDataChannel(CHAT_CHANNEL_LABEL);
        this.bindChatDataChannel(targetPeerId, session, dc);
        const offer = await session.peerConnection.createOffer();
        await session.peerConnection.setLocalDescription(offer);
        await signalingService.sendSdp(targetPeerId, offer.sdp ?? '', 'offer');
    }

    // Handle an incoming SDP offer from a peer, create an answer, and send it back
    private async handleOffer(senderId:string, sdpOffer:string): Promise<void> {
        const session = this.getOrCreatePeer(senderId);
        await session.peerConnection.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: sdpOffer }));
        session.hasRemoteDescription = true;
        await this.flushPendingCandidates(session);
        const answer = await session.peerConnection.createAnswer();
        await session.peerConnection.setLocalDescription(answer);
        await signalingService.sendSdp(senderId, answer.sdp ?? '', 'answer');
    }

    // Handle an incoming SDP answer from a peer and set it as the remote description
    private async handleAnswer(senderId:string, sdpAnswer:string): Promise<void> {
        const session = this.peers.get(senderId);
        if(!session) return;
        await session.peerConnection.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: sdpAnswer }));
        session.hasRemoteDescription = true;
        await this.flushPendingCandidates(session);
    }

    // channel for text messages
    private bindChatDataChannel(peerId:string, session: PeerSession, channel: RTCDataChannel): void {
        session.dataChannel = channel;
        channel.binaryType = 'arraybuffer'; // failsafe
        channel.onopen = () => {
            this.notifyPeersChange();
        }
        channel.onclose = () => {
            // Only the live chat channel tearing down means we lost the peer
            if(session.dataChannel === channel) this.removePeer(peerId);
        }
        channel.onmessage = (event) => {
            if(typeof event.data === 'string') {
                this.onMessageReceived?.(peerId, event.data);
            }
        }
    }

    //channel for concurrent file transfers
    private bindFileRecieverChannel(peerId:string, fileId: string, channel: RTCDataChannel): void {
        channel.binaryType = 'arraybuffer';
        const key = this.transferKey(peerId, fileId);

        // A file channel closing is normal end-of-transfer, never a peer teardown
        channel.onclose = () => {
            this.activeIncoming.delete(key);
        }

        channel.onmessage = (event) => {
            if(typeof event.data === 'string') {
                try {
                    const parsed = JSON.parse(event.data);
                    if(parsed.type === 'file-meta') {
                        this.activeIncoming.set(key, {
                        meta: parsed,
                        recievedChunks: [],
                        receivedBytes: 0
                    });
                    this.onFileProgress?.(peerId, fileId, parsed.fileName, 0);
                    return;
                    }
                    if(parsed.type === 'file-cancel') {
                        const transfer = this.activeIncoming.get(key);
                        const fileName = transfer?.meta.fileName ?? 'Transfer';
                        this.activeIncoming.delete(key);
                        this.onFileCancelled?.(peerId, fileId, fileName);
                        return;
                    }
                } catch (error) {
                console.error('Error parsing file message:', error);
                }
                return;
            }
            if(event.data instanceof ArrayBuffer) {
                const transfer = this.activeIncoming.get(key);
                if(!transfer) return;

                transfer.recievedChunks.push(event.data);
                transfer.receivedBytes += event.data.byteLength;

                const percent = Math.min(Math.round((transfer.receivedBytes / transfer.meta.fileSize) * 100), 100);
                this.onFileProgress?.(peerId, fileId, transfer.meta.fileName, percent);

                if(transfer.receivedBytes >= transfer.meta.fileSize) {
                    const finalBlob = new Blob(transfer.recievedChunks);
                    this.onFileReceived?.(peerId, transfer.meta.fileName, finalBlob);
                    this.activeIncoming.delete(key);
                    channel.close();
                }
            }
        }
    }

    private notifyPeersChange(): void {
        const openPeers = Array.from(this.peers.entries())
            .filter(([_, s]) => s.dataChannel?.readyState === 'open')
            .map(([id, _]) => id);

        this.onPeersChange?.(openPeers);
    }

    private removePeer(peerId:string): void {
        const session = this.peers.get(peerId);
        if(session){
            session.dataChannel?.close();
            session.peerConnection.close();
            this.peers.delete(peerId);
            // drop any half-received transfers from this peer
            for(const key of Array.from(this.activeIncoming.keys())){
                if(key.startsWith(`${peerId}::`)) this.activeIncoming.delete(key);
            }
            this.notifyPeersChange();
        }
    }

    public closeAllConnections(): void {
        for(const [_peerId,session] of this.peers){
            session.dataChannel?.close();
            session.peerConnection.close();
        }
        this.peers.clear();
        this.activeIncoming.clear();
        this.activeUploads.forEach((controller)=>controller.abort());
        this.activeUploads.clear();
        this.notifyPeersChange();
    }

    public broadcastMessage(msg:string): void {
        console.log(`[WebRTC] Attempting to broadcast to ${this.peers.size} peer(s): "${msg}"`);
        for(const [_, session] of this.peers.entries()){
            if(session.dataChannel?.readyState === 'open')
                session.dataChannel?.send(msg);
        }
    }

    public cancelTransfer(fileId: string): void {
        const controller = this.activeUploads.get(fileId);
        if(controller){
            controller.abort();
            this.activeUploads.delete(fileId);
        }
        // notification happens in sendFile method
    }

    // Resolve true once the channel is usable, false if it never gets there.
    private waitForChannelOpen(dc: RTCDataChannel, signal: AbortSignal): Promise<boolean> {
        if(dc.readyState === 'open') return Promise.resolve(true);
        if(dc.readyState === 'closing' || dc.readyState === 'closed') return Promise.resolve(false);

        return new Promise<boolean>((resolve) => {
            const settle = (ok: boolean) => {
                clearTimeout(timer);
                dc.removeEventListener('open', onOpen);
                dc.removeEventListener('error', onFail);
                dc.removeEventListener('close', onFail);
                signal.removeEventListener('abort', onFail);
                resolve(ok);
            };
            const onOpen = () => settle(true);
            const onFail = () => settle(false);
            const timer = setTimeout(onFail, CHANNEL_OPEN_TIMEOUT);

            dc.addEventListener('open', onOpen, { once: true });
            dc.addEventListener('error', onFail, { once: true });
            dc.addEventListener('close', onFail, { once: true });
            signal.addEventListener('abort', onFail, { once: true });
        });
    }

    // Wait for a congested channel to drain, bailing out on abort or close.
    private waitForDrain(dc: RTCDataChannel, signal: AbortSignal): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            const cleanup = () => {
                dc.removeEventListener('bufferedamountlow', onDrain);
                dc.removeEventListener('close', onClose);
                signal.removeEventListener('abort', onAbort);
            };
            const onDrain = () => { cleanup(); resolve(); };
            const onClose = () => { cleanup(); resolve(); }; // dropped peer, skipped by readyState check
            const onAbort = () => { cleanup(); reject(new DOMException('File transfer aborted', 'AbortError')); };

            dc.addEventListener('bufferedamountlow', onDrain, { once: true });
            dc.addEventListener('close', onClose, { once: true });
            signal.addEventListener('abort', onAbort, { once: true });
        });
    }

    public async sendFile(file: File, onFileReady?:(fileId: string)=> void, onProgress?: (percent: number)=> void): Promise<string> {
        const fileId = crypto.randomUUID();
        const abortController = new AbortController();
        const signal = abortController.signal;
        this.activeUploads.set(fileId, abortController);
        onFileReady?. (fileId);

        // Open one dedicated channel per peer that has a live chat channel --
        // an open chat channel proves the SCTP association is up, which is all
        // a new data channel needs (no renegotiation required).
        const openedChannels:RTCDataChannel[] = [];
        for(const[_, session] of this.peers){
            if(session.dataChannel?.readyState === 'open'){
                const dc = session.peerConnection.createDataChannel(`${FILE_CHANNEL_PREFIX}${fileId}`);
                dc.binaryType = 'arraybuffer';
                dc.bufferedAmountLowThreshold = BUFFER_THRESHOLD;
                openedChannels.push(dc);
            }
        }

        try{
            // Must not start chunking before the channels are open, otherwise
            // slow peers get their early chunks silently dropped.
            const readiness = await Promise.all(
                openedChannels.map((dc) => this.waitForChannelOpen(dc, signal))
            );
            const fileChannels = openedChannels.filter((_, i) => readiness[i]);

            if(signal.aborted){
                throw new DOMException('File transfer aborted', 'AbortError');
            }
            if(fileChannels.length === 0){
                throw new Error('No peer accepted a file channel for this transfer');
            }
            if(fileChannels.length < openedChannels.length){
                console.warn(`[WebRTC] ${openedChannels.length - fileChannels.length} peer(s) did not open a file channel in time`);
            }

            const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
            const meta: FileMetadata = {
                type: 'file-meta',
                fileId,
                fileName: file.name,
                fileSize: file.size,
                totalChunks
            };
            for(const channel of fileChannels){
                channel.send(JSON.stringify(meta));
            }

            let offset = 0;
            let chunksSent = 0;

            //send file in chunks
            while(offset < file.size){
                if(signal.aborted){
                    throw new DOMException('File transfer aborted', 'AbortError');
                }
                const slice = file.slice(offset, offset + CHUNK_SIZE);
                const buffer = await slice.arrayBuffer();
                for(const dc of fileChannels){
                    if(dc.readyState !== 'open') continue;
                    if(dc.bufferedAmount > BUFFER_THRESHOLD) {
                        await this.waitForDrain(dc, signal);
                    }
                    if(dc.readyState !== 'open') continue;
                    dc.send(buffer);
                }
                offset += CHUNK_SIZE;
                chunksSent++;
                if(onProgress){
                    const percent = Math.min(Math.round((chunksSent / totalChunks) * 100), 100);
                    onProgress(percent);
                }
            }

            //close channels after transfer
            for(const dc of fileChannels){
                if(dc.readyState === 'open') dc.close();
            }
            return fileId;
        }
        catch (error:any) {
            const isAborted = error?.name === 'AbortError';
            for(const dc of openedChannels){
                try{
                    if(dc.readyState === 'open') {
                        if(isAborted){
                            dc.send(JSON.stringify({ type: 'file-cancel', fileId }));
                        }
                    }
                    dc.close();
                }
                catch{}
            }
            throw error;
        }
        finally {
            this.activeUploads.delete(fileId);
        }

    }
};

export const webRTCService = new WebRTCService();
