import {signalingService} from './signaling.service';

const RTC_CONFIG: RTCConfiguration = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
    ],
};

const CHUNK_SIZE = 16 * 1024; // 16KB chunk size 
const BUFFER_THRESHOLD = 64 * 1024; // 64KB untill the buffer is full

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
            try {
                await session.peerConnection.addIceCandidate(new RTCIceCandidate({ candidate, sdpMid, sdpMLineIndex }));
            } catch (error) {
                console.error("Error adding received ICE candidate", error);
            }
        }

        //cleanup if a peer left the group
        signalingService.onPeerLeft = (peerId: string) => {
            this.removePeer(peerId);
        }
    }

    // Get or create a peer connection for a specific target peer
    private getOrCreatePeer(peerId:string): PeerSession {
        const existing = this.peers.get(peerId);
        if(existing) return existing;

        const pc = new RTCPeerConnection(RTC_CONFIG);
        const session: PeerSession = { peerConnection: pc, dataChannel: null };
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

        //binds appropriate data channels for chat and file transfer
        pc.ondatachannel = (event) => {
            const channel = event.channel;
            if(channel.label.startsWith('file-')){
                const fileId = channel.label.replace('file-', '');
                this.bindFileRecieverChannel(peerId, fileId, channel);
                return;
            }
            if(channel.label === 'chat-channel'){
                session.dataChannel = channel;
                this.bindChatDataChannel(peerId, session, channel);
            }
        }
        this.peers.set(peerId, session);
        return session;
    }

    // Create an SDP offer for a specific target peer and send it via the signaling service
    public async createOffer(targetPeerId:string): Promise<void> {
        const session = this.getOrCreatePeer(targetPeerId);
        const dc = session.peerConnection.createDataChannel('data-channel');
        this.bindChatDataChannel(targetPeerId, session, dc);
        const offer = await session.peerConnection.createOffer();
        await session.peerConnection.setLocalDescription(offer);
        await signalingService.sendSdp(targetPeerId, offer.sdp ?? '', 'offer');
    }

    // Handle an incoming SDP offer from a peer, create an answer, and send it back
    private async handleOffer(senderId:string, sdpOffer:string): Promise<void> {
        const session = this.getOrCreatePeer(senderId);
        session.peerConnection.ondatachannel = (event) => {
            this.bindChatDataChannel(senderId, session, event.channel);
        }
        await session.peerConnection.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp: sdpOffer }));
        const answer = await session.peerConnection.createAnswer();
        await session.peerConnection.setLocalDescription(answer);
        await signalingService.sendSdp(senderId, answer.sdp ?? '', 'answer');
    }

    // Handle an incoming SDP answer from a peer and set it as the remote description
    private async handleAnswer(senderId:string, sdpAnswer:string): Promise<void> {
        const session = this.peers.get(senderId);
        if(!session) return;
        await session.peerConnection.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: sdpAnswer }));
    }

    // channel for text messages 
    private bindChatDataChannel(peerId:string, session: PeerSession, channel: RTCDataChannel): void {
        session.dataChannel = channel;
        channel.binaryType = 'arraybuffer'; // failsafe
        channel.onopen = () => {
            this.notifyPeersChange();
        }
        channel.onclose = () => {
            this.removePeer(peerId);
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

        channel.onmessage = (event) => {
            if(typeof event.data === 'string') {
                try {
                    const parsed = JSON.parse(event.data);
                    if(parsed.type === 'file-meta') {
                        this.activeIncoming.set(fileId, {
                        meta: parsed,
                        recievedChunks: [],
                        receivedBytes: 0
                    });
                    this.onFileProgress?.(peerId, fileId, parsed.fileName, 0);
                    return;
                    }
                    if(parsed.type === 'file-cancel') {
                        const transfer = this.activeIncoming.get(fileId);
                        const fileName = transfer?.meta.fileName ?? 'Transfer';
                        this.activeIncoming.delete(fileId);
                        this.onFileCancelled?.(peerId, fileId, fileName);
                        return;
                    }
                } catch (error) {
                console.error('Error parsing file message:', error);
                }
                return;
            }
            if(event.data instanceof ArrayBuffer) {
                const transfer = this.activeIncoming.get(fileId);
                if(!transfer) return;

                transfer.recievedChunks.push(event.data);
                transfer.receivedBytes += event.data.byteLength;

                const percent = Math.min(Math.round((transfer.receivedBytes / transfer.meta.fileSize) * 100), 100);
                this.onFileProgress?.(peerId, fileId, transfer.meta.fileName, percent);

                if(transfer.recievedChunks.length >= transfer.meta.totalChunks) {
                    const finalBlob = new Blob(transfer.recievedChunks);
                    this.onFileReceived?.(peerId, transfer.meta.fileName, finalBlob);
                    this.activeIncoming.delete(fileId);
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

    public async sendFile(file: File, onFileReady?:(fileId: string)=> void, onProgress?: (percent: number)=> void): Promise<string> {
        const fileId = crypto.randomUUID();
        const abortController = new AbortController();
        this.activeUploads.set(fileId, abortController);
        onFileReady?. (fileId);

        const fileChannels:RTCDataChannel[] = [];
        for(const[_, session] of this.peers){
            if(session.peerConnection.connectionState === 'connected'){
                const dc = session.peerConnection.createDataChannel(`file-${fileId}`);
                dc.binaryType = 'arraybuffer';
                dc.bufferedAmountLowThreshold = BUFFER_THRESHOLD;
                fileChannels.push(dc);
            }
        }

        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
        const meta: FileMetadata = {
            type: 'file-meta',
            fileId,
            fileName: file.name,
            fileSize: file.size,
            totalChunks
        };
        //send metadata after channel opens
        for(const channel of fileChannels){
            if(channel.readyState === 'open'){
                channel.send(JSON.stringify(meta));
            }
            else{
                channel.onopen = () => {
                    channel.send(JSON.stringify(meta));
                }
            }
        }

        let offset = 0;
        let chunksSent = 0;

        //send file in chunks
        try{
            while(offset < file.size){
                if(abortController.signal.aborted){
                    throw new DOMException('File transfer aborted', 'AbortError');
                }
                const slice = file.slice(offset, offset + CHUNK_SIZE);
                const buffer = await slice.arrayBuffer();
                for(const dc of fileChannels){
                    if(!dc || dc.readyState !== 'open') continue;
                    if(dc.bufferedAmount > BUFFER_THRESHOLD) {
                        await new Promise<void>((resolve,reject)=>{
                            const handleDrain = () => {
                                dc.removeEventListener('bufferedamountlow', handleDrain);
                                resolve();
                            }
                            const handleAbort = () => {
                                dc.removeEventListener('bufferedamountlow', handleDrain);
                                reject(new DOMException('File transfer aborted', 'AbortError'));
                            }
                            abortController.signal.addEventListener('abort', handleAbort, { once: true });
                            dc.addEventListener('bufferedamountlow', handleDrain);
                        });
                    }
                    dc.send(buffer);
                }
                offset += CHUNK_SIZE;
                chunksSent++;
                if(onProgress){
                    const percent = Math.min(Math.round((chunksSent / totalChunks) * 100), 100);
                    onProgress(percent);
                }
            }

            //close cannels afrer transfer
            for(const dc of fileChannels){
                if(dc.readyState === 'open') dc.close();
            }
            return fileId;
        }
        catch (error:any) {
            const isAborted = error.name === 'AbortError';
            for(const dc of fileChannels){
                try{
                    if(dc.readyState === 'open') {
                        if(isAborted){
                            dc.send(JSON.stringify({ type: 'file-cancel', fileId }));
                        }
                        dc.close();
                    }
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
