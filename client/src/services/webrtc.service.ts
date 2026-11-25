import {signalingService} from './signaling.service';
import {transferStore, transferRecordId, type TransferRecord} from './transfer.store';

const RTC_CONFIG: RTCConfiguration = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
    ],
};

const FLUSH_EVERY_CHUNKS = 64; // receiver persists data in ~1MB batches (64 x 16KB)
const CHAT_CHANNEL_LABEL = 'chat-channel'; // must match on both ends of the router
const FILE_CHANNEL_PREFIX = 'file-';
const CHANNEL_OPEN_TIMEOUT = 15000; // give a fresh file channel this long to open
const REPLY_TIMEOUT = 15000; // how long a sender waits for the receiver's file-ack
const DELIVERY_TIMEOUT = 30000; // how long a sender waits for the receiver to confirm it saved everything
const CLIENT_ID_KEY = 'p2p_client_id';

export interface FileMetadata {
    type: 'file-meta';
    fileId: string;
    fileKey: string;
    fileName: string;
    fileSize: number;
    chunkSize: number;
    totalChunks: number;
}

interface FileAckMessage {
    type: 'file-ack';
    fileKey: string;
    fromChunk: number; // the receiver already has every chunk before this one
}

interface FileDeclineMessage {
    type: 'file-decline'; // receiver is already getting this file from someone else
    fileKey: string;
}

export interface FileCancelMessage {
    type: 'file-cancel';
    fileKey: string;
}

type FileReply = FileAckMessage | FileDeclineMessage;

type ChatEnvelope =
    | { type: 'chat'; text: string }
    | { type: 'resume-request'; fileKey: string };

export type UploadOutcome = 'done' | 'interrupted' | 'cancelled' | 'failed' | 'skipped';
type PeerSendResult = 'complete' | 'interrupted' | 'declined';

export interface PausedTransfer {
    fileKey: string;
    fileName: string;
    progressPercent: number;
}

interface IncomingTransfer {
    peerId: string;
    channel: RTCDataChannel;
    record: TransferRecord;   // last state confirmed on disk
    pending: ArrayBuffer[];   // received, not yet handed to the write chain
    receivedChunks: number;   // stored + queued + pending
    queuedChunks: number;     // stored + queued (what the next record will claim)
    nextPartIndex: number;
    writeChain: Promise<void>; // serialises IndexedDB writes in arrival order
    lastPercent: number;
    state: 'receiving' | 'finishing' | 'paused' | 'discarded' | 'done';
    failed: boolean;
}

interface ActiveUpload {
    controller: AbortController;
    fileKey: string;
}

interface PeerSession{
    peerConnection: RTCPeerConnection;
    dataChannel: RTCDataChannel|null;
    // ICE can arrive before the offer/answer is applied; hold them until then
    pendingCandidates: RTCIceCandidateInit[];
    hasRemoteDescription: boolean;
    // Aborted when the peer is removed. pc.close() does not fire 'close' on its
    // data channels, so anything waiting on a channel listens to this as well.
    teardown: AbortController;
}

// sessionStorage is per tab and survives reloads, so this identifies "this tab"
// across a refresh. Partial files are namespaced by it because every tab of the
// same browser shares one IndexedDB.
function getClientId(): string {
    let id = sessionStorage.getItem(CLIENT_ID_KEY);
    if(!id){
        id = crypto.randomUUID();
        sessionStorage.setItem(CLIENT_ID_KEY, id);
    }
    return id;
}

// Stable identity for a file, independent of peer IDs (which change on every
// reconnect). Including lastModified means an edited file gets a new key, so we
// never resume one version of a file with bytes from another.
async function fileKeyOf(file: File): Promise<string> {
    const raw = new TextEncoder().encode(`${file.name}|${file.size}|${file.lastModified}`);
    const digest = await crypto.subtle.digest('SHA-256', raw);
    return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, '0')).join('');
}

function percentOf(done: number, total: number): number {
    if(total === 0) return 100;
    return Math.min(Math.round((done / total) * 100), 100);
}

function abortError(): DOMException {
    return new DOMException('File transfer aborted', 'AbortError');
}

export class WebRTCService {
    public currentChunkSize = 64 * 1024; // the bufferedAmountLowThreshold is twice the chunk size with a flooe of 64kb
    private peers = new Map<string, PeerSession>();
    private readonly clientId = getClientId();
    // Receiving side
    private activeIncoming = new Map<string, IncomingTransfer>(); // fileKey -> transfer
    private claimedIncoming = new Set<string>(); // fileKeys being set up right now
    private settlingWrites = new Map<string, Promise<void>>(); // writes still landing for paused transfers
    private restorePromise: Promise<PausedTransfer[]> | null = null;
    // Sending side
    private activeUploads = new Map<string, ActiveUpload>(); // uploadId -> upload
    private sharedFiles = new Map<string, File>(); // fileKey -> file we can (re)send
    private outgoing = new Set<string>(); // `${peerId}::${fileKey}` currently being sent

    public onPeersChange?: (connectedPeerIds: string[]) => void;
    public onMessageReceived?: (senderId:string, message:string) => void;
    public onFileProgress?: (senderId:string, fileKey:string, fileName:string, progressPercent:number) => void;
    public onFileReceived?: (senderId:string, fileName:string, fileBlob:Blob) => void;
    public onFileCancelled?: (senderId:string, fileKey:string, fileName:string) => void;
    public onFilePaused?: (fileKey:string, fileName:string, progressPercent:number) => void;
    public onUploadProgress?: (uploadId:string, fileName:string, progressPercent:number, resumed:boolean) => void;
    public onUploadFinished?: (uploadId:string, fileName:string, outcome:UploadOutcome) => void;

    constructor() {
        this.setupSignalListeners();
        transferStore.removeStale().catch((error) => console.warn('[WebRTC] Could not clean up stale transfers', error));
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
            teardown: new AbortController(),
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
                this.bindFileRecieverChannel(peerId, channel);
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

    // channel for text messages and control messages
    private bindChatDataChannel(peerId:string, session: PeerSession, channel: RTCDataChannel): void {
        session.dataChannel = channel;
        channel.binaryType = 'arraybuffer'; // failsafe
        const handleOpen = () => {
            this.notifyPeersChange();
            // A (re)connected peer may hold files we only partly received
            void this.requestResumes(channel);
        };
        channel.onopen = handleOpen;
        channel.onclose = () => {
            // Only the live chat channel tearing down means we lost the peer
            if(session.dataChannel === channel) this.removePeer(peerId);
        }
        channel.onmessage = (event) => {
            if(typeof event.data === 'string') {
                this.handleChatMessage(peerId, event.data);
            }
        }
        // The answering side can be handed a channel that is already open
        if(channel.readyState === 'open') handleOpen();
    }

    private handleChatMessage(peerId: string, raw: string): void {
        let envelope: ChatEnvelope | null = null;
        try {
            const parsed = JSON.parse(raw);
            if(parsed && typeof parsed === 'object' && typeof parsed.type === 'string') envelope = parsed;
        } catch {
            // not JSON: plain text from a client that predates envelopes
        }
        if(!envelope){
            this.onMessageReceived?.(peerId, raw);
            return;
        }
        switch(envelope.type){
            case 'chat':
                this.onMessageReceived?.(peerId, envelope.text);
                break;
            case 'resume-request': {
                const file = this.sharedFiles.get(envelope.fileKey);
                if(file) void this.runUpload(file, envelope.fileKey, [peerId], true);
                break;
            }
        }
    }

    // Ask a peer to resend anything we have only partly received
    private async requestResumes(channel: RTCDataChannel): Promise<void> {
        let records: TransferRecord[];
        try {
            records = await transferStore.listForClient(this.clientId);
        } catch (error) {
            console.warn('[WebRTC] Could not read stored transfers', error);
            return;
        }
        for(const record of records){
            if(record.committedChunks >= record.totalChunks) continue;
            if(this.activeIncoming.has(record.fileKey)) continue;
            this.sendJson(channel, { type: 'resume-request', fileKey: record.fileKey });
        }
    }

    private sendJson(channel: RTCDataChannel, message: object): void {
        if(channel.readyState !== 'open') return;
        try {
            channel.send(JSON.stringify(message));
        } catch (error) {
            console.warn('[WebRTC] Failed to send control message', error);
        }
    }

    // ---- Receiving ----

    //channel for concurrent file transfers
    private bindFileRecieverChannel(peerId:string, channel: RTCDataChannel): void {
        channel.binaryType = 'arraybuffer';
        let transfer: IncomingTransfer | null = null;

        // A file channel closing mid-transfer pauses it; the data so far stays on disk
        channel.onclose = () => {
            if(transfer) this.pauseIncoming(transfer);
        }

        channel.onmessage = (event) => {
            if(typeof event.data === 'string') {
                let parsed: any;
                try {
                    parsed = JSON.parse(event.data);
                } catch (error) {
                    console.error('Error parsing file message:', error);
                    return;
                }
                if(parsed?.type === 'file-meta') {
                    void this.acceptIncoming(peerId, channel, parsed as FileMetadata, (t) => { transfer = t; });
                    return;
                }
                if(parsed?.type === 'file-cancel' && transfer) {
                    const { fileKey, fileName } = transfer.record;
                    void this.discardTransfer(transfer);
                    this.onFileCancelled?.(peerId, fileKey, fileName);
                }
                return;
            }
            if(event.data instanceof ArrayBuffer && transfer) {
                this.handleChunk(transfer, event.data);
            }
        }
    }

    // Look up what we already have for this file and tell the sender where to start
    private async acceptIncoming(peerId: string, channel: RTCDataChannel, meta: FileMetadata, bind: (t: IncomingTransfer) => void): Promise<void> {
        const { fileKey } = meta;
        // Several peers may hold the same file; take it from whoever got here first
        if(this.activeIncoming.has(fileKey) || this.claimedIncoming.has(fileKey)){
            this.sendJson(channel, { type: 'file-decline', fileKey });
            return;
        }
        this.claimedIncoming.add(fileKey);
        try {
            // A just-paused transfer may still be writing its last batch
            await this.settlingWrites.get(fileKey);

            const id = transferRecordId(this.clientId, fileKey);
            const existing = await transferStore.get(id);
            const canResume = !!existing
                && existing.fileSize === meta.fileSize
                && existing.chunkSize === meta.chunkSize
                && existing.totalChunks === meta.totalChunks;

            const record: TransferRecord = canResume
                ? { ...existing!, updatedAt: Date.now() }
                : {
                    id,
                    clientId: this.clientId,
                    fileKey,
                    fileName: meta.fileName,
                    fileSize: meta.fileSize,
                    chunkSize: meta.chunkSize,
                    totalChunks: meta.totalChunks,
                    committedChunks: 0,
                    partCount: 0,
                    updatedAt: Date.now(),
                };
            if(!canResume) await transferStore.reset(record);
            if(channel.readyState !== 'open') return; // sender left while we were reading storage

            const transfer: IncomingTransfer = {
                peerId,
                channel,
                record,
                pending: [],
                receivedChunks: record.committedChunks,
                queuedChunks: record.committedChunks,
                nextPartIndex: record.partCount,
                writeChain: Promise.resolve(),
                lastPercent: -1,
                state: 'receiving',
                failed: false,
            };
            this.activeIncoming.set(fileKey, transfer);
            bind(transfer);
            this.reportIncomingProgress(transfer);
            this.sendJson(channel, { type: 'file-ack', fileKey, fromChunk: record.committedChunks });

            // Nothing left to send (empty file, or everything already stored)
            if(transfer.receivedChunks >= record.totalChunks) this.finishIncoming(transfer);
        } catch (error) {
            console.error('[WebRTC] Could not prepare incoming transfer', error);
            this.sendJson(channel, { type: 'file-decline', fileKey });
        } finally {
            this.claimedIncoming.delete(fileKey);
        }
    }

    private handleChunk(transfer: IncomingTransfer, data: ArrayBuffer): void {
        if(transfer.state !== 'receiving') return;
        transfer.pending.push(data);
        transfer.receivedChunks++;
        const isLast = transfer.receivedChunks >= transfer.record.totalChunks;
        if(transfer.pending.length >= FLUSH_EVERY_CHUNKS || isLast) this.queueFlush(transfer);
        this.reportIncomingProgress(transfer);
        if(isLast) this.finishIncoming(transfer);
    }

    // onmessage is synchronous but IndexedDB is not, so chunks keep arriving while
    // a batch is being written. Take the batch now and chain the write, so batches
    // land in order and the stored counter only ever covers contiguous data.
    private queueFlush(transfer: IncomingTransfer): void {
        const batch = transfer.pending;
        transfer.pending = [];
        if(batch.length === 0) return;

        const index = transfer.nextPartIndex++;
        transfer.queuedChunks += batch.length;
        const committedAfter = transfer.queuedChunks;

        transfer.writeChain = transfer.writeChain.then(async () => {
            if(transfer.failed || transfer.state === 'discarded') return;
            const record: TransferRecord = {
                ...transfer.record,
                committedChunks: committedAfter,
                partCount: index + 1,
                updatedAt: Date.now(),
            };
            try {
                await transferStore.appendPart(record, index, new Blob(batch));
                transfer.record = record;
            } catch (error) {
                // e.g. storage quota. Later batches are skipped so the stored data stays
                // contiguous; closing the channel pauses the transfer at the last good batch.
                console.error('[WebRTC] Failed to store received data', error);
                transfer.failed = true;
                transfer.channel.close();
            }
        });
    }

    private reportIncomingProgress(transfer: IncomingTransfer): void {
        const percent = percentOf(transfer.receivedChunks, transfer.record.totalChunks);
        if(percent === transfer.lastPercent) return; // avoid a React update per 16KB chunk
        transfer.lastPercent = percent;
        this.onFileProgress?.(transfer.peerId, transfer.record.fileKey, transfer.record.fileName, percent);
    }

    private finishIncoming(transfer: IncomingTransfer): void {
        transfer.state = 'finishing';
        transfer.writeChain
            .then(async () => {
                if(transfer.failed || transfer.state !== 'finishing') return;
                const blob = await transferStore.assemble(transfer.record.id);
                transfer.state = 'done';
                this.activeIncoming.delete(transfer.record.fileKey);
                this.onFileReceived?.(transfer.peerId, transfer.record.fileName, blob);
                // Closing tells the sender the file is safely stored
                transfer.channel.close();
                await transferStore.remove(transfer.record.id);
            })
            .catch((error) => {
                console.error('[WebRTC] Failed to assemble received file', error);
                transfer.failed = true;
                this.pauseIncoming(transfer);
                transfer.channel.close();
            });
    }

    private pauseIncoming(transfer: IncomingTransfer): void {
        if(transfer.state === 'paused' || transfer.state === 'discarded' || transfer.state === 'done') return;
        // Every chunk is already in hand; let the final save finish
        if(transfer.state === 'finishing' && !transfer.failed) return;

        transfer.state = 'paused';
        transfer.pending = []; // never stored, so the sender will resend these
        const { fileKey, fileName } = transfer.record;
        if(this.activeIncoming.get(fileKey) === transfer) this.activeIncoming.delete(fileKey);

        const settled = transfer.writeChain.then(() => {
            if(this.settlingWrites.get(fileKey) === settled) this.settlingWrites.delete(fileKey);
            this.onFilePaused?.(fileKey, fileName, percentOf(transfer.record.committedChunks, transfer.record.totalChunks));
        });
        this.settlingWrites.set(fileKey, settled);
    }

    private async discardTransfer(transfer: IncomingTransfer): Promise<void> {
        transfer.state = 'discarded';
        const { fileKey, id } = transfer.record;
        if(this.activeIncoming.get(fileKey) === transfer) this.activeIncoming.delete(fileKey);
        transfer.channel.close();
        await transfer.writeChain;
        await transferStore.remove(id).catch((error) => console.warn('[WebRTC] Could not delete partial file', error));
    }

    // Drop a paused (or running) download and its stored data
    public async discardIncoming(fileKey: string): Promise<void> {
        const active = this.activeIncoming.get(fileKey);
        if(active){
            await this.discardTransfer(active);
            return;
        }
        await this.settlingWrites.get(fileKey);
        await transferStore.remove(transferRecordId(this.clientId, fileKey));
    }

    // Called once on startup: deliver files that were fully stored but never handed
    // to the user (e.g. the tab closed mid-download), and report the rest as paused.
    public restoreTransfers(): Promise<PausedTransfer[]> {
        if(!this.restorePromise) this.restorePromise = this.loadStoredTransfers();
        return this.restorePromise;
    }

    private async loadStoredTransfers(): Promise<PausedTransfer[]> {
        let records: TransferRecord[];
        try {
            records = await transferStore.listForClient(this.clientId);
        } catch (error) {
            console.warn('[WebRTC] Could not read stored transfers', error);
            return [];
        }
        const paused: PausedTransfer[] = [];
        for(const record of records){
            if(record.committedChunks >= record.totalChunks){
                try {
                    const blob = await transferStore.assemble(record.id);
                    this.onFileReceived?.('restored', record.fileName, blob);
                    await transferStore.remove(record.id);
                } catch (error) {
                    console.error('[WebRTC] Could not restore stored file', error);
                }
                continue;
            }
            paused.push({
                fileKey: record.fileKey,
                fileName: record.fileName,
                progressPercent: percentOf(record.committedChunks, record.totalChunks),
            });
        }
        return paused;
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
            session.teardown.abort();
            session.dataChannel?.close();
            session.peerConnection.close();
            this.peers.delete(peerId);
            // pc.close() doesn't fire 'close' on file channels, so pause their transfers here
            for(const transfer of Array.from(this.activeIncoming.values())){
                if(transfer.peerId === peerId) this.pauseIncoming(transfer);
            }
            this.notifyPeersChange();
        }
    }

    public closeAllConnections(): void {
        for(const [_peerId,session] of this.peers){
            session.teardown.abort();
            session.dataChannel?.close();
            session.peerConnection.close();
        }
        this.peers.clear();
        for(const transfer of Array.from(this.activeIncoming.values())){
            this.pauseIncoming(transfer);
        }
        this.activeUploads.forEach((upload)=>upload.controller.abort());
        this.activeUploads.clear();
        this.sharedFiles.clear();
        this.notifyPeersChange();
    }

    public broadcastMessage(msg:string): void {
        console.log(`[WebRTC] Attempting to broadcast to ${this.peers.size} peer(s): "${msg}"`);
        const envelope: ChatEnvelope = { type: 'chat', text: msg };
        const payload = JSON.stringify(envelope);
        for(const [_, session] of this.peers.entries()){
            if(session.dataChannel?.readyState === 'open')
                session.dataChannel?.send(payload);
        }
    }

    // ---- Sending ----

    public cancelTransfer(uploadId: string): void {
        this.activeUploads.get(uploadId)?.controller.abort();
        // the upload loop reports the cancellation through onUploadFinished
    }

    // Resolve true once the channel is usable, false if it never gets there.
    private waitForChannelOpen(dc: RTCDataChannel, signal: AbortSignal): Promise<boolean> {
        if(dc.readyState === 'open') return Promise.resolve(true);
        if(dc.readyState === 'closing' || dc.readyState === 'closed' || signal.aborted) return Promise.resolve(false);

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

    // Wait for the receiver's file-ack / file-decline. Null if it never answers.
    private waitForReply(dc: RTCDataChannel, fileKey: string, signal: AbortSignal): Promise<FileReply | null> {
        return new Promise<FileReply | null>((resolve, reject) => {
            if(signal.aborted){
                reject(abortError());
                return;
            }
            const cleanup = () => {
                clearTimeout(timer);
                dc.removeEventListener('message', onMessage);
                dc.removeEventListener('close', onClose);
                signal.removeEventListener('abort', onAbort);
            };
            const onMessage = (event: MessageEvent) => {
                if(typeof event.data !== 'string') return;
                let parsed: any;
                try { parsed = JSON.parse(event.data); } catch { return; }
                if((parsed?.type === 'file-ack' || parsed?.type === 'file-decline') && parsed.fileKey === fileKey){
                    cleanup();
                    resolve(parsed as FileReply);
                }
            };
            const onClose = () => { cleanup(); resolve(null); };
            const onAbort = () => { cleanup(); reject(abortError()); };
            const timer = setTimeout(onClose, REPLY_TIMEOUT);

            dc.addEventListener('message', onMessage);
            dc.addEventListener('close', onClose, { once: true });
            signal.addEventListener('abort', onAbort, { once: true });
        });
    }

    // Wait for a congested channel to drain, bailing out on abort or close.
    private waitForDrain(dc: RTCDataChannel, signal: AbortSignal): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            if(signal.aborted){
                reject(abortError());
                return;
            }
            const cleanup = () => {
                dc.removeEventListener('bufferedamountlow', onDrain);
                dc.removeEventListener('close', onClose);
                signal.removeEventListener('abort', onAbort);
            };
            const onDrain = () => { cleanup(); resolve(); };
            const onClose = () => { cleanup(); resolve(); }; // dropped peer, caught by the readyState check
            const onAbort = () => { cleanup(); reject(abortError()); };

            dc.addEventListener('bufferedamountlow', onDrain, { once: true });
            dc.addEventListener('close', onClose, { once: true });
            signal.addEventListener('abort', onAbort, { once: true });
        });
    }

    // True if the receiver closed the channel (its "all saved" signal) in time
    private waitForClose(dc: RTCDataChannel, timeoutMs: number, signal: AbortSignal): Promise<boolean> {
        if(dc.readyState === 'closed') return Promise.resolve(true);
        if(signal.aborted) return Promise.resolve(false);
        return new Promise<boolean>((resolve) => {
            const settle = (closed: boolean) => {
                clearTimeout(timer);
                dc.removeEventListener('close', onClose);
                signal.removeEventListener('abort', onAbort);
                resolve(closed);
            };
            const onClose = () => settle(true);
            const onAbort = () => settle(false);
            const timer = setTimeout(() => settle(false), timeoutMs);
            dc.addEventListener('close', onClose, { once: true });
            signal.addEventListener('abort', onAbort, { once: true });
        });
    }

    public async sendFile(file: File): Promise<UploadOutcome> {
        const fileKey = await fileKeyOf(file);
        const targets = Array.from(this.peers.entries())
            .filter(([_, session]) => session.dataChannel?.readyState === 'open')
            .map(([peerId]) => peerId);
        if(targets.length === 0){
            throw new Error('No connected peers to send to');
        }
        // Keep the file so peers that drop out can ask for the rest later
        this.sharedFiles.set(fileKey, file);
        return this.runUpload(file, fileKey, targets, false);
    }

    // One independent send loop per peer: each peer can start from a different
    // chunk, and a slow peer no longer holds back the fast ones.
    private async runUpload(file: File, fileKey: string, peerIds: string[], resumed: boolean): Promise<UploadOutcome> {
        const uploadId = crypto.randomUUID();
        const controller = new AbortController();
        this.activeUploads.set(uploadId, { controller, fileKey });

        const chunkSize = this.currentChunkSize;
        const totalChunks = Math.ceil(file.size / chunkSize);
        const sentByPeer = new Map<string, number>(); // peers still receiving -> chunks sent
        let lastPercent = 0;
        const report = () => {
            if(sentByPeer.size === 0) return;
            // Overall progress is the slowest peer that is still receiving
            const percent = percentOf(Math.min(...sentByPeer.values()), totalChunks);
            if(percent === lastPercent) return;
            lastPercent = percent;
            this.onUploadProgress?.(uploadId, file.name, percent, resumed);
        };
        this.onUploadProgress?.(uploadId, file.name, 0, resumed);
        const uploadStart = performance.now();
        let outcome: UploadOutcome = 'failed';
        try {
            const results = await Promise.allSettled(peerIds.map((peerId) =>
                this.sendToPeer(peerId, file, fileKey, totalChunks, controller.signal, (sent) => {
                    sentByPeer.set(peerId, sent);
                    report();
                }).finally(() => {
                    sentByPeer.delete(peerId);
                    report();
                })
            ));

            if(controller.signal.aborted){
                outcome = 'cancelled';
            } else {
                const perPeer = results.map((result) => {
                    if(result.status === 'fulfilled') return result.value;
                    console.error('[WebRTC] File transfer error:', result.reason);
                    return 'failed' as const;
                });
                if(perPeer.every((r) => r === 'declined')) outcome = 'skipped';
                else if(perPeer.every((r) => r === 'complete' || r === 'declined')) outcome = 'done';
                else if(perPeer.some((r) => r === 'interrupted')) outcome = 'interrupted';
                else outcome = 'failed';
            }
        } finally {
            if (outcome === 'done' && !resumed) {
        const elapsedSec = (performance.now() - uploadStart) / 1000;
        const receiverCount = peerIds.length;
        const groupSize = receiverCount + 1; // sender + receivers
        const fileSizeMB = file.size / (1024 * 1024);
        const totalSentMB = fileSizeMB * receiverCount;
        const aggregateMBs = +(totalSentMB / elapsedSec).toFixed(2);
        const perPeerMBs = +(fileSizeMB / elapsedSec).toFixed(2);
        console.log(
            `[BENCH_MESH_UPLOAD] group_size=${groupSize} receivers=${receiverCount} file="${file.name}" file_mb=${fileSizeMB.toFixed(2)} total_sent_mb=${totalSentMB.toFixed(2)} elapsed_s=${elapsedSec.toFixed(3)} agg_MBs=${aggregateMBs} per_peer_MBs=${perPeerMBs}`
        );
    }
            this.activeUploads.delete(uploadId);
            // A cancelled file must not be resumed by a later resume-request
            if(outcome === 'cancelled') this.sharedFiles.delete(fileKey);
            this.onUploadFinished?.(uploadId, file.name, outcome);
        }
        return outcome;
    }

    private async sendToPeer(
        peerId: string,
        file: File,
        fileKey: string,
        totalChunks: number,
        signal: AbortSignal,
        onSent: (chunksSent: number) => void,
    ): Promise<PeerSendResult> {
        const session = this.peers.get(peerId);
        // An open chat channel proves the SCTP association is up, which is all
        // a new data channel needs (no renegotiation required).
        if(!session || session.dataChannel?.readyState !== 'open') return 'interrupted';

        const guardKey = `${peerId}::${fileKey}`;
        if(this.outgoing.has(guardKey)) return 'declined'; // already sending this file to this peer
        this.outgoing.add(guardKey);

        // Stop waiting if the user cancels or the peer connection is torn down
        const waitSignal = AbortSignal.any([signal, session.teardown.signal]);
        const fileId = crypto.randomUUID();
        const dc = session.peerConnection.createDataChannel(`${FILE_CHANNEL_PREFIX}${fileId}`);
        const bufferThreshold = Math.max(64 * 1024, this.currentChunkSize * 2);
        dc.binaryType = 'arraybuffer';
        dc.bufferedAmountLowThreshold = bufferThreshold;

        try {
            // Must not start chunking before the channel is open, otherwise
            // early chunks get silently dropped.
            if(!(await this.waitForChannelOpen(dc, waitSignal))){
                if(signal.aborted) throw abortError();
                console.warn(`[WebRTC] Peer ${peerId} did not open a file channel in time`);
                return 'interrupted';
            }

            const meta: FileMetadata = {
                type: 'file-meta',
                fileId,
                fileKey,
                fileName: file.name,
                fileSize: file.size,
                chunkSize: this.currentChunkSize,
                totalChunks,
            };
            const reply = this.waitForReply(dc, fileKey, waitSignal); // listen before sending
            dc.send(JSON.stringify(meta));
            const answer = await reply;
            if(!answer) return 'interrupted';
            if(answer.type === 'file-decline') return 'declined';

            // The receiver tells us how much it already has
            const fromChunk = Math.max(0, Math.min(answer.fromChunk, totalChunks));
            onSent(fromChunk);

            for(let i = fromChunk; i < totalChunks; i++){
                if(signal.aborted) throw abortError();
                if(dc.bufferedAmount > bufferThreshold) {
                    await this.waitForDrain(dc, waitSignal);
                }
                if(dc.readyState !== 'open') return 'interrupted';
                const buffer = await file.slice(i * this.currentChunkSize, (i + 1) * this.currentChunkSize).arrayBuffer();
                if(dc.readyState !== 'open') return 'interrupted';
                dc.send(buffer);
                onSent(i + 1);
            }

            // The receiver closes the channel once everything is stored
            const delivered = await this.waitForClose(dc, DELIVERY_TIMEOUT, waitSignal);
            return delivered ? 'complete' : 'interrupted';
        }
        catch (error:any) {
            if(signal.aborted){
                this.sendJson(dc, { type: 'file-cancel', fileKey });
                throw abortError();
            }
            // The peer went away mid-send: it can resume later
            if(session.teardown.signal.aborted || dc.readyState !== 'open') return 'interrupted';
            throw error;
        }
        finally {
            this.outgoing.delete(guardKey);
            if(dc.readyState === 'open' || dc.readyState === 'connecting') dc.close();
        }
    }
};

export const webRTCService = new WebRTCService();
