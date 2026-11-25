import * as signalR from '@microsoft/signalr';

export class SignalingService {
    private hubConnection: signalR.HubConnection | null = null;
    private startPromise: Promise<string> | null = null;
    private handlersRegistered = false;

    public onPeerJoined?: (peerId: string) => void;
    public onPeerLeft?: (peerId: string) => void;
    public onSdpReceived?: (senderId: string, sdp: string, type: 'offer' | 'answer') => void;
    public onIceCandidateReceived?: (senderId: string, candidate: string, sdpMid: string | null, sdpMLineIndex: number | null) => void;

    private registerHandlers(connection: signalR.HubConnection): void {
        if (this.handlersRegistered) return;
        this.handlersRegistered = true;

        // Register event handlers for incoming messages from the SignalR hub
        connection.on('PeerJoined', (peerId: string) => {
            console.log('[SignalR] PeerJoined received for:', peerId);
            this.onPeerJoined?.(peerId);
        });

        connection.on('PeerLeft', (peerId: string) => {
            console.log('[SignalR] PeerLeft received for:', peerId);
            this.onPeerLeft?.(peerId);
        });

        connection.on('ReceiveSdp', (senderId: string, sdp: string, type: 'offer' | 'answer') => {
            this.onSdpReceived?.(senderId, sdp, type);
        });

        connection.on('ReceiveIceCandidate', (senderId: string, candidate: string, sdpMid: string | null, sdpMLineIndex: number | null) => {
            this.onIceCandidateReceived?.(senderId, candidate, sdpMid, sdpMLineIndex);
        });

        connection.onclose(() => {
            this.startPromise = null;
        });
    }

    public async startConnection(hubUrl = 'http://localhost:5041/hubs/signaling'): Promise<string> {
        if (this.hubConnection?.state === signalR.HubConnectionState.Connected) {
            return this.hubConnection.connectionId ?? '';
        }

        if (this.startPromise) {
            return this.startPromise;
        }

        if (!this.hubConnection) {
            this.hubConnection = new signalR.HubConnectionBuilder()
                .withUrl(hubUrl)
                .withAutomaticReconnect()
                .build();
        }

        const connection = this.hubConnection;
        this.registerHandlers(connection);

        this.startPromise = (async () => {
            try {
                if (connection.state === signalR.HubConnectionState.Disconnected) {
                    await connection.start();
                }
                return connection.connectionId ?? '';
            } catch (err) {
                this.startPromise = null;
                throw err;
            }
        })();

        return this.startPromise;
    }

    // Join a specific room
    public async joinRoom(roomName: string): Promise<void> {
        if (!this.hubConnection || this.hubConnection.state !== signalR.HubConnectionState.Connected)
            throw new Error("Not Connected");
        await this.hubConnection.invoke('joinRoom', roomName);
    }

    // Leave a room
    public async leaveRoom(roomName: string): Promise<void> {
        if (this.hubConnection?.state === signalR.HubConnectionState.Connected)
            await this.hubConnection.invoke('leaveRoom', roomName);
    }

    // Send an SDP offer or answer to a specific peer
    public async sendSdp(targetPeerId: string, sdp: string, type: 'offer' | 'answer'): Promise<void> {
        if (!this.hubConnection || this.hubConnection.state !== signalR.HubConnectionState.Connected)
            throw new Error("Not Connected");
        await this.hubConnection.invoke('SendSdp', targetPeerId, sdp, type);
    }

    // Send an ICE candidate to a specific peer
    public async sendIceCandidate(targetPeerId: string, candidate: string, sdpMid: string | null, sdpMLineIndex: number | null): Promise<void> {
        if (!this.hubConnection || this.hubConnection.state !== signalR.HubConnectionState.Connected)
            throw new Error("Not Connected");
        await this.hubConnection.invoke('SendIceCandidate', targetPeerId, candidate, sdpMid, sdpMLineIndex);
    }

    // Disconnect from the signaling server
    public async disconnect(): Promise<void> {
        this.startPromise = null;
        if (this.hubConnection) {
            await this.hubConnection.stop();
        }
    }
}

export const signalingService = new SignalingService();