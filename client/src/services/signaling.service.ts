import * as signalR from '@microsoft/signalr';

export class SignalingService{
    private hubConnection: signalR.HubConnection | null=null;
    public onPeerJoined?: (peerId: string) => void;
    public onPeerLeft?: (peerId: string) => void;
    public onSdpReceived?: (senderId: string, sdp: string, type: 'offer'|'answer') => void;
    public onIceCandidateReceived?: (senderId: string, candidate: string, sdpMid: string|null, sdpMLineIndex: number | null) => void;

    public async startConnection(hubUrl = 'http://localhost:5041/hubs/signaling'):Promise<string>{
        this.hubConnection = new signalR.HubConnectionBuilder()
        .withUrl(hubUrl)
        .withAutomaticReconnect()
        .build();''

        // Register event handlers for incoming messages from the SignalR hub
        this.hubConnection.on('PeerJoined',(peerId:string) => {
            console.log('[SignalR] PeerJoined received for:', peerId);
            this.onPeerJoined?.(peerId);
        });

        this.hubConnection.on('PeerLeft', (peerId:string) => {
            console.log('[SignalR] PeerLeft received for:', peerId);
            this.onPeerLeft?.(peerId);
        });

        this.hubConnection.on('ReceiveSdp',(senderId:string, sdp:string, type:'offer'|'answer') => {
            this.onSdpReceived?.(senderId, sdp, type);
        });

        this.hubConnection.on('ReceiveIceCandidate',(senderId:string, candidate:string, sdpMid:string|null, sdpMLineIndex:number|null) => {
            this.onIceCandidateReceived?.(senderId, candidate, sdpMid, sdpMLineIndex);
        });

        await this.hubConnection.start();
        return this.hubConnection.connectionId ?? '';
    }

    // Join a specific room 
    public async joinRoom(roomName:string):Promise<void>{
        if(!this.hubConnection)
            throw new Error("Not Connected");
        await this.hubConnection.invoke('joinRoom', roomName);
            
    }

    //leave a room
    public async leaveRoom(roomName:string):Promise<void>{
        if(this.hubConnection?.state == signalR.HubConnectionState.Connected)
            await this.hubConnection.invoke('leaveRoom', roomName);
    }

    // Send an SDP offer or answer to a specific peer
    public async sendSdp(targetPeerId:string, sdp:string, type:'offer'|'answer'):Promise<void>{
        if(!this.hubConnection)
            throw new Error("Not Connected");
        await this.hubConnection.invoke('SendSdp', targetPeerId, sdp, type);
    }

    // Send an ICE candidate to a specific peer
    public async sendIceCandidate(targetPeerId:string, candidate:string, sdpMid:string|null, sdpMLineIndex:number|null):Promise<void>{
        if(!this.hubConnection)
            throw new Error("Not Connected");
        await this.hubConnection.invoke('SendIceCandidate', targetPeerId, candidate, sdpMid, sdpMLineIndex);
    }

    // Disconnect from the signaling server
    public async disconnect():Promise<void>{
        if(this.hubConnection){
            await this.hubConnection.stop();
        }
    }
}

export const signalingService = new SignalingService();