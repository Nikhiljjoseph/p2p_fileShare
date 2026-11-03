using Microsoft.AspNetCore.SignalR;

namespace P2PDataShare.Signaling.Hubs;

public class SignalingHub : Hub
{
    public async Task JoinRoom(string roomName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, roomName);
        await Clients.OthersInGroup(roomName).SendAsync("PeerJoined", Context.ConnectionId);
    }

    public async Task LeaveRoom(string roomName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, roomName);
        await Clients.OthersInGroup(roomName).SendAsync("PeerLeft", Context.ConnectionId);

    }

    public async Task SendSdp(string targetPeerId, string sdp, string type)
    {
        await Clients.Client(targetPeerId).SendAsync("ReceiveSdp", Context.ConnectionId, sdp, type);
    }

    public async Task SendIceCandidate(string targetPeerId, string candidate, string sdpMid, int? sdpMLineIndex)
    {
        await Clients.Client(targetPeerId).SendAsync("ReceiveIceCandidate", Context.ConnectionId, candidate, sdpMid, sdpMLineIndex);
    }
}