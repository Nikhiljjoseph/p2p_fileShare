using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

namespace P2PDataShare.Signaling.Hubs;

public class SignalingHub : Hub
{
    private static readonly ConcurrentDictionary<string, HashSet<string>> RoomsByConnection = new();
    public async Task JoinRoom(string roomName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, roomName);
        var rooms = RoomsByConnection.GetOrAdd(Context.ConnectionId, _ => new HashSet<string>());
        lock(rooms){rooms.Add(roomName);}
        await Clients.OthersInGroup(roomName).SendAsync("PeerJoined", Context.ConnectionId);
    }

    public async Task LeaveRoom(string roomName)
    {
        await Clients.OthersInGroup(roomName).SendAsync("PeerLeft", Context.ConnectionId);
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, roomName);
        if(RoomsByConnection.TryGetValue(Context.ConnectionId, out var rooms))
        {
            lock(rooms){rooms.Remove(roomName);}
        }
    }

    public async Task SendSdp(string targetPeerId, string sdp, string type)
    {
        await Clients.Client(targetPeerId).SendAsync("ReceiveSdp", Context.ConnectionId, sdp, type);
    }

    public async Task SendIceCandidate(string targetPeerId, string candidate, string sdpMid, int? sdpMLineIndex)
    {
        await Clients.Client(targetPeerId).SendAsync("ReceiveIceCandidate", Context.ConnectionId, candidate, sdpMid, sdpMLineIndex);
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        if(RoomsByConnection.TryRemove(Context.ConnectionId, out var rooms))
        {
            string[] snapshot;
            lock(rooms){ snapshot = rooms.ToArray();}
            foreach(var roomName in snapshot)
            {
                await Clients.OthersInGroup(roomName).SendAsync("PeerLeft", Context.ConnectionId);
            }
        }

        await base.OnDisconnectedAsync(exception);   
    }
}