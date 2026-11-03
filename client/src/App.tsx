import { useState, useEffect, useRef } from 'react';
import { signalingService } from './services/signaling.service';
import { webRTCService } from './services/webrtc.service';

interface ReceivedMessage {
  senderId: string;
  text: string;
  timestamp: string;
}

interface TransferStatus {
  fileId: string;
  fileName: string;
  progress: number;
  type: 'upload' | 'download';
  isCancelling?: boolean;
}

export default function App() {
  const [connectionId, setConnectionId] = useState<string>('Connecting...');
  const [currentRoom, setCurrentRoom] = useState<string | null>(null);
  const [roomInput, setRoomInput] = useState<string>('lobby');
  const [connectedPeers, setConnectedPeers] = useState<string[]>([]);
  const [messages, setMessages] = useState<ReceivedMessage[]>([]);
  const [inputDraft, setInputDraft] = useState('');
  const [transfers, setTransfers] = useState<Record<string, TransferStatus>>({});
  const chatBottomRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  // Initialize connection and set up event listeners
  useEffect(() => {
    signalingService.startConnection('http://localhost:5041/hubs/signaling')
      .then((id) => setConnectionId(id))
      .catch((err) => console.error(err));
    
    webRTCService.onPeersChange = (peers) => {
      setConnectedPeers([...peers]);
    }

    webRTCService.onMessageReceived = (senderId, text) => {
      setMessages((prev) => [
        ...prev,
        {
          senderId,
          text,
          timestamp: new Date().toLocaleTimeString(),
        },
      ]);
    }

    //incoming file transfer progress
    webRTCService.onFileProgress = (_senderId, fileId, fileName, progressPercent) => {
      setTransfers((prev) => ({
        ...prev,
        [fileId]: {
          fileId,
          fileName,
          progress: progressPercent,
          type: 'download',
        },
      }));
      if (progressPercent >= 100) {
        setTimeout(() => {
          setTransfers((prev) => {
            const copy = { ...prev };
            delete copy[fileId];
            return copy;
          });
        }, 3000);
      }
    }
    
    //incoming file transfer cancelled
    webRTCService.onFileCancelled = (senderId, fileId, fileName) => {
      setTransfers((prev) => {
        const copy = { ...prev };
        delete copy[fileId];
        return copy;
      });
      setMessages((prev) => [
        ...prev,
        {
          senderId,
          text: `File transfer for ${fileName} was cancelled.`,
          timestamp: new Date().toLocaleTimeString(),
        },
      ]);
    }

    //trigger download after file is fully received
    webRTCService.onFileReceived = (senderId, fileName, fileBlob) => {
      const url = URL.createObjectURL(fileBlob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileName;
      a.click();
      URL.revokeObjectURL(url);
      setMessages((prev) => [
        ...prev,
        {
          senderId,
          text: `Sent a file: ${fileName}`,
          timestamp: new Date().toLocaleTimeString(),
        },
      ]);
    }

    return () => {
      signalingService.disconnect();
    };
  }, []);

  // Scroll to the bottom of the chat when new messages arrive
  useEffect(() => {
    chatBottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Load last joined room from localStorage on mount
  useEffect(() => {
    const savedRoom = localStorage.getItem('p2p_room');
    if (savedRoom) {
      setRoomInput(savedRoom);
    }
    const autoJoin = async () => {
      try {
        //tiny delay for establishing connection before joining room
        await new Promise((resolve) => setTimeout(resolve, 500));
        await signalingService.joinRoom(savedRoom!);
        setCurrentRoom(savedRoom);
      }
      catch (err) {
        console.error('Failed to auto-join room:', err);
        localStorage.removeItem('p2p_room');
      }
    }
    autoJoin();
  }, []);

  const  handleJoinRoom = async() => {
    const trimmed = roomInput.trim();
    if(!trimmed) return;
    //if in a room, leave it first
    if(currentRoom){
      await signalingService.leaveRoom(currentRoom);
      webRTCService.closeAllConnections();
    }
    await signalingService.joinRoom(trimmed);
    setCurrentRoom(trimmed);
    localStorage.setItem('p2p_room', trimmed);
    setMessages([]);
    setTransfers({});
  }

  const handleLeaveRoom = async() => {
    if(!currentRoom) return;
    await signalingService.leaveRoom(currentRoom);
    webRTCService.closeAllConnections();
    setCurrentRoom(null);
    localStorage.removeItem('p2p_room');
    setMessages([]);
    setTransfers({});
  }

  const handleSendMessage = () => {
    const trimmed = inputDraft.trim();
    if(!trimmed || connectedPeers.length === 0) return;
    webRTCService.broadcastMessage(trimmed);
    setMessages(prev => [...prev, { senderId: 'Me', text: trimmed, timestamp: new Date().toLocaleTimeString()}]);
    setInputDraft('');
  }

  const handleFileChange = async(e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if(!files || files.length === 0 || connectedPeers.length === 0) return;

    Array.from(files).forEach(async (file) => {
      let currentFileId = '';
      try{
        await webRTCService.sendFile(
          file,
          (fileId) => {
            currentFileId = fileId;
            setTransfers((prev) => ({
              ...prev,
              [fileId]: {fileId, fileName: file.name, progress: 0, type: 'upload', isCancelling: false}
            }));
          },
          (percent) => {
            setTransfers((prev) => {
              if(!currentFileId) return prev;
              return {
                ...prev,
                [currentFileId]: {...prev[currentFileId], progress: percent}
              }
            });
          }
        );
        setMessages((prev) => [
          ...prev,
          { senderId: 'Me', text: `Sent file: ${file.name}`, timestamp: new Date().toLocaleTimeString() },
        ]);
      }
      catch(error:any){
        if(error.name === 'AbortError'){
            setMessages((prev) => [
            ...prev,
            { senderId: 'Me', text: `Cancelled transfer: ${file.name}`, timestamp: new Date().toLocaleTimeString() },
          ]);
        }
        else{
            console.error('File transfer error:', error);
        }
      }
      finally{
        setTransfers((prev) => {
          const copy = { ...prev };
          if(currentFileId) delete copy[currentFileId];
          return copy;
        });
      }
    });

    if(fileInputRef.current) { fileInputRef.current.value = ''; } //reset file input
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if(e.key === 'Enter') {
      e.preventDefault();
      handleSendMessage();
    }
  }
  return (
    <div style={{ padding: 24, fontFamily: 'system-ui, sans-serif', maxWidth: 640, margin: '0 auto' }}>
      <h2>P2P Mesh Transfer & Rooms</h2>
      <p style={{ margin: '4px 0 16px' }}>
        <strong>My Peer ID:</strong>{' '}
        <code style={{ background: '#eee', padding: '2px 6px', borderRadius: 4 }}>{connectionId}</code>
      </p>

      {/* Room Selection Controls */}
      <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 16 }}>
        <input
          type="text"
          placeholder="Room name..."
          disabled={!!currentRoom}
          value={roomInput}
          onChange={(e) => setRoomInput(e.target.value)}
          style={{ padding: '8px 12px', border: '1px solid #cbd5e1', borderRadius: 6, outline: 'none' }}
        />

        {!currentRoom ? (
          <button
            onClick={handleJoinRoom}
            style={{
              padding: '8px 16px',
              cursor: 'pointer',
              background: '#0070f3',
              color: 'white',
              border: 'none',
              borderRadius: 6,
              fontWeight: 600,
            }}
          >
            Join Room
          </button>
        ) : (
          <button
            onClick={handleLeaveRoom}
            style={{
              padding: '8px 16px',
              cursor: 'pointer',
              background: '#ef4444',
              color: 'white',
              border: 'none',
              borderRadius: 6,
              fontWeight: 600,
            }}
          >
            Leave ({currentRoom})
          </button>
        )}
      </div>

      {/* Peer list */}
      <div style={{ padding: 14, background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 8 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <strong>Connected Peers ({connectedPeers.length})</strong>
          <span style={{ fontSize: '0.85rem', color: connectedPeers.length > 0 ? '#16a34a' : '#64748b' }}>
            {connectedPeers.length > 0 ? `In "${currentRoom}"` : currentRoom ? 'Waiting for others in room...' : 'Join a room to start'}
          </span>
        </div>

        {connectedPeers.length > 0 && (
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginTop: 10 }}>
            {connectedPeers.map((peerId) => (
              <span
                key={peerId}
                style={{
                  padding: '3px 8px',
                  background: '#e0f2fe',
                  color: '#0369a1',
                  borderRadius: 999,
                  fontSize: '0.8rem',
                  fontFamily: 'monospace',
                }}
              >
                ● {peerId.slice(0, 8)}
              </span>
            ))}
          </div>
        )}
      </div>

      {/* Concurrent Transfer Progress Bars */}
      {Object.keys(transfers).length > 0 && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginTop: 14 }}>
          {Object.values(transfers).map((t) => (
            <div key={t.fileId} style={{ padding: 12, background: '#ecfdf5', border: '1px solid #a7f3d0', borderRadius: 8 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '0.85rem', marginBottom: 6 }}>
                <span>
                  {t.type === 'upload' ? '📤 Uploading' : '📥 Downloading'}: <strong>{t.fileName}</strong> ({t.progress}%)
                </span>
                {t.type === 'upload' && (
                  <button
                    disabled={t.isCancelling}
                    onClick={() => {
                      setTransfers((prev) => ({
                        ...prev,
                        [t.fileId]: { ...prev[t.fileId], isCancelling: true },
                      }));
                      webRTCService.cancelTransfer(t.fileId);
                    }}
                    style={{
                      padding: '2px 8px',
                      background: t.isCancelling ? '#94a3b8' : '#ef4444',
                      color: 'white',
                      border: 'none',
                      borderRadius: 4,
                      cursor: t.isCancelling ? 'not-allowed' : 'pointer',
                      fontSize: '0.75rem',
                      fontWeight: 600,
                    }}
                  >
                    {t.isCancelling ? 'Cancelling...' : 'Cancel'}
                  </button>
                )}
              </div>
              <div style={{ width: '100%', height: 8, background: '#d1fae5', borderRadius: 4, overflow: 'hidden' }}>
                <div
                  style={{
                    width: `${t.progress}%`,
                    height: '100%',
                    background: '#10b981',
                    transition: 'width 0.15s ease',
                  }}
                />
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Chat Area */}
      <div style={{ marginTop: 20, display: 'flex', flexDirection: 'column', height: 380, border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden' }}>
        <div style={{ flex: 1, overflowY: 'auto', padding: 16, background: '#ffffff', display: 'flex', flexDirection: 'column', gap: 10 }}>
          {messages.length === 0 ? (
            <div style={{ color: '#94a3b8', textAlign: 'center', margin: 'auto' }}>
              {currentRoom ? 'No activity in this room yet.' : 'Join a room to begin.'}
            </div>
          ) : (
            messages.map((m, idx) => {
              const isMe = m.senderId === 'Me';
              return (
                <div
                  key={idx}
                  style={{
                    alignSelf: isMe ? 'flex-end' : 'flex-start',
                    maxWidth: '80%',
                    display: 'flex',
                    flexDirection: 'column',
                    alignItems: isMe ? 'flex-end' : 'flex-start',
                  }}
                >
                  <span style={{ fontSize: '0.75rem', color: '#94a3b8', marginBottom: 2 }}>
                    {isMe ? 'You' : m.senderId.slice(0, 6)} • {m.timestamp}
                  </span>
                  <div
                    style={{
                      padding: '8px 14px',
                      borderRadius: 12,
                      background: isMe ? '#0070f3' : '#f1f5f9',
                      color: isMe ? '#ffffff' : '#0f172a',
                      wordBreak: 'break-word',
                      fontSize: '0.95rem',
                    }}
                  >
                    {m.text}
                  </div>
                </div>
              );
            })
          )}
          <div ref={chatBottomRef} />
        </div>

        {/* Action input bar */}
        <div style={{ display: 'flex', gap: 8, padding: 12, borderTop: '1px solid #e2e8f0', background: '#f8fafc', alignItems: 'center' }}>
          <input
            type="file"
            multiple 
            ref={fileInputRef}
            onChange={handleFileChange}
            disabled={connectedPeers.length === 0}
            style={{ display: 'none' }}
          />

          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            disabled={connectedPeers.length === 0}
            title="Attach File"
            style={{
              padding: '8px 12px',
              background: '#e2e8f0',
              border: 'none',
              borderRadius: 6,
              cursor: connectedPeers.length > 0 ? 'pointer' : 'not-allowed',
              fontSize: '1.1rem',
            }}
          >
            📎
          </button>

          <input
            type="text"
            placeholder={connectedPeers.length > 0 ? 'Type a direct P2P message...' : 'Need peers in room to chat'}
            disabled={connectedPeers.length === 0}
            value={inputDraft}
            onChange={(e) => setInputDraft(e.target.value)}
            onKeyDown={handleKeyDown}
            style={{
              flex: 1,
              padding: '10px 14px',
              border: '1px solid #cbd5e1',
              borderRadius: 6,
              outline: 'none',
              fontSize: '0.95rem',
            }}
          />

          <button
            onClick={handleSendMessage}
            disabled={connectedPeers.length === 0 || !inputDraft.trim()}
            style={{
              padding: '10px 18px',
              background: connectedPeers.length > 0 && inputDraft.trim() ? '#0070f3' : '#94a3b8',
              color: 'white',
              border: 'none',
              borderRadius: 6,
              cursor: connectedPeers.length > 0 && inputDraft.trim() ? 'pointer' : 'not-allowed',
              fontWeight: 600,
            }}
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}