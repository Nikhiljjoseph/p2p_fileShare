# P2P Mesh File Transfer & Real-Time Chat

A robust, production-grade, browser-based peer-to-peer (P2P) file transfer and real-time chat application built with **React**, **TypeScript**, **SignalR**, and **WebRTC Data Channels**. 

Designed to support multi-peer room meshing, concurrent multi-file streaming with isolated channels, and real-time network backpressure handling.

---

## Key Features

- **P2P Room Mesh Networking:** Join named rooms via a SignalR signaling backend to discover and connect directly with peers in a mesh topology.
- **Persistent Chat Channel:** A dedicated, low-latency data channel (`chat-channel`) that stays open for the lifecycle of the session to handle direct text messaging.
- **Dynamic Multi-File Streaming (`file-{fileId}`):** Spun up on-demand per file transfer to ensure total packet isolation and prevent data corruption or interleaving.
- **Network Backpressure Control:** Implements `bufferedAmountLowThreshold` and `bufferedamountlow` event listeners on the sender side to monitor socket congestion and prevent browser memory overflow when streaming large files.
- **Multi-File Concurrent Progress:** A reactive UI state dictionary tracking multiple concurrent uploads and downloads simultaneously with individual progress bars.
- **Graceful Abort & Cancellation:** Built with `AbortController` support to instantly halt transfers, tear down specific data channels, and broadcast clean cancellation packets to peers.
- **Session Auto-Reconnect:** Seamlessly persists and restores room state via `localStorage` on page refresh.

---

## Architecture & Technical Highlights

--                   
       ┌─────────────────────────────────────────────────────────────┐
       │                    SIGNALING SERVER                         │
       │  (Coordinates room lookups, SDP offers, and ICE candidates) │
       └──────────┬───────────────────────────────┬──────────────────┘
                  │                               │
           (Room "Alpha")                  (Room "Beta")
                  │                               │
          ┌───────┴───────┐               ┌───────┴───────┐
          ▼               ▼               ▼               ▼
      ┌───────┐       ┌───────┐       ┌───────┐       ┌───────┐
      │Peer A1│◄─────►│Peer A2│       │Peer B1│◄─────►│Peer B2│
      └───────┘       └───────┘       └───────┘       └───────┘
       │                               │
       └── P2P Encrypted Mesh          └── P2P Encrypted Mesh
        ├──  chat-channel               ├──  chat-channel
        ├──  file-{uuid-1}              └──  file-{uuid-1}
        └──  file-{uuid-2}              └──  file-{uuid-2}

- **Signaling & Handshake:** WebRTC SDP Offers/Answers and ICE candidates are exchanged via a SignalR server hub to establish direct encryption between browsers.
- **Channel Separation:** Unlike naive implementations that multiplex everything over a single pipe, this architecture treats every file transfer as an independent micro-stream with its own `RTCDataChannel`.
- **Memory & Buffer Management:** Chunks are sliced as `ArrayBuffer` items, streamed sequentially, and throttled using native browser socket buffer metrics to keep memory footprint minimal.

---

## Getting Started

### Prerequisites
- Node.js (v18+ recommended)
- Your corresponding SignalR backend signaling server running locally or deployed.

### Installation

- Clone the repository.
- Install dependencies (npm install)
- Configure your signaling server endpoint in your environment variables or config service.
- Run the development server (npm run dev)

### Usage

- Open the app in two or more browser tabs.
- Type a room name and click Join Room.
- Once peers are detected in the connected list, use the attachment button (📎) to select one or multiple files to stream concurrently.
- Monitor live progress, cancel mid-transfer if needed, or chat in real time.

   
