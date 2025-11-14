# Sync Server Research

## Overview

The sync server is implemented in the `@livestore/sync-cf` package and is designed to run on Cloudflare Workers with Durable Objects. It provides real-time event synchronization for LiveStore using multiple transport protocols.

## Key Components

### 1. Worker Entry Point (`worker.ts`)
- Entry point for Cloudflare Worker
- Routes sync requests to appropriate Durable Object
- Handles payload validation
- Supports CORS configuration
- Main functions: `makeWorker()`, `handleSyncRequest()`

### 2. Durable Object (`durable-object.ts`)
- Core server implementation using Cloudflare Durable Objects
- Scoped to a specific `storeId` (one DO per store)
- Supports 3 transport modes:
  - HTTP JSON-RPC
  - WebSocket (with hibernation support)
  - Durable Object RPC (for DO-to-DO communication)
- Main function: `makeDurableObject(options)`
- Lifecycle hooks: `onPush`, `onPushRes`, `onPull`, `onPullRes`

### 3. Storage Layer (`sync-storage.ts`, `sqlite.ts`)
- Two storage engines:
  - **DO SQLite** (default): Data co-located with DO, simpler deployment
  - **D1**: Centralized database, externally queryable
- Database schema versioning via `PERSISTENCE_FORMAT_VERSION`
- Tables:
  - `eventlog_${version}_${storeId}`: Main event storage
  - `context_${version}`: Metadata (currentHead, backendId)
- Pagination support with automatic chunk sizing for D1

### 4. Push Operation (`push.ts`)
- Validates incoming events against current head
- Ensures sequential consistency (prevents race conditions)
- Uses `blockConcurrencyWhile` for atomic writes
- Broadcasts to connected clients (WebSocket + RPC)
- Split large batches to respect WS message size limits
- Returns acknowledgment immediately while broadcasting in background

### 5. Pull Operation (`pull.ts`)
- Streams events from storage
- Cursor-based pagination
- Supports resuming from specific sequence number
- Validates backendId to prevent cross-backend issues
- Automatic chunking based on message size limits
- Can keep streams alive for live updates

### 6. Transport Layer
- **WebSocket RPC** (`ws-rpc-server.ts`): Live bidirectional communication
- **HTTP RPC** (`http-rpc-server.ts`): Request/response pattern
- **DO RPC** (`do-rpc-server.ts`): Server-to-server communication
- All use Effect RPC protocol for message handling

### 7. Context/State Management (`layer.ts`)
- `DoCtx` service manages DO state
- Caches storage, backendId, currentHead
- Initializes database tables
- Maintains RPC subscription registry

### 8. Message Types (`sync-message-types.ts`)
- Type-safe message schemas using Effect Schema
- Client-to-Backend: PullRequest, PushRequest, Ping, Admin operations
- Backend-to-Client: PullResponse, PushAck, Pong, Admin responses
- Metadata includes creation timestamps

## Key Concepts

### Event Sequence
- Each event has `seqNum` and `parentSeqNum`
- Forms a linked list of events
- `currentHead` tracks the latest sequence number
- Push must start from current head (prevents conflicts)

### Backend ID
- Unique identifier for each backend instance
- Prevents accidental cross-backend operations
- Generated once per DO and persisted

### Hibernation
- WebSocket connections can hibernate to save resources
- Attachment stores storeId, payload, and active pullRequestIds
- Allows DO to wake up and handle messages efficiently

### Broadcasting
- When events are pushed, they're broadcast to:
  - All connected WebSocket clients
  - All RPC subscribers
- Ensures real-time synchronization
- Uses Effect RPC chunking for large event batches

### Concurrency Control
- Push operations use `blockConcurrencyWhile`
- Prevents race conditions during writes
- Sequential consistency guarantee

## File Structure

```
packages/@livestore/sync-cf/
├── src/
│   ├── cf-worker/          # Server implementation
│   │   ├── do/             # Durable Object logic
│   │   │   ├── durable-object.ts    # Main DO class
│   │   │   ├── layer.ts             # Context management
│   │   │   ├── push.ts              # Push handler
│   │   │   ├── pull.ts              # Pull handler
│   │   │   ├── sync-storage.ts      # Storage abstraction
│   │   │   ├── sqlite.ts            # Schema definitions
│   │   │   └── transport/           # Transport implementations
│   │   ├── worker.ts       # Worker entry point
│   │   ├── shared.ts       # Shared types/utilities
│   │   └── mod.ts          # Package exports
│   ├── client/             # Client SDK
│   └── common/             # Shared types
│       ├── sync-message-types.ts    # Message schemas
│       ├── ws-rpc-schema.ts         # WebSocket RPC
│       ├── http-rpc-schema.ts       # HTTP RPC
│       └── do-rpc-schema.ts         # DO RPC
```

## Storage Format

### Eventlog Table
- `seqNum` (INTEGER, PRIMARY KEY): Global sequence number
- `parentSeqNum` (INTEGER): Parent event sequence
- `name` (TEXT): Event name
- `args` (TEXT, nullable): JSON-encoded event arguments
- `createdAt` (TEXT): ISO timestamp
- `clientId` (TEXT): Originating client
- `sessionId` (TEXT): Client session

### Context Table
- `storeId` (TEXT, PRIMARY KEY): Store identifier
- `currentHead` (INTEGER): Latest sequence number
- `backendId` (TEXT): Backend instance ID

## Transport Comparison

| Feature | HTTP RPC | WebSocket | DO RPC |
|---------|----------|-----------|---------|
| Real-time | No | Yes | Yes |
| Connection | Request/Response | Persistent | Server-to-server |
| Hibernation | N/A | Yes | N/A |
| Use case | Simple pulls | Live sync | DO-to-DO communication |
| Overhead | Higher | Lower | Lowest |
