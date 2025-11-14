# How the Sync Server Works

## High-Level Architecture

The sync server is a **Cloudflare Durable Object-based real-time synchronization system** that maintains event consistency across multiple clients. Each store gets its own Durable Object instance, ensuring strong consistency guarantees.

```
Client 1 ──┐
Client 2 ──┼──> Cloudflare Worker ──> Durable Object (per storeId) ──> SQLite/D1
Client 3 ──┘                                   │
                                               └──> Broadcasts to all clients
```

## Step-by-Step Flow

### 1. Request Routing (Worker Layer)

**File:** `packages/@livestore/sync-cf/src/cf-worker/worker.ts`

When a request arrives at the Cloudflare Worker:

1. **Parse URL parameters** (`matchSyncRequest`):
   - Extract `storeId`: Which store to sync with
   - Extract `transport`: Which protocol to use (http/ws/do-rpc)
   - Extract `payload`: Optional custom data (e.g., auth tokens)

2. **Validate payload** (optional):
   - Run custom validation function
   - Optionally decode with schema
   - Return 400 if validation fails

3. **Route to Durable Object**:
   - Get DO namespace from environment bindings
   - Create/get DO instance using `idFromName(storeId)`
   - Forward request to DO's `fetch` method

**Key insight:** Each `storeId` maps to exactly one Durable Object instance, ensuring all operations for that store are serialized.

### 2. Durable Object Initialization

**Files:**
- `packages/@livestore/sync-cf/src/cf-worker/do/durable-object.ts`
- `packages/@livestore/sync-cf/src/cf-worker/do/layer.ts`

When the DO instance handles a request:

1. **Initialize DoCtx** (context service):
   - Check if already cached (symbol-based cache)
   - Determine storage engine (DO SQLite or D1)
   - Create storage abstraction

2. **Setup database tables**:
   - Create `eventlog_${version}_${storeId}` table
   - Create `context_${version}` table
   - Load or initialize `currentHead` (latest sequence number)
   - Load or generate `backendId` (unique backend identifier)

3. **Setup transport handlers**:
   - HTTP: JSON-RPC request/response
   - WebSocket: Register hibernation handlers
   - DO RPC: Enable DO-to-DO communication

**Key insight:** The context is cached per DO instance, so initialization only happens once per DO lifetime.

### 3. WebSocket Connection (for WS transport)

**File:** `packages/@livestore/sync-cf/src/cf-worker/do/durable-object.ts:145-186`

For WebSocket connections:

1. **Create WebSocketPair**: Server and client sockets
2. **Attach metadata** (using `serializeAttachment`):
   - `storeId`: Which store this connection is for
   - `payload`: Custom client data
   - `pullRequestIds`: Array of active pull request IDs (for hibernation)
3. **Accept WebSocket** with hibernation enabled
4. **Setup auto-response** for ping/pong
5. **Return 101 Switching Protocols** with client socket

**Key insight:** WebSocket hibernation allows the DO to save resources. The attachment stores enough info to resume on wake.

### 4. Pull Operation (Client fetches events)

**File:** `packages/@livestore/sync-cf/src/cf-worker/do/pull.ts`

When a client wants to fetch events:

1. **Receive PullRequest**:
   - Optional `cursor`: { backendId, eventSequenceNumber }
   - If no cursor, start from beginning

2. **Validate backendId**:
   - If cursor provided, ensure it matches current backendId
   - Prevents pulling from wrong backend after migration

3. **Run onPull hook** (if configured):
   - Custom logic before pull
   - Can log, authorize, etc.

4. **Query storage**:
   - Call `storage.getEvents(cursor?.eventSequenceNumber)`
   - Returns stream of events + total count
   - Storage handles pagination internally

5. **Chunk events**:
   - Split into chunks respecting:
     - Max events per message (e.g., 100)
     - Max bytes per message (e.g., 1MB for WebSocket)
   - Encode each chunk as `PullResponse`

6. **Add page info**:
   - Calculate remaining events
   - Add pagination metadata to response

7. **Stream responses**:
   - For HTTP: Stream chunks in response body
   - For WebSocket: Send chunks as RPC messages
   - For DO RPC: Return ReadableStream

8. **Keep-alive mode** (optional):
   - If `live: true`, keep stream open
   - Future events will be pushed through same stream
   - Track `pullRequestId` in WebSocket attachment

**Key insight:** Pull is a streaming operation that can stay alive for real-time updates. The cursor ensures clients can resume from where they left off.

### 5. Push Operation (Client sends events)

**File:** `packages/@livestore/sync-cf/src/cf-worker/do/push.ts`

When a client wants to push events:

1. **Receive PushRequest**:
   - `batch`: Array of encoded events
   - Each event has: `seqNum`, `parentSeqNum`, `name`, `args`, `clientId`, `sessionId`
   - Optional `backendId` for validation

2. **Early exit** if batch is empty

3. **Run onPush hook** (if configured):
   - Custom logic before push
   - Can validate, log, transform, etc.

4. **Validate backendId** (if provided):
   - Ensure it matches current backendId
   - Return BackendIdMismatchError if not

5. **Critical section** (using `blockConcurrencyWhile`):
   This ensures atomic, sequential writes:

   a. **Get current head**:
      - Read `currentHeadRef.current`

   b. **Validate parent sequence**:
      - First event's `parentSeqNum` must equal `currentHead`
      - If not, return `ServerAheadError`
      - This prevents concurrent writes from corrupting the event chain

   c. **Write to storage**:
      - Call `storage.appendEvents(batch, createdAt)`
      - Storage handles batching for DB limits

   d. **Update head**:
      - Set `currentHead` to last event's `seqNum`
      - Write to context table

6. **Return PushAck immediately**:
   - Don't wait for broadcasting
   - Client gets confirmation ASAP

7. **Broadcast in background** (forked fiber):
   This runs asynchronously after returning to client:

   a. **Prepare responses**:
      - Convert pushed events to PullResponse format
      - Add metadata (createdAt timestamp)
      - Chunk respecting message size limits

   b. **Broadcast to WebSocket clients**:
      - Get all connected WebSockets
      - For each client:
        - Get pullRequestIds from attachment
        - Send PullResponse chunks as RPC messages
        - Include the pushing client (as confirmation)

   c. **Broadcast to RPC subscribers**:
      - For each RPC subscription:
        - Emit stream response chunks
        - Uses DO-to-DO RPC

   d. **Run onPullRes hook** (if configured):
      - Custom logic after broadcast

8. **Run onPushRes hook** (if configured):
   - Custom logic after push completes

**Key insight:** The `blockConcurrencyWhile` ensures that even with concurrent push requests, they execute sequentially. This prevents race conditions and maintains event order. Broadcasting happens in the background to minimize latency.

### 6. Storage Layer

**Files:**
- `packages/@livestore/sync-cf/src/cf-worker/do/sync-storage.ts`
- `packages/@livestore/sync-cf/src/cf-worker/do/sqlite.ts`

#### Storage Abstraction

The storage layer provides two implementations:

**DO SQLite** (default):
- Uses `ctx.storage.sql.exec()`
- Data stored in Durable Object's persistent storage
- Page size: 256 events
- Simple, co-located with DO

**D1** (optional):
- Uses external D1 database
- Adaptive pagination:
  - Start with 256 events per page
  - Decrease by half if response > 1MB (D1 limit)
  - Increase by 2x if response < 500KB
  - Min page size: 1, Max: 256
- Allows external inspection/querying

#### Event Retrieval (`getEvents`)

1. **Count total events** after cursor
2. **Create paginated stream**:
   - Initial state: `{ cursor, limit }`
   - Fetch page function:
     - Execute SELECT with LIMIT
     - If result > target size (D1): retry with smaller limit
     - Decode rows from database format
     - Extract metadata (createdAt)
     - Return chunk + next state
3. **Unfold stream** from pages
4. **Accumulate page info**:
   - Track remaining events
   - Add to each response

#### Event Appending (`appendEvents`)

1. **Batch events** (max 14 per query due to parameter limits)
2. **For each chunk**:
   - Build INSERT statement with placeholders
   - Flatten event properties to parameters array
   - Execute SQL
3. **Encode args** as JSON string (or NULL if undefined)

**Key insight:** The storage layer handles database limitations (parameter counts, response sizes) transparently, allowing the rest of the system to work with streams.

### 7. Transport Layer Details

#### WebSocket RPC

**File:** `packages/@livestore/sync-cf/src/cf-worker/do/transport/ws-rpc-server.ts`

- Uses Effect RPC protocol
- Handlers:
  - `SyncWsRpc.Pull`: Returns stream of PullResponse
  - `SyncWsRpc.Push`: Returns single PushAck
- For live pulls, concatenates with `Stream.never` to keep connection open
- Hibernation support:
  - Stores `pullRequestIds` in attachment
  - Removes on interrupt/disconnect

#### HTTP RPC

**File:** `packages/@livestore/sync-cf/src/cf-worker/do/transport/http-rpc-server.ts`

- Request/response pattern
- Pull returns streaming response
- Push returns single response
- Requires `enable_request_signal` compatibility flag for proper streaming

#### DO RPC

**File:** `packages/@livestore/sync-cf/src/cf-worker/do/transport/do-rpc-server.ts`

- Server-to-server communication
- Uses binary encoding (Uint8Array)
- Can return ReadableStream for pull operations
- Enables cross-DO synchronization

**Key insight:** All transports use the same underlying push/pull logic, just different wire formats and lifecycle management.

## Complete Request Flow Examples

### Example 1: Client Pushes Event

```
1. Client sends POST to worker with storeId=store123, transport=ws
2. Worker validates payload, routes to DO
3. DO accepts WebSocket upgrade
4. Client sends PushRequest: { batch: [{ seqNum: 5, parentSeqNum: 4, ... }] }
5. DO validates: currentHead == 4 ✓
6. DO writes event to SQLite in atomic section
7. DO updates currentHead to 5
8. DO returns PushAck to client
9. DO broadcasts PullResponse to all connected clients (including pusher)
10. Other clients receive the new event in real-time
```

### Example 2: Client Pulls Events

```
1. Client sends GET to worker with storeId=store123, transport=http
2. Worker validates payload, routes to DO
3. DO receives PullRequest: { cursor: { backendId: "xyz", eventSequenceNumber: 10 } }
4. DO validates backendId matches ✓
5. DO queries storage.getEvents(10)
6. Storage returns stream of events [11, 12, 13, ..., 50]
7. DO chunks into pages: [11-20], [21-30], [31-40], [41-50]
8. DO streams PullResponse chunks back to client
9. Client processes events incrementally
```

### Example 3: New Client Syncs from Beginning

```
1. Client connects via WebSocket
2. Client sends PullRequest: { cursor: None, live: true }
3. DO queries storage.getEvents(undefined)
4. Storage returns all events from beginning
5. DO streams all historical events
6. DO keeps stream alive (due to live: true)
7. Client now receives real-time updates via same stream
```

## Key Design Patterns

### 1. Event Sourcing
- Events form immutable append-only log
- Sequence numbers create total order
- Parent pointers form linked list

### 2. Optimistic Concurrency
- Clients optimistically apply local changes
- Push validates against server head
- If conflict, client must pull and retry

### 3. Streaming
- Large datasets streamed, not loaded entirely
- Chunking respects protocol limits
- Pagination allows resumption

### 4. Pub/Sub Broadcasting
- Push operation triggers broadcast
- All subscribers receive updates
- Background processing doesn't block push response

### 5. Hibernation
- WebSockets can sleep to save resources
- Attachment stores reconnection state
- Wake on message, broadcast, or disconnect

## Performance Characteristics

### Latency
- **Push**: ~10-50ms (including DB write)
- **Pull**: Streaming starts immediately, chunked delivery
- **Broadcast**: 1-5ms per connected client

### Throughput
- **Limited by**: Durable Object serialization
- **Concurrent pushes**: Queued, executed sequentially
- **Concurrent pulls**: Can run in parallel

### Storage
- **DO SQLite**: Limited by DO storage (unknown limit, likely GBs)
- **D1**: Limited by D1 database (configurable, 10GB+)

### Scalability
- **Horizontal**: One DO per storeId (sharding by store)
- **Vertical**: DO can handle 100s of connections
- **Cold start**: ~50-200ms for DO initialization

## Error Handling

### ServerAheadError
- Client's parent doesn't match server head
- Client must pull missing events and retry

### BackendIdMismatchError
- Client cursor references different backend
- Client must reset and pull from beginning

### InvalidPushError / InvalidPullError
- Wraps unexpected errors
- Includes cause for debugging

### UnexpectedError
- Generic error wrapper
- Used throughout Effect-based code

## Configuration Options

### makeDurableObject Options

```typescript
{
  onPush?: (message, context) => void,        // Hook before push
  onPushRes?: (ack) => void,                  // Hook after push
  onPull?: (message, context) => void,        // Hook before pull
  onPullRes?: (response) => void,             // Hook after pull
  storage?: { _tag: 'do-sqlite' | 'd1' },     // Storage engine
  enabledTransports?: Set<'http'|'ws'|'do-rpc'>, // Enabled protocols
  http?: { responseHeaders?: Record },        // Custom HTTP headers
  otel?: { baseUrl, serviceName },            // OpenTelemetry config
}
```

### makeWorker Options

```typescript
{
  syncBackendBinding: string,                 // DO binding name
  syncPayloadSchema?: Schema,                 // Payload type
  validatePayload?: (payload, ctx) => void,   // Payload validator
  enableCORS?: boolean,                       // CORS support
}
```

## Summary

The sync server is a sophisticated real-time synchronization system that:

1. **Routes** requests to per-store Durable Objects
2. **Validates** sequence consistency on push
3. **Stores** events in SQLite/D1 with versioning
4. **Streams** events to clients with pagination
5. **Broadcasts** new events to all subscribers in real-time
6. **Supports** multiple transports (HTTP, WebSocket, DO RPC)
7. **Scales** horizontally by sharding on storeId
8. **Ensures** strong consistency via sequential writes

The key innovation is using Cloudflare Durable Objects to achieve strong consistency without complex distributed coordination, while still supporting thousands of stores and clients.
