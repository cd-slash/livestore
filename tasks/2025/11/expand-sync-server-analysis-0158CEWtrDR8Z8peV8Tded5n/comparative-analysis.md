# Comparative Analysis: Sync Provider Implementations

## Overview

This document analyzes the three existing sync provider implementations (Cloudflare, ElectricSQL, S2) to identify:
- **Common patterns** that can be reused
- **Differences** that require custom implementation
- **Guidance** for building a custom sync provider

## The SyncBackend Interface

All sync providers must implement the client-side `SyncBackend` interface:

```typescript
type SyncBackend<TSyncMetadata> = {
  // Connection management
  connect: Effect<void, IsOfflineError | UnexpectedError>
  ping: Effect<void, IsOfflineError | UnexpectedError | TimeoutException>
  isConnected: SubscriptionRef<boolean>

  // Core operations
  pull: (cursor, options?) => Stream<PullResItem<TSyncMetadata>, IsOfflineError | InvalidPullError>
  push: (batch) => Effect<void, IsOfflineError | InvalidPushError>

  // Metadata
  metadata: { name: string; description: string; ... }
  supports: { pullPageInfoKnown: boolean; pullLive: boolean }
}

type PullResItem<TSyncMetadata> = {
  batch: Array<{ eventEncoded: LiveStoreEvent; metadata: Option<TSyncMetadata> }>
  pageInfo: 'NoMore' | 'MoreUnknown' | { _tag: 'MoreKnown'; remaining: number }
}
```

**Key constraints**:
- Push batch: 1-100 events, ascending sequence numbers
- Pull: Cursor-based pagination with optional live mode
- Metadata: Provider-specific cursor information

## Architectural Patterns

### Pattern 1: Full Server (Cloudflare)

**Architecture**: Client → Server (all logic server-side)

```
Client (SyncBackend) → Cloudflare Worker → Durable Object
                                              ├─ Storage (SQLite/D1)
                                              ├─ Push validation
                                              ├─ Pull queries
                                              └─ Broadcasting
```

**Characteristics**:
- Server implements full sync logic
- Client is thin adapter
- Server owns storage
- Server coordinates all operations

**When to use**:
- Need strong consistency guarantees
- Want centralized control
- Building serverless architecture
- Need built-in broadcasting

### Pattern 2: Client Adapter with External Storage (ElectricSQL, S2)

**Architecture**: Client → API Proxy → External Service

```
Client (SyncBackend) → API Proxy → External Service
                        ├─ Auth         ├─ PostgreSQL (Electric)
                        ├─ Validation   └─ S2 Streams
                        └─ Forwarding
```

**Characteristics**:
- External service owns storage
- Proxy provides auth/validation layer
- Client implements pull/push logic
- Leverages existing infrastructure

**When to use**:
- Already using the external service
- Need external queryability
- Want managed infrastructure
- Can accept eventual consistency

## Component-by-Component Comparison

### 1. Client Implementation

#### Commonalities (100% Reusable)

All three implementations share identical patterns:

**Connection Management**:
```typescript
// All providers implement this identically
const isConnected = yield* SubscriptionRef.make(false)

const ping = Effect.gen(function* () {
  yield* httpClient.head(pingEndpoint)
  yield* SubscriptionRef.set(isConnected, true)
}).pipe(
  Effect.timeout(10_000),
  Effect.catchTag('TimeoutException', () => SubscriptionRef.set(isConnected, false))
)

// Auto-ping every 10 seconds
yield* ping.pipe(Effect.repeat(Schedule.spaced(10_000)), Effect.forkScoped)
```

**Connect Strategy**:
```typescript
// All providers use same-origin optimization
const pullEndpointHasSameOrigin =
  pullEndpoint.startsWith('/') ||
  (globalThis.location?.origin === new URL(pullEndpoint).origin)

const connect = pullEndpointHasSameOrigin
  ? Effect.void
  : ping
```

**Error Wrapping**:
```typescript
// All providers wrap errors consistently
Effect.mapError((cause) =>
  cause._tag === 'InvalidPullError' ? cause : new InvalidPullError({ cause })
)

Effect.mapError((cause) =>
  new InvalidPushError({ cause: new UnexpectedError({ cause }) })
)
```

**Metadata Structure**:
```typescript
metadata: {
  name: '@livestore/sync-{provider}',
  description: 'LiveStore sync backend implementation for {provider}',
  protocol: 'http',
  endpoint: endpointConfig
}
```

**Conclusion**: The connection management, error handling, and metadata patterns are **completely reusable** across all implementations.

#### Differences (Provider-Specific)

**Pull Implementation**:

| Provider | Protocol | Streaming | Pagination | Cursor |
|----------|----------|-----------|------------|--------|
| Cloudflare | HTTP/WS/RPC | WebSocket | Effect chunks | `{ backendId, seqNum }` |
| ElectricSQL | HTTP | Long-polling | Unfold | `{ handle, offset }` |
| S2 | SSE | True streaming | SSE events | `{ s2SeqNum }` |

**Push Implementation**:

| Provider | Chunking | Validation | Error Handling |
|----------|----------|------------|----------------|
| Cloudflare | Server-side | Server validates head | RPC-specific errors |
| ElectricSQL | None | Client-side only | Generic HTTP errors |
| S2 | Client pre-chunks | Client validates limits | S2-specific limit errors |

### 2. Message Schemas

#### Commonalities (Reusable Patterns)

All providers use Effect Schema for type-safe messages:

**Pull Request Pattern**:
```typescript
// Common structure across all providers
{
  storeId: string,           // Which store to sync
  cursor: ProviderCursor,    // Where to resume from (provider-specific)
  live: boolean,             // Whether to keep connection open
  payload?: JsonValue        // Custom client data (auth, etc.)
}
```

**Push Request Pattern**:
```typescript
// Identical across all providers
{
  storeId: string,
  batch: Array<LiveStoreEvent.AnyEncodedGlobal>
}
```

**Response Pattern**:
```typescript
// All providers return this structure (mapped to PullResItem)
{
  events: Array<LiveStoreEvent>,
  metadata: ProviderCursorInfo,  // For next pull
  remaining?: number              // Optional progress info
}
```

**Conclusion**: The **message structure patterns** are reusable. The cursor format is provider-specific.

#### Differences (Provider-Specific)

**Cursor Formats**:

```typescript
// Cloudflare
type CloudflareCursor = {
  backendId: string,           // Prevent cross-backend pulls
  eventSequenceNumber: number  // LiveStore seqNum
}

// ElectricSQL
type ElectricCursor = {
  handle: string,              // Shape identifier
  offset: string               // LSN-based position (e.g., "27294160_0")
}

// S2
type S2Cursor = {
  s2SeqNum: number             // S2 stream position (separate from LiveStore seqNum!)
}
```

**Key insight**: Cursor is the primary provider-specific component. It must track the provider's native pagination mechanism.

### 3. Transport Layer

#### Full Comparison

| Aspect | Cloudflare | ElectricSQL | S2 |
|--------|------------|-------------|-----|
| **Pull Protocol** | HTTP/WebSocket/DO RPC | HTTP (long-poll) | HTTP (SSE) |
| **Push Protocol** | HTTP/WebSocket/DO RPC | HTTP POST | HTTP POST |
| **Live Updates** | WebSocket subscription | Long-polling (20s) | SSE streaming |
| **Message Format** | Effect RPC (binary) | JSON | SSE + JSON |
| **Connection Type** | Persistent (WS) / Request (HTTP) | Request only | Persistent (SSE) |
| **Bidirectional** | Yes (WebSocket, DO RPC) | No | No (server→client only) |
| **Chunking** | Server-side automatic | None | Client-side pre-chunk |

#### Reusable Patterns

**HTTP Request Building**:
```typescript
// All providers use similar HTTP patterns
const buildGetRequest = (endpoint: string, args: PullArgs) => {
  const argsJson = Schema.encode(ArgsSchema)(args)
  return HttpClientRequest.get(`${endpoint}?args=${argsJson}`)
}

const buildPostRequest = (endpoint: string, payload: PushPayload) =>
  HttpClientRequest.post(endpoint).pipe(
    HttpClientRequest.schemaBodyJson(PushPayload)(payload)
  )
```

**Response Handling**:
```typescript
// All providers handle status codes similarly
if (resp.status === 401) return InvalidPullError.make({ cause: new Error('Unauthorized') })
if (resp.status === 400) return Option.some([[], Option.none()]) // Empty result
if (resp.status < 200 || resp.status >= 300) return InvalidPullError.make({ ... })
```

**Conclusion**: HTTP request/response patterns are reusable. The actual transport protocol (HTTP, WebSocket, SSE) depends on requirements.

### 4. Storage Layer

#### Full Comparison

| Provider | Storage | Schema | Provisioning | Querying |
|----------|---------|--------|--------------|----------|
| Cloudflare | DO SQLite or D1 | Versioned tables | Automatic | SQL (limited) |
| ElectricSQL | PostgreSQL | PostgreSQL tables | Manual | Full SQL |
| S2 | S2 Streams | Append-only log | API-based | Sequential only |

#### Server-Side Storage Requirements

**All implementations must**:

1. **Store events persistently**:
   - Events must survive server restarts
   - Events must be queryable by sequence number
   - Events must maintain order

2. **Track current head**:
   - Latest sequence number in the store
   - Used for push validation
   - Updated atomically with event writes

3. **Support cursor-based reads**:
   - Query events starting from position
   - Return events in order
   - Support pagination

**Cloudflare Example** (DO SQLite):
```sql
-- Eventlog table
CREATE TABLE eventlog_6_{storeId} (
  seqNum INTEGER PRIMARY KEY,
  parentSeqNum INTEGER,
  name TEXT NOT NULL,
  args TEXT,  -- JSON-encoded
  clientId TEXT NOT NULL,
  sessionId TEXT NOT NULL,
  createdAt TEXT NOT NULL
);

-- Context table
CREATE TABLE context_6 (
  storeId TEXT PRIMARY KEY,
  currentHead INTEGER,
  backendId TEXT
);
```

**ElectricSQL Example** (PostgreSQL):
```sql
CREATE TABLE eventlog_6_{storeId} (
  "seqNum" INTEGER PRIMARY KEY,
  "parentSeqNum" INTEGER,
  "name" TEXT NOT NULL,
  "args" JSONB NOT NULL,
  "clientId" TEXT NOT NULL,
  "sessionId" TEXT NOT NULL
);
```

**S2 Example** (Stream):
```typescript
// No schema - just append JSON strings
{
  body: JSON.stringify(event),  // The event itself
  seq_num: 0                    // Assigned by S2
}
```

**Conclusion**: Storage schema is **highly provider-specific**. However, the **logical requirements** (persist events, track head, cursor-based reads) are universal.

### 5. Push Operation

#### Universal Requirements (from guidance)

All push implementations must:

1. **Validate sequence order**:
   ```typescript
   // Ensure batch is sequential
   for (let i = 0; i < batch.length - 1; i++) {
     if (batch[i+1].seqNum !== batch[i].seqNum + 1) {
       return InvalidPushError('Events not sequential')
     }
   }
   ```

2. **Validate against current head**:
   ```typescript
   // First event must follow current head
   if (batch[0].parentSeqNum !== currentHead) {
     return ServerAheadError({ serverHead: currentHead, clientHead: batch[0].parentSeqNum })
   }
   ```

3. **Atomically persist and update head**:
   ```typescript
   // Pseudo-code for atomic operation
   atomic(() => {
     const head = getHead()
     validate(batch, head)
     appendEvents(batch)
     setHead(batch.last.seqNum)
   })
   ```

4. **Ensure serial execution** (one push at a time):
   - Prevents race conditions
   - Maintains total ordering
   - Critical for consistency

#### Implementation Comparison

**Cloudflare** (Full Server):
```typescript
// packages/@livestore/sync-cf/src/cf-worker/do/push.ts
ctx.storage.blockConcurrencyWhile(async () => {
  const currentHead = currentHeadRef.current

  // Validate parent matches head
  if (batch[0].parentSeqNum !== currentHead) {
    return ServerAheadError({ ... })
  }

  // Write to storage
  await storage.appendEvents(batch, createdAt)

  // Update head
  currentHeadRef.current = batch[batch.length - 1].seqNum
  await storage.setContext({ currentHead: currentHeadRef.current })
})

// Broadcast in background (don't wait)
Effect.fork(broadcastToSubscribers(batch))
```

**Key features**:
- `blockConcurrencyWhile` ensures serial execution
- Head validation happens in critical section
- Broadcasting is async (doesn't block response)
- Server owns all logic

**ElectricSQL** (Client Adapter):
```typescript
// Proxy writes directly to PostgreSQL
const sql = postgres({ ... })

// PostgreSQL ACID guarantees serialization
await sql`
  INSERT INTO ${sql(tableName)}
    ("seqNum", "parentSeqNum", "name", "args", "clientId", "sessionId")
  VALUES (${event.seqNum}, ${event.parentSeqNum}, ...)
`

// ElectricSQL detects changes via logical replication
// No explicit validation - relies on PRIMARY KEY constraint
```

**Key features**:
- PostgreSQL's ACID provides serialization
- PRIMARY KEY on seqNum prevents duplicates
- No explicit head validation (constraint-based)
- External system (Electric) handles propagation

**S2** (Client Adapter with Pre-chunking):
```typescript
// Client pre-chunks to respect S2 limits
const chunks = chunkEventsForS2(batch)  // Max 1000 records, 1 MiB

for (const chunk of chunks) {
  await fetch(s2Url, {
    method: 'POST',
    body: JSON.stringify({
      records: chunk.map(event => ({ body: JSON.stringify(event) }))
    })
  })
}

// S2 assigns seq_nums and broadcasts via SSE
// No explicit validation - S2 guarantees order within stream
```

**Key features**:
- Client chunks to respect limits
- S2 provides ordering guarantees
- No head validation (append-only log)
- Sequential appends ensure order

#### Reusable Components

**Validation Logic**:
```typescript
// This can be reused across implementations
const validatePushBatch = (batch: LiveStoreEvent[], currentHead: number) => {
  // Check batch not empty
  if (batch.length === 0) return { valid: false, error: 'Empty batch' }

  // Check sequential within batch
  for (let i = 0; i < batch.length - 1; i++) {
    if (batch[i+1].seqNum !== batch[i].seqNum + 1) {
      return { valid: false, error: 'Non-sequential events' }
    }
    if (batch[i+1].parentSeqNum !== batch[i].seqNum) {
      return { valid: false, error: 'Invalid parent sequence' }
    }
  }

  // Check first event follows head
  if (batch[0].parentSeqNum !== currentHead) {
    return {
      valid: false,
      error: 'ServerAheadError',
      serverHead: currentHead,
      clientHead: batch[0].parentSeqNum
    }
  }

  return { valid: true }
}
```

**Retry Logic**:
```typescript
// All providers use similar retry patterns
const pushWithRetry = push.pipe(
  Effect.retry(Schedule.compose(
    Schedule.recurs(2),           // 2 retries
    Schedule.spaced(100)          // 100ms between attempts
  ))
)
```

### 6. Pull Operation

#### Universal Requirements

All pull implementations must:

1. **Validate cursor** (if provided):
   - Check cursor is from same backend (if applicable)
   - Ensure cursor is not in the future

2. **Query events from storage**:
   - Start from cursor position (or beginning)
   - Return events in order
   - Support pagination

3. **Return batch with metadata**:
   - Events with provider-specific metadata
   - Page info (hasMore, remaining count if known)

4. **Support live mode** (optional):
   - Keep connection open
   - Stream new events as they arrive

#### Implementation Comparison

**Cloudflare** (Full Server with WebSocket):
```typescript
// packages/@livestore/sync-cf/src/cf-worker/do/pull.ts
const pull = (request: PullRequest) => {
  // Validate cursor backendId
  if (cursor?.backendId !== currentBackendId) {
    return BackendIdMismatchError({ ... })
  }

  // Query storage
  const stream = storage.getEvents(cursor?.eventSequenceNumber)

  // Chunk for transport limits
  const chunked = stream.pipe(
    Stream.chunks(maxEventsPerMessage),
    Stream.map(addPageInfo)
  )

  // For live pulls, keep stream open
  if (live) {
    return chunked.pipe(Stream.concat(liveUpdatesStream))
  }

  return chunked
}
```

**Key features**:
- Server-side pagination (storage layer handles it)
- Live updates via WebSocket subscription
- Automatic chunking for message size limits
- Backend ID validation prevents cross-backend issues

**ElectricSQL** (Client Adapter with Long-Polling):
```typescript
// packages/@livestore/sync-electric/src/index.ts
const pull = (cursor, { live }) => {
  return Stream.unfoldEffect(cursor, (handle) => {
    // Build Electric URL with handle/offset
    const url = buildElectricUrl({ handle, live })

    // GET request to Electric
    const resp = await httpClient.get(url)

    // Electric long-polls for ~20s if no data
    if (resp.status === 204) {
      // No new data, return empty with same handle
      return Option.some([[], handle])
    }

    // Parse Electric response format
    const items = parseElectricResponse(resp)
    const nextHandle = extractHandleFromHeaders(resp)

    return Option.some([items, nextHandle])
  })
}
```

**Key features**:
- Client-side pagination (unfold pattern)
- Long-polling simulates live mode
- Electric-specific response format
- Handle-based cursor management

**S2** (Client Adapter with SSE):
```typescript
// packages/@livestore/sync-s2/src/sync-provider.ts
const pull = (cursor, { live }) => {
  if (live) {
    return ssePull(cursor)  // Auto-reconnecting live stream
  }

  // Non-live: SSE with wait=0
  const argsJson = Schema.encode(ArgsSchema)({
    storeId,
    s2SeqNum: cursor?.s2SeqNum ?? 'from-start',
    live: false
  })
  const url = `${endpoint}?args=${argsJson}`

  return httpClient.execute(
    HttpClientRequest.get(url).pipe(
      HttpClientRequest.setHeaders({ accept: 'text/event-stream' })
    )
  ).pipe(
    HttpClientResponse.stream,
    Stream.decodeText('utf8'),
    Stream.pipeThroughChannel(Sse.makeChannel()),
    Stream.filterMap(parseSseEvent),
    Stream.mapError(toInvalidPullError)
  )
}

const ssePull = (cursor) => {
  // Loop: initial pull (fast) → live pull (wait for data) → reconnect
  const loop = (cursor, isFirst) => {
    const stream = runPullSse(cursor, live: !isFirst)
    return stream.pipe(
      Stream.concatWithLastElement((lastItem) =>
        loop(computeNextCursor(lastItem, cursor), false)
      )
    )
  }
  return loop(cursor, true)
}
```

**Key features**:
- True streaming via SSE
- Smart reconnection (fast catchup → live mode)
- Auto-resume from last position
- Separate S2 seq_num from LiveStore seqNum

#### Reusable Components

**Cursor Extraction**:
```typescript
// All providers use this pattern
const cursorFromBatch = <T>(batch: PullResItem<T>) => {
  const lastEvent = batch.batch.at(-1)
  if (!lastEvent) return Option.none()

  return Option.some({
    eventSequenceNumber: lastEvent.eventEncoded.seqNum,
    metadata: lastEvent.metadata
  })
}
```

**Page Info Computation**:
```typescript
// Providers with known remaining use this
const computePageInfo = (remaining: number): PullResPageInfo =>
  remaining > 0
    ? { _tag: 'MoreKnown', remaining }
    : { _tag: 'NoMore' }

// Providers without known remaining
const pageInfo = hasMore
  ? { _tag: 'MoreUnknown' }
  : { _tag: 'NoMore' }
```

**Stream Patterns**:
```typescript
// Unfold pattern (ElectricSQL)
Stream.unfoldEffect(initialState, (state) => {
  const result = fetchNextPage(state)
  return result ? Option.some([data, nextState]) : Option.none()
})

// SSE pattern (S2)
httpClient.execute(request).pipe(
  HttpClientResponse.stream,
  Stream.decodeText('utf8'),
  Stream.pipeThroughChannel(Sse.makeChannel()),
  Stream.filterMap(parseEvent)
)

// WebSocket pattern (Cloudflare)
// Handled server-side, client receives via RPC
```

### 7. Broadcasting / Live Updates

#### Comparison

| Provider | Mechanism | Who Broadcasts | Latency |
|----------|-----------|----------------|---------|
| Cloudflare | WebSocket push | Durable Object | ~1-5ms |
| ElectricSQL | Long-poll | ElectricSQL (via replication) | ~100-1000ms |
| S2 | SSE push | S2 Service | ~10-100ms |

**Cloudflare** (Server-Controlled):
```typescript
// Server broadcasts after push
const broadcast = Effect.gen(function* () {
  const pullResponses = preparePullResponses(events)

  // Broadcast to all WebSocket clients
  for (const ws of getWebSockets()) {
    const pullRequestIds = getAttachment(ws).pullRequestIds
    for (const id of pullRequestIds) {
      yield* sendRpcResponse(ws, id, pullResponses)
    }
  }

  // Broadcast to RPC subscribers
  for (const subscription of rpcSubscriptions) {
    yield* subscription.emit(pullResponses)
  }
})

// Forked after push completes (non-blocking)
yield* broadcast.pipe(Effect.fork)
```

**ElectricSQL** (External System):
```typescript
// No explicit broadcasting in LiveStore code
// ElectricSQL detects changes via PostgreSQL logical replication
// Long-polling clients receive updates on next poll (up to 20s delay)

// Client side: long-poll loop
while (live) {
  const resp = await httpClient.get(electricUrl)
  if (resp.status === 204) {
    // No data, Electric times out after ~20s
    continue  // Immediately retry
  }
  emitEvents(parseResponse(resp))
}
```

**S2** (External System):
```typescript
// No explicit broadcasting in LiveStore code
// S2 broadcasts via SSE to all connected clients
// Clients with open SSE connections receive events as they're appended

// Client side: SSE stream
const sseStream = httpClient.execute(request).pipe(
  HttpClientResponse.stream,
  Stream.pipeThroughChannel(Sse.makeChannel()),
  Stream.filterMap((msg) => {
    if (msg.event === 'batch') {
      return Option.some(parseRecords(msg.data))
    }
    return Option.none()  // Ignore pings
  })
)

// Auto-reconnect on disconnect
Stream.concatWithLastElement((lastItem) =>
  reconnect(cursorFromBatch(lastItem))
)
```

#### Key Insights

**Full Server (Cloudflare)**:
- Server controls all broadcasting
- Instant updates (in-process notification)
- Requires persistent connections (WebSocket)
- More complex server implementation

**External System (ElectricSQL, S2)**:
- External system handles broadcasting
- Small propagation delay (eventual consistency)
- Client implements reconnection logic
- Simpler proxy implementation

### 8. Concurrency Control

#### Server-Side Serialization

**Cloudflare** (Explicit):
```typescript
// Durable Object guarantees serialization for the storeId
// Plus explicit blockConcurrencyWhile for push
ctx.storage.blockConcurrencyWhile(async () => {
  validateAndWrite()
})
```

**ElectricSQL** (Database):
```typescript
// PostgreSQL ACID transactions provide serialization
// PRIMARY KEY constraint prevents duplicate seqNums
await sql`
  INSERT INTO eventlog (seqNum, ...)
  VALUES (${seqNum}, ...)
`
// If seqNum already exists, INSERT fails
```

**S2** (Service Guarantee):
```typescript
// S2 service guarantees sequential appends within a stream
// Client can append concurrently, S2 assigns seq_nums in order
await fetch(s2Url, { method: 'POST', body: records })
```

#### Key Insight

All approaches ensure **serial execution of pushes per store**:
- Cloudflare: Per-DO serialization + explicit lock
- ElectricSQL: Database constraints
- S2: Service-level ordering

This is **critical** for maintaining event order and preventing race conditions.

## Summary: Reusable vs Custom Components

### ✅ Fully Reusable Components

These can be copied directly from existing implementations:

1. **Connection Management**:
   - `isConnected` ref and ping mechanism
   - Same-origin optimization
   - Auto-ping with timeout handling

2. **Error Handling**:
   - InvalidPullError / InvalidPushError wrapping
   - Status code handling (401, 400, 409, etc.)
   - Retry schedules

3. **Metadata Structure**:
   - Provider name, description, protocol
   - Supports flags (pullPageInfoKnown, pullLive)

4. **Message Schemas**:
   - PullRequest structure (storeId, cursor, live, payload)
   - PushRequest structure (storeId, batch)
   - Effect Schema patterns

5. **Validation Logic**:
   - Batch sequence validation
   - Head validation patterns
   - Cursor extraction from batches

6. **HTTP Patterns**:
   - Query param encoding
   - POST body encoding
   - Response streaming

7. **Page Info**:
   - NoMore / MoreUnknown / MoreKnown types
   - Remaining count computation

### ⚠️ Partially Reusable (Adaptable Patterns)

These patterns are reusable but need customization:

1. **Pull Implementation**:
   - **Reusable**: Stream composition, unfold/map patterns
   - **Custom**: Protocol (HTTP/WS/SSE), response parsing, cursor format

2. **Push Implementation**:
   - **Reusable**: Batch validation, error handling
   - **Custom**: Chunking strategy, transport, serialization

3. **Cursor Management**:
   - **Reusable**: Option types, cursor extraction logic
   - **Custom**: Cursor format (provider-specific pagination)

4. **Transport Layer**:
   - **Reusable**: HttpClient usage, Effect streams
   - **Custom**: Protocol choice, message format

### ❌ Must Implement Custom

These components are inherently provider-specific:

1. **Storage Backend**:
   - Schema design
   - Query implementation
   - Persistence mechanism
   - Head tracking

2. **Server-Side Logic** (for full server):
   - Event validation
   - Atomic writes
   - Broadcasting
   - Concurrency control

3. **Provider API Integration** (for adapters):
   - API client
   - Response format parsing
   - Authentication
   - Provisioning

4. **Cursor Format**:
   - Must match provider's pagination mechanism
   - Provider-specific metadata structure

5. **Live Updates**:
   - Protocol choice (WS, SSE, polling)
   - Connection management
   - Reconnection logic

## Building a Custom Sync Provider: Decision Tree

### Step 1: Choose Architecture Pattern

**Option A: Full Server** (like Cloudflare)
- ✅ Choose if: Need strong consistency, full control, centralized logic
- ⚠️ Requires: Implement all server-side logic (storage, validation, broadcasting)
- 📋 Complexity: High

**Option B: Client Adapter** (like ElectricSQL/S2)
- ✅ Choose if: Leveraging existing infrastructure, need external queryability
- ⚠️ Requires: API integration, client-side chunking/pagination
- 📋 Complexity: Medium

### Step 2: Select Transport Protocol

**For Pull**:
- **HTTP (request/response)**: Simple, but requires polling for live updates
- **HTTP (SSE)**: True streaming, good for live updates, unidirectional
- **WebSocket**: Bidirectional, most flexible, requires connection management
- **RPC**: Low overhead, type-safe, requires framework

**For Push**:
- **HTTP POST**: Universal, simple
- **WebSocket**: Efficient for frequent pushes
- **RPC**: Type-safe, low overhead

### Step 3: Design Storage

This step involves making specific decisions about how your sync server will persist and query events.

#### 3.1: Choose Data Store

**Options**:
- **SQL Database** (PostgreSQL, MySQL, SQLite): Best for queryability, ACID guarantees
- **NoSQL** (DynamoDB, MongoDB): Good for scalability, flexible schema
- **Object Storage** (S3, R2): Cheapest for large volumes, higher latency
- **Stream Service** (S2, Kafka, Kinesis): Built for event streaming, sequential access only

**Example schemas**:

```sql
-- SQL approach (like Cloudflare DO SQLite)
CREATE TABLE eventlog_v6_mystore (
  seqNum INTEGER PRIMARY KEY,      -- Ensures uniqueness, enables fast lookups
  parentSeqNum INTEGER NOT NULL,
  name TEXT NOT NULL,
  args TEXT,                       -- JSON-encoded
  clientId TEXT NOT NULL,
  sessionId TEXT NOT NULL,
  createdAt TEXT NOT NULL
);

-- Metadata table for head tracking
CREATE TABLE context_v6 (
  storeId TEXT PRIMARY KEY,
  currentHead INTEGER NOT NULL,
  backendId TEXT NOT NULL,
  updatedAt TEXT NOT NULL
);
```

#### 3.2: Design Head Tracking

The "head" is the sequence number of the latest event in the store. It's critical for push validation.

**Decision: Where to store head?**

**Option A: Separate metadata table** (Cloudflare approach)
```typescript
// Pros: Clean separation, easy to query, supports multiple stores in one DB
// Cons: Requires separate update, potential for inconsistency
{
  storeId: "myStore",
  currentHead: 42,
  backendId: "abc123",
  updatedAt: "2025-11-14T10:30:00Z"
}
```

**Option B: In-memory with persistence**
```typescript
// Pros: Fast reads, no DB query needed
// Cons: Lost on restart (need to recompute), harder to scale
let currentHead = 42  // Recomputed on startup: SELECT MAX(seqNum) FROM eventlog
```

**Option C: Derived from events table**
```typescript
// Pros: Always consistent, no separate updates needed
// Cons: Slower (needs query on every push), doesn't support backendId
SELECT MAX(seqNum) FROM eventlog WHERE storeId = ?
```

**Recommendation**: Use separate metadata table for production (Option A). It's the most robust and supports backendId validation.

**Decision: What to include?**

**Minimum**:
- `currentHead` (number): Latest sequence number

**Recommended**:
- `backendId` (string): Prevents cross-backend pulls after migration
- `updatedAt` (timestamp): For debugging and monitoring
- `storeId` (string): If supporting multiple stores in one DB

**Decision: How to update?**

**Option A: Same transaction as events** (Recommended)
```typescript
await db.transaction(async (tx) => {
  // Insert events
  await tx.insert(eventlog).values(events)

  // Update head in same transaction
  await tx.update(context)
    .set({ currentHead: lastEvent.seqNum })
    .where(eq(context.storeId, storeId))
})
// Pros: Atomic, always consistent
// Cons: Slightly more complex
```

**Option B: Separate update**
```typescript
await db.insert(eventlog).values(events)
await db.update(context).set({ currentHead: lastEvent.seqNum })
// Pros: Simpler
// Cons: Risk of inconsistency if second update fails
```

**Recommendation**: Use same transaction (Option A) for strong consistency.

#### 3.3: Design Cursor-Based Queries

Cursors track where a client left off in the event stream. Your server must efficiently query events starting from a cursor.

**Decision: Index strategy?**

**Option A: Primary key on seqNum** (Recommended)
```sql
CREATE TABLE eventlog (
  seqNum INTEGER PRIMARY KEY,  -- Clustered index, very fast
  ...
);

-- Query is efficient: O(log n) seek + O(k) scan
SELECT * FROM eventlog
WHERE seqNum > ?      -- Cursor position
ORDER BY seqNum ASC
LIMIT 100;
```
- **Pros**: Fastest queries, no additional index needed
- **Cons**: Requires seqNum uniqueness (which we already need)

**Option B: Composite index**
```sql
CREATE TABLE eventlog (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  storeId TEXT NOT NULL,
  seqNum INTEGER NOT NULL,
  ...
);

CREATE INDEX idx_store_seq ON eventlog(storeId, seqNum);

-- Query uses composite index
SELECT * FROM eventlog
WHERE storeId = ? AND seqNum > ?
ORDER BY seqNum ASC
LIMIT 100;
```
- **Pros**: Supports multiple stores in same table
- **Cons**: Additional index overhead, slightly slower

**Option C: Sequential scan** (Not recommended for production)
```sql
-- No index, just scan
SELECT * FROM eventlog
WHERE seqNum > ?
ORDER BY seqNum ASC;
```
- **Pros**: No index maintenance
- **Cons**: Slow for large datasets (O(n) scan)

**Recommendation**: Primary key on seqNum (Option A) for single-store tables, composite index (Option B) for multi-store.

**Decision: Query pattern?**

**Option A: Limit-based pagination**
```typescript
const events = await db.query(
  `SELECT * FROM eventlog
   WHERE seqNum > $1
   ORDER BY seqNum ASC
   LIMIT $2`,
  [cursor, pageSize]
)
```
- **Pros**: Simple, predictable page size
- **Cons**: Doesn't handle "get all remaining" efficiently

**Option B: Size-based chunking**
```typescript
let chunk = []
let totalBytes = 0
for await (const event of queryStream(cursor)) {
  chunk.push(event)
  totalBytes += estimateSize(event)

  if (totalBytes >= MAX_BYTES || chunk.length >= MAX_COUNT) {
    yield chunk
    chunk = []
    totalBytes = 0
  }
}
```
- **Pros**: Respects transport limits (WebSocket message size, etc.)
- **Cons**: More complex, variable page size

**Recommendation**: Use limit-based for simplicity (Option A). Add size-based chunking (Option B) if needed for transport constraints.

**Decision: Ordering guarantee?**

**Option A: Implicit from storage**
```typescript
// If using stream service like S2
const records = await s2.read(stream, { seqNum: cursor })
// Order guaranteed by service
```

**Option B: Explicit ORDER BY**
```sql
SELECT * FROM eventlog
WHERE seqNum > ?
ORDER BY seqNum ASC  -- Explicit ordering
```

**Recommendation**: Always use explicit ORDER BY for SQL databases. Rely on service guarantees for stream services.

#### 3.4: Design Atomicity Guarantees

Push operations must be atomic to prevent race conditions and maintain event order.

**Decision: Atomicity mechanism?**

**Option A: Database transactions** (PostgreSQL, MySQL)
```typescript
await db.transaction(async (tx) => {
  // 1. Read current head
  const { currentHead } = await tx.query('SELECT currentHead FROM context WHERE storeId = ?', [storeId])

  // 2. Validate
  if (batch[0].parentSeqNum !== currentHead) {
    throw new ServerAheadError()
  }

  // 3. Write events
  await tx.insert(eventlog).values(batch)

  // 4. Update head
  await tx.update(context).set({ currentHead: batch[batch.length - 1].seqNum })
})
// Pros: Standard, well-understood, ACID guarantees
// Cons: Requires transaction support, slower than locks
```

**Option B: Explicit locks** (Cloudflare Durable Objects)
```typescript
await storage.blockConcurrencyWhile(async () => {
  // Only one push can execute at a time for this storeId
  const currentHead = getCurrentHead()
  validate(batch, currentHead)
  await persistEvents(batch)
  setHead(batch[batch.length - 1].seqNum)
})
// Pros: Fast, simple, guaranteed serialization
// Cons: Platform-specific (DO feature)
```

**Option C: Compare-and-swap** (DynamoDB, Redis)
```typescript
// DynamoDB conditional update
await dynamodb.put({
  TableName: 'context',
  Item: { storeId, currentHead: newHead },
  ConditionExpression: 'currentHead = :expectedHead',
  ExpressionAttributeValues: { ':expectedHead': oldHead }
})
// If condition fails, another process updated head -> retry
// Pros: Optimistic concurrency, scales well
// Cons: Requires retry logic, more complex
```

**Option D: Service guarantees** (S2, Kafka)
```typescript
// S2 guarantees sequential appends within a stream
await s2.append(stream, { records: batch })
// S2 assigns seq_nums sequentially, no explicit locking needed
// Pros: Simple, managed by service
// Cons: Requires external service, less control
```

**Option E: Application-level locking** (Redis, in-memory)
```typescript
// Acquire distributed lock
const lock = await redis.lock(`push:${storeId}`, { timeout: 5000 })
try {
  const currentHead = await getHead(storeId)
  validate(batch, currentHead)
  await persistEvents(batch)
  await setHead(batch[batch.length - 1].seqNum)
} finally {
  await lock.unlock()
}
// Pros: Works with any storage backend
// Cons: Additional infrastructure (Redis), failure handling complex
```

**Recommendation by storage type**:
- **SQL databases**: Use transactions (Option A)
- **Cloudflare/Durable Objects**: Use `blockConcurrencyWhile` (Option B)
- **DynamoDB/NoSQL**: Use compare-and-swap (Option C)
- **Stream services**: Rely on service guarantees (Option D)
- **Other**: Use application-level locking (Option E)

**Decision: Isolation level?**

For SQL transactions:

**Option A: SERIALIZABLE**
```sql
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN;
  -- Read + write operations
COMMIT;
```
- **Pros**: Strongest guarantee, prevents all anomalies
- **Cons**: Slowest, highest contention

**Option B: REPEATABLE READ**
```sql
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
```
- **Pros**: Prevents phantom reads, good balance
- **Cons**: Still some contention possible

**Option C: READ COMMITTED** (PostgreSQL default)
```sql
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
```
- **Pros**: Good performance, low contention
- **Cons**: Phantom reads possible (okay for our use case with proper validation)

**Recommendation**: READ COMMITTED is sufficient. Push validation (checking parentSeqNum matches currentHead) provides the necessary consistency guarantee without needing SERIALIZABLE.

**Decision: Failure handling?**

**Option A: Automatic retry**
```typescript
async function pushWithRetry(batch) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await atomicPush(batch)
    } catch (error) {
      if (error instanceof ConflictError && attempt < 2) {
        await sleep(100 * Math.pow(2, attempt))  // Exponential backoff
        continue
      }
      throw error
    }
  }
}
```
- **Use for**: Transient failures (deadlocks, network errors)
- **Don't use for**: Validation failures (ServerAheadError)

**Option B: Explicit rollback**
```typescript
try {
  await db.transaction(async (tx) => {
    // ... operations
  })
} catch (error) {
  // Transaction automatically rolled back
  // Just propagate error to client
  throw new InvalidPushError({ cause: error })
}
```
- **Use for**: All SQL transactions (automatic)

**Recommendation**: Combine both - automatic rollback (implicit in transactions) + retry for transient failures.

### Step 4: Implement Client

**IMPORTANT: For full servers, you can reuse the Cloudflare client!**

If you're building a custom sync server (not a client adapter to an external service), you should **implement the Cloudflare RPC protocol** server-side and **reuse the existing Cloudflare client** client-side.

#### Option A: Reuse Cloudflare Client (Recommended for Full Servers)

**Server requirements**:

Your custom server must implement the Cloudflare RPC protocol:

1. **Accept messages** defined in `@livestore/sync-cf/src/common/sync-message-types.ts`:
   - `PullRequest`: `{ cursor: Option<{ backendId, eventSequenceNumber }> }`
   - `PushRequest`: `{ batch: LiveStoreEvent[], backendId: Option<string> }`
   - `Ping`: `{}`

2. **Return responses** in the same format:
   - `PullResponse`: `{ batch: Array<{ eventEncoded, metadata }>, pageInfo, backendId }`
   - `PushAck`: `{}`
   - `Pong`: `{}`

3. **Support at least one transport**:
   - **HTTP RPC** (`SyncHttpRpc`): Request/response pattern
   - **WebSocket RPC** (`SyncWsRpc`): Persistent connection with streaming

4. **Use simple cursor format**:
   ```typescript
   type CloudflareCursor = {
     backendId: string,              // Your server's unique ID
     eventSequenceNumber: number     // Last seen seqNum
   }
   ```

**Client usage** (no custom code needed):

```typescript
import { makeHttpSync } from '@livestore/sync-cf/client'
// or: import { makeWsSync } from '@livestore/sync-cf/client'

// Just point to your custom server!
const syncBackend = makeHttpSync({
  url: 'https://my-custom-sync-server.com/sync',
  headers: {
    'Authorization': 'Bearer my-token'  // Your custom auth
  }
})

const adapter = makeAdapter({
  sync: {
    backend: syncBackend
  }
})
```

**Benefits**:
- ✅ **No client code to write** - Reuse battle-tested implementation
- ✅ **All transport options** - HTTP, WebSocket, or both
- ✅ **Built-in features** - Connection management, error handling, retry logic
- ✅ **Type safety** - Effect Schema validation
- ✅ **Observability** - Built-in tracing spans

**Server implementation example**:

```typescript
// Your custom server (Node.js, Deno, Bun, etc.)
import express from 'express'
import { RpcServer } from '@effect/rpc'
import { SyncHttpRpc } from '@livestore/sync-cf/common'

const app = express()

// Implement HTTP RPC endpoint
app.all('/sync', async (req, res) => {
  const handler = RpcServer.handler(SyncHttpRpc, {
    // Implement Pull
    'SyncHttpRpc.Pull': ({ storeId, cursor, payload }) =>
      Effect.gen(function* () {
        // Your custom storage logic
        const events = yield* queryEvents(storeId, cursor?.eventSequenceNumber)
        const backendId = yield* getBackendId()

        return {
          batch: events.map(e => ({
            eventEncoded: e,
            metadata: Option.some({ createdAt: e.createdAt })
          })),
          pageInfo: events.length > 0
            ? { _tag: 'MoreUnknown' }
            : { _tag: 'NoMore' },
          backendId
        }
      }),

    // Implement Push
    'SyncHttpRpc.Push': ({ storeId, batch, backendId }) =>
      Effect.gen(function* () {
        // Your custom validation + persistence logic
        yield* validateAndPersist(storeId, batch)
        return {}  // PushAck
      }),

    // Implement Ping
    'SyncHttpRpc.Ping': ({ storeId, payload }) =>
      Effect.succeed({})  // Pong
  })

  // Handle request
  await Effect.runPromise(handler(req, res))
})

app.listen(3000)
```

**When to use this approach**:
- Building a custom full server (not integrating with external service)
- Want production-ready client without writing client code
- Need HTTP and/or WebSocket support
- Want to focus on server-side storage/logic only

#### Option B: Build Custom Client (Only When Necessary)

**When you must build a custom client**:
- Integrating with external service (like ElectricSQL, S2)
- Need fundamentally different protocol (not RPC-based)
- External service has its own client requirements
- Custom transport needs (e.g., gRPC, custom WebSocket protocol)

**If building custom client**, use reusable components

**Use reusable components**:
```typescript
import { /* connection management patterns */ } from './reusable'

export const makeSyncBackend = (options: MyOptions): SyncBackendConstructor =>
  ({ storeId, payload }) => Effect.gen(function* () {
    // ✅ Reuse: Connection management
    const isConnected = yield* SubscriptionRef.make(false)
    const ping = /* reuse pattern */
    yield* ping.pipe(Effect.repeat(Schedule.spaced(10_000)), Effect.forkScoped)

    // ⚠️ Customize: Pull implementation
    const pull = (cursor, options) => {
      // Your protocol-specific implementation
      return myPullStream(cursor, options)
    }

    // ⚠️ Customize: Push implementation
    const push = (batch) => {
      // Your protocol-specific implementation
      return myPushEffect(batch)
    }

    // ✅ Reuse: Return structure
    return SyncBackend.of({
      connect,
      pull,
      push,
      ping,
      isConnected,
      metadata: /* your metadata */,
      supports: /* your capabilities */
    })
  })
```

### Step 5: Implement Server (if building full server)

**Note**: If you chose to reuse the Cloudflare client (Step 4, Option A), implement the server to match the RPC protocol as shown in the example above. Otherwise, implement your custom protocol.

**Minimum server requirements** (generic approach):

1. **Push handler**:
   ```typescript
   const handlePush = async (request: PushRequest) => {
     // ✅ Reuse: Validation logic
     const validation = validatePushBatch(request.batch, currentHead)
     if (!validation.valid) return error(validation.error)

     // ❌ Custom: Atomic write (using decision from Step 3.4)
     await atomicWrite(() => {
       const head = getHead(request.storeId)
       if (request.batch[0].parentSeqNum !== head) {
         throw new ServerAheadError({ serverHead: head })
       }

       persistEvents(request.batch)
       setHead(request.batch.at(-1)!.seqNum)
     })

     // ❌ Custom: Broadcasting (if applicable)
     await broadcast(request.storeId, request.batch)

     return { success: true }
   }
   ```

2. **Pull handler**:
   ```typescript
   const handlePull = (request: PullRequest) => {
     // ❌ Custom: Cursor validation
     validateCursor(request.cursor)

     // ❌ Custom: Query storage (using decision from Step 3.3)
     const events = queryEvents(request.storeId, request.cursor)

     // ✅ Reuse: Response format
     return {
       batch: events.map(e => ({
         eventEncoded: e,
         metadata: extractMetadata(e)
       })),
       pageInfo: computePageInfo(events, totalRemaining)
     }
   }
   ```

## Quick Decision Summary

| Decision Point | Recommended Choice | Why |
|----------------|-------------------|-----|
| **Architecture** | Full server if need control; Adapter if using external service | Control vs convenience trade-off |
| **Client** | Reuse Cloudflare client for full servers | No client code to write |
| **Transport** | HTTP RPC or WebSocket RPC (for Cloudflare client) | Standard, well-tested |
| **Storage** | SQL with transactions | ACID guarantees, queryable |
| **Head tracking** | Separate metadata table | Robust, supports backendId |
| **Cursor queries** | Primary key on seqNum | Fastest, simplest |
| **Atomicity** | Database transactions | Standard, reliable |
| **Isolation** | READ COMMITTED | Good balance of performance and safety |

## Key Recommendations

### For Custom Full Server

1. **Reuse Cloudflare client** - Implement server-side RPC protocol, use existing client
2. **Start with Cloudflare server** as reference for storage and validation patterns
3. **Focus on**: Storage backend (Step 3), concurrency control (Step 3.4), broadcasting
4. **Message format**: Use Cloudflare's message types from `sync-message-types.ts`
5. **Cursor format**: Simple `{ backendId, eventSequenceNumber }`
6. **Test**: Race conditions, concurrent pushes, disconnection scenarios, backendId validation

**Recommended tech stack**:
- **Runtime**: Node.js, Bun, or Deno
- **Database**: PostgreSQL with transactions
- **Transport**: Effect RPC over HTTP and/or WebSocket
- **Deployment**: Fly.io, Railway, or any Node.js host

### For Custom Client Adapter

1. **Start with ElectricSQL or S2** as reference (depending on target service)
2. **Reuse**: Entire client structure, error handling, retry logic
3. **Focus on**: API integration, response parsing, cursor format
4. **Consider**: Whether target service provides ordering/atomicity guarantees
5. **Test**: Network failures, reconnection, cursor resumption

### Universal Best Practices

1. **Type safety**: Use Effect Schema for all messages
2. **Error handling**: Wrap all errors in InvalidPullError/InvalidPushError
3. **Observability**: Add spans for all operations (`Effect.withSpan`)
4. **Testing**: Test against the sync-provider test suite
5. **Documentation**: Document cursor format, limitations, requirements

## Conclusion

Building a custom sync provider is **significantly simpler** than it first appears, especially for full servers:

### For Custom Full Servers

The recommended approach is to **implement only the server-side**:

1. **Reuse** the Cloudflare client entirely (zero client code to write!)
2. **Implement** the Cloudflare RPC protocol server-side
3. **Focus** on storage decisions (Step 3): head tracking, cursor queries, atomicity
4. **Use** simple cursor format: `{ backendId, eventSequenceNumber }`
5. **Test** thoroughly: race conditions, concurrent pushes, backendId validation

**Effort breakdown**:
- ❌ **No client code** - Reuse `makeHttpSync` or `makeWsSync`
- ✅ **Storage layer** - Implement based on Step 3 decisions (your main work)
- ✅ **RPC handlers** - Implement Pull, Push, Ping (straightforward with Effect RPC)
- ✅ **Validation** - Reuse patterns from Cloudflare implementation

### For Client Adapters

If integrating with external services (ElectricSQL, S2, etc.):

1. **Implement** the `SyncBackend` interface client-side
2. **Reuse** connection management, error handling, retry patterns
3. **Customize** pull/push for external service's protocol
4. **Test** network failures, reconnection, cursor management

### Key Insights

The LiveStore sync provider interface is **exceptionally well-designed**:

- **Separation of concerns**: Client and server can be developed independently
- **Protocol flexibility**: HTTP RPC, WebSocket RPC, SSE, long-polling all supported
- **Reusable components**: Connection management, validation, errors are universal
- **Simple cursor model**: Just track position and backend ID
- **Type safety**: Effect Schema validates all messages

The **biggest simplification** for custom implementations is that **you don't need to write a client** if you implement the Cloudflare RPC protocol server-side. This reduces the work to:
1. Storage design (choose data store, implement head tracking and cursor queries)
2. Atomicity mechanism (choose transactions, locks, or service guarantees)
3. RPC handlers (implement Pull, Push, Ping using your storage)

The existing implementations provide excellent reference patterns, and the decision tree in this document gives you specific choices to make at each step.
