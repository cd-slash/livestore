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

**Questions**:
1. What data store? (SQL, NoSQL, object storage, stream service)
2. How to track head? (column, metadata table, service-managed)
3. How to query by cursor? (indexed column, range queries, stream API)
4. How to ensure atomicity? (transactions, locks, service guarantees)

### Step 4: Implement Client

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

### Step 5: Implement Server (if full server)

**Minimum requirements**:

1. **Push handler**:
   ```typescript
   const handlePush = async (request: PushRequest) => {
     // ✅ Reuse: Validation logic
     const validation = validatePushBatch(request.batch, currentHead)
     if (!validation.valid) return error(validation.error)

     // ❌ Custom: Atomic write
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

     // ❌ Custom: Query storage
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

## Key Recommendations

### For Custom Full Server

1. **Start with Cloudflare implementation** as reference
2. **Reuse**: Connection management, validation logic, message schemas
3. **Focus on**: Storage backend, concurrency control, broadcasting
4. **Consider**: Durable Objects / Fly Machines / Cloudflare Workers pattern
5. **Test**: Race conditions, concurrent pushes, disconnection scenarios

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

Building a custom sync provider is **straightforward** if you:

1. **Reuse** the client structure (connection, errors, schemas)
2. **Choose** an appropriate architecture (full server vs adapter)
3. **Implement** storage and protocol-specific logic
4. **Follow** the server-side requirements (validation, atomicity, ordering)
5. **Test** thoroughly (race conditions, network failures, disconnections)

The LiveStore sync provider interface is **well-designed** to allow diverse implementations while maintaining consistency guarantees. The existing implementations provide excellent reference patterns for both full servers and client adapters.
