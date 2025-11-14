# How the ElectricSQL Sync Provider Works

## High-Level Architecture

The ElectricSQL sync provider is a **client-side adapter** that bridges LiveStore to ElectricSQL's shape-based replication system. Unlike a standalone sync server, it leverages external infrastructure (PostgreSQL + ElectricSQL) to provide real-time synchronization.

```
LiveStore Client ──> API Proxy ──> ElectricSQL Server ──> PostgreSQL
                                         │
                                         └──> Other Clients (via shapes)
```

## Key Insight

**Write path**: Events bypass ElectricSQL and go directly to PostgreSQL
**Read path**: Events are read through ElectricSQL's optimized replication layer

This architecture leverages ElectricSQL's strengths (shape-based sync, optimized caching) while maintaining LiveStore's event sourcing guarantees through direct PostgreSQL writes.

## Step-by-Step Flow

### 1. Initialization

**File**: `packages/@livestore/sync-electric/src/index.ts:164-368`

When `makeSyncBackend()` is called:

1. **Create connection state**:
   - `isConnected`: SubscriptionRef tracking connectivity
   - Initially `false`

2. **Extract endpoints**:
   - Support single endpoint or separate push/pull/ping URLs
   - `pullEndpoint`: For GET requests (pull)
   - `pushEndpoint`: For POST requests (push)
   - `pingEndpoint`: For HEAD requests (ping)

3. **Get HTTP client** from Effect context

4. **Setup ping mechanism** (if enabled):
   - Send HEAD request every 10 seconds
   - Timeout after 10 seconds
   - Update `isConnected` based on success/failure
   - Fork as background fiber

5. **Determine connect strategy**:
   - If same-origin: Skip (assume connected)
   - If remote: Send initial ping

6. **Return SyncBackend interface**:
   - `connect`: Initial connection setup
   - `pull`: Stream events from ElectricSQL
   - `push`: Write events to PostgreSQL
   - `ping`: Check server availability
   - `isConnected`: Reactive connection state
   - `metadata`: Provider information
   - `supports`: Capability flags

### 2. Pull Operation (Historical Data)

**File**: `packages/@livestore/sync-electric/src/index.ts:303-334`

When client calls `pull(cursor, { live: false })`:

1. **Start unfold stream** with initial handle from cursor metadata

2. **For each iteration**, call `runPull(handle, { live: false })`:

   **a. Encode request** (`index.ts:193-196`):
   - Serialize `PullPayload`: `{ storeId, handle, payload, live: false }`
   - Encode as URI component
   - Build URL: `${pullEndpoint}?args=${encodedJson}`

   **b. Send HTTP GET** to API proxy

   **c. Proxy processes request** (in test setup):
   - Decode args from query params
   - Call `makeElectricUrl()` to build ElectricSQL URL
   - Check if table needs initialization (`needsInit = handle is None`)
   - If new table: Create PostgreSQL schema
   - Forward request to ElectricSQL server

   **d. ElectricSQL processes**:
   - Parse table name and offset
   - Query PostgreSQL for changes since offset
   - Return response with data + new handle/offset
   - Long-poll up to ~20 seconds if no data

   **e. Handle HTTP status** (`index.ts:200-226`):
   - **400**: Table doesn't exist → Return empty batch
   - **401**: Unauthorized → Return `InvalidPullError`
   - **409**: Handle mismatch → Not yet implemented (would reset)
   - **204**: No new data (long-poll timeout) → Return empty batch with same handle
   - **2xx**: Success, process response

   **f. Extract headers** (`index.ts:228-232`):
   - `electric-handle`: Shape identifier
   - `electric-offset`: Current position (e.g., "27294160_0")

   **g. Parse response body** (`index.ts:240-255`):
   - Decode as array of `ResponseItem`
   - Check for invalid operations (update/delete)
   - If found: Throw `InvalidOperationError` with helpful message
   - Filter to only `ResponseItemInsert` items

   **h. Map to LiveStore format** (`index.ts:257-260`):
   - Extract event data from `value` field
   - Parse string-encoded numbers (`seqNum`, `parentSeqNum`)
   - Parse JSON-encoded `args`
   - Attach metadata (handle + offset) to each event

   **i. Return batch** (`index.ts:264`):
   - Return `[batch, nextHandle]`

3. **Emit to stream** (`index.ts:314-317`):
   - If batch has data: Emit with `hasMore: true`
   - If no data but first iteration: Emit empty with `hasMore: false`
   - If no data and not first: Stop stream
   - Map to `pageInfo` format

4. **Continue or stop**:
   - If batch empty and not live: Stop
   - Otherwise: Use next handle for next iteration

### 3. Pull Operation (Live Updates)

**File**: `packages/@livestore/sync-electric/src/index.ts:303-334`

When client calls `pull(cursor, { live: true })`:

Same as historical pull, but:
- `live: true` passed in PullPayload
- ElectricSQL keeps connection open for new changes
- Stream never stops (returns `hasMore: false` for empty batches but continues)
- Client receives real-time updates via long-polling

The unfold stream continues indefinitely, with Electric sending data as it arrives.

### 4. Push Operation

**File**: `packages/@livestore/sync-electric/src/index.ts:337-352`

When client calls `push(batch)`:

1. **Build POST request**:
   - URL: `${pushEndpoint}`
   - Body: `{ storeId, batch }` (JSON-encoded `PushPayload`)
   - Content-Type: application/json

2. **Send to API proxy**

3. **Proxy processes** (in test setup, `tests/sync-provider/src/providers/electric.ts:174-189`):
   - Decode request body
   - Extract storeId and batch
   - Get PostgreSQL connection for this store
   - Run migration if table doesn't exist
   - **Insert events directly to PostgreSQL**:
     ```sql
     INSERT INTO "eventlog_6_myStore"
       ("seqNum", "parentSeqNum", "name", "args", "clientId", "sessionId")
     VALUES
       (0, -1, 'todoCreated', '{"id":"123"}', 'client1', 'session1'),
       (1, 0, 'todoCompleted', '{"id":"123"}', 'client1', 'session1')
     ```
   - Batch insert all events in the request
   - Close database connection

4. **Return response**: `{ success: true }`

5. **ElectricSQL picks up changes**:
   - Monitors PostgreSQL via logical replication
   - Detects new INSERTs
   - Propagates to all subscribed clients via shapes
   - May have small delay (eventual consistency)

6. **Handle errors**:
   - If push fails: Wrap in `InvalidPushError`
   - If `success: false`: Return error

### 5. Response Decoding Details

**File**: `packages/@livestore/sync-electric/src/index.ts:74-108`

ElectricSQL returns a complex response format. The provider decodes:

**Insert item** (valid):
```json
{
  "key": "\"public\".\"events_xyz\"/\"0\"",
  "value": {
    "seqNum": "0",
    "parentSeqNum": "-1",
    "name": "todoCreated",
    "args": "{\"id\": \"123\"}",
    "clientId": "abc",
    "sessionId": "xyz"
  },
  "headers": {
    "operation": "insert",
    "relation": ["public", "events_xyz"],
    "lsn": 27294160,
    "op_position": 0,
    "txids": [753],
    "last": true
  }
}
```

**Control item** (metadata):
```json
{
  "headers": {
    "control": "up-to-date",
    "global_last_seen_lsn": 27294160
  }
}
```

**Invalid item** (rejected):
```json
{
  "value": { ... },
  "headers": {
    "operation": "update"  // or "delete"
  }
}
```

The schema `LiveStoreEventGlobalFromStringRecord`:
- Parses string-encoded numbers: `NumberFromString`
- Parses JSON-encoded args: `parseJson(Schema.Any)`
- Transforms to `LiveStoreEvent.AnyEncodedGlobal`

### 6. Table Name Generation

**File**: `packages/@livestore/sync-electric/src/make-electric-url.ts:88-103`

For storeId `"my-app:user123"`:

1. **Escape**: Replace non-alphanumeric → `my_app_user123`
2. **Add version prefix**: `eventlog_6_my_app_user123`
3. **Check length**: Must be ≤ 63 chars (PostgreSQL limit)
4. **If too long**:
   - Hash storeId: `Hash.string("my-app:user123")` → `1234567890`
   - Use: `eventlog_6_hash_1234567890`
   - Warn in console

Result: `"eventlog_6_my_app_user123"`

### 7. ElectricSQL URL Construction

**File**: `packages/@livestore/sync-electric/src/make-electric-url.ts:9-86`

The proxy calls `makeElectricUrl()` to build the Electric request:

**Input**:
```typescript
{
  electricHost: "http://localhost:5050",
  searchParams: URLSearchParams from client request,
  apiSecret: "change-me-electric-secret"  // or sourceId/sourceSecret for Electric Cloud
}
```

**Process**:
1. Decode args from search params
2. Convert storeId → table name
3. Build ElectricSQL endpoint: `${electricHost}/v1/shape`
4. Add search params:
   - `table`: `"eventlog_6_myStore"` (quoted for capitals)
   - `offset`: From handle or -1 for initial
   - `handle`: From handle if resuming
   - `live`: true/false
   - `api_secret`: For authentication (or `source_id`/`source_secret`)

**Output**:
```typescript
{
  url: "http://localhost:5050/v1/shape?table=\"eventlog_6_myStore\"&offset=-1&api_secret=xxx",
  storeId: "myStore",
  needsInit: true,  // true if no handle (first pull)
  payload: { authToken: "..." }  // custom payload from client
}
```

### 8. PostgreSQL Schema

**File**: `tests/sync-provider/src/providers/electric.ts:217-229`

When table needs initialization:

```sql
CREATE TABLE IF NOT EXISTS "eventlog_6_myStore" (
  "seqNum" INTEGER PRIMARY KEY,
  "parentSeqNum" INTEGER,
  "name" TEXT NOT NULL,
  "args" JSONB NOT NULL,
  "clientId" TEXT NOT NULL,
  "sessionId" TEXT NOT NULL
)
```

**Key points**:
- PRIMARY KEY on seqNum (ensures uniqueness, prevents duplicates)
- JSONB for args (efficient storage + querying)
- No indexes on other columns (can be added for queries)
- No foreign keys (events are independent)

### 9. Error Handling Examples

**Invalid Operation** (`index.ts:245-255`):
```typescript
// ElectricSQL returns delete operation
{
  "value": { "seqNum": "5", ... },
  "headers": { "operation": "delete" }
}

// Provider throws:
new InvalidOperationError({
  operation: "delete",
  message: "ElectricSQL 'delete' event received. This results from directly
            mutating the event log. Append a series of events that produce
            the desired state instead of mutating the event log."
})
```

**Unauthorized** (`index.ts:200-205`):
```typescript
// HTTP 401 response
return InvalidPullError.make({
  cause: new Error("Unauthorized (401): Couldn't connect to ElectricSQL: Access denied")
})
```

**Handle mismatch** (`index.ts:209-220`):
```typescript
// HTTP 409 response
// TODO: Not yet implemented
// Should reset handle and pull from scratch
return notYetImplemented(`Electric shape not found`)
```

## Complete Request Flow Examples

### Example 1: Initial Pull (Empty Store)

```
1. Client: pull(None, { live: false })
2. Provider: GET /api/electric?args={"storeId":"myStore","handle":null,"live":false}
3. Proxy: Decode args, call makeElectricUrl()
4. Proxy: needsInit=true, create table in PostgreSQL
5. Proxy: GET http://electric:5050/v1/shape?table="eventlog_6_myStore"&offset=-1
6. Electric: Query PostgreSQL, table is empty
7. Electric: Return [], headers: { electric-handle: "abc_123", electric-offset: "0_0" }
8. Provider: Decode response, empty batch
9. Provider: Emit { batch: [], pageInfo: { hasMore: false } }
10. Provider: Stop stream (no data, not live)
```

### Example 2: Pull with Existing Data

```
1. Client: pull(None, { live: false })
2. Provider: GET /api/electric?args={"storeId":"myStore","handle":null,"live":false}
3. Proxy: Forward to Electric with offset=-1
4. Electric: Query PostgreSQL, find 50 events
5. Electric: Return events 0-49, handle="abc_123", offset="49_0"
6. Provider: Decode 50 events, attach metadata
7. Provider: Emit { batch: [50 events], pageInfo: { hasMore: unknown } }
8. Provider: Continue with handle="abc_123", offset="49_0"
9. Provider: GET with new handle
10. Electric: No new data, long-poll 20s, return 204
11. Provider: Empty batch, stop stream
```

### Example 3: Push Events

```
1. Client: push([{ seqNum: 0, parentSeqNum: -1, name: "todoCreated", ... }])
2. Provider: POST /api/electric, body: { storeId: "myStore", batch: [...] }
3. Proxy: Decode request
4. Proxy: Create table if needed
5. Proxy: INSERT INTO "eventlog_6_myStore" VALUES (0, -1, 'todoCreated', ...)
6. PostgreSQL: Row inserted, transaction committed
7. Proxy: Return { success: true }
8. Provider: Return void (success)
9. Electric (background): Detect new row via logical replication
10. Electric (background): Broadcast to subscribed clients
```

### Example 4: Live Pull (Real-time)

```
1. Client: pull(cursor, { live: true })
2. Provider: GET with live=true
3. Electric: Return historical data immediately
4. Provider: Emit batch
5. Provider: Continue with next handle, live=true
6. Electric: Long-poll, wait for new data
7. [User pushes new event from another client]
8. Electric: Detect change, return new events
9. Provider: Emit new batch
10. Provider: Continue polling (never stops)
```

## Key Design Patterns

### 1. Adapter Pattern
- Wraps external system (ElectricSQL) in LiveStore interface
- Hides complexity of shape-based replication
- Provides familiar pull/push/ping API

### 2. Long-Polling
- Electric keeps connection open for ~20s
- Returns 204 if no data
- Client immediately retries
- Simulates real-time without WebSocket

### 3. Direct Writes
- Bypass Electric for push operations
- Write directly to PostgreSQL
- Electric detects via replication
- Ensures immediate consistency for writes

### 4. Cursor-based Pagination
- Handle + offset tracks position
- Resumable from any point
- Efficient for large datasets
- Survives disconnections

### 5. Immutability Enforcement
- Reject update/delete operations
- Force append-only pattern
- Align with event sourcing principles
- Clear error messages guide users

## Performance Characteristics

### Latency
- **Pull (cold)**: 10-100ms (Postgres query)
- **Pull (live)**: 20s max (long-poll timeout)
- **Push**: 20-100ms (Postgres insert)
- **Propagation**: 100-1000ms (Electric replication lag)

### Throughput
- **Limited by**: PostgreSQL + Electric capacity
- **Pull**: High (Electric's shape caching)
- **Push**: Medium (single Postgres connection per request)

### Scalability
- **Horizontal**: Scale PostgreSQL + Electric servers
- **Vertical**: Postgres handles thousands of concurrent connections
- **Caching**: Electric caches shapes aggressively

## Limitations vs Cloudflare

| Aspect | ElectricSQL | Cloudflare |
|--------|-------------|------------|
| **Infrastructure** | Requires Postgres + Electric | Serverless (no setup) |
| **Latency** | Higher (multi-hop) | Lower (single DO) |
| **Consistency** | Eventual (replication lag) | Immediate (DO serialization) |
| **Queryability** | Full SQL on Postgres | Limited |
| **Cost** | Infrastructure costs | Per-request costs |
| **Ops complexity** | High (manage servers) | Low (managed) |

## Summary

The ElectricSQL sync provider is a **thin client adapter** that:

1. **Reads** via ElectricSQL's shape-based replication (optimized caching, long-polling)
2. **Writes** directly to PostgreSQL (immediate consistency)
3. **Relies** on external infrastructure (Postgres + Electric)
4. **Enforces** immutability (rejects updates/deletes)
5. **Supports** live pulls via long-polling
6. **Provides** queryability via PostgreSQL

It's ideal for applications that:
- Already use PostgreSQL
- Need SQL querying on event data
- Can manage infrastructure
- Want Electric's caching optimizations
- Require strong consistency on writes

The key innovation is **splitting read and write paths**: writes go directly to Postgres for consistency, reads go through Electric for optimization.
