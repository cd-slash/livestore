# How the S2 Sync Provider Works

## High-Level Architecture

The S2 sync provider is a **client-side adapter** that bridges LiveStore to S2's managed streaming service. S2 provides ordered, append-only streams with efficient SSE-based tailing.

```
LiveStore Client ──> API Proxy ──> S2 Service (Basin/Stream)
                                         │
                                         └──> Other Clients (via SSE tails)
```

## Key Insight

**S2 as a streaming primitive**: S2 is a managed append-only log service optimized for real-time streaming. The provider maps LiveStore's event sourcing model directly onto S2's stream model, with one critical separation: **S2's physical sequence numbers (seq_num) are independent from LiveStore's logical sequence numbers (seqNum)**.

This separation enables future optimizations (compaction, filtering) without breaking application logic.

## Step-by-Step Flow

### 1. Initialization

**File**: `packages/@livestore/sync-s2/src/sync-provider.ts:82-311`

When `makeSyncBackend()` is called:

1. **Create connection state**:
   - `isConnected`: SubscriptionRef tracking connectivity
   - Initially `false`

2. **Extract endpoints** (`sync-provider.ts:87-89`):
   - Support single endpoint or separate push/pull/ping URLs
   - `pullEndpoint`: For GET requests (pull via SSE)
   - `pushEndpoint`: For POST requests (push/append)
   - `pingEndpoint`: For HEAD requests (ping)

3. **Get HTTP client** from Effect context

4. **Determine origin** (`sync-provider.ts:93-95`):
   - Check if pullEndpoint is same-origin
   - Used to optimize connect strategy

5. **Setup ping mechanism** (`sync-provider.ts:97-111`):
   - Send HEAD request every 10 seconds (configurable)
   - Timeout after 10 seconds (configurable)
   - Update `isConnected` based on success/failure
   - Fork as background fiber if enabled

6. **Determine connect strategy** (`sync-provider.ts:114-116`):
   - If same-origin: Skip (assume connected)
   - If remote: Send initial ping

7. **Return SyncBackend interface** (`sync-provider.ts:236-310`):
   - `connect`: Initial connection setup
   - `pull`: Stream events from S2 via SSE
   - `push`: Append events to S2 with chunking
   - `ping`: Check server availability
   - `isConnected`: Reactive connection state
   - `metadata`: Provider information
   - `supports`: Capability flags

### 2. Pull Operation (Non-Live)

**File**: `packages/@livestore/sync-s2/src/sync-provider.ts:238-248`

When client calls `pull(cursor, { live: false })`:

1. **Call runPullSse** with `live: false`

2. **Inside runPullSse** (`sync-provider.ts:118-183`):

   **a. Extract S2 cursor** (`sync-provider.ts:125-130`):
   - Get s2SeqNum from cursor metadata
   - If None: Use 'from-start' (maps to seq_num=0)
   - If Some: Use stored s2SeqNum value

   **b. Encode request** (`sync-provider.ts:132-133`):
   - Build PullArgs: `{ storeId, payload, s2SeqNum, live: false }`
   - Encode as URI component
   - Build URL: `${pullEndpoint}?args=${encodedJson}`

   **c. Send SSE request** (`sync-provider.ts:135-141`):
   - GET request with `accept: text/event-stream` header
   - Execute via HttpClient
   - Get response stream

   **d. Decode SSE stream** (`sync-provider.ts:138-182`):
   - Decode as UTF-8 text
   - Parse SSE format via `Sse.makeChannel()`
   - Process each SSE message:

     **i. Filter pings** (`sync-provider.ts:146`):
     - SSE event type: "ping" → Return `Option.none()` (skip)

     **ii. Handle errors** (`sync-provider.ts:147-149`):
     - SSE event type: "error" → Throw `InvalidPullError`

     **iii. Process batch** (`sync-provider.ts:150-170`):
     - SSE event type: "batch"
     - Parse data as JSON: `ReadBatch` schema
     - Decode batch using `decodeReadBatch()`:
       - Extract records array
       - Parse JSON body from each record
       - Filter out records with undefined body
       - Map to `{ eventEncoded, metadata: { s2SeqNum } }`
     - Extract tail position from batch
     - Compute remaining events:
       ```typescript
       remaining = tail.seq_num - last_record.s2SeqNum - 1
       ```
     - Build page info:
       - If remaining > 0: `pageInfoMoreKnown(remaining)`
       - Else: `pageInfoNoMore`
     - Return `{ batch, pageInfo }`

     **iv. Handle end-of-stream** (`sync-provider.ts:173-174`):
     - SSE event type: "message", data: "[DONE]"
     - Return `Option.none()` (end stream)

   **e. Filter and error handling** (`sync-provider.ts:179-182`):
   - Filter out `Option.none()` values
   - Map errors to `InvalidPullError`
   - Apply retry schedule (2 retries, 100ms spacing)

3. **Emit empty batch if needed** (`sync-provider.ts:242-246`):
   - If stream is empty, emit one empty batch
   - Ensures client knows pull completed even with no data

### 3. Pull Operation (Live)

**File**: `packages/@livestore/sync-s2/src/sync-provider.ts:185-234`

When client calls `pull(cursor, { live: true })`:

1. **Call ssePull** with initial cursor

2. **Inside ssePull** (`sync-provider.ts:185-234`):

   **a. Define cursor computation** (`sync-provider.ts:191-208`):
   - Extract last seen event from batch
   - Update cursor to last event's seqNum and s2SeqNum
   - Fallback to current cursor if batch empty

   **b. Define reconnection loop** (`sync-provider.ts:210-231`):
   - `loop(cursor, isFirst)` function:

     **i. Initial pull** (if isFirst):
     - Call `runPullSse(cursor, live: false)`
     - Sets `wait=0` in S2 request
     - S2 returns immediately with historical data
     - Emit results

     **ii. Subsequent pulls** (if !isFirst):
     - Call `runPullSse(cursor, live: true)`
     - Omits `wait` param (S2 waits for new data)
     - S2 keeps connection open
     - Emit results as they arrive

     **iii. Auto-reconnect** (`sync-provider.ts:228-229`):
     - Use `Stream.concatWithLastElement`
     - When stream ends (disconnect, timeout, etc.)
     - Extract last emitted item
     - Compute next cursor from last item
     - Call `loop` again with new cursor and `isFirst: false`
     - Creates infinite reconnection loop

3. **Result**: Continuous stream that:
   - Starts with historical data (fast, wait=0)
   - Switches to live mode (slow, waits for new events)
   - Auto-reconnects on disconnect
   - Never stops (until client cancels)

### 4. Proxy Request Building

**Files**:
- `packages/@livestore/sync-s2/src/s2-proxy-helpers.ts:102-127` (buildPullRequest)
- `tests/sync-provider/src/providers/s2.ts:173-214` (proxy handler)

When proxy receives pull request:

1. **Decode args** from URL query params

2. **Ensure stream exists** (`s2.ts:181-189`):
   - Check if stream already created (in-memory cache)
   - If not: Call S2 API to create stream
   - Add to cache to avoid redundant calls
   - Retry with exponential backoff (10s max)

3. **Build S2 pull request** (`s2-proxy-helpers.ts:102-127`):
   - Convert storeId → stream name (sanitized)
   - Convert cursor → seq_num:
     ```typescript
     seq_num = s2SeqNum === 'from-start' ? 0 : s2SeqNum + 1
     ```
     Note: Cursor points to **last seen**, seq_num is **next to read**
   - Build URL with params:
     - `seq_num`: Where to start reading
     - `clamp: true`: Respect count limits
     - `wait: 0` (non-live) or omitted (live)

4. **Forward to S2** (`s2.ts:201-209`):
   - GET request with SSE headers:
     - `Authorization: Bearer {token}`
     - `accept: text/event-stream`
     - `s2-format: raw` (use raw JSON, not base64)
   - Retry with exponential backoff
   - Stream response back to client

5. **Handle errors** (`s2.ts:211-213`):
   - Catch all errors
   - Return empty ReadBatch: `{ records: [] }`
   - Keeps pull stream alive even on transient errors

### 5. Push Operation

**File**: `packages/@livestore/sync-s2/src/sync-provider.ts:250-297`

When client calls `push(batch)`:

1. **Define error mapper** (`sync-provider.ts:252-282`):
   - Maps various error types to `InvalidPushError`
   - Special handling for `S2LimitExceededError`:
     - Extract limit type, max, actual values
     - Build helpful error message
     - Include metadata in error payload

2. **Chunk events** (`sync-provider.ts:284`):
   - Call `chunkEventsForS2(batch)`
   - Pre-computes metered bytes for each event
   - Validates no single event > 1 MiB
   - Splits into chunks:
     - Max 1,000 records per chunk
     - Max 1 MiB metered bytes per chunk
   - Returns array of chunks: `S2Chunk[]`
   - Each chunk contains: `{ events, records, meteredBytes }`

3. **For each chunk** (`sync-provider.ts:286-296`):

   **a. Build POST request** (`sync-provider.ts:287-289`):
   - URL: `${pushEndpoint}`
   - Body: `{ storeId, batch: chunk.events }`
   - Content-Type: application/json

   **b. Send to proxy**

   **c. Proxy processes** (`s2.ts:218-254`):
   - Decode request body
   - Ensure stream exists (create if needed)
   - Check for test failure injection (for testing)
   - Convert events to S2 records:
     ```typescript
     records = events.map(ev => ({
       body: JSON.stringify(ev)
     }))
     ```
   - Call S2 append API with retry:
     - POST to `/streams/{stream}/records`
     - Headers: auth + `s2-format: raw`
     - Body: `{ records }`
     - Retry with exponential backoff (10s max)

   **d. S2 processes**:
   - Validate batch limits (1 MiB, 1000 records)
   - Assign seq_nums sequentially
   - Persist records to stream
   - Broadcast to SSE subscribers
   - Return success

   **e. Proxy returns** `{ success: true }`

   **f. Provider validates** (`sync-provider.ts:291-294`):
   - Apply error mapper on failure
   - Apply retry schedule (2 retries, 100ms spacing)

4. **Complete** when all chunks appended

### 6. Chunking Details

**File**: `packages/@livestore/sync-s2/src/limits.ts:104-135`

The `chunkEventsForS2()` function:

1. **Pre-process events** (`limits.ts:109`):
   - For each event:
     - Stringify to JSON: `body = JSON.stringify(event)`
     - Create S2 record: `{ body }`
     - Compute metered bytes:
       ```typescript
       bytes = 8 + utf8_length(body)
       ```
       (8 byte overhead + body size)
     - Validate: If bytes > 1 MiB, throw `S2LimitExceededError`
     - Store: `{ event, record, meteredBytes, index }`

2. **Split into chunks** (`limits.ts:112-120`):
   - Use Effect's `splitChunkBySize`
   - Config:
     - `maxItems: 1000` (MAX_RECORDS_PER_BATCH)
     - `maxBytes: 1_048_576` (MAX_BATCH_METERED_BYTES)
     - `measure`: Sum of meteredBytes
   - Returns: `Chunk<Chunk<PreparedEvent>>`

3. **Map chunks** (`limits.ts:122`):
   - For each chunk:
     - Extract events: `chunk.map(item => item.event)`
     - Extract records: `chunk.map(item => item.record)`
     - Sum metered bytes
     - Return: `{ events, records, meteredBytes }`

4. **Handle errors** (`limits.ts:123-134`):
   - If single event > 1 MiB: `S2LimitExceededError`
   - Includes: limitType, max, actual, recordIndex

### 7. Decoding Details

**File**: `packages/@livestore/sync-s2/src/decode.ts:15-28`

When S2 returns a batch:

**Input** (from S2):
```json
{
  "records": [
    {
      "body": "{\"seqNum\":0,\"parentSeqNum\":-1,\"name\":\"todoCreated\",\"args\":{\"id\":\"123\"},\"clientId\":\"abc\",\"sessionId\":\"xyz\"}",
      "seq_num": 0
    },
    {
      "body": "{\"seqNum\":1,\"parentSeqNum\":0,\"name\":\"todoCompleted\",\"args\":{\"id\":\"123\"},\"clientId\":\"abc\",\"sessionId\":\"xyz\"}",
      "seq_num": 1
    }
  ],
  "tail": {
    "seq_num": 1
  }
}
```

**Processing**:
1. Validate schema: `ReadBatchSchema`
2. Filter records with undefined body
3. Parse JSON body → `LiveStoreEvent.AnyEncodedGlobal`
4. Attach metadata: `{ s2SeqNum: record.seq_num }`

**Output**:
```typescript
[
  {
    eventEncoded: { seqNum: 0, parentSeqNum: -1, name: "todoCreated", ... },
    metadata: Option.some({ s2SeqNum: 0 })
  },
  {
    eventEncoded: { seqNum: 1, parentSeqNum: 0, name: "todoCompleted", ... },
    metadata: Option.some({ s2SeqNum: 1 })
  }
]
```

### 8. Stream Name Generation

**File**: `packages/@livestore/sync-s2/src/make-s2-url.ts:5`

```typescript
makeS2StreamName("my-app:user@example.com")
// Step 1: Replace non-alphanumeric/dash/underscore with dash
// → "my-app-user-example-com"
// Step 2: Truncate to 100 chars
// → "my-app-user-example-com" (already < 100)
```

Simple, deterministic mapping. No hashing (unlike Electric's table names).

### 9. Provisioning

**Basin creation** (once per deployment):

```typescript
POST https://aws.s2.dev/v1/basins
Authorization: Bearer {token}
Content-Type: application/json

{ "basin": "my-basin" }
```

**Stream creation** (once per storeId):

```typescript
POST https://{basin}.b.aws.s2.dev/v1/streams
Authorization: Bearer {token}
Content-Type: application/json

{ "stream": "my-stream" }
```

Both return 409 if already exists (treated as success).

## Complete Request Flow Examples

### Example 1: Initial Pull (Empty Stream)

```
1. Client: pull(None, { live: false })
2. Provider: Encode args { storeId: "myStore", s2SeqNum: "from-start", live: false }
3. Provider: GET /api/s2?args={...}, headers: { accept: text/event-stream }
4. Proxy: Decode args, s2SeqNum="from-start" → seq_num=0
5. Proxy: Create stream "myStore" in S2 (if not exists)
6. Proxy: GET https://{basin}.b.aws.s2.dev/v1/streams/myStore/records?seq_num=0&wait=0&clamp=true
7. S2: Query stream from position 0, no records
8. S2: Return SSE:
   event: batch
   data: {"records":[],"tail":{"seq_num":0}}

   event: message
   data: [DONE]
9. Provider: Decode batch, empty records
10. Provider: Emit { batch: [], pageInfo: { hasMore: false } }
11. Provider: Receive [DONE], stop stream
```

### Example 2: Pull with Existing Data

```
1. Client: pull(None, { live: false })
2. Provider: GET with seq_num=0, wait=0
3. S2: Query stream, find 50 records (seq_num 0-49)
4. S2: Return SSE:
   event: batch
   data: {"records":[...50 events...],"tail":{"seq_num":99}}

   event: message
   data: [DONE]
5. Provider: Decode 50 events
6. Provider: Compute remaining = 99 - 49 - 1 = 49
7. Provider: Emit { batch: [50 events], pageInfo: { hasMore: true, remaining: 49 } }
8. Provider: Receive [DONE], stop stream
```

Note: In non-live mode, S2 returns all available data in one or more batches, then sends [DONE].

### Example 3: Push Events (Small Batch)

```
1. Client: push([ev0, ev1, ev2])
2. Provider: Call chunkEventsForS2([ev0, ev1, ev2])
3. Provider: Pre-compute metered bytes:
   - ev0: 250 bytes
   - ev1: 300 bytes
   - ev2: 280 bytes
   - Total: 830 bytes (< 1 MiB, < 1000 records)
4. Provider: Single chunk: { events: [ev0, ev1, ev2], records: [...], meteredBytes: 830 }
5. Provider: POST /api/s2, body: { storeId: "myStore", batch: [ev0, ev1, ev2] }
6. Proxy: Create stream if needed
7. Proxy: Build S2 records: [{ body: JSON.stringify(ev0) }, ...]
8. Proxy: POST https://{basin}.b.aws.s2.dev/v1/streams/myStore/records
9. S2: Validate limits (✓), assign seq_nums (0, 1, 2)
10. S2: Persist records, broadcast to subscribers
11. Proxy: Return { success: true }
12. Provider: Return void (success)
```

### Example 4: Push Events (Large Batch with Chunking)

```
1. Client: push([...2000 events])
2. Provider: Call chunkEventsForS2(events)
3. Provider: Pre-compute metered bytes for each event
4. Provider: Split into chunks:
   - Chunk 0: events 0-999 (1000 records, 950 KiB)
   - Chunk 1: events 1000-1999 (1000 records, 980 KiB)
5. Provider: For chunk 0:
   - POST /api/s2, body: { storeId: "myStore", batch: [events 0-999] }
   - Proxy → S2 append
   - S2 assigns seq_nums 0-999
   - Return success
6. Provider: For chunk 1:
   - POST /api/s2, body: { storeId: "myStore", batch: [events 1000-1999] }
   - Proxy → S2 append
   - S2 assigns seq_nums 1000-1999
   - Return success
7. Provider: Return void (all chunks succeeded)
```

### Example 5: Live Pull (Real-time)

```
1. Client: pull(None, { live: true })
2. Provider: Call ssePull(None)
3. Provider: Initial pull with live=false (fast catchup):
   - GET with seq_num=0, wait=0
   - S2 returns historical data (events 0-99)
   - Provider emits batch
4. Provider: Stream ends, trigger reconnection with live=true:
   - Compute cursor from last event: s2SeqNum=99
   - GET with seq_num=100, no wait param (live mode)
   - S2 keeps connection open, waits for new events
5. [User pushes new event from another client]
6. S2: Detect new record (seq_num=100), stream via SSE:
   event: batch
   data: {"records":[{seq_num:100,body:"..."}],"tail":{"seq_num":100}}
7. Provider: Decode event, emit batch
8. Provider: Continue polling (infinite loop)
9. [Connection drops]
10. Provider: Detect stream end, auto-reconnect:
    - Cursor: s2SeqNum=100
    - GET with seq_num=101, live mode
    - Resume seamlessly
```

## Key Design Patterns

### 1. Dual Sequence Numbers
- S2 seq_num: Physical stream position (0, 1, 2, ...)
- LiveStore seqNum: Logical event order (in payload)
- Decoupled for flexibility (compaction, filtering)

### 2. SSE Streaming
- True streaming (not polling)
- Server pushes data as available
- Efficient for real-time updates
- Standard protocol (browser-native)

### 3. Pre-chunking
- Compute limits before sending
- Avoid 413 errors at runtime
- Transparent to client
- Efficient (stringify once)

### 4. Auto-reconnection
- Initial: Fast (wait=0)
- Subsequent: Live (wait for data)
- Infinite loop (never give up)
- Cursor-based (resume from last seen)

### 5. Stateless Proxy
- No server-side state
- Stream lifecycle in S2
- Simple HTTP handlers
- Easy to scale

## Performance Characteristics

### Latency
- **Pull (cold)**: 50-200ms (S2 query)
- **Pull (live)**: Instant when data available (SSE push)
- **Push (small)**: 20-100ms (single S2 append)
- **Push (large)**: 100-500ms (multiple chunks, sequential)
- **Propagation**: 10-100ms (SSE broadcast)

### Throughput
- **Limited by**: S2 service limits
- **Pull**: High (S2 optimized streaming)
- **Push**: Medium (chunked, sequential appends)

### Scalability
- **Horizontal**: Scale proxy servers (stateless)
- **Vertical**: S2 handles high throughput
- **Streaming**: Thousands of concurrent SSE connections

## Comparison Summary

### vs ElectricSQL

| Aspect | S2 | ElectricSQL |
|--------|-----|-------------|
| **Protocol** | SSE (true streaming) | Long-polling (simulated) |
| **Seq num** | Dual (s2SeqNum + seqNum) | Single (PG column) |
| **Latency** | Lower (SSE push) | Higher (poll interval) |
| **Querying** | None (stream only) | Full SQL |
| **Setup** | Managed service | Self-host Postgres + Electric |

### vs Cloudflare

| Aspect | S2 | Cloudflare |
|--------|-----|------------|
| **Architecture** | Client adapter | Full server |
| **Real-time** | SSE streaming | WebSocket |
| **Deployment** | Proxy + S2 account | Serverless |
| **Limits** | 1 MiB record/batch | DO limits |
| **Consistency** | Eventual (small lag) | Immediate (DO serialization) |

## Summary

The S2 sync provider is a **thin client adapter** that:

1. **Reads** via S2's SSE streaming (true real-time, efficient)
2. **Writes** via S2's append API (with transparent chunking)
3. **Separates** physical positions (s2SeqNum) from logical order (seqNum)
4. **Auto-reconnects** for live pulls (fast catchup → live mode)
5. **Pre-chunks** pushes to respect S2 limits
6. **Leverages** S2's managed infrastructure (minimal ops)

It's ideal for applications that:
- Want true streaming (not polling)
- Need managed infrastructure
- Can accept sequential access (no SQL)
- Value simplicity and predictable costs
- Require real-time updates with low latency

The key innovation is **smart reconnection**: initial pulls are fast (wait=0) to catch up on historical data, then switch to live mode (wait for new events) for real-time updates. Combined with automatic reconnection on disconnect, this provides a robust, efficient real-time sync experience.
