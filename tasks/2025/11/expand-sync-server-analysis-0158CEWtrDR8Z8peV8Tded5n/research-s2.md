# S2 Sync Provider Research

## Overview

The S2 sync provider (`@livestore/sync-s2`) is a **client-side adapter** that integrates LiveStore with S2's stream-based event store. S2 (pronounced "S-two") is a managed service for storing and streaming ordered records. Like the ElectricSQL provider, this is a client adapter rather than a full sync server.

**Architecture**: Client → API Proxy → S2 Service

## Key Components

### 1. Client Implementation (`sync-provider.ts`)

The main sync backend implementation that:
- Implements the `SyncBackend` interface required by LiveStore
- Uses Server-Sent Events (SSE) for pull operations (both live and non-live)
- Uses HTTP POST for push operations with automatic chunking
- Manages connection state and ping/pong
- Handles S2-specific response formats and limits

**Main function**: `makeSyncBackend(options)`

### 2. Type Definitions (`types.ts`)

Defines S2-specific types:
- `S2SeqNum`: Branded type for S2's sequence numbers (separate from LiveStore's seqNum)
- `SyncMetadata`: Contains `s2SeqNum` for cursor management

**Critical insight**: S2's seq_num is **independent** from LiveStore's seqNum:
- S2 seq_num: Physical stream position (0-indexed, assigned by S2)
- LiveStore seqNum: Logical event ordering (in event payload)
- They are decoupled to support future optimizations like compaction

### 3. API Schema (`api-schema.ts`)

Defines the message schemas:
- `PullArgs`: Contains storeId, s2SeqNum (or 'from-start'), live flag, payload
- `PushPayload`: Contains storeId and batch of events
- `PushResponse`: Simple success indicator
- `ArgsSchema`: URL-encoded query parameter schema

### 4. Proxy Helpers (`s2-proxy-helpers.ts`)

Comprehensive helpers for building S2 API requests:
- URL construction (basin URLs, stream URLs, account URLs)
- Header builders (auth, SSE, push)
- Request builders (pull, push)
- Response helpers (empty batch, SSE keepalive, success, error)
- Basin/stream provisioning (ensureBasin, ensureStream)

### 5. Limits & Chunking (`limits.ts`)

S2 enforces strict limits on records and batches:
- **MAX_RECORD_METERED_BYTES**: 1 MiB per record
- **MAX_BATCH_METERED_BYTES**: 1 MiB per batch
- **MAX_RECORDS_PER_BATCH**: 1,000 records

The `chunkEventsForS2()` function:
- Pre-computes metered bytes for each event
- Splits batches to respect S2 limits
- Throws `S2LimitExceededError` if single event exceeds limits
- Uses Effect's `splitChunkBySize` for efficient chunking

### 6. Decoding (`decode.ts`)

Decodes S2 read responses:
- Parses S2 record format (body + seq_num)
- Extracts LiveStore events from JSON-encoded bodies
- Attaches S2 metadata (seq_num) to each event
- Filters out records with undefined bodies

### 7. Stream Name Generation (`make-s2-url.ts`)

Converts storeId to S2 stream name:
- Replaces non-alphanumeric/dash/underscore with dashes
- Truncates to 100 chars (S2 stream name limit)
- Simple, deterministic mapping

### 8. Generated HTTP Client (`http-client-generated.ts`)

Auto-generated TypeScript client for S2 API:
- Type-safe methods for all S2 operations
- Basin operations: createBasin, deleteBasin
- Stream operations: createStream, deleteStream, append, check
- Read operations: SSE streaming

## File Structure

```
packages/@livestore/sync-s2/
├── src/
│   ├── sync-provider.ts          # Main sync backend implementation
│   ├── types.ts                  # S2-specific type definitions
│   ├── api-schema.ts             # Message type definitions
│   ├── s2-proxy-helpers.ts       # Proxy utilities
│   ├── limits.ts                 # S2 limits and chunking
│   ├── decode.ts                 # Response decoding
│   ├── make-s2-url.ts            # Stream name generation
│   ├── http-client-generated.ts  # Auto-generated S2 client
│   └── mod.ts                    # Package exports
```

## Key Concepts

### S2 Stream Model

**S2** is a managed event streaming service with these primitives:

- **Basin**: Top-level container (like a database)
- **Stream**: Ordered sequence of records (like a table)
- **Record**: Individual entry with body (string) and headers (key-value pairs)
- **seq_num**: 0-indexed position in stream (assigned by S2)

### LiveStore → S2 Mapping

- **storeId** → S2 stream name (sanitized, max 100 chars)
- **LiveStore event** → S2 record body (JSON-encoded string)
- **Sequence numbers** → INDEPENDENT systems:
  - LiveStore seqNum: In event payload, tracks logical order
  - S2 seq_num: Stream position, tracks physical order
- **Pull cursor** → S2 seq_num (from metadata)
- **Push** → S2 append records

### Stream Naming

Streams are named by sanitizing storeId:
```typescript
makeS2StreamName("my-app:user123")
// → "my-app-user123"

makeS2StreamName("my_app/user@example.com")
// → "my_app-user-example-com"
```

Rules:
- Replace non-alphanumeric/dash/underscore with dash
- Truncate to 100 chars
- Result must be valid S2 stream name

### S2 Sequence Numbers

**Critical**: S2's seq_num is separate from LiveStore's seqNum:

```typescript
// S2 stream:
{ seq_num: 0, body: '{"seqNum":5,"parentSeqNum":4,...}' }
{ seq_num: 1, body: '{"seqNum":6,"parentSeqNum":5,...}' }
{ seq_num: 2, body: '{"seqNum":7,"parentSeqNum":6,...}' }

// seq_num tracks position in S2 stream (0, 1, 2)
// seqNum tracks LiveStore event order (5, 6, 7)
```

This separation enables:
- Future compaction (remove old events, renumber S2 positions)
- Multiple stores in one stream (with filtering)
- Independent evolution of storage and application logic

### SSE Protocol

S2 uses Server-Sent Events for reads:

**Format**:
```
event: batch
data: {"records":[...],"tail":{"seq_num":42}}

event: batch
data: {"records":[...],"tail":{"seq_num":50}}

event: ping
data: {}

event: message
data: [DONE]
```

**Event types**:
- `batch`: Contains records and optional tail position
- `ping`: Keepalive (ignored by provider)
- `message: [DONE]`: End of stream (for non-live pulls)
- `error`: Error message (converted to InvalidPullError)

### S2 Limits

S2 enforces limits on append operations:

**Metered bytes calculation**:
```
record_size = 8 + (2 * header_count) + sum(utf8_bytes(header.name + header.value)) + utf8_bytes(body)
```

**Limits**:
- Single record: ≤ 1 MiB metered bytes
- Batch total: ≤ 1 MiB metered bytes
- Batch count: ≤ 1,000 records

If exceeded, S2 returns **413 Payload Too Large**.

The provider **pre-chunks** batches to avoid 413 errors.

### Cursor Management

Pull cursors track S2 position:

**Format**:
```typescript
{
  eventSequenceNumber: 42,  // LiveStore seqNum (for LiveStore's use)
  metadata: {
    s2SeqNum: 10            // S2 position (for S2's use)
  }
}
```

**Cursor → seq_num conversion**:
- `None` → seq_num = 0 (start from beginning)
- `Some({ s2SeqNum: 10 })` → seq_num = 11 (next record after cursor)
- Note: cursor points to **last seen** record, seq_num is **next to read**

### Pull Modes

**Non-live pull**:
- Sets `wait=0` in S2 request
- S2 returns immediately when tail is reached
- SSE ends with `[DONE]` message
- Provider stops stream

**Live pull**:
- Omits `wait` param (defaults to S2's wait time)
- S2 keeps connection open
- New records arrive via SSE as they're appended
- Provider reconnects on disconnect (auto-resume from last position)

### S2 Configuration

The `S2Config` type:
```typescript
{
  basin: string,              // Basin name
  token: string,              // S2 access token
  accountBase?: string,       // Account API base URL (default: https://aws.s2.dev/v1)
  basinBase?: string,         // Basin API base URL (default: https://{basin}.b.aws.s2.dev/v1)
}
```

### Request Headers

**For SSE reads**:
```
Authorization: Bearer {token}
accept: text/event-stream
s2-format: raw
```

**For appends**:
```
Authorization: Bearer {token}
content-type: application/json
s2-format: raw
```

The `s2-format: raw` header tells S2 to use raw string bodies instead of base64 encoding.

## Pull Operation Flow

1. **Encode pull args** with storeId, s2SeqNum, live, payload
2. **Send GET request** to API proxy with args as query param
3. **Proxy ensures stream exists** (create if needed)
4. **Proxy builds S2 URL**:
   - Convert cursor to seq_num (last seen + 1, or 0 for start)
   - Add `wait=0` for non-live, omit for live
   - Add `clamp=true` to respect count limits
5. **Proxy forwards to S2** with SSE headers
6. **S2 streams records**:
   - Query stream from seq_num onward
   - Return records as SSE batches
   - Include tail position in each batch
   - Send ping events for keepalive
   - Send [DONE] when no more data (non-live only)
7. **Provider decodes SSE stream**:
   - Filter out ping events
   - Map error events to InvalidPullError
   - Decode batch events (parse JSON, extract records)
   - Extract LiveStore events from record bodies
   - Attach S2 metadata (seq_num) to each event
8. **Provider computes page info**:
   - Calculate remaining = tail.seq_num - last.s2SeqNum - 1
   - If remaining > 0: hasMore with known count
   - Else: no more
9. **Provider emits batches** to stream
10. **For live pulls**: Auto-reconnect on stream end

## Push Operation Flow

1. **Client calls push(batch)**
2. **Provider chunks events** for S2 limits:
   - Pre-compute metered bytes for each event
   - Split into chunks ≤ 1 MiB, ≤ 1000 records
   - Throw if single event exceeds 1 MiB
3. **For each chunk**:
   - Build POST request to API proxy
   - Body: `{ storeId, batch: chunkEvents }`
4. **Proxy ensures stream exists** (create if needed)
5. **Proxy builds S2 append requests**:
   - Convert events to S2 records (JSON-encode as body)
   - Call S2 append endpoint
6. **S2 appends records**:
   - Validate limits
   - Assign seq_nums (sequential)
   - Persist to stream
   - Return success
7. **S2 broadcasts to subscribers** (via SSE)
8. **Proxy returns** `{ success: true }`
9. **Provider continues** to next chunk if any
10. **Handle errors**:
    - Retry on transient failures (2 retries, 100ms spacing)
    - Convert to InvalidPushError on permanent failure

## Retry Logic

**Pull retries** (non-live only):
- Schedule: 2 retries, 100ms spacing
- Applied per-batch, not entire stream
- Retries transient network errors

**Push retries**:
- Schedule: 2 retries, 100ms spacing (configurable)
- Applied per-chunk
- Exponential backoff for provisioning operations (10s max)

**Live pull reconnection**:
- Automatic via `concatWithLastElement`
- Resumes from last seen record
- Infinite reconnection loop
- Initial pull is non-live (wait=0), subsequent pulls are live

## Provisioning

The API proxy handles basin/stream lifecycle:

**Basin creation** (once per test/deployment):
```typescript
POST https://aws.s2.dev/v1/basins
{ "basin": "my-basin" }
```

**Stream creation** (once per storeId):
```typescript
POST https://{basin}.b.aws.s2.dev/v1/streams
{ "stream": "my-stream" }
```

Both operations are idempotent (409 if exists, treated as success).

## Testing Architecture

The test provider (`tests/sync-provider/src/providers/s2.ts`):
1. Creates unique basin per test run
2. Starts HTTP API proxy server
3. Routes pull → S2 SSE read (with stream provisioning)
4. Routes push → S2 append (with stream provisioning)
5. Provides test helpers (appendRaw, failNextAppend, failNextRead)
6. Cleans up basin on test completion (optional)

## Error Handling

### InvalidPullError

Wraps pull failures:
- SSE error events
- HTTP errors (non-2xx)
- Decoding errors

### InvalidPushError

Wraps push failures:
- HTTP errors
- S2 limit exceeded errors (with detailed info)
- Unexpected errors

### S2LimitExceededError

Specific error for S2 limit violations:
- `limitType`: 'record-metered-bytes' | 'batch-metered-bytes' | 'batch-count'
- `max`: Limit value
- `actual`: Actual value
- `recordIndex`: Which record exceeded (if applicable)

## Configuration

```typescript
{
  endpoint: string | { push, pull, ping },
  ping?: {
    enabled?: boolean,           // default: true
    requestTimeout?: Duration,   // default: 10s
    requestInterval?: Duration,  // default: 10s
  },
  retry?: {
    pull?: Schedule,             // default: 2 recurs, 100ms spaced
    push?: Schedule,             // default: 2 recurs, 100ms spaced
  }
}
```

## Metadata

Each event carries metadata:
```typescript
{
  s2SeqNum: number  // S2 stream position (branded type)
}
```

This metadata is used for cursor management in subsequent pulls.

## Limitations

1. **Requires external infrastructure**: S2 managed service
2. **Stream name limits**: Max 100 chars, limited character set
3. **Record size limits**: 1 MiB per record, 1 MiB per batch
4. **No built-in querying**: Must read stream sequentially
5. **Eventual consistency**: Small lag between append and read availability

## Comparison to Cloudflare and ElectricSQL

| Feature | S2 | ElectricSQL | Cloudflare |
|---------|-----|-------------|------------|
| Architecture | Client adapter | Client adapter | Full server |
| Storage | S2 managed stream | PostgreSQL | DO SQLite/D1 |
| Push protocol | HTTP POST + chunking | HTTP POST | HTTP/WebSocket/RPC |
| Pull protocol | SSE streaming | HTTP long-polling | HTTP/WebSocket/RPC |
| Real-time | SSE (true streaming) | Long-polling (simulated) | WebSocket (true streaming) |
| Limits | 1 MiB record/batch | Postgres limits | DO limits |
| Provisioning | Auto (in proxy) | Manual (DB/Electric) | Auto (DO) |
| Deployment | Proxy + S2 account | Postgres + Electric | Serverless |
| Querying | None (stream only) | Full SQL | Limited |
| Cost model | S2 usage + proxy | Infrastructure | Per-request |
| Seq num separation | Yes (independent) | No (same as PG column) | No (same as event) |

## Advanced Features

### Page Info with Known Remaining

S2 returns tail position in each batch:
```json
{
  "records": [...],
  "tail": { "seq_num": 100 }
}
```

The provider computes:
```typescript
remaining = tail.seq_num - last_record.seq_num - 1
```

This enables accurate progress bars and "N more events" UI.

### Auto-reconnection for Live Pulls

The `ssePull` function uses `concatWithLastElement`:
- Initial pull with `live=false` (gets historical data fast)
- On stream end, switch to `live=true` (wait for new data)
- On disconnect, auto-resume from last seen record
- Infinite loop ensures continuous connection

### Test-only Helpers

The test proxy provides debugging helpers:
- `appendRaw`: Append arbitrary JSON strings
- `failNextAppend`: Inject append failures
- `failNextRead`: Inject read failures
- `testCloseOnce`: Test reconnection logic

### Metadata in Client Payload

The `payload` field in PullArgs allows custom client data:
```typescript
pull({ payload: { userId: "123", authToken: "xyz" } })
```

The proxy can:
- Validate auth tokens
- Apply per-user rate limiting
- Customize S2 stream selection
- Log user actions

## Summary

The S2 sync provider is a **thin client adapter** that:

1. **Reads** via S2's SSE streaming (true real-time, efficient)
2. **Writes** via S2's append API (with automatic chunking)
3. **Relies** on S2 managed service (minimal ops)
4. **Separates** storage positions (s2SeqNum) from logical order (seqNum)
5. **Supports** live pulls via SSE streaming
6. **Handles** S2 limits transparently (pre-chunking)

It's ideal for applications that:
- Want managed infrastructure (no DB/server to run)
- Need true streaming (not polling)
- Can work with sequential access (no SQL querying)
- Want predictable costs (S2 pricing)
- Value simplicity over queryability

The key innovation is **dual sequence numbers**: S2's physical positions are separate from LiveStore's logical ordering, enabling future optimizations like compaction while maintaining application correctness.
