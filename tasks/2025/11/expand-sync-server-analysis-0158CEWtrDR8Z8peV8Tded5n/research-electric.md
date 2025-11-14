# ElectricSQL Sync Provider Research

## Overview

The ElectricSQL sync provider (`@livestore/sync-electric`) is a **client-side adapter** that integrates LiveStore with ElectricSQL's shape-based replication system. Unlike the Cloudflare implementation which is a full sync server, the Electric provider is a client that talks to an external ElectricSQL server.

**Architecture**: Client → API Proxy → ElectricSQL Server → PostgreSQL

## Key Components

### 1. Client Implementation (`index.ts`)

The main sync backend implementation that:
- Implements the `SyncBackend` interface required by LiveStore
- Uses HTTP long-polling for pull operations
- Uses HTTP POST for push operations
- Manages connection state and ping/pong
- Handles ElectricSQL-specific response formats

**Main function**: `makeSyncBackend(options)`

### 2. API Schema (`api-schema.ts`)

Defines the message schemas for communication:
- `PullPayload`: Contains storeId, payload, handle (ElectricSQL cursor), and live flag
- `PushPayload`: Contains storeId and batch of events
- `ArgsSchema`: URL-encoded query parameter schema

### 3. URL Builder (`make-electric-url.ts`)

Helper for building ElectricSQL API URLs:
- Converts storeId to PostgreSQL table name
- Manages Electric handles and offsets
- Supports Electric Cloud (sourceId/sourceSecret) and self-hosted (apiSecret)
- Includes persistence format version for schema migrations

### 4. Response Schemas

ElectricSQL returns data in a specific format:
- `ResponseItemInsert`: Valid insert operations
- `ResponseItemInvalid`: Invalid update/delete operations (not supported)
- `ResponseItemControl`: Control messages (up-to-date, etc.)

## File Structure

```
packages/@livestore/sync-electric/
├── src/
│   ├── index.ts               # Main sync backend implementation
│   ├── api-schema.ts          # Message type definitions
│   └── make-electric-url.ts   # URL construction helpers
```

## Key Concepts

### ElectricSQL Integration

**ElectricSQL** is a local-first sync engine that replicates PostgreSQL data to clients using "shapes" (declarative queries). Key concepts:

- **Shape**: A declarative query that defines what data to sync
- **Handle**: Unique identifier for a shape instance
- **Offset**: Position in the replication stream (LSN-based)
- **Long-polling**: Electric uses 20-second long-polls for real-time updates

### LiveStore → ElectricSQL Mapping

- **storeId** → PostgreSQL table name (sanitized, versioned)
- **LiveStore event** → PostgreSQL row
- **Sequence numbers** → `seqNum` column in PostgreSQL
- **Pull cursor** → Electric handle + offset
- **Push** → Direct PostgreSQL INSERT (via proxy)

### Table Naming

Tables are named: `eventlog_{VERSION}_{escapedStoreId}`

- Escaped: Replace non-alphanumeric with underscores
- Truncated to 63 chars (PostgreSQL limit)
- If too long, use hash: `eventlog_{VERSION}_hash_{hash}`

### Persistence Format Version

Current version: **6**

Schema:
- `seqNum` (INTEGER PRIMARY KEY)
- `parentSeqNum` (INTEGER)
- `name` (TEXT NOT NULL)
- `args` (JSONB NOT NULL)
- `clientId` (TEXT NOT NULL)
- `sessionId` (TEXT NOT NULL)

Incrementing this version creates new tables, making old data inaccessible (soft reset).

### Handle Management

Electric uses handles to track sync state:
- **No handle**: Initial sync from offset -1 (all data)
- **With handle**: Resume from specific offset
- **Handle mismatch (409)**: Shape changed, need full resync (not yet implemented)

### Response Format

ElectricSQL returns arrays of response items:

```json
[
  {
    "key": "\"public\".\"events_xyz\"/\"0\"",
    "value": {
      "seqNum": "0",
      "parentSeqNum": "-1",
      "name": "todoCreated",
      "args": "{\"id\": \"123\", \"text\": \"Hello\"}",
      "clientId": "S_YOa",
      "sessionId": "xyz"
    },
    "headers": {
      "operation": "insert",
      "relation": ["public", "events_xyz"]
    }
  },
  {
    "headers": {
      "control": "up-to-date"
    }
  }
]
```

Response headers include:
- `electric-handle`: Shape handle
- `electric-offset`: Current offset (e.g., "26799576_0")

### Status Codes

- **200**: Success with data
- **204**: No new data (long-poll timeout after ~20s)
- **400**: Table doesn't exist (treated as empty)
- **401**: Unauthorized
- **409**: Shape not found / handle mismatch

### Operation Validation

The provider validates that only INSERT operations occur:
- **update/delete**: Throw `InvalidOperationError`
- This enforces event sourcing immutability
- Users must append compensating events instead of mutating

## Pull Operation Flow

1. **Encode pull request** with storeId, handle, payload, live flag
2. **Send GET request** to API proxy with args as query param
3. **Proxy builds ElectricSQL URL** with table name, handle, offset
4. **Proxy creates table if needed** (first pull only)
5. **Proxy forwards request to Electric**
6. **Electric polls PostgreSQL** for changes
7. **Electric returns response** with items and new handle/offset
8. **Provider decodes items**:
   - Extract valid inserts
   - Reject updates/deletes
   - Extract control messages
9. **Provider maps to LiveStore events** with metadata (offset, handle)
10. **Emit batch** with pageInfo
11. **Continue pagination** using new handle

For live pulls, the stream unfolds indefinitely, using long-polling to get new events.

## Push Operation Flow

1. **Encode push request** with storeId and batch
2. **Send POST request** to API proxy
3. **Proxy creates table if needed**
4. **Proxy inserts events directly to PostgreSQL**
   - Uses batch INSERT for efficiency
   - Preserves all event fields (seqNum, parentSeqNum, etc.)
5. **ElectricSQL detects changes** (via logical replication)
6. **ElectricSQL propagates to subscribers**
7. **Return success response**

**Important**: Push bypasses ElectricSQL and writes directly to PostgreSQL. ElectricSQL then picks up the changes through PostgreSQL's logical replication and syncs to clients.

## Ping/Connect

- **Ping**: HEAD request to check server availability
- **Connect**: Same as ping for remote endpoints; skipped for same-origin
- **Auto-ping**: Every 10 seconds by default to keep connection alive
- **Timeout**: 10 seconds for ping responses
- Updates `isConnected` ref based on ping success/failure

## Error Handling

### InvalidPullError

Wraps pull failures:
- HTTP errors (non-2xx)
- Decoding errors
- Invalid operations (update/delete)

### InvalidPushError

Wraps push failures:
- HTTP errors
- Database errors

### InvalidOperationError

Specific error for update/delete operations:
- Explains that event log mutation is not allowed
- Instructs to append compensating events

## Configuration

```typescript
{
  endpoint: string | { push, pull, ping },
  ping?: {
    enabled?: boolean,           // default: true
    requestTimeout?: Duration,   // default: 10s
    requestInterval?: Duration,  // default: 10s
  }
}
```

## Metadata

Each event carries metadata:
```typescript
{
  offset: string,   // e.g., "26799576_0"
  handle: string,   // e.g., "2494_84241"
}
```

This metadata is used for cursor management in subsequent pulls.

## Testing Architecture

The test provider (`tests/sync-provider/src/providers/electric.ts`):
1. Starts PostgreSQL + ElectricSQL via Docker Compose
2. Creates HTTP API proxy server
3. Routes pull → ElectricSQL (GET)
4. Routes push → PostgreSQL (direct INSERT)
5. Routes ping → ElectricSQL health check

This mirrors the expected production architecture where applications write to PostgreSQL and read through ElectricSQL.

## Limitations

1. **No handle mismatch recovery**: 409 errors not yet handled
2. **No update/delete support**: Enforces append-only log
3. **Unknown remaining count**: Can't know total events until stream ends (Electric optimization)
4. **Requires external infrastructure**: PostgreSQL + ElectricSQL server
5. **Push latency**: Events go through PostgreSQL → Electric → clients (multi-hop)

## Comparison to Cloudflare Implementation

| Feature | ElectricSQL | Cloudflare |
|---------|------------|------------|
| Architecture | Client adapter | Full server |
| Storage | PostgreSQL (external) | DO SQLite/D1 |
| Push path | Client → Postgres → Electric → Clients | Client → DO → Clients |
| Pull protocol | HTTP long-polling | HTTP/WebSocket/DO RPC |
| Real-time | Via long-polling | Via WebSocket/RPC |
| Scalability | PostgreSQL + Electric | Durable Objects |
| Deployment | Requires infrastructure | Serverless |
| Querying | SQL queries on Postgres | Limited (storage-specific) |
| Live pulls | Yes (via long-polling) | Yes (via WebSocket) |
