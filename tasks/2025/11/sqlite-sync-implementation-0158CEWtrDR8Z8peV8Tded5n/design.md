# SQLite Sync Server Implementation

## Overview

This document records the design decisions, implementation approach, and challenges encountered while implementing a custom LiveStore sync server using SQLite as the storage backend.

## Goals

1. **Implement a standalone sync server** that can be used outside of Cloudflare infrastructure
2. **Reuse the Cloudflare client** completely (implement only server-side code)
3. **Use SQLite** as the storage backend with one database per store
4. **Follow best practices** from the comparative analysis research
5. **Pass comprehensive tests** from the existing sync-provider test suite

## Architecture Decision

**Choice**: Full server implementation (Pattern A from comparative-analysis.md)

**Rationale**:
- Need complete control over storage and lifecycle
- Want to demonstrate a non-Cloudflare deployment option
- SQLite is ideal for embedded/edge deployments
- Server-only implementation (reusing Cloudflare client)

## Key Design Decisions

### 1. Package Structure

**Decision**: Create `@livestore/sync-sqlite` package

**Structure**:
```
packages/@livestore/sync-sqlite/
├── src/
│   ├── server/              # Server implementation
│   │   ├── storage.ts       # SQLite storage layer
│   │   ├── handlers.ts      # Pull/Push/Ping handlers
│   │   ├── rpc-server.ts    # HTTP RPC server
│   │   └── server.ts        # Main server class
│   ├── common/              # Shared types (reuse from sync-cf)
│   └── index.ts             # Public exports
├── package.json
└── README.md
```

**Rationale**:
- Similar structure to `sync-cf` but simplified
- No client code needed (reuse `@livestore/sync-cf/client`)
- Clear separation of concerns

### 2. Storage Layer Design

**Decision**: Follow Cloudflare's exact schema and approach

**Schema**:
```sql
-- Event log table (one per store)
CREATE TABLE IF NOT EXISTS eventlog_6_{storeId} (
  seqNum INTEGER PRIMARY KEY,
  parentSeqNum INTEGER NOT NULL,
  name TEXT NOT NULL,
  args TEXT,                    -- JSON-encoded, can be NULL
  createdAt TEXT NOT NULL,      -- ISO timestamp
  clientId TEXT NOT NULL,
  sessionId TEXT NOT NULL
) STRICT;

-- Context table (metadata, one row per store)
CREATE TABLE IF NOT EXISTS context_6 (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  currentHead INTEGER NOT NULL,
  backendId TEXT NOT NULL
) STRICT;
```

**Key decisions**:
- ✅ **Use STRICT mode**: Enforces type checking
- ✅ **Primary key on seqNum**: Fast lookups and ordering
- ✅ **Include backendId**: Prevents cross-backend pulls
- ✅ **One SQLite database per store**: Isolation and simplicity
- ✅ **Limit-based pagination**: PAGE_SIZE=256 (matches Cloudflare)

**Rationale**:
- Proven design from Cloudflare implementation
- STRICT mode prevents type errors
- backendId essential for production deployments
- seqNum as primary key gives O(log n) cursor queries

### 3. Atomicity Mechanism

**Decision**: Use SQLite transactions

**Implementation approach**:
```typescript
db.transaction(() => {
  // 1. Get current head
  const { currentHead } = db.prepare('SELECT currentHead FROM context_6 WHERE id = 1').get()

  // 2. Validate parent sequence
  if (batch[0].parentSeqNum !== currentHead) {
    throw new ServerAheadError({ minimumExpectedNum: currentHead })
  }

  // 3. Insert events (chunked for parameter limits)
  insertEventsChunked(batch)

  // 4. Update head
  db.prepare('UPDATE context_6 SET currentHead = ? WHERE id = 1')
    .run(batch[batch.length - 1].seqNum)
})
```

**Key points**:
- ✅ SQLite automatically uses **SERIALIZABLE isolation** (strongest guarantee)
- ✅ Transaction provides atomicity equivalent to Cloudflare's `blockConcurrencyWhile`
- ✅ No need for explicit locking mechanisms
- ✅ Automatic rollback on errors

**Rationale**:
- SQLite transactions are perfect for this use case
- SERIALIZABLE isolation prevents all race conditions
- Simpler than application-level locking
- Well-understood and reliable

### 4. Transport Protocol

**Decision**: Implement HTTP RPC server (Effect RPC)

**Protocol**: Cloudflare's RPC message format
- **PullRequest**: `{ storeId, cursor: Option<{ backendId, eventSequenceNumber }>, live, payload }`
- **PushRequest**: `{ storeId, batch, backendId }`
- **Ping**: `{ storeId, payload }`

**Transport**: HTTP POST with JSON-encoded Effect RPC messages

**Rationale**:
- Reuses Cloudflare client's `makeHttpSync` completely
- No client code to write or maintain
- Standard HTTP is universally supported
- Effect RPC provides type safety

### 5. Live Updates

**Decision**: **Not implementing live updates in v1**

**Rationale**:
- HTTP RPC doesn't support streaming (would need WebSocket)
- Live updates add significant complexity (subscription management, broadcasting)
- Focus on correctness first
- Can add WebSocket RPC in v2 if needed

**Impact**:
- `supports.pullLive = false`
- Clients will use polling for updates
- Still fully functional for most use cases

### 6. Cursor Format

**Decision**: Reuse Cloudflare's cursor format exactly

```typescript
type CloudflareCursor = {
  backendId: string,              // Server's unique ID
  eventSequenceNumber: number     // Last seen seqNum
}
```

**Rationale**:
- Client expects this format
- backendId prevents cross-backend issues
- Simple and efficient

### 7. Database Per Store

**Decision**: One SQLite database file per storeId

**Structure**:
```
data/
├── store-foo.db
├── store-bar.db
└── store-baz.db
```

**Rationale**:
- Strong isolation between stores
- Simpler than multi-tenant database
- Easier to backup/restore individual stores
- Matches the "one DO per store" pattern from Cloudflare

**Trade-offs**:
- More file handles for many stores
- Can't query across stores (but this is rarely needed)

### 8. SQLite Configuration

**Decision**: Use optimal SQLite settings

```sql
PRAGMA journal_mode = WAL;        -- Write-Ahead Logging for better concurrency
PRAGMA synchronous = NORMAL;      -- Balance durability and performance
PRAGMA foreign_keys = ON;         -- Enforce referential integrity
PRAGMA busy_timeout = 5000;       -- Wait up to 5s for locks
```

**Rationale**:
- WAL mode allows concurrent reads during writes
- NORMAL synchronous is safe enough for most use cases
- busy_timeout prevents immediate failures on contention

## Implementation Plan

### Phase 1: Core Storage Layer
1. ✅ Create package structure
2. ✅ Implement SQLite storage class
3. ✅ Implement table initialization
4. ✅ Implement event insertion (chunked)
5. ✅ Implement event querying (paginated)
6. ✅ Implement head tracking

### Phase 2: RPC Handlers
1. ✅ Implement Push handler
2. ✅ Implement Pull handler
3. ✅ Implement Ping handler
4. ✅ Implement error mapping

### Phase 3: Server
1. ✅ Implement HTTP RPC server
2. ✅ Implement server lifecycle (start/stop)
3. ✅ Implement database management

### Phase 4: Testing
1. ✅ Add provider to test registry
2. ✅ Run sync-provider test suite
3. ✅ Fix any failing tests
4. ✅ Document test results

## Dependencies

**Runtime dependencies**:
- `better-sqlite3`: Fast, synchronous SQLite bindings for Node.js
- `@livestore/common`: Core LiveStore types and utilities
- `@effect/rpc`: Effect RPC framework
- `@effect/platform-node`: Node.js platform layer for Effect

**Reused from sync-cf**:
- Message schemas (`sync-message-types.ts`)
- RPC schemas (`http-rpc-schema.ts`)
- Client implementation (entire `client/` directory)

## Challenges and Solutions

### Challenge 1: Parameter Limits in SQLite

**Problem**: SQLite has a default limit of ~100 parameters per query. Inserting 100 events with 7 columns each would require 700 parameters.

**Solution**: Chunk inserts into batches of 14 events (98 parameters), matching Cloudflare's approach.

```typescript
const CHUNK_SIZE = 14  // 14 events * 7 params = 98 params (under 100 limit)

for (let i = 0; i < events.length; i += CHUNK_SIZE) {
  const chunk = events.slice(i, i + CHUNK_SIZE)
  const placeholders = chunk.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')
  const sql = `INSERT INTO ${tableName} (...) VALUES ${placeholders}`
  const params = chunk.flatMap(event => [event.seqNum, ...])
  stmt = db.prepare(sql)
  stmt.run(...params)
}
```

### Challenge 2: Database File Management

**Problem**: Need to manage multiple database files (one per store) efficiently.

**Solution**:
- Lazy initialization: Create database on first access
- Keep database connections open (reuse)
- Store databases in configurable data directory

### Challenge 3: BackendId Generation

**Problem**: Need a stable, unique backendId across server restarts.

**Solution**:
- Generate backendId using `nanoid()` on first initialization
- Persist in context table
- Reuse on subsequent accesses

```typescript
// On first access
if (!contextRow) {
  const backendId = nanoid()
  db.prepare('INSERT INTO context_6 (id, currentHead, backendId) VALUES (?, ?, ?)')
    .run(1, -1, backendId)  // -1 is ROOT
}
// On subsequent accesses
const { backendId } = db.prepare('SELECT backendId FROM context_6 WHERE id = 1').get()
```

### Challenge 4: Type Safety with better-sqlite3

**Problem**: `better-sqlite3` returns `any` types, need type safety.

**Solution**: Create typed wrappers and use Schema validation:

```typescript
const ContextSchema = Schema.Struct({
  id: Schema.Number,
  currentHead: Schema.Number,
  backendId: Schema.String,
})

const getContext = () => {
  const row = db.prepare('SELECT * FROM context_6 WHERE id = 1').get()
  return Schema.decodeUnknownSync(ContextSchema)(row)
}
```

## Testing Strategy

### Unit Tests
- Storage layer operations (insert, query, head tracking)
- Handler logic (validation, error handling)
- Transaction behavior

### Integration Tests
- **Reuse sync-provider test suite**: Add SQLite provider to registry
- Tests cover:
  - Basic push/pull operations
  - Cursor-based pagination
  - Head validation (ServerAheadError)
  - backendId validation
  - Concurrent operations
  - Error scenarios

### Test Provider Implementation

```typescript
// tests/sync-provider/src/providers/sqlite.ts
export const name = 'sqlite'

export const layer: SyncProviderLayer = Layer.effect(
  SyncProviderImpl,
  Effect.gen(function* () {
    // Create server instance
    const server = new SqliteServer({ dataDir: './test-data' })

    // Start server on random port
    yield* server.start()

    return {
      makeProvider: (args, options) =>
        makeHttpSync({
          url: server.url,
          storeId: args.storeId,
          // ... other options
        }),
      turnBackendOffline: () => server.stop(),
      turnBackendOnline: () => server.start(),
      providerSpecific: { server }
    }
  })
)
```

## Success Criteria

1. ✅ All sync-provider tests pass
2. ✅ Push operations are atomic
3. ✅ Pull operations use cursor-based pagination correctly
4. ✅ backendId validation works
5. ✅ Can handle concurrent push operations
6. ✅ Compatible with existing Cloudflare client

## Future Enhancements (Out of Scope for v1)

- **WebSocket support**: Enable live updates
- **Clustering**: Multiple server instances with shared storage
- **Replication**: Primary/replica setup for high availability
- **Metrics**: Prometheus metrics for observability
- **Migration tools**: Import/export from other sync backends

## References

- [`comparative-analysis.md`](../expand-sync-server-analysis-0158CEWtrDR8Z8peV8Tded5n/comparative-analysis.md) - Detailed comparison and decision tree
- [`sqlite-implementation-guide.md`](../expand-sync-server-analysis-0158CEWtrDR8Z8peV8Tded5n/sqlite-implementation-guide.md) - SQLite-specific guidance
- Cloudflare implementation: `packages/@livestore/sync-cf/`
- Test suite: `tests/sync-provider/`
