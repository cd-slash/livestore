# SQLite Custom Sync Server Implementation Guide

## Your Decisions Answered

Based on the Cloudflare implementation, here are specific answers to your questions:

### 3.1: SQLite Data Store ✅

**Your choice**: SQLite with one database per store

**Answer**: Yes, you can absolutely reuse the Cloudflare schemas! Here's exactly what Cloudflare uses:

#### Event Log Table

```sql
CREATE TABLE IF NOT EXISTS "eventlog_6_mystore" (
  seqNum INTEGER PRIMARY KEY,
  parentSeqNum INTEGER,
  name TEXT,
  args TEXT,              -- JSON-encoded, can be NULL
  createdAt TEXT,         -- ISO date format (for debugging)
  clientId TEXT,
  sessionId TEXT
) STRICT;
```

**Key points**:
- Table name: `eventlog_{VERSION}_{sanitizedStoreId}`
- `STRICT` mode enforces type checking in SQLite
- `seqNum` is PRIMARY KEY (ensures uniqueness, fastest queries)
- `args` is TEXT (JSON-encoded), nullable
- One table per store (matches your decision)

#### Context/Metadata Table

```sql
CREATE TABLE IF NOT EXISTS "context_6" (
  storeId TEXT PRIMARY KEY,
  currentHead INTEGER,
  backendId TEXT
) STRICT;
```

**Note**: Since you're doing one database per store, you could simplify this to just store `currentHead` and `backendId` as a single row without needing `storeId` as the key.

**Simplified for single-store database**:
```sql
CREATE TABLE IF NOT EXISTS "context_6" (
  id INTEGER PRIMARY KEY CHECK (id = 1),  -- Ensure only one row
  currentHead INTEGER NOT NULL,
  backendId TEXT NOT NULL
) STRICT;
```

### 3.2: Head Tracking

#### Decision: Where to store head?

**Your choice**: Option A - Separate metadata table ✅

**What Cloudflare uses**: Exactly this! The `context_6` table shown above.

#### Decision: What to include?

**Your question**: "Should I include currentHead only if that's what CloudFlare uses?"

**Answer**: Cloudflare stores **THREE fields**:
1. ✅ **currentHead** (required) - Latest sequence number
2. ✅ **backendId** (required) - Server's unique identifier
3. ❌ **storeId** (not needed for you) - Only needed if multiple stores share one DB

**What is backendId?**

`backendId` is a **server-side unique identifier** that:
- Is generated once when the server first starts: `nanoid()` (e.g., "abc123xyz")
- Stays the same for the lifetime of your server instance
- **Prevents cross-backend pulls**: If you migrate from Server A to Server B, clients pulling with Server A's backendId will get an error, forcing them to resync

**When to include backendId**:
- ✅ **Yes, include it** - It prevents subtle bugs during server migrations
- Cloudflare uses it, and it's minimal overhead
- If a client has a cursor with `backendId: "old-server"` but your server has `backendId: "new-server"`, you return `BackendIdMismatchError` instead of serving potentially inconsistent data

**Your simplified schema**:
```sql
CREATE TABLE IF NOT EXISTS "context_6" (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  currentHead INTEGER NOT NULL,
  backendId TEXT NOT NULL
) STRICT;
```

**Initialization code**:
```typescript
// On server startup
const existingContext = db.query('SELECT * FROM context_6 WHERE id = 1')[0]

if (!existingContext) {
  const backendId = nanoid()  // Generate once
  db.run(
    'INSERT INTO context_6 (id, currentHead, backendId) VALUES (?, ?, ?)',
    [1, -1, backendId]  // -1 is ROOT sequence number
  )
}

const backendId = existingContext.backendId
const currentHead = existingContext.currentHead
```

#### Decision: How to update?

**Your choice**: Option A - Same transaction as events ✅

**What Cloudflare uses**: Actually, Cloudflare uses `blockConcurrencyWhile` (Durable Objects feature) which **combines both**: atomic execution + in-memory update.

**For SQLite without Durable Objects**, you should use **database transactions**:

```typescript
db.transaction(() => {
  // 1. Validate current head
  const { currentHead } = db.query('SELECT currentHead FROM context_6 WHERE id = 1')[0]

  if (batch[0].parentSeqNum !== currentHead) {
    throw new ServerAheadError({ serverHead: currentHead })
  }

  // 2. Insert events
  for (const event of batch) {
    db.run(
      'INSERT INTO eventlog_6_mystore (seqNum, parentSeqNum, name, args, createdAt, clientId, sessionId) VALUES (?, ?, ?, ?, ?, ?, ?)',
      [event.seqNum, event.parentSeqNum, event.name, JSON.stringify(event.args), new Date().toISOString(), event.clientId, event.sessionId]
    )
  }

  // 3. Update head
  db.run(
    'UPDATE context_6 SET currentHead = ? WHERE id = 1',
    [batch[batch.length - 1].seqNum]
  )
})
```

### 3.3: Cursor-Based Queries

#### Decision: Index strategy?

**Your choice**: Option A - Primary key on seqNum ✅

**What Cloudflare uses**: Exactly this! `seqNum INTEGER PRIMARY KEY`

#### Decision: Query pattern?

**Your question**: "What does CloudFlare use?"

**Answer**: Cloudflare uses **limit-based pagination**:

**DO SQLite** (simpler, what you should use):
```typescript
// Query with fixed page size
const DO_PAGE_SIZE = 256  // Cloudflare's value

const query = cursor === undefined
  ? `SELECT * FROM "${dbName}" ORDER BY seqNum ASC LIMIT ?`
  : `SELECT * FROM "${dbName}" WHERE seqNum > ? ORDER BY seqNum ASC LIMIT ?`

const params = cursor === undefined
  ? [DO_PAGE_SIZE]
  : [cursor, DO_PAGE_SIZE]

const events = db.query(query, params)
```

**D1** (adaptive, only needed for D1's HTTP response limits):
- Starts with 256 events per page
- Decreases page size if response > 1MB
- Increases page size if response < 500KB
- You don't need this complexity for regular SQLite

**Your implementation**: Use the simple DO SQLite approach with `LIMIT 256`.

#### Decision: Ordering guarantee?

**Your choice**: Always use explicit ORDER BY ✅

**What Cloudflare uses**: `ORDER BY seqNum ASC` (explicit ordering)

### 3.4: Atomicity Guarantees

#### Decision: Atomicity mechanism?

**Your question**: "Is this to prevent race conditions if two clients attempt to append to the log simultaneously?"

**Answer**: **Yes, exactly!** Without atomicity:
1. Client A reads currentHead = 10
2. Client B reads currentHead = 10
3. Client A writes event 11
4. Client B writes event 11 (duplicate!)
5. Events are out of order ❌

**Your question**: "Can I use Option A: Database transactions in SQLite?"

**Answer**: **Yes!** SQLite fully supports transactions. Here's what Cloudflare does and how you replicate it:

**What Cloudflare uses** (`blockConcurrencyWhile`):
```typescript
// Cloudflare Durable Objects
ctx.storage.blockConcurrencyWhile(async () => {
  const currentHead = getCurrentHead()
  validate(batch, currentHead)
  await appendEvents(batch)
  updateHead(newHead)
})
```

This ensures **only one push executes at a time** for the entire Durable Object.

**Your SQLite equivalent** (database transaction):
```typescript
// Your custom server with SQLite
db.transaction(() => {
  // 1. Read current head
  const { currentHead } = db.query('SELECT currentHead FROM context_6 WHERE id = 1')[0]

  // 2. Validate
  if (batch[0].parentSeqNum !== currentHead) {
    throw new ServerAheadError({ serverHead: currentHead, clientHead: batch[0].parentSeqNum })
  }

  // 3. Insert events (chunked to avoid parameter limits)
  const CHUNK_SIZE = 14  // Cloudflare's value to stay under 100 params
  for (let i = 0; i < batch.length; i += CHUNK_SIZE) {
    const chunk = batch.slice(i, i + CHUNK_SIZE)
    const placeholders = chunk.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')
    const sql = `INSERT INTO eventlog_6_mystore (seqNum, parentSeqNum, args, name, createdAt, clientId, sessionId) VALUES ${placeholders}`
    const params = chunk.flatMap(event => [
      event.seqNum,
      event.parentSeqNum,
      event.args === undefined ? null : JSON.stringify(event.args),
      event.name,
      new Date().toISOString(),
      event.clientId,
      event.sessionId
    ])
    db.run(sql, params)
  }

  // 4. Update head
  db.run('UPDATE context_6 SET currentHead = ? WHERE id = 1', [batch[batch.length - 1].seqNum])
})
```

**Key difference**:
- Cloudflare: `blockConcurrencyWhile` prevents concurrent execution at the Durable Object level
- You: SQLite transaction provides the same guarantee at the database level

**Important**: SQLite's default behavior is **serialized transactions**, meaning only one write transaction can execute at a time. This gives you the same guarantee as `blockConcurrencyWhile`!

#### Decision: Isolation level?

**Your question**: "I don't fully understand this. Is there an existing example in the CloudFlare implementation?"

**Answer**: **You don't need to worry about this for SQLite!**

Here's why:
- SQLite has a single fixed isolation mode: **SERIALIZABLE** (the strongest)
- Unlike PostgreSQL/MySQL, you can't change SQLite's isolation level
- SQLite automatically handles this for you

**What this means in practice**:
- When you call `db.transaction(...)`, SQLite ensures:
  - No other transaction can interfere
  - All reads/writes are isolated
  - Either all operations succeed or all fail (atomicity)

**You don't need to set any isolation level - SQLite does it automatically!**

#### Decision: Failure handling?

**Your choice**: Automatic retry for transient failures ✅

**What Cloudflare does**: Cloudflare doesn't retry in the push handler itself (it returns errors immediately), but the **client** retries using Effect's retry schedule.

**Your implementation**:
```typescript
const handlePush = (batch) => {
  try {
    db.transaction(() => {
      // Validation + insert + update head
    })
    return { success: true }
  } catch (error) {
    // Check if it's a validation error (don't retry)
    if (error instanceof ServerAheadError) {
      throw new InvalidPushError({ cause: error })
    }

    // Check if it's a transient error (database locked, etc.)
    if (error.code === 'SQLITE_BUSY' || error.code === 'SQLITE_LOCKED') {
      // Let client retry
      throw new InvalidPushError({ cause: error })
    }

    // Unknown error
    throw new InvalidPushError({ cause: error })
  }
}
```

**Client-side retry** (already built into Cloudflare client):
- The client automatically retries `InvalidPushError` with exponential backoff
- You don't need to implement retry on the server

## Complete Example: Push Handler for SQLite

Here's a complete push handler that matches Cloudflare's behavior:

```typescript
import { Database } from 'better-sqlite3'
import { InvalidPushError, ServerAheadError } from '@livestore/common'

const db = new Database('mystore.db')

// Enable WAL mode for better concurrency
db.pragma('journal_mode = WAL')

// Initialize tables (run once on startup)
db.exec(`
  CREATE TABLE IF NOT EXISTS "eventlog_6_mystore" (
    seqNum INTEGER PRIMARY KEY,
    parentSeqNum INTEGER,
    name TEXT,
    args TEXT,
    createdAt TEXT,
    clientId TEXT,
    sessionId TEXT
  ) STRICT;

  CREATE TABLE IF NOT EXISTS "context_6" (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    currentHead INTEGER NOT NULL,
    backendId TEXT NOT NULL
  ) STRICT;
`)

// Initialize context if not exists
const existing = db.prepare('SELECT * FROM context_6 WHERE id = 1').get()
if (!existing) {
  const backendId = nanoid()
  db.prepare('INSERT INTO context_6 (id, currentHead, backendId) VALUES (?, ?, ?)').run(1, -1, backendId)
}

const getBackendId = () => {
  return db.prepare('SELECT backendId FROM context_6 WHERE id = 1').get().backendId
}

const handlePush = (request: { storeId: string; batch: LiveStoreEvent[]; backendId?: string }) => {
  const backendId = getBackendId()

  // Validate backendId if provided
  if (request.backendId && request.backendId !== backendId) {
    throw new BackendIdMismatchError({ expected: backendId, received: request.backendId })
  }

  if (request.batch.length === 0) {
    return { success: true }
  }

  try {
    // Use transaction for atomicity
    const result = db.transaction(() => {
      // 1. Get current head
      const { currentHead } = db.prepare('SELECT currentHead FROM context_6 WHERE id = 1').get()

      // 2. Validate parent sequence
      if (request.batch[0].parentSeqNum !== currentHead) {
        throw new ServerAheadError({
          minimumExpectedNum: currentHead,
          providedNum: request.batch[0].parentSeqNum
        })
      }

      // 3. Insert events (chunked)
      const CHUNK_SIZE = 14
      const createdAt = new Date().toISOString()

      for (let i = 0; i < request.batch.length; i += CHUNK_SIZE) {
        const chunk = request.batch.slice(i, i + CHUNK_SIZE)
        const placeholders = chunk.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')
        const sql = `INSERT INTO eventlog_6_mystore (seqNum, parentSeqNum, args, name, createdAt, clientId, sessionId) VALUES ${placeholders}`
        const params = chunk.flatMap(event => [
          event.seqNum,
          event.parentSeqNum,
          event.args === undefined ? null : JSON.stringify(event.args),
          event.name,
          createdAt,
          event.clientId,
          event.sessionId
        ])
        db.prepare(sql).run(...params)
      }

      // 4. Update head
      const newHead = request.batch[request.batch.length - 1].seqNum
      db.prepare('UPDATE context_6 SET currentHead = ? WHERE id = 1').run(newHead)

      return { createdAt }
    })()

    // TODO: Broadcast to connected clients here

    return { success: true }

  } catch (error) {
    if (error instanceof ServerAheadError) {
      throw error  // Let RPC handler wrap in InvalidPushError
    }
    throw new InvalidPushError({ cause: error })
  }
}
```

## Complete Example: Pull Handler for SQLite

```typescript
const handlePull = (request: { storeId: string; cursor?: { backendId: string; eventSequenceNumber: number } }) => {
  const backendId = getBackendId()

  // Validate backendId if provided
  if (request.cursor?.backendId && request.cursor.backendId !== backendId) {
    throw new BackendIdMismatchError({ expected: backendId, received: request.cursor.backendId })
  }

  const cursor = request.cursor?.eventSequenceNumber
  const PAGE_SIZE = 256

  // Query events
  const query = cursor === undefined
    ? db.prepare('SELECT * FROM eventlog_6_mystore ORDER BY seqNum ASC LIMIT ?')
    : db.prepare('SELECT * FROM eventlog_6_mystore WHERE seqNum > ? ORDER BY seqNum ASC LIMIT ?')

  const events = cursor === undefined
    ? query.all(PAGE_SIZE)
    : query.all(cursor, PAGE_SIZE)

  // Map to response format
  return {
    batch: events.map(event => ({
      eventEncoded: {
        seqNum: event.seqNum,
        parentSeqNum: event.parentSeqNum,
        name: event.name,
        args: event.args ? JSON.parse(event.args) : undefined,
        clientId: event.clientId,
        sessionId: event.sessionId
      },
      metadata: { createdAt: event.createdAt }
    })),
    pageInfo: events.length === PAGE_SIZE
      ? { _tag: 'MoreUnknown' }  // Might be more
      : { _tag: 'NoMore' },
    backendId
  }
}
```

## Summary of Your Decisions

| Decision | Your Choice | Matches Cloudflare? | Notes |
|----------|------------|---------------------|-------|
| **Data store** | SQLite, 1 DB per store | ✅ Yes (DO SQLite mode) | Use exact same schemas |
| **Head location** | Separate metadata table | ✅ Yes | Use `context_6` table |
| **Head fields** | currentHead + backendId | ✅ Yes | Include backendId! |
| **Head update** | Same transaction | ✅ Yes (via blockConcurrencyWhile) | Use SQLite transactions |
| **Index** | PRIMARY KEY on seqNum | ✅ Yes | Fastest approach |
| **Query pattern** | Limit-based (256) | ✅ Yes (DO SQLite mode) | Simple and efficient |
| **Ordering** | Explicit ORDER BY | ✅ Yes | Always use ORDER BY seqNum ASC |
| **Atomicity** | Database transactions | ✅ Equivalent | SQLite transactions = blockConcurrencyWhile |
| **Isolation** | (Not configurable) | ✅ Yes | SQLite is always SERIALIZABLE |
| **Retry** | Client-side | ✅ Yes | Cloudflare client handles this |

## Key Takeaways

1. **You can reuse Cloudflare schemas exactly** - They're well-designed for SQLite
2. **Include backendId** - It prevents bugs during server migrations
3. **SQLite transactions are perfect** - They give you the same atomicity as Cloudflare's `blockConcurrencyWhile`
4. **No isolation level needed** - SQLite automatically uses SERIALIZABLE
5. **Use PAGE_SIZE = 256** - Cloudflare's proven value
6. **Chunk inserts (14 events)** - Stays under SQLite parameter limits
7. **One database per store** - Simpler than multi-store setup

Your decisions align perfectly with Cloudflare's proven implementation! 🎉
