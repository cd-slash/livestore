# @livestore/sync-sqlite

SQLite-based sync server for LiveStore.

## Features

- ✅ **Standalone sync server** - Run outside of Cloudflare infrastructure
- ✅ **SQLite storage** - One database file per store for isolation
- ✅ **ACID transactions** - Atomic push operations with SERIALIZABLE isolation
- ✅ **Cloudflare compatible** - Reuse `@livestore/sync-cf/client` directly
- ✅ **backendId validation** - Prevents cross-backend pulls
- ✅ **Cursor-based pagination** - Efficient event streaming
- ✅ **Type-safe** - Full TypeScript support with Effect Schema

## Installation

```bash
pnpm add @livestore/sync-sqlite
```

## Usage

### Server

```typescript
import { SqliteServer } from '@livestore/sync-sqlite/server'

// Create and start server
const server = new SqliteServer({
  dataDir: './data',  // Where SQLite databases are stored
  port: 3000,         // Optional, default: 3000
  host: '0.0.0.0',    // Optional, default: '0.0.0.0'
})

await server.start()
console.log(`Server running at ${server.url}`)

// Later: stop server
await server.stop()
```

### Client

```typescript
import { makeHttpSync } from '@livestore/sync-cf/client'
import { makeAdapter } from '@livestore/livestore'

// No custom client code needed! Reuse Cloudflare client
const syncBackend = makeHttpSync({
  url: 'http://localhost:3000/http-rpc',
  storeId: 'my-store',
})

const adapter = makeAdapter({
  sync: {
    backend: syncBackend,
  },
})
```

## Architecture

### Storage

- **One SQLite database per store**: `data/<storeId>.db`
- **Schema**: Identical to Cloudflare D1 implementation
- **Atomicity**: SQLite transactions (SERIALIZABLE isolation)
- **Pagination**: Limit-based with PAGE_SIZE=256

### RPC Protocol

Implements Cloudflare's HTTP RPC protocol:
- **Pull**: Cursor-based event streaming
- **Push**: Atomic batch operations with validation
- **Ping**: Health check endpoint

### Validation

- **Sequence validation**: Ensures events are sequential
- **Head validation**: Checks parent sequence matches current head
- **backendId validation**: Prevents cross-backend pulls after migration

## Configuration

### Server Config

```typescript
interface ServerConfig {
  /** Directory where SQLite databases will be stored */
  dataDir: string

  /** Port to listen on (default: 3000) */
  port?: number

  /** Host to bind to (default: '0.0.0.0') */
  host?: string
}
```

### SQLite Settings

Automatically configured for optimal performance:

```sql
PRAGMA journal_mode = WAL;        -- Write-Ahead Logging
PRAGMA synchronous = NORMAL;      -- Balance durability and performance
PRAGMA foreign_keys = ON;         -- Enforce constraints
PRAGMA busy_timeout = 5000;       -- Wait up to 5s for locks
```

## Limitations

### v1.0

- ❌ **No live updates**: `supports.pullLive = false` (polling required)
- ❌ **No WebSocket**: HTTP RPC only
- ❌ **No clustering**: Single server instance per database

These limitations may be addressed in future versions.

## Comparison with Cloudflare

| Feature | Cloudflare DO | SQLite Server |
|---------|--------------|---------------|
| **Storage** | D1 or DO SQLite | better-sqlite3 |
| **Deployment** | Cloudflare Workers | Node.js/Bun/Deno |
| **Live updates** | ✅ WebSocket | ❌ Polling only |
| **Atomicity** | blockConcurrencyWhile | SQLite transactions |
| **Isolation** | Per-DO | Per-database file |
| **Clustering** | Automatic | Single instance |
| **Cost** | Pay per request | Self-hosted |

## Examples

### Testing Setup

```typescript
import { SqliteServer } from '@livestore/sync-sqlite/server'
import { makeHttpSync } from '@livestore/sync-cf/client'

// Create server for testing
const server = new SqliteServer({
  dataDir: './test-data',
  port: 0, // Random port
})

await server.start()

// Use in tests
const syncBackend = makeHttpSync({
  url: server.url,
  storeId: 'test-store',
})

// Cleanup
await server.stop()
```

### Production Deployment

```typescript
import { SqliteServer } from '@livestore/sync-sqlite/server'

const server = new SqliteServer({
  dataDir: process.env.DATA_DIR || './data',
  port: parseInt(process.env.PORT || '3000'),
  host: process.env.HOST || '0.0.0.0',
})

await server.start()

// Graceful shutdown
process.on('SIGTERM', async () => {
  await server.stop()
  process.exit(0)
})
```

## Development

### Building

```bash
pnpm direnv exec . mono ts
```

### Testing

```bash
pnpm test
```

## License

Apache-2.0
