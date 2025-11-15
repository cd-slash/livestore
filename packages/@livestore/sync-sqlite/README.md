# @livestore/sync-sqlite

SQLite-based sync server for LiveStore, powered by Bun.

## Features

- ✅ **Bun-powered** - Fast server using Bun's native APIs
- ✅ **WebSocket support** - Real-time updates with Bun's WebSocket (v1.3+)
- ✅ **HTTP RPC** - Standard request/response for polling
- ✅ **SQLite storage** - One database file per store for isolation
- ✅ **ACID transactions** - Atomic push operations with SERIALIZABLE isolation
- ✅ **Cloudflare compatible** - Reuse `@livestore/sync-cf/client` directly
- ✅ **backendId validation** - Prevents cross-backend pulls
- ✅ **Cursor-based pagination** - Efficient event streaming
- ✅ **Type-safe** - Full TypeScript support with Effect Schema

## Requirements

- **Bun v1.3+** (for improved WebSocket support)

## Installation

```bash
bun add @livestore/sync-sqlite
```

## Usage

### Server

```typescript
import { SqliteServer } from '@livestore/sync-sqlite/server'

// Create and start server
const server = new SqliteServer({
  dataDir: './data',       // Where SQLite databases are stored
  port: 3000,              // Optional, default: 3000
  host: '0.0.0.0',         // Optional, default: '0.0.0.0'
  enableWebSocket: true,   // Optional, default: true
})

await server.start()
console.log(`HTTP RPC: ${server.url}`)
console.log(`WebSocket: ${server.wsUrl}`)

// Later: stop server
await server.stop()
```

Run with Bun:

```bash
bun run server.ts
```

### Client (HTTP)

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

### Client (WebSocket)

```typescript
import { makeWsSync } from '@livestore/sync-cf/client'
import { makeAdapter } from '@livestore/livestore'

// WebSocket for real-time updates
const syncBackend = makeWsSync({
  url: 'ws://localhost:3000/ws',
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

Implements Cloudflare's RPC protocols:

**HTTP RPC** (`/http-rpc`):
- **Pull**: Cursor-based event streaming (polling)
- **Push**: Atomic batch operations with validation
- **Ping**: Health check endpoint

**WebSocket RPC** (`/ws`):
- **Pull**: Real-time streaming with live updates
- **Push**: Atomic batch operations with validation
- Uses Bun's native WebSocket implementation (v1.3+)

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

  /** Enable WebSocket transport (default: true) */
  enableWebSocket?: boolean
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

### Current

- ⚠️ **WebSocket RPC integration**: Initial implementation, being refined
- ❌ **No clustering**: Single server instance per database
- ❌ **No replication**: No built-in primary/replica setup

These limitations may be addressed in future versions.

## Comparison with Cloudflare

| Feature | Cloudflare DO | SQLite Server (Bun) |
|---------|--------------|---------------------|
| **Storage** | D1 or DO SQLite | better-sqlite3 |
| **Deployment** | Cloudflare Workers | Bun runtime |
| **Live updates** | ✅ WebSocket | ✅ WebSocket (Bun native) |
| **HTTP RPC** | ✅ | ✅ |
| **Atomicity** | blockConcurrencyWhile | SQLite transactions |
| **Isolation** | Per-DO | Per-database file |
| **Clustering** | Automatic | Single instance |
| **Cost** | Pay per request | Self-hosted |
| **Runtime** | Cloudflare edge | Bun v1.3+ |

## Examples

### Testing Setup

```typescript
import { SqliteServer } from '@livestore/sync-sqlite/server'
import { makeHttpSync } from '@livestore/sync-cf/client'

// Create server for testing
const server = new SqliteServer({
  dataDir: './test-data',
  port: 0, // Random port
  enableWebSocket: true,
})

await server.start()

// Use in tests (HTTP)
const syncBackend = makeHttpSync({
  url: server.url,
  storeId: 'test-store',
})

// Or WebSocket for live updates
const wsSync = makeWsSync({
  url: server.wsUrl,
  storeId: 'test-store',
})

// Cleanup
await server.stop()
```

### Production Deployment

```typescript
import { SqliteServer } from '@livestore/sync-sqlite/server'

const server = new SqliteServer({
  dataDir: Bun.env.DATA_DIR || './data',
  port: parseInt(Bun.env.PORT || '3000'),
  host: Bun.env.HOST || '0.0.0.0',
  enableWebSocket: true,
})

await server.start()

console.log(`✓ Server started`)
console.log(`  HTTP: ${server.url}`)
console.log(`  WS:   ${server.wsUrl}`)

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Shutting down...')
  await server.stop()
  process.exit(0)
})
```

Run with:

```bash
bun run server.ts
```

## Development

### Building

```bash
bun run direnv exec . mono ts
```

### Testing

```bash
bun test
```

### Running locally

```bash
bun run examples/server.ts
```

## License

Apache-2.0
