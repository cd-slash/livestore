/**
 * @livestore/sync-sqlite
 *
 * SQLite-based sync server for LiveStore.
 *
 * This package provides a standalone sync server that:
 * - Uses SQLite for persistent event storage
 * - Implements the Cloudflare RPC protocol
 * - Is compatible with @livestore/sync-cf/client
 * - Supports one database file per store
 * - Uses transactions for atomicity
 * - Includes backendId validation
 *
 * ## Usage
 *
 * ```typescript
 * import { SqliteServer } from '@livestore/sync-sqlite/server'
 * import { makeHttpSync } from '@livestore/sync-cf/client'
 *
 * // Start server
 * const server = new SqliteServer({ dataDir: './data' })
 * await server.start()
 *
 * // Create client (reuses Cloudflare client!)
 * const syncBackend = makeHttpSync({
 *   url: server.url,
 *   storeId: 'my-store',
 * })
 * ```
 *
 * @see {@link https://livestore.dev/docs/sync} for more information
 */

export * from './server/mod.ts'
