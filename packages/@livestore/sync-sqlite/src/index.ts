/**
 * @livestore/sync-sqlite
 *
 * SQLite-based sync server for LiveStore, powered by Bun.
 *
 * This package provides a standalone sync server that:
 * - Uses Bun's native server and WebSocket APIs (v1.3+)
 * - Uses SQLite for persistent event storage
 * - Implements the Cloudflare RPC protocol (HTTP and WebSocket)
 * - Is compatible with @livestore/sync-cf/client
 * - Supports one database file per store
 * - Uses transactions for atomicity
 * - Includes backendId validation
 * - Enables real-time updates via WebSocket
 *
 * ## Requirements
 *
 * - Bun v1.3+ (for improved WebSocket support)
 *
 * ## Usage
 *
 * ```typescript
 * import { SqliteServer } from '@livestore/sync-sqlite/server'
 * import { makeHttpSync, makeWsSync } from '@livestore/sync-cf/client'
 *
 * // Start server
 * const server = new SqliteServer({
 *   dataDir: './data',
 *   enableWebSocket: true,
 * })
 * await server.start()
 *
 * // Create HTTP client
 * const httpSync = makeHttpSync({
 *   url: server.url,
 *   storeId: 'my-store',
 * })
 *
 * // Or WebSocket client for real-time updates
 * const wsSync = makeWsSync({
 *   url: server.wsUrl,
 *   storeId: 'my-store',
 * })
 * ```
 *
 * @see {@link https://livestore.dev/docs/sync} for more information
 */

export * from './server/mod.ts'
