/**
 * SQLite sync server implementation using Bun runtime.
 *
 * This module provides a standalone sync server that:
 * - Uses Bun's native server and WebSocket APIs
 * - Uses SQLite for event storage
 * - Implements the Cloudflare RPC protocol (HTTP and WebSocket)
 * - Is compatible with @livestore/sync-cf/client
 * - Supports one database file per store
 * - Broadcasts push events to connected WebSocket clients
 */

import type { UnexpectedError } from '@livestore/common'
import { SyncHttpRpc } from '@livestore/sync-cf/common'
import { Context, Effect, HttpApp, Layer, RpcSerialization, RpcServer } from '@livestore/utils/effect'
import type { ServerWebSocket } from 'bun'
import { nanoid } from 'nanoid'
import { makePingHandler, makePullHandler, makePushHandler } from './handlers.ts'
import { makeStoreStorage, type StoreStorage } from './storage.ts'
import { type WebSocketData, WebSocketManager } from './websocket.ts'
import { handleWebSocketMessage } from './ws-rpc-handler.ts'

/**
 * Server configuration
 */
export interface ServerConfig {
  /**
   * Directory where SQLite databases will be stored
   */
  dataDir: string

  /**
   * Port to listen on (default: 3000)
   */
  port?: number

  /**
   * Host to bind to (default: '0.0.0.0')
   */
  host?: string

  /**
   * Enable WebSocket transport (default: true)
   */
  enableWebSocket?: boolean
}

/**
 * Storage context - manages store storage instances
 */
export class StorageContext extends Context.Tag('StorageContext')<
  StorageContext,
  {
    getStorage: (storeId: string) => Effect.Effect<StoreStorage, UnexpectedError>
    closeAll: () => Effect.Effect<void, UnexpectedError>
  }
>() {}

/**
 * Create storage context that manages store instances
 */
export const makeStorageContext = (dataDir: string) =>
  Effect.gen(function* () {
    // Cache of storage instances
    const storages = new Map<string, StoreStorage>()

    const getStorage = (storeId: string): Effect.Effect<StoreStorage, UnexpectedError> =>
      Effect.gen(function* () {
        // Return cached instance if exists
        const cached = storages.get(storeId)
        if (cached) {
          return cached
        }

        // Create new storage instance
        const storage = yield* makeStoreStorage(storeId, dataDir)
        storages.set(storeId, storage)

        return storage
      })

    const closeAll = (): Effect.Effect<void, UnexpectedError> =>
      Effect.gen(function* () {
        for (const [storeId, storage] of storages) {
          yield* storage
            .close()
            .pipe(Effect.catchAll((error) => Effect.logWarning(`Failed to close storage for ${storeId}`, error)))
        }
        storages.clear()
      })

    return { getStorage, closeAll }
  })

/**
 * Create HTTP RPC handler layer
 */
const createHttpRpcLayer = SyncHttpRpc.toLayer({
  'SyncHttpRpc.Pull': (req) =>
    Effect.gen(function* () {
      const ctx = yield* StorageContext
      const storage = yield* ctx.getStorage(req.storeId)
      const handler = makePullHandler(storage)
      return yield* handler(req)
    }),

  'SyncHttpRpc.Push': (req) =>
    Effect.gen(function* () {
      const ctx = yield* StorageContext
      const storage = yield* ctx.getStorage(req.storeId)
      const handler = makePushHandler(storage)
      return yield* handler(req)
    }),

  'SyncHttpRpc.Ping': (req) =>
    Effect.gen(function* () {
      const ctx = yield* StorageContext
      const storage = yield* ctx.getStorage(req.storeId)
      const handler = makePingHandler(storage)
      return yield* handler(req)
    }),
}).pipe(
  Layer.provideMerge(RpcServer.layerProtocolHttp({ path: '/http-rpc' })),
  Layer.provideMerge(RpcSerialization.layerJson),
)

/**
 * SQLite sync server using Bun runtime
 */
export class SqliteServer {
  private config: ServerConfig
  private server: ReturnType<typeof Bun.serve> | null = null
  private storageContext: Awaited<ReturnType<typeof makeStorageContext>> | null = null
  private wsManager: WebSocketManager

  constructor(config: ServerConfig) {
    this.config = {
      port: 3000,
      host: '0.0.0.0',
      enableWebSocket: true,
      ...config,
    }
    this.wsManager = new WebSocketManager()
  }

  /**
   * Start the server
   */
  async start(): Promise<void> {
    if (this.server) {
      throw new Error('Server is already running')
    }

    // Create storage context
    this.storageContext = await Effect.runPromise(makeStorageContext(this.config.dataDir))

    // Create storage layer
    const storageLayer = Layer.succeed(StorageContext, this.storageContext)

    // Create HTTP app
    const httpApp = RpcServer.toHttpApp(SyncHttpRpc).pipe(Effect.provide(createHttpRpcLayer))

    // Get web handler for HTTP requests
    const webHandler = await Effect.runPromise(
      httpApp.pipe(Effect.map(HttpApp.toWebHandler), Effect.provide(storageLayer)),
    )

    // Capture storageContext and wsManager for WebSocket handlers
    const storageContext = this.storageContext
    const wsManager = this.wsManager

    // Create Bun server with WebSocket support
    this.server = Bun.serve({
      port: this.config.port,
      hostname: this.config.host,

      // HTTP request handler
      fetch: async (req, server) => {
        const url = new URL(req.url)

        // Handle WebSocket upgrade for /ws path
        if (url.pathname === '/ws' && this.config.enableWebSocket) {
          const upgraded = server.upgrade(req, {
            data: {
              id: nanoid(),
              pullRequestIds: [],
              connectedAt: new Date().toISOString(),
            } as WebSocketData,
          })

          if (upgraded) {
            return undefined // WebSocket upgrade successful
          }

          // Upgrade failed
          return new Response('WebSocket upgrade failed', { status: 400 })
        }

        // Handle regular HTTP requests
        try {
          return await webHandler(req)
        } catch (error) {
          console.error('Request error:', error)
          return new Response(JSON.stringify({ error: 'Internal server error' }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' },
          })
        }
      },

      // WebSocket handlers
      websocket: this.config.enableWebSocket
        ? {
            open(ws: ServerWebSocket<WebSocketData>) {
              // Register connection
              wsManager.register(ws)
              console.log(`WebSocket connected: ${ws.data.id} (total: ${wsManager.count})`)
            },

            async message(ws: ServerWebSocket<WebSocketData>, message: string | Buffer) {
              // Handle RPC message using the dedicated handler
              await Effect.runPromise(handleWebSocketMessage(ws, message, storageContext, wsManager))
            },

            close(ws: ServerWebSocket<WebSocketData>) {
              // Unregister connection
              wsManager.unregister(ws)
              console.log(`WebSocket disconnected: ${ws.data.id} (total: ${wsManager.count})`)
            },

            error(ws: ServerWebSocket<WebSocketData>, error: Error) {
              console.error(`WebSocket error on ${ws.data.id}:`, error)
            },
          }
        : undefined,
    })

    console.log(`SQLite sync server listening on http://${this.config.host}:${this.server.port}`)
    if (this.config.enableWebSocket) {
      console.log(`WebSocket endpoint: ws://${this.config.host}:${this.server.port}/ws`)
    }
  }

  /**
   * Stop the server
   */
  async stop(): Promise<void> {
    if (!this.server) {
      return
    }

    // Stop Bun server
    this.server.stop()
    this.server = null

    // Close all storages
    if (this.storageContext) {
      await Effect.runPromise(this.storageContext.closeAll())
      this.storageContext = null
    }
  }

  /**
   * Get server URL (HTTP RPC endpoint)
   */
  get url(): string {
    if (!this.server) {
      throw new Error('Server is not running')
    }
    return `http://${this.config.host}:${this.server.port}/http-rpc`
  }

  /**
   * Get WebSocket URL
   */
  get wsUrl(): string {
    if (!this.server) {
      throw new Error('Server is not running')
    }
    if (!this.config.enableWebSocket) {
      throw new Error('WebSocket is not enabled')
    }
    return `ws://${this.config.host}:${this.server.port}/ws`
  }

  /**
   * Check if server is running
   */
  get isRunning(): boolean {
    return this.server !== null
  }

  /**
   * Get server port (useful when using port: 0 for random port)
   */
  get port(): number {
    if (!this.server) {
      throw new Error('Server is not running')
    }
    return this.server.port
  }

  /**
   * Get WebSocket connection count
   */
  get connectionCount(): number {
    return this.wsManager.count
  }
}
