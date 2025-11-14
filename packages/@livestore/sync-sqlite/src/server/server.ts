/**
 * SQLite sync server implementation.
 *
 * This module provides a standalone sync server that:
 * - Uses SQLite for event storage
 * - Implements the Cloudflare RPC protocol
 * - Is compatible with @livestore/sync-cf/client
 * - Supports one database per store
 */

import { UnexpectedError } from '@livestore/common'
import {
  Effect,
  Layer,
  HttpApp,
  HttpServer,
  RpcServer,
  RpcSerialization,
  Context,
  Scope,
} from '@livestore/utils/effect'
import { SyncHttpRpc } from '@livestore/sync-cf/common'
import { makeStoreStorage, type StoreStorage } from './storage.ts'
import { makePullHandler, makePushHandler, makePingHandler } from './handlers.ts'
import * as http from 'node:http'

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
const makeStorageContext = (dataDir: string) =>
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
          yield* storage.close().pipe(
            Effect.catchAll((error) =>
              Effect.logWarning(`Failed to close storage for ${storeId}`, error),
            ),
          )
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
 * SQLite sync server
 */
export class SqliteServer {
  private config: ServerConfig
  private runtime: Effect.RuntimeFiber<StorageContext> | null = null
  private httpServer: http.Server | null = null

  constructor(config: ServerConfig) {
    this.config = {
      port: 3000,
      host: '0.0.0.0',
      ...config,
    }
  }

  /**
   * Start the server
   */
  async start(): Promise<void> {
    if (this.runtime) {
      throw new Error('Server is already running')
    }

    // Create storage context layer
    const storageLayer = Layer.effect(StorageContext, makeStorageContext(this.config.dataDir))

    // Create HTTP app
    const httpApp = RpcServer.toHttpApp(SyncHttpRpc).pipe(Effect.provide(createHttpRpcLayer))

    // Create full layer stack
    const fullLayer = storageLayer

    // Create runtime
    const runtimeEffect = Effect.gen(function* () {
      // Get web handler
      const webHandler = yield* httpApp.pipe(Effect.map(HttpApp.toWebHandler), Effect.provide(fullLayer))

      // Create Node.js HTTP server
      const server = http.createServer((req, res) => {
        // Convert Node.js request to web Request
        const url = `http://${req.headers.host}${req.url}`

        // Collect request body
        const chunks: Buffer[] = []
        req.on('data', (chunk) => chunks.push(chunk))
        req.on('end', async () => {
          const body = chunks.length > 0 ? Buffer.concat(chunks).toString() : undefined

          // Create Request object
          const request = new Request(url, {
            method: req.method,
            headers: req.headers as HeadersInit,
            body: body,
          })

          try {
            // Handle request
            const response = await webHandler(request)

            // Send response
            res.statusCode = response.status
            response.headers.forEach((value, key) => {
              res.setHeader(key, value)
            })

            // Stream response body
            if (response.body) {
              const reader = response.body.getReader()
              while (true) {
                const { done, value } = await reader.read()
                if (done) break
                res.write(value)
              }
            }

            res.end()
          } catch (error) {
            res.statusCode = 500
            res.end(JSON.stringify({ error: 'Internal server error' }))
          }
        })
      })

      // Start listening
      yield* Effect.promise(
        () =>
          new Promise<void>((resolve, reject) => {
            server.on('error', reject)
            server.listen(this.config.port, this.config.host, () => {
              console.log(`SQLite sync server listening on http://${this.config.host}:${this.config.port}`)
              resolve()
            })
          }),
      )

      return { server }
    })

    // Run and capture runtime
    const fiber = await runtimeEffect.pipe(Effect.provide(fullLayer), Effect.runFork)

    this.runtime = fiber as Effect.RuntimeFiber<StorageContext>

    // Wait for server to be ready
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const { server } = yield* Effect.promise(() => fiber as Promise<{ server: http.Server }>)
        return server
      }),
    )

    this.httpServer = result
  }

  /**
   * Stop the server
   */
  async stop(): Promise<void> {
    if (!this.runtime) {
      return
    }

    // Close HTTP server
    if (this.httpServer) {
      await new Promise<void>((resolve) => {
        this.httpServer!.close(() => resolve())
      })
      this.httpServer = null
    }

    // Close all storages
    await Effect.runPromise(
      Effect.gen(function* () {
        const ctx = yield* StorageContext
        yield* ctx.closeAll()
      }).pipe(Effect.provide(this.runtime)),
    )

    // Interrupt runtime
    await Effect.runPromise(Effect.interrupt)

    this.runtime = null
  }

  /**
   * Get server URL
   */
  get url(): string {
    if (!this.httpServer) {
      throw new Error('Server is not running')
    }
    return `http://${this.config.host}:${this.config.port}/http-rpc`
  }

  /**
   * Check if server is running
   */
  get isRunning(): boolean {
    return this.runtime !== null
  }
}
