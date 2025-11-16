/**
 * WebSocket RPC handler for SQLite sync server.
 *
 * Implements Effect RPC over WebSocket with streaming support.
 */

import { Effect, Schema, Stream } from '@livestore/utils/effect'
import type { ServerWebSocket } from 'bun'
import { makePullHandler, makePushHandler } from './handlers.ts'
import type { StoreStorage } from './storage.ts'
import { RpcMessage, type WebSocketData, type WebSocketManager } from './websocket.ts'

/**
 * Handle incoming WebSocket RPC message
 */
export const handleWebSocketMessage = (
  ws: ServerWebSocket<WebSocketData>,
  message: string | Buffer,
  storageContext: Awaited<ReturnType<typeof import('./server.ts').makeStorageContext>>,
  wsManager: WebSocketManager,
) =>
  Effect.gen(function* () {
    // Parse message
    const messageStr = typeof message === 'string' ? message : message.toString('utf-8')

    let parsed: RpcMessage.AnyMessage
    try {
      const json = JSON.parse(messageStr)
      parsed = Schema.decodeSync(RpcMessage.AnyMessage)(json)
    } catch (error) {
      // Invalid message format
      ws.send(
        JSON.stringify({
          _tag: 'Error',
          error: { message: 'Invalid message format', cause: String(error) },
        }),
      )
      return
    }

    // Handle different message types
    switch (parsed._tag) {
      case 'Request':
        yield* handleRequest(ws, parsed, storageContext, wsManager)
        break

      case 'Interrupt':
        yield* handleInterrupt(ws, parsed, wsManager)
        break

      default:
        // Ignore other message types (responses are client-to-server only)
        break
    }
  }).pipe(
    Effect.catchAll((error) =>
      Effect.sync(() => {
        console.error('WebSocket message handling error:', error)
        ws.send(
          JSON.stringify({
            _tag: 'Error',
            error: { message: 'Internal error', cause: String(error) },
          }),
        )
      }),
    ),
  )

/**
 * Handle RPC request
 */
const handleRequest = (
  ws: ServerWebSocket<WebSocketData>,
  request: RpcMessage.Request,
  storageContext: Awaited<ReturnType<typeof import('./server.ts').makeStorageContext>>,
  wsManager: WebSocketManager,
) =>
  Effect.gen(function* () {
    // Decode request based on method name
    if (request.name === 'SyncWsRpc.Pull') {
      yield* handlePullRequest(ws, request, storageContext, wsManager)
    } else if (request.name === 'SyncWsRpc.Push') {
      yield* handlePushRequest(ws, request, storageContext, wsManager)
    } else {
      // Unknown method
      ws.send(
        JSON.stringify({
          _tag: 'Error',
          requestId: request.requestId,
          error: { message: `Unknown method: ${request.name}` },
        }),
      )
    }
  })

/**
 * Handle Pull request (streaming)
 */
const handlePullRequest = (
  ws: ServerWebSocket<WebSocketData>,
  request: RpcMessage.Request,
  storageContext: Awaited<ReturnType<typeof import('./server.ts').makeStorageContext>>,
  wsManager: WebSocketManager,
) =>
  Effect.gen(function* () {
    // Decode Pull request
    const PullRequestSchema = Schema.Struct({
      storeId: Schema.String,
      payload: Schema.optional(Schema.JsonValue),
      live: Schema.Boolean,
      cursor: Schema.optional(
        Schema.Struct({
          backendId: Schema.String,
          eventSequenceNumber: Schema.Number,
        }),
      ),
    })

    let pullReq: Schema.Schema.Type<typeof PullRequestSchema>
    try {
      pullReq = Schema.decodeSync(PullRequestSchema)(request.args)
    } catch (error) {
      ws.send(
        JSON.stringify({
          _tag: 'Error',
          requestId: request.requestId,
          error: { message: 'Invalid Pull request', cause: String(error) },
        }),
      )
      return
    }

    // Store the storeId in connection data
    ws.data.storeId = pullReq.storeId

    // Register pull request ID
    wsManager.addPullRequest(ws, request.requestId)

    // Get storage for store
    const storage = yield* storageContext.getStorage(pullReq.storeId)

    // Create pull handler
    const handler = makePullHandler(storage)

    // Convert request to handler format
    const handlerReq = {
      cursor: pullReq.cursor ? { _tag: 'Some' as const, value: pullReq.cursor } : { _tag: 'None' as const },
    }

    // Execute pull
    const stream = yield* handler(handlerReq)

    // If live mode, append Stream.never to keep stream alive
    const finalStream = pullReq.live ? stream.pipe(Stream.concat(Stream.never)) : stream

    // Stream responses to WebSocket
    yield* finalStream.pipe(
      Stream.runForEach((pullResponse) =>
        Effect.sync(() => {
          // Send as chunk
          const encoded = Schema.encodeSync(Schema.parseJson(Schema.Unknown))(pullResponse)
          ws.send(
            JSON.stringify({
              _tag: 'Chunk',
              requestId: request.requestId,
              values: [encoded],
            }),
          )
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          // Send Exit when stream completes
          ws.send(
            JSON.stringify({
              _tag: 'Exit',
              requestId: request.requestId,
            }),
          )

          // Unregister pull request ID
          wsManager.removePullRequest(ws, request.requestId)
        }),
      ),
      Effect.catchAll((error) =>
        Effect.sync(() => {
          // Send error
          ws.send(
            JSON.stringify({
              _tag: 'Error',
              requestId: request.requestId,
              error: {
                _tag: error._tag || 'UnexpectedError',
                message: String(error),
              },
            }),
          )

          // Unregister pull request ID
          wsManager.removePullRequest(ws, request.requestId)
        }),
      ),
      Effect.fork, // Run in background
    )
  })

/**
 * Handle Push request (non-streaming)
 */
const handlePushRequest = (
  ws: ServerWebSocket<WebSocketData>,
  request: RpcMessage.Request,
  storageContext: Awaited<ReturnType<typeof import('./server.ts').makeStorageContext>>,
  wsManager: WebSocketManager,
) =>
  Effect.gen(function* () {
    // Decode Push request
    const PushRequestSchema = Schema.Struct({
      storeId: Schema.String,
      payload: Schema.optional(Schema.JsonValue),
      batch: Schema.Array(Schema.Unknown), // LiveStoreEvent array
      backendId: Schema.optional(Schema.String),
    })

    let pushReq: Schema.Schema.Type<typeof PushRequestSchema>
    try {
      pushReq = Schema.decodeSync(PushRequestSchema)(request.args)
    } catch (error) {
      ws.send(
        JSON.stringify({
          _tag: 'Error',
          requestId: request.requestId,
          error: { message: 'Invalid Push request', cause: String(error) },
        }),
      )
      return
    }

    // Get storage for store
    const storage = yield* storageContext.getStorage(pushReq.storeId)

    // Create push handler
    const handler = makePushHandler(storage)

    // Convert request to handler format
    const handlerReq = {
      batch: pushReq.batch as any[], // Type assertion for now
      backendId: pushReq.backendId ? { _tag: 'Some' as const, value: pushReq.backendId } : { _tag: 'None' as const },
    }

    // Execute push
    const result = yield* handler(handlerReq).pipe(
      Effect.catchAll((error) => {
        // Send error
        ws.send(
          JSON.stringify({
            _tag: 'Error',
            requestId: request.requestId,
            error: {
              _tag: error._tag || 'UnexpectedError',
              message: String(error),
            },
          }),
        )
        return Effect.fail(error)
      }),
    )

    // Send success response
    const encoded = Schema.encodeSync(Schema.parseJson(Schema.Unknown))(result)
    ws.send(
      JSON.stringify({
        _tag: 'Success',
        requestId: request.requestId,
        value: encoded,
      }),
    )

    // **Broadcast to connected clients** (this is the key feature!)
    yield* broadcastPushToClients(pushReq.storeId, pushReq.batch as any[], storage, wsManager).pipe(Effect.fork)
  })

/**
 * Handle Interrupt request
 */
const handleInterrupt = (
  ws: ServerWebSocket<WebSocketData>,
  interrupt: RpcMessage.Interrupt,
  wsManager: WebSocketManager,
) =>
  Effect.sync(() => {
    // Remove pull request ID from connection
    wsManager.removePullRequest(ws, interrupt.requestId)

    // Send Exit to confirm interruption
    ws.send(
      JSON.stringify({
        _tag: 'Exit',
        requestId: interrupt.requestId,
      }),
    )
  })

/**
 * Broadcast push events to all connected WebSocket clients
 * This is the critical feature for real-time updates!
 */
const broadcastPushToClients = (storeId: string, batch: any[], storage: StoreStorage, wsManager: WebSocketManager) =>
  Effect.gen(function* () {
    // Get backendId
    const backendId = yield* storage.getBackendId()

    // Get all connected clients for this store
    const clients = wsManager.getForStore(storeId)

    if (clients.length === 0) {
      return // No clients to broadcast to
    }

    // Create pull response for broadcast
    const createdAt = new Date().toISOString()
    const pullResponse = {
      batch: batch.map((eventEncoded) => ({
        eventEncoded,
        metadata: { _tag: 'Some' as const, value: { createdAt } },
      })),
      pageInfo: { _tag: 'NoMore' as const },
      backendId,
    }

    // Encode response
    const encoded = Schema.encodeSync(Schema.parseJson(Schema.Unknown))(pullResponse)

    // Broadcast to all clients with active pull requests
    let broadcastCount = 0
    for (const client of clients) {
      for (const requestId of client.data.pullRequestIds) {
        client.send(
          JSON.stringify({
            _tag: 'Chunk',
            requestId,
            values: [encoded],
          }),
        )
        broadcastCount++
      }
    }

    yield* Effect.logDebug(`Broadcasted to ${broadcastCount} active pull requests on ${clients.length} clients`)
  })
