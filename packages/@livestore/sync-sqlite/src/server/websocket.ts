/**
 * WebSocket connection management for SQLite sync server.
 *
 * Handles WebSocket client tracking, attachments, and broadcasting.
 */

import { Schema } from '@livestore/utils/effect'
import type { ServerWebSocket } from 'bun'

/**
 * WebSocket attachment data stored on each connection
 * Matches Cloudflare's WebSocketAttachment schema
 */
export const WebSocketAttachment = Schema.Struct({
  /** Request IDs for active pull streams on this connection */
  pullRequestIds: Schema.Array(Schema.Number),

  /** When the connection was established */
  connectedAt: Schema.String,

  /** Store ID for this connection (optional, for filtering) */
  storeId: Schema.optional(Schema.String),
})

export type WebSocketAttachmentType = Schema.Schema.Type<typeof WebSocketAttachment>

/**
 * WebSocket data stored in ws.data
 */
export interface WebSocketData extends WebSocketAttachmentType {
  id: string // Unique connection ID
}

/**
 * WebSocket manager - tracks all connected clients
 */
export class WebSocketManager {
  private connections = new Map<string, ServerWebSocket<WebSocketData>>()

  /**
   * Register a new WebSocket connection
   */
  register(ws: ServerWebSocket<WebSocketData>): void {
    this.connections.set(ws.data.id, ws)
  }

  /**
   * Unregister a WebSocket connection
   */
  unregister(ws: ServerWebSocket<WebSocketData>): void {
    this.connections.delete(ws.data.id)
  }

  /**
   * Get all connected WebSocket clients
   */
  getAll(): ServerWebSocket<WebSocketData>[] {
    return Array.from(this.connections.values())
  }

  /**
   * Get WebSocket clients for a specific store
   */
  getForStore(storeId: string): ServerWebSocket<WebSocketData>[] {
    return Array.from(this.connections.values()).filter((ws) => ws.data.storeId === storeId)
  }

  /**
   * Get count of connected clients
   */
  get count(): number {
    return this.connections.size
  }

  /**
   * Add a pull request ID to a connection
   */
  addPullRequest(ws: ServerWebSocket<WebSocketData>, requestId: number): void {
    if (!ws.data.pullRequestIds.includes(requestId)) {
      ws.data.pullRequestIds.push(requestId)
    }
  }

  /**
   * Remove a pull request ID from a connection
   */
  removePullRequest(ws: ServerWebSocket<WebSocketData>, requestId: number): void {
    const index = ws.data.pullRequestIds.indexOf(requestId)
    if (index !== -1) {
      ws.data.pullRequestIds.splice(index, 1)
    }
  }

  /**
   * Broadcast a message to all connections with a specific pull request ID
   */
  broadcastToRequest(requestId: number, message: string): number {
    let sent = 0
    for (const ws of this.connections.values()) {
      if (ws.data.pullRequestIds.includes(requestId)) {
        ws.send(message)
        sent++
      }
    }
    return sent
  }

  /**
   * Broadcast to all connections for a specific store
   */
  broadcastToStore(storeId: string, message: string): number {
    let sent = 0
    for (const ws of this.connections.values()) {
      if (ws.data.storeId === storeId) {
        ws.send(message)
        sent++
      }
    }
    return sent
  }
}

/**
 * RPC message types for WebSocket communication
 * Based on Effect RPC protocol
 */
export namespace RpcMessage {
  export const Request = Schema.Struct({
    _tag: Schema.Literal('Request'),
    requestId: Schema.Number,
    name: Schema.String,
    args: Schema.Unknown,
  })

  export const ResponseSuccess = Schema.Struct({
    _tag: Schema.Literal('Success'),
    requestId: Schema.Number,
    value: Schema.Unknown,
  })

  export const ResponseChunk = Schema.Struct({
    _tag: Schema.Literal('Chunk'),
    requestId: Schema.Number,
    values: Schema.Array(Schema.Unknown),
  })

  export const ResponseExit = Schema.Struct({
    _tag: Schema.Literal('Exit'),
    requestId: Schema.Number,
  })

  export const ResponseError = Schema.Struct({
    _tag: Schema.Literal('Error'),
    requestId: Schema.Number,
    error: Schema.Unknown,
  })

  export const Interrupt = Schema.Struct({
    _tag: Schema.Literal('Interrupt'),
    requestId: Schema.Number,
  })

  export const AnyMessage = Schema.Union(
    Request,
    ResponseSuccess,
    ResponseChunk,
    ResponseExit,
    ResponseError,
    Interrupt,
  )

  export type Request = Schema.Schema.Type<typeof Request>
  export type ResponseSuccess = Schema.Schema.Type<typeof ResponseSuccess>
  export type ResponseChunk = Schema.Schema.Type<typeof ResponseChunk>
  export type ResponseExit = Schema.Schema.Type<typeof ResponseExit>
  export type ResponseError = Schema.Schema.Type<typeof ResponseError>
  export type Interrupt = Schema.Schema.Type<typeof Interrupt>
  export type AnyMessage = Schema.Schema.Type<typeof AnyMessage>
}
