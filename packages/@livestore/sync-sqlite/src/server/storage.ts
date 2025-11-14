/**
 * SQLite storage layer for LiveStore sync server.
 *
 * This module implements the persistence layer using better-sqlite3, providing:
 * - Event log storage with cursor-based pagination
 * - Context metadata (currentHead, backendId)
 * - Atomic transactions for push operations
 * - Schema initialization and migration
 */

import Database from 'better-sqlite3'
import { UnexpectedError } from '@livestore/common'
import type { LiveStoreEvent } from '@livestore/common/schema'
import { Effect, Option, Stream, Chunk, Schema } from '@livestore/utils/effect'
import { SyncMetadata } from '@livestore/sync-cf/common'
import { nanoid } from 'nanoid'
import * as path from 'node:path'
import * as fs from 'node:fs'

/**
 * Persistence format version - bump this when schema changes
 */
const PERSISTENCE_FORMAT_VERSION = 6

/**
 * Page size for cursor-based pagination
 * Matches Cloudflare's PAGE_SIZE for consistency
 */
const PAGE_SIZE = 256

/**
 * Maximum events per INSERT statement (SQLite parameter limit)
 * 14 events * 7 params = 98 params (under 100 limit)
 */
const INSERT_CHUNK_SIZE = 14

/**
 * Storage interface for a single store
 */
export interface StoreStorage {
  /**
   * Get the backend ID for this store
   */
  getBackendId: () => Effect.Effect<string, UnexpectedError>

  /**
   * Get the current head (latest sequence number)
   */
  getCurrentHead: () => Effect.Effect<number, UnexpectedError>

  /**
   * Set the current head
   */
  setCurrentHead: (head: number) => Effect.Effect<void, UnexpectedError>

  /**
   * Query events starting from a cursor
   * Returns a stream of events with metadata
   */
  getEvents: (cursor: number | undefined) => Effect.Effect<
    {
      total: number
      stream: Stream.Stream<
        { eventEncoded: LiveStoreEvent.AnyEncodedGlobal; metadata: Option.Option<SyncMetadata> },
        UnexpectedError
      >
    },
    UnexpectedError
  >

  /**
   * Append events to the log
   * NOTE: Caller must wrap in transaction for atomicity
   */
  appendEvents: (
    batch: ReadonlyArray<LiveStoreEvent.AnyEncodedGlobal>,
    createdAt: string,
  ) => Effect.Effect<void, UnexpectedError>

  /**
   * Execute a function within a transaction
   * Provides atomicity for push operations
   */
  transaction: <A, E>(effect: Effect.Effect<A, E>) => Effect.Effect<A, E | UnexpectedError>

  /**
   * Close the database connection
   */
  close: () => Effect.Effect<void, UnexpectedError>
}

/**
 * Database row schemas for type safety
 */
const EventRow = Schema.Struct({
  seqNum: Schema.Number,
  parentSeqNum: Schema.Number,
  name: Schema.String,
  args: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  clientId: Schema.String,
  sessionId: Schema.String,
})

const ContextRow = Schema.Struct({
  id: Schema.Literal(1),
  currentHead: Schema.Number,
  backendId: Schema.String,
})

type EventRowType = Schema.Schema.Type<typeof EventRow>
type ContextRowType = Schema.Schema.Type<typeof ContextRow>

/**
 * Convert storeId to valid table name (replace invalid characters)
 */
const toValidTableName = (storeId: string): string => {
  return storeId.replace(/[^a-zA-Z0-9_]/g, '_')
}

/**
 * Create storage for a single store
 */
export const makeStoreStorage = (storeId: string, dataDir: string): Effect.Effect<StoreStorage, UnexpectedError> =>
  Effect.gen(function* () {
    // Ensure data directory exists
    yield* Effect.try({
      try: () => {
        if (!fs.existsSync(dataDir)) {
          fs.mkdirSync(dataDir, { recursive: true })
        }
      },
      catch: (error) => new UnexpectedError({ cause: error, payload: { dataDir } }),
    })

    // Create database file path
    const dbPath = path.join(dataDir, `${toValidTableName(storeId)}.db`)

    // Open database connection
    const db = yield* Effect.try({
      try: () => new Database(dbPath),
      catch: (error) => new UnexpectedError({ cause: error, payload: { dbPath } }),
    })

    // Configure SQLite for optimal performance and safety
    yield* Effect.try({
      try: () => {
        db.pragma('journal_mode = WAL') // Write-Ahead Logging for better concurrency
        db.pragma('synchronous = NORMAL') // Balance durability and performance
        db.pragma('foreign_keys = ON') // Enforce referential integrity
        db.pragma('busy_timeout = 5000') // Wait up to 5s for locks
      },
      catch: (error) => new UnexpectedError({ cause: error, payload: { dbPath } }),
    })

    // Initialize schema
    const tableName = `eventlog_${PERSISTENCE_FORMAT_VERSION}_${toValidTableName(storeId)}`

    yield* Effect.try({
      try: () => {
        // Create eventlog table
        db.exec(`
          CREATE TABLE IF NOT EXISTS "${tableName}" (
            seqNum INTEGER PRIMARY KEY,
            parentSeqNum INTEGER NOT NULL,
            name TEXT NOT NULL,
            args TEXT,
            createdAt TEXT NOT NULL,
            clientId TEXT NOT NULL,
            sessionId TEXT NOT NULL
          ) STRICT
        `)

        // Create context table
        db.exec(`
          CREATE TABLE IF NOT EXISTS context_${PERSISTENCE_FORMAT_VERSION} (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            currentHead INTEGER NOT NULL,
            backendId TEXT NOT NULL
          ) STRICT
        `)
      },
      catch: (error) => new UnexpectedError({ cause: error, payload: { tableName } }),
    })

    // Initialize context if not exists
    yield* Effect.try({
      try: () => {
        const row = db
          .prepare(`SELECT * FROM context_${PERSISTENCE_FORMAT_VERSION} WHERE id = 1`)
          .get() as ContextRowType | undefined

        if (!row) {
          // First time initialization
          const backendId = nanoid()
          db.prepare(`INSERT INTO context_${PERSISTENCE_FORMAT_VERSION} (id, currentHead, backendId) VALUES (?, ?, ?)`)
            .run(1, -1, backendId) // -1 is ROOT sequence number
        }
      },
      catch: (error) => new UnexpectedError({ cause: error, payload: { tableName } }),
    })

    // Helper: decode event row
    const decodeEventRow = Schema.decodeUnknownSync(EventRow)

    // Helper: decode context row
    const decodeContextRow = Schema.decodeUnknownSync(ContextRow)

    // Implementation
    const getBackendId = (): Effect.Effect<string, UnexpectedError> =>
      Effect.try({
        try: () => {
          const row = db
            .prepare(`SELECT backendId FROM context_${PERSISTENCE_FORMAT_VERSION} WHERE id = 1`)
            .get() as { backendId: string } | undefined

          if (!row) {
            throw new Error('Context row not found')
          }

          return row.backendId
        },
        catch: (error) => new UnexpectedError({ cause: error, payload: { storeId } }),
      })

    const getCurrentHead = (): Effect.Effect<number, UnexpectedError> =>
      Effect.try({
        try: () => {
          const row = db
            .prepare(`SELECT currentHead FROM context_${PERSISTENCE_FORMAT_VERSION} WHERE id = 1`)
            .get() as { currentHead: number } | undefined

          if (!row) {
            throw new Error('Context row not found')
          }

          return row.currentHead
        },
        catch: (error) => new UnexpectedError({ cause: error, payload: { storeId } }),
      })

    const setCurrentHead = (head: number): Effect.Effect<void, UnexpectedError> =>
      Effect.try({
        try: () => {
          db.prepare(`UPDATE context_${PERSISTENCE_FORMAT_VERSION} SET currentHead = ? WHERE id = 1`).run(head)
        },
        catch: (error) => new UnexpectedError({ cause: error, payload: { storeId, head } }),
      })

    const getEvents = (
      cursor: number | undefined,
    ): Effect.Effect<
      {
        total: number
        stream: Stream.Stream<
          { eventEncoded: LiveStoreEvent.AnyEncodedGlobal; metadata: Option.Option<SyncMetadata> },
          UnexpectedError
        >
      },
      UnexpectedError
    > =>
      Effect.gen(function* () {
        // Get total count
        const total = yield* Effect.try({
          try: () => {
            const countStatement =
              cursor === undefined
                ? `SELECT COUNT(*) as total FROM "${tableName}"`
                : `SELECT COUNT(*) as total FROM "${tableName}" WHERE seqNum > ?`

            const row = (
              cursor === undefined
                ? db.prepare(countStatement).get()
                : db.prepare(countStatement).get(cursor)
            ) as { total: number } | undefined

            return Number(row?.total ?? 0)
          },
          catch: (error) => new UnexpectedError({ cause: error, payload: { storeId, cursor } }),
        })

        // Create paginated stream
        type State = { cursor: number | undefined }
        type EmittedEvent = { eventEncoded: LiveStoreEvent.AnyEncodedGlobal; metadata: Option.Option<SyncMetadata> }

        const initialState: State = { cursor }

        const fetchPage = (
          state: State,
        ): Effect.Effect<Option.Option<readonly [Chunk.Chunk<EmittedEvent>, State]>, UnexpectedError> =>
          Effect.gen(function* () {
            const statement =
              state.cursor === undefined
                ? `SELECT * FROM "${tableName}" ORDER BY seqNum ASC LIMIT ?`
                : `SELECT * FROM "${tableName}" WHERE seqNum > ? ORDER BY seqNum ASC LIMIT ?`

            const rawEvents = yield* Effect.try({
              try: () => {
                return (
                  state.cursor === undefined
                    ? db.prepare(statement).all(PAGE_SIZE)
                    : db.prepare(statement).all(state.cursor, PAGE_SIZE)
                ) as unknown[]
              },
              catch: (error) => new UnexpectedError({ cause: error, payload: { storeId, cursor: state.cursor } }),
            })

            if (rawEvents.length === 0) {
              return Option.none()
            }

            // Decode and map rows
            const decodedRows = Chunk.fromIterable(rawEvents.map(decodeEventRow))

            const eventsChunk = Chunk.map(decodedRows, ({ createdAt, args, ...rest }) => ({
              eventEncoded: {
                ...rest,
                args: args ? JSON.parse(args) : undefined,
              } as LiveStoreEvent.AnyEncodedGlobal,
              metadata: Option.some(SyncMetadata.make({ createdAt })),
            }))

            const lastSeqNum = Chunk.unsafeLast(decodedRows).seqNum
            const nextState: State = { cursor: lastSeqNum }

            return Option.some([eventsChunk, nextState] as const)
          })

        const stream = Stream.unfoldChunkEffect(initialState, fetchPage)

        return { total, stream }
      })

    const appendEvents = (
      batch: ReadonlyArray<LiveStoreEvent.AnyEncodedGlobal>,
      createdAt: string,
    ): Effect.Effect<void, UnexpectedError> =>
      Effect.gen(function* () {
        if (batch.length === 0) return

        // Split into chunks to respect SQLite parameter limits
        for (let i = 0; i < batch.length; i += INSERT_CHUNK_SIZE) {
          const chunk = batch.slice(i, i + INSERT_CHUNK_SIZE)

          yield* Effect.try({
            try: () => {
              const valuesPlaceholders = chunk.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')
              const sql = `INSERT INTO "${tableName}" (seqNum, parentSeqNum, name, args, createdAt, clientId, sessionId) VALUES ${valuesPlaceholders}`

              const params = chunk.flatMap((event) => [
                event.seqNum,
                event.parentSeqNum,
                event.name,
                event.args === undefined ? null : JSON.stringify(event.args),
                createdAt,
                event.clientId,
                event.sessionId,
              ])

              db.prepare(sql).run(...params)
            },
            catch: (error) => new UnexpectedError({ cause: error, payload: { storeId, batchLength: batch.length } }),
          })
        }
      })

    const transaction = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<A, E | UnexpectedError> =>
      Effect.gen(function* () {
        // Start transaction
        yield* Effect.try({
          try: () => db.prepare('BEGIN').run(),
          catch: (error) => new UnexpectedError({ cause: error, payload: { storeId } }),
        })

        try {
          // Execute effect
          const result = yield* effect

          // Commit transaction
          yield* Effect.try({
            try: () => db.prepare('COMMIT').run(),
            catch: (error) => new UnexpectedError({ cause: error, payload: { storeId } }),
          })

          return result
        } catch (error) {
          // Rollback on error
          yield* Effect.try({
            try: () => db.prepare('ROLLBACK').run(),
            catch: (rollbackError) =>
              new UnexpectedError({ cause: rollbackError, payload: { storeId, originalError: error } }),
          })

          throw error
        }
      })

    const close = (): Effect.Effect<void, UnexpectedError> =>
      Effect.try({
        try: () => db.close(),
        catch: (error) => new UnexpectedError({ cause: error, payload: { storeId } }),
      })

    return {
      getBackendId,
      getCurrentHead,
      setCurrentHead,
      getEvents,
      appendEvents,
      transaction,
      close,
    }
  }).pipe(
    UnexpectedError.mapToUnexpectedError,
    Effect.withSpan('@livestore/sync-sqlite:makeStoreStorage', { attributes: { storeId, dataDir } }),
  )
