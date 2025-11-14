/**
 * RPC handlers for Pull, Push, and Ping operations.
 *
 * These handlers implement the Cloudflare RPC protocol, making the server
 * compatible with the existing @livestore/sync-cf/client.
 */

import {
  BackendIdMismatchError,
  InvalidPullError,
  InvalidPushError,
  ServerAheadError,
  SyncBackend,
  UnexpectedError,
} from '@livestore/common'
import { Effect, Option, Stream, Chunk } from '@livestore/utils/effect'
import { SyncMessage } from '@livestore/sync-cf/common'
import type { StoreStorage } from './storage.ts'

/**
 * Create pull handler
 *
 * Returns a stream of events starting from the cursor.
 * Does not support live updates (pullLive = false).
 */
export const makePullHandler =
  (storage: StoreStorage) =>
  (req: SyncMessage.PullRequest): Stream.Stream<SyncMessage.PullResponse, InvalidPullError> =>
    Effect.gen(function* () {
      // Get backend ID
      const backendId = yield* storage.getBackendId()

      // Validate cursor backendId if provided
      if (req.cursor._tag === 'Some' && req.cursor.value.backendId !== backendId) {
        return yield* new BackendIdMismatchError({
          expected: backendId,
          received: req.cursor.value.backendId,
        })
      }

      // Get events from storage
      const { stream: storedEvents, total } = yield* storage.getEvents(
        Option.getOrUndefined(req.cursor)?.eventSequenceNumber,
      )

      // Convert storage stream to pull response stream
      return storedEvents.pipe(
        // Map storage events to pull response format
        Stream.mapChunks((chunk) => {
          const batch = Chunk.toReadonlyArray(chunk)
          return Chunk.of({
            batch,
            pageInfo: SyncBackend.pageInfoNoMore, // Will be updated with remaining count
            backendId,
          })
        }),
        // Add remaining count to page info
        Stream.mapAccum(total, (remaining, response) => {
          const batchSize = response.batch.length
          const nextRemaining = Math.max(0, remaining - batchSize)

          return [
            nextRemaining,
            SyncMessage.PullResponse.make({
              batch: response.batch,
              pageInfo:
                nextRemaining > 0 ? SyncBackend.pageInfoMoreKnown(nextRemaining) : SyncBackend.pageInfoNoMore,
              backendId,
            }),
          ] as const
        }),
        // Emit empty response if no events
        Stream.emitIfEmpty(SyncMessage.emptyPullResponse(backendId)),
      )
    }).pipe(
      Stream.unwrap,
      Stream.mapError((cause) => InvalidPullError.make({ cause })),
      Stream.withSpan('@livestore/sync-sqlite:pull'),
    )

/**
 * Create push handler
 *
 * Validates and persists events atomically.
 * Updates current head and validates parent sequence.
 */
export const makePushHandler =
  (storage: StoreStorage) =>
  (req: SyncMessage.PushRequest): Effect.Effect<SyncMessage.PushAck, InvalidPushError> =>
    Effect.gen(function* () {
      // Empty batch - nothing to do
      if (req.batch.length === 0) {
        return SyncMessage.PushAck.make({})
      }

      // Get backend ID
      const backendId = yield* storage.getBackendId()

      // Validate backendId if provided
      if (req.backendId._tag === 'Some' && req.backendId.value !== backendId) {
        return yield* new BackendIdMismatchError({
          expected: backendId,
          received: req.backendId.value,
        })
      }

      // Execute push within transaction for atomicity
      yield* storage.transaction(
        Effect.gen(function* () {
          // 1. Get current head
          const currentHead = yield* storage.getCurrentHead()

          // 2. Validate parent sequence
          const firstEventParent = req.batch[0]!.parentSeqNum
          if (firstEventParent !== currentHead) {
            return yield* new ServerAheadError({
              minimumExpectedNum: currentHead,
              providedNum: firstEventParent,
            })
          }

          // 3. Persist events
          const createdAt = new Date().toISOString()
          yield* storage.appendEvents(req.batch, createdAt)

          // 4. Update head
          const newHead = req.batch[req.batch.length - 1]!.seqNum
          yield* storage.setCurrentHead(newHead)
        }),
      )

      return SyncMessage.PushAck.make({})
    }).pipe(
      Effect.mapError((cause) => InvalidPushError.make({ cause })),
      Effect.withSpan('@livestore/sync-sqlite:push', {
        attributes: { batchSize: req.batch.length },
      }),
    )

/**
 * Create ping handler
 *
 * Simple health check that returns a pong response.
 */
export const makePingHandler =
  (_storage: StoreStorage) =>
  (_req: SyncMessage.Ping): Effect.Effect<SyncMessage.Pong, never> =>
    Effect.succeed(SyncMessage.Pong.make({}))
