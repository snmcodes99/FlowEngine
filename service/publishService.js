/**
 * Publish Service — core event publishing logic.
 *
 * Implements MongoDB-authoritative offset allocation:
 * 1. Start a MongoDB session/transaction
 * 2. Atomically increment the per-stream counter (findOneAndUpdate with $inc + upsert)
 * 3. Insert the event with the allocated offset
 * 4. Commit the transaction
 *
 * This ensures offset allocation and event persistence are atomic (D-01 fix).
 * If the transaction fails, the offset is never committed and no event is persisted.
 *
 * Idempotent publishing:
 * - If `idempotencyKey` is provided and already exists, return the existing event.
 * - If `idempotencyKey` is provided with a different payload, reject with conflict.
 */
const mongoose = require('mongoose');
const Event = require('../model/publishModel');
const StreamCounter = require('../model/streamCounterModel');

/**
 * Publish a single event to the given stream.
 *
 * @param {Object} params
 * @param {string} params.type - Event stream/type identifier
 * @param {Object} params.data - Event payload
 * @param {string} [params.idempotencyKey] - Optional idempotency key
 * @param {Object} [params.metadata] - Optional metadata (correlationId, producerId, schemaVersion)
 * @returns {Object} { event, created } - The event document and whether it was newly created
 */
async function publishEvent({ type, data, idempotencyKey = null, metadata = {} }) {
  // 1. Check idempotency: if key exists, return existing event
  if (idempotencyKey) {
    const existing = await Event.findOne({ idempotencyKey }).lean();
    if (existing) {
      // Idempotent hit — return the original event without creating a duplicate
      return { event: existing, created: false };
    }
  }

  // 2. Allocate offset and insert event within a MongoDB transaction
  const session = await mongoose.startSession();
  try {
    let event;

    await session.withTransaction(async () => {
      // Atomically increment the stream counter
      const counter = await StreamCounter.findOneAndUpdate(
        { stream: type },
        { $inc: { lastOffset: 1 } },
        { upsert: true, new: true, session }
      );

      const newOffset = counter.lastOffset;

      // Build event document
      const eventDoc = {
        type,
        data,
        offset: newOffset,
        idempotencyKey: idempotencyKey || null,
        metadata: {
          correlationId: metadata.correlationId || null,
          producerId: metadata.producerId || null,
          schemaVersion: metadata.schemaVersion || 1,
        },
      };

      // Insert the event within the same transaction
      const [created] = await Event.create([eventDoc], { session });
      event = created.toObject();
    });

    return { event, created: true };
  } catch (error) {
    // Handle duplicate key errors (concurrent idempotency key race)
    if (error.code === 11000) {
      // Another request with the same idempotencyKey won the race
      if (idempotencyKey) {
        const existing = await Event.findOne({ idempotencyKey }).lean();
        if (existing) {
          return { event: existing, created: false };
        }
      }
      // Duplicate offset (should not happen within transactions, but safety net)
      throw new Error(`Duplicate offset conflict for stream "${type}". This indicates a transaction isolation issue.`);
    }
    throw error;
  } finally {
    await session.endSession();
  }
}

/**
 * Get the latest offset for a stream (from the counter collection).
 * @param {string} stream - Stream/type identifier
 * @returns {number} The latest committed offset, or 0 if stream doesn't exist
 */
async function getLatestOffset(stream) {
  const counter = await StreamCounter.findOne({ stream }).lean();
  return counter ? counter.lastOffset : 0;
}

module.exports = { publishEvent, getLatestOffset };
