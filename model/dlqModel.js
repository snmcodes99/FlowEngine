/**
 * Dead Letter Queue (DLQ) model.
 * Stores events that exhausted all retry attempts during consumer processing.
 *
 * Enhanced from original:
 * - Added `eventId` linking back to the original event
 * - Added `attempts` count and `errors` array for debugging
 * - Added `status` for DLQ lifecycle (pending → replayed / discarded)
 * - Made fields required with validation
 */
const mongoose = require('mongoose');

const dlqSchema = new mongoose.Schema({
  eventId: {
    type: String,
    required: [true, 'Event ID is required'],
    index: true,
  },
  type: {
    type: String,
    required: [true, 'Event type is required'],
  },
  data: {
    type: mongoose.Schema.Types.Mixed,
    required: [true, 'Event data is required'],
  },
  offset: {
    type: Number,
    required: [true, 'Offset is required'],
  },
  reason: {
    type: String,
    required: true,
    default: 'Processing failed',
  },
  attempts: {
    type: Number,
    required: true,
    default: 3,
    min: 1,
  },
  errorLog: [{
    message: String,
    attemptNumber: Number,
    timestamp: { type: Date, default: Date.now },
  }],
  consumerGroup: {
    type: String,
    default: 'default',
  },
  status: {
    type: String,
    enum: ['pending', 'replayed', 'discarded'],
    default: 'pending',
    index: true,
  },
  failedAt: {
    type: Date,
    default: Date.now,
    immutable: true,
  },
  replayedAt: {
    type: Date,
    default: null,
  },
});

// Compound index for querying DLQ by stream and status
dlqSchema.index({ type: 1, status: 1 });

// Prevent duplicate DLQ entries for the same event in the same consumer group
dlqSchema.index({ eventId: 1, consumerGroup: 1 }, { unique: true });

module.exports = mongoose.model('DLQ', dlqSchema);