/**
 * Event model — the append-only event log.
 *
 * Schema changes from original:
 * - Added `eventId` (UUID) as a stable unique identifier for idempotent consumers
 * - Added `idempotencyKey` for idempotent publishing (optional, unique when present)
 * - Added `createdAt` server-generated timestamp
 * - Added `metadata` for optional correlation/tracing data
 * - Made `type`, `data`, `offset` required with validation
 * - Made (type, offset) a UNIQUE compound index (D-09 fix)
 */
const mongoose = require('mongoose');
const crypto = require('crypto');

const eventSchema = new mongoose.Schema({
  eventId: {
    type: String,
    required: true,
    default: () => crypto.randomUUID(),
    index: true,
  },
  type: {
    type: String,
    required: [true, 'Event type is required'],
    trim: true,
    minlength: [1, 'Event type cannot be empty'],
    maxlength: [128, 'Event type cannot exceed 128 characters'],
    match: [/^[a-zA-Z0-9._-]+$/, 'Event type can only contain alphanumeric characters, dots, hyphens, and underscores'],
  },
  data: {
    type: mongoose.Schema.Types.Mixed,
    required: [true, 'Event data is required'],
    validate: {
      validator: function (v) {
        return v !== null && typeof v === 'object' && !Array.isArray(v);
      },
      message: 'Event data must be a non-null object',
    },
  },
  offset: {
    type: Number,
    required: [true, 'Offset is required'],
    min: [1, 'Offset must be at least 1'],
  },
  idempotencyKey: {
    type: String,
    default: null,
    maxlength: [256, 'Idempotency key cannot exceed 256 characters'],
  },
  metadata: {
    correlationId: { type: String, default: null },
    producerId: { type: String, default: null },
    schemaVersion: { type: Number, default: 1 },
  },
  createdAt: {
    type: Date,
    default: Date.now,
    immutable: true,
  },
});

// UNIQUE compound index: ensures no duplicate offsets within a stream (D-09 fix)
eventSchema.index({ type: 1, offset: 1 }, { unique: true });

// Sparse unique index on idempotencyKey for idempotent publishing
// Only enforced when idempotencyKey is present (non-null)
eventSchema.index({ idempotencyKey: 1 }, { unique: true, sparse: true });

// Index for efficient consume queries: find events in a stream after a given offset
eventSchema.index({ type: 1, offset: 1, createdAt: 1 });

module.exports = mongoose.model('Event', eventSchema);