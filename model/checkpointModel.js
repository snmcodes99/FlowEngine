/**
 * ConsumerCheckpoint model.
 * Persists consumer group progress per stream in MongoDB.
 *
 * Each document tracks the last committed offset for a specific
 * (consumerGroup, stream) pair. Used for crash recovery and at-least-once delivery.
 */
const mongoose = require('mongoose');

const checkpointSchema = new mongoose.Schema({
  consumerGroup: {
    type: String,
    required: [true, 'Consumer group is required'],
    trim: true,
  },
  stream: {
    type: String,
    required: [true, 'Stream is required'],
    trim: true,
  },
  committedOffset: {
    type: Number,
    required: true,
    default: 0,
    min: 0,
  },
  lastCommitAt: {
    type: Date,
    default: Date.now,
  },
}, {
  timestamps: true,
});

// Unique compound index: one checkpoint per (consumerGroup, stream)
checkpointSchema.index({ consumerGroup: 1, stream: 1 }, { unique: true });

module.exports = mongoose.model('ConsumerCheckpoint', checkpointSchema);
