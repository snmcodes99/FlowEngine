/**
 * StreamCounter model.
 * Maintains a per-stream monotonically increasing counter for offset allocation.
 * Used within a MongoDB transaction to atomically allocate offsets during publishing.
 *
 * Each document represents one event stream (identified by `stream` field).
 * The `lastOffset` field holds the highest committed offset for that stream.
 */
const mongoose = require('mongoose');

const streamCounterSchema = new mongoose.Schema({
  stream: {
    type: String,
    required: [true, 'Stream name is required'],
    unique: true,
    index: true,
  },
  lastOffset: {
    type: Number,
    required: true,
    default: 0,
    min: [0, 'Offset cannot be negative'],
  },
}, {
  timestamps: true,
});

module.exports = mongoose.model('StreamCounter', streamCounterSchema);
