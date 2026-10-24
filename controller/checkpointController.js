/**
 * Checkpoint Controller — HTTP handlers for consumer checkpoint management.
 *
 * GET  /checkpoints       — List all checkpoints (optionally filtered by stream)
 * GET  /checkpoints/:group/:stream — Get a specific checkpoint
 * POST /checkpoints       — Manually set a checkpoint (for replay/admin)
 */
const ConsumerCheckpoint = require('../model/checkpointModel');

/**
 * GET /checkpoints — List checkpoints.
 * Query params: stream (optional), consumerGroup (optional)
 */
const listCheckpoints = async (req, res) => {
  try {
    const filter = {};
    if (req.query.stream) filter.stream = req.query.stream;
    if (req.query.consumerGroup) filter.consumerGroup = req.query.consumerGroup;

    const checkpoints = await ConsumerCheckpoint.find(filter)
      .sort({ consumerGroup: 1, stream: 1 })
      .lean();

    return res.status(200).json({
      checkpoints,
      count: checkpoints.length,
    });
  } catch (err) {
    console.error('Checkpoint list error:', err.message);
    return res.status(500).json({
      error: 'Failed to list checkpoints',
      message: err.message,
    });
  }
};

/**
 * GET /checkpoints/:group/:stream — Get a specific consumer group's checkpoint.
 */
const getCheckpoint = async (req, res) => {
  try {
    const { group, stream } = req.params;

    const checkpoint = await ConsumerCheckpoint.findOne({
      consumerGroup: group,
      stream,
    }).lean();

    if (!checkpoint) {
      return res.status(404).json({
        error: 'Checkpoint not found',
        message: `No checkpoint for group="${group}" stream="${stream}"`,
      });
    }

    return res.status(200).json({ checkpoint });
  } catch (err) {
    console.error('Checkpoint get error:', err.message);
    return res.status(500).json({
      error: 'Failed to get checkpoint',
      message: err.message,
    });
  }
};

/**
 * POST /checkpoints — Manually set a checkpoint.
 * Used for administrative replay (reset offset to replay from a specific position).
 * Body: { consumerGroup, stream, committedOffset }
 */
const setCheckpoint = async (req, res) => {
  try {
    const { consumerGroup, stream, committedOffset } = req.body;

    const errors = [];
    if (!consumerGroup || typeof consumerGroup !== 'string') {
      errors.push('consumerGroup is required');
    }
    if (!stream || typeof stream !== 'string') {
      errors.push('stream is required');
    }
    if (committedOffset === undefined || typeof committedOffset !== 'number' || committedOffset < 0) {
      errors.push('committedOffset must be a non-negative number');
    }

    if (errors.length > 0) {
      return res.status(400).json({ error: 'Validation failed', details: errors });
    }

    const checkpoint = await ConsumerCheckpoint.findOneAndUpdate(
      { consumerGroup, stream },
      { committedOffset, lastCommitAt: new Date() },
      { upsert: true, new: true }
    );

    return res.status(200).json({
      message: 'Checkpoint updated',
      checkpoint,
    });
  } catch (err) {
    console.error('Checkpoint set error:', err.message);
    return res.status(500).json({
      error: 'Failed to set checkpoint',
      message: err.message,
    });
  }
};

module.exports = { listCheckpoints, getCheckpoint, setCheckpoint };
