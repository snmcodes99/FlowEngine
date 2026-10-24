/**
 * DLQ Controller — HTTP handler for POST /dlq and GET /dlq.
 *
 * POST /dlq — Records a failed event in the dead-letter queue.
 * GET /dlq  — Retrieves DLQ entries filtered by type and status.
 */
const DLQ = require('../model/dlqModel');

/**
 * POST /dlq — Add a failed event to the dead-letter queue.
 * Idempotent: duplicate (eventId, consumerGroup) pairs are handled gracefully.
 */
const addToDLQ = async (req, res) => {
  try {
    const { eventId, type, data, offset, reason, attempts, errorLog, consumerGroup } = req.body;

    // Input validation
    const validationErrors = [];
    if (!eventId || typeof eventId !== 'string') {
      validationErrors.push('eventId is required');
    }
    if (!type || typeof type !== 'string') {
      validationErrors.push('type is required');
    }
    if (!data || typeof data !== 'object') {
      validationErrors.push('data is required and must be an object');
    }
    if (offset === undefined || typeof offset !== 'number') {
      validationErrors.push('offset is required and must be a number');
    }

    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: validationErrors,
      });
    }

    const saved = await DLQ.create({
      eventId,
      type,
      data,
      offset,
      reason: reason || 'Processing failed',
      attempts: attempts || 3,
      errorLog: errorLog || [],
      consumerGroup: consumerGroup || 'default',
    });

    return res.status(201).json({
      message: 'Event moved to DLQ',
      dlqEntry: saved,
    });
  } catch (err) {
    // Handle duplicate DLQ entry gracefully (idempotent)
    if (err.code === 11000) {
      return res.status(200).json({
        message: 'Event already in DLQ (idempotent)',
      });
    }

    console.error('DLQ write error:', err.message);
    return res.status(500).json({
      error: 'Failed to write to DLQ',
      message: err.message,
    });
  }
};

/**
 * GET /dlq — Retrieve DLQ entries.
 * Query params: type (optional), status (optional, default: 'pending'), limit (optional)
 */
const getDLQ = async (req, res) => {
  try {
    const filter = {};
    if (req.query.type) filter.type = req.query.type;
    filter.status = req.query.status || 'pending';

    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);

    const entries = await DLQ.find(filter)
      .sort({ failedAt: -1 })
      .limit(limit)
      .lean();

    return res.status(200).json({
      entries,
      count: entries.length,
    });
  } catch (err) {
    console.error('DLQ read error:', err.message);
    return res.status(500).json({
      error: 'Failed to read DLQ',
      message: err.message,
    });
  }
};

/**
 * POST /dlq/replay/:id — Replay a DLQ entry by re-publishing it.
 * Marks the DLQ entry as 'replayed' and publishes the event back to the stream.
 */
const { publishEvent } = require('../service/publishService');

const replayDLQ = async (req, res) => {
  try {
    const { id } = req.params;

    const entry = await DLQ.findById(id);
    if (!entry) {
      return res.status(404).json({
        error: 'DLQ entry not found',
      });
    }

    if (entry.status !== 'pending') {
      return res.status(409).json({
        error: `DLQ entry already ${entry.status}`,
      });
    }

    // Re-publish the event to the stream (gets a new offset)
    const { event } = await publishEvent({
      type: entry.type,
      data: entry.data,
      metadata: { correlationId: entry.eventId },
    });

    // Mark as replayed
    entry.status = 'replayed';
    entry.replayedAt = new Date();
    await entry.save();

    return res.status(200).json({
      message: 'DLQ entry replayed',
      newEvent: event,
    });
  } catch (err) {
    console.error('DLQ replay error:', err.message);
    return res.status(500).json({
      error: 'Failed to replay DLQ entry',
      message: err.message,
    });
  }
};

module.exports = { addToDLQ, getDLQ, replayDLQ };