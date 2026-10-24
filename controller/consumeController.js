/**
 * Consume Controller — HTTP handler for GET /consume.
 *
 * Returns events from a stream after a given offset, sorted by offset ascending.
 * Supports configurable batch size via `limit` query parameter.
 */
const Event = require('../model/publishModel');
const config = require('../config');

const ConsumeEvents = async (req, res) => {
  try {
    const type = req.query.type;
    const offset = parseInt(req.query.offset, 10);
    const limit = Math.min(
      parseInt(req.query.limit, 10) || config.consumer.batchSize,
      config.consumer.batchSize
    );

    // Input validation
    if (!type || typeof type !== 'string') {
      return res.status(400).json({
        error: 'Validation failed',
        details: ['type query parameter is required'],
      });
    }

    if (isNaN(offset) || offset < 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: ['offset query parameter must be a non-negative integer'],
      });
    }

    const events = await Event.find({
      type,
      offset: { $gt: offset },
    })
      .sort({ offset: 1 })
      .limit(limit)
      .lean();

    const nextOffset = events.length > 0
      ? events[events.length - 1].offset
      : offset;

    return res.status(200).json({
      events,
      nextOffset,
      count: events.length,
    });
  } catch (err) {
    console.error('Consume error:', err.message);
    return res.status(500).json({
      error: 'Failed to fetch events',
      message: err.message,
    });
  }
};

module.exports = ConsumeEvents;