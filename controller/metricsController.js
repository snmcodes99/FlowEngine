/**
 * Metrics Controller — HTTP handler for GET /metrics.
 *
 * Derives metrics from MongoDB as the source of truth (D-11 fix).
 * No longer relies on volatile Redis counters.
 *
 * Metrics returned:
 * - totalEvents: count of events in the stream (or all streams)
 * - failedEvents: count of DLQ entries
 * - latestOffset: highest committed offset for the stream
 * - consumerLag: latestOffset - committed consumer offset (when checkpoints exist)
 */
const Event = require('../model/publishModel');
const DLQ = require('../model/dlqModel');
const StreamCounter = require('../model/streamCounterModel');
const ConsumerCheckpoint = require('../model/checkpointModel');

const getMetrics = async (req, res) => {
  try {
    const type = req.query.type;
    const requestedGroup = req.query.consumerGroup;

    if (!type || typeof type !== 'string') {
      return res.status(400).json({
        error: 'Validation failed',
        details: ['type query parameter is required'],
      });
    }

    // Derive all metrics from MongoDB — source of truth
    const [totalEvents, failedEvents, counter, checkpoints] = await Promise.all([
      Event.countDocuments({ type }),
      DLQ.countDocuments({ type, status: 'pending' }),
      StreamCounter.findOne({ stream: type }).lean(),
      ConsumerCheckpoint.find({ stream: type }).lean(),
    ]);

    const latestOffset = counter ? counter.lastOffset : 0;

    // Build consumer lag details for all consumer groups tracking this stream
    const consumerGroups = checkpoints.map((cp) => ({
      consumerGroup: cp.consumerGroup,
      committedOffset: cp.committedOffset,
      consumerLag: Math.max(0, latestOffset - cp.committedOffset),
      lastCommitAt: cp.lastCommitAt,
    }));

    // If a specific consumerGroup or offset was queried
    let consumerLag = 0;
    let consumerOffset = 0;

    if (requestedGroup) {
      const match = checkpoints.find((cp) => cp.consumerGroup === requestedGroup);
      consumerOffset = match ? match.committedOffset : 0;
      consumerLag = Math.max(0, latestOffset - consumerOffset);
    } else if (req.query.offset !== undefined) {
      consumerOffset = parseInt(req.query.offset, 10) || 0;
      consumerLag = Math.max(0, latestOffset - consumerOffset);
    } else if (consumerGroups.length > 0) {
      // Default to the max lag across active groups
      consumerLag = Math.max(...consumerGroups.map((g) => g.consumerLag));
      consumerOffset = Math.min(...consumerGroups.map((g) => g.committedOffset));
    }

    return res.status(200).json({
      stream: type,
      totalEvents,
      failedEvents,
      latestOffset,
      consumerOffset,
      consumerLag,
      consumerGroups,
    });
  } catch (err) {
    console.error('Metrics error:', err.message);
    return res.status(500).json({
      error: 'Failed to fetch metrics',
      message: err.message,
    });
  }
};

module.exports = getMetrics;