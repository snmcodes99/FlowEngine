/**
 * Publish Controller — HTTP handler for POST /publish.
 *
 * Validates the request body and delegates to the publish service.
 * Returns the created event or the existing event on idempotent retry.
 */
const { publishEvent } = require('../service/publishService');

const PublishEvent = async (req, res) => {
  try {
    const { type, data, idempotencyKey, metadata } = req.body;

    // Input validation
    const errors = [];
    if (!type || typeof type !== 'string') {
      errors.push('type is required and must be a non-empty string');
    } else if (!/^[a-zA-Z0-9._-]+$/.test(type)) {
      errors.push('type can only contain alphanumeric characters, dots, hyphens, and underscores');
    }

    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      errors.push('data is required and must be a non-null, non-array object');
    }

    if (idempotencyKey !== undefined && (typeof idempotencyKey !== 'string' || idempotencyKey.length === 0)) {
      errors.push('idempotencyKey must be a non-empty string when provided');
    }

    if (errors.length > 0) {
      return res.status(400).json({
        error: 'Validation failed',
        details: errors,
      });
    }

    const { event, created } = await publishEvent({
      type,
      data,
      idempotencyKey: idempotencyKey || null,
      metadata: metadata || {},
    });

    const statusCode = created ? 201 : 200;
    return res.status(statusCode).json({
      message: created ? 'Event published' : 'Event already exists (idempotent)',
      event,
    });
  } catch (err) {
    console.error('Publish error:', err.message);
    return res.status(500).json({
      error: 'Failed to publish event',
      message: err.message,
    });
  }
};

module.exports = PublishEvent;