/**
 * API Key Authentication Middleware.
 *
 * If API_KEY is configured in the environment, incoming requests
 * must provide the key via `x-api-key` header or `Authorization: Bearer <key>`.
 * If API_KEY is empty or not configured, requests pass through (for local dev / unauthenticated setups).
 * Health check endpoint (/health) is always public.
 */
const config = require('../config');

function authMiddleware(req, res, next) {
  // Always permit health checks
  if (req.path === '/health') {
    return next();
  }

  const configuredKey = config.api.key;
  if (!configuredKey) {
    // No API key configured — authentication disabled
    return next();
  }

  const headerKey = req.headers['x-api-key'];
  const authHeader = req.headers['authorization'];
  let bearerKey = null;

  if (authHeader && authHeader.startsWith('Bearer ')) {
    bearerKey = authHeader.slice(7).trim();
  }

  const providedKey = headerKey || bearerKey;

  if (!providedKey || providedKey !== configuredKey) {
    return res.status(401).json({
      error: 'Unauthorized',
      message: 'Valid API key required via x-api-key header or Authorization: Bearer <key>',
    });
  }

  next();
}

module.exports = authMiddleware;
