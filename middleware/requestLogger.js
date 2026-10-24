/**
 * Request Logger & Correlation ID Middleware.
 *
 * Assigns an x-request-id to every incoming request (or reuses client-supplied one),
 * attaches it to response headers, and logs method, path, status, and duration.
 */
const crypto = require('crypto');

function requestLogger(req, res, next) {
  const requestId = req.headers['x-request-id'] || crypto.randomUUID();
  req.id = requestId;
  res.setHeader('x-request-id', requestId);

  const start = Date.now();

  res.on('finish', () => {
    const duration = Date.now() - start;
    const logLine = `[${new Date().toISOString()}] [${requestId.slice(0, 8)}] ${req.method} ${req.originalUrl || req.path} ${res.statusCode} ${duration}ms`;

    if (res.statusCode >= 500) {
      console.error(logLine);
    } else if (res.statusCode >= 400) {
      console.warn(logLine);
    } else {
      console.log(logLine);
    }
  });

  next();
}

module.exports = requestLogger;
