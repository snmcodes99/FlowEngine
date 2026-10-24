/**
 * Express application setup.
 * Configures middleware and routes.
 */
const express = require('express');
const config = require('./config');
const requestLogger = require('./middleware/requestLogger');
const authMiddleware = require('./middleware/auth');
const router = require('./routes/Route');

const app = express();

// Request logging & correlation IDs
app.use(requestLogger);

// Body parsing with size limits (D-13 fix)
const maxPayloadSize = `${config.publish.maxPayloadSizeKB}kb`;
app.use(express.json({ limit: maxPayloadSize }));
app.use(express.urlencoded({ extended: true, limit: maxPayloadSize }));

// Authentication (D-14 fix)
app.use(authMiddleware);

// Routes
app.use(router);

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    error: 'Not found',
    message: `Route ${req.method} ${req.path} does not exist`,
  });
});

// Global error handler — catches unhandled errors from async routes
app.use((err, req, res, _next) => {
  console.error('Unhandled error:', err.message);
  res.status(500).json({
    error: 'Internal server error',
    message: config.nodeEnv === 'production' ? 'An unexpected error occurred' : err.message,
  });
});

module.exports = app;