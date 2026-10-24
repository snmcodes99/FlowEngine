/**
 * Health Controller — HTTP handler for GET /health.
 * Reports server status and dependency connectivity.
 */
const mongoose = require('mongoose');

const healthCheck = async (req, res) => {
  const mongoStatus = mongoose.connection.readyState === 1 ? 'connected' : 'disconnected';

  let redisStatus = 'disconnected';
  try {
    const { getRedisClient } = require('../config/redis');
    const client = getRedisClient();
    if (client && client.isOpen) {
      await client.ping();
      redisStatus = 'connected';
    }
  } catch {
    redisStatus = 'disconnected';
  }

  const healthy = mongoStatus === 'connected' && redisStatus === 'connected';

  return res.status(healthy ? 200 : 503).json({
    status: healthy ? 'healthy' : 'degraded',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
    dependencies: {
      mongodb: mongoStatus,
      redis: redisStatus,
    },
  });
};

module.exports = healthCheck;