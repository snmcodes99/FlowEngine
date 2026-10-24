/**
 * Centralized, validated configuration module.
 * All environment-dependent values are read here and exported as a frozen object.
 * No other module should read process.env directly.
 */
require('dotenv').config();

const config = {
  port: parseInt(process.env.PORT, 10) || 3000,
  nodeEnv: process.env.NODE_ENV || 'development',

  mongo: {
    uri: process.env.MONGO_URI || 'mongodb://localhost:27017/eventflow?replicaSet=rs0',
  },

  redis: {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.REDIS_PORT, 10) || 6379,
    password: process.env.REDIS_PASSWORD || undefined,
  },

  api: {
    key: process.env.API_KEY || '',
  },

  publish: {
    maxPayloadSizeKB: parseInt(process.env.MAX_PAYLOAD_SIZE_KB, 10) || 1024,
  },

  consumer: {
    pollIntervalMs: parseInt(process.env.CONSUMER_POLL_INTERVAL_MS, 10) || 2000,
    batchSize: parseInt(process.env.CONSUMER_BATCH_SIZE, 10) || 100,
    maxRetryAttempts: parseInt(process.env.DEFAULT_MAX_RETRY_ATTEMPTS, 10) || 3,
  },
};

/**
 * Validate required configuration at startup.
 * Throws if critical values are missing or invalid.
 */
function validate() {
  const errors = [];

  if (!config.mongo.uri) {
    errors.push('MONGO_URI is required');
  }

  if (isNaN(config.port) || config.port < 1 || config.port > 65535) {
    errors.push(`PORT must be a valid port number, got: ${process.env.PORT}`);
  }

  if (isNaN(config.redis.port) || config.redis.port < 1 || config.redis.port > 65535) {
    errors.push(`REDIS_PORT must be a valid port number, got: ${process.env.REDIS_PORT}`);
  }

  if (errors.length > 0) {
    throw new Error(`Configuration validation failed:\n  - ${errors.join('\n  - ')}`);
  }
}

validate();

module.exports = Object.freeze(config);
