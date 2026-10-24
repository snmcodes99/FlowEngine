/**
 * Redis connection module.
 * Used for caching and pub/sub only — NOT for offset allocation.
 * flushAll() has been removed to prevent data destruction on restart (D-02).
 */
const redis = require('redis');
const config = require('./index');

let client = null;

const ConnectRedis = async () => {
  try {
    const options = {
      socket: {
        host: config.redis.host,
        port: config.redis.port,
        reconnectStrategy: (retries) => {
          if (retries > 2) {
            // Stop retrying to avoid console flooding when Redis isn't running
            return false;
          }
          return 500;
        },
      },
    };

    if (config.redis.password) {
      options.password = config.redis.password;
    }

    const testClient = redis.createClient(options);

    testClient.on('error', (err) => {
      // Only log if client is actively connected, otherwise it's just startup unreachable
      if (testClient.isOpen) {
        console.error('Redis Client Error:', err.message);
      }
    });

    await testClient.connect();
    client = testClient;
    console.log(`Connected to Redis at ${config.redis.host}:${config.redis.port}`);
  } catch (error) {
    client = null;
    throw error;
  }
};

const getRedisClient = () => {
  return client;
};

module.exports = { ConnectRedis, getRedisClient };
