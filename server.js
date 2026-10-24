/**
 * Server entry point.
 * Connects to all dependencies before starting the HTTP server.
 * Exits with code 1 on any startup failure (D-16 fix).
 */
const config = require('./config');
const app = require('./app');
const ConnectToDB = require('./config/db');
const { ConnectRedis } = require('./config/redis');

async function start() {
  try {
    await ConnectToDB();

    try {
      await ConnectRedis();
    } catch (redisErr) {
      console.warn(`⚠️ Warning: Redis is not reachable at ${config.redis.host}:${config.redis.port}. Running in standalone MongoDB mode.`);
    }

    app.listen(config.port, () => {
      console.log(`EventFlow server running on port ${config.port} [${config.nodeEnv}]`);
    });
  } catch (error) {
    console.error('Failed to start EventFlow:', error.message);
    process.exit(1);
  }
}

// Handle unhandled rejections globally
process.on('unhandledRejection', (reason) => {
  console.error('Unhandled Rejection:', reason);
  process.exit(1);
});

start();