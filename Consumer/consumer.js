/**
 * EventFlow Consumer Worker.
 *
 * A standalone process that polls the EventFlow API for new events,
 * processes them sequentially, and persists checkpoints to MongoDB.
 *
 * Fixes from audit:
 * - D-04: Durable checkpoints in MongoDB (no longer in-memory)
 * - D-05: Offset only advances after confirmed DLQ write
 * - D-06: Correctly reads response.data.events (not response.data)
 * - D-07: Starts from committed offset (default 0), not 1
 * - D-17: Configurable via environment variables
 *
 * Delivery guarantee: at-least-once.
 * If the worker crashes after processing but before committing the checkpoint,
 * the event will be re-delivered on restart. Consumers should be idempotent.
 */
require('dotenv').config();
const axios = require('axios');
const mongoose = require('mongoose');
const ConsumerCheckpoint = require('../model/checkpointModel');

// ── Configuration ──────────────────────────────────────────────────────
const CONFIG = {
  serverUrl: process.env.EVENTFLOW_SERVER_URL || 'http://localhost:3000',
  mongoUri: process.env.MONGO_URI || 'mongodb://localhost:27017/eventflow?replicaSet=rs0',
  apiKey: process.env.API_KEY || '',
  consumerGroup: process.env.CONSUMER_GROUP || 'default',
  stream: process.env.CONSUMER_STREAM || 'payment.authorized',
  pollIntervalMs: parseInt(process.env.CONSUMER_POLL_INTERVAL_MS, 10) || 2000,
  batchSize: parseInt(process.env.CONSUMER_BATCH_SIZE, 10) || 100,
  maxRetryAttempts: parseInt(process.env.MAX_RETRY_ATTEMPTS, 10) || 3,
  baseRetryDelayMs: parseInt(process.env.BASE_RETRY_DELAY_MS, 10) || 500,
  maxRetryDelayMs: parseInt(process.env.MAX_RETRY_DELAY_MS, 10) || 10000,
};

const apiClient = axios.create({
  baseURL: CONFIG.serverUrl,
  headers: CONFIG.apiKey ? { 'x-api-key': CONFIG.apiKey } : {},
});

// ── Utilities ──────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Calculate retry delay with exponential backoff and jitter.
 * delay = min(maxDelay, baseDelay * 2^attempt) + random jitter
 */
function getRetryDelay(attempt) {
  const exponential = CONFIG.baseRetryDelayMs * Math.pow(2, attempt);
  const capped = Math.min(exponential, CONFIG.maxRetryDelayMs);
  const jitter = Math.random() * capped * 0.3; // up to 30% jitter
  return Math.floor(capped + jitter);
}

// ── Checkpoint Management ──────────────────────────────────────────────
/**
 * Load the last committed offset from MongoDB.
 * Returns 0 if no checkpoint exists (start from the beginning).
 */
async function loadCheckpoint() {
  const checkpoint = await ConsumerCheckpoint.findOne({
    consumerGroup: CONFIG.consumerGroup,
    stream: CONFIG.stream,
  }).lean();

  return checkpoint ? checkpoint.committedOffset : 0;
}

/**
 * Persist the committed offset to MongoDB.
 * Uses upsert to handle both new and existing checkpoints.
 */
async function commitCheckpoint(offset) {
  await ConsumerCheckpoint.findOneAndUpdate(
    { consumerGroup: CONFIG.consumerGroup, stream: CONFIG.stream },
    { committedOffset: offset, lastCommitAt: new Date() },
    { upsert: true, new: true }
  );
}

// ── Event Processing ───────────────────────────────────────────────────
/**
 * Process a single event.
 * Replace this with actual business logic.
 * Currently simulates a 10% failure rate for testing retry/DLQ behavior.
 */
async function processEvent(event) {
  if (Math.random() < 0.1) {
    throw new Error(`Simulated processing failure for event ${event.eventId} at offset ${event.offset}`);
  }
  console.log(`[${CONFIG.consumerGroup}] ✅ Processed: stream=${event.type} offset=${event.offset} eventId=${event.eventId}`);
}

// ── DLQ ────────────────────────────────────────────────────────────────
/**
 * Send a failed event to the DLQ via the EventFlow API.
 * Retries the DLQ write itself to prevent silent data loss (D-05 fix).
 * Throws if the DLQ write ultimately fails — the caller must NOT advance the offset.
 */
async function sendToDLQ(event, errorLog) {
  const maxDlqAttempts = 3;

  for (let attempt = 1; attempt <= maxDlqAttempts; attempt++) {
    try {
      await apiClient.post('/dlq', {
        eventId: event.eventId,
        type: event.type,
        data: event.data,
        offset: event.offset,
        reason: 'Max retries exceeded',
        attempts: CONFIG.maxRetryAttempts,
        errorLog,
        consumerGroup: CONFIG.consumerGroup,
      });

      console.log(`[${CONFIG.consumerGroup}] 💀 DLQ: offset=${event.offset} eventId=${event.eventId}`);
      return; // DLQ write succeeded
    } catch (err) {
      console.error(`[${CONFIG.consumerGroup}] DLQ write attempt ${attempt}/${maxDlqAttempts} failed: ${err.message}`);
      if (attempt < maxDlqAttempts) {
        await sleep(getRetryDelay(attempt));
      }
    }
  }

  // All DLQ write attempts failed — throw to prevent offset advancement
  throw new Error(`Failed to write event ${event.eventId} to DLQ after ${maxDlqAttempts} attempts. Offset NOT advanced.`);
}

// ── Main Consumer Loop ─────────────────────────────────────────────────
async function consume() {
  // Connect to MongoDB for checkpoint persistence
  await mongoose.connect(CONFIG.mongoUri);
  console.log(`[${CONFIG.consumerGroup}] Connected to MongoDB for checkpoints`);

  // Load last committed checkpoint
  let lastOffset = await loadCheckpoint();
  console.log(`[${CONFIG.consumerGroup}] Starting consumption: stream=${CONFIG.stream} from offset=${lastOffset}`);

  while (true) {
    try {
      // Fetch events after the last committed offset (D-06 fix: read response.data.events)
      const response = await apiClient.get('/consume', {
        params: {
          type: CONFIG.stream,
          offset: lastOffset,
          limit: CONFIG.batchSize,
        },
      });

      const events = response.data.events; // D-06 fix: was incorrectly response.data

      if (!events || events.length === 0) {
        await sleep(CONFIG.pollIntervalMs);
        continue;
      }

      console.log(`[${CONFIG.consumerGroup}] Fetched ${events.length} events (offsets ${events[0].offset}-${events[events.length - 1].offset})`);

      // Process events sequentially
      for (const event of events) {
        let success = false;
        let attempts = 0;
        const errorLog = [];

        while (!success && attempts < CONFIG.maxRetryAttempts) {
          try {
            await processEvent(event);
            success = true;
          } catch (err) {
            attempts++;
            errorLog.push({
              message: err.message,
              attemptNumber: attempts,
              timestamp: new Date(),
            });
            console.log(`[${CONFIG.consumerGroup}] ⚠️ Retry ${attempts}/${CONFIG.maxRetryAttempts} for offset=${event.offset}: ${err.message}`);

            if (attempts < CONFIG.maxRetryAttempts) {
              await sleep(getRetryDelay(attempts));
            }
          }
        }

        if (!success) {
          // All retries exhausted — write to DLQ
          // D-05 fix: sendToDLQ throws if write fails; offset won't advance
          try {
            await sendToDLQ(event, errorLog);
          } catch (dlqErr) {
            // DLQ write failed — STOP processing to prevent data loss
            console.error(`[${CONFIG.consumerGroup}] 🛑 FATAL: ${dlqErr.message}`);
            console.error(`[${CONFIG.consumerGroup}] Consumer halting to prevent data loss. Last committed offset: ${lastOffset}`);
            // Wait and retry from the same offset on next iteration
            await sleep(CONFIG.pollIntervalMs * 5);
            break; // Break inner loop, outer loop retries from lastOffset
          }
        }

        // Only advance and commit checkpoint after successful processing OR confirmed DLQ write
        lastOffset = event.offset;
        await commitCheckpoint(lastOffset);
      }
    } catch (err) {
      console.error(`[${CONFIG.consumerGroup}] Error in consume loop: ${err.message}`);
      await sleep(CONFIG.pollIntervalMs);
    }
  }
}

// ── Graceful Shutdown ──────────────────────────────────────────────────
let shuttingDown = false;

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\n[${CONFIG.consumerGroup}] Received ${signal}. Shutting down gracefully...`);

  try {
    await mongoose.connection.close();
    console.log(`[${CONFIG.consumerGroup}] MongoDB connection closed.`);
  } catch (err) {
    console.error(`[${CONFIG.consumerGroup}] Error during shutdown:`, err.message);
  }

  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Start the consumer
consume().catch((err) => {
  console.error(`[${CONFIG.consumerGroup}] Fatal error:`, err.message);
  process.exit(1);
});