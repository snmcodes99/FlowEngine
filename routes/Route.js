/**
 * Route definitions for EventFlow API.
 *
 * Routes:
 * - POST /publish    — Publish an event to a stream
 * - GET  /consume    — Consume events from a stream after a given offset
 * - POST /dlq        — Record a failed event in the dead-letter queue
 * - GET  /dlq        — Inspect DLQ entries
 * - GET  /metrics    — Get stream metrics (event counts, lag)
 * - GET  /health     — Health check with dependency status
 */
const express = require('express');
const router = express.Router();

const publishFunc = require('../controller/publishController');
const consumeFunc = require('../controller/consumeController');
const { addToDLQ, getDLQ, replayDLQ } = require('../controller/dlqController');
const { listCheckpoints, getCheckpoint, setCheckpoint } = require('../controller/checkpointController');
const metricsFunc = require('../controller/metricsController');
const healthFunc = require('../controller/healthController');

router.post('/publish', publishFunc);
router.get('/consume', consumeFunc);
router.post('/dlq', addToDLQ);
router.get('/dlq', getDLQ);
router.post('/dlq/replay/:id', replayDLQ);
router.get('/checkpoints', listCheckpoints);
router.get('/checkpoints/:group/:stream', getCheckpoint);
router.post('/checkpoints', setCheckpoint);
router.get('/metrics', metricsFunc);
router.get('/health', healthFunc);

module.exports = router;