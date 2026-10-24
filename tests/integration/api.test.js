/**
 * ============================================================================
 * 🧪 WELCOME TO TESTING IN NODE.JS!
 * ============================================================================
 * Testing is simply writing code that checks if your real code works as expected.
 *
 * Every test follows 3 simple steps (the "AAA" pattern):
 *   1. ARRANGE: Prepare the input or URL.
 *   2. ACT:     Call the function or make an HTTP request.
 *   3. ASSERT:  Check that the result equals what you expected.
 *
 * Key tools:
 *   - test('name', async () => { ... }) : Defines a test case.
 *   - assert.equal(actual, expected)    : Fails the test if actual !== expected.
 *   - assert.ok(condition)              : Fails if condition is false.
 */

const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const http = require('http');
const app = require('../../app');

let server;
let baseUrl;

// ----------------------------------------------------------------------------
// SETUP & TEARDOWN
// Before tests run, we start our Express server on a temporary port.
// After tests finish, we shut it down.
// ----------------------------------------------------------------------------
before(async () => {
  return new Promise((resolve) => {
    server = http.createServer(app);
    // Port 0 tells the operating system to pick any available free port
    server.listen(0, () => {
      const port = server.address().port;
      baseUrl = `http://127.0.0.1:${port}`;
      resolve();
    });
  });
});

after(async () => {
  return new Promise((resolve) => {
    server.close(resolve);
  });
});

// ============================================================================
// TEST 1: Health Check Endpoint
// ============================================================================
test('GET /health returns status code 200 or 503', async () => {
  // 1. ACT: Call the /health endpoint
  const response = await fetch(`${baseUrl}/health`);

  // 2. ASSERT: We expect a valid HTTP response
  assert.ok(response.status === 200 || response.status === 503);

  const data = await response.json();
  // Check that the returned JSON has a 'status' field (e.g. "healthy" or "degraded")
  assert.ok(data.status);
});

// ============================================================================
// TEST 2: 404 Route Not Found
// ============================================================================
test('GET /unknown-path returns 404 Not Found', async () => {
  // 1. ACT: Call a route that does not exist
  const response = await fetch(`${baseUrl}/does-not-exist`);

  // 2. ASSERT: Expect HTTP 404 status code
  assert.equal(response.status, 404);

  const data = await response.json();
  assert.equal(data.error, 'Not found');
});

// ============================================================================
// TEST 3: Validation - Missing "type" in Publish
// ============================================================================
test('POST /publish rejects request when "type" is missing', async () => {
  // 1. ARRANGE: Prepare payload with NO "type" field
  const invalidPayload = {
    data: { amount: 100 }
  };

  // 2. ACT: Send POST request
  const response = await fetch(`${baseUrl}/publish`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(invalidPayload),
  });

  // 3. ASSERT: Expect 400 Bad Request
  assert.equal(response.status, 400);

  const data = await response.json();
  assert.equal(data.error, 'Validation failed');
});

// ============================================================================
// TEST 4: Validation - Invalid "type" with spaces
// ============================================================================
test('POST /publish rejects request when "type" has invalid characters', async () => {
  // "type" should only contain alphanumeric, dots, hyphens, underscores (no spaces)
  const response = await fetch(`${baseUrl}/publish`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      type: 'invalid stream name with spaces',
      data: { amount: 50 },
    }),
  });

  assert.equal(response.status, 400);
});

// ============================================================================
// TEST 5: Validation - Missing parameters in Consume
// ============================================================================
test('GET /consume rejects request without "type" query param', async () => {
  // 1. ACT: Call /consume with offset=0, but forgetting ?type=...
  const response = await fetch(`${baseUrl}/consume?offset=0`);

  // 2. ASSERT: Expect 400 Bad Request
  assert.equal(response.status, 400);

  const data = await response.json();
  assert.equal(data.error, 'Validation failed');
});

// ============================================================================
// TEST 6: Validation - Negative offset in Checkpoints
// ============================================================================
test('POST /checkpoints rejects negative committedOffset', async () => {
  // Offsets cannot be negative (e.g. -1 is invalid)
  const response = await fetch(`${baseUrl}/checkpoints`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      consumerGroup: 'payment-group',
      stream: 'payment.authorized',
      committedOffset: -5,
    }),
  });

  assert.equal(response.status, 400);
});

// ============================================================================
// TEST 7: Tracing - Request ID header is present
// ============================================================================
test('All responses include an x-request-id header for tracing', async () => {
  const response = await fetch(`${baseUrl}/health`);

  // Check that the server attached an 'x-request-id' header
  const requestId = response.headers.get('x-request-id');
  assert.ok(requestId, 'Response should contain an x-request-id header');
});
