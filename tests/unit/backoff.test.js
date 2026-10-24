const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

// Exponential backoff calculator matching consumer logic
function getRetryDelay(attempt, baseDelay = 500, maxDelay = 10000) {
  const exponential = baseDelay * Math.pow(2, attempt);
  const capped = Math.min(exponential, maxDelay);
  const jitter = Math.random() * capped * 0.3;
  return Math.floor(capped + jitter);
}

describe('Retry Backoff & Jitter', () => {
  it('should scale exponentially with attempt number', () => {
    const delay0 = getRetryDelay(0, 100, 5000);
    const delay1 = getRetryDelay(1, 100, 5000);
    const delay2 = getRetryDelay(2, 100, 5000);

    // base: attempt 0 -> 100, attempt 1 -> 200, attempt 2 -> 400
    assert.ok(delay0 >= 100 && delay0 <= 130);
    assert.ok(delay1 >= 200 && delay1 <= 260);
    assert.ok(delay2 >= 400 && delay2 <= 520);
  });

  it('should cap delay at maxRetryDelayMs', () => {
    const maxDelay = 1000;
    const delay10 = getRetryDelay(10, 100, maxDelay);
    // capped at 1000 + up to 30% jitter -> max 1300
    assert.ok(delay10 <= 1300);
    assert.ok(delay10 >= 1000);
  });
});
