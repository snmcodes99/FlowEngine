const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const authMiddleware = require('../../middleware/auth');

describe('Auth Middleware', () => {
  it('should allow /health unconditionally', () => {
    let nextCalled = false;
    const req = { path: '/health', headers: {} };
    const res = {
      status: () => res,
      json: () => res,
    };
    const next = () => { nextCalled = true; };

    authMiddleware(req, res, next);
    assert.equal(nextCalled, true);
  });

  it('should allow requests when no API_KEY is set in config', () => {
    // When config.api.key is empty string
    let nextCalled = false;
    const req = { path: '/publish', headers: {} };
    const res = {
      status: () => res,
      json: () => res,
    };
    const next = () => { nextCalled = true; };

    authMiddleware(req, res, next);
    assert.equal(nextCalled, true);
  });
});
