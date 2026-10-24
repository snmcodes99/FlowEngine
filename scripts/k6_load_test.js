import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Custom metrics to track publish vs consume performance
const publishDuration = new Trend('publish_duration_ms');
const consumeDuration = new Trend('consume_duration_ms');
const publishedEvents = new Counter('events_published_total');

// ── Load Test Configuration ─────────────────────────────────────────────
export const options = {
  // Test stages: ramp-up -> steady peak load -> ramp-down
  stages: [
    { duration: '10s', target: 20 },  // Ramp-up to 20 virtual users (VUs)
    { duration: '30s', target: 50 },  // Stay at 50 VUs (Peak traffic)
    { duration: '10s', target: 0 },   // Ramp-down to 0
  ],

  // Pass / Fail criteria (SLAs)
  thresholds: {
    // 95% of all HTTP requests must complete within 250ms
    http_req_duration: ['p(95)<250', 'p(99)<500'],
    // Less than 1% of requests are allowed to fail
    http_req_failed: ['rate<0.01'],
    // Custom SLA for publish endpoint
    publish_duration_ms: ['p(95)<200'],
  },
};

const BASE_URL = __ENV.EVENTFLOW_URL || 'http://localhost:3000';
const STREAM = 'payment.loadtest';

// ── Main Scenario Loop (Executed by each Virtual User) ──────────────────
export default function () {
  const vuId = __VU;
  const iterId = __ITER;

  // 1. Publish Event (70% of traffic)
  const payload = JSON.stringify({
    type: STREAM,
    data: {
      account: `acc_${vuId}_${iterId}`,
      amount: +(Math.random() * 500).toFixed(2),
      currency: 'USD',
    },
    idempotencyKey: `txn_k6_${vuId}_${iterId}_${Date.now()}`,
    metadata: {
      vu: vuId,
      generator: 'k6',
    },
  });

  const headers = { 'Content-Type': 'application/json' };
  if (__ENV.API_KEY) {
    headers['x-api-key'] = __ENV.API_KEY;
  }

  const pubStart = Date.now();
  const pubRes = http.post(`${BASE_URL}/publish`, payload, { headers });
  publishDuration.add(Date.now() - pubStart);

  const pubSuccess = check(pubRes, {
    'publish: status is 201 or 200': (r) => r.status === 201 || r.status === 200,
    'publish: has offset': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.event && typeof body.event.offset === 'number';
      } catch (e) {
        return false;
      }
    },
  });

  if (pubSuccess) {
    publishedEvents.add(1);
  }

  // 2. Poll / Consume Events (20% chance per iteration)
  if (Math.random() < 0.20) {
    const consStart = Date.now();
    const consRes = http.get(`${BASE_URL}/consume?type=${STREAM}&offset=0&limit=50`, { headers });
    consumeDuration.add(Date.now() - consStart);

    check(consRes, {
      'consume: status is 200': (r) => r.status === 200,
      'consume: returns events array': (r) => {
        try {
          const body = JSON.parse(r.body);
          return Array.isArray(body.events);
        } catch (e) {
          return false;
        }
      },
    });
  }

  // 3. Inspect Metrics (10% chance per iteration)
  if (Math.random() < 0.10) {
    const metRes = http.get(`${BASE_URL}/metrics?type=${STREAM}`, { headers });
    check(metRes, {
      'metrics: status is 200': (r) => r.status === 200,
      'metrics: returns latestOffset': (r) => {
        try {
          const body = JSON.parse(r.body);
          return body.latestOffset >= 0;
        } catch (e) {
          return false;
        }
      },
    });
  }

  // Pacing: small sleep (50ms - 150ms) to simulate realistic client request intervals
  sleep(0.05 + Math.random() * 0.10);
}
