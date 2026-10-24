/**
 * EventFlow Performance & Load Testing Benchmark
 * 
 * Measures:
 * - Throughput (Events Published / Sec)
 * - Latencies (Min, Max, Mean, P50, P95, P99)
 * - Concurrent publishing with atomic offsets
 */
const http = require('http');

const CONFIG = {
  url: process.env.EVENTFLOW_URL || 'http://localhost:3000',
  totalRequests: parseInt(process.env.BENCH_TOTAL, 10) || 500,
  concurrency: parseInt(process.env.BENCH_CONCURRENCY, 10) || 20,
  stream: 'benchmark.events',
};

async function sendPublishRequest(index) {
  const payload = JSON.stringify({
    type: CONFIG.stream,
    data: {
      orderId: `order_${index}`,
      amount: (Math.random() * 100).toFixed(2),
      timestamp: Date.now(),
    },
    idempotencyKey: `bench_key_${index}_${Date.now()}`,
    metadata: { benchmark: true },
  });

  const start = process.hrtime.bigint();

  const res = await fetch(`${CONFIG.url}/publish`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: payload,
  });

  const end = process.hrtime.bigint();
  const durationMs = Number(end - start) / 1_000_000;

  if (res.status !== 200 && res.status !== 201) {
    const body = await res.text();
    throw new Error(`HTTP ${res.status}: ${body}`);
  }

  return durationMs;
}

async function runBenchmark() {
  console.log(`\n⚡ Starting EventFlow Performance Benchmark...`);
  console.log(`Target URL:    ${CONFIG.url}`);
  console.log(`Total Events:  ${CONFIG.totalRequests}`);
  console.log(`Concurrency:   ${CONFIG.concurrency} parallel workers\n`);

  // First check if server is running
  try {
    const health = await fetch(`${CONFIG.url}/health`);
    if (!health.ok && health.status !== 503) {
      console.error(`❌ Server returned HTTP ${health.status} on /health`);
      process.exit(1);
    }
  } catch (err) {
    console.error(`❌ Could not connect to EventFlow server at ${CONFIG.url}`);
    console.error(`   Please ensure your server is running with 'npm start' or 'npm run dev' first.\n`);
    process.exit(1);
  }

  const latencies = [];
  let completed = 0;
  let errors = 0;

  const benchStartTime = Date.now();
  let currentIndex = 0;

  async function worker() {
    while (currentIndex < CONFIG.totalRequests) {
      const idx = currentIndex++;
      try {
        const ms = await sendPublishRequest(idx);
        latencies.push(ms);
      } catch (err) {
        errors++;
      }
      completed++;
      if (completed % 100 === 0 || completed === CONFIG.totalRequests) {
        process.stdout.write(`\rProgress: ${completed}/${CONFIG.totalRequests} events (${errors} errors)...`);
      }
    }
  }

  // Spawn parallel workers
  const workers = Array.from({ length: CONFIG.concurrency }, () => worker());
  await Promise.all(workers);

  const totalTimeSeconds = (Date.now() - benchStartTime) / 1000;
  latencies.sort((a, b) => a - b);

  const min = latencies[0] || 0;
  const max = latencies[latencies.length - 1] || 0;
  const mean = latencies.reduce((sum, v) => sum + v, 0) / (latencies.length || 1);
  const p50 = latencies[Math.floor(latencies.length * 0.50)] || 0;
  const p95 = latencies[Math.floor(latencies.length * 0.95)] || 0;
  const p99 = latencies[Math.floor(latencies.length * 0.99)] || 0;
  const rps = (latencies.length / totalTimeSeconds).toFixed(2);

  console.log(`\n\n==================================================`);
  console.log(`📊 PERFORMANCE BENCHMARK RESULTS`);
  console.log(`==================================================`);
  console.log(`Total Requests:      ${CONFIG.totalRequests}`);
  console.log(`Successful:          ${latencies.length}`);
  console.log(`Failed:              ${errors}`);
  console.log(`Total Duration:      ${totalTimeSeconds.toFixed(2)} seconds`);
  console.log(`Throughput:          ${rps} events / second`);
  console.log(`--------------------------------------------------`);
  console.log(`LATENCIES (Round-Trip):`);
  console.log(`  Min:               ${min.toFixed(2)} ms`);
  console.log(`  Mean (Avg):        ${mean.toFixed(2)} ms`);
  console.log(`  50th Percentile:   ${p50.toFixed(2)} ms (p50)`);
  console.log(`  95th Percentile:   ${p95.toFixed(2)} ms (p95)`);
  console.log(`  99th Percentile:   ${p99.toFixed(2)} ms (p99)`);
  console.log(`  Max:               ${max.toFixed(2)} ms`);
  console.log(`==================================================\n`);
}

runBenchmark().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
