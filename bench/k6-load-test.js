import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend } from 'k6/metrics';

// Custom metrics
const cacheHitRate = new Rate('cache_hit_rate');
const rpcLatency = new Trend('rpc_latency', true);

// Configuration — adjust BASE_URL and CHAIN_PATH for your setup
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const CHAIN_PATH = __ENV.CHAIN_PATH || '/polygon';

export const options = {
  scenarios: {
    // Ramp-up load test
    load_test: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '10s', target: 10 },
        { duration: '30s', target: 50 },
        { duration: '30s', target: 100 },
        { duration: '10s', target: 0 },
      ],
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<500', 'p(99)<1000'],
    http_req_failed: ['rate<0.05'],
  },
};

const requests = [
  {
    name: 'eth_blockNumber',
    body: JSON.stringify({ jsonrpc: '2.0', method: 'eth_blockNumber', params: [], id: 1 }),
    weight: 30,
  },
  {
    name: 'eth_chainId',
    body: JSON.stringify({ jsonrpc: '2.0', method: 'eth_chainId', params: [], id: 1 }),
    weight: 20, // should be cached
  },
  {
    name: 'eth_gasPrice',
    body: JSON.stringify({ jsonrpc: '2.0', method: 'eth_gasPrice', params: [], id: 1 }),
    weight: 20,
  },
  {
    name: 'eth_getBalance',
    body: JSON.stringify({
      jsonrpc: '2.0',
      method: 'eth_getBalance',
      params: ['0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045', 'latest'],
      id: 1,
    }),
    weight: 15,
  },
  {
    name: 'eth_getBlockByNumber',
    body: JSON.stringify({
      jsonrpc: '2.0',
      method: 'eth_getBlockByNumber',
      params: ['latest', false],
      id: 1,
    }),
    weight: 15,
  },
];

// Weighted random selection
function pickRequest() {
  const totalWeight = requests.reduce((sum, r) => sum + r.weight, 0);
  let rand = Math.random() * totalWeight;
  for (const req of requests) {
    rand -= req.weight;
    if (rand <= 0) return req;
  }
  return requests[0];
}

export default function () {
  const req = pickRequest();
  const params = {
    headers: { 'Content-Type': 'application/json' },
    tags: { method: req.name },
  };

  const res = http.post(`${BASE_URL}${CHAIN_PATH}`, req.body, params);

  rpcLatency.add(res.timings.duration);

  check(res, {
    'status is 200': (r) => r.status === 200,
    'has jsonrpc field': (r) => {
      try {
        const body = JSON.parse(r.body);
        return body.jsonrpc === '2.0';
      } catch {
        return false;
      }
    },
    'no rpc error': (r) => {
      try {
        const body = JSON.parse(r.body);
        return !body.error;
      } catch {
        return false;
      }
    },
  });

  // Small think time to simulate real usage
  sleep(0.1);
}

// After test — fetch dashboard status
export function handleSummary(data) {
  const summary = {
    total_requests: data.metrics.http_reqs.values.count,
    rps: data.metrics.http_reqs.values.rate.toFixed(1),
    avg_latency_ms: data.metrics.http_req_duration.values.avg.toFixed(1),
    p95_latency_ms: data.metrics.http_req_duration.values['p(95)'].toFixed(1),
    p99_latency_ms: data.metrics.http_req_duration.values['p(99)'].toFixed(1),
    error_rate: ((data.metrics.http_req_failed.values.rate || 0) * 100).toFixed(2) + '%',
  };

  console.log('\n=== RPC Aggregator Load Test Results ===');
  console.log(`Total requests:  ${summary.total_requests}`);
  console.log(`Throughput:      ${summary.rps} req/s`);
  console.log(`Avg latency:     ${summary.avg_latency_ms}ms`);
  console.log(`P95 latency:     ${summary.p95_latency_ms}ms`);
  console.log(`P99 latency:     ${summary.p99_latency_ms}ms`);
  console.log(`Error rate:      ${summary.error_rate}`);
  console.log('========================================\n');

  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    'bench/results.json': JSON.stringify(summary, null, 2),
  };
}

import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';
