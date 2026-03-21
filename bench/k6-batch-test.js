import http from 'k6/http';
import { check } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const CHAIN_PATH = __ENV.CHAIN_PATH || '/polygon';

export const options = {
  scenarios: {
    batch_load: {
      executor: 'constant-vus',
      vus: 20,
      duration: '30s',
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<2000'],
    http_req_failed: ['rate<0.05'],
  },
};

// Batch of 10 mixed requests
const batchBody = JSON.stringify([
  { jsonrpc: '2.0', method: 'eth_blockNumber', params: [], id: 1 },
  { jsonrpc: '2.0', method: 'eth_chainId', params: [], id: 2 },
  { jsonrpc: '2.0', method: 'eth_gasPrice', params: [], id: 3 },
  { jsonrpc: '2.0', method: 'eth_blockNumber', params: [], id: 4 },
  { jsonrpc: '2.0', method: 'eth_chainId', params: [], id: 5 },
  { jsonrpc: '2.0', method: 'eth_getBalance', params: ['0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045', 'latest'], id: 6 },
  { jsonrpc: '2.0', method: 'eth_blockNumber', params: [], id: 7 },
  { jsonrpc: '2.0', method: 'eth_gasPrice', params: [], id: 8 },
  { jsonrpc: '2.0', method: 'eth_chainId', params: [], id: 9 },
  { jsonrpc: '2.0', method: 'eth_getBlockByNumber', params: ['latest', false], id: 10 },
]);

export default function () {
  const res = http.post(`${BASE_URL}${CHAIN_PATH}`, batchBody, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
    'response is array': (r) => {
      try {
        const body = JSON.parse(r.body);
        return Array.isArray(body) && body.length === 10;
      } catch {
        return false;
      }
    },
  });
}
