# rpc-aggregator

A high-performance RPC aggregator that transparently proxies JSON-RPC requests across a scored pool of blockchain nodes with intelligent caching, automatic failover, and observability.

[![Go](https://img.shields.io/badge/Go-1.22%2B-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-224%20passed-brightgreen)]()

## Features

- **Multi-chain** — EVM and Solana chains via pluggable `ChainAdapter` interface
- **Transparent proxy** — consumers send standard JSON-RPC requests over HTTP or WebSocket
- **WebSocket subscriptions** — `eth_subscribe`, Solana `accountSubscribe`, shared upstream connections with automatic reconnect and re-subscribe
- **Weighted scoring** — selects the best node based on latency, block height, error rate, and cost tier
- **Smart finalization cache** — automatically caches responses forever when the requested block is below the chain's finalized height
- **Intelligent caching** — per-method cache rules with TTL, singleflight deduplication, immutable data cached forever (e.g. `eth_chainId`)
- **Circuit breaker** — sliding window error tracking, automatic node exclusion and health-check-driven recovery
- **Fault tolerance** — retries on healthy nodes for reads, parallel fan-out for critical transactions (`sendTransaction`)
- **Cost-aware routing** — free nodes preferred over paid, daily budget limits for paid nodes
- **Rate limiting** — per-IP and per-API-key limits with configurable XFF trust, disabled by default
- **Auto-discovery** — periodically discovers new public RPC nodes from chainid.network
- **Batch requests** — JSON-RPC 2.0 batch support with per-request caching
- **Observability** — Prometheus metrics, structured logging, web dashboard
- **Admin API** — runtime node management, circuit breaker reset, status dashboard, Bearer token auth
- **Hot reload** — config changes picked up without restart
- **Docker** — one-command launch with docker-compose (aggregator + Prometheus)

## Retry classification and log-range learning

Reads are retried on another node depending on what the upstream answered (EVM adapter):

| Upstream answer | Treated as | Effect |
|---|---|---|
| Result, or a legitimate JSON-RPC error (`execution reverted`, bad params on non-log calls) | usable | returned to the client |
| Transport error, 5xx, 429, 401/403/404, non-JSON body, rate-limit errors | node failure | retried elsewhere, counts against the circuit breaker |
| `eth_getLogs` range/size refusals (400, 413, `-32602`, `-32005`, "range"/"limit"/"exceed" messages), method not available, pruned state, body over 64MB | capability limit | retried elsewhere, node stays healthy |

For `eth_getLogs` the router computes the query's block span and remembers, per node, the widest span
served and the narrowest span refused (trusted for 10 minutes). Wide queries skip nodes that recently
refused that width; if every node refuses, the client gets the upstream's own error (e.g. `range 99999
exceeds limit of 10000`) so it can split the query. Learned limits are shown in `/admin/status` as
`logs_max_ok_span` / `logs_fail_span`. Discovery skips URLs that are already configured.

## Architecture

```
                      ┌─────────────────────────────────────┐
                      │           RPC Aggregator            │
 Consumers ──HTTP──►  │                                     │
           ──WS───►   │  ┌───────────┐    ┌──────────────┐  │
                      │  │  Router   │───►│  Cache Layer │  │
                      │  └─────┬─────┘    └──────────────┘  │
                      │        │                            │
                      │  ┌─────▼──────────────────────────┐ │
                      │  │       Node Pool (per chain)    │ │
                      │  │  ┌──────┐ ┌──────┐ ┌──────┐    │ │
                      │  │  │Node 1│ │Node 2│ │Node N│    │ │
                      │  │  └──────┘ └──────┘ └──────┘    │ │
                      │  └────────────────────────────────┘ │
                      │                                     │
 Admin ──HTTP──►      │  ┌─────────────┐  ┌─────────────┐   │
                      │  │ Admin API   │  │ Health Check│   │
                      │  └─────────────┘  └──────┬──────┘   │
                      │                          │          │
                      │  ┌───────────────────────▼──────┐   │
                      │  │   Metrics (Prometheus)       │   │
                      │  └──────────────────────────────┘   │
                      └─────────────────────────────────────┘
```

**Scoring formula:**
```
score = weight * (W_latency * (1 - norm_latency) + W_height * norm_height + W_errors * (1 - error_rate)) * cost_multiplier
```

## Quick Start

### Prerequisites

- Go 1.22+ **or** Docker + Docker Compose
- RPC endpoints for the chains you want to aggregate

### Option 1: Go

```bash
git clone https://github.com/spiehdid/rpc-aggregator.git
cd rpc-aggregator

cp config.example.yaml config.yaml
# Edit config.yaml — replace example URLs with real RPC endpoints

go run ./cmd/aggregator config.yaml
```

### Option 2: Docker Compose

```bash
git clone https://github.com/spiehdid/rpc-aggregator.git
cd rpc-aggregator

cp config.example.yaml config.yaml
# Edit config.yaml

docker-compose up
```

This starts the aggregator on `:8080` (consumer) / `:8081` (admin) and Prometheus on `:9090`.

### Verify it works

```bash
# HTTP
curl -X POST http://localhost:8080/polygon \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# WebSocket (browser console)
const ws = new WebSocket('ws://localhost:8080/polygon');
ws.onmessage = (e) => console.log(JSON.parse(e.data));
ws.onopen = () => ws.send(JSON.stringify({jsonrpc:"2.0", method:"eth_blockNumber", params:[], id:1}));

# Dashboard
open http://localhost:8081/dashboard
```

## Configuration

See [config.example.yaml](config.example.yaml) for a full example.

### Key parameters

#### Server

| Parameter | Default | Description |
|---|---|---|
| `server.listen` | `:8080` | Consumer-facing port (HTTP + WS) |
| `server.admin_listen` | `:8081` | Admin API + dashboard port |
| `server.request_timeout` | `30s` | Max time for a single RPC request |
| `server.drain_timeout` | `30s` | Grace period for in-flight requests on shutdown |
| `server.admin_token` | `""` | Bearer token for admin API (empty = no auth, warns at startup) |
| `server.allowed_origins` | `[]` | WS origin allowlist (empty = allow all) |

#### Scoring & routing

| Parameter | Default | Description |
|---|---|---|
| `scoring.weights.latency` | `0.4` | Weight for node latency in scoring formula |
| `scoring.weights.block_height` | `0.3` | Weight for block height freshness |
| `scoring.weights.error_rate` | `0.3` | Weight for error rate |
| `scoring.weights.paid_penalty` | `0.5` | Cost multiplier for paid nodes (0.0-1.0, lower = less likely to use) |

#### Health checks & circuit breaker

| Parameter | Default | Description |
|---|---|---|
| `health_check.interval` | `5s` | How often to poll each node for block height and latency |
| `health_check.timeout` | `3s` | Max time to wait for a health check response |
| `circuit_breaker.error_threshold` | `0.5` | Error rate (0.0-1.0) that triggers circuit open |
| `circuit_breaker.window` | `30s` | Sliding window for error rate calculation |
| `circuit_breaker.recovery_timeout` | `30s` | How long to wait before probing an open circuit |

#### Retry & fan-out

| Parameter | Default | Description |
|---|---|---|
| `retry.max_attempts` | `3` | Max retry attempts for failed requests (each on a different node) |
| `retry.critical_fan_out` | `3` | Number of nodes to send critical methods to in parallel |
| `retry.max_batch_size` | `100` | Max items in a JSON-RPC batch request |

#### Rate limiting

| Parameter | Default | Description |
|---|---|---|
| `rate_limit.enabled` | `false` | Enable rate limiting |
| `rate_limit.trust_x_forwarded_for` | `false` | Trust X-Forwarded-For header (enable only behind a trusted proxy) |
| `rate_limit.default.requests_per_second` | `100` | Per-IP request rate for unauthenticated clients |
| `rate_limit.default.burst` | `200` | Burst capacity for per-IP rate limiting |

#### WebSocket

| Parameter | Default | Description |
|---|---|---|
| `websocket.max_connections` | `1000` | Max concurrent consumer WS connections |
| `websocket.max_subscriptions_per_conn` | `100` | Max subscriptions per WS connection |
| `websocket.max_total_subscriptions` | `10000` | Max total subscriptions across all connections |
| `websocket.max_message_size` | `65536` | Max inbound WS message size (bytes) |
| `websocket.send_queue_size` | `256` | Per-consumer send buffer (events dropped if full) |
| `websocket.write_timeout` | `5s` | Max time to write a message to a consumer |
| `websocket.ping_interval` | `30s` | Keepalive ping interval |
| `websocket.pong_timeout` | `10s` | Max time to wait for pong response |

### Adding a chain

```yaml
chains:
  polygon:
    adapter: evm          # or "solana"
    path: /polygon
    finalization: true
    discovery:
      enabled: true
      chain_id: 137
      interval: 30m
    cache:
      default_ttl: 5s
      max_entries: 10000
      rules:
        - method: eth_chainId
          ttl: 0s
        - method: eth_blockNumber
          ttl: 500ms
    nodes:
      - url: https://polygon.drpc.org
        ws_url: wss://polygon.drpc.org
        weight: 1.0
      - url: https://paid-node.com/v2/KEY
        weight: 1.0
        tier: paid
        budget:
          daily_limit: 1000
```

## API

### Consumer endpoints

Same path serves both HTTP POST and WebSocket:

```bash
# HTTP request
curl -X POST http://localhost:8080/polygon \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_getBalance","params":["0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045","latest"],"id":1}'

# WebSocket subscription (wscat)
wscat -c ws://localhost:8080/polygon
> {"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":1}
```

### Admin endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/dashboard` | Web UI dashboard |
| `GET` | `/admin/status` | JSON status — all chains, nodes, scores |
| `GET` | `/admin/chains/{chain}/nodes` | List nodes with scores |
| `POST` | `/admin/chains/{chain}/nodes` | Add a node |
| `DELETE` | `/admin/chains/{chain}/nodes/{id}` | Remove a node |
| `PATCH` | `/admin/chains/{chain}/nodes/{id}` | Update node weight |
| `POST` | `/admin/chains/{chain}/nodes/{id}/reset` | Reset circuit breaker |
| `GET` | `/health` | Health check |

## Project Structure

```
rpc-aggregator/
├── cmd/aggregator/main.go       # Entry point, wiring
├── internal/
│   ├── adapter/                 # ChainAdapter interface + registry
│   │   ├── evm/                 # EVM implementation
│   │   └── solana/              # Solana implementation
│   ├── admin/                   # Admin REST API + web dashboard
│   ├── cache/                   # LRU cache + rule matcher
│   ├── config/                  # YAML config + hot reload
│   ├── discovery/               # Auto-discovery from chainid.network
│   ├── health/                  # Background health checker + finalization
│   ├── metrics/                 # Prometheus collectors
│   ├── pool/                    # Node, scorer, circuit breaker, budget
│   ├── ratelimit/               # Per-IP / per-API-key rate limiting
│   ├── router/                  # HTTP router, retry, fan-out, batch
│   └── ws/                      # WebSocket handler, subscriptions, upstream pool
├── tests/                       # Integration tests
├── bench/                       # Go benchmarks + k6 load tests
├── config.example.yaml
├── Dockerfile
├── docker-compose.yaml
└── prometheus.yml
```

## License

[MIT](LICENSE)
