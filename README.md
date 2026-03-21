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

| Parameter | Default | Description |
|---|---|---|
| `server.listen` | `:8080` | Consumer-facing port (HTTP + WS) |
| `server.admin_listen` | `:8081` | Admin API + dashboard port |
| `scoring.weights.latency` | `0.4` | Weight for node latency |
| `scoring.weights.block_height` | `0.3` | Weight for block height freshness |
| `scoring.weights.error_rate` | `0.3` | Weight for error rate |
| `scoring.weights.paid_penalty` | `0.5` | Cost multiplier for paid nodes |
| `circuit_breaker.error_threshold` | `0.5` | Error rate to open circuit |
| `rate_limit.enabled` | `false` | Enable rate limiting |
| `rate_limit.trust_x_forwarded_for` | `false` | Trust XFF header (enable behind proxy) |
| `server.admin_token` | `""` | Bearer token for admin API (empty = no auth) |
| `server.allowed_origins` | `[]` | WS origin allowlist (empty = allow all) |
| `websocket.max_connections` | `1000` | Max WS connections |

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
