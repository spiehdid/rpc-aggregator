# Benchmarks

## Go Benchmarks (internal overhead)

Measures raw component performance without network I/O.

```bash
go test ./internal/pool/ ./internal/cache/ ./internal/router/ -bench=. -benchmem
```

### Results (Intel i9-14900K)

| Benchmark | ops/sec | ns/op | allocs/op |
|---|---|---|---|
| Scorer SelectBest (10 nodes) | 3,645,890 | 325 | 2 |
| Scorer SelectBest (50 nodes) | 810,405 | 1,562 | 4 |
| Scorer SelectTopN (20 nodes) | 1,465,009 | 805 | 8 |
| Cache Get (hit) | 78,425,733 | 16 | 0 |
| Cache Get (miss) | 100,000,000 | 10 | 0 |
| Cache Set | 16,686,342 | 71 | 1 |
| Cache Set (eviction) | 7,567,047 | 165 | 4 |
| Router Proxy (full roundtrip) | 255,370 | 5,112 | 51 |
| Router Cache Hit | 204,420 | 8,136 | 51 |

**Key takeaway:** Cache lookups at 16ns (zero alloc), scoring at 325ns for 10 nodes. The aggregator adds minimal overhead to RPC calls.

Note: Results above are from an earlier build. Scoring now includes cost-aware routing (paid/free tiers). Run `go test -bench=. -benchmem` to get current numbers for your hardware.

## k6 Load Tests (realistic scenarios)

Requires [k6](https://k6.io/docs/get-started/installation/).

### Single request load test

Ramps from 1 to 100 virtual users over 80 seconds with weighted mix of RPC methods.

```bash
# Start the aggregator first
go run ./cmd/aggregator config.yaml

# Run load test
k6 run bench/k6-load-test.js
```

### Batch request load test

20 concurrent users sending batches of 10 mixed requests for 30 seconds.

```bash
k6 run bench/k6-batch-test.js
```

### Custom configuration

```bash
# Test against a different chain
k6 run bench/k6-load-test.js -e CHAIN_PATH=/arb

# Test against a different host
k6 run bench/k6-load-test.js -e BASE_URL=http://your-host:8080
```
