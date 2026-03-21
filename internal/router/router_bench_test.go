package router

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

func setupBenchRouter(b *testing.B) (*Router, *httptest.Server) {
	b.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("upstream1", upstream.URL, 1.0)
	node := p.GetNode("upstream1")
	node.SetBlockHeight(100)
	node.SetLatency(10 * time.Millisecond)

	cacheRules := []cache.Rule{{Method: "eth_chainId", TTL: 0}}
	rm := cache.NewRuleMatcher(cacheRules, 5*time.Second)
	c := cache.New(10000)

	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: c, RuleMatcher: rm},
	}
	return New(chains, m, 3, 3, 100, 30*time.Second), upstream
}

func BenchmarkRouter_ProxyRequest(b *testing.B) {
	rtr, upstream := setupBenchRouter(b)
	defer upstream.Close()

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		rtr.ServeHTTP(w, req)
	}
}

func BenchmarkRouter_CacheHit(b *testing.B) {
	rtr, upstream := setupBenchRouter(b)
	defer upstream.Close()

	// Prime the cache
	body := `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		rtr.ServeHTTP(w, req)
	}
}
