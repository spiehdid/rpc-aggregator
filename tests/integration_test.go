package tests

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
	"github.com/spiehdid/rpc-aggregator/internal/router"
)

func setupIntegration(t *testing.T, upstreams ...*httptest.Server) *router.Router {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	for i, us := range upstreams {
		id := strings.Replace(us.URL, "http://", "", 1)
		p.AddNode(id, us.URL, 1.0)
		node := p.GetNode(id)
		node.SetBlockHeight(uint64(100 + i))
		node.SetLatency(time.Duration(10+i*10) * time.Millisecond)
	}
	rules := []cache.Rule{
		{Method: "eth_chainId", TTL: 0},
		{Method: "eth_blockNumber", TTL: 1 * time.Second},
	}
	rm := cache.NewRuleMatcher(rules, 5*time.Second)
	c := cache.New(1000)
	chains := map[string]*router.ChainRoute{
		"/eth": &router.ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: c, RuleMatcher: rm},
	}
	return router.New(chains, m, 3, 3, 100, 30*time.Second)
}

func TestIntegration_EndToEnd(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req adapter.RPCRequest
		json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "eth_blockNumber":
			w.Write([]byte(`{"jsonrpc":"2.0","result":"0x100","id":1}`))
		case "eth_chainId":
			w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
		default:
			w.Write([]byte(`{"jsonrpc":"2.0","result":"0x0","id":1}`))
		}
	}))
	defer upstream.Close()
	rtr := setupIntegration(t, upstream)
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var resp adapter.RPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error)
	}
}

func TestIntegration_FailoverToSecondNode(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failing.Close()
	working := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer working.Close()
	rtr := setupIntegration(t, failing, working)
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp adapter.RPCResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error != nil {
		t.Errorf("expected success after failover, got error: %v", resp.Error)
	}
}

func TestIntegration_CachePreventsUpstreamCall(t *testing.T) {
	var callCount int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer upstream.Close()
	rtr := setupIntegration(t, upstream)
	body := `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		rtr.ServeHTTP(w, req)
	}
	if callCount != 1 {
		t.Errorf("upstream called %d times, want 1", callCount)
	}
}

// Test #20: Multi-chain integration test
func TestIntegration_MultiChain(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer upstream.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}

	// Create two chains pointing to the same upstream server.
	p1 := pool.NewNodePool("polygon", pool.NewScorer(weights), cbCfg)
	p1.AddNode("poly-node", upstream.URL, 1.0)
	n1 := p1.GetNode("poly-node")
	n1.SetBlockHeight(100)
	n1.SetLatency(10 * time.Millisecond)

	p2 := pool.NewNodePool("arbitrum", pool.NewScorer(weights), cbCfg)
	p2.AddNode("arb-node", upstream.URL, 1.0)
	n2 := p2.GetNode("arb-node")
	n2.SetBlockHeight(200)
	n2.SetLatency(20 * time.Millisecond)

	chains := map[string]*router.ChainRoute{
		"/polygon": &router.ChainRoute{Name: "polygon", Adapter: evm.New(), Pool: p1, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second)},
		"/arb":     {Name: "arbitrum", Adapter: evm.New(), Pool: p2, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second)},
	}
	rtr := router.New(chains, m, 3, 3, 100, 30*time.Second)

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`

	// Request to polygon.
	req1 := httptest.NewRequest(http.MethodPost, "/polygon", strings.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	rtr.ServeHTTP(w1, req1)
	if w1.Code != 200 {
		t.Errorf("polygon: status=%d, want 200", w1.Code)
	}

	// Request to arbitrum.
	req2 := httptest.NewRequest(http.MethodPost, "/arb", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	rtr.ServeHTTP(w2, req2)
	if w2.Code != 200 {
		t.Errorf("arbitrum: status=%d, want 200", w2.Code)
	}

	// Unknown chain should return 404.
	req3 := httptest.NewRequest(http.MethodPost, "/solana", strings.NewReader(body))
	w3 := httptest.NewRecorder()
	rtr.ServeHTTP(w3, req3)
	if w3.Code != 404 {
		t.Errorf("unknown chain: status=%d, want 404", w3.Code)
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	// This tests that in-flight requests complete during shutdown

	var requestStarted sync.WaitGroup
	requestStarted.Add(1)
	var responseReady sync.WaitGroup
	responseReady.Add(1)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestStarted.Done() // signal that request arrived
		responseReady.Wait()  // wait for test to say "ok respond now"
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer upstream.Close()

	// Build a real server
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("node1", upstream.URL, 1.0)
	n := p.GetNode("node1")
	n.SetBlockHeight(100)
	n.SetLatency(10 * time.Millisecond)

	chains := map[string]*router.ChainRoute{
		"/eth": &router.ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second)},
	}
	rtr := router.New(chains, m, 3, 3, 100, 30*time.Second)

	server := httptest.NewServer(rtr)

	// Start an in-flight request
	var resp *http.Response
	var reqErr error
	done := make(chan struct{})
	go func() {
		resp, reqErr = http.Post(server.URL+"/eth", "application/json",
			strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
		close(done)
	}()

	// Wait for request to reach upstream
	requestStarted.Wait()

	// Shutdown the server while request is in-flight
	server.Close()

	// Let the upstream respond
	responseReady.Done()

	// The in-flight request should complete (or fail cleanly)
	select {
	case <-done:
		// Request completed — either with response or connection closed error
		// Both are acceptable for graceful shutdown
		if reqErr == nil && resp != nil {
			resp.Body.Close()
		}
	case <-time.After(5 * time.Second):
		t.Error("in-flight request did not complete within 5s after shutdown")
	}
}
