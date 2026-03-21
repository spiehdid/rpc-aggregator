package router

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

func setupRouter(t *testing.T, upstreamResponse string) (*Router, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(upstreamResponse))
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

	evmAdapter := evm.New()
	cacheRules := []cache.Rule{{Method: "eth_chainId", TTL: 0}}
	rm := cache.NewRuleMatcher(cacheRules, 5*time.Second)
	c := cache.New(1000)

	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{Name: "ethereum", Adapter: evmAdapter, Pool: p, Cache: c, RuleMatcher: rm},
	}
	rtr := New(chains, m, 3, 3, 100, 30*time.Second)
	return rtr, upstream
}

func TestRouter_ProxiesRequest(t *testing.T) {
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	defer upstream.Close()
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	respBody, _ := io.ReadAll(resp.Body)
	var rpcResp adapter.RPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
}

func TestRouter_UnknownChain_Returns404(t *testing.T) {
	rtr, upstream := setupRouter(t, `{}`)
	defer upstream.Close()
	req := httptest.NewRequest(http.MethodPost, "/unknown", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Result().StatusCode)
	}
}

func TestRouter_CacheHit(t *testing.T) {
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	defer upstream.Close()
	body := `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`
	req1 := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	rtr.ServeHTTP(w1, req1)
	if w1.Result().StatusCode != http.StatusOK {
		t.Fatalf("first request status = %d", w1.Result().StatusCode)
	}
	upstream.Close()
	req2 := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	rtr.ServeHTTP(w2, req2)
	if w2.Result().StatusCode != http.StatusOK {
		t.Errorf("cached request status = %d, want 200", w2.Result().StatusCode)
	}
}

func TestRouter_BatchRequest(t *testing.T) {
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	defer upstream.Close()
	body := `[{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1},{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":2}]`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	respBody, _ := io.ReadAll(resp.Body)
	if respBody[0] != '[' {
		t.Errorf("expected batch response (array), got: %s", string(respBody[:50]))
	}
}

func TestRouter_ProcessRequest(t *testing.T) {
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","result":"0x1","id":1}`)
	defer upstream.Close()
	body := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	result := rtr.ProcessRequest(context.Background(), "/eth", body)
	var resp adapter.RPCResponse
	if err := json.Unmarshal(result, &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp.Error != nil {
		t.Errorf("unexpected error: %v", resp.Error)
	}
}

func TestRouter_ProcessRequest_UnknownChain(t *testing.T) {
	rtr, upstream := setupRouter(t, `{}`)
	defer upstream.Close()
	result := rtr.ProcessRequest(context.Background(), "/unknown", []byte(`{}`))
	var resp adapter.RPCResponse
	json.Unmarshal(result, &resp)
	if resp.Error == nil {
		t.Error("expected error for unknown chain")
	}
}

func TestRouter_OversizedBatch_ReturnsError(t *testing.T) {
	rtr, upstream := setupRouter(t, `{}`)
	defer upstream.Close()
	var reqs []string
	for i := 0; i < 101; i++ {
		reqs = append(reqs, `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	}
	body := "[" + strings.Join(reqs, ",") + "]"
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	if w.Result().StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Result().StatusCode)
	}
	respBody, _ := io.ReadAll(w.Result().Body)
	var rpcResp adapter.RPCResponse
	json.Unmarshal(respBody, &rpcResp)
	if rpcResp.Error == nil {
		t.Error("expected JSON-RPC error for oversized batch")
	}
}

func TestRouter_RetryOnUpstreamFailure(t *testing.T) {
	var callCount int
	// First call returns 500, second succeeds.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer upstream.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	// Add two nodes pointing to the same upstream so a retry is attempted.
	p.AddNode("node1", upstream.URL, 1.0)
	p.AddNode("node2", upstream.URL, 1.0)
	n1 := p.GetNode("node1")
	n1.SetBlockHeight(100)
	n1.SetLatency(10 * time.Millisecond)
	n2 := p.GetNode("node2")
	n2.SetBlockHeight(100)
	n2.SetLatency(20 * time.Millisecond)

	evmAdapter := evm.New()
	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{
			Name:        "ethereum",
			Adapter:     evmAdapter,
			Pool:        p,
			Cache:       cache.New(1000),
			RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second),
		},
	}
	rtr := New(chains, m, 3, 3, 100, 30*time.Second)

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error != nil {
		t.Errorf("expected success after retry, got error: %v", resp.Error)
	}
}

func TestRouter_AllNodesDown(t *testing.T) {
	// Upstream always returns 500 — every attempt will fail.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer upstream.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("node1", upstream.URL, 1.0)
	n1 := p.GetNode("node1")
	n1.SetBlockHeight(100)
	n1.SetLatency(10 * time.Millisecond)

	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{
			Name:        "ethereum",
			Adapter:     evm.New(),
			Pool:        p,
			Cache:       cache.New(1000),
			RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second),
		},
	}
	rtr := New(chains, m, 3, 3, 100, 30*time.Second)

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error == nil {
		t.Error("expected error when all nodes are down")
	}
}

func TestRouter_InvalidJSON(t *testing.T) {
	rtr, upstream := setupRouter(t, `{}`)
	defer upstream.Close()

	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(`{invalid json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRouter_FanOut_CriticalMethod(t *testing.T) {
	// eth_sendRawTransaction is classified as critical (mutating), so fan-out is
	// used. All nodes succeed; we just want to confirm no panic and a valid reply.
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","result":"0xabc","id":1}`)
	defer upstream.Close()

	body := `{"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0xdeadbeef"],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestRouter_ProcessRequest_UnknownChainReturnsError(t *testing.T) {
	rtr, upstream := setupRouter(t, `{}`)
	defer upstream.Close()

	result := rtr.ProcessRequest(context.Background(), "/nonexistent", []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	var resp adapter.RPCResponse
	json.Unmarshal(result, &resp)
	if resp.Error == nil {
		t.Error("expected error for unknown chain path")
	}
}

func TestRouter_InvalidBatchJSON(t *testing.T) {
	rtr, upstream := setupRouter(t, `{}`)
	defer upstream.Close()

	// Starts with '[' so it goes through handleBatch but is not valid JSON array.
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(`[{bad`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error == nil {
		t.Error("expected error for invalid batch JSON")
	}
}

func TestRouter_NoNodes_ReturnsError(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	// Empty pool — no nodes at all.

	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{
			Name:        "ethereum",
			Adapter:     evm.New(),
			Pool:        p,
			Cache:       cache.New(1000),
			RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second),
		},
	}
	rtr := New(chains, m, 3, 3, 100, 30*time.Second)

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error == nil {
		t.Error("expected error when pool is empty")
	}
}

// Test #8: Circuit breaker recovery end-to-end
func TestRouter_CircuitBreakerRecovery(t *testing.T) {
	// Use an atomic to control whether the upstream succeeds or fails.
	var failUpstream atomic.Int32
	failUpstream.Store(1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failUpstream.Load() == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer upstream.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	// Use a very short recovery timeout (50ms) so the test doesn't take long.
	cbCfg := pool.CircuitBreakerConfig{
		ErrorThreshold:  0.5,
		Window:          1 * time.Second,
		RecoveryTimeout: 50 * time.Millisecond,
	}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("node1", upstream.URL, 1.0)
	node := p.GetNode("node1")
	node.SetBlockHeight(100)
	node.SetLatency(10 * time.Millisecond)

	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{
			Name:        "ethereum",
			Adapter:     evm.New(),
			Pool:        p,
			Cache:       cache.New(1000),
			RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second),
		},
	}
	// Single retry so failures are recorded quickly.
	rtr := New(chains, m, 1, 1, 100, 5*time.Second)

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`

	// Send enough failing requests to open the circuit (errorThreshold=0.5, so
	// we need more than half of recorded calls to be errors).
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		rtr.ServeHTTP(w, req)
	}

	// Verify circuit is now open.
	if node.CircuitState() != pool.CircuitOpen {
		t.Fatalf("expected circuit to be open after repeated failures, got %v", node.CircuitState())
	}

	// Make upstream healthy again.
	failUpstream.Store(0)

	// Wait for the recovery timeout to expire so state transitions to HalfOpen.
	time.Sleep(100 * time.Millisecond)

	if node.CircuitState() != pool.CircuitHalfOpen {
		t.Fatalf("expected circuit to be half-open after recovery timeout, got %v", node.CircuitState())
	}

	// Simulate the health checker recovering the node: record a success
	// while in HalfOpen state, which closes the circuit.
	node.CircuitBreaker().RecordSuccess()

	if node.CircuitState() != pool.CircuitClosed {
		t.Fatalf("expected circuit to be closed after health-check success, got %v", node.CircuitState())
	}

	// Now user traffic should succeed.
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 after circuit recovery", w.Code)
	}
	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp.Error != nil {
		t.Errorf("expected success after circuit recovery, got error: %v", resp.Error)
	}
}

// Test #16: Upstream returns HTTP 200 with JSON-RPC error body
func TestRouter_Upstream200WithRPCError(t *testing.T) {
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","error":{"code":-32005,"message":"rate limited"},"id":1}`)
	defer upstream.Close()

	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)

	var resp adapter.RPCResponse
	json.Unmarshal(w.Body.Bytes(), &resp)
	// Error should propagate to client.
	if resp.Error == nil {
		t.Error("expected RPC error to propagate")
	}
	if resp.Error.Code != -32005 {
		t.Errorf("error code = %d, want -32005", resp.Error.Code)
	}
}

// Test #17: Client disconnect mid-request
func TestRouter_ClientContextCancellation(t *testing.T) {
	// Upstream is slow — takes 2 seconds to respond.
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Block until the request context is done or timeout.
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	defer upstream.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("node1", upstream.URL, 1.0)
	node := p.GetNode("node1")
	node.SetBlockHeight(100)
	node.SetLatency(10 * time.Millisecond)

	chains := map[string]*ChainRoute{
		"/eth": &ChainRoute{
			Name:        "ethereum",
			Adapter:     evm.New(),
			Pool:        p,
			Cache:       cache.New(1000),
			RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second),
		},
	}
	rtr := New(chains, m, 1, 1, 100, 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	body := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/eth", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	done := make(chan struct{})
	go func() {
		w := httptest.NewRecorder()
		rtr.ServeHTTP(w, req)
		close(done)
	}()

	// Cancel after 100ms — well before the 2-second upstream delay.
	time.Sleep(100 * time.Millisecond)
	cancel()

	// The handler should complete quickly after cancellation.
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("request did not complete after context cancellation")
	}
}

func TestRouter_CacheRewritesResponseID(t *testing.T) {
	rtr, upstream := setupRouter(t, `{"jsonrpc":"2.0","result":"0x1","id":99}`)
	defer upstream.Close()

	// First request with id:1 — cache miss
	body1 := `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`
	req1 := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()
	rtr.ServeHTTP(w1, req1)

	// Second request with id:42 — cache hit, should have id:42 in response
	body2 := `{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":42}`
	req2 := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body2))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()
	rtr.ServeHTTP(w2, req2)

	var resp2 struct {
		ID json.RawMessage `json:"id"`
	}
	json.Unmarshal(w2.Body.Bytes(), &resp2)
	if string(resp2.ID) != "42" {
		t.Errorf("cached response ID = %s, want 42", string(resp2.ID))
	}
}
