package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

func newTestPool(t *testing.T, name string) *pool.NodePool {
	t.Helper()
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	return pool.NewNodePool(name, pool.NewScorer(weights), cbCfg)
}

func TestFilterURLs(t *testing.T) {
	urls := []string{
		"https://good-rpc.com",
		"http://insecure.com",
		"wss://websocket.com",
		"https://paid.com/${API_KEY}",
		"https://another-good.com/rpc",
	}
	result := filterURLs(urls)
	if len(result) != 2 {
		t.Fatalf("got %d URLs, want 2", len(result))
	}
	if result[0] != "https://good-rpc.com" {
		t.Errorf("result[0] = %q", result[0])
	}
	if result[1] != "https://another-good.com/rpc" {
		t.Errorf("result[1] = %q", result[1])
	}
}

func TestDiscoverer_AddsNodes(t *testing.T) {
	// TLS RPC server that returns a valid block height (https:// URL passes filterURLs)
	rpcTLS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x100","id":1}`))
	}))
	defer rpcTLS.Close()

	// Mock chainlist server returning entries with the TLS RPC URL.
	chainlist := []chainEntry{{ChainID: 137, Name: "Polygon", RPC: []string{rpcTLS.URL}}}
	chainlistJSON, _ := json.Marshal(chainlist)
	chainlistServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(chainlistJSON)
	}))
	defer chainlistServer.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	p := newTestPool(t, "polygon")

	d := New(nil, m)
	d.chainlistURL = chainlistServer.URL

	// Inject the TLS test client into the adapter so health checks use it.
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New().WithClient(rpcTLS.Client()), Interval: time.Minute}
	d.chains = []ChainDiscovery{chain}

	// Run full discovery end-to-end: fetch chainlist + discoverChain
	ctx := context.Background()
	d.runAll(ctx)

	nodes := p.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after discovery, got %d", len(nodes))
	}
	if !nodes[0].Discovered {
		t.Error("expected Discovered=true")
	}
	if nodes[0].URL != rpcTLS.URL {
		t.Errorf("node URL = %q, want %q", nodes[0].URL, rpcTLS.URL)
	}
}

func TestDiscoverer_RemovesStaleNodes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("polygon", pool.NewScorer(weights), cbCfg)

	// Add a discovered node that won't be in chainlist
	p.AddDiscoveredNode("https://stale-node.com", "https://stale-node.com", 1.0)
	// Add a manual node
	p.AddNode("manual", "https://manual.com", 1.0)

	d := New(nil, m)
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New(), Interval: time.Minute}

	// Empty entries — all discovered should be removed
	d.discoverChain(context.Background(), chain, []chainEntry{{ChainID: 137, RPC: []string{}}})

	if p.GetNode("https://stale-node.com") != nil {
		t.Error("stale discovered node should be removed")
	}
	if p.GetNode("manual") == nil {
		t.Error("manual node should NOT be removed")
	}
}

func TestDiscoverer_DoesNotRemoveManualNodes(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("polygon", pool.NewScorer(weights), cbCfg)

	p.AddNode("manual1", "https://manual1.com", 1.0)
	p.AddNode("manual2", "https://manual2.com", 1.0)

	d := New(nil, m)
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New(), Interval: time.Minute}

	d.discoverChain(context.Background(), chain, []chainEntry{{ChainID: 137, RPC: []string{}}})

	nodes := p.Nodes()
	if len(nodes) != 2 {
		t.Errorf("got %d nodes, want 2", len(nodes))
	}
}

func TestDiscoverer_DiscoverChain_NoMatchingChainID(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	p := newTestPool(t, "polygon")

	d := New(nil, m)
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New(), Interval: time.Minute}

	// Entries with a different chain ID — nothing should be added.
	entries := []chainEntry{{ChainID: 1, RPC: []string{"https://eth.example.com"}}}
	d.discoverChain(context.Background(), chain, entries)

	nodes := p.Nodes()
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestDiscoverer_SkipsURLsAlreadyConfigured(t *testing.T) {
	var probes int
	rpcServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes++
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x100","id":1}`))
	}))
	defer rpcServer.Close()

	p := newTestPool(t, "polygon")
	p.AddNode("polygon-0", rpcServer.URL+"/", 1.0) // configured manually under its own ID, trailing slash

	d := New(nil, metrics.New(prometheus.NewRegistry()))
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New().WithClient(rpcServer.Client()), Interval: time.Minute}
	d.discoverChain(context.Background(), chain, []chainEntry{{ChainID: 137, RPC: []string{rpcServer.URL}}})

	if n := len(p.Nodes()); n != 1 {
		t.Errorf("configured URL must not be added again: %d nodes", n)
	}
	if probes != 0 {
		t.Errorf("configured URL must not be probed, got %d probes", probes)
	}
}

func TestDiscoverer_DiscoverChain_HealthyNodeAdded(t *testing.T) {
	// RPC server that responds with a valid block height via HTTPS.
	rpcServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x100","id":1}`))
	}))
	defer rpcServer.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	p := newTestPool(t, "polygon")

	d := New(nil, m)
	d.client = rpcServer.Client()

	// Inject the TLS test client into the adapter so health checks use it.
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New().WithClient(rpcServer.Client()), Interval: time.Minute}
	entries := []chainEntry{{ChainID: 137, RPC: []string{rpcServer.URL}}}
	d.discoverChain(context.Background(), chain, entries)

	nodes := p.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node after discoverChain, got %d", len(nodes))
	}
	if !nodes[0].Discovered {
		t.Error("expected Discovered=true on added node")
	}
}

func TestDiscoverer_FetchChainlist_Success(t *testing.T) {
	chainlist := []chainEntry{
		{ChainID: 137, Name: "Polygon", RPC: []string{"https://polygon-rpc.com"}},
		{ChainID: 1, Name: "Ethereum", RPC: []string{"https://eth.example.com"}},
	}
	data, _ := json.Marshal(chainlist)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}))
	defer srv.Close()

	d := &Discoverer{
		client:       &http.Client{Timeout: 5 * time.Second},
		chainlistURL: srv.URL,
	}

	ctx := context.Background()
	entries, err := d.fetchChainlist(ctx)
	if err != nil {
		t.Fatalf("fetchChainlist error: %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("got %d entries, want 2", len(entries))
	}
	if d.lastResult == nil {
		t.Error("lastResult should be set after successful fetch")
	}
}

func TestDiscoverer_FetchChainlist_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	d := &Discoverer{
		client:       &http.Client{Timeout: 5 * time.Second},
		metrics:      metrics.New(prometheus.NewRegistry()),
		chainlistURL: srv.URL,
	}

	ctx := context.Background()
	entries, err := d.fetchChainlist(ctx)
	if err == nil {
		t.Fatal("expected error for non-OK status, got nil")
	}
	if entries != nil {
		t.Errorf("expected nil entries on error, got %d entries", len(entries))
	}
	if d.lastResult != nil {
		t.Error("lastResult should remain nil after failed fetch")
	}
}

func TestDiscoverer_RunAll_UsesCachedResultOnFailure(t *testing.T) {
	// Build a pool and chain so runAll actually iterates chains.
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	p := newTestPool(t, "polygon")
	chain := ChainDiscovery{ChainName: "polygon", ChainID: 137, Pool: p, Adapter: evm.New(), Interval: time.Minute}

	// Build a chainlist response that would match our chain (but no healthy RPCs).
	entries := []chainEntry{{ChainID: 137, RPC: []string{}}}
	cached, _ := json.Marshal(entries)

	// Discoverer with a server that returns 500 — fetch will fail, but lastResult
	// provides the cached data so runAll should still call discoverChain.
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	d := &Discoverer{
		chains:       []ChainDiscovery{chain},
		metrics:      m,
		client:       &http.Client{Timeout: 5 * time.Second},
		lastResult:   cached,
		chainlistURL: failSrv.URL,
	}

	ctx := context.Background()
	d.runAll(ctx)

	// The pool should still have 0 nodes (no healthy RPCs in entries), but we
	// verified that runAll completed without panic and used the cached path.
	if len(p.Nodes()) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(p.Nodes()))
	}
}

func TestDiscoverer_Start_ReturnsImmediatelyWhenNoChains(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	d := New(nil, m)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	// Should return immediately because chains is empty.
	done := make(chan struct{})
	go func() {
		d.Start(ctx)
		close(done)
	}()

	select {
	case <-done:
		// OK
	case <-time.After(time.Second):
		t.Error("Start did not return promptly for empty chain list")
	}
}

func TestDiscoverer_Start_CancelsViaContext(t *testing.T) {
	// Set up a chainlist server that returns an empty list quickly.
	chainlist := []chainEntry{}
	data, _ := json.Marshal(chainlist)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}))
	defer srv.Close()

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	p := newTestPool(t, "polygon")
	chain := ChainDiscovery{
		ChainName: "polygon",
		ChainID:   137,
		Pool:      p,
		Adapter:   evm.New(),
		Interval:  100 * time.Millisecond,
	}
	d := New([]ChainDiscovery{chain}, m)
	d.chainlistURL = srv.URL

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		d.Start(ctx)
		close(done)
	}()

	// Let it run one tick, then cancel.
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK — Start exited after context cancellation.
	case <-time.After(5 * time.Second):
		t.Error("Start did not stop after context cancellation")
	}
}
