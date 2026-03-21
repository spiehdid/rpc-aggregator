package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

// mockAdapter is a test adapter that can be configured to return specific values.
type mockAdapter struct {
	blockHeight     uint64
	blockHeightErr  error
	finalizedHeight uint64
	finalizedErr    error
	blockCalls      atomic.Int32
	finalCalls      atomic.Int32
}

func (m *mockAdapter) Name() string { return "mock" }
func (m *mockAdapter) ParseRequest(raw []byte) (adapter.RPCRequest, error) {
	return adapter.RPCRequest{}, nil
}
func (m *mockAdapter) BuildUpstreamRequest(req adapter.RPCRequest) ([]byte, error) { return nil, nil }
func (m *mockAdapter) GetBlockHeight(ctx context.Context, nodeURL string) (uint64, error) {
	m.blockCalls.Add(1)
	return m.blockHeight, m.blockHeightErr
}
func (m *mockAdapter) GetFinalizedHeight(ctx context.Context, nodeURL string) (uint64, error) {
	m.finalCalls.Add(1)
	return m.finalizedHeight, m.finalizedErr
}
func (m *mockAdapter) ExtractBlockNumber(req adapter.RPCRequest) (uint64, bool) { return 0, false }
func (m *mockAdapter) ClassifyMethod(method string) adapter.MethodClassification {
	return adapter.MethodClassification{}
}
func (m *mockAdapter) ParseResponse(raw []byte) (adapter.RPCResponse, error) {
	return adapter.RPCResponse{}, nil
}

func newTestPool(t *testing.T, nodeIDs ...string) (*pool.NodePool, string) {
	t.Helper()
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("test", pool.NewScorer(weights), cbCfg)
	for _, id := range nodeIDs {
		p.AddNode(id, "http://fake-"+id, 1.0)
	}
	firstID := ""
	if len(nodeIDs) > 0 {
		firstID = nodeIDs[0]
	}
	return p, firstID
}

func TestChecker_UpdatesBlockHeight(t *testing.T) {
	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x64","id":1}`))
	}))
	defer server.Close()

	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("test", server.URL, 1.0)

	chains := map[string]ChainEntry{
		"ethereum": {Pool: p, Adapter: evm.New()},
	}

	checker := New(chains, 50*time.Millisecond, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go checker.Start(ctx)
	time.Sleep(120 * time.Millisecond)
	cancel()

	node := p.GetNode("test")
	if node.GetBlockHeight() != 100 {
		t.Errorf("BlockHeight = %d, want 100 (0x64)", node.GetBlockHeight())
	}
	if callCount.Load() < 1 {
		t.Error("expected at least one health check call")
	}
}

func TestChecker_StopsOnContextCancel(t *testing.T) {
	chains := map[string]ChainEntry{}
	checker := New(chains, 50*time.Millisecond, 3*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { checker.Start(ctx); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Error("checker did not stop after context cancel")
	}
}

// TestChecker_NodeFailRecordsError verifies that when GetBlockHeight returns an
// error the node's circuit breaker registers it (error rate rises above zero).
func TestChecker_NodeFailRecordsError(t *testing.T) {
	a := &mockAdapter{blockHeightErr: context.DeadlineExceeded}
	p, nodeID := newTestPool(t, "node1")

	chains := map[string]ChainEntry{
		"test": {Pool: p, Adapter: a},
	}
	checker := New(chains, 1*time.Hour, 1*time.Second)
	checker.checkAll(context.Background())

	node := p.GetNode(nodeID)
	if node.CircuitBreaker().ErrorRate() == 0 {
		t.Error("expected non-zero error rate after failed health check")
	}
	if a.blockCalls.Load() < 1 {
		t.Error("expected GetBlockHeight to be called")
	}
}

// TestChecker_SuccessRecordsSuccess verifies that a successful check updates
// BlockHeight and leaves the circuit breaker error rate at zero.
func TestChecker_SuccessRecordsSuccess(t *testing.T) {
	a := &mockAdapter{blockHeight: 42}
	p, nodeID := newTestPool(t, "node1")

	chains := map[string]ChainEntry{
		"test": {Pool: p, Adapter: a},
	}
	checker := New(chains, 1*time.Hour, 1*time.Second)
	checker.checkAll(context.Background())

	node := p.GetNode(nodeID)
	if node.GetBlockHeight() != 42 {
		t.Errorf("BlockHeight = %d, want 42", node.GetBlockHeight())
	}
	if node.CircuitBreaker().ErrorRate() != 0 {
		t.Error("expected zero error rate after successful health check")
	}
}

// TestChecker_MultipleNodes verifies that all nodes in a chain are checked.
func TestChecker_MultipleNodes(t *testing.T) {
	a := &mockAdapter{blockHeight: 10}
	p, _ := newTestPool(t, "node1", "node2", "node3")

	chains := map[string]ChainEntry{
		"test": {Pool: p, Adapter: a},
	}
	checker := New(chains, 1*time.Hour, 1*time.Second)
	checker.checkAll(context.Background())

	if got := a.blockCalls.Load(); got != 3 {
		t.Errorf("blockCalls = %d, want 3", got)
	}
	for _, id := range []string{"node1", "node2", "node3"} {
		node := p.GetNode(id)
		if node.GetBlockHeight() != 10 {
			t.Errorf("node %s BlockHeight = %d, want 10", id, node.GetBlockHeight())
		}
	}
}

// TestChecker_FinalizationEnabled verifies that when Finalization is true the
// adapter's GetFinalizedHeight is called and the node's FinalizedHeight is set.
func TestChecker_FinalizationEnabled(t *testing.T) {
	a := &mockAdapter{blockHeight: 100, finalizedHeight: 90}
	p, nodeID := newTestPool(t, "node1")

	chains := map[string]ChainEntry{
		"test": {Pool: p, Adapter: a, Finalization: true},
	}
	checker := New(chains, 1*time.Hour, 1*time.Second)
	checker.checkAll(context.Background())

	// The finalized height is set in a goroutine; give it a moment to complete.
	time.Sleep(50 * time.Millisecond)

	node := p.GetNode(nodeID)
	if node.GetFinalizedHeight() != 90 {
		t.Errorf("FinalizedHeight = %d, want 90", node.GetFinalizedHeight())
	}
	if a.finalCalls.Load() < 1 {
		t.Error("expected GetFinalizedHeight to be called")
	}
}

// TestChecker_FinalizationError verifies that a finalized-height error does not
// affect the circuit breaker (the node stays healthy).
func TestChecker_FinalizationError(t *testing.T) {
	a := &mockAdapter{blockHeight: 50, finalizedErr: context.DeadlineExceeded}
	p, nodeID := newTestPool(t, "node1")

	chains := map[string]ChainEntry{
		"test": {Pool: p, Adapter: a, Finalization: true},
	}
	checker := New(chains, 1*time.Hour, 1*time.Second)
	checker.checkAll(context.Background())

	// Allow the goroutine to run and return the error.
	time.Sleep(50 * time.Millisecond)

	node := p.GetNode(nodeID)
	if node.CircuitBreaker().ErrorRate() != 0 {
		t.Error("finalized-height error should not affect circuit breaker")
	}
	// BlockHeight should still be updated from the successful GetBlockHeight call.
	if node.GetBlockHeight() != 50 {
		t.Errorf("BlockHeight = %d, want 50", node.GetBlockHeight())
	}
}

// TestChecker_FinalizationDisabled verifies that GetFinalizedHeight is never
// called when Finalization is false.
func TestChecker_FinalizationDisabled(t *testing.T) {
	a := &mockAdapter{blockHeight: 5}
	p, _ := newTestPool(t, "node1")

	chains := map[string]ChainEntry{
		"test": {Pool: p, Adapter: a, Finalization: false},
	}
	checker := New(chains, 1*time.Hour, 1*time.Second)
	checker.checkAll(context.Background())

	time.Sleep(30 * time.Millisecond)

	if a.finalCalls.Load() != 0 {
		t.Errorf("GetFinalizedHeight called %d times, want 0", a.finalCalls.Load())
	}
}
