package pool

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func newTestPool() *NodePool {
	weights := ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	return NewNodePool("ethereum", NewScorer(weights), cbCfg)
}

func TestNodePool_AddAndListNodes(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	p.AddNode("rpc2", "https://rpc2.example.com", 0.8)
	nodes := p.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("Nodes() = %d, want 2", len(nodes))
	}
}

func TestNodePool_AddDuplicate(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	err := p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	if err == nil {
		t.Error("expected error when adding duplicate node ID")
	}
}

func TestNodePool_RemoveNode(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	err := p.RemoveNode("rpc1")
	if err != nil {
		t.Fatalf("RemoveNode() error: %v", err)
	}
	if len(p.Nodes()) != 0 {
		t.Error("expected 0 nodes after removal")
	}
}

func TestNodePool_RemoveNonexistent(t *testing.T) {
	p := newTestPool()
	err := p.RemoveNode("ghost")
	if err == nil {
		t.Error("expected error when removing nonexistent node")
	}
}

func TestNodePool_SelectBest(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	p.AddNode("rpc2", "https://rpc2.example.com", 1.0)
	// Use GetNode for deterministic access
	n1 := p.GetNode("rpc1")
	n1.SetLatency(10 * time.Millisecond)
	n1.SetBlockHeight(100)
	n2 := p.GetNode("rpc2")
	n2.SetLatency(200 * time.Millisecond)
	n2.SetBlockHeight(100)

	best := p.SelectBest()
	if best == nil {
		t.Fatal("SelectBest() returned nil")
	}
	if best.ID != "rpc1" {
		t.Errorf("selected %q, want %q", best.ID, "rpc1")
	}
}

func TestNodePool_SelectBestExcluding(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	p.AddNode("rpc2", "https://rpc2.example.com", 1.0)
	n1 := p.GetNode("rpc1")
	n1.SetLatency(10 * time.Millisecond)
	n1.SetBlockHeight(100)
	n2 := p.GetNode("rpc2")
	n2.SetLatency(200 * time.Millisecond)
	n2.SetBlockHeight(100)

	best := p.SelectBestExcluding(map[string]bool{"rpc1": true})
	if best == nil {
		t.Fatal("SelectBestExcluding() returned nil")
	}
	if best.ID != "rpc2" {
		t.Errorf("selected %q, want %q", best.ID, "rpc2")
	}
}

func TestNodePool_UpdateWeight(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	err := p.UpdateWeight("rpc1", 2.5)
	if err != nil {
		t.Fatalf("UpdateWeight() error: %v", err)
	}
	node := p.GetNode("rpc1")
	if node.GetWeight() != 2.5 {
		t.Errorf("Weight = %v, want 2.5", node.GetWeight())
	}
}

func TestNodePool_FinalizedHeight(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	p.AddNode("rpc2", "https://rpc2.example.com", 1.0)
	n1 := p.GetNode("rpc1")
	n1.SetFinalizedHeight(100)
	n1.SetBlockHeight(110)
	n1.SetLatency(10 * time.Millisecond)
	n2 := p.GetNode("rpc2")
	n2.SetFinalizedHeight(95)
	n2.SetBlockHeight(110)
	n2.SetLatency(20 * time.Millisecond)
	fh := p.FinalizedHeight()
	if fh != 95 {
		t.Errorf("FinalizedHeight = %d, want 95 (min)", fh)
	}
}

func TestNodePool_FinalizedHeight_IgnoresOpenCircuit(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	p.AddNode("rpc2", "https://rpc2.example.com", 1.0)
	n1 := p.GetNode("rpc1")
	n1.SetFinalizedHeight(50)
	n1.SetBlockHeight(60)
	n1.SetLatency(10 * time.Millisecond)
	// Open circuit on rpc2
	n2 := p.GetNode("rpc2")
	n2.SetFinalizedHeight(100)
	for i := 0; i < 10; i++ {
		n2.CircuitBreaker().RecordError()
	}
	fh := p.FinalizedHeight()
	if fh != 50 {
		t.Errorf("FinalizedHeight = %d, want 50 (rpc2 is open)", fh)
	}
}

func TestNodePool_ResetCircuitBreaker(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	node := p.GetNode("rpc1")
	for i := 0; i < 10; i++ {
		node.CircuitBreaker().RecordError()
	}
	if node.CircuitState() != CircuitOpen {
		t.Fatal("expected circuit to be open")
	}
	err := p.ResetCircuitBreaker("rpc1")
	if err != nil {
		t.Fatalf("ResetCircuitBreaker() error: %v", err)
	}
	if node.CircuitState() != CircuitClosed {
		t.Error("expected circuit to be closed after reset")
	}
}

func TestNodePool_AddDiscoveredNode(t *testing.T) {
	p := newTestPool()
	err := p.AddDiscoveredNode("disc1", "https://discovered.example.com", 1.0)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	node := p.GetNode("disc1")
	if node == nil {
		t.Fatal("node not found")
	}
	if !node.Discovered {
		t.Error("expected Discovered=true")
	}
}

func TestNodePool_RemoveDiscoveredNodes(t *testing.T) {
	p := newTestPool()
	p.AddNode("manual1", "https://manual.example.com", 1.0)
	p.AddDiscoveredNode("disc1", "https://disc1.example.com", 1.0)
	p.AddDiscoveredNode("disc2", "https://disc2.example.com", 1.0)
	// Keep disc1, remove disc2
	removed := p.RemoveDiscoveredNodes(map[string]bool{"disc1": true})
	if removed != 1 {
		t.Errorf("removed = %d, want 1", removed)
	}
	if p.GetNode("manual1") == nil {
		t.Error("manual node should not be removed")
	}
	if p.GetNode("disc1") == nil {
		t.Error("disc1 should be kept")
	}
	if p.GetNode("disc2") != nil {
		t.Error("disc2 should be removed")
	}
}

func TestNodePool_ConcurrentAccess(t *testing.T) {
	p := newTestPool()
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	n := p.GetNode("rpc1")
	n.SetBlockHeight(100)
	n.SetLatency(10 * time.Millisecond)

	var wg sync.WaitGroup
	// Concurrent reads
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				p.SelectBest()
				p.Nodes()
				p.FinalizedHeight()
			}
		}()
	}
	// Concurrent writes
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("concurrent-%d", id)
			p.AddNode(nodeID, "https://"+nodeID+".example.com", 1.0)
			p.RemoveNode(nodeID)
		}(i)
	}
	wg.Wait()
}
