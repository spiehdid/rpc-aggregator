package pool

import (
	"math"
	"testing"
	"time"
)

func defaultWeights() ScoringWeights {
	return ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
}

func makeTestNode(id string, latency time.Duration, blockHeight uint64, weight float64, tier string, cb *CircuitBreaker) *Node {
	n := &Node{ID: id, Tier: tier, circuit: cb}
	n.SetWeight(weight)
	n.SetLatency(latency)
	n.SetBlockHeight(blockHeight)
	return n
}

func TestScorer_SelectsBestNode(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	nodes := []*Node{
		makeTestNode("fast", 10*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("slow", 200*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best == nil {
		t.Fatal("SelectBest returned nil")
	}
	if best.ID != "fast" {
		t.Errorf("selected %q, want %q", best.ID, "fast")
	}
}

func TestScorer_SkipsOpenCircuit(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	openCB := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
	for i := 0; i < 10; i++ {
		openCB.RecordError()
	}
	nodes := []*Node{
		makeTestNode("broken", 1*time.Millisecond, 200, 1.0, "", openCB),
		makeTestNode("ok", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best.ID != "ok" {
		t.Errorf("selected %q, want %q", best.ID, "ok")
	}
}

func TestScorer_ReturnsNilWhenAllOpen(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	openCB := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
	for i := 0; i < 10; i++ {
		openCB.RecordError()
	}
	n := &Node{ID: "a", circuit: openCB}
	n.SetWeight(1.0)
	nodes := []*Node{n}
	best := scorer.SelectBest(nodes)
	if best != nil {
		t.Errorf("expected nil when all circuits open, got %q", best.ID)
	}
}

func TestScorer_SingleNode(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	nodes := []*Node{
		makeTestNode("only", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best == nil || best.ID != "only" {
		t.Error("single node should always be selected")
	}
}

func TestScorer_EqualNodes(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	nodes := []*Node{
		makeTestNode("a", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("b", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best == nil {
		t.Fatal("SelectBest returned nil for equal nodes")
	}
}

func TestScorer_WeightMultiplier(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	nodes := []*Node{
		makeTestNode("low-weight", 10*time.Millisecond, 100, 0.1, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("high-weight", 50*time.Millisecond, 100, 2.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best.ID != "high-weight" {
		t.Errorf("selected %q, want %q", best.ID, "high-weight")
	}
}

func TestScorer_PrefersHigherBlockHeight(t *testing.T) {
	scorer := NewScorer(ScoringWeights{Latency: 0.0, BlockHeight: 1.0, ErrorRate: 0.0})
	nodes := []*Node{
		makeTestNode("behind", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("ahead", 50*time.Millisecond, 200, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best.ID != "ahead" {
		t.Errorf("selected %q, want %q", best.ID, "ahead")
	}
}

func TestScorer_SelectTopN(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	nodes := []*Node{
		makeTestNode("a", 10*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("b", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("c", 200*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	top := scorer.SelectTopN(nodes, 2)
	if len(top) != 2 {
		t.Fatalf("SelectTopN(2) returned %d nodes, want 2", len(top))
	}
	if top[0].ID != "a" {
		t.Errorf("top[0] = %q, want %q", top[0].ID, "a")
	}
}

func TestScoreValue(t *testing.T) {
	scorer := NewScorer(defaultWeights())
	node := makeTestNode("", 50*time.Millisecond, 100, 1.0, "", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second))
	score := scorer.Score(node, 10*time.Millisecond, 200*time.Millisecond, 50, 150)
	if math.IsNaN(score) || math.IsInf(score, 0) {
		t.Errorf("score is %v, expected finite", score)
	}
	if score <= 0 {
		t.Errorf("score = %v, expected positive", score)
	}
}

func TestScorer_PrefersFreeOverPaid(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("paid", 100)
	scorer := NewScorerWithBudget(defaultWeights(), bt, 0.5)
	nodes := []*Node{
		makeTestNode("free", 50*time.Millisecond, 100, 1.0, "free", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
		makeTestNode("paid", 50*time.Millisecond, 100, 1.0, "paid", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best.ID != "free" {
		t.Errorf("selected %q, want free", best.ID)
	}
}

func TestScorer_FallbackToPaid(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("paid", 100)
	scorer := NewScorerWithBudget(defaultWeights(), bt, 0.5)
	// Free node has open circuit
	freeCB := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
	for i := 0; i < 10; i++ {
		freeCB.RecordError()
	}
	nodes := []*Node{
		makeTestNode("free", 50*time.Millisecond, 100, 1.0, "free", freeCB),
		makeTestNode("paid", 50*time.Millisecond, 100, 1.0, "paid", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best == nil || best.ID != "paid" {
		t.Errorf("expected paid fallback, got %v", best)
	}
}

func TestScorer_ExhaustedPaidExcluded(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("paid", 1)
	bt.TryRecord("paid") // exhaust budget
	scorer := NewScorerWithBudget(defaultWeights(), bt, 0.5)
	nodes := []*Node{
		makeTestNode("paid", 10*time.Millisecond, 100, 1.0, "paid", NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)),
	}
	best := scorer.SelectBest(nodes)
	if best != nil {
		t.Errorf("exhausted paid should be excluded, got %v", best)
	}
}

func TestComputeBounds_EmptySlice(t *testing.T) {
	minLat, maxLat, minH, maxH := computeBounds([]*Node{})
	if minLat != 0 || maxLat != 0 || minH != 0 || maxH != 0 {
		t.Error("expected all zeros for empty slice")
	}
}
