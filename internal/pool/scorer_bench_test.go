package pool

import (
	"testing"
	"time"
)

func BenchmarkScorer_SelectBest_10Nodes(b *testing.B) {
	scorer := NewScorer(ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3})
	nodes := make([]*Node, 10)
	for i := range nodes {
		cb := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
		n := &Node{
			ID: "node-" + string(rune('a'+i)), circuit: cb,
		}
		n.SetWeight(1.0)
		n.SetLatency(time.Duration(10+i*5) * time.Millisecond)
		n.SetBlockHeight(uint64(100000 + i))
		nodes[i] = n
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.SelectBest(nodes)
	}
}

func BenchmarkScorer_SelectBest_50Nodes(b *testing.B) {
	scorer := NewScorer(ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3})
	nodes := make([]*Node, 50)
	for i := range nodes {
		cb := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
		n := &Node{
			ID: "node-" + string(rune(i)), circuit: cb,
		}
		n.SetWeight(1.0)
		n.SetLatency(time.Duration(10+i*3) * time.Millisecond)
		n.SetBlockHeight(uint64(100000 + i))
		nodes[i] = n
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.SelectBest(nodes)
	}
}

func BenchmarkScorer_SelectTopN(b *testing.B) {
	scorer := NewScorer(ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3})
	nodes := make([]*Node, 20)
	for i := range nodes {
		cb := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
		n := &Node{
			ID: "node-" + string(rune(i)), circuit: cb,
		}
		n.SetWeight(1.0)
		n.SetLatency(time.Duration(10+i*5) * time.Millisecond)
		n.SetBlockHeight(uint64(100000 + i))
		nodes[i] = n
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scorer.SelectTopN(nodes, 3)
	}
}
