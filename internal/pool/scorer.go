package pool

import (
	"sort"
	"time"
)

type ScoringWeights struct {
	Latency     float64
	BlockHeight float64
	ErrorRate   float64
}

type Scorer struct {
	weights     ScoringWeights
	budget      *BudgetTracker
	paidPenalty float64
}

func NewScorer(weights ScoringWeights) *Scorer {
	return &Scorer{weights: weights, paidPenalty: 0.5}
}

func NewScorerWithBudget(weights ScoringWeights, budget *BudgetTracker, paidPenalty float64) *Scorer {
	return &Scorer{weights: weights, budget: budget, paidPenalty: paidPenalty}
}

func (s *Scorer) costMultiplier(n *Node) float64 {
	if n.Tier != "paid" {
		return 1.0
	}
	if s.budget != nil && s.budget.IsExhausted(n.ID) {
		return 0.0
	}
	return s.paidPenalty
}

func (s *Scorer) Score(n *Node, minLatency, maxLatency time.Duration, minHeight, maxHeight uint64) float64 {
	normLatency := normalize(float64(n.GetLatency()), float64(minLatency), float64(maxLatency))
	normHeight := normalize(float64(n.GetBlockHeight()), float64(minHeight), float64(maxHeight))
	errRate := n.ErrorRate()
	score := s.weights.Latency*(1-normLatency) +
		s.weights.BlockHeight*normHeight +
		s.weights.ErrorRate*(1-errRate)
	return n.GetWeight() * score * s.costMultiplier(n)
}

func (s *Scorer) filterAvailable(nodes []*Node) []*Node {
	var available []*Node
	for _, n := range nodes {
		state := n.CircuitState()
		if state == CircuitOpen || state == CircuitHalfOpen {
			continue
		}
		if s.costMultiplier(n) == 0.0 {
			continue
		}
		available = append(available, n)
	}
	return available
}

func (s *Scorer) SelectBest(nodes []*Node) *Node {
	available := s.filterAvailable(nodes)
	if len(available) == 0 {
		return nil
	}
	if len(available) == 1 {
		return available[0]
	}
	minLat, maxLat, minH, maxH := computeBounds(available)
	var best *Node
	bestScore := -1.0
	for _, n := range available {
		score := s.Score(n, minLat, maxLat, minH, maxH)
		if score > bestScore {
			bestScore = score
			best = n
		}
	}
	return best
}

func (s *Scorer) SelectTopN(nodes []*Node, n int) []*Node {
	available := s.filterAvailable(nodes)
	if len(available) == 0 {
		return nil
	}
	if len(available) <= n {
		return available
	}
	minLat, maxLat, minH, maxH := computeBounds(available)
	type scored struct {
		node  *Node
		score float64
	}
	scoredNodes := make([]scored, len(available))
	for i, nd := range available {
		scoredNodes[i] = scored{node: nd, score: s.Score(nd, minLat, maxLat, minH, maxH)}
	}
	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score > scoredNodes[j].score
	})
	result := make([]*Node, n)
	for i := 0; i < n; i++ {
		result[i] = scoredNodes[i].node
	}
	return result
}

func computeBounds(nodes []*Node) (minLat, maxLat time.Duration, minH, maxH uint64) {
	if len(nodes) == 0 {
		return 0, 0, 0, 0
	}
	minLat = nodes[0].GetLatency()
	maxLat = nodes[0].GetLatency()
	minH = nodes[0].GetBlockHeight()
	maxH = nodes[0].GetBlockHeight()
	for _, n := range nodes[1:] {
		if n.GetLatency() < minLat {
			minLat = n.GetLatency()
		}
		if n.GetLatency() > maxLat {
			maxLat = n.GetLatency()
		}
		if n.GetBlockHeight() < minH {
			minH = n.GetBlockHeight()
		}
		if n.GetBlockHeight() > maxH {
			maxH = n.GetBlockHeight()
		}
	}
	return
}

func normalize(val, min, max float64) float64 {
	if max == min {
		return 1.0
	}
	return (val - min) / (max - min)
}
