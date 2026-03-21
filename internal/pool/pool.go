package pool

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

type CircuitBreakerConfig struct {
	ErrorThreshold  float64
	Window          time.Duration
	RecoveryTimeout time.Duration
}

type NodePool struct {
	mu      sync.RWMutex
	chainID string
	nodes   map[string]*Node
	scorer  *Scorer
	cbCfg   CircuitBreakerConfig
}

func NewNodePool(chainID string, scorer *Scorer, cbCfg CircuitBreakerConfig) *NodePool {
	return &NodePool{chainID: chainID, nodes: make(map[string]*Node), scorer: scorer, cbCfg: cbCfg}
}

func (p *NodePool) AddNode(id, url string, weight float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.nodes[id]; exists {
		return fmt.Errorf("node %q already exists", id)
	}
	cb := NewCircuitBreaker(p.cbCfg.ErrorThreshold, p.cbCfg.Window, p.cbCfg.RecoveryTimeout)
	p.nodes[id] = NewNode(id, url, weight, cb)
	return nil
}

func (p *NodePool) RemoveNode(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.nodes[id]; !exists {
		return fmt.Errorf("node %q not found", id)
	}
	delete(p.nodes, id)
	return nil
}

func (p *NodePool) GetNode(id string) *Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nodes[id]
}

func (p *NodePool) Nodes() []*Node {
	p.mu.RLock()
	defer p.mu.RUnlock()
	nodes := make([]*Node, 0, len(p.nodes))
	for _, n := range p.nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	return nodes
}

func (p *NodePool) SelectBest() *Node { return p.scorer.SelectBest(p.Nodes()) }

func (p *NodePool) SelectBestExcluding(exclude map[string]bool) *Node {
	nodes := p.Nodes()
	var filtered []*Node
	for _, n := range nodes {
		if !exclude[n.ID] {
			filtered = append(filtered, n)
		}
	}
	return p.scorer.SelectBest(filtered)
}

func (p *NodePool) SelectTopN(n int) []*Node { return p.scorer.SelectTopN(p.Nodes(), n) }

func (p *NodePool) UpdateWeight(id string, weight float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	node, exists := p.nodes[id]
	if !exists {
		return fmt.Errorf("node %q not found", id)
	}
	node.SetWeight(weight)
	return nil
}

func (p *NodePool) FinalizedHeight() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var minHeight uint64
	for _, n := range p.nodes {
		if n.CircuitState() == CircuitOpen {
			continue
		}
		fh := n.GetFinalizedHeight()
		if fh == 0 {
			continue
		}
		if minHeight == 0 || fh < minHeight {
			minHeight = fh
		}
	}
	return minHeight
}

func (p *NodePool) ResetCircuitBreaker(id string) error {
	p.mu.RLock()
	node, exists := p.nodes[id]
	p.mu.RUnlock()
	if !exists {
		return fmt.Errorf("node %q not found", id)
	}
	node.CircuitBreaker().Reset()
	return nil
}

func (p *NodePool) AddDiscoveredNode(id, url string, weight float64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.nodes[id]; exists {
		return fmt.Errorf("node %q already exists", id)
	}
	cb := NewCircuitBreaker(p.cbCfg.ErrorThreshold, p.cbCfg.Window, p.cbCfg.RecoveryTimeout)
	node := NewNode(id, url, weight, cb)
	node.Discovered = true
	p.nodes[id] = node
	return nil
}

func (p *NodePool) RemoveDiscoveredNodes(keepIDs map[string]bool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	removed := 0
	for id, node := range p.nodes {
		if node.Discovered && !keepIDs[id] {
			delete(p.nodes, id)
			removed++
		}
	}
	return removed
}
