package pool

import (
	"math"
	"sync/atomic"
	"time"
)

type Node struct {
	ID          string
	URL         string
	ChainID     string
	blockHeight atomic.Uint64
	latency     atomic.Int64 // nanoseconds
	finalHeight atomic.Uint64
	weight      atomic.Uint64 // stored as Float64 bits
	Discovered  bool
	Tier        string // "free" or "paid"
	DailyLimit  int    // 0 = unlimited
	circuit     *CircuitBreaker
}

func NewNode(id, url string, weight float64, cb *CircuitBreaker) *Node {
	n := &Node{ID: id, URL: url, circuit: cb}
	n.SetWeight(weight)
	return n
}

func (n *Node) SetWeight(w float64) { n.weight.Store(math.Float64bits(w)) }
func (n *Node) GetWeight() float64  { return math.Float64frombits(n.weight.Load()) }

func (n *Node) CircuitBreaker() *CircuitBreaker { return n.circuit }
func (n *Node) CircuitState() CircuitState      { return n.circuit.State() }
func (n *Node) ErrorRate() float64              { return n.circuit.ErrorRate() }

func (n *Node) SetBlockHeight(h uint64)     { n.blockHeight.Store(h) }
func (n *Node) GetBlockHeight() uint64      { return n.blockHeight.Load() }
func (n *Node) SetLatency(d time.Duration)  { n.latency.Store(int64(d)) }
func (n *Node) GetLatency() time.Duration   { return time.Duration(n.latency.Load()) }
func (n *Node) SetFinalizedHeight(h uint64) { n.finalHeight.Store(h) }
func (n *Node) GetFinalizedHeight() uint64  { return n.finalHeight.Load() }
