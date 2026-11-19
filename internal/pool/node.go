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

	// learned eth_getLogs block-range limits (best-effort, lock-free)
	logsFailSpan atomic.Uint64 // smallest span that failed recently; 0 = unknown
	logsFailAt   atomic.Int64  // unix nanos of that failure
	logsOKSpan   atomic.Uint64 // largest span that succeeded
}

// LogsSpanFailureTTL bounds how long a learned range limit is trusted; providers change limits
// and some failures are transient.
const LogsSpanFailureTTL = 10 * time.Minute

// RecordLogsSpanFailure remembers that a log query of `span` blocks was refused by this node.
func (n *Node) RecordLogsSpanFailure(span uint64, now time.Time) {
	cur := n.logsFailSpan.Load()
	expired := now.Sub(time.Unix(0, n.logsFailAt.Load())) >= LogsSpanFailureTTL
	if cur == 0 || span < cur || expired {
		n.logsFailSpan.Store(span)
		n.logsFailAt.Store(now.UnixNano())
	}
}

// RecordLogsSpanSuccess remembers a served span and clears a stale limit it disproves.
func (n *Node) RecordLogsSpanSuccess(span uint64) {
	for {
		ok := n.logsOKSpan.Load()
		if span <= ok || n.logsOKSpan.CompareAndSwap(ok, span) {
			break
		}
	}
	if f := n.logsFailSpan.Load(); f != 0 && span >= f {
		n.logsFailSpan.CompareAndSwap(f, 0)
	}
}

// RejectsLogsSpan reports whether this node recently refused log queries at least this wide.
func (n *Node) RejectsLogsSpan(span uint64, now time.Time) bool {
	f := n.logsFailSpan.Load()
	return f != 0 && span >= f && now.Sub(time.Unix(0, n.logsFailAt.Load())) < LogsSpanFailureTTL
}

func (n *Node) MaxOKLogsSpan() uint64 { return n.logsOKSpan.Load() }

// LogsFailSpan returns the currently trusted refused span (0 if none or expired).
func (n *Node) LogsFailSpan(now time.Time) uint64 {
	f := n.logsFailSpan.Load()
	if f == 0 || now.Sub(time.Unix(0, n.logsFailAt.Load())) >= LogsSpanFailureTTL {
		return 0
	}
	return f
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
