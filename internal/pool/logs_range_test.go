package pool

import (
	"testing"
	"time"
)

func TestNode_LogsRangeLearning(t *testing.T) {
	n := NewNode("n", "http://x", 1, NewCircuitBreaker(0.5, time.Minute, time.Minute))
	now := time.Unix(1_000_000, 0)

	if n.RejectsLogsSpan(1_000_000, now) {
		t.Fatal("unknown node must not reject any span")
	}
	n.RecordLogsSpanFailure(10_000, now)
	if !n.RejectsLogsSpan(10_000, now) || !n.RejectsLogsSpan(50_000, now) {
		t.Error("spans >= failed span must be rejected")
	}
	if n.RejectsLogsSpan(9_999, now) {
		t.Error("smaller span must still be allowed")
	}
	// a larger failure doesn't loosen a tighter known limit
	n.RecordLogsSpanFailure(20_000, now)
	if !n.RejectsLogsSpan(15_000, now) {
		t.Error("tighter limit (10000) must be kept")
	}
	// tighter failure tightens
	n.RecordLogsSpanFailure(1_000, now)
	if !n.RejectsLogsSpan(1_000, now) {
		t.Error("limit should tighten to 1000")
	}
	// expires
	if n.RejectsLogsSpan(5_000, now.Add(LogsSpanFailureTTL+time.Second)) {
		t.Error("failure must expire after TTL")
	}
	// success at/above the failed span clears it
	n.RecordLogsSpanFailure(1_000, now)
	n.RecordLogsSpanSuccess(2_000)
	if n.RejectsLogsSpan(1_500, now) {
		t.Error("success at a larger span must clear the failure")
	}
	if n.MaxOKLogsSpan() != 2_000 {
		t.Errorf("MaxOKLogsSpan = %d, want 2000", n.MaxOKLogsSpan())
	}
}

func TestNodePool_MaxBlockHeight(t *testing.T) {
	p := newTestPool()
	if p.MaxBlockHeight() != 0 {
		t.Fatal("empty pool height must be 0")
	}
	p.AddNode("a", "http://a", 1)
	p.AddNode("b", "http://b", 1)
	p.GetNode("a").SetBlockHeight(100)
	p.GetNode("b").SetBlockHeight(250)
	if h := p.MaxBlockHeight(); h != 250 {
		t.Errorf("MaxBlockHeight = %d, want 250", h)
	}
}
