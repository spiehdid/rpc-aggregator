package pool

import (
	"testing"
	"time"
)

func TestBudgetTracker_TryRecord(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("paid-node", 3)

	if !bt.TryRecord("paid-node") {
		t.Error("1st should be allowed")
	}
	if !bt.TryRecord("paid-node") {
		t.Error("2nd should be allowed")
	}
	if !bt.TryRecord("paid-node") {
		t.Error("3rd should be allowed")
	}
	if bt.TryRecord("paid-node") {
		t.Error("4th should be denied")
	}
}

func TestBudgetTracker_IsExhausted(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("paid-node", 2)

	if bt.IsExhausted("paid-node") {
		t.Error("should not be exhausted initially")
	}
	bt.TryRecord("paid-node")
	bt.TryRecord("paid-node")
	if !bt.IsExhausted("paid-node") {
		t.Error("should be exhausted after 2 records")
	}
}

func TestBudgetTracker_UnregisteredNode(t *testing.T) {
	bt := NewBudgetTracker()
	if !bt.TryRecord("unknown") {
		t.Error("unregistered should always be allowed")
	}
	if bt.IsExhausted("unknown") {
		t.Error("unregistered should never be exhausted")
	}
	if bt.Remaining("unknown") != -1 {
		t.Error("unregistered remaining should be -1")
	}
}

func TestBudgetTracker_Remaining(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("node", 5)
	if bt.Remaining("node") != 5 {
		t.Errorf("remaining = %d, want 5", bt.Remaining("node"))
	}
	bt.TryRecord("node")
	bt.TryRecord("node")
	if bt.Remaining("node") != 3 {
		t.Errorf("remaining = %d, want 3", bt.Remaining("node"))
	}
}

func TestBudgetTracker_DailyReset(t *testing.T) {
	bt := NewBudgetTracker()
	bt.Register("node", 2)
	bt.TryRecord("node")
	bt.TryRecord("node")
	if !bt.IsExhausted("node") {
		t.Fatal("should be exhausted")
	}

	// Simulate next day
	bt.nowFunc = func() time.Time { return time.Now().Add(25 * time.Hour) }
	if bt.IsExhausted("node") {
		t.Error("should reset after day change")
	}
	if bt.Remaining("node") != 2 {
		t.Errorf("remaining = %d, want 2", bt.Remaining("node"))
	}
}
