package pool

import (
	"sync"
	"time"
)

type nodeCounter struct {
	count     int
	limit     int
	resetDate string
}

type BudgetTracker struct {
	mu       sync.Mutex
	counters map[string]*nodeCounter
	nowFunc  func() time.Time
}

func NewBudgetTracker() *BudgetTracker {
	return &BudgetTracker{counters: make(map[string]*nodeCounter), nowFunc: time.Now}
}

func (bt *BudgetTracker) Register(nodeID string, dailyLimit int) {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	bt.counters[nodeID] = &nodeCounter{limit: dailyLimit, resetDate: bt.today()}
}

func (bt *BudgetTracker) TryRecord(nodeID string) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	nc, ok := bt.counters[nodeID]
	if !ok {
		return true
	} // not tracked = unlimited
	bt.maybeReset(nc)
	if nc.count >= nc.limit {
		return false
	}
	nc.count++
	return true
}

func (bt *BudgetTracker) IsExhausted(nodeID string) bool {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	nc, ok := bt.counters[nodeID]
	if !ok {
		return false
	}
	bt.maybeReset(nc)
	return nc.count >= nc.limit
}

func (bt *BudgetTracker) Remaining(nodeID string) int {
	bt.mu.Lock()
	defer bt.mu.Unlock()
	nc, ok := bt.counters[nodeID]
	if !ok {
		return -1
	} // -1 = unlimited
	bt.maybeReset(nc)
	rem := nc.limit - nc.count
	if rem < 0 {
		return 0
	}
	return rem
}

func (bt *BudgetTracker) maybeReset(nc *nodeCounter) {
	t := bt.today()
	if nc.resetDate != t {
		nc.count = 0
		nc.resetDate = t
	}
}

func (bt *BudgetTracker) today() string {
	return bt.nowFunc().UTC().Format("2006-01-02")
}
