package ws

import (
	"fmt"
	"sync"
	"testing"
)

func TestSubscriptionManager_Subscribe(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	subID, err := sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "upstream-sub-1", "node1")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if subID == "" {
		t.Error("empty sub ID")
	}
}

func TestSubscriptionManager_Sharing(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	sub1, _ := sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "upstream-sub-1", "node1")
	sub2, existed, err := sm.SubscribeShared("consumer-2", "ethereum", "newHeads", "[]")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if !existed {
		t.Error("should exist")
	}
	if sub1 == sub2 {
		t.Error("IDs should differ")
	}
	consumers := sm.GetConsumersForUpstream("upstream-sub-1")
	if len(consumers) != 2 {
		t.Errorf("got %d consumers, want 2", len(consumers))
	}
}

func TestSubscriptionManager_Unsubscribe(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	subID, _ := sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "upstream-sub-1", "node1")
	_, orphaned := sm.Unsubscribe("consumer-1", subID)
	if orphaned == nil {
		t.Error("should be last (orphaned)")
	}
}

func TestSubscriptionManager_UnsubscribeNotLast(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "upstream-sub-1", "node1")
	sm.SubscribeShared("consumer-2", "ethereum", "newHeads", "[]")
	sub1 := sm.GetConsumerSubs("consumer-1")
	_, orphaned := sm.Unsubscribe("consumer-1", sub1[0])
	if orphaned != nil {
		t.Error("should not be last (not orphaned)")
	}
}

func TestSubscriptionManager_RemoveConsumer(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "upstream-sub-1", "node1")
	sm.Subscribe("consumer-1", "ethereum", "logs", `[{"address":"0x123"}]`, "upstream-sub-2", "node1")
	orphaned := sm.RemoveConsumer("consumer-1")
	if len(orphaned) != 2 {
		t.Errorf("got %d orphaned, want 2", len(orphaned))
	}
}

func TestSubscriptionManager_MaxPerConn(t *testing.T) {
	sm := NewSubscriptionManager(2, 10000)
	sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "up-1", "node1")
	sm.Subscribe("consumer-1", "ethereum", "logs", "[]", "up-2", "node1")
	_, err := sm.Subscribe("consumer-1", "ethereum", "pending", "[]", "up-3", "node1")
	if err == nil {
		t.Error("should exceed limit")
	}
}

func TestSubscriptionManager_InvalidateNode(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	sm.Subscribe("consumer-1", "ethereum", "newHeads", "[]", "upstream-sub-1", "node1")
	affected := sm.InvalidateUpstream("upstream-sub-1")
	if len(affected) != 1 {
		t.Errorf("got %d affected, want 1", len(affected))
	}
}

func TestSubscriptionManager_ConcurrentAccess(t *testing.T) {
	sm := NewSubscriptionManager(100, 10000)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			consumerID := fmt.Sprintf("consumer-%d", id)
			subID, _ := sm.Subscribe(consumerID, "eth", "newHeads", "[]", fmt.Sprintf("up-%d", id), "node1")
			sm.GetConsumersForUpstream(fmt.Sprintf("up-%d", id))
			_, _ = sm.Unsubscribe(consumerID, subID)
		}(i)
	}
	wg.Wait()
}
