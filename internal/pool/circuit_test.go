package pool

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_StartsClosedAllowsRequests(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 10*time.Second)
	if cb.State() != CircuitClosed {
		t.Errorf("state = %v, want CircuitClosed", cb.State())
	}
}

func TestCircuitBreaker_OpensOnHighErrorRate(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 10*time.Second)
	for i := 0; i < 4; i++ {
		cb.RecordSuccess()
	}
	for i := 0; i < 6; i++ {
		cb.RecordError()
	}
	if cb.State() != CircuitOpen {
		t.Errorf("state = %v, want CircuitOpen", cb.State())
	}
}

func TestCircuitBreaker_StaysClosedBelowThreshold(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 10*time.Second)
	for i := 0; i < 7; i++ {
		cb.RecordSuccess()
	}
	for i := 0; i < 3; i++ {
		cb.RecordError()
	}
	if cb.State() != CircuitClosed {
		t.Errorf("state = %v, want CircuitClosed (30%% errors < 50%% threshold)", cb.State())
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 50*time.Millisecond)
	for i := 0; i < 10; i++ {
		cb.RecordError()
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want CircuitOpen", cb.State())
	}
	time.Sleep(60 * time.Millisecond)
	if cb.State() != CircuitHalfOpen {
		t.Errorf("state = %v, want CircuitHalfOpen after recovery timeout", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenToClosedOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 50*time.Millisecond)
	for i := 0; i < 10; i++ {
		cb.RecordError()
	}
	time.Sleep(60 * time.Millisecond)
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Errorf("state = %v, want CircuitClosed after successful probe", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenToOpenOnError(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 50*time.Millisecond)
	for i := 0; i < 10; i++ {
		cb.RecordError()
	}
	time.Sleep(60 * time.Millisecond)
	cb.RecordError()
	if cb.State() != CircuitOpen {
		t.Errorf("state = %v, want CircuitOpen after failed probe", cb.State())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 10*time.Second)
	for i := 0; i < 10; i++ {
		cb.RecordError()
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("state = %v, want CircuitOpen", cb.State())
	}
	cb.Reset()
	if cb.State() != CircuitClosed {
		t.Errorf("state = %v, want CircuitClosed after reset", cb.State())
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(0.5, 30*time.Second, 30*time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				cb.State()
				cb.ErrorRate()
				if j%3 == 0 {
					cb.RecordError()
				} else {
					cb.RecordSuccess()
				}
			}
		}()
	}
	wg.Wait()
}
