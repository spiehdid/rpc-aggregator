package pool

import (
	"sync"
	"time"
)

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type slidingWindow struct {
	buckets    []bucket
	bucketSize time.Duration
	numBuckets int
	current    int
	lastTick   time.Time
}

type bucket struct {
	successes int
	errors    int
}

func newSlidingWindow(window time.Duration, numBuckets int) *slidingWindow {
	return &slidingWindow{
		buckets:    make([]bucket, numBuckets),
		bucketSize: window / time.Duration(numBuckets),
		numBuckets: numBuckets,
		lastTick:   time.Now(),
	}
}

func (sw *slidingWindow) advance() {
	now := time.Now()
	elapsed := now.Sub(sw.lastTick)
	bucketsToAdvance := int(elapsed / sw.bucketSize)
	if bucketsToAdvance <= 0 {
		return
	}
	if bucketsToAdvance >= sw.numBuckets {
		for i := range sw.buckets {
			sw.buckets[i] = bucket{}
		}
		sw.current = 0
	} else {
		for i := 0; i < bucketsToAdvance; i++ {
			sw.current = (sw.current + 1) % sw.numBuckets
			sw.buckets[sw.current] = bucket{}
		}
	}
	sw.lastTick = now
}

func (sw *slidingWindow) recordSuccess() {
	sw.advance()
	sw.buckets[sw.current].successes++
}

func (sw *slidingWindow) recordError() {
	sw.advance()
	sw.buckets[sw.current].errors++
}

func (sw *slidingWindow) errorRate() float64 {
	sw.advance()
	var totalSucc, totalErr int
	for _, b := range sw.buckets {
		totalSucc += b.successes
		totalErr += b.errors
	}
	total := totalSucc + totalErr
	if total == 0 {
		return 0
	}
	return float64(totalErr) / float64(total)
}

func (sw *slidingWindow) reset() {
	for i := range sw.buckets {
		sw.buckets[i] = bucket{}
	}
	sw.current = 0
	sw.lastTick = time.Now()
}

type CircuitBreaker struct {
	mu              sync.Mutex
	state           CircuitState
	errorThreshold  float64
	recoveryTimeout time.Duration
	openedAt        time.Time
	window          *slidingWindow
}

func NewCircuitBreaker(errorThreshold float64, window, recoveryTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:           CircuitClosed,
		errorThreshold:  errorThreshold,
		recoveryTimeout: recoveryTimeout,
		window:          newSlidingWindow(window, 10),
	}
}

// State returns the current circuit state, transitioning Open→HalfOpen once
// the recovery timeout elapses. Recovery to Closed is driven by the health
// checker via RecordSuccess/RecordError.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitOpen && time.Since(cb.openedAt) >= cb.recoveryTimeout {
		cb.state = CircuitHalfOpen
	}
	return cb.state
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	// Promote Open→HalfOpen if the recovery timeout has elapsed.
	if cb.state == CircuitOpen && time.Since(cb.openedAt) >= cb.recoveryTimeout {
		cb.state = CircuitHalfOpen
	}
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
		cb.window.reset()
		return
	}
	cb.window.recordSuccess()
}

func (cb *CircuitBreaker) RecordError() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	// Promote Open→HalfOpen if the recovery timeout has elapsed.
	if cb.state == CircuitOpen && time.Since(cb.openedAt) >= cb.recoveryTimeout {
		cb.state = CircuitHalfOpen
	}
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.openedAt = time.Now()
		return
	}
	cb.window.recordError()
	if cb.window.errorRate() >= cb.errorThreshold {
		cb.state = CircuitOpen
		cb.openedAt = time.Now()
	}
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = CircuitClosed
	cb.window.reset()
}

func (cb *CircuitBreaker) ErrorRate() float64 {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.window.errorRate()
}
