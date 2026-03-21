package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"golang.org/x/time/rate"
)

func testMetrics() *metrics.Metrics {
	return metrics.New(prometheus.NewRegistry())
}

func TestRateLimiter_Disabled(t *testing.T) {
	rl := New(Config{Enabled: false, DefaultRPS: 1, DefaultBurst: 1, CleanupInterval: time.Minute, CleanupMaxAge: time.Minute}, testMetrics())
	defer rl.Close()

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 100; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, w.Code)
		}
	}
}

func TestRateLimiter_IPLimit(t *testing.T) {
	rl := New(Config{Enabled: true, DefaultRPS: 2, DefaultBurst: 2, CleanupInterval: time.Minute, CleanupMaxAge: time.Minute}, testMetrics())
	defer rl.Close()

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First 2 requests should pass (burst=2)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", nil)
		req.RemoteAddr = "1.2.3.4:1234"
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, w.Code)
		}
	}

	// 3rd request should be rejected
	req := httptest.NewRequest(http.MethodPost, "/eth", nil)
	req.RemoteAddr = "1.2.3.4:1234"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After header")
	}
}

func TestRateLimiter_APIKey(t *testing.T) {
	apiKeys := map[string]*APIKey{
		"test-key": {Name: "test", Limiter: rate.NewLimiter(10, 5)},
	}
	rl := New(Config{Enabled: true, DefaultRPS: 1, DefaultBurst: 1, CleanupInterval: time.Minute, CleanupMaxAge: time.Minute, APIKeys: apiKeys}, testMetrics())
	defer rl.Close()

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// API key with burst=5 should allow 5 requests
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest(http.MethodPost, "/eth", nil)
		req.Header.Set("X-API-Key", "test-key")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, w.Code)
		}
	}
}

func TestRateLimiter_InvalidAPIKey(t *testing.T) {
	rl := New(Config{Enabled: true, DefaultRPS: 100, DefaultBurst: 100, CleanupInterval: time.Minute, CleanupMaxAge: time.Minute, APIKeys: map[string]*APIKey{}}, testMetrics())
	defer rl.Close()

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/eth", nil)
	req.Header.Set("X-API-Key", "invalid-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestRateLimiter_DifferentIPsIndependent(t *testing.T) {
	rl := New(Config{Enabled: true, DefaultRPS: 1, DefaultBurst: 1, CleanupInterval: time.Minute, CleanupMaxAge: time.Minute}, testMetrics())
	defer rl.Close()

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Two different IPs should have independent limits
	for _, ip := range []string{"1.2.3.4:1234", "5.6.7.8:5678"} {
		req := httptest.NewRequest(http.MethodPost, "/eth", nil)
		req.RemoteAddr = ip
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("IP %s: status = %d, want 200", ip, w.Code)
		}
	}
}

func TestRateLimiter_XForwardedFor(t *testing.T) {
	rl := New(Config{Enabled: true, TrustXForwardedFor: true, DefaultRPS: 1, DefaultBurst: 1, CleanupInterval: time.Minute, CleanupMaxAge: time.Minute}, testMetrics())
	defer rl.Close()

	handler := rl.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// First request from 10.0.0.1 via X-Forwarded-For
	req1 := httptest.NewRequest(http.MethodPost, "/eth", nil)
	req1.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.1")
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("first: status = %d", w1.Code)
	}

	// Second request from same forwarded IP should be rejected
	req2 := httptest.NewRequest(http.MethodPost, "/eth", nil)
	req2.Header.Set("X-Forwarded-For", "10.0.0.1, 192.168.1.1")
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, req2)
	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("second: status = %d, want 429", w2.Code)
	}
}
