package ratelimit

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"golang.org/x/time/rate"
)

type Config struct {
	Enabled            bool
	TrustXForwardedFor bool
	DefaultRPS         float64
	DefaultBurst       int
	CleanupInterval    time.Duration
	CleanupMaxAge      time.Duration
	APIKeys            map[string]*APIKey // key string → config
}

type APIKey struct {
	Name    string
	Limiter *rate.Limiter
}

type ipEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64 // unix nano
}

type Limiter struct {
	cfg        Config
	ipLimiters sync.Map // IP → *ipEntry
	metrics    *metrics.Metrics
	ctx        context.Context
	cancel     context.CancelFunc
}

func New(cfg Config, m *metrics.Metrics) *Limiter {
	ctx, cancel := context.WithCancel(context.Background())
	rl := &Limiter{cfg: cfg, metrics: m, ctx: ctx, cancel: cancel}
	go rl.cleanupLoop()
	return rl
}

func (rl *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rl.cfg.Enabled {
			next.ServeHTTP(w, r)
			return
		}

		apiKey := r.Header.Get("X-API-Key")
		if apiKey != "" {
			ak, ok := rl.cfg.APIKeys[apiKey]
			if !ok {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"jsonrpc":"2.0","error":{"code":-32029,"message":"invalid API key"},"id":null}`))
				return
			}
			if !ak.Limiter.Allow() {
				rl.metrics.RateLimitRejected.WithLabelValues("api_key").Inc()
				writeRateLimitError(w)
				return
			}
			rl.metrics.RateLimitAllowed.WithLabelValues("api_key").Inc()
			next.ServeHTTP(w, r)
			return
		}

		ip := rl.extractIP(r)
		entry := rl.getOrCreateIPEntry(ip)
		entry.lastSeen.Store(time.Now().UnixNano())

		if !entry.limiter.Allow() {
			rl.metrics.RateLimitRejected.WithLabelValues("ip").Inc()
			writeRateLimitError(w)
			return
		}
		rl.metrics.RateLimitAllowed.WithLabelValues("ip").Inc()
		next.ServeHTTP(w, r)
	})
}

func (rl *Limiter) getOrCreateIPEntry(ip string) *ipEntry {
	if v, ok := rl.ipLimiters.Load(ip); ok {
		return v.(*ipEntry)
	}
	entry := &ipEntry{limiter: rate.NewLimiter(rate.Limit(rl.cfg.DefaultRPS), rl.cfg.DefaultBurst)}
	entry.lastSeen.Store(time.Now().UnixNano())
	actual, _ := rl.ipLimiters.LoadOrStore(ip, entry)
	return actual.(*ipEntry)
}

func (rl *Limiter) cleanupLoop() {
	ticker := time.NewTicker(rl.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-rl.ctx.Done():
			return
		case <-ticker.C:
			cutoff := time.Now().Add(-rl.cfg.CleanupMaxAge).UnixNano()
			rl.ipLimiters.Range(func(key, value interface{}) bool {
				entry := value.(*ipEntry)
				if entry.lastSeen.Load() < cutoff {
					rl.ipLimiters.Delete(key)
				}
				return true
			})
		}
	}
}

func (rl *Limiter) Close() {
	rl.cancel()
}

func (rl *Limiter) extractIP(r *http.Request) string {
	if rl.cfg.TrustXForwardedFor {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.SplitN(xff, ",", 2)
			return strings.TrimSpace(parts[0])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeRateLimitError(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "1")
	w.WriteHeader(http.StatusTooManyRequests)
	resp := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      nil,
		"error":   map[string]interface{}{"code": -32029, "message": "rate limit exceeded"},
	}
	json.NewEncoder(w).Encode(resp)
}
