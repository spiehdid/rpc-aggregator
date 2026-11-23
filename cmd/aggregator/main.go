package main

import (
	"context"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/solana"
	"github.com/spiehdid/rpc-aggregator/internal/admin"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/config"
	"github.com/spiehdid/rpc-aggregator/internal/discovery"
	"github.com/spiehdid/rpc-aggregator/internal/health"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
	"github.com/spiehdid/rpc-aggregator/internal/ratelimit"
	"github.com/spiehdid/rpc-aggregator/internal/router"
	"github.com/spiehdid/rpc-aggregator/internal/ws"
	"golang.org/x/time/rate"
)

func sanitizeNodeID(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	host := u.Hostname()
	h := crc32.ChecksumIEEE([]byte(rawURL))
	return fmt.Sprintf("%s-%08x", host, h)
}

func main() {
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	registry := adapter.NewRegistry()
	registry.Register("evm", evm.New())
	registry.Register("solana", solana.New())

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	startedAt := time.Now()

	chainRoutes := make(map[string]*router.ChainRoute)
	healthChains := make(map[string]health.ChainEntry)
	adminChains := make(map[string]admin.ChainInfo)

	cbCfg := pool.CircuitBreakerConfig{
		ErrorThreshold:  cfg.CircuitBreaker.ErrorThreshold,
		Window:          cfg.CircuitBreaker.Window,
		RecoveryTimeout: cfg.CircuitBreaker.RecoveryTimeout,
	}

	budget := pool.NewBudgetTracker()

	for chainName, chainCfg := range cfg.Chains {
		a, err := registry.Get(chainCfg.Adapter)
		if err != nil {
			slog.Error("unknown adapter", "chain", chainName, "adapter", chainCfg.Adapter)
			os.Exit(1)
		}

		weights := pool.ScoringWeights{
			Latency:     cfg.Scoring.Weights.Latency,
			BlockHeight: cfg.Scoring.Weights.BlockHeight,
			ErrorRate:   cfg.Scoring.Weights.ErrorRate,
		}
		scorer := pool.NewScorerWithBudget(weights, budget, cfg.Scoring.Weights.PaidPenalty)
		p := pool.NewNodePool(chainName, scorer, cbCfg)

		for _, nodeCfg := range chainCfg.Nodes {
			id := nodeCfg.ID
			if id == "" {
				id = sanitizeNodeID(nodeCfg.URL)
			}
			if err := p.AddNode(id, nodeCfg.URL, nodeCfg.Weight); err != nil {
				slog.Error("failed to add node", "chain", chainName, "url", nodeCfg.URL, "error", err)
			}
			if nodeCfg.Tier == "paid" {
				node := p.GetNode(id)
				node.Tier = "paid"
				node.DailyLimit = nodeCfg.Budget.DailyLimit
				budget.Register(id, nodeCfg.Budget.DailyLimit)
			}
		}

		c := cache.New(chainCfg.Cache.MaxEntries)
		if chainCfg.Cache.MaxBytes > 0 {
			c.SetMaxBytes(chainCfg.Cache.MaxBytes)
		}
		var rules []cache.Rule
		for _, r := range chainCfg.Cache.Rules {
			rules = append(rules, cache.Rule{Method: r.Method, TTL: r.TTL, Condition: r.Condition})
		}
		rm := cache.NewRuleMatcher(rules, chainCfg.Cache.DefaultTTL)

		chainRoutes[chainCfg.Path] = &router.ChainRoute{Name: chainName, Adapter: a, Pool: p, Cache: c, RuleMatcher: rm}
		healthChains[chainName] = health.ChainEntry{Pool: p, Adapter: a, Finalization: chainCfg.IsFinalizationEnabled()}
		adminChains[chainName] = admin.ChainInfo{Pool: p, Cache: c, Scorer: scorer}
	}

	var discoveryChains []discovery.ChainDiscovery
	for chainName, chainCfg := range cfg.Chains {
		if chainCfg.Discovery.Enabled {
			for _, route := range chainRoutes {
				if route.Name == chainName {
					discoveryChains = append(discoveryChains, discovery.ChainDiscovery{
						ChainName: chainName,
						ChainID:   chainCfg.Discovery.ChainID,
						Pool:      route.Pool,
						Adapter:   route.Adapter,
						Interval:  chainCfg.Discovery.Interval,
					})
					break
				}
			}
		}
	}

	if cfg.Server.AdminToken == "" {
		slog.Warn("admin API has no authentication configured - set server.admin_token for production use")
	}

	rtr := router.New(chainRoutes, m, cfg.Retry.MaxAttempts, cfg.Retry.CriticalFanOut, cfg.Retry.MaxBatchSize, cfg.Server.RequestTimeout)
	rtr.SetBudget(budget)

	// Wire hot-reload watcher for retry settings. The watcher is set up after
	// the router so the callback can reference rtr.
	var cfgWatcher *config.Watcher
	if cfgPath != "" {
		cfgWatcher, err = config.NewWatcher(cfgPath, func(newCfg *config.Config) {
			slog.Info("config file changed",
				"new_retry_max_attempts", newCfg.Retry.MaxAttempts,
				"new_scoring_weights", newCfg.Scoring.Weights,
			)
			rtr.SetRetryConfig(newCfg.Retry.MaxAttempts, newCfg.Retry.CriticalFanOut, newCfg.Retry.MaxBatchSize)
		})
		if err != nil {
			slog.Warn("config watcher failed to start", "error", err)
		}
	}

	checker := health.New(healthChains, cfg.HealthCheck.Interval, cfg.HealthCheck.Timeout)
	adminAPI := admin.New(adminChains, startedAt, cfg.Server.AdminToken)

	wsCfg := ws.HandlerConfig{
		MaxConnections:          cfg.WebSocket.MaxConnections,
		MaxSubscriptionsPerConn: cfg.WebSocket.MaxSubscriptionsPerConn,
		MaxTotalSubscriptions:   cfg.WebSocket.MaxTotalSubscriptions,
		MaxMessageSize:          cfg.WebSocket.MaxMessageSize,
		SendQueueSize:           cfg.WebSocket.SendQueueSize,
		WriteTimeout:            cfg.WebSocket.WriteTimeout,
		ConsumerReadBuffer:      cfg.WebSocket.ConsumerReadBuffer,
		ConsumerWriteBuffer:     cfg.WebSocket.ConsumerWriteBuffer,
		UpstreamReadBuffer:      cfg.WebSocket.UpstreamReadBuffer,
		UpstreamWriteBuffer:     cfg.WebSocket.UpstreamWriteBuffer,
		PingInterval:            cfg.WebSocket.PingInterval,
		PongTimeout:             cfg.WebSocket.PongTimeout,
		ReconnectBackoff:        cfg.WebSocket.ReconnectBackoff,
		ReconnectMaxBackoff:     cfg.WebSocket.ReconnectMaxBackoff,
		AllowedOrigins:          cfg.Server.AllowedOrigins,
	}
	wsHandler := ws.NewHandler(chainRoutes, rtr, m, wsCfg)

	var mainHandler http.Handler = wsHandler
	if cfg.RateLimit.Enabled {
		apiKeys := make(map[string]*ratelimit.APIKey, len(cfg.RateLimit.APIKeys))
		for _, ak := range cfg.RateLimit.APIKeys {
			apiKeys[ak.Key] = &ratelimit.APIKey{
				Name:    ak.Name,
				Limiter: rate.NewLimiter(rate.Limit(ak.RequestsPerSecond), ak.Burst),
			}
		}
		rl := ratelimit.New(ratelimit.Config{
			Enabled:            true,
			TrustXForwardedFor: cfg.RateLimit.TrustXForwardedFor,
			DefaultRPS:         cfg.RateLimit.Default.RequestsPerSecond,
			DefaultBurst:       cfg.RateLimit.Default.Burst,
			CleanupInterval:    cfg.RateLimit.CleanupInterval,
			CleanupMaxAge:      cfg.RateLimit.CleanupMaxAge,
			APIKeys:            apiKeys,
		}, m)
		mainHandler = rl.Middleware(wsHandler)
		defer rl.Close()
	}

	mainMux := http.NewServeMux()
	mainMux.Handle("/", mainHandler)
	mainMux.Handle("/health", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	}))
	mainMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	adminMux := http.NewServeMux()
	adminMux.Handle("/", adminAPI.Handler())
	adminMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	mainServer := &http.Server{Addr: cfg.Server.Listen, Handler: mainMux}
	adminServer := &http.Server{Addr: cfg.Server.AdminListen, Handler: adminMux}

	ctx, cancel := context.WithCancel(context.Background())
	go checker.Start(ctx)
	if len(discoveryChains) > 0 {
		disc := discovery.New(discoveryChains, m)
		go disc.Start(ctx)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		slog.Info("main server starting", "addr", cfg.Server.Listen)
		if err := mainServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("main server error", "error", err)
		}
	}()
	go func() {
		defer wg.Done()
		slog.Info("admin server starting", "addr", cfg.Server.AdminListen)
		if err := adminServer.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("admin server error", "error", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh
	slog.Info("shutting down...")
	if cfgWatcher != nil {
		cfgWatcher.Close()
	}
	cancel()
	wsHandler.Close()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.Server.DrainTimeout)
	defer shutdownCancel()
	mainServer.Shutdown(shutdownCtx)
	adminServer.Shutdown(shutdownCtx)
	wg.Wait()
	slog.Info("shutdown complete")
}
