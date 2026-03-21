package health

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

const maxConcurrentHealthChecks = 10

type ChainEntry struct {
	Pool         *pool.NodePool
	Adapter      adapter.ChainAdapter
	Finalization bool
}

type Checker struct {
	chains    map[string]ChainEntry
	interval  time.Duration
	timeout   time.Duration
	lastError sync.Map // nodeID → last error string, for log dedup
}

func New(chains map[string]ChainEntry, interval, timeout time.Duration) *Checker {
	return &Checker{chains: chains, interval: interval, timeout: timeout}
}

func (c *Checker) Start(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	c.checkAll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkAll(ctx)
		}
	}
}

func (c *Checker) checkAll(ctx context.Context) {
	sem := make(chan struct{}, maxConcurrentHealthChecks)
	var wg sync.WaitGroup
	for chainName, entry := range c.chains {
		nodes := entry.Pool.Nodes()
		for _, node := range nodes {
			wg.Add(1)
			sem <- struct{}{} // acquire
			go func(cn string, e ChainEntry, n *pool.Node) {
				defer wg.Done()
				defer func() { <-sem }() // release
				c.checkNode(ctx, cn, e, n)
			}(chainName, entry, node)
		}
	}
	wg.Wait()
}

func (c *Checker) checkNode(ctx context.Context, chainName string, entry ChainEntry, node *pool.Node) {
	checkCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	start := time.Now()
	height, err := entry.Adapter.GetBlockHeight(checkCtx, node.URL)
	latency := time.Since(start)
	if err != nil {
		errStr := err.Error()
		prev, loaded := c.lastError.LoadOrStore(node.ID, errStr)
		if !loaded || prev.(string) != errStr {
			c.lastError.Store(node.ID, errStr)
			slog.Warn("health check failed", "chain", chainName, "node", node.ID, "error", err)
		} else {
			slog.Debug("health check failed (repeat)", "chain", chainName, "node", node.ID, "error", err)
		}
		node.CircuitBreaker().RecordError()
		return
	}
	c.lastError.Delete(node.ID)
	node.SetBlockHeight(height)
	node.SetLatency(latency)
	node.CircuitBreaker().RecordSuccess()

	// Poll finalized height (non-blocking, doesn't affect circuit breaker)
	if entry.Finalization {
		go func() {
			fCtx, fCancel := context.WithTimeout(ctx, c.timeout)
			defer fCancel()
			fh, err := entry.Adapter.GetFinalizedHeight(fCtx, node.URL)
			if err != nil {
				slog.Debug("finalized height unavailable", "chain", chainName, "node", node.ID, "error", err)
				return
			}
			node.SetFinalizedHeight(fh)
		}()
	}
}
