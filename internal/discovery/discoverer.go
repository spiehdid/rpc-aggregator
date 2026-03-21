package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

const chainlistURL = "https://chainid.network/chains.json"
const maxResponseSize = 10 * 1024 * 1024 // 10MB

type ChainDiscovery struct {
	ChainName string
	ChainID   int
	Pool      *pool.NodePool
	Adapter   adapter.ChainAdapter
	Interval  time.Duration
}

type Discoverer struct {
	chains       []ChainDiscovery
	metrics      *metrics.Metrics
	client       *http.Client
	lastResult   []byte
	chainlistURL string
}

type chainEntry struct {
	ChainID int      `json:"chainId"`
	Name    string   `json:"name"`
	RPC     []string `json:"rpc"`
}

func New(chains []ChainDiscovery, m *metrics.Metrics) *Discoverer {
	return &Discoverer{
		chains:       chains,
		metrics:      m,
		client:       &http.Client{Timeout: 30 * time.Second},
		chainlistURL: chainlistURL,
	}
}

func (d *Discoverer) Start(ctx context.Context) {
	if len(d.chains) == 0 {
		return
	}

	d.runAll(ctx)

	minInterval := d.chains[0].Interval
	for _, c := range d.chains[1:] {
		if c.Interval < minInterval {
			minInterval = c.Interval
		}
	}

	ticker := time.NewTicker(minInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.runAll(ctx)
		}
	}
}

func (d *Discoverer) runAll(ctx context.Context) {
	entries, err := d.fetchChainlist(ctx)
	if err != nil {
		slog.Warn("chainlist fetch failed", "error", err)
		if d.lastResult != nil {
			if err := json.Unmarshal(d.lastResult, &entries); err != nil {
				slog.Warn("failed to parse cached chainlist", "error", err)
			}
		}
		if entries == nil {
			for _, c := range d.chains {
				d.metrics.DiscoveryRuns.WithLabelValues(c.ChainName, "error").Inc()
			}
			return
		}
	}

	for _, chain := range d.chains {
		d.discoverChain(ctx, chain, entries)
	}
}

func (d *Discoverer) fetchChainlist(ctx context.Context) ([]chainEntry, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.chainlistURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("chainlist returned HTTP %d", resp.StatusCode)
	}

	limited := io.LimitReader(resp.Body, maxResponseSize)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}

	var entries []chainEntry
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil, fmt.Errorf("parse chainlist: %w", err)
	}

	d.lastResult = body
	return entries, nil
}

func (d *Discoverer) discoverChain(ctx context.Context, chain ChainDiscovery, entries []chainEntry) {
	var rpcURLs []string
	for _, entry := range entries {
		if entry.ChainID == chain.ChainID {
			rpcURLs = filterURLs(entry.RPC)
			break
		}
	}

	keepIDs := make(map[string]bool)

	if len(rpcURLs) == 0 {
		slog.Debug("no RPC URLs found for chain", "chain", chain.ChainName, "chainID", chain.ChainID)
		removed := chain.Pool.RemoveDiscoveredNodes(keepIDs)
		if removed > 0 {
			d.metrics.DiscoveryNodesRemoved.WithLabelValues(chain.ChainName).Add(float64(removed))
			slog.Info("removed stale discovered nodes", "chain", chain.ChainName, "count", removed)
		}
		d.metrics.DiscoveryRuns.WithLabelValues(chain.ChainName, "success").Inc()
		return
	}
	added := 0
	for _, url := range rpcURLs {
		nodeID := url
		keepIDs[nodeID] = true

		if chain.Pool.GetNode(nodeID) != nil {
			continue
		}

		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := chain.Adapter.GetBlockHeight(checkCtx, url)
		cancel()

		if err != nil {
			slog.Debug("discovered node health check failed", "chain", chain.ChainName, "url", url, "error", err)
			continue
		}

		if err := chain.Pool.AddDiscoveredNode(nodeID, url, 1.0); err != nil {
			continue // duplicate
		}
		added++
		slog.Info("discovered new node", "chain", chain.ChainName, "url", url)
	}

	removed := chain.Pool.RemoveDiscoveredNodes(keepIDs)

	if added > 0 {
		d.metrics.DiscoveryNodesAdded.WithLabelValues(chain.ChainName).Add(float64(added))
	}
	if removed > 0 {
		d.metrics.DiscoveryNodesRemoved.WithLabelValues(chain.ChainName).Add(float64(removed))
		slog.Info("removed stale discovered nodes", "chain", chain.ChainName, "count", removed)
	}
	d.metrics.DiscoveryRuns.WithLabelValues(chain.ChainName, "success").Inc()
}

func filterURLs(urls []string) []string {
	var filtered []string
	for _, u := range urls {
		if !strings.HasPrefix(u, "https://") {
			continue
		}
		if strings.Contains(u, "${") {
			continue
		}
		filtered = append(filtered, u)
	}
	return filtered
}
