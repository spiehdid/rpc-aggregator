package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

const (
	maxRequestBodySize      = 1 << 20 // 1MB
	maxUpstreamResponseSize = 5 << 20 // 5MB
	defaultBatchConcurrency = 10
)

type ChainRoute struct {
	Name        string
	Adapter     adapter.ChainAdapter
	Pool        *pool.NodePool
	Cache       *cache.Cache
	RuleMatcher *cache.RuleMatcher
	sfGroup     singleflight.Group // dedup concurrent cache misses
}

type Router struct {
	chains           map[string]*ChainRoute
	metrics          *metrics.Metrics
	budget           *pool.BudgetTracker
	maxRetries       atomic.Int32
	criticalFanOut   atomic.Int32
	maxBatchSize     atomic.Int32
	maxRequestSize   int64
	requestTimeout   time.Duration
	client           *http.Client
	batchConcurrency int
}

func New(chains map[string]*ChainRoute, m *metrics.Metrics, maxRetries, criticalFanOut, maxBatchSize int, requestTimeout time.Duration) *Router {
	rt := &Router{
		chains: chains, metrics: m,
		maxRequestSize: maxRequestBodySize, requestTimeout: requestTimeout,
		client: &http.Client{
			Timeout: requestTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		batchConcurrency: defaultBatchConcurrency,
	}
	rt.maxRetries.Store(int32(maxRetries))
	rt.criticalFanOut.Store(int32(criticalFanOut))
	rt.maxBatchSize.Store(int32(maxBatchSize))
	return rt
}

func (rt *Router) SetBudget(b *pool.BudgetTracker) {
	rt.budget = b
}

// SetRetryConfig atomically updates hot-reloadable retry settings.
func (rt *Router) SetRetryConfig(maxRetries, criticalFanOut, maxBatchSize int) {
	rt.maxRetries.Store(int32(maxRetries))
	rt.criticalFanOut.Store(int32(criticalFanOut))
	rt.maxBatchSize.Store(int32(maxBatchSize))
	slog.Info("router config updated", "maxRetries", maxRetries, "criticalFanOut", criticalFanOut, "maxBatchSize", maxBatchSize)
}

func (rt *Router) ProcessRequest(ctx context.Context, chainPath string, rawReq []byte) []byte {
	chain, ok := rt.chains[chainPath]
	if !ok {
		return makeRPCError(nil, -32600, "unknown chain")
	}
	return rt.processSingleRequest(ctx, chain, rawReq)
}

func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	path := r.URL.Path
	chain, ok := rt.chains[path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, rt.maxRequestSize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeRPCError(w, nil, -32700, "request body too large")
		return
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		rt.handleBatch(w, r, chain, trimmed, start)
		return
	}
	rt.handleSingle(w, r, chain, trimmed, start)
}

func (rt *Router) handleBatch(w http.ResponseWriter, r *http.Request, chain *ChainRoute, body []byte, start time.Time) {
	var rawRequests []json.RawMessage
	if err := json.Unmarshal(body, &rawRequests); err != nil {
		writeRPCError(w, nil, -32700, "invalid batch request")
		return
	}
	maxBatch := int(rt.maxBatchSize.Load())
	if len(rawRequests) > maxBatch {
		writeRPCError(w, nil, -32600, fmt.Sprintf("batch size %d exceeds maximum %d", len(rawRequests), maxBatch))
		return
	}
	responses := make([]json.RawMessage, len(rawRequests))
	var wg sync.WaitGroup
	sem := make(chan struct{}, rt.batchConcurrency)
batchLoop:
	for i, raw := range rawRequests {
		select {
		case sem <- struct{}{}: // acquire
		case <-r.Context().Done():
			// Client disconnected; fill remaining responses with error and stop spawning
			for j := i; j < len(rawRequests); j++ {
				responses[j] = makeRPCError(nil, -32603, "request cancelled")
			}
			break batchLoop
		}
		wg.Add(1)
		go func(idx int, rawReq json.RawMessage) {
			defer func() { <-sem }() // release
			defer wg.Done()
			responses[idx] = rt.processSingleRequest(r.Context(), chain, rawReq)
		}(i, raw)
	}
	wg.Wait()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(responses)
	rt.metrics.RPCRequestDuration.WithLabelValues(chain.Name, "batch").Observe(time.Since(start).Seconds())
	rt.metrics.RPCRequests.WithLabelValues(chain.Name, "batch", "success").Inc()
}

func (rt *Router) handleSingle(w http.ResponseWriter, r *http.Request, chain *ChainRoute, body []byte, start time.Time) {
	// Parse once upfront for the method label; processSingleRequest will parse again internally.
	req, parseErr := chain.Adapter.ParseRequest(body)
	method := "unknown"
	if parseErr == nil {
		method = req.Method
	}

	respBytes := rt.processSingleRequest(r.Context(), chain, body)
	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)

	duration := time.Since(start).Seconds()
	rt.metrics.RPCRequestDuration.WithLabelValues(chain.Name, method).Observe(duration)

	status := "success"
	var resp adapter.RPCResponse
	if err := json.Unmarshal(respBytes, &resp); err == nil && resp.Error != nil {
		status = "error"
	}
	rt.metrics.RPCRequests.WithLabelValues(chain.Name, method, status).Inc()
}

func (rt *Router) processSingleRequest(ctx context.Context, chain *ChainRoute, rawReq json.RawMessage) []byte {
	req, err := chain.Adapter.ParseRequest(rawReq)
	if err != nil {
		return makeRPCError(nil, -32700, "parse error")
	}
	mc := chain.Adapter.ClassifyMethod(req.Method)

	if mc.Cacheable && mc.CacheKeyFunc != nil {
		cacheKey := mc.CacheKeyFunc(req)
		if cached, ok := chain.Cache.Get(cacheKey); ok {
			rt.metrics.CacheHits.WithLabelValues(chain.Name, req.Method).Inc()
			return rewriteResponseID(cached, req.ID)
		}
		rt.metrics.CacheMisses.WithLabelValues(chain.Name, req.Method).Inc()

		result, _, _ := chain.sfGroup.Do(cacheKey, func() (interface{}, error) {
			// Double-check cache after acquiring singleflight slot
			if cached, ok := chain.Cache.Get(cacheKey); ok {
				return cached, nil
			}

			upstreamBody, err := chain.Adapter.BuildUpstreamRequest(req)
			if err != nil {
				return makeRPCError(req.ID, -32603, "internal error"), nil
			}
			var respBytes []byte
			if mc.Critical {
				respBytes = rt.fanOut(ctx, chain, req, upstreamBody)
			} else if mc.Mutating {
				respBytes = rt.sendToNode(ctx, chain, req, upstreamBody, nil)
			} else {
				respBytes = rt.sendWithRetry(ctx, chain, req, upstreamBody)
			}

			if respBytes != nil {
				resp, err := chain.Adapter.ParseResponse(respBytes)
				if err == nil && resp.Error == nil {
					condition := extractCondition(req)
					ttl, matched := chain.RuleMatcher.Match(req.Method, condition)
					if !matched {
						ttl = chain.RuleMatcher.DefaultTTL()
					}
					if blockNum, ok := chain.Adapter.ExtractBlockNumber(req); ok {
						fh := chain.Pool.FinalizedHeight()
						if fh > 0 && blockNum <= fh {
							ttl = 24 * time.Hour
							rt.metrics.SmartFinalizationOverrides.WithLabelValues(chain.Name, req.Method).Inc()
						}
					}
					chain.Cache.Set(cacheKey, respBytes, ttl)
				}
			}
			return respBytes, nil
		})
		if result == nil {
			return makeRPCError(req.ID, -32603, "internal error")
		}
		return rewriteResponseID(result.([]byte), req.ID)
	}

	upstreamBody, err := chain.Adapter.BuildUpstreamRequest(req)
	if err != nil {
		return makeRPCError(req.ID, -32603, "internal error")
	}
	var respBytes []byte
	if mc.Critical {
		respBytes = rt.fanOut(ctx, chain, req, upstreamBody)
	} else if mc.Mutating {
		respBytes = rt.sendToNode(ctx, chain, req, upstreamBody, nil)
	} else {
		respBytes = rt.sendWithRetry(ctx, chain, req, upstreamBody)
	}
	return respBytes
}

func (rt *Router) sendWithRetry(ctx context.Context, chain *ChainRoute, req adapter.RPCRequest, body []byte) []byte {
	maxRetries := int(rt.maxRetries.Load())
	excluded := make(map[string]bool)
	for attempt := 0; attempt < maxRetries; attempt++ {
		node := chain.Pool.SelectBestExcluding(excluded)
		if node == nil {
			rt.metrics.ChainUnavailable.WithLabelValues(chain.Name).Inc()
			return makeRPCError(req.ID, -32603, "no healthy upstream nodes")
		}
		if node.Tier == "paid" && rt.budget != nil && !rt.budget.TryRecord(node.ID) {
			excluded[node.ID] = true
			continue // pick next node
		}
		respBytes, err := rt.doUpstreamRequest(ctx, chain.Name, node, body)
		if err != nil {
			slog.Debug("upstream failed", "chain", chain.Name, "node", node.ID, "error", err, "attempt", attempt+1)
			excluded[node.ID] = true
			node.CircuitBreaker().RecordError()
			if attempt < maxRetries-1 {
				rt.metrics.RetryTotal.WithLabelValues(chain.Name, req.Method).Inc()
			}
			continue
		}
		node.CircuitBreaker().RecordSuccess()
		return respBytes
	}
	rt.metrics.ChainUnavailable.WithLabelValues(chain.Name).Inc()
	return makeRPCError(req.ID, -32603, "all upstream nodes failed")
}

func (rt *Router) sendToNode(ctx context.Context, chain *ChainRoute, req adapter.RPCRequest, body []byte, excluded map[string]bool) []byte {
	if excluded == nil {
		excluded = make(map[string]bool)
	}
	maxRetries := int(rt.maxRetries.Load())
	for attempts := 0; attempts < maxRetries; attempts++ {
		node := chain.Pool.SelectBestExcluding(excluded)
		if node == nil {
			rt.metrics.ChainUnavailable.WithLabelValues(chain.Name).Inc()
			return makeRPCError(req.ID, -32603, "no healthy upstream nodes")
		}
		if node.Tier == "paid" && rt.budget != nil && !rt.budget.TryRecord(node.ID) {
			excluded[node.ID] = true
			continue
		}
		respBytes, err := rt.doUpstreamRequest(ctx, chain.Name, node, body)
		if err != nil {
			node.CircuitBreaker().RecordError()
			return makeRPCError(req.ID, -32603, "upstream request failed")
		}
		node.CircuitBreaker().RecordSuccess()
		return respBytes
	}
	rt.metrics.ChainUnavailable.WithLabelValues(chain.Name).Inc()
	return makeRPCError(req.ID, -32603, "no available upstream nodes")
}

func (rt *Router) fanOut(ctx context.Context, chain *ChainRoute, req adapter.RPCRequest, body []byte) []byte {
	rt.metrics.CriticalFanout.WithLabelValues(chain.Name, req.Method).Inc()
	nodes := chain.Pool.SelectTopN(int(rt.criticalFanOut.Load()))
	if len(nodes) == 0 {
		rt.metrics.ChainUnavailable.WithLabelValues(chain.Name).Inc()
		return makeRPCError(req.ID, -32603, "no healthy upstream nodes")
	}
	// Exclude budget-exhausted paid nodes without recording budget yet;
	// budget is recorded only for the node whose response is actually used.
	var eligible []*pool.Node
	for _, node := range nodes {
		if node.Tier == "paid" && rt.budget != nil && rt.budget.IsExhausted(node.ID) {
			continue
		}
		eligible = append(eligible, node)
	}
	if len(eligible) == 0 {
		rt.metrics.ChainUnavailable.WithLabelValues(chain.Name).Inc()
		return makeRPCError(req.ID, -32603, "no healthy upstream nodes")
	}
	nodes = eligible
	type fanOutResult struct {
		data   []byte
		err    error
		nodeID string
	}
	results := make(chan fanOutResult, len(nodes))
	fanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, node := range nodes {
		go func(n *pool.Node) {
			respBytes, err := rt.doUpstreamRequest(fanCtx, chain.Name, n, body)
			if err != nil {
				// Only record error if it was NOT due to context cancellation
				// (i.e., another goroutine already succeeded and cancelled fanCtx).
				if fanCtx.Err() == nil {
					n.CircuitBreaker().RecordError()
				}
				results <- fanOutResult{err: err, nodeID: n.ID}
				return
			}
			n.CircuitBreaker().RecordSuccess()
			results <- fanOutResult{data: respBytes, nodeID: n.ID}
		}(node)
	}
	for range nodes {
		r := <-results
		if r.err == nil {
			cancel()
			// Record budget only for the node whose response we actually use.
			if rt.budget != nil {
				rt.budget.TryRecord(r.nodeID)
			}
			return r.data
		}
	}
	return makeRPCError(req.ID, -32603, "all upstream nodes failed")
}

func (rt *Router) doUpstreamRequest(ctx context.Context, chainName string, node *pool.Node, body []byte) ([]byte, error) {
	start := time.Now()
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, node.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := rt.client.Do(httpReq)
	if err != nil {
		rt.metrics.UpstreamRequests.WithLabelValues(chainName, node.ID, "error").Inc()
		return nil, err
	}
	defer resp.Body.Close()
	rt.metrics.UpstreamLatency.WithLabelValues(chainName, node.ID).Observe(time.Since(start).Seconds())
	if resp.StatusCode >= 500 || resp.StatusCode == 429 {
		rt.metrics.UpstreamRequests.WithLabelValues(chainName, node.ID, "error").Inc()
		return nil, fmt.Errorf("upstream returned %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponseSize))
	if err != nil {
		rt.metrics.UpstreamRequests.WithLabelValues(chainName, node.ID, "error").Inc()
		return nil, err
	}
	rt.metrics.UpstreamRequests.WithLabelValues(chainName, node.ID, "success").Inc()
	return respBody, nil
}

func extractCondition(req adapter.RPCRequest) string {
	if req.Params == nil {
		return ""
	}
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return ""
	}
	var firstParam string
	if err := json.Unmarshal(params[0], &firstParam); err != nil {
		return ""
	}
	switch firstParam {
	case "latest", "earliest", "pending", "finalized", "safe":
		return firstParam
	}
	return ""
}

func writeRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(makeRPCError(id, code, message))
}

func rewriteResponseID(respBytes []byte, requestID json.RawMessage) []byte {
	if requestID == nil {
		return respBytes
	}
	var resp struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result,omitempty"`
		Error   json.RawMessage `json:"error,omitempty"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return respBytes
	}
	resp.ID = requestID
	rewritten, err := json.Marshal(resp)
	if err != nil {
		return respBytes
	}
	return rewritten
}

func makeRPCError(id json.RawMessage, code int, message string) []byte {
	if id == nil {
		id = json.RawMessage(`null`)
	}
	resp := adapter.RPCResponse{JSONRPC: "2.0", ID: id, Error: &adapter.RPCError{Code: code, Message: message}}
	data, _ := json.Marshal(resp)
	return data
}
