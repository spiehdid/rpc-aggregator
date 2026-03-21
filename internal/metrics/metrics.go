package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	RPCRequests                *prometheus.CounterVec
	RPCRequestDuration         *prometheus.HistogramVec
	UpstreamRequests           *prometheus.CounterVec
	UpstreamLatency            *prometheus.HistogramVec
	CacheHits                  *prometheus.CounterVec
	CacheMisses                *prometheus.CounterVec
	NodeBlockHeight            *prometheus.GaugeVec
	NodeScore                  *prometheus.GaugeVec
	NodeCircuitState           *prometheus.GaugeVec
	RetryTotal                 *prometheus.CounterVec
	CriticalFanout             *prometheus.CounterVec
	ChainUnavailable           *prometheus.CounterVec
	WSConnectionsActive        *prometheus.GaugeVec
	WSSubscriptionsActive      *prometheus.GaugeVec
	WSUpstreamConnections      *prometheus.GaugeVec
	WSEventsTotal              *prometheus.CounterVec
	WSReconnectsTotal          *prometheus.CounterVec
	WSReconnectsFailed         *prometheus.CounterVec
	SmartFinalizationOverrides *prometheus.CounterVec
	RateLimitAllowed           *prometheus.CounterVec
	RateLimitRejected          *prometheus.CounterVec
	DiscoveryNodesAdded        *prometheus.CounterVec
	DiscoveryNodesRemoved      *prometheus.CounterVec
	DiscoveryRuns              *prometheus.CounterVec
	PaidNodeRequests           *prometheus.CounterVec
	PaidNodeBudgetRemaining    *prometheus.GaugeVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		RPCRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rpc_requests_total", Help: "Total incoming JSON-RPC requests",
		}, []string{"chain", "method", "status"}),
		RPCRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "rpc_request_duration_seconds", Help: "Response latency to consumer", Buckets: prometheus.DefBuckets,
		}, []string{"chain", "method"}),
		UpstreamRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "upstream_requests_total", Help: "Requests to upstream nodes",
		}, []string{"chain", "node", "status"}),
		UpstreamLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "upstream_latency_seconds", Help: "Upstream node latency", Buckets: prometheus.DefBuckets,
		}, []string{"chain", "node"}),
		CacheHits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_hits_total", Help: "Cache hits",
		}, []string{"chain", "method"}),
		CacheMisses: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_misses_total", Help: "Cache misses",
		}, []string{"chain", "method"}),
		NodeBlockHeight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_block_height", Help: "Current block height per node",
		}, []string{"chain", "node"}),
		NodeScore: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_score", Help: "Current node score",
		}, []string{"chain", "node"}),
		NodeCircuitState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "node_circuit_state", Help: "Circuit breaker state: 0=closed, 1=open, 2=half-open",
		}, []string{"chain", "node"}),
		RetryTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retry_total", Help: "Number of retries",
		}, []string{"chain", "method"}),
		CriticalFanout: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "critical_fanout_total", Help: "Critical parallel sends",
		}, []string{"chain", "method"}),
		ChainUnavailable: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "chain_unavailable_total", Help: "All nodes unavailable for chain",
		}, []string{"chain"}),
		WSConnectionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ws_connections_active", Help: "Active WebSocket consumer connections",
		}, []string{"chain"}),
		WSSubscriptionsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ws_subscriptions_active", Help: "Active WebSocket subscriptions",
		}, []string{"chain", "method"}),
		WSUpstreamConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "ws_upstream_connections", Help: "Active WebSocket upstream connections",
		}, []string{"chain", "node"}),
		WSEventsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_events_total", Help: "Total WebSocket events forwarded to consumers",
		}, []string{"chain", "method"}),
		WSReconnectsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_reconnects_total", Help: "Total WebSocket upstream reconnect attempts",
		}, []string{"chain", "node"}),
		WSReconnectsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_reconnects_failed_total", Help: "Permanent upstream WS reconnect failures",
		}, []string{"node"}),
		SmartFinalizationOverrides: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "smart_finalization_overrides_total",
			Help: "Cache TTL overrides due to finalized block",
		}, []string{"chain", "method"}),
		RateLimitAllowed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rate_limit_allowed_total", Help: "Requests allowed by rate limiter",
		}, []string{"key_type"}),
		RateLimitRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "rate_limit_rejected_total", Help: "Requests rejected by rate limiter",
		}, []string{"key_type"}),
		DiscoveryNodesAdded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "discovery_nodes_added_total", Help: "Nodes added by auto-discovery",
		}, []string{"chain"}),
		DiscoveryNodesRemoved: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "discovery_nodes_removed_total", Help: "Nodes removed by auto-discovery",
		}, []string{"chain"}),
		DiscoveryRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "discovery_runs_total", Help: "Discovery run attempts",
		}, []string{"chain", "status"}),
		PaidNodeRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "paid_node_requests_total", Help: "Requests routed to paid nodes",
		}, []string{"chain", "node"}),
		PaidNodeBudgetRemaining: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "paid_node_budget_remaining", Help: "Remaining daily budget for paid nodes",
		}, []string{"chain", "node"}),
	}
	reg.MustRegister(m.RPCRequests, m.RPCRequestDuration, m.UpstreamRequests, m.UpstreamLatency,
		m.CacheHits, m.CacheMisses, m.NodeBlockHeight, m.NodeScore, m.NodeCircuitState,
		m.RetryTotal, m.CriticalFanout, m.ChainUnavailable,
		m.WSConnectionsActive, m.WSSubscriptionsActive, m.WSUpstreamConnections,
		m.WSEventsTotal, m.WSReconnectsTotal, m.WSReconnectsFailed, m.SmartFinalizationOverrides,
		m.RateLimitAllowed, m.RateLimitRejected,
		m.DiscoveryNodesAdded, m.DiscoveryNodesRemoved, m.DiscoveryRuns,
		m.PaidNodeRequests, m.PaidNodeBudgetRemaining)
	return m
}
