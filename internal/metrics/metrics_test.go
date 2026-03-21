package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetrics_AllCollectorsRegistered(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	if m.RPCRequests == nil {
		t.Error("RPCRequests is nil")
	}
	if m.RPCRequestDuration == nil {
		t.Error("RPCRequestDuration is nil")
	}
	if m.UpstreamRequests == nil {
		t.Error("UpstreamRequests is nil")
	}
	if m.UpstreamLatency == nil {
		t.Error("UpstreamLatency is nil")
	}
	if m.CacheHits == nil {
		t.Error("CacheHits is nil")
	}
	if m.CacheMisses == nil {
		t.Error("CacheMisses is nil")
	}
	if m.NodeBlockHeight == nil {
		t.Error("NodeBlockHeight is nil")
	}
	if m.NodeScore == nil {
		t.Error("NodeScore is nil")
	}
	if m.NodeCircuitState == nil {
		t.Error("NodeCircuitState is nil")
	}
	if m.RetryTotal == nil {
		t.Error("RetryTotal is nil")
	}
	if m.CriticalFanout == nil {
		t.Error("CriticalFanout is nil")
	}
	if m.ChainUnavailable == nil {
		t.Error("ChainUnavailable is nil")
	}
	if m.WSConnectionsActive == nil {
		t.Error("WSConnectionsActive is nil")
	}
	if m.WSSubscriptionsActive == nil {
		t.Error("WSSubscriptionsActive is nil")
	}
	if m.WSUpstreamConnections == nil {
		t.Error("WSUpstreamConnections is nil")
	}
	if m.WSEventsTotal == nil {
		t.Error("WSEventsTotal is nil")
	}
	if m.WSReconnectsTotal == nil {
		t.Error("WSReconnectsTotal is nil")
	}
	if m.WSReconnectsFailed == nil {
		t.Error("WSReconnectsFailed is nil")
	}
	if m.SmartFinalizationOverrides == nil {
		t.Error("SmartFinalizationOverrides is nil")
	}
	if m.RateLimitAllowed == nil {
		t.Error("RateLimitAllowed is nil")
	}
	if m.RateLimitRejected == nil {
		t.Error("RateLimitRejected is nil")
	}
	if m.DiscoveryNodesAdded == nil {
		t.Error("DiscoveryNodesAdded is nil")
	}
	if m.DiscoveryNodesRemoved == nil {
		t.Error("DiscoveryNodesRemoved is nil")
	}
	if m.DiscoveryRuns == nil {
		t.Error("DiscoveryRuns is nil")
	}
	if m.PaidNodeRequests == nil {
		t.Error("PaidNodeRequests is nil")
	}
	if m.PaidNodeBudgetRemaining == nil {
		t.Error("PaidNodeBudgetRemaining is nil")
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}
	_ = families
}

func TestMetrics_IncrementCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.RPCRequests.WithLabelValues("ethereum", "eth_blockNumber", "success").Inc()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}
	found := false
	for _, f := range families {
		if f.GetName() == "rpc_requests_total" {
			found = true
			metrics := f.GetMetric()
			if len(metrics) != 1 {
				t.Fatalf("expected 1 metric sample, got %d", len(metrics))
			}
			val := metrics[0].GetCounter().GetValue()
			if val != 1.0 {
				t.Errorf("counter value = %f, want 1.0", val)
			}
		}
	}
	if !found {
		t.Error("rpc_requests_total not found")
	}
}

func TestMetrics_MetricNamesAndLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	// Trigger a few metrics so they appear in Gather
	m.RPCRequests.WithLabelValues("ethereum", "eth_blockNumber", "success").Inc()
	m.UpstreamRequests.WithLabelValues("ethereum", "node1", "success").Inc()
	m.CacheHits.WithLabelValues("ethereum", "eth_blockNumber").Inc()
	m.NodeBlockHeight.WithLabelValues("ethereum", "node1").Set(100)
	m.WSConnectionsActive.WithLabelValues("ethereum").Set(1)
	m.RateLimitAllowed.WithLabelValues("ip").Inc()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}

	// Build a map of metric name -> label names for easy lookup
	type metricInfo struct {
		found      bool
		labelNames []string
	}
	familyMap := make(map[string]*metricInfo)
	for _, f := range families {
		info := &metricInfo{found: true}
		if len(f.GetMetric()) > 0 {
			for _, lp := range f.GetMetric()[0].GetLabel() {
				info.labelNames = append(info.labelNames, lp.GetName())
			}
		}
		familyMap[f.GetName()] = info
	}

	// Verify specific metric names exist with correct label names
	tests := []struct {
		name   string
		labels []string
	}{
		{"rpc_requests_total", []string{"chain", "method", "status"}},
		{"upstream_requests_total", []string{"chain", "node", "status"}},
		{"cache_hits_total", []string{"chain", "method"}},
		{"node_block_height", []string{"chain", "node"}},
		{"ws_connections_active", []string{"chain"}},
		{"rate_limit_allowed_total", []string{"key_type"}},
	}

	for _, tc := range tests {
		info, ok := familyMap[tc.name]
		if !ok || !info.found {
			t.Errorf("metric %q not found in gathered families", tc.name)
			continue
		}
		if len(info.labelNames) != len(tc.labels) {
			t.Errorf("metric %q: got %d labels, want %d", tc.name, len(info.labelNames), len(tc.labels))
			continue
		}
		for i, want := range tc.labels {
			if info.labelNames[i] != want {
				t.Errorf("metric %q label[%d] = %q, want %q", tc.name, i, info.labelNames[i], want)
			}
		}
	}
}
