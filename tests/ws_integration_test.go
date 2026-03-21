package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
	"github.com/spiehdid/rpc-aggregator/internal/router"
	"github.com/spiehdid/rpc-aggregator/internal/ws"
)

func setupWSIntegration(t *testing.T) (*httptest.Server, *httptest.Server) {
	t.Helper()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("node1", upstream.URL, 1.0)
	node := p.GetNode("node1")
	node.SetBlockHeight(100)
	node.SetLatency(10 * time.Millisecond)
	chains := map[string]*router.ChainRoute{
		"/eth": &router.ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second)},
	}
	rtr := router.New(chains, m, 3, 3, 100, 30*time.Second)
	cfg := ws.HandlerConfig{
		MaxConnections: 100, MaxSubscriptionsPerConn: 10, MaxTotalSubscriptions: 1000,
		MaxMessageSize: 65536, SendQueueSize: 256, WriteTimeout: 5 * time.Second,
		ConsumerReadBuffer: 1024, ConsumerWriteBuffer: 1024,
		UpstreamReadBuffer: 4096, UpstreamWriteBuffer: 4096,
		PingInterval: 30 * time.Second, PongTimeout: 10 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, ReconnectMaxBackoff: 1 * time.Second,
	}
	handler := ws.NewHandler(chains, rtr, m, cfg)
	server := httptest.NewServer(handler)
	return server, upstream
}

func TestWSIntegration_JSONRPCOverWebSocket(t *testing.T) {
	server, upstream := setupWSIntegration(t)
	defer server.Close()
	defer upstream.Close()
	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	_, msg, _ := conn.ReadMessage()
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct{}       `json:"error"`
	}
	json.Unmarshal(msg, &resp)
	if resp.Error != nil {
		t.Error("unexpected error")
	}
	if string(resp.Result) != `"0x1"` {
		t.Errorf("result=%s", resp.Result)
	}
}

func TestWSIntegration_HTTPStillWorks(t *testing.T) {
	server, upstream := setupWSIntegration(t)
	defer server.Close()
	defer upstream.Close()
	resp, err := http.Post(server.URL+"/eth", "application/json", strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status=%d", resp.StatusCode)
	}
}

func TestWSIntegration_MultipleMessages(t *testing.T) {
	server, upstream := setupWSIntegration(t)
	defer server.Close()
	defer upstream.Close()
	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()
	for i := 0; i < 3; i++ {
		conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}`))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("msg %d read error: %v", i, err)
		}
		var resp struct {
			Result json.RawMessage  `json:"result"`
			Error  *json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(msg, &resp); err != nil {
			t.Fatalf("msg %d unmarshal error: %v", i, err)
		}
		if resp.Error != nil {
			t.Errorf("msg %d: unexpected error in response: %s", i, string(*resp.Error))
		}
		if resp.Result == nil {
			t.Errorf("msg %d: expected a result in response, got nil", i)
		}
	}
}
