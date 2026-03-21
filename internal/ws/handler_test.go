package ws

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
)

func setupWSTest(t *testing.T) (*Handler, *httptest.Server, *httptest.Server) {
	t.Helper()
	upstreamHTTP := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`))
	}))
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	p.AddNode("node1", upstreamHTTP.URL, 1.0)
	node := p.GetNode("node1")
	node.SetBlockHeight(100)
	node.SetLatency(10 * time.Millisecond)

	chains := map[string]*router.ChainRoute{
		"/eth": &router.ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher([]cache.Rule{{Method: "eth_chainId", TTL: 0}}, 5*time.Second)},
	}
	rtr := router.New(chains, m, 3, 3, 100, 30*time.Second)

	cfg := HandlerConfig{
		MaxConnections: 100, MaxSubscriptionsPerConn: 10, MaxTotalSubscriptions: 1000,
		MaxMessageSize: 65536, SendQueueSize: 256, WriteTimeout: 5 * time.Second,
		ConsumerReadBuffer: 1024, ConsumerWriteBuffer: 1024,
		UpstreamReadBuffer: 4096, UpstreamWriteBuffer: 4096,
		PingInterval: 30 * time.Second, PongTimeout: 10 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, ReconnectMaxBackoff: 1 * time.Second,
	}
	handler := NewHandler(chains, rtr, m, cfg)
	server := httptest.NewServer(handler)
	return handler, server, upstreamHTTP
}

// setupWSTestWithWSUpstream creates a handler whose node points at a real WS
// server (wsUpstream). The WS upstream responds to every subscribe request with
// a synthetic subscription ID so that handleSubscribe can complete its flow.
func setupWSTestWithWSUpstream(t *testing.T) (*Handler, *httptest.Server, *httptest.Server) {
	t.Helper()

	upgrader := gws.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	wsUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if err := json.Unmarshal(raw, &req); err != nil {
				return
			}
			// Reply with a subscription ID for any subscribe-like request.
			resp := `{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":"0xaabbccdd"}`
			conn.WriteMessage(mt, []byte(resp))
		}
	}))

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	// Node URL must use the WS upstream's HTTP address; DeriveWSURL will convert it.
	p.AddNode("node1", wsUpstream.URL, 1.0)
	node := p.GetNode("node1")
	node.SetBlockHeight(100)
	node.SetLatency(10 * time.Millisecond)

	chains := map[string]*router.ChainRoute{
		"/eth": &router.ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher([]cache.Rule{{Method: "eth_chainId", TTL: 0}}, 5*time.Second)},
	}
	rtr := router.New(chains, m, 3, 3, 100, 30*time.Second)
	cfg := HandlerConfig{
		MaxConnections: 100, MaxSubscriptionsPerConn: 10, MaxTotalSubscriptions: 1000,
		MaxMessageSize: 65536, SendQueueSize: 256, WriteTimeout: 5 * time.Second,
		ConsumerReadBuffer: 1024, ConsumerWriteBuffer: 1024,
		UpstreamReadBuffer: 4096, UpstreamWriteBuffer: 4096,
		PingInterval: 30 * time.Second, PongTimeout: 10 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, ReconnectMaxBackoff: 1 * time.Second,
	}
	handler := NewHandler(chains, rtr, m, cfg)
	server := httptest.NewServer(handler)
	return handler, server, wsUpstream
}

func TestWSHandler_HTTPFallback(t *testing.T) {
	_, server, upstream := setupWSTest(t)
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

func TestWSHandler_WSConnect(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()
	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()
}

func TestWSHandler_JSONRPCOverWS(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()
	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()
	conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	var resp struct {
		Error *struct{} `json:"error"`
	}
	json.Unmarshal(msg, &resp)
	if resp.Error != nil {
		t.Error("unexpected error")
	}
}

func TestWSHandler_UnknownChain(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()
	_, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/unknown", nil)
	if err == nil {
		t.Error("expected error")
	}
}

// TestDeriveWSURL covers all three branches of DeriveWSURL.
func TestDeriveWSURL(t *testing.T) {
	tests := []struct {
		httpURL  string
		override string
		want     string
	}{
		{"https://rpc.example.com", "", "wss://rpc.example.com"},
		{"http://localhost:8080", "", "ws://localhost:8080"},
		{"https://rpc.example.com", "wss://custom.com/ws", "wss://custom.com/ws"},
		// URL that is neither http nor https is returned unchanged.
		{"wss://already.ws", "", "wss://already.ws"},
	}
	for _, tt := range tests {
		got := DeriveWSURL(tt.httpURL, tt.override)
		if got != tt.want {
			t.Errorf("DeriveWSURL(%q, %q) = %q, want %q", tt.httpURL, tt.override, got, tt.want)
		}
	}
}

// TestWSHandler_ConnectionLimit verifies that the handler rejects a second
// connection when MaxConnections is 1.
func TestWSHandler_ConnectionLimit(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	// Override the handler with MaxConnections=1.
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
		"/eth": &router.ChainRoute{Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher([]cache.Rule{{Method: "eth_chainId", TTL: 0}}, 5*time.Second)},
	}
	rtr := router.New(chains, m, 3, 3, 100, 30*time.Second)
	cfg := HandlerConfig{
		MaxConnections: 1, MaxSubscriptionsPerConn: 10, MaxTotalSubscriptions: 1000,
		MaxMessageSize: 65536, SendQueueSize: 256, WriteTimeout: 5 * time.Second,
		ConsumerReadBuffer: 1024, ConsumerWriteBuffer: 1024,
		UpstreamReadBuffer: 4096, UpstreamWriteBuffer: 4096,
		PingInterval: 30 * time.Second, PongTimeout: 10 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, ReconnectMaxBackoff: 1 * time.Second,
	}
	handler := NewHandler(chains, rtr, m, cfg)
	limitedServer := httptest.NewServer(handler)
	defer limitedServer.Close()

	wsAddr := "ws" + strings.TrimPrefix(limitedServer.URL, "http") + "/eth"

	conn1, _, err := gws.DefaultDialer.Dial(wsAddr, nil)
	if err != nil {
		t.Fatalf("first connection failed: %v", err)
	}
	defer conn1.Close()

	_, resp, err := gws.DefaultDialer.Dial(wsAddr, nil)
	if err == nil {
		t.Error("expected second connection to be rejected")
	}
	if resp != nil && resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}
}

// TestWSHandler_InvalidJSONErrorResponse verifies that sending malformed JSON
// over a WebSocket connection returns a JSON-RPC parse-error response.
func TestWSHandler_InvalidJSONErrorResponse(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	conn.WriteMessage(gws.TextMessage, []byte(`not valid json{{{`))
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var resp struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error response")
	}
	if resp.Error.Code != -32700 {
		t.Errorf("error code = %d, want -32700", resp.Error.Code)
	}
}

// TestWSHandler_MultipleMessagesInSequence verifies that multiple JSON-RPC
// requests sent in sequence each receive a response.
func TestWSHandler_MultipleMessagesInSequence(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 1; i <= 3; i++ {
		req := strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":` + strings.Repeat("0", 0) + `}`)
		_ = req
		conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("message %d read error: %v", i, err)
		}
		if len(msg) == 0 {
			t.Errorf("message %d: empty response", i)
		}
	}
}

// TestWSHandler_ConsumerDisconnectCleansUp verifies that after a consumer
// disconnects the connection count returns to zero.
func TestWSHandler_ConsumerDisconnectCleansUp(t *testing.T) {
	handler, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}

	// Wait until the handler registers the connection.
	time.Sleep(50 * time.Millisecond)
	if handler.connCount.Load() != 1 {
		t.Errorf("connCount = %d before disconnect, want 1", handler.connCount.Load())
	}

	conn.Close()

	// Give the handler time to process the disconnect.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if handler.connCount.Load() == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if handler.connCount.Load() != 0 {
		t.Errorf("connCount = %d after disconnect, want 0", handler.connCount.Load())
	}
}

// TestWSHandler_Close verifies that Close does not panic even with an active
// consumer connection.
func TestWSHandler_Close(t *testing.T) {
	handler, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	// Should not panic.
	done := make(chan struct{})
	go func() {
		defer close(done)
		handler.Close()
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("Close() did not return within timeout")
	}
}

// TestWSHandler_UnsubscribeMissingParams verifies that eth_Unsubscribe with no
// params returns a -32602 error (the handler uses HasSuffix "Unsubscribe").
func TestWSHandler_UnsubscribeMissingParams(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	// Method must end with capital "Unsubscribe" to reach handleUnsubscribe.
	conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_Unsubscribe","params":[],"id":1}`))
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var resp struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error response")
	}
	if resp.Error.Code != -32602 {
		t.Errorf("error code = %d, want -32602", resp.Error.Code)
	}
}

// TestWSHandler_UnsubscribeSuccess verifies that a well-formed eth_Unsubscribe
// returns a true result.
func TestWSHandler_UnsubscribeSuccess(t *testing.T) {
	_, server, upstream := setupWSTest(t)
	defer server.Close()
	defer upstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	// Method must end with capital "Unsubscribe" to reach handleUnsubscribe.
	conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_Unsubscribe","params":["0xdeadbeef"],"id":2}`))
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	// handleUnsubscribe returns json.RawMessage(`true`) as the result.
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if string(resp.Result) != "true" {
		t.Errorf("expected result=true, got: %s", msg)
	}
}

// TestWSHandler_Subscribe verifies the handleSubscribe path: the handler
// connects to the upstream WS, sends a subscribe request, and returns the
// consumer subscription ID to the caller.
func TestWSHandler_Subscribe(t *testing.T) {
	handler, server, wsUpstream := setupWSTestWithWSUpstream(t)
	defer handler.Close()
	defer server.Close()
	defer wsUpstream.Close()

	conn, _, err := gws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/eth", nil)
	if err != nil {
		t.Fatalf("dial error: %v", err)
	}
	defer conn.Close()

	// "eth_Subscribe" ends with capital "Subscribe" → routes to handleSubscribe.
	conn.WriteMessage(gws.TextMessage, []byte(`{"jsonrpc":"2.0","method":"eth_Subscribe","params":["newHeads"],"id":1}`))
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read error: %v", err)
	}

	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(msg, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error: %d %s", resp.Error.Code, resp.Error.Message)
	}
	var subID string
	if err := json.Unmarshal(resp.Result, &subID); err != nil || subID == "" {
		t.Errorf("expected non-empty subscription ID, got result=%s", resp.Result)
	}
}

func TestMakeOriginChecker(t *testing.T) {
	// Empty = allow all
	checker := makeOriginChecker(nil)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Origin", "https://evil.com")
	if !checker(req) {
		t.Error("nil should allow all")
	}

	// Wildcard = allow all
	checker = makeOriginChecker([]string{"*"})
	if !checker(req) {
		t.Error("wildcard should allow all")
	}

	// Allowlist
	checker = makeOriginChecker([]string{"https://mydapp.com", "https://admin.local"})
	req.Header.Set("Origin", "https://mydapp.com")
	if !checker(req) {
		t.Error("allowed origin should pass")
	}
	req.Header.Set("Origin", "https://evil.com")
	if checker(req) {
		t.Error("disallowed origin should fail")
	}

	// No Origin header = allow (non-browser)
	req.Header.Del("Origin")
	if !checker(req) {
		t.Error("no origin header should pass")
	}
}

// TestWSHandler_SubscribeShared verifies that a second consumer subscribing to
// the same method/params receives a different subscription ID but shares the
// upstream subscription (SubscribeShared path).
func TestWSHandler_SubscribeShared(t *testing.T) {
	handler, server, wsUpstream := setupWSTestWithWSUpstream(t)
	defer handler.Close()
	defer server.Close()
	defer wsUpstream.Close()

	wsAddr := "ws" + strings.TrimPrefix(server.URL, "http") + "/eth"
	subscribeMsg := []byte(`{"jsonrpc":"2.0","method":"eth_Subscribe","params":["newHeads"],"id":1}`)

	// First consumer subscribes — creates the upstream subscription.
	conn1, _, err := gws.DefaultDialer.Dial(wsAddr, nil)
	if err != nil {
		t.Fatalf("conn1 dial error: %v", err)
	}
	defer conn1.Close()
	conn1.WriteMessage(gws.TextMessage, subscribeMsg)
	conn1.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg1, err := conn1.ReadMessage()
	if err != nil {
		t.Fatalf("conn1 read error: %v", err)
	}

	// Second consumer subscribes — should hit SubscribeShared.
	conn2, _, err := gws.DefaultDialer.Dial(wsAddr, nil)
	if err != nil {
		t.Fatalf("conn2 dial error: %v", err)
	}
	defer conn2.Close()
	conn2.WriteMessage(gws.TextMessage, subscribeMsg)
	conn2.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, msg2, err := conn2.ReadMessage()
	if err != nil {
		t.Fatalf("conn2 read error: %v", err)
	}

	var r1, r2 struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	json.Unmarshal(msg1, &r1)
	json.Unmarshal(msg2, &r2)

	var sub1, sub2 string
	json.Unmarshal(r1.Result, &sub1)
	json.Unmarshal(r2.Result, &sub2)

	if sub1 == "" || sub2 == "" {
		t.Fatalf("expected non-empty subscription IDs, got %q and %q", sub1, sub2)
	}
	if sub1 == sub2 {
		t.Error("expected different consumer subscription IDs for different connections")
	}
}
