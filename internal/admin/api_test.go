package admin

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

func newTestAdmin() *API {
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	scorer := pool.NewScorer(weights)
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", scorer, cbCfg)
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	node := p.GetNode("rpc1")
	node.SetBlockHeight(100)
	node.SetLatency(50 * time.Millisecond)
	chains := map[string]ChainInfo{
		"ethereum": {Pool: p, Cache: cache.New(100), Scorer: scorer},
	}
	return New(chains, time.Now(), "")
}

func TestAdminAPI_Status(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var status StatusResponse
	json.NewDecoder(w.Body).Decode(&status)
	chain, ok := status.Chains["ethereum"]
	if !ok {
		t.Fatal("missing ethereum")
	}
	if chain.TotalNodes != 1 {
		t.Errorf("TotalNodes = %d, want 1", chain.TotalNodes)
	}
}

func TestAdminAPI_ListNodes(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/chains/ethereum/nodes", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
}

func TestAdminAPI_AddNode(t *testing.T) {
	api := newTestAdmin()
	body := `{"id":"rpc2","url":"https://rpc2.example.com","weight":0.8}`
	req := httptest.NewRequest(http.MethodPost, "/admin/chains/ethereum/nodes", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201", w.Code)
	}
}

func TestAdminAPI_DeleteNode(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodDelete, "/admin/chains/ethereum/nodes/rpc1", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", w.Code)
	}
}

func TestAdminAPI_UnknownChain_Returns404(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/chains/solana/nodes", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_Dashboard(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/html") {
		t.Errorf("content-type = %q, want text/html", w.Header().Get("Content-Type"))
	}
}

func TestAdminAPI_Config(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	var result map[string]struct {
		NodeCount int `json:"node_count"`
	}
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode config response: %v", err)
	}
	if _, ok := result["ethereum"]; !ok {
		t.Error("expected ethereum entry in config response")
	}
}

func TestAdminAPI_Config_MethodNotAllowed(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPost, "/admin/config", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestAdminAPI_Health(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "ok") {
		t.Error("expected ok in body")
	}
}

func TestAdminAPI_UpdateWeight(t *testing.T) {
	api := newTestAdmin()
	body := `{"weight":2.5}`
	req := httptest.NewRequest(http.MethodPatch, "/admin/chains/ethereum/nodes/rpc1", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestAdminAPI_UpdateWeight_InvalidJSON(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPatch, "/admin/chains/ethereum/nodes/rpc1", strings.NewReader(`{bad}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAdminAPI_UpdateWeight_UnknownNode(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPatch, "/admin/chains/ethereum/nodes/ghost", strings.NewReader(`{"weight":1.0}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_ResetCircuit(t *testing.T) {
	api := newTestAdmin()
	// Trip the circuit by recording enough errors to exceed the threshold.
	node := api.chains["ethereum"].Pool.GetNode("rpc1")
	for i := 0; i < 10; i++ {
		node.CircuitBreaker().RecordError()
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/chains/ethereum/nodes/rpc1/reset", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

func TestAdminAPI_ResetCircuit_UnknownNode(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPost, "/admin/chains/ethereum/nodes/ghost/reset", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_StatusMethodNotAllowed(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPost, "/admin/status", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestAdminAPI_DeleteNonexistent(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodDelete, "/admin/chains/ethereum/nodes/ghost", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_AddNode_InvalidJSON(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPost, "/admin/chains/ethereum/nodes", strings.NewReader(`{bad json}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestAdminAPI_AddNode_Duplicate(t *testing.T) {
	api := newTestAdmin()
	body := `{"id":"rpc1","url":"https://rpc1.example.com","weight":1.0}`
	req := httptest.NewRequest(http.MethodPost, "/admin/chains/ethereum/nodes", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusConflict {
		t.Errorf("status = %d, want 409", w.Code)
	}
}

func TestAdminAPI_Chains_MethodNotAllowed_OnNodeList(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPut, "/admin/chains/ethereum/nodes", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestAdminAPI_Chains_MethodNotAllowed_OnNodeItem(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodPut, "/admin/chains/ethereum/nodes/rpc1", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestAdminAPI_Chains_MethodNotAllowed_OnReset(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/chains/ethereum/nodes/rpc1/reset", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestAdminAPI_Chains_ShortPath_Returns404(t *testing.T) {
	api := newTestAdmin()
	// Path with only one part after /admin/chains/ (no "nodes" segment).
	req := httptest.NewRequest(http.MethodGet, "/admin/chains/ethereum", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_Chains_UnknownSegment_Returns404(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/chains/ethereum/other", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestAdminAPI_Status_HasUptimeField(t *testing.T) {
	api := newTestAdmin()
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var status StatusResponse
	if err := json.NewDecoder(w.Body).Decode(&status); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if status.Uptime == "" {
		t.Error("expected non-empty uptime")
	}
}

func TestAdminAPI_AuthRequired(t *testing.T) {
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	scorer := pool.NewScorer(weights)
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", scorer, cbCfg)
	p.AddNode("rpc1", "https://rpc1.example.com", 1.0)
	chains := map[string]ChainInfo{"ethereum": {Pool: p, Cache: cache.New(100), Scorer: scorer}}
	api := New(chains, time.Now(), "secret-token")

	// Without auth -> 401
	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("no auth: status=%d, want 401", w.Code)
	}

	// With wrong token -> 401
	req2 := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	req2.Header.Set("Authorization", "Bearer wrong")
	w2 := httptest.NewRecorder()
	api.Handler().ServeHTTP(w2, req2)
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("wrong token: status=%d, want 401", w2.Code)
	}

	// With correct token -> 200
	req3 := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	req3.Header.Set("Authorization", "Bearer secret-token")
	w3 := httptest.NewRecorder()
	api.Handler().ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Errorf("correct token: status=%d, want 200", w3.Code)
	}

	// Health does not require auth
	req4 := httptest.NewRequest(http.MethodGet, "/health", nil)
	w4 := httptest.NewRecorder()
	api.Handler().ServeHTTP(w4, req4)
	if w4.Code != http.StatusOK {
		t.Errorf("health without auth: status=%d, want 200", w4.Code)
	}

	// Dashboard requires auth when token is set
	req5 := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	w5 := httptest.NewRecorder()
	api.Handler().ServeHTTP(w5, req5)
	if w5.Code != http.StatusUnauthorized {
		t.Errorf("dashboard without auth: status=%d, want 401", w5.Code)
	}

	// Dashboard with correct token → 200
	req6 := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	req6.Header.Set("Authorization", "Bearer secret-token")
	w6 := httptest.NewRecorder()
	api.Handler().ServeHTTP(w6, req6)
	if w6.Code != http.StatusOK {
		t.Errorf("dashboard with auth: status=%d, want 200", w6.Code)
	}
}
