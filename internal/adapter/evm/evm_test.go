package evm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
)

func TestEVMAdapter_Name(t *testing.T) {
	a := New()
	if a.Name() != "evm" {
		t.Errorf("Name() = %q, want %q", a.Name(), "evm")
	}
}

func TestEVMAdapter_ParseRequest(t *testing.T) {
	a := New()
	raw := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	req, err := a.ParseRequest(raw)
	if err != nil {
		t.Fatalf("ParseRequest() error: %v", err)
	}
	if req.Method != "eth_blockNumber" {
		t.Errorf("Method = %q, want %q", req.Method, "eth_blockNumber")
	}
}

func TestEVMAdapter_ParseRequest_Invalid(t *testing.T) {
	a := New()
	_, err := a.ParseRequest([]byte(`{invalid`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEVMAdapter_BuildUpstreamRequest(t *testing.T) {
	a := New()
	req := adapter.RPCRequest{ID: json.RawMessage(`1`), Method: "eth_blockNumber", Params: json.RawMessage(`[]`)}
	data, err := a.BuildUpstreamRequest(req)
	if err != nil {
		t.Fatalf("BuildUpstreamRequest() error: %v", err)
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if parsed["jsonrpc"] != "2.0" {
		t.Errorf("jsonrpc = %v, want %q", parsed["jsonrpc"], "2.0")
	}
}

func TestEVMAdapter_ClassifyMethod(t *testing.T) {
	a := New()
	tests := []struct {
		method   string
		cache    bool
		critical bool
		mutating bool
	}{
		{"eth_blockNumber", true, false, false},
		{"eth_chainId", true, false, false},
		{"eth_getBlockByNumber", true, false, false},
		{"eth_sendRawTransaction", false, true, false},
		{"eth_call", true, false, false},
		{"eth_getBalance", true, false, false},
		{"unknown_method", false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			mc := a.ClassifyMethod(tt.method)
			if mc.Cacheable != tt.cache {
				t.Errorf("Cacheable = %v, want %v", mc.Cacheable, tt.cache)
			}
			if mc.Critical != tt.critical {
				t.Errorf("Critical = %v, want %v", mc.Critical, tt.critical)
			}
			if mc.Mutating != tt.mutating {
				t.Errorf("Mutating = %v, want %v", mc.Mutating, tt.mutating)
			}
		})
	}
}

func TestEVMAdapter_GetBlockHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":"0x10","id":1}`))
	}))
	defer server.Close()
	a := New()
	height, err := a.GetBlockHeight(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("GetBlockHeight() error: %v", err)
	}
	if height != 16 {
		t.Errorf("height = %d, want 16", height)
	}
}

func TestEVMAdapter_ParseResponse(t *testing.T) {
	a := New()
	raw := []byte(`{"jsonrpc":"2.0","result":"0x1","id":1}`)
	resp, err := a.ParseResponse(raw)
	if err != nil {
		t.Fatalf("ParseResponse() error: %v", err)
	}
	if resp.Error != nil {
		t.Errorf("unexpected error: %v", resp.Error)
	}
}

func TestEVMAdapter_ParseResponse_Error(t *testing.T) {
	a := New()
	raw := []byte(`{"jsonrpc":"2.0","error":{"code":-32600,"message":"invalid request"},"id":1}`)
	resp, err := a.ParseResponse(raw)
	if err != nil {
		t.Fatalf("ParseResponse() error: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error")
	}
	if resp.Error.Code != -32600 {
		t.Errorf("code = %d, want -32600", resp.Error.Code)
	}
}

func TestEVMAdapter_GetFinalizedHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":{"number":"0x100"},"id":1}`))
	}))
	defer server.Close()
	a := New()
	height, err := a.GetFinalizedHeight(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if height != 256 {
		t.Errorf("height = %d, want 256", height)
	}
}

func TestEVMAdapter_ExtractBlockNumber(t *testing.T) {
	a := New()
	tests := []struct {
		method string
		params string
		want   uint64
		ok     bool
	}{
		{"eth_getBlockByNumber", `["0x100", true]`, 256, true},
		{"eth_getBlockByNumber", `["0xff", false]`, 255, true},
		{"eth_getBlockByNumber", `["latest", true]`, 0, false},
		{"eth_getBlockByNumber", `["finalized", false]`, 0, false},
		{"eth_blockNumber", `[]`, 0, false},
		{"eth_getBalance", `["0x123", "latest"]`, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.method+"_"+tt.params, func(t *testing.T) {
			req := adapter.RPCRequest{Method: tt.method, Params: json.RawMessage(tt.params)}
			got, ok := a.ExtractBlockNumber(req)
			if ok != tt.ok {
				t.Errorf("ok = %v, want %v", ok, tt.ok)
			}
			if got != tt.want {
				t.Errorf("block = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestEVMAdapter_CacheKeyFunc(t *testing.T) {
	a := New()
	mc := a.ClassifyMethod("eth_getBlockByNumber")
	if mc.CacheKeyFunc == nil {
		t.Fatal("expected CacheKeyFunc")
	}
	req := adapter.RPCRequest{Method: "eth_getBlockByNumber", Params: json.RawMessage(`["0x10", true]`)}
	key := mc.CacheKeyFunc(req)
	if key == "" {
		t.Error("expected non-empty cache key")
	}
	key2 := mc.CacheKeyFunc(req)
	if key != key2 {
		t.Errorf("keys differ: %q vs %q", key, key2)
	}
}
