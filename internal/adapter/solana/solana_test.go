package solana

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
)

func TestSolanaAdapter_Name(t *testing.T) {
	a := New()
	if a.Name() != "solana" {
		t.Errorf("Name() = %q, want %q", a.Name(), "solana")
	}
}

func TestSolanaAdapter_GetBlockHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":123456789,"id":1}`))
	}))
	defer server.Close()
	a := New()
	height, err := a.GetBlockHeight(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if height != 123456789 {
		t.Errorf("height = %d, want 123456789", height)
	}
}

func TestSolanaAdapter_GetFinalizedHeight(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","result":123456700,"id":1}`))
	}))
	defer server.Close()
	a := New()
	height, err := a.GetFinalizedHeight(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if height != 123456700 {
		t.Errorf("height = %d, want 123456700", height)
	}
}

func TestSolanaAdapter_ParseRequest(t *testing.T) {
	a := New()
	raw := []byte(`{"jsonrpc":"2.0","method":"getSlot","params":[],"id":1}`)
	req, err := a.ParseRequest(raw)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if req.Method != "getSlot" {
		t.Errorf("Method = %q, want %q", req.Method, "getSlot")
	}
}

func TestSolanaAdapter_BuildUpstreamRequest(t *testing.T) {
	a := New()
	req := adapter.RPCRequest{ID: json.RawMessage(`1`), Method: "getSlot", Params: json.RawMessage(`[]`)}
	data, err := a.BuildUpstreamRequest(req)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	var parsed map[string]interface{}
	json.Unmarshal(data, &parsed)
	if parsed["jsonrpc"] != "2.0" {
		t.Errorf("jsonrpc = %v", parsed["jsonrpc"])
	}
	if parsed["method"] != "getSlot" {
		t.Errorf("method = %v", parsed["method"])
	}
}

func TestSolanaAdapter_ClassifyMethod(t *testing.T) {
	a := New()
	tests := []struct {
		method   string
		cache    bool
		critical bool
		mutating bool
	}{
		{"getSlot", true, false, false},
		{"getGenesisHash", true, false, false},
		{"getBlock", true, false, false},
		{"getBalance", true, false, false},
		{"getAccountInfo", true, false, false},
		{"sendTransaction", false, true, false},
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

func TestSolanaAdapter_ExtractBlockNumber(t *testing.T) {
	a := New()
	tests := []struct {
		name   string
		method string
		params string
		want   uint64
		ok     bool
	}{
		{"getBlock with slot", "getBlock", `[123456]`, 123456, true},
		{"getBlockTime with slot", "getBlockTime", `[789012]`, 789012, true},
		{"getBlock no params", "getBlock", `[]`, 0, false},
		{"getSlot", "getSlot", `[]`, 0, false},
		{"getBalance", "getBalance", `["addr"]`, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

func TestSolanaAdapter_ParseResponse(t *testing.T) {
	a := New()
	raw := []byte(`{"jsonrpc":"2.0","result":12345,"id":1}`)
	resp, err := a.ParseResponse(raw)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if resp.Error != nil {
		t.Errorf("unexpected error: %v", resp.Error)
	}
}
