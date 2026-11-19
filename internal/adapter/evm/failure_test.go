package evm

import (
	"testing"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
)

func req(method, params string) adapter.RPCRequest {
	return adapter.RPCRequest{ID: []byte("1"), Method: method, Params: []byte(params)}
}

func TestEVMAdapter_ClassifyFailure(t *testing.T) {
	a := New()
	logs := req("eth_getLogs", `[{"fromBlock":"0x1","toBlock":"0x2710"}]`)
	call := req("eth_call", `[{"to":"0x0"},"latest"]`)
	rpcErr := func(code int, msg string) []byte {
		return []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":` + itoa(code) + `,"message":"` + msg + `"}}`)
	}
	ok := []byte(`{"jsonrpc":"2.0","id":1,"result":[]}`)

	tests := []struct {
		name   string
		req    adapter.RPCRequest
		status int
		body   []byte
		want   adapter.FailureKind
	}{
		{"success", logs, 200, ok, adapter.FailureNone},
		{"revert is a legitimate answer", call, 200, rpcErr(3, "execution reverted"), adapter.FailureNone},
		{"invalid params is the client's fault", call, 200, rpcErr(-32602, "invalid argument 0"), adapter.FailureNone},
		{"5xx", logs, 502, []byte("bad gateway"), adapter.FailureNode},
		{"429", call, 429, []byte("slow down"), adapter.FailureNode},
		{"403 blocked", call, 403, []byte("forbidden"), adapter.FailureNode},
		{"non-JSON 200", call, 200, []byte("<html>"), adapter.FailureNode},
		{"rate limit in body", call, 200, rpcErr(-32005, "rate limited"), adapter.FailureNode},
		{"413 on getLogs", logs, 413, []byte("Request Entity Too Large"), adapter.FailureCapability},
		{"400 on getLogs", logs, 400, []byte("Bad Request"), adapter.FailureCapability},
		{"400 on eth_call", call, 400, []byte("Bad Request"), adapter.FailureNode},
		{"bare invalid params on getLogs is a range refusal", logs, 200, rpcErr(-32602, "invalid params"), adapter.FailureCapability},
		{"range too large", logs, 200, rpcErr(-32602, "block range too large: total blocks 10000"), adapter.FailureCapability},
		{"exceed max range", logs, 200, rpcErr(-32701, "exceed maximum block range: 500"), adapter.FailureCapability},
		{"limit exceeded on getLogs", logs, 200, rpcErr(-32005, "limit exceeded"), adapter.FailureCapability},
		{"getLogs limited", logs, 200, rpcErr(-32602, "eth_getLogs is limited to a 1000 range"), adapter.FailureCapability},
		{"too many results", logs, 200, rpcErr(-32005, "query returned more than 10000 results"), adapter.FailureCapability},
		{"method not available", logs, 200, rpcErr(-32601, "method is not available"), adapter.FailureCapability},
		{"pruned state", call, 200, rpcErr(-32000, "missing trie node abc"), adapter.FailureCapability},
		{"header not found", call, 200, rpcErr(-32000, "header not found"), adapter.FailureCapability},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := a.ClassifyFailure(tt.req, tt.status, tt.body); got != tt.want {
				t.Errorf("ClassifyFailure = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEVMAdapter_LogsSpan(t *testing.T) {
	a := New()
	tests := []struct {
		name   string
		req    adapter.RPCRequest
		head   uint64
		want   uint64
		wantOK bool
	}{
		{"hex range is inclusive", req("eth_getLogs", `[{"fromBlock":"0x10","toBlock":"0x1f"}]`), 0, 16, true},
		{"single block", req("eth_getLogs", `[{"fromBlock":"0x10","toBlock":"0x10"}]`), 0, 1, true},
		{"latest resolves to head", req("eth_getLogs", `[{"fromBlock":"0x64","toBlock":"latest"}]`), 199, 100, true},
		{"missing bounds default to latest", req("eth_getLogs", `[{"address":"0x0"}]`), 500, 1, true},
		{"blockHash covers one block", req("eth_getLogs", `[{"blockHash":"0xabc"}]`), 0, 1, true},
		{"tag without known head", req("eth_getLogs", `[{"fromBlock":"0x1","toBlock":"latest"}]`), 0, 0, false},
		{"earliest is block 0", req("eth_getLogs", `[{"fromBlock":"earliest","toBlock":"0x9"}]`), 0, 10, true},
		{"inverted range", req("eth_getLogs", `[{"fromBlock":"0x10","toBlock":"0x1"}]`), 0, 0, false},
		{"garbage", req("eth_getLogs", `[{"fromBlock":"xyz"}]`), 100, 0, false},
		{"other method", req("eth_call", `[{"to":"0x0"},"latest"]`), 100, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := a.LogsSpan(tt.req, tt.head)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("LogsSpan = (%d, %v), want (%d, %v)", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func itoa(i int) string {
	neg := i < 0
	if neg {
		i = -i
	}
	s := ""
	for {
		s = string(rune('0'+i%10)) + s
		i /= 10
		if i == 0 {
			break
		}
	}
	if neg {
		s = "-" + s
	}
	return s
}
