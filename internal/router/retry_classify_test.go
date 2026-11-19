package router

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spiehdid/rpc-aggregator/internal/adapter"
	"github.com/spiehdid/rpc-aggregator/internal/adapter/evm"
	"github.com/spiehdid/rpc-aggregator/internal/cache"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/pool"
)

type fakeNode struct {
	hits    atomic.Int32
	handler func(w http.ResponseWriter, body []byte)
}

// setupMultiNode builds a router over fake upstreams; node i gets latency (i+1)*10ms, so node 0 is preferred.
func setupMultiNode(t *testing.T, nodes ...*fakeNode) (*Router, *pool.NodePool) {
	t.Helper()
	weights := pool.ScoringWeights{Latency: 0.4, BlockHeight: 0.3, ErrorRate: 0.3}
	cbCfg := pool.CircuitBreakerConfig{ErrorThreshold: 0.5, Window: 30 * time.Second, RecoveryTimeout: 30 * time.Second}
	p := pool.NewNodePool("ethereum", pool.NewScorer(weights), cbCfg)
	for i, fn := range nodes {
		fn := fn
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fn.hits.Add(1)
			b, _ := io.ReadAll(r.Body)
			fn.handler(w, b)
		}))
		t.Cleanup(srv.Close)
		id := fmt.Sprintf("node%d", i)
		p.AddNode(id, srv.URL, 1.0)
		p.GetNode(id).SetBlockHeight(1_000_000)
		p.GetNode(id).SetLatency(time.Duration(i+1) * 10 * time.Millisecond)
	}
	chains := map[string]*ChainRoute{
		"/eth": {Name: "ethereum", Adapter: evm.New(), Pool: p, Cache: cache.New(1000), RuleMatcher: cache.NewRuleMatcher(nil, 5*time.Second)},
	}
	return New(chains, metrics.New(prometheus.NewRegistry()), 3, 3, 100, 30*time.Second), p
}

func reply(body string) func(http.ResponseWriter, []byte) {
	return func(w http.ResponseWriter, _ []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}
}

func status(code int, body string) func(http.ResponseWriter, []byte) {
	return func(w http.ResponseWriter, _ []byte) {
		w.WriteHeader(code)
		w.Write([]byte(body))
	}
}

// rangeLimited answers log queries wider than max blocks with a range error.
func rangeLimited(max uint64) func(http.ResponseWriter, []byte) {
	return func(w http.ResponseWriter, b []byte) {
		var req struct {
			Params []struct{ FromBlock, ToBlock string } `json:"params"`
		}
		json.Unmarshal(b, &req)
		from, _ := strconv.ParseUint(strings.TrimPrefix(req.Params[0].FromBlock, "0x"), 16, 64)
		to, _ := strconv.ParseUint(strings.TrimPrefix(req.Params[0].ToBlock, "0x"), 16, 64)
		w.Header().Set("Content-Type", "application/json")
		if to-from+1 > max {
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"block range too large"}}`))
			return
		}
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":["from-limited-node"]}`))
	}
}

func logsReq(from, to uint64) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock":"0x%x","toBlock":"0x%x"}],"id":1}`, from, to)
}

func call(t *testing.T, rtr *Router, body string) adapter.RPCResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/eth", strings.NewReader(body))
	w := httptest.NewRecorder()
	rtr.ServeHTTP(w, req)
	var resp adapter.RPCResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response %q: %v", w.Body.String(), err)
	}
	return resp
}

func TestRetry_GetLogsRangeErrorGoesToAnotherNode(t *testing.T) {
	a := &fakeNode{handler: rangeLimited(1000)}
	b := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"result":["from-b"]}`)}
	rtr, p := setupMultiNode(t, a, b)

	resp := call(t, rtr, logsReq(1, 10_000))
	if resp.Error != nil || !strings.Contains(string(resp.Result), "from-b") {
		t.Fatalf("want result from node b, got %+v / %s", resp.Error, resp.Result)
	}
	if a.hits.Load() != 1 || b.hits.Load() != 1 {
		t.Errorf("hits a=%d b=%d, want 1/1", a.hits.Load(), b.hits.Load())
	}
	if er := p.GetNode("node0").ErrorRate(); er != 0 {
		t.Errorf("range refusal must not count against the circuit breaker, error rate %v", er)
	}
}

func TestRetry_HTTP403GoesToAnotherNodeAndCountsAsNodeError(t *testing.T) {
	a := &fakeNode{handler: status(403, "forbidden")}
	b := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"result":"0x10"}`)}
	rtr, p := setupMultiNode(t, a, b)

	resp := call(t, rtr, `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0","latest"],"id":1}`)
	if resp.Error != nil || string(resp.Result) != `"0x10"` {
		t.Fatalf("want 0x10 from node b, got %+v / %s", resp.Error, resp.Result)
	}
	if p.GetNode("node0").ErrorRate() == 0 {
		t.Error("403 must count against the node's circuit breaker")
	}
}

func TestRetry_LearnedRangeLimitRoutesWideQueriesAway(t *testing.T) {
	a := &fakeNode{handler: rangeLimited(1000)}
	b := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"result":["from-b"]}`)}
	rtr, _ := setupMultiNode(t, a, b)

	call(t, rtr, logsReq(1, 10_000)) // a refuses, learns limit
	call(t, rtr, logsReq(1, 20_000)) // wider: must skip a entirely
	if a.hits.Load() != 1 {
		t.Errorf("node a hit %d times, want 1 (wide query must skip it)", a.hits.Load())
	}
	resp := call(t, rtr, logsReq(1, 500)) // narrow: a is still preferred and serves it
	if a.hits.Load() != 2 || !strings.Contains(string(resp.Result), "from-limited-node") {
		t.Errorf("narrow query should go to node a: hits=%d result=%s", a.hits.Load(), resp.Result)
	}
}

func TestRetry_LimitedNodeStillTriedWhenItIsTheOnlyOne(t *testing.T) {
	a := &fakeNode{handler: rangeLimited(1000)}
	rtr, _ := setupMultiNode(t, a)

	call(t, rtr, logsReq(1, 10_000))
	resp := call(t, rtr, logsReq(1, 20_000))
	if a.hits.Load() != 2 {
		t.Errorf("only node must still be tried, hits=%d", a.hits.Load())
	}
	if resp.Error == nil || !strings.Contains(resp.Error.Message, "range") {
		t.Errorf("client must see the upstream range error to split its query, got %+v", resp.Error)
	}
}

func TestRetry_ErrorAnswerDoesNotCountAsServedSpan(t *testing.T) {
	// a JSON-RPC error that isn't classified as a failure is returned, but must not teach the
	// router that the node serves that span
	a := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"filter not found"}}`)}
	rtr, p := setupMultiNode(t, a)

	call(t, rtr, logsReq(1, 50_000))
	if got := p.GetNode("node0").MaxOKLogsSpan(); got != 0 {
		t.Errorf("MaxOKLogsSpan = %d after an error answer, want 0", got)
	}
}

func TestRetry_RevertIsReturnedNotRetried(t *testing.T) {
	a := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`)}
	b := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"result":"0x"}`)}
	rtr, _ := setupMultiNode(t, a, b)

	resp := call(t, rtr, `{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x0"},"latest"],"id":1}`)
	if resp.Error == nil || resp.Error.Code != 3 {
		t.Fatalf("revert must reach the client, got %+v", resp.Error)
	}
	if b.hits.Load() != 0 {
		t.Error("legitimate errors must not be retried")
	}
}

func TestRetry_OversizedResponseIsRetriedNotTruncated(t *testing.T) {
	big := `{"jsonrpc":"2.0","id":1,"result":["` + strings.Repeat("x", 500) + `"]}`
	a := &fakeNode{handler: reply(big)}
	b := &fakeNode{handler: reply(`{"jsonrpc":"2.0","id":1,"result":["small"]}`)}
	rtr, p := setupMultiNode(t, a, b)
	rtr.maxResponseSize = 200

	resp := call(t, rtr, logsReq(1, 50))
	if resp.Error != nil || !strings.Contains(string(resp.Result), "small") {
		t.Fatalf("want small result from node b, got %+v / %s", resp.Error, resp.Result)
	}
	if p.GetNode("node0").ErrorRate() != 0 {
		t.Error("oversized response is a capability limit, not a node failure")
	}
}

func TestRetry_AllNodesForbiddenGivesGenericError(t *testing.T) {
	a := &fakeNode{handler: status(403, "<html>forbidden</html>")}
	b := &fakeNode{handler: status(403, "<html>forbidden</html>")}
	rtr, _ := setupMultiNode(t, a, b)

	resp := call(t, rtr, `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x0","latest"],"id":1}`)
	if resp.Error == nil || resp.Error.Code != -32603 {
		t.Errorf("want -32603 when no node returned a JSON-RPC answer, got %+v", resp.Error)
	}
}
