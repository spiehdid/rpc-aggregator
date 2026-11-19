package evm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
)

type EVMAdapter struct {
	client *http.Client
}

func New() *EVMAdapter {
	return &EVMAdapter{
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// WithClient returns a new EVMAdapter that uses the given HTTP client.
// This is intended for testing.
func (a *EVMAdapter) WithClient(c *http.Client) *EVMAdapter {
	return &EVMAdapter{client: c}
}

func (a *EVMAdapter) Name() string { return "evm" }

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id"`
}

func (a *EVMAdapter) ParseRequest(raw []byte) (adapter.RPCRequest, error) {
	var req jsonrpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return adapter.RPCRequest{}, fmt.Errorf("parse JSON-RPC request: %w", err)
	}
	return adapter.RPCRequest{ID: req.ID, Method: req.Method, Params: req.Params}, nil
}

func (a *EVMAdapter) BuildUpstreamRequest(req adapter.RPCRequest) ([]byte, error) {
	r := jsonrpcRequest{JSONRPC: "2.0", Method: req.Method, Params: req.Params, ID: req.ID}
	return json.Marshal(r)
}

func (a *EVMAdapter) GetBlockHeight(ctx context.Context, nodeURL string) (uint64, error) {
	body := []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024)) // 1MB limit for health checks
	if err != nil {
		return 0, err
	}

	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return 0, fmt.Errorf("invalid JSON response: %w", err)
	}

	if rpcResp.Error != nil {
		return 0, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	var hexStr string
	if err := json.Unmarshal(rpcResp.Result, &hexStr); err != nil {
		return 0, fmt.Errorf("result is not a string: %s", string(rpcResp.Result))
	}

	if hexStr == "" || hexStr == "0x" {
		return 0, fmt.Errorf("empty block height from %s", nodeURL)
	}

	height, err := strconv.ParseUint(strings.TrimPrefix(hexStr, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse block height %q: %w", hexStr, err)
	}
	return height, nil
}

func (a *EVMAdapter) GetFinalizedHeight(ctx context.Context, nodeURL string) (uint64, error) {
	body := []byte(`{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["finalized",false],"id":1}`)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, nodeURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
	}
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024)) // 1MB limit for health checks
	if err != nil {
		return 0, err
	}
	var rpcResp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return 0, fmt.Errorf("invalid JSON response: %w", err)
	}
	if rpcResp.Error != nil {
		return 0, fmt.Errorf("rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}
	var block struct {
		Number string `json:"number"`
	}
	if err := json.Unmarshal(rpcResp.Result, &block); err != nil {
		return 0, err
	}
	if block.Number == "" {
		return 0, fmt.Errorf("empty block number")
	}
	height, err := strconv.ParseUint(strings.TrimPrefix(block.Number, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("parse finalized height %q: %w", block.Number, err)
	}
	return height, nil
}

func (a *EVMAdapter) ExtractBlockNumber(req adapter.RPCRequest) (uint64, bool) {
	if req.Method != "eth_getBlockByNumber" {
		return 0, false
	}
	if req.Params == nil {
		return 0, false
	}
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return 0, false
	}
	var blockTag string
	if err := json.Unmarshal(params[0], &blockTag); err != nil {
		return 0, false
	}
	if blockTag == "" || blockTag == "latest" || blockTag == "earliest" || blockTag == "pending" || blockTag == "safe" || blockTag == "finalized" {
		return 0, false
	}
	height, err := strconv.ParseUint(strings.TrimPrefix(blockTag, "0x"), 16, 64)
	if err != nil {
		return 0, false
	}
	return height, true
}

func (a *EVMAdapter) ClassifyMethod(method string) adapter.MethodClassification {
	switch method {
	case "eth_sendRawTransaction":
		return adapter.MethodClassification{Cacheable: false, Critical: true, Mutating: false}
	case "eth_blockNumber", "eth_chainId", "eth_gasPrice", "net_version":
		return adapter.MethodClassification{Cacheable: true, CacheKeyFunc: simpleKeyFunc}
	case "eth_getBlockByNumber", "eth_getBlockByHash",
		"eth_getTransactionByHash", "eth_getTransactionReceipt",
		"eth_call", "eth_getBalance", "eth_getCode",
		"eth_getStorageAt", "eth_getLogs", "eth_getTransactionCount":
		return adapter.MethodClassification{Cacheable: true, CacheKeyFunc: paramsKeyFunc}
	default:
		return adapter.MethodClassification{Cacheable: false}
	}
}

func (a *EVMAdapter) ParseResponse(raw []byte) (adapter.RPCResponse, error) {
	var resp adapter.RPCResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return adapter.RPCResponse{}, fmt.Errorf("parse JSON-RPC response: %w", err)
	}
	return resp, nil
}

// ClassifyFailure separates node problems (blocked, rate-limited, broken) from request-specific
// limitations (log range/size caps, missing method or archive state) and legitimate answers.
func (a *EVMAdapter) ClassifyFailure(req adapter.RPCRequest, status int, body []byte) adapter.FailureKind {
	isLogs := req.Method == "eth_getLogs"
	switch {
	case status == http.StatusTooManyRequests || status >= 500:
		return adapter.FailureNode
	case status == http.StatusRequestEntityTooLarge:
		return adapter.FailureCapability
	case status == http.StatusBadRequest && isLogs:
		return adapter.FailureCapability // public nodes answer oversized log ranges with a bare 400
	case status != http.StatusOK:
		return adapter.FailureNode
	}
	var resp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return adapter.FailureNode
	}
	if resp.Error == nil {
		return adapter.FailureNone
	}
	code, msg := resp.Error.Code, strings.ToLower(resp.Error.Message)
	switch {
	case strings.HasPrefix(msg, "execution reverted"):
		return adapter.FailureNone
	case code == -32601 || containsAny(msg, "method not found", "method not available", "is not available",
		"not supported", "unsupported method", "not whitelisted"):
		return adapter.FailureCapability
	case containsAny(msg, "missing trie node", "header not found", "pruned", "historical state", "state not available"):
		return adapter.FailureCapability
	// several public nodes answer an oversized log range with a bare -32602 "invalid params"
	case isLogs && (code == -32005 || code == -32602 || containsAny(msg, "range", "too large", "exceed", "limit", "more than", "too many")):
		return adapter.FailureCapability
	case code == -32005 || containsAny(msg, "rate limit", "rate-limit", "too many requests", "capacity", "quota"):
		return adapter.FailureNode
	}
	return adapter.FailureNone
}

// LogsSpan returns the inclusive number of blocks an eth_getLogs filter covers.
func (a *EVMAdapter) LogsSpan(req adapter.RPCRequest, head uint64) (uint64, bool) {
	if req.Method != "eth_getLogs" {
		return 0, false
	}
	var params []struct {
		FromBlock *string `json:"fromBlock"`
		ToBlock   *string `json:"toBlock"`
		BlockHash *string `json:"blockHash"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return 0, false
	}
	f := params[0]
	if f.BlockHash != nil {
		return 1, true
	}
	from, ok := resolveBlockTag(f.FromBlock, head)
	if !ok {
		return 0, false
	}
	to, ok := resolveBlockTag(f.ToBlock, head)
	if !ok || to < from {
		return 0, false
	}
	return to - from + 1, true
}

func resolveBlockTag(tag *string, head uint64) (uint64, bool) {
	if tag == nil {
		return head, head > 0
	}
	switch *tag {
	case "latest", "pending", "safe", "finalized":
		return head, head > 0
	case "earliest":
		return 0, true
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(*tag, "0x"), 16, 64)
	return n, err == nil
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func simpleKeyFunc(req adapter.RPCRequest) string { return req.Method }

func paramsKeyFunc(req adapter.RPCRequest) string {
	return req.Method + ":" + string(req.Params)
}
