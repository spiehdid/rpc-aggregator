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

func simpleKeyFunc(req adapter.RPCRequest) string { return req.Method }

func paramsKeyFunc(req adapter.RPCRequest) string {
	return req.Method + ":" + string(req.Params)
}
