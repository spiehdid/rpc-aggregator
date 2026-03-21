package solana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spiehdid/rpc-aggregator/internal/adapter"
)

type SolanaAdapter struct {
	client *http.Client
}

func New() *SolanaAdapter {
	return &SolanaAdapter{
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (a *SolanaAdapter) Name() string { return "solana" }

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id"`
}

func (a *SolanaAdapter) ParseRequest(raw []byte) (adapter.RPCRequest, error) {
	var req jsonrpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return adapter.RPCRequest{}, fmt.Errorf("parse JSON-RPC request: %w", err)
	}
	return adapter.RPCRequest{ID: req.ID, Method: req.Method, Params: req.Params}, nil
}

func (a *SolanaAdapter) BuildUpstreamRequest(req adapter.RPCRequest) ([]byte, error) {
	r := jsonrpcRequest{JSONRPC: "2.0", Method: req.Method, Params: req.Params, ID: req.ID}
	return json.Marshal(r)
}

func (a *SolanaAdapter) GetBlockHeight(ctx context.Context, nodeURL string) (uint64, error) {
	return a.callGetSlot(ctx, nodeURL, nil)
}

func (a *SolanaAdapter) GetFinalizedHeight(ctx context.Context, nodeURL string) (uint64, error) {
	params := json.RawMessage(`[{"commitment":"finalized"}]`)
	return a.callGetSlot(ctx, nodeURL, params)
}

func (a *SolanaAdapter) callGetSlot(ctx context.Context, nodeURL string, params json.RawMessage) (uint64, error) {
	reqBody := jsonrpcRequest{JSONRPC: "2.0", Method: "getSlot", ID: json.RawMessage(`1`)}
	if params != nil {
		reqBody.Params = params
	}
	body, _ := json.Marshal(reqBody)

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

	var slot uint64
	if err := json.Unmarshal(rpcResp.Result, &slot); err != nil {
		return 0, fmt.Errorf("parse slot: %w", err)
	}
	return slot, nil
}

func (a *SolanaAdapter) ClassifyMethod(method string) adapter.MethodClassification {
	switch method {
	case "sendTransaction":
		return adapter.MethodClassification{Cacheable: false, Critical: true, Mutating: false}
	case "getSlot", "getVersion", "getGenesisHash", "getMinimumBalanceForRentExemption", "getEpochInfo":
		return adapter.MethodClassification{Cacheable: true, CacheKeyFunc: simpleKeyFunc}
	case "getBlock", "getTransaction", "getBalance", "getAccountInfo",
		"getTokenAccountBalance", "getTokenAccountsByOwner",
		"getProgramAccounts", "getSignaturesForAddress",
		"getBlockHeight", "getBlockTime", "getMultipleAccounts":
		return adapter.MethodClassification{Cacheable: true, CacheKeyFunc: paramsKeyFunc}
	default:
		return adapter.MethodClassification{Cacheable: false}
	}
}

func (a *SolanaAdapter) ParseResponse(raw []byte) (adapter.RPCResponse, error) {
	var resp adapter.RPCResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		return adapter.RPCResponse{}, fmt.Errorf("parse JSON-RPC response: %w", err)
	}
	return resp, nil
}

func (a *SolanaAdapter) ExtractBlockNumber(req adapter.RPCRequest) (uint64, bool) {
	if req.Method != "getBlock" && req.Method != "getBlockTime" {
		return 0, false
	}
	if req.Params == nil {
		return 0, false
	}
	var params []json.RawMessage
	if err := json.Unmarshal(req.Params, &params); err != nil || len(params) == 0 {
		return 0, false
	}
	var slot uint64
	if err := json.Unmarshal(params[0], &slot); err != nil {
		return 0, false
	}
	return slot, slot > 0
}

func simpleKeyFunc(req adapter.RPCRequest) string { return req.Method }

func paramsKeyFunc(req adapter.RPCRequest) string {
	return req.Method + ":" + string(req.Params)
}
