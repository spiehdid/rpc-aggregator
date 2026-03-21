package adapter

import (
	"context"
	"encoding/json"
)

type RPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type RPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type MethodClassification struct {
	Cacheable    bool
	CacheKeyFunc func(req RPCRequest) string
	Critical     bool
	Mutating     bool
}

type ChainAdapter interface {
	Name() string
	ParseRequest(raw []byte) (RPCRequest, error)
	BuildUpstreamRequest(req RPCRequest) ([]byte, error)
	GetBlockHeight(ctx context.Context, nodeURL string) (uint64, error)
	GetFinalizedHeight(ctx context.Context, nodeURL string) (uint64, error)
	ExtractBlockNumber(req RPCRequest) (uint64, bool)
	ClassifyMethod(method string) MethodClassification
	ParseResponse(raw []byte) (RPCResponse, error)
}
