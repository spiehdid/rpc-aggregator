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

// FailureKind classifies an upstream response for retry decisions.
type FailureKind int

const (
	// FailureNone: the response is usable as-is, including legitimate JSON-RPC errors (e.g. reverts).
	FailureNone FailureKind = iota
	// FailureNode: the node is unhealthy or rejecting us; retry elsewhere and count it against the circuit breaker.
	FailureNode
	// FailureCapability: the node can't serve this particular request (range/size limits, method or
	// archive state unavailable); retry elsewhere but keep the node healthy.
	FailureCapability
)

func (k FailureKind) String() string {
	switch k {
	case FailureNone:
		return "none"
	case FailureNode:
		return "node"
	case FailureCapability:
		return "capability"
	}
	return "unknown"
}

// FailureClassifier is optionally implemented by adapters that can tell node failures from
// request-specific limitations. Adapters without it only treat 5xx/429 as failures.
type FailureClassifier interface {
	ClassifyFailure(req RPCRequest, status int, body []byte) FailureKind
}

// RangeExtractor is optionally implemented by adapters whose log queries have per-node block-range limits.
type RangeExtractor interface {
	// LogsSpan returns how many blocks a log query covers; head resolves block tags like "latest".
	LogsSpan(req RPCRequest, head uint64) (uint64, bool)
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
