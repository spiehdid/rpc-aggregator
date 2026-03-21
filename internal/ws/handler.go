package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/spiehdid/rpc-aggregator/internal/metrics"
	"github.com/spiehdid/rpc-aggregator/internal/router"
)

type HandlerConfig struct {
	MaxConnections          int
	MaxSubscriptionsPerConn int
	MaxTotalSubscriptions   int
	MaxMessageSize          int64
	SendQueueSize           int
	WriteTimeout            time.Duration
	ConsumerReadBuffer      int
	ConsumerWriteBuffer     int
	UpstreamReadBuffer      int
	UpstreamWriteBuffer     int
	PingInterval            time.Duration
	PongTimeout             time.Duration
	ReconnectBackoff        time.Duration
	ReconnectMaxBackoff     time.Duration
	AllowedOrigins          []string
}

// Handler wraps the HTTP router, upgrading WebSocket connections and delegating
// plain HTTP requests to the underlying router.
type Handler struct {
	chains        map[string]*router.ChainRoute
	router        *router.Router
	metrics       *metrics.Metrics
	cfg           HandlerConfig
	upgrader      websocket.Upgrader
	pool          *UpstreamPool
	subs          *SubscriptionManager
	connCount     atomic.Int32
	consumersMu   sync.Mutex
	consumers     map[string]*websocket.Conn
	consumerChMu  sync.RWMutex
	consumerChans map[string]chan<- []byte
	ctx           context.Context
	cancel        context.CancelFunc
	// upstreamTrackedMu guards upstreamTracked which records nodeIDs for which
	// WSUpstreamConnections has been incremented so we can decrement on disconnect.
	upstreamTrackedMu sync.Mutex
	upstreamTracked   map[string]string // nodeID -> chainName
	// subscribeLocks is a fixed-size pool of mutexes indexed by hash of shareKey.
	// This bounds memory and eliminates the need for cleanup.
	subscribeLocks [64]sync.Mutex
}

// jsonRPCRequest is a minimal JSON-RPC request for parsing method and id.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      json.RawMessage `json:"id"`
}

// jsonRPCResponse is a minimal JSON-RPC response.
type jsonRPCResponse struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      json.RawMessage  `json:"id"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *json.RawMessage `json:"error,omitempty"`
}

// subscriptionEvent represents an eth_subscription notification from upstream.
type subscriptionEvent struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	} `json:"params"`
}

// NewHandler creates a new WS handler that wraps the HTTP router.
func NewHandler(chains map[string]*router.ChainRoute, rtr *router.Router, m *metrics.Metrics, cfg HandlerConfig) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{
		chains:  chains,
		router:  rtr,
		metrics: m,
		cfg:     cfg,
		pool: NewUpstreamPool(ctx, UpstreamPoolConfig{
			ReadBufferSize:      cfg.UpstreamReadBuffer,
			WriteBufferSize:     cfg.UpstreamWriteBuffer,
			PingInterval:        cfg.PingInterval,
			PongTimeout:         cfg.PongTimeout,
			ReconnectBackoff:    cfg.ReconnectBackoff,
			ReconnectMaxBackoff: cfg.ReconnectMaxBackoff,
		}),
		subs:            NewSubscriptionManager(cfg.MaxSubscriptionsPerConn, cfg.MaxTotalSubscriptions),
		consumers:       make(map[string]*websocket.Conn),
		consumerChans:   make(map[string]chan<- []byte),
		upstreamTracked: make(map[string]string),
		ctx:             ctx,
		cancel:          cancel,
	}
	h.upgrader = websocket.Upgrader{
		ReadBufferSize:  cfg.ConsumerReadBuffer,
		WriteBufferSize: cfg.ConsumerWriteBuffer,
		CheckOrigin:     makeOriginChecker(cfg.AllowedOrigins),
	}
	h.pool.SetReconnectFailedHandler(func(nodeID string) {
		h.metrics.WSReconnectsFailed.WithLabelValues(nodeID).Inc()
	})
	h.pool.SetReconnectHandler(func(nodeID string, uc *UpstreamConn) {
		h.upstreamTrackedMu.Lock()
		chainName := h.upstreamTracked[nodeID]
		h.upstreamTrackedMu.Unlock()
		if chainName == "" {
			chainName = "unknown"
		}
		h.metrics.WSReconnectsTotal.WithLabelValues(chainName, nodeID).Inc()
		h.metrics.WSUpstreamConnections.WithLabelValues(chainName, nodeID).Set(1)
		subs := h.subs.GetSubsByNodeID(nodeID)
		for _, sub := range subs {
			go h.resubscribeUpstream(uc, sub)
		}
	})
	return h
}

// ServeHTTP upgrades WebSocket connections or delegates to the HTTP router.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isWebSocketUpgrade(r) {
		h.router.ServeHTTP(w, r)
		return
	}

	path := r.URL.Path
	chain, ok := h.chains[path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	chainName := chain.Name

	if int(h.connCount.Load()) >= h.cfg.MaxConnections {
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}

	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade failed", "error", err)
		return
	}

	consumerID := generateSubID()
	consumerCtx, consumerCancel := context.WithCancel(h.ctx)

	h.connCount.Add(1)
	h.metrics.WSConnectionsActive.WithLabelValues(chainName).Inc()
	h.consumersMu.Lock()
	h.consumers[consumerID] = conn
	h.consumersMu.Unlock()

	sendCh := make(chan []byte, h.cfg.SendQueueSize)

	h.consumerChMu.Lock()
	h.consumerChans[consumerID] = sendCh
	h.consumerChMu.Unlock()

	go h.writeLoop(consumerCtx, conn, sendCh)
	go func() {
		defer func() {
			consumerCancel()

			// Unregister consumer channel BEFORE closing sendCh to prevent
			// send-on-closed-channel panic in fanOutEvent.
			h.consumerChMu.Lock()
			delete(h.consumerChans, consumerID)
			h.consumerChMu.Unlock()

			close(sendCh)

			// Clean up subscriptions and unsubscribe orphaned upstream subs.
			orphaned := h.subs.RemoveConsumer(consumerID)
			for _, sub := range orphaned {
				go h.sendUpstreamUnsubscribe(sub)
			}

			h.consumersMu.Lock()
			delete(h.consumers, consumerID)
			h.consumersMu.Unlock()

			h.connCount.Add(-1)
			h.metrics.WSConnectionsActive.WithLabelValues(chainName).Dec()

			conn.Close()
		}()
		h.readLoop(consumerCtx, conn, consumerID, path, sendCh)
	}()
}

// readLoop reads messages from a consumer WebSocket and routes them.
func (h *Handler) readLoop(ctx context.Context, conn *websocket.Conn, consumerID, path string, sendCh chan<- []byte) {
	conn.SetReadLimit(h.cfg.MaxMessageSize)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Debug("consumer ws read error", "consumer", consumerID, "error", err)
			}
			return
		}

		var req jsonRPCRequest
		if err := json.Unmarshal(msg, &req); err != nil {
			resp := makeJSONRPCError(nil, -32700, "parse error")
			select {
			case sendCh <- resp:
			case <-ctx.Done():
				return
			}
			continue
		}

		switch {
		case strings.HasSuffix(req.Method, "Subscribe"):
			h.handleSubscribe(ctx, consumerID, path, &req, sendCh)
		case strings.HasSuffix(req.Method, "Unsubscribe"):
			h.handleUnsubscribe(ctx, consumerID, &req, sendCh)
		default:
			reqCtx, reqCancel := context.WithTimeout(ctx, h.cfg.WriteTimeout)
			result := h.router.ProcessRequest(reqCtx, path, msg)
			reqCancel()
			select {
			case sendCh <- result:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (h *Handler) writeLoop(ctx context.Context, conn *websocket.Conn, sendCh <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-sendCh:
			if !ok {
				return
			}
			conn.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				slog.Debug("consumer ws write error", "error", err)
				return
			}
		}
	}
}

// getSubscribeLock returns a mutex from a fixed-size pool indexed by the FNV-1a
// hash of shareKey. This bounds memory and requires no cleanup.
func (h *Handler) getSubscribeLock(shareKey string) *sync.Mutex {
	return &h.subscribeLocks[fnv32(shareKey)%64]
}

// fnv32 computes a 32-bit FNV-1a hash of s.
func fnv32(s string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (h *Handler) handleSubscribe(ctx context.Context, consumerID, path string, req *jsonRPCRequest, sendCh chan<- []byte) {
	chain, ok := h.chains[path]
	if !ok {
		resp := makeJSONRPCError(req.ID, -32600, "unknown chain")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	shareMethod := req.Method
	paramsStr := string(req.Params)

	// Acquire per-shareKey lock to serialize the SubscribeShared check →
	// upstream subscribe → Subscribe sequence and prevent duplicate upstream
	// subscriptions from concurrent requests for the same topic.
	shareKey := chain.Name + ":" + shareMethod + ":" + paramsStr
	skMu := h.getSubscribeLock(shareKey)
	skMu.Lock()
	defer skMu.Unlock()

	subID, shared, err := h.subs.SubscribeShared(consumerID, chain.Name, shareMethod, paramsStr)
	if err != nil {
		resp := makeJSONRPCError(req.ID, -32603, err.Error())
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}
	if shared {
		h.metrics.WSSubscriptionsActive.WithLabelValues(chain.Name, shareMethod).Inc()
		subIDJSON, _ := json.Marshal(subID)
		resp := makeJSONRPCResult(req.ID, subIDJSON)
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	node := chain.Pool.SelectBest()
	if node == nil {
		resp := makeJSONRPCError(req.ID, -32603, "no healthy upstream nodes")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	wsURL := DeriveWSURL(node.URL, "")
	uc, err := h.pool.GetOrConnect(ctx, node.ID, wsURL)
	if err != nil {
		resp := makeJSONRPCError(req.ID, -32603, "upstream connection failed")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	h.upstreamTrackedMu.Lock()
	if _, alreadyTracked := h.upstreamTracked[node.ID]; !alreadyTracked {
		h.upstreamTracked[node.ID] = chain.Name
		h.metrics.WSUpstreamConnections.WithLabelValues(chain.Name, node.ID).Inc()
		// Arrange to decrement gauge when this upstream disconnects.
		localChain := chain.Name
		localNodeID := node.ID
		uc.AddDisconnectHook(func(_ string) {
			h.upstreamTrackedMu.Lock()
			delete(h.upstreamTracked, localNodeID)
			h.upstreamTrackedMu.Unlock()
			h.metrics.WSUpstreamConnections.WithLabelValues(localChain, localNodeID).Dec()
		})
	}
	h.upstreamTrackedMu.Unlock()

	uc.SetMessageHandler(func(msg []byte) {
		h.handleUpstreamMessage(uc, msg)
	})

	requestID := generateSubID()
	upstreamReq, _ := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  req.Method,
		Params:  req.Params,
		ID:      json.RawMessage(`"` + requestID + `"`),
	})

	responseCh := make(chan []byte, 1)
	uc.RegisterPendingRequest(requestID, responseCh)
	defer uc.RemovePendingRequest(requestID)

	if err := uc.Send(upstreamReq); err != nil {
		resp := makeJSONRPCError(req.ID, -32603, "upstream send failed")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	var upstreamResp []byte
	select {
	case upstreamResp = <-responseCh:
	case <-time.After(h.cfg.WriteTimeout):
		resp := makeJSONRPCError(req.ID, -32603, "upstream subscribe timeout")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	case <-ctx.Done():
		return
	}

	var upResp jsonRPCResponse
	if err := json.Unmarshal(upstreamResp, &upResp); err != nil || upResp.Error != nil {
		resp := makeJSONRPCError(req.ID, -32603, "upstream subscribe failed")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	var upstreamSubID string
	if err := json.Unmarshal(upResp.Result, &upstreamSubID); err != nil {
		resp := makeJSONRPCError(req.ID, -32603, "invalid upstream subscription id")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	consumerSubID, err := h.subs.Subscribe(consumerID, chain.Name, shareMethod, paramsStr, upstreamSubID, node.ID)
	if err != nil {
		resp := makeJSONRPCError(req.ID, -32603, err.Error())
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	h.metrics.WSSubscriptionsActive.WithLabelValues(chain.Name, shareMethod).Inc()
	consumerSubIDJSON, _ := json.Marshal(consumerSubID)
	resp := makeJSONRPCResult(req.ID, consumerSubIDJSON)
	select {
	case sendCh <- resp:
	case <-ctx.Done():
	}
}

func (h *Handler) handleUnsubscribe(ctx context.Context, consumerID string, req *jsonRPCRequest, sendCh chan<- []byte) {
	var params []json.RawMessage
	if req.Params != nil {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			slog.Warn("failed to parse unsubscribe params", "error", err)
			return
		}
	}
	if len(params) == 0 {
		resp := makeJSONRPCError(req.ID, -32602, "missing subscription id")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}
	var subID string
	if err := json.Unmarshal(params[0], &subID); err != nil {
		resp := makeJSONRPCError(req.ID, -32602, "invalid subscription id")
		select {
		case sendCh <- resp:
		case <-ctx.Done():
		}
		return
	}

	subInfo, orphaned := h.subs.Unsubscribe(consumerID, subID)
	if subInfo != nil {
		h.metrics.WSSubscriptionsActive.WithLabelValues(subInfo.Chain, subInfo.Method).Dec()
	}
	if orphaned != nil {
		go h.sendUpstreamUnsubscribe(*orphaned)
	}
	resp := makeJSONRPCResult(req.ID, json.RawMessage(`true`))
	select {
	case sendCh <- resp:
	case <-ctx.Done():
	}
}

// handleUpstreamMessage routes messages received from an upstream connection.
// It dispatches RPC responses to pending requests and fans out subscription events.
func (h *Handler) handleUpstreamMessage(uc *UpstreamConn, msg []byte) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(msg, &raw); err != nil {
		return
	}

	if idRaw, hasID := raw["id"]; hasID {
		var id string
		if err := json.Unmarshal(idRaw, &id); err == nil && id != "" {
			uc.DispatchResponse(id, msg)
			return
		}
	}

	if paramsRaw, hasParams := raw["params"]; hasParams {
		var params struct {
			Subscription string `json:"subscription"`
		}
		if err := json.Unmarshal(paramsRaw, &params); err == nil && params.Subscription != "" {
			h.fanOutEvent(msg)
		}
	}
}

// fanOutEvent parses an upstream subscription event, finds all consumers
// subscribed to it, replaces the subscription ID, and sends to each consumer.
func (h *Handler) fanOutEvent(msg []byte) {
	var event subscriptionEvent
	if err := json.Unmarshal(msg, &event); err != nil {
		return
	}

	upstreamSubID := event.Params.Subscription
	consumers := h.subs.GetConsumersForUpstream(upstreamSubID)
	if len(consumers) == 0 {
		return
	}

	if info := h.subs.GetUpstreamSubInfo(upstreamSubID); info != nil {
		h.metrics.WSEventsTotal.WithLabelValues(info.Chain, info.Method).Add(float64(len(consumers)))
	}

	for consumerID, consumerSubID := range consumers {
		outEvent := subscriptionEvent{
			JSONRPC: "2.0",
			Method:  event.Method,
		}
		outEvent.Params.Subscription = consumerSubID
		outEvent.Params.Result = event.Params.Result

		data, err := json.Marshal(outEvent)
		if err != nil {
			continue
		}

		h.consumerChMu.RLock()
		ch, ok := h.consumerChans[consumerID]
		h.consumerChMu.RUnlock()

		if ok {
			select {
			case ch <- data:
			default:
				slog.Debug("consumer send queue full, dropping event", "consumer", consumerID)
			}
		}
	}
}

// resubscribeUpstream re-establishes a single subscription on a freshly reconnected upstream.
func (h *Handler) resubscribeUpstream(uc *UpstreamConn, sub SubResubInfo) {
	requestID := generateSubID()
	var params json.RawMessage
	if sub.Params != "" && sub.Params != "[]" {
		params = json.RawMessage(sub.Params)
	} else {
		params = json.RawMessage(`[]`)
	}
	upReq, _ := json.Marshal(struct {
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		ID      json.RawMessage `json:"id"`
	}{
		JSONRPC: "2.0",
		Method:  sub.Method,
		Params:  params,
		ID:      json.RawMessage(`"` + requestID + `"`),
	})

	responseCh := make(chan []byte, 1)
	uc.RegisterPendingRequest(requestID, responseCh)
	defer uc.RemovePendingRequest(requestID)

	uc.SetMessageHandler(func(msg []byte) {
		h.handleUpstreamMessage(uc, msg)
	})

	if err := uc.Send(upReq); err != nil {
		slog.Error("resubscribe send failed", "node", uc.NodeID(), "method", sub.Method, "error", err)
		return
	}

	select {
	case resp := <-responseCh:
		var parsed struct {
			Result json.RawMessage `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(resp, &parsed); err != nil {
			slog.Warn("failed to parse resubscribe response", "error", err)
			return
		}
		if parsed.Error != nil {
			slog.Error("resubscribe failed", "node", uc.NodeID(), "error", parsed.Error.Message)
			return
		}
		var newUpstreamSubID string
		if err := json.Unmarshal(parsed.Result, &newUpstreamSubID); err != nil {
			slog.Warn("failed to parse resubscribe result", "error", err)
			return
		}
		h.subs.ReattachUpstream(sub.ShareKey, newUpstreamSubID)
		slog.Info("resubscribed after reconnect", "node", uc.NodeID(), "method", sub.Method)
	case <-h.ctx.Done():
	case <-time.After(10 * time.Second):
		slog.Error("resubscribe timeout", "node", uc.NodeID(), "method", sub.Method)
	}
}

// sendUpstreamUnsubscribe sends an unsubscribe request to the upstream node
// for an orphaned subscription. This is fire-and-forget.
func (h *Handler) sendUpstreamUnsubscribe(sub OrphanedSub) {
	uc, err := h.pool.GetOrConnect(h.ctx, sub.NodeID, "")
	if err != nil {
		slog.Debug("cannot unsubscribe orphaned upstream sub, no connection", "node", sub.NodeID, "upstreamSub", sub.UpstreamSubID)
		return
	}
	unsubReq, _ := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		Method:  sub.UnsubscribeMethod,
		Params:  json.RawMessage(`["` + sub.UpstreamSubID + `"]`),
		ID:      json.RawMessage(`"` + generateSubID() + `"`),
	})
	if err := uc.Send(unsubReq); err != nil {
		slog.Debug("failed to send upstream unsubscribe", "node", sub.NodeID, "upstreamSub", sub.UpstreamSubID, "error", err)
	}
}

// Close shuts down the handler, closing all consumer and upstream connections.
func (h *Handler) Close() {
	h.cancel()

	h.consumersMu.Lock()
	for id, conn := range h.consumers {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutting down"))
		conn.Close()
		delete(h.consumers, id)
	}
	h.consumersMu.Unlock()

	h.pool.Close()
}

// DeriveWSURL converts an HTTP URL to a WebSocket URL.
// If override is non-empty, it is returned as-is.
func DeriveWSURL(httpURL, override string) string {
	if override != "" {
		return override
	}
	if rest, ok := strings.CutPrefix(httpURL, "https://"); ok {
		return "wss://" + rest
	}
	if rest, ok := strings.CutPrefix(httpURL, "http://"); ok {
		return "ws://" + rest
	}
	return httpURL
}

// makeOriginChecker returns a CheckOrigin function for the WebSocket upgrader.
// If allowed is empty or contains "*", all origins are permitted (backwards compatible).
// Otherwise only exact matches against the allowlist pass.
func makeOriginChecker(allowed []string) func(r *http.Request) bool {
	if len(allowed) == 0 {
		return func(r *http.Request) bool { return true }
	}
	// Check for wildcard
	for _, o := range allowed {
		if o == "*" {
			return func(r *http.Request) bool { return true }
		}
	}
	// Build allowlist set
	set := make(map[string]bool, len(allowed))
	for _, o := range allowed {
		set[o] = true
	}
	return func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true // non-browser clients don't send Origin
		}
		return set[origin]
	}
}

func isWebSocketUpgrade(r *http.Request) bool {
	for _, v := range r.Header["Upgrade"] {
		if strings.EqualFold(v, "websocket") {
			return true
		}
	}
	return false
}

func makeJSONRPCError(id json.RawMessage, code int, message string) []byte {
	if id == nil {
		id = json.RawMessage(`null`)
	}
	type rpcError struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	resp := rpcError{JSONRPC: "2.0", ID: id}
	resp.Error.Code = code
	resp.Error.Message = message
	data, _ := json.Marshal(resp)
	return data
}

func makeJSONRPCResult(id json.RawMessage, result json.RawMessage) []byte {
	if id == nil {
		id = json.RawMessage(`null`)
	}
	type rpcResult struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
	}
	resp := rpcResult{JSONRPC: "2.0", ID: id, Result: result}
	data, _ := json.Marshal(resp)
	return data
}
