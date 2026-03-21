package ws

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const maxUpstreamWSMessageSize = 10 << 20 // 10MB

type UpstreamPoolConfig struct {
	ReadBufferSize      int
	WriteBufferSize     int
	PingInterval        time.Duration
	PongTimeout         time.Duration
	ReconnectBackoff    time.Duration
	ReconnectMaxBackoff time.Duration
}

type UpstreamConn struct {
	mu                sync.Mutex
	conn              *websocket.Conn
	nodeID            string
	url               string
	msgHandler        func([]byte)
	disconnectHandler func(nodeID string)
	pendingMu         sync.RWMutex
	pendingRequests   map[string]chan []byte
	done              chan struct{}
	closeOnce         sync.Once
}

func newUpstreamConn(conn *websocket.Conn, nodeID, url string, pingInterval, pongTimeout time.Duration) *UpstreamConn {
	uc := &UpstreamConn{conn: conn, nodeID: nodeID, url: url, pendingRequests: make(map[string]chan []byte), done: make(chan struct{})}
	conn.SetReadLimit(maxUpstreamWSMessageSize)                      // 10MB max message from upstream
	conn.SetReadDeadline(time.Now().Add(pongTimeout + pingInterval)) // initial deadline before first pong
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(pongTimeout + pingInterval)); return nil })
	go uc.readLoop()
	go uc.pingLoop(pingInterval)
	return uc
}

func (uc *UpstreamConn) NodeID() string { return uc.nodeID }

func (uc *UpstreamConn) SetMessageHandler(h func([]byte)) {
	uc.mu.Lock()
	uc.msgHandler = h
	uc.mu.Unlock()
}
func (uc *UpstreamConn) SetDisconnectHandler(h func(string)) {
	uc.mu.Lock()
	uc.disconnectHandler = h
	uc.mu.Unlock()
}

// AddDisconnectHook registers an additional callback that fires when the
// upstream connection is lost, after the primary disconnect handler runs.
// It is safe to call concurrently with the connection lifecycle.
func (uc *UpstreamConn) AddDisconnectHook(hook func(nodeID string)) {
	uc.mu.Lock()
	prev := uc.disconnectHandler
	uc.disconnectHandler = func(nid string) {
		if prev != nil {
			prev(nid)
		}
		hook(nid)
	}
	uc.mu.Unlock()
}

func (uc *UpstreamConn) Send(msg []byte) error {
	uc.mu.Lock()
	defer uc.mu.Unlock()
	return uc.conn.WriteMessage(websocket.TextMessage, msg)
}

func (uc *UpstreamConn) RegisterPendingRequest(id string, ch chan []byte) {
	uc.pendingMu.Lock()
	uc.pendingRequests[id] = ch
	uc.pendingMu.Unlock()
}

func (uc *UpstreamConn) RemovePendingRequest(id string) {
	uc.pendingMu.Lock()
	delete(uc.pendingRequests, id)
	uc.pendingMu.Unlock()
}

func (uc *UpstreamConn) DispatchResponse(id string, msg []byte) {
	uc.pendingMu.RLock()
	ch, ok := uc.pendingRequests[id]
	if !ok {
		ch, ok = uc.pendingRequests[strings.Trim(id, `"`)]
	}
	uc.pendingMu.RUnlock()
	if ok {
		select {
		case ch <- msg:
		default:
		}
	}
}

func (uc *UpstreamConn) Close() {
	uc.closeOnce.Do(func() {
		close(uc.done)
		uc.mu.Lock()
		uc.conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		uc.conn.Close()
		uc.mu.Unlock()
	})
}

func (uc *UpstreamConn) readLoop() {
	defer func() {
		uc.mu.Lock()
		h := uc.disconnectHandler
		uc.mu.Unlock()
		if h != nil {
			h(uc.nodeID)
		}
	}()
	for {
		select {
		case <-uc.done:
			return
		default:
		}
		_, msg, err := uc.conn.ReadMessage()
		if err != nil {
			slog.Debug("upstream ws read error", "node", uc.nodeID, "error", err)
			return
		}
		uc.mu.Lock()
		h := uc.msgHandler
		uc.mu.Unlock()
		if h != nil {
			h(msg)
		}
	}
}

func (uc *UpstreamConn) pingLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-uc.done:
			return
		case <-ticker.C:
			uc.mu.Lock()
			err := uc.conn.WriteMessage(websocket.PingMessage, nil)
			uc.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

type UpstreamPool struct {
	mu                sync.RWMutex
	conns             map[string]*UpstreamConn
	cfg               UpstreamPoolConfig
	ctx               context.Context
	cancel            context.CancelFunc
	reconnectHandler  func(nodeID string, uc *UpstreamConn)
	onReconnectFailed func(nodeID string)
	reconnecting      map[string]bool // nodeID → reconnect loop in progress
	reconnectingMu    sync.Mutex
}

func NewUpstreamPool(ctx context.Context, cfg UpstreamPoolConfig) *UpstreamPool {
	poolCtx, cancel := context.WithCancel(ctx)
	return &UpstreamPool{
		conns:        make(map[string]*UpstreamConn),
		cfg:          cfg,
		ctx:          poolCtx,
		cancel:       cancel,
		reconnecting: make(map[string]bool),
	}
}

func (p *UpstreamPool) SetReconnectHandler(h func(nodeID string, uc *UpstreamConn)) {
	p.mu.Lock()
	p.reconnectHandler = h
	p.mu.Unlock()
}

func (p *UpstreamPool) SetReconnectFailedHandler(h func(nodeID string)) {
	p.mu.Lock()
	p.onReconnectFailed = h
	p.mu.Unlock()
}

func (p *UpstreamPool) GetOrConnect(ctx context.Context, nodeID, wsURL string) (*UpstreamConn, error) {
	p.mu.RLock()
	if uc, ok := p.conns[nodeID]; ok {
		p.mu.RUnlock()
		return uc, nil
	}
	p.mu.RUnlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if uc, ok := p.conns[nodeID]; ok {
		return uc, nil
	}
	dialer := websocket.Dialer{ReadBufferSize: p.cfg.ReadBufferSize, WriteBufferSize: p.cfg.WriteBufferSize}
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return nil, err
	}
	uc := newUpstreamConn(conn, nodeID, wsURL, p.cfg.PingInterval, p.cfg.PongTimeout)
	uc.SetDisconnectHandler(func(nid string) {
		p.mu.Lock()
		delete(p.conns, nid)
		p.mu.Unlock()
		slog.Warn("upstream ws disconnected, reconnecting", "node", nid)
		go p.reconnectLoop(nid, wsURL)
	})
	p.conns[nodeID] = uc
	return uc, nil
}

func (p *UpstreamPool) Remove(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if uc, ok := p.conns[nodeID]; ok {
		uc.Close()
		delete(p.conns, nodeID)
	}
}

func (p *UpstreamPool) reconnectLoop(nodeID, wsURL string) {
	p.reconnectingMu.Lock()
	if p.reconnecting[nodeID] {
		p.reconnectingMu.Unlock()
		return // another loop is already handling this node
	}
	p.reconnecting[nodeID] = true
	p.reconnectingMu.Unlock()

	defer func() {
		p.reconnectingMu.Lock()
		delete(p.reconnecting, nodeID)
		p.reconnectingMu.Unlock()
	}()

	backoff := p.cfg.ReconnectBackoff
	const maxRetries = 100
	for attempt := 0; attempt < maxRetries; attempt++ {
		select {
		case <-p.ctx.Done():
			return
		case <-time.After(backoff):
		}

		dialer := websocket.Dialer{ReadBufferSize: p.cfg.ReadBufferSize, WriteBufferSize: p.cfg.WriteBufferSize}
		conn, _, err := dialer.DialContext(p.ctx, wsURL, nil)
		if err != nil {
			slog.Debug("upstream ws reconnect failed", "node", nodeID, "error", err, "backoff", backoff, "attempt", attempt+1)
			backoff = backoff * 2
			if backoff > p.cfg.ReconnectMaxBackoff {
				backoff = p.cfg.ReconnectMaxBackoff
			}
			continue
		}

		uc := newUpstreamConn(conn, nodeID, wsURL, p.cfg.PingInterval, p.cfg.PongTimeout)
		uc.SetDisconnectHandler(func(nid string) {
			p.mu.Lock()
			delete(p.conns, nid)
			p.mu.Unlock()
			go p.reconnectLoop(nid, wsURL)
		})

		p.mu.Lock()
		p.conns[nodeID] = uc
		handler := p.reconnectHandler
		p.mu.Unlock()

		slog.Info("upstream ws reconnected", "node", nodeID)
		if handler != nil {
			handler(nodeID, uc)
		}
		return
	}
	slog.Error("upstream ws reconnect gave up after max retries", "node", nodeID, "attempts", maxRetries)
	p.mu.RLock()
	h := p.onReconnectFailed
	p.mu.RUnlock()
	if h != nil {
		h(nodeID)
	}
}

func (p *UpstreamPool) Close() {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, uc := range p.conns {
		uc.Close()
		delete(p.conns, id)
	}
}
