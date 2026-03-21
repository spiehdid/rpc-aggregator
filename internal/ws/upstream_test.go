package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var testUpgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func startMockWSServer(t *testing.T, handler func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		defer conn.Close()
		handler(conn)
	}))
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func newTestUpstreamPool() *UpstreamPool {
	return NewUpstreamPool(context.Background(), UpstreamPoolConfig{
		ReadBufferSize: 1024, WriteBufferSize: 1024,
		PingInterval: 30 * time.Second, PongTimeout: 10 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, ReconnectMaxBackoff: 1 * time.Second,
	})
}

func TestUpstreamPool_Connect(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			conn.WriteMessage(mt, msg)
		}
	})
	defer server.Close()
	pool := newTestUpstreamPool()
	defer pool.Close()
	conn, err := pool.GetOrConnect(context.Background(), "node1", wsURL(server.URL))
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if conn == nil {
		t.Fatal("nil connection")
	}
}

func TestUpstreamPool_ReusesConnection(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()
	pool := newTestUpstreamPool()
	defer pool.Close()
	ctx := context.Background()
	conn1, _ := pool.GetOrConnect(ctx, "node1", wsURL(server.URL))
	conn2, _ := pool.GetOrConnect(ctx, "node1", wsURL(server.URL))
	if conn1 != conn2 {
		t.Error("expected same connection")
	}
}

func TestUpstreamPool_SendAndReceive(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			conn.WriteMessage(mt, msg)
		}
	})
	defer server.Close()
	pool := newTestUpstreamPool()
	defer pool.Close()
	uc, _ := pool.GetOrConnect(context.Background(), "node1", wsURL(server.URL))
	var received []byte
	var wg sync.WaitGroup
	wg.Add(1)
	uc.SetMessageHandler(func(msg []byte) { received = msg; wg.Done() })
	uc.Send([]byte(`{"jsonrpc":"2.0","method":"eth_subscribe","params":["newHeads"],"id":1}`))
	wg.Wait()
	if len(received) == 0 {
		t.Error("no message received")
	}
}

func TestUpstreamPool_DispatchResponse(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()
	pool := newTestUpstreamPool()
	defer pool.Close()
	uc, err := pool.GetOrConnect(context.Background(), "node1", wsURL(server.URL))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}

	// Register a pending request and dispatch a response
	ch := make(chan []byte, 1)
	uc.RegisterPendingRequest("42", ch)

	expectedMsg := []byte(`{"jsonrpc":"2.0","result":"0xabc","id":42}`)
	uc.DispatchResponse("42", expectedMsg)

	select {
	case msg := <-ch:
		if string(msg) != string(expectedMsg) {
			t.Errorf("dispatched message = %q, want %q", string(msg), string(expectedMsg))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatched response")
	}

	// Clean up
	uc.RemovePendingRequest("42")
}

func TestUpstreamPool_Remove(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})
	defer server.Close()
	pool := newTestUpstreamPool()
	defer pool.Close()
	ctx := context.Background()

	_, err := pool.GetOrConnect(ctx, "node1", wsURL(server.URL))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}

	// Verify the connection exists
	pool.mu.RLock()
	_, exists := pool.conns["node1"]
	pool.mu.RUnlock()
	if !exists {
		t.Fatal("node1 should exist before removal")
	}

	// Remove and verify it's gone
	pool.Remove("node1")

	pool.mu.RLock()
	_, exists = pool.conns["node1"]
	pool.mu.RUnlock()
	if exists {
		t.Error("node1 should not exist after removal")
	}
}

func TestUpstreamPool_DisconnectHandler(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		// Read one message then close to trigger disconnect
		conn.ReadMessage()
	})
	defer server.Close()
	pool := newTestUpstreamPool()
	defer pool.Close()

	uc, err := pool.GetOrConnect(context.Background(), "node1", wsURL(server.URL))
	if err != nil {
		t.Fatalf("connect error: %v", err)
	}

	disconnected := make(chan string, 1)
	uc.SetDisconnectHandler(func(nodeID string) {
		disconnected <- nodeID
	})

	// Send a message so the server reads it and closes the connection
	uc.Send([]byte(`{"id":1}`))

	select {
	case nodeID := <-disconnected:
		if nodeID != "node1" {
			t.Errorf("disconnect handler got nodeID = %q, want %q", nodeID, "node1")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for disconnect handler to be called")
	}
}

func TestUpstreamPool_ConnectToDeadServer(t *testing.T) {
	pool := newTestUpstreamPool()
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Use an invalid URL that will fail to connect
	_, err := pool.GetOrConnect(ctx, "dead-node", "ws://127.0.0.1:1")
	if err == nil {
		t.Fatal("expected error when connecting to dead server, got nil")
	}
}

func TestUpstreamPool_ReconnectsAfterDisconnect(t *testing.T) {
	server := startMockWSServer(t, func(conn *websocket.Conn) {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reconnected := make(chan string, 1)
	pool := NewUpstreamPool(ctx, UpstreamPoolConfig{
		ReadBufferSize: 1024, WriteBufferSize: 1024,
		PingInterval: 30 * time.Second, PongTimeout: 10 * time.Second,
		ReconnectBackoff: 50 * time.Millisecond, ReconnectMaxBackoff: 200 * time.Millisecond,
	})
	pool.SetReconnectHandler(func(nodeID string, uc *UpstreamConn) {
		reconnected <- nodeID
	})
	defer pool.Close()

	uc, err := pool.GetOrConnect(ctx, "node1", wsURL(server.URL))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Force disconnect by closing the underlying connection
	uc.Close()

	// Wait for reconnect
	select {
	case nid := <-reconnected:
		if nid != "node1" {
			t.Errorf("reconnected node = %q, want node1", nid)
		}
	case <-time.After(2 * time.Second):
		t.Error("reconnect did not happen within 2s")
	}
}
