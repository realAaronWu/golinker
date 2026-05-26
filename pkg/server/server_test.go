package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/config"
)

// --- Mock types ---

// mockConnection implements api.Connection for testing.
type mockConnection struct {
	id       uint64
	addr     string
	state    api.ConnectionState
	recvCh   chan *api.Message
	sendMu   sync.Mutex
	sent     []*api.Message
	closed   bool
	closeMu  sync.Mutex
}

func newMockConn(id uint64) *mockConnection {
	return &mockConnection{
		id:     id,
		addr:   "127.0.0.1:8629",
		state:  api.StateConnected,
		recvCh: make(chan *api.Message, 16),
	}
}

func (m *mockConnection) ID() uint64                                         { return m.id }
func (m *mockConnection) RemoteAddr() string                                 { return m.addr }
func (m *mockConnection) State() api.ConnectionState                         { return m.state }
func (m *mockConnection) SendPayload(_ []byte) error                         { return nil }
func (m *mockConnection) OnStateChange(fn func(old, new api.ConnectionState)) {}

func (m *mockConnection) Send(msg *api.Message) error {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockConnection) Recv(ctx context.Context) (*api.Message, error) {
	select {
	case msg := <-m.recvCh:
		if msg == nil {
			return nil, errors.New("connection closed")
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *mockConnection) Close() error {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConnection) isClosed() bool {
	m.closeMu.Lock()
	defer m.closeMu.Unlock()
	return m.closed
}

func (m *mockConnection) getSent() []*api.Message {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	cp := make([]*api.Message, len(m.sent))
	copy(cp, m.sent)
	return cp
}

// mockConnMgr implements api.ConnectionManager for testing.
type mockConnMgr struct {
	acceptCh chan api.Connection
}

func newMockConnMgr() *mockConnMgr {
	return &mockConnMgr{
		acceptCh: make(chan api.Connection, 16),
	}
}

func (m *mockConnMgr) Accept(ctx context.Context) (api.Connection, error) {
	select {
	case conn := <-m.acceptCh:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *mockConnMgr) Connect(ctx context.Context, addr string) (api.Connection, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConnMgr) GetConnection(id uint64) (api.Connection, bool) {
	return nil, false
}

func (m *mockConnMgr) Close() error {
	return nil
}

// mockHandler implements api.MessageHandler for testing.
type mockHandler struct {
	mu      sync.Mutex
	handled []handledMsg
	resp    *api.Message
	respErr error
}

type handledMsg struct {
	connID uint64
	msg    *api.Message
}

func (m *mockHandler) Handle(conn api.Connection, msg *api.Message) (*api.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handled = append(m.handled, handledMsg{connID: conn.ID(), msg: msg})
	return m.resp, m.respErr
}

func (m *mockHandler) getHandled() []handledMsg {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]handledMsg, len(m.handled))
	copy(cp, m.handled)
	return cp
}

// --- Helper ---

func newTestServer(t *testing.T, connMgr *mockConnMgr) *Server {
	t.Helper()
	cfg := config.DefaultConfig()
	srv, err := NewServer(cfg, ServerDeps{ConnMgr: connMgr})
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	return srv
}

// --- Tests ---

func TestServerStartStop(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if !srv.started.Load() {
		t.Fatal("expected started to be true")
	}

	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if srv.started.Load() {
		t.Fatal("expected started to be false after Stop")
	}
}

func TestServerAcceptConnection(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	conn := newMockConn(42)
	mgr.acceptCh <- conn

	// Wait for the connection to be tracked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		_, ok := srv.conns[42]
		srv.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	srv.mu.RLock()
	_, ok := srv.conns[42]
	srv.mu.RUnlock()
	if !ok {
		t.Fatal("expected connection 42 to be tracked")
	}

	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srv.Stop(stopCtx)
}

func TestServerHandleMessage(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)
	h := &mockHandler{}
	srv.RegisterHandler(h)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	conn := newMockConn(1)
	msg := &api.Message{Length: 100}
	conn.recvCh <- msg

	mgr.acceptCh <- conn

	// Wait for handler to process.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.getHandled()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	handled := h.getHandled()
	if len(handled) == 0 {
		t.Fatal("expected handler to be called")
	}
	if handled[0].connID != 1 {
		t.Fatalf("expected connID=1, got %d", handled[0].connID)
	}
	if handled[0].msg != msg {
		t.Fatal("expected same message pointer")
	}

	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srv.Stop(stopCtx)
}

func TestServerHandlerResponse(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)
	resp := &api.Message{Length: 200}
	h := &mockHandler{resp: resp}
	srv.RegisterHandler(h)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	conn := newMockConn(2)
	conn.recvCh <- &api.Message{Length: 100}

	mgr.acceptCh <- conn

	// Wait for send to happen.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(conn.getSent()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	sent := conn.getSent()
	if len(sent) == 0 {
		t.Fatal("expected response to be sent")
	}
	if sent[0] != resp {
		t.Fatal("expected the handler response to be sent back")
	}

	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srv.Stop(stopCtx)
}

func TestServerGracefulShutdown(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Accept a connection that will block on Recv.
	conn := newMockConn(10)
	mgr.acceptCh <- conn

	// Wait for connection to be tracked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		_, ok := srv.conns[10]
		srv.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Stop with a short deadline - should drain gracefully.
	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := srv.Stop(stopCtx); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// After stop, connections map should be empty.
	srv.mu.RLock()
	remaining := len(srv.conns)
	srv.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("expected 0 remaining connections, got %d", remaining)
	}
}

func TestServerRegisterHandler(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)

	h := &mockHandler{}
	srv.RegisterHandler(h)

	if srv.handler != h {
		t.Fatal("expected handler to be set")
	}
}

func TestServerMultipleConnections(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Accept 5 connections.
	conns := make([]*mockConnection, 5)
	for i := range conns {
		conns[i] = newMockConn(uint64(i + 100))
		mgr.acceptCh <- conns[i]
	}

	// Wait for all to be tracked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		count := len(srv.conns)
		srv.mu.RUnlock()
		if count >= 5 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	srv.mu.RLock()
	count := len(srv.conns)
	srv.mu.RUnlock()
	if count != 5 {
		t.Fatalf("expected 5 tracked connections, got %d", count)
	}

	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srv.Stop(stopCtx)
}

func TestServerConnectionRemoval(t *testing.T) {
	mgr := newMockConnMgr()
	srv := newTestServer(t, mgr)

	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	conn := newMockConn(77)
	mgr.acceptCh <- conn

	// Wait for the connection to be tracked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		_, ok := srv.conns[77]
		srv.mu.RUnlock()
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	srv.mu.RLock()
	_, ok := srv.conns[77]
	srv.mu.RUnlock()
	if !ok {
		t.Fatal("expected connection 77 to be tracked")
	}

	// Close the recvCh to simulate connection close (Recv returns error).
	close(conn.recvCh)

	// Wait for connection to be removed.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		srv.mu.RLock()
		_, exists := srv.conns[77]
		srv.mu.RUnlock()
		if !exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	srv.mu.RLock()
	_, exists := srv.conns[77]
	srv.mu.RUnlock()
	if exists {
		t.Fatal("expected connection 77 to be removed after recv error")
	}

	stopCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srv.Stop(stopCtx)
}
