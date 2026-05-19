//go:build mock || !cgo

package connection

import (
	"context"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
)

// --- Mock SendBufferPool ---

type mockSendPool struct {
	mu      sync.Mutex
	buffers []*api.Buffer
}

func newMockSendPool() *mockSendPool {
	return &mockSendPool{}
}

func (p *mockSendPool) Alloc(size int) (*api.Buffer, error) {
	buf := &api.Buffer{
		Addr:   unsafe.Pointer(&make([]byte, size)[0]),
		Length: size,
		LKey:   1,
		RKey:   2,
	}
	p.mu.Lock()
	p.buffers = append(p.buffers, buf)
	p.mu.Unlock()
	return buf, nil
}

func (p *mockSendPool) Free(buf *api.Buffer) {}

func (p *mockSendPool) Stats() api.BufferPoolStats {
	return api.BufferPoolStats{}
}

func (p *mockSendPool) Close() error { return nil }

func (p *mockSendPool) AcquireForSend() (*api.Buffer, error) {
	return p.Alloc(4096)
}

func (p *mockSendPool) CompleteSend(buf *api.Buffer) {}

// --- Mock RecvBufferPool ---

type mockRecvPool struct {
	mu      sync.Mutex
	buffers []*api.Buffer
}

func newMockRecvPool() *mockRecvPool {
	return &mockRecvPool{}
}

func (p *mockRecvPool) Alloc(size int) (*api.Buffer, error) {
	buf := &api.Buffer{
		Addr:   unsafe.Pointer(&make([]byte, size)[0]),
		Length: size,
		LKey:   1,
		RKey:   2,
	}
	p.mu.Lock()
	p.buffers = append(p.buffers, buf)
	p.mu.Unlock()
	return buf, nil
}

func (p *mockRecvPool) Free(buf *api.Buffer) {}

func (p *mockRecvPool) Stats() api.BufferPoolStats {
	return api.BufferPoolStats{}
}

func (p *mockRecvPool) Close() error { return nil }

func (p *mockRecvPool) PostRecvBuffers(qp api.QueuePair, count int) error {
	return nil
}

func (p *mockRecvPool) Replenish(qp api.QueuePair, consumed int) error {
	return nil
}

// --- Mock CQPoller ---

type mockCQPoller struct{}

func (p *mockCQPoller) Start(ctx context.Context) error             { return nil }
func (p *mockCQPoller) AddCQ(cq api.CompletionQueue, handler api.CompletionHandler) error {
	return nil
}
func (p *mockCQPoller) RemoveCQ(cq api.CompletionQueue) error { return nil }
func (p *mockCQPoller) Stats() api.CQPollerStats              { return api.CQPollerStats{} }
func (p *mockCQPoller) Close() error                          { return nil }

// --- Helper functions ---

func newTestConn(mockVerbs *rdma.MockVerbs) *Conn {
	deps := ConnDeps{
		Verbs:    mockVerbs,
		QP:       &rdma.MockQP{},
		SendPool: newMockSendPool(),
		RecvPool: newMockRecvPool(),
	}
	return NewConn(1, "192.168.1.1:4791", deps)
}

func newTestManager() (*Manager, *rdma.MockVerbs, *rdma.MockCMEventChannel) {
	mockVerbs := rdma.NewMockVerbs()
	mockCM := rdma.NewMockCMEventChannel(10)

	cfg := ManagerConfig{
		Verbs:      mockVerbs,
		CMChannel:  mockCM,
		SendPool:   newMockSendPool(),
		RecvPool:   newMockRecvPool(),
		CQPoller:   &mockCQPoller{},
		QueueDepth: 64,
	}

	mgr := NewManager(cfg)
	return mgr, mockVerbs, mockCM
}

// --- Conn Tests ---

func TestConnStateMachine(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)

	// Should start in StateInit
	if conn.State() != api.StateInit {
		t.Fatalf("expected StateInit, got %d", conn.State())
	}

	// Transition to Connecting
	conn.SetState(api.StateConnecting)
	if conn.State() != api.StateConnecting {
		t.Fatalf("expected StateConnecting, got %d", conn.State())
	}

	// Transition to Connected
	conn.SetState(api.StateConnected)
	if conn.State() != api.StateConnected {
		t.Fatalf("expected StateConnected, got %d", conn.State())
	}

	// Transition to Draining
	conn.SetState(api.StateDraining)
	if conn.State() != api.StateDraining {
		t.Fatalf("expected StateDraining, got %d", conn.State())
	}

	// Transition to Closed
	conn.SetState(api.StateClosed)
	if conn.State() != api.StateClosed {
		t.Fatalf("expected StateClosed, got %d", conn.State())
	}
}

func TestConnSend(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)

	// Move to Connected state
	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	// Create a message with a buffer
	data := make([]byte, 64)
	msg := &api.Message{
		Buffer: &api.Buffer{
			Addr:   unsafe.Pointer(&data[0]),
			Length: 64,
			LKey:   1,
		},
		Length:  64,
		ImmData: 42,
	}

	// Send should succeed
	err := conn.Send(msg)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Verify PostSend was called
	log := mockVerbs.GetPostSendLog()
	if len(log) != 1 {
		t.Fatalf("expected 1 PostSend call, got %d", len(log))
	}

	if log[0].ImmData != 42 {
		t.Fatalf("expected ImmData=42, got %d", log[0].ImmData)
	}
}

func TestConnSendNotConnected(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)

	// In StateInit - Send should fail
	msg := &api.Message{Length: 10}
	err := conn.Send(msg)
	if err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected, got %v", err)
	}
}

func TestConnRecv(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)
	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	// Deliver a message
	msg := &api.Message{
		Length:  32,
		ImmData: 99,
	}
	conn.DeliverRecv(msg)

	// Receive it
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received, err := conn.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}

	if received.ImmData != 99 {
		t.Fatalf("expected ImmData=99, got %d", received.ImmData)
	}

	if received.Length != 32 {
		t.Fatalf("expected Length=32, got %d", received.Length)
	}
}

func TestConnRecvContextCancel(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)
	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	// Create a context that we'll cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel it immediately
	cancel()

	// Recv should return context.Canceled
	_, err := conn.Recv(ctx)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestConnClose(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)
	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	err := conn.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if conn.State() != api.StateClosed {
		t.Fatalf("expected StateClosed, got %d", conn.State())
	}

	// Send should fail after close
	msg := &api.Message{Length: 10}
	err = conn.Send(msg)
	if err != ErrNotConnected {
		t.Fatalf("expected ErrNotConnected after close, got %v", err)
	}
}

func TestConnStateCallback(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)

	var transitions []struct {
		old, new api.ConnectionState
	}
	var mu sync.Mutex

	conn.OnStateChange(func(old, new api.ConnectionState) {
		mu.Lock()
		transitions = append(transitions, struct {
			old, new api.ConnectionState
		}{old, new})
		mu.Unlock()
	})

	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	mu.Lock()
	defer mu.Unlock()

	if len(transitions) != 2 {
		t.Fatalf("expected 2 transitions, got %d", len(transitions))
	}

	if transitions[0].old != api.StateInit || transitions[0].new != api.StateConnecting {
		t.Fatalf("unexpected first transition: %v -> %v", transitions[0].old, transitions[0].new)
	}

	if transitions[1].old != api.StateConnecting || transitions[1].new != api.StateConnected {
		t.Fatalf("unexpected second transition: %v -> %v", transitions[1].old, transitions[1].new)
	}
}

func TestConnID(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	deps := ConnDeps{
		Verbs:    mockVerbs,
		QP:       &rdma.MockQP{},
		SendPool: newMockSendPool(),
		RecvPool: newMockRecvPool(),
	}

	conn := NewConn(42, "10.0.0.1:4791", deps)
	if conn.ID() != 42 {
		t.Fatalf("expected ID=42, got %d", conn.ID())
	}
	if conn.RemoteAddr() != "10.0.0.1:4791" {
		t.Fatalf("expected RemoteAddr=10.0.0.1:4791, got %s", conn.RemoteAddr())
	}
}

// --- Manager Tests ---

func TestManagerAccept(t *testing.T) {
	mgr, _, mockCM := newTestManager()
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the CM event loop in a goroutine
	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()

	go mgr.RunCMEventLoop(loopCtx)

	// Inject a ConnectRequest event
	mockCM.InjectEvent(&api.CMEvent{
		Type:        api.EventConnectRequest,
		PrivateData: []byte("192.168.1.100:4791"),
	})

	// Accept should return the connection
	conn, err := mgr.Accept(ctx)
	if err != nil {
		t.Fatalf("Accept failed: %v", err)
	}

	if conn == nil {
		t.Fatal("Accept returned nil connection")
	}

	if conn.State() != api.StateConnecting {
		t.Fatalf("expected StateConnecting, got %d", conn.State())
	}

	if conn.RemoteAddr() != "192.168.1.100:4791" {
		t.Fatalf("expected remote addr 192.168.1.100:4791, got %s", conn.RemoteAddr())
	}
}

func TestManagerConnect(t *testing.T) {
	mgr, _, _ := newTestManager()
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := mgr.Connect(ctx, "10.0.0.1:4791")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	if conn == nil {
		t.Fatal("Connect returned nil connection")
	}

	if conn.State() != api.StateConnected {
		t.Fatalf("expected StateConnected, got %d", conn.State())
	}

	if conn.RemoteAddr() != "10.0.0.1:4791" {
		t.Fatalf("expected remote addr 10.0.0.1:4791, got %s", conn.RemoteAddr())
	}
}

func TestManagerGetConnection(t *testing.T) {
	mgr, _, _ := newTestManager()
	defer mgr.Close()

	ctx := context.Background()

	// Create a connection
	conn, err := mgr.Connect(ctx, "10.0.0.1:4791")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Should be retrievable by ID
	found, ok := mgr.GetConnection(conn.ID())
	if !ok {
		t.Fatal("GetConnection returned false")
	}

	if found.ID() != conn.ID() {
		t.Fatalf("expected ID=%d, got %d", conn.ID(), found.ID())
	}

	// Non-existent ID should return false
	_, ok = mgr.GetConnection(99999)
	if ok {
		t.Fatal("GetConnection returned true for non-existent ID")
	}
}

func TestManagerClose(t *testing.T) {
	mgr, _, _ := newTestManager()

	ctx := context.Background()

	// Create some connections
	conn1, _ := mgr.Connect(ctx, "10.0.0.1:4791")
	conn2, _ := mgr.Connect(ctx, "10.0.0.2:4791")

	// Close the manager
	err := mgr.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// All connections should be closed
	if conn1.State() != api.StateClosed {
		t.Fatalf("expected conn1 StateClosed, got %d", conn1.State())
	}
	if conn2.State() != api.StateClosed {
		t.Fatalf("expected conn2 StateClosed, got %d", conn2.State())
	}

	// Connect should fail after close
	_, err = mgr.Connect(ctx, "10.0.0.3:4791")
	if err != ErrManagerClosed {
		t.Fatalf("expected ErrManagerClosed, got %v", err)
	}

	// Accept should fail after close
	_, err = mgr.Accept(ctx)
	if err != ErrManagerClosed {
		t.Fatalf("expected ErrManagerClosed, got %v", err)
	}
}

func TestCMEventLoop(t *testing.T) {
	mgr, _, mockCM := newTestManager()
	defer mgr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the CM event loop
	loopDone := make(chan error, 1)
	go func() {
		loopDone <- mgr.RunCMEventLoop(ctx)
	}()

	// Inject a ConnectRequest
	mockCM.InjectEvent(&api.CMEvent{
		Type:        api.EventConnectRequest,
		PrivateData: []byte("peer1"),
	})

	// Wait for the connection to appear in Accept
	acceptCtx, acceptCancel := context.WithTimeout(context.Background(), time.Second)
	defer acceptCancel()

	conn, err := mgr.Accept(acceptCtx)
	if err != nil {
		t.Fatalf("Accept failed: %v", err)
	}
	if conn.State() != api.StateConnecting {
		t.Fatalf("expected StateConnecting, got %d", conn.State())
	}

	// Inject Established event - should transition the connecting conn
	mockCM.InjectEvent(&api.CMEvent{
		Type: api.EventEstablished,
	})

	// Give the event loop a moment to process
	time.Sleep(50 * time.Millisecond)

	if conn.State() != api.StateConnected {
		t.Fatalf("expected StateConnected after Established event, got %d", conn.State())
	}

	// Inject Disconnected event
	mockCM.InjectEvent(&api.CMEvent{
		Type: api.EventDisconnected,
	})

	// Give the event loop a moment to process
	time.Sleep(50 * time.Millisecond)

	if conn.State() != api.StateClosed {
		t.Fatalf("expected StateClosed after Disconnected event, got %d", conn.State())
	}

	// Cancel context to stop the event loop
	cancel()

	select {
	case err := <-loopDone:
		if err != nil && err != context.Canceled {
			t.Fatalf("unexpected event loop error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("event loop did not stop after context cancel")
	}
}

func TestCMEventLoopRejected(t *testing.T) {
	mgr, _, mockCM := newTestManager()
	defer mgr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.RunCMEventLoop(ctx)

	// Inject ConnectRequest followed by Rejected
	mockCM.InjectEvent(&api.CMEvent{
		Type:        api.EventConnectRequest,
		PrivateData: []byte("rejected-peer"),
	})

	acceptCtx, acceptCancel := context.WithTimeout(context.Background(), time.Second)
	defer acceptCancel()

	conn, err := mgr.Accept(acceptCtx)
	if err != nil {
		t.Fatalf("Accept failed: %v", err)
	}

	// Inject Rejected event
	mockCM.InjectEvent(&api.CMEvent{
		Type: api.EventRejected,
	})

	// Give the event loop a moment to process
	time.Sleep(50 * time.Millisecond)

	if conn.State() != api.StateError {
		t.Fatalf("expected StateError after Rejected event, got %d", conn.State())
	}
}

func TestConnRecvAfterClose(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)
	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	// Close the connection
	conn.Close()

	// Recv should return ErrConnClosed
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := conn.Recv(ctx)
	if err != ErrConnClosed {
		t.Fatalf("expected ErrConnClosed, got %v", err)
	}
}

func TestConnDoubleClose(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	conn := newTestConn(mockVerbs)
	conn.SetState(api.StateConnecting)
	conn.SetState(api.StateConnected)

	// First close should succeed
	err := conn.Close()
	if err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	// Second close should be a no-op
	err = conn.Close()
	if err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}
