//go:build mock || !cgo

// Package integration contains end-to-end smoke tests that exercise the full
// data path (send and recv) using mock RDMA verbs. These tests verify that
// all the cross-package wiring from Phase 5 (FLOW-001, FLOW-002) works
// correctly end-to-end.
package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
	"github.com/wua20/golinker/pkg/connection"
	"github.com/wua20/golinker/pkg/cq"
	"github.com/wua20/golinker/pkg/message"
)

// --- Lightweight mocks for integration tests ---

type mockPool struct{}

func (p *mockPool) Alloc(_ int) (*api.Buffer, error) {
	data := make([]byte, 4096)
	return &api.Buffer{
		Addr:   unsafe.Pointer(&data[0]),
		Length: 4096,
		LKey:   1,
	}, nil
}
func (p *mockPool) Free(_ *api.Buffer)                           {}
func (p *mockPool) Stats() api.BufferPoolStats                   { return api.BufferPoolStats{} }
func (p *mockPool) Close() error                                 { return nil }
func (p *mockPool) AcquireForSend() (*api.Buffer, error)         { return p.Alloc(0) }
func (p *mockPool) CompleteSend(_ *api.Buffer)                   {}
func (p *mockPool) BusyFlag() *atomic.Bool                       { return &atomic.Bool{} }
func (p *mockPool) PostRecvBuffers(_ api.QueuePair, _ int) error { return nil }
func (p *mockPool) Replenish(_ api.QueuePair, _ int) error       { return nil }

type mockCQPoller struct{}

func (p *mockCQPoller) Start(_ context.Context) error                              { return nil }
func (p *mockCQPoller) AddCQ(_ api.CompletionQueue, _ api.CompletionHandler) error { return nil }
func (p *mockCQPoller) RemoveCQ(_ api.CompletionQueue) error                       { return nil }
func (p *mockCQPoller) Stats() api.CQPollerStats                                   { return api.CQPollerStats{} }
func (p *mockCQPoller) Close() error                                               { return nil }

// TestSendPathEndToEnd verifies:
// 1. Aggregator acquires buffer from pool
// 2. Packs wire-format headers (cmd + app)
// 3. Connection.Send posts with unique WRID and SendSignaled
// 4. CQ completion → SendCompletionHandler → CompleteSend + OnSendComplete callback
func TestSendPathEndToEnd(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	sendPool := &mockPool{}
	sendCompleteCount := atomic.Int32{}

	cfg := connection.ManagerConfig{
		Verbs:          mockVerbs,
		CMChannel:      rdma.NewMockCMEventChannel(10),
		SendPool:       sendPool,
		RecvPool:       sendPool,
		CQPoller:       &mockCQPoller{},
		QueueDepth:     4,
		OnSendComplete: func() { sendCompleteCount.Add(1) },
	}

	mgr := connection.NewManager(cfg)
	defer mgr.Close()

	conn, err := mgr.Connect(context.Background(), "10.0.0.1:4791")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	c := conn.(*connection.Conn)
	handler := c.GetSendHandler()
	if handler == nil {
		t.Fatal("expected send handler to be wired")
	}

	agg := message.NewAggregator(conn, sendPool, message.AggregatorConfig{
		BufferSize:      4096,
		SendThreshold:   3072,
		EnableAggregate: false,
	})

	payload := []byte("hello end-to-end!")
	if err := agg.Send(payload); err != nil {
		t.Fatalf("Aggregator.Send failed: %v", err)
	}

	// Verify PostSend was called with correct flags
	log := mockVerbs.GetPostSendLog()
	if len(log) != 1 {
		t.Fatalf("expected 1 PostSend, got %d", len(log))
	}
	if log[0].SendFlags&api.SendSignaled == 0 {
		t.Fatal("expected SendSignaled flag on PostSend")
	}
	if log[0].WRID == 0 {
		t.Fatal("expected non-zero WRID")
	}
	if len(log[0].SGList) != 1 {
		t.Fatalf("expected 1 SGE, got %d", len(log[0].SGList))
	}

	// Simulate CQ send completion → should release buffer and call callback
	handler.OnCompletion(&api.WorkCompletion{
		WRID:   log[0].WRID,
		Status: api.WCSuccess,
		Opcode: api.WCSend,
	})

	if sendCompleteCount.Load() != 1 {
		t.Fatalf("expected OnSendComplete called once, got %d", sendCompleteCount.Load())
	}

	sh := handler.(*cq.SendCompletionHandler)
	if sh.InFlight() != 0 {
		t.Fatalf("expected 0 in-flight after completion, got %d", sh.InFlight())
	}

	t.Log("Send path: Aggregator → Pack → Send(Signaled) → CQ completion → Release verified")
}

// TestRecvPathEndToEnd verifies:
// 1. Connection tracks recv buffers
// 2. Simulated recv completion → OnCompletion
// 3. Wire format is unpacked
// 4. Messages delivered to Recv channel
func TestRecvPathEndToEnd(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()

	cfg := connection.ManagerConfig{
		Verbs:      mockVerbs,
		CMChannel:  rdma.NewMockCMEventChannel(10),
		SendPool:   &mockPool{},
		RecvPool:   &mockPool{},
		CQPoller:   &mockCQPoller{},
		QueueDepth: 4,
	}

	mgr := connection.NewManager(cfg)
	defer mgr.Close()

	conn, err := mgr.Connect(context.Background(), "10.0.0.2:4791")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	c := conn.(*connection.Conn)

	// Pack a message in wire format into a recv buffer
	payload := []byte("recv end-to-end!")
	bufData := make([]byte, 4096)
	n := message.PackSingle(bufData, payload)

	wrid := uint64(7777)
	c.TrackRecvBuffer(wrid, bufData)

	// Simulate CQ recv completion
	c.OnCompletion(&api.WorkCompletion{
		WRID:    wrid,
		Status:  api.WCSuccess,
		Opcode:  api.WCRecv,
		ByteLen: uint32(n),
	})

	// Verify message was delivered via Recv()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	received, err := conn.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if received.Length != len(payload) {
		t.Fatalf("expected length %d, got %d", len(payload), received.Length)
	}

	t.Log("Recv path: Track → CQ completion → Unpack → Deliver verified")
}

// TestSendRecvRoundTrip verifies a full send→recv cycle:
// Send side: Aggregator → Pack → Connection.Send → PostSend → CQ → Release
// Recv side: Track buffer → CQ completion → Unpack → Deliver → Recv()
func TestSendRecvRoundTrip(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	sendPool := &mockPool{}
	sendCompleteCount := atomic.Int32{}

	cfg := connection.ManagerConfig{
		Verbs:          mockVerbs,
		CMChannel:      rdma.NewMockCMEventChannel(10),
		SendPool:       sendPool,
		RecvPool:       &mockPool{},
		CQPoller:       &mockCQPoller{},
		QueueDepth:     4,
		OnSendComplete: func() { sendCompleteCount.Add(1) },
	}

	mgr := connection.NewManager(cfg)
	defer mgr.Close()

	conn, err := mgr.Connect(context.Background(), "10.0.0.3:4791")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	c := conn.(*connection.Conn)
	handler := c.GetSendHandler()
	if handler == nil {
		t.Fatal("expected send handler to be set")
	}

	// --- SEND PHASE ---
	agg := message.NewAggregator(conn, sendPool, message.AggregatorConfig{
		BufferSize:      4096,
		SendThreshold:   3072,
		EnableAggregate: false,
	})

	if err := agg.Send([]byte("round-trip")); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	log := mockVerbs.GetPostSendLog()
	if len(log) == 0 {
		t.Fatal("expected at least 1 PostSend")
	}

	// Simulate send completion
	handler.OnCompletion(&api.WorkCompletion{
		WRID:   log[0].WRID,
		Status: api.WCSuccess,
		Opcode: api.WCSend,
	})

	if sendCompleteCount.Load() != 1 {
		t.Fatalf("expected OnSendComplete called once, got %d", sendCompleteCount.Load())
	}

	// --- RECV PHASE ---
	recvBuf := make([]byte, 4096)
	rn := message.PackSingle(recvBuf, []byte("round-trip"))

	recvWRID := uint64(9999)
	c.TrackRecvBuffer(recvWRID, recvBuf)

	c.OnCompletion(&api.WorkCompletion{
		WRID:    recvWRID,
		Status:  api.WCSuccess,
		Opcode:  api.WCRecv,
		ByteLen: uint32(rn),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	msg, err := conn.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv failed: %v", err)
	}
	if msg.Length != len("round-trip") {
		t.Fatalf("expected length %d, got %d", len("round-trip"), msg.Length)
	}

	t.Log("Full round-trip: Send → CQ → Release + Recv → Unpack → Deliver verified")
}

// TestAggregatorBatchWithCQCompletion verifies that batched aggregation works
// with the full send path (pool → pack → send → CQ → release).
func TestAggregatorBatchWithCQCompletion(t *testing.T) {
	mockVerbs := rdma.NewMockVerbs()
	sendPool := &mockPool{}

	cfg := connection.ManagerConfig{
		Verbs:      mockVerbs,
		CMChannel:  rdma.NewMockCMEventChannel(10),
		SendPool:   sendPool,
		RecvPool:   &mockPool{},
		CQPoller:   &mockCQPoller{},
		QueueDepth: 4,
	}

	mgr := connection.NewManager(cfg)
	defer mgr.Close()

	conn, err := mgr.Connect(context.Background(), "10.0.0.4:4791")
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	c := conn.(*connection.Conn)
	handler := c.GetSendHandler()

	agg := message.NewAggregator(conn, sendPool, message.AggregatorConfig{
		BufferSize:      4096,
		SendThreshold:   100,
		EnableAggregate: true,
	})

	// Set isBusy to trigger aggregation
	busy := &atomic.Bool{}
	busy.Store(true)
	agg.SetBusy(busy)

	// Send multiple small messages
	for i := 0; i < 5; i++ {
		if err := agg.Send([]byte("batch-msg")); err != nil {
			t.Fatalf("Send %d failed: %v", i, err)
		}
	}

	if err := agg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	log := mockVerbs.GetPostSendLog()
	if len(log) == 0 {
		t.Fatal("expected at least 1 PostSend after batching")
	}

	// Simulate completions for all sends
	for _, wr := range log {
		handler.OnCompletion(&api.WorkCompletion{
			WRID:   wr.WRID,
			Status: api.WCSuccess,
			Opcode: api.WCSend,
		})
	}

	sh := handler.(*cq.SendCompletionHandler)
	if sh.InFlight() != 0 {
		t.Fatalf("expected 0 in-flight after all completions, got %d", sh.InFlight())
	}

	stats := agg.Stats()
	if stats.Sent < 5 {
		t.Fatalf("expected at least 5 messages sent, got %d", stats.Sent)
	}

	t.Logf("Aggregator batch: %d flushes, %d messages, %d PostSends",
		stats.Flushed, stats.Sent, len(log))
}
