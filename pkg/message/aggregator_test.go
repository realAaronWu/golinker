package message

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// --- Mock implementations ---

type mockConn struct {
	sendLog []*api.Message
	mu      sync.Mutex
}

func (m *mockConn) Send(msg *api.Message) error {
	m.mu.Lock()
	m.sendLog = append(m.sendLog, msg)
	m.mu.Unlock()
	return nil
}

func (m *mockConn) ID() uint64                                         { return 1 }
func (m *mockConn) RemoteAddr() string                                 { return "mock://remote" }
func (m *mockConn) State() api.ConnectionState                         { return api.StateConnected }
func (m *mockConn) SendPayload(_ []byte) error                         { return nil }
func (m *mockConn) Recv(_ context.Context) (*api.Message, error)       { return nil, nil }
func (m *mockConn) Close() error                                       { return nil }
func (m *mockConn) OnStateChange(_ func(old, new api.ConnectionState)) {}

func (m *mockConn) getSendLog() []*api.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	log := make([]*api.Message, len(m.sendLog))
	copy(log, m.sendLog)
	return log
}

type mockSendPool struct {
	bufSize int
	mu      sync.Mutex
	allocs  int
	frees   int
}

func newMockSendPool(bufSize int) *mockSendPool {
	return &mockSendPool{bufSize: bufSize}
}

func (p *mockSendPool) AcquireForSend() (*api.Buffer, error) {
	p.mu.Lock()
	p.allocs++
	p.mu.Unlock()
	mem := make([]byte, p.bufSize)
	return &api.Buffer{
		Addr:   unsafe.Pointer(&mem[0]),
		Length: p.bufSize,
		PoolID: 0,
	}, nil
}

func (p *mockSendPool) CompleteSend(buf *api.Buffer) {
	p.mu.Lock()
	p.frees++
	p.mu.Unlock()
}

func (p *mockSendPool) Alloc(size int) (*api.Buffer, error) {
	return p.AcquireForSend()
}

func (p *mockSendPool) Free(buf *api.Buffer) {
	p.CompleteSend(buf)
}

func (p *mockSendPool) Stats() api.BufferPoolStats {
	return api.BufferPoolStats{}
}

func (p *mockSendPool) Close() error {
	return nil
}

func (p *mockSendPool) BusyFlag() *atomic.Bool {
	return &atomic.Bool{}
}

// --- Tests ---

func TestImmediateSendWhenNotBusy(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(false) // not busy

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   DefaultSendThreshold,
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	data := []byte("hello world")
	err := agg.Send(data)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	log := conn.getSendLog()
	if len(log) != 1 {
		t.Fatalf("expected 1 send, got %d", len(log))
	}

	// Verify the message can be unpacked
	msg := log[0]
	dest := unsafe.Slice((*byte)(msg.Buffer.Addr), msg.Buffer.Length)
	msgs, err := UnpackBatch(dest, msg.Length)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message in batch, got %d", len(msgs))
	}
	if !bytes.Equal(msgs[0], data) {
		t.Fatalf("message mismatch: got %q, want %q", msgs[0], data)
	}
}

func TestAggregateWhenBusy(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(true) // busy

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   DefaultSendThreshold,
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	// Simulate an ongoing send so idle trigger doesn't fire
	agg.ongoingSends.Store(1)

	data := []byte("small msg")
	err := agg.Send(data)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// With ongoing sends and small message, it should be pending (not yet flushed)
	stats := agg.Stats()
	if stats.PendingCount != 1 {
		t.Fatalf("expected 1 pending message, got %d", stats.PendingCount)
	}

	// No send should have happened yet (message is aggregated)
	log := conn.getSendLog()
	if len(log) != 0 {
		t.Fatalf("expected 0 sends (aggregated), got %d", len(log))
	}
}

func TestFlushOnThreshold(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(true)

	// Use small threshold to trigger easily
	threshold := 100
	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   threshold,
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	// Simulate ongoing send to prevent idle flush
	agg.ongoingSends.Store(1)

	// Send enough data to exceed threshold
	// CmdHeaderSize(12) + AppHeaderSize(12) + 50 = 74 per single message batch
	// 2 messages = 12 + (12+50)*2 = 136 > 100 threshold
	data := bytes.Repeat([]byte("x"), 50)

	err := agg.Send(data)
	if err != nil {
		t.Fatalf("Send 1 failed: %v", err)
	}
	// First message: CmdHeaderSize + (AppHeaderSize+50) = 12+62 = 74, below threshold
	log := conn.getSendLog()
	if len(log) != 0 {
		t.Fatalf("expected 0 sends after first msg, got %d", len(log))
	}

	err = agg.Send(data)
	if err != nil {
		t.Fatalf("Send 2 failed: %v", err)
	}
	// Second message: CmdHeaderSize + 2*(AppHeaderSize+50) = 12+124 = 136 > 100
	log = conn.getSendLog()
	if len(log) != 1 {
		t.Fatalf("expected 1 flush (threshold), got %d", len(log))
	}
}

func TestFlushOnOverflow(t *testing.T) {
	conn := &mockConn{}
	bufSize := 128
	pool := newMockSendPool(bufSize)
	busy := &atomic.Bool{}
	busy.Store(true)

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      bufSize,
		SendThreshold:   bufSize, // set threshold = bufSize so only overflow triggers
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	// Simulate ongoing send to prevent idle flush
	agg.ongoingSends.Store(1)

	// Fill most of the buffer: CmdHeaderSize(12) + AppHeaderSize(12) + 100 = 124
	data1 := bytes.Repeat([]byte("a"), 100)
	err := agg.Send(data1)
	if err != nil {
		t.Fatalf("Send 1 failed: %v", err)
	}

	// This should trigger overflow: 124 + (12 + 50) = 186 > 128
	data2 := bytes.Repeat([]byte("b"), 50)
	err = agg.Send(data2)
	if err != nil {
		t.Fatalf("Send 2 failed: %v", err)
	}

	// First message should have been flushed alone (overflow trigger)
	log := conn.getSendLog()
	if len(log) < 1 {
		t.Fatalf("expected at least 1 flush (overflow), got %d", len(log))
	}

	// Verify first flush contains data1
	msg := log[0]
	dest := unsafe.Slice((*byte)(msg.Buffer.Addr), msg.Buffer.Length)
	msgs, err := UnpackBatch(dest, msg.Length)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}
	if len(msgs) != 1 || !bytes.Equal(msgs[0], data1) {
		t.Fatalf("overflow flush should contain data1")
	}
}

func TestFlushOnIdle(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(true)

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   DefaultSendThreshold,
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	// Start with an ongoing send
	agg.ongoingSends.Store(1)

	data := []byte("idle test message")
	err := agg.Send(data)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Message should be pending
	log := conn.getSendLog()
	if len(log) != 0 {
		t.Fatalf("expected 0 sends before idle, got %d", len(log))
	}

	// Simulate send completion (ongoingSends -> 0 triggers idle flush)
	agg.OnSendComplete()

	log = conn.getSendLog()
	if len(log) != 1 {
		t.Fatalf("expected 1 send after idle trigger, got %d", len(log))
	}

	// Verify the flushed data
	msg := log[0]
	dest := unsafe.Slice((*byte)(msg.Buffer.Addr), msg.Buffer.Length)
	msgs, err := UnpackBatch(dest, msg.Length)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}
	if len(msgs) != 1 || !bytes.Equal(msgs[0], data) {
		t.Fatalf("idle flush data mismatch")
	}
}

func TestPackUnpackBatch(t *testing.T) {
	messages := [][]byte{
		[]byte("message one"),
		[]byte("message two"),
		[]byte("msg three"),
		[]byte("four"),
		[]byte("fifth message here"),
	}

	buf := make([]byte, 1024)
	n := PackBatch(buf, messages)

	unpacked, err := UnpackBatch(buf, n)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}
	if len(unpacked) != len(messages) {
		t.Fatalf("expected %d messages, got %d", len(messages), len(unpacked))
	}
	for i := range messages {
		if !bytes.Equal(unpacked[i], messages[i]) {
			t.Fatalf("message %d mismatch: got %q, want %q", i, unpacked[i], messages[i])
		}
	}
}

func TestPackSingle(t *testing.T) {
	msg := []byte("single message test data")
	buf := make([]byte, 256)
	n := PackSingle(buf, msg)

	unpacked, err := UnpackBatch(buf, n)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}
	if len(unpacked) != 1 {
		t.Fatalf("expected 1 message, got %d", len(unpacked))
	}
	if !bytes.Equal(unpacked[0], msg) {
		t.Fatalf("message mismatch: got %q, want %q", unpacked[0], msg)
	}
}

func TestAggregatorDisabled(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(true) // even though busy

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   DefaultSendThreshold,
		EnableAggregate: false, // aggregation disabled
	})
	agg.SetBusy(busy)

	data := []byte("should be immediate")
	err := agg.Send(data)
	if err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Should be sent immediately despite being busy
	log := conn.getSendLog()
	if len(log) != 1 {
		t.Fatalf("expected 1 immediate send, got %d", len(log))
	}

	// Verify message content
	msg := log[0]
	dest := unsafe.Slice((*byte)(msg.Buffer.Addr), msg.Buffer.Length)
	msgs, err := UnpackBatch(dest, msg.Length)
	if err != nil {
		t.Fatalf("UnpackBatch failed: %v", err)
	}
	if len(msgs) != 1 || !bytes.Equal(msgs[0], data) {
		t.Fatalf("message mismatch")
	}
}

func TestConcurrentSend(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(false) // immediate mode for simplicity

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   DefaultSendThreshold,
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	var wg sync.WaitGroup
	goroutines := 32
	msgsPerGoroutine := 10

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < msgsPerGoroutine; j++ {
				data := []byte("concurrent msg")
				if err := agg.Send(data); err != nil {
					t.Errorf("goroutine %d send %d failed: %v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all messages were sent
	log := conn.getSendLog()
	expected := goroutines * msgsPerGoroutine
	if len(log) != expected {
		t.Fatalf("expected %d sends, got %d", expected, len(log))
	}
}

func TestStats(t *testing.T) {
	conn := &mockConn{}
	pool := newMockSendPool(DefaultBufferSize)
	busy := &atomic.Bool{}
	busy.Store(false) // immediate mode

	agg := NewAggregator(conn, pool, AggregatorConfig{
		BufferSize:      DefaultBufferSize,
		SendThreshold:   DefaultSendThreshold,
		EnableAggregate: true,
	})
	agg.SetBusy(busy)

	// Send 3 immediate messages
	for i := 0; i < 3; i++ {
		err := agg.Send([]byte("test"))
		if err != nil {
			t.Fatalf("Send %d failed: %v", i, err)
		}
	}

	stats := agg.Stats()
	if stats.Sent != 3 {
		t.Fatalf("expected Sent=3, got %d", stats.Sent)
	}
	if stats.Flushed != 0 {
		t.Fatalf("expected Flushed=0 (immediate sends), got %d", stats.Flushed)
	}
	if stats.PendingCount != 0 {
		t.Fatalf("expected PendingCount=0, got %d", stats.PendingCount)
	}

	// Now test aggregate path with flush
	busy.Store(true)
	agg.ongoingSends.Store(1) // prevent idle flush

	// Use a small threshold so flush is triggered
	agg.mu.Lock()
	agg.sendThreshold = 50
	agg.mu.Unlock()

	// Send message that exceeds threshold: CmdHeaderSize(12) + AppHeaderSize(12) + 50 = 74 > 50
	err := agg.Send(bytes.Repeat([]byte("x"), 50))
	if err != nil {
		t.Fatalf("aggregate Send failed: %v", err)
	}

	stats = agg.Stats()
	if stats.Flushed != 1 {
		t.Fatalf("expected Flushed=1, got %d", stats.Flushed)
	}
	if stats.Sent != 4 {
		t.Fatalf("expected Sent=4, got %d", stats.Sent)
	}
}
