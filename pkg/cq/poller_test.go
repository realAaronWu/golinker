//go:build mock || !cgo

package cq

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/config"
)

// --- Mock types for tests ---

type mockCQ struct {
	id   int
	size int
}

func (m *mockCQ) Handle() unsafe.Pointer { return unsafe.Pointer(m) }
func (m *mockCQ) Size() int              { return m.size }
func (m *mockCQ) CompChannelFD() int     { return -1 }
func (m *mockCQ) ReqNotify() error       { return nil }
func (m *mockCQ) AckEvents(nevents uint) {}

type mockHandler struct {
	mu          sync.Mutex
	completions []*api.WorkCompletion
	errors      []error
}

func (h *mockHandler) OnCompletion(wc *api.WorkCompletion) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.completions = append(h.completions, wc)
}

func (h *mockHandler) OnError(wc *api.WorkCompletion, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.errors = append(h.errors, err)
}

func (h *mockHandler) getCompletions() []*api.WorkCompletion {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]*api.WorkCompletion, len(h.completions))
	copy(result, h.completions)
	return result
}

func (h *mockHandler) getErrors() []error {
	h.mu.Lock()
	defer h.mu.Unlock()
	result := make([]error, len(h.errors))
	copy(result, h.errors)
	return result
}

// mockVerbs implements api.Verbs for pool tests.
type mockVerbs struct {
	mu      sync.Mutex
	cqCount int
}

func (m *mockVerbs) OpenDevice(devName string) error                  { return nil }
func (m *mockVerbs) AllocPD() (api.ProtectionDomain, error)           { return nil, nil }
func (m *mockVerbs) CreateCQ(size int) (api.CompletionQueue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cqCount++
	return &mockCQ{id: m.cqCount, size: size}, nil
}
func (m *mockVerbs) CreateQP(pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, error) {
	return nil, nil
}
func (m *mockVerbs) RegMR(pd api.ProtectionDomain, addr unsafe.Pointer, length int, access api.AccessFlags) (api.MemoryRegion, error) {
	return nil, nil
}
func (m *mockVerbs) CreateCQWithChannel(size int) (api.CompletionQueue, error) {
	return m.CreateCQ(size)
}
func (m *mockVerbs) DeregMR(mr api.MemoryRegion) error                { return nil }
func (m *mockVerbs) PostSend(qp api.QueuePair, wr *api.SendWR) error  { return nil }
func (m *mockVerbs) PostRecv(qp api.QueuePair, wr *api.RecvWR) error  { return nil }
func (m *mockVerbs) Close() error                                      { return nil }

// --- Tests ---

func TestPollerBusyMode(t *testing.T) {
	// Create a channel to inject completions
	injected := make(chan []api.WorkCompletion, 10)

	pollFn := func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		select {
		case wcs := <-injected:
			return wcs, nil
		default:
			return nil, nil
		}
	}

	cfg := PollerConfig{
		PollMode:     config.PollModeBusy,
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc:     pollFn,
	}

	poller := NewPoller(cfg)
	handler := &mockHandler{}
	cq := &mockCQ{id: 1, size: 128}

	if err := poller.AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}

	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Inject a completion
	injected <- []api.WorkCompletion{
		{WRID: 42, Status: api.WCSuccess, Opcode: api.WCSend, ByteLen: 100},
	}

	// Wait for it to be processed
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for completion")
		default:
		}
		if len(handler.getCompletions()) > 0 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	completions := handler.getCompletions()
	if len(completions) != 1 {
		t.Fatalf("expected 1 completion, got %d", len(completions))
	}
	if completions[0].WRID != 42 {
		t.Errorf("expected WRID 42, got %d", completions[0].WRID)
	}
	if completions[0].ByteLen != 100 {
		t.Errorf("expected ByteLen 100, got %d", completions[0].ByteLen)
	}

	if err := poller.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPollerSmartMode(t *testing.T) {
	var pollCount atomic.Int64

	pollFn := func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		pollCount.Add(1)
		return nil, nil // always empty
	}

	cfg := PollerConfig{
		PollMode:     config.PollModeSmart,
		MaxBatchSize: 32,
		SpinCount:    100, // Low spin count for faster test
		PollFunc:     pollFn,
	}

	poller := NewPoller(cfg)
	handler := &mockHandler{}
	cq := &mockCQ{id: 1, size: 128}

	if err := poller.AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}

	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait until at least SpinCount polls have happened (poller enters event mode)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for spin count to be reached")
		default:
		}
		if pollCount.Load() >= int64(cfg.SpinCount) {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// After SpinCount empty polls, smart mode should block on eventCh.
	// Record current count, wait a bit, and verify it didn't increase
	// (because it's now truly blocked in event mode).
	countAfterSpin := pollCount.Load()
	time.Sleep(50 * time.Millisecond)
	countAfterWait := pollCount.Load()

	// In event mode (blocking), no new polls should happen without Notify()
	delta := countAfterWait - countAfterSpin
	if delta > 5 {
		t.Errorf("smart mode didn't block after empty polls: delta=%d (expected <= 5)", delta)
	}

	// Now notify and verify it resumes polling
	poller.Notify()
	time.Sleep(20 * time.Millisecond)
	countAfterNotify := pollCount.Load()
	if countAfterNotify <= countAfterWait {
		t.Error("expected polling to resume after Notify()")
	}

	stats := poller.Stats()
	if stats.EmptyPolls == 0 {
		t.Error("expected non-zero EmptyPolls in smart mode")
	}

	if err := poller.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPollerAddRemoveCQ(t *testing.T) {
	cfg := PollerConfig{
		PollMode:     config.PollModeBusy,
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc: func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
			return nil, nil
		},
	}

	poller := NewPoller(cfg)
	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Concurrent add/remove
	const numGoroutines = 10
	var wg sync.WaitGroup

	cqs := make([]*mockCQ, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		cqs[i] = &mockCQ{id: i, size: 64}
	}

	// Add all CQs concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			handler := &mockHandler{}
			_ = poller.AddCQ(cqs[idx], handler)
		}(i)
	}
	wg.Wait()

	if poller.NumCQs() != numGoroutines {
		t.Errorf("expected %d CQs, got %d", numGoroutines, poller.NumCQs())
	}

	// Remove all CQs concurrently
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_ = poller.RemoveCQ(cqs[idx])
		}(i)
	}
	wg.Wait()

	if poller.NumCQs() != 0 {
		t.Errorf("expected 0 CQs after removal, got %d", poller.NumCQs())
	}

	// Test error cases
	if err := poller.AddCQ(nil, &mockHandler{}); err == nil {
		t.Error("expected error for nil CQ")
	}
	if err := poller.AddCQ(&mockCQ{id: 99, size: 1}, nil); err == nil {
		t.Error("expected error for nil handler")
	}
	if err := poller.RemoveCQ(nil); err == nil {
		t.Error("expected error for nil CQ removal")
	}
	if err := poller.RemoveCQ(&mockCQ{id: 999, size: 1}); err == nil {
		t.Error("expected error for unregistered CQ removal")
	}

	if err := poller.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPollerStats(t *testing.T) {
	callCount := 0
	pollFn := func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		callCount++
		if callCount <= 3 {
			return []api.WorkCompletion{
				{WRID: uint64(callCount), Status: api.WCSuccess, Opcode: api.WCSend, ByteLen: 64},
				{WRID: uint64(callCount + 100), Status: api.WCSuccess, Opcode: api.WCRecv, ByteLen: 128},
			}, nil
		}
		return nil, nil
	}

	cfg := PollerConfig{
		PollMode:     config.PollModeEvent, // event mode so we control exactly when polls happen
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc:     pollFn,
	}

	poller := NewPoller(cfg)
	handler := &mockHandler{}
	cq := &mockCQ{id: 1, size: 128}

	if err := poller.AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}

	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Trigger 3 poll events that return completions
	for i := 0; i < 3; i++ {
		poller.Notify()
		time.Sleep(10 * time.Millisecond)
	}

	// Trigger 2 empty polls
	for i := 0; i < 2; i++ {
		poller.Notify()
		time.Sleep(10 * time.Millisecond)
	}

	stats := poller.Stats()

	if stats.Completions < 6 {
		t.Errorf("expected at least 6 completions, got %d", stats.Completions)
	}
	if stats.PollCycles < 5 {
		t.Errorf("expected at least 5 poll cycles, got %d", stats.PollCycles)
	}
	if stats.EmptyPolls < 2 {
		t.Errorf("expected at least 2 empty polls, got %d", stats.EmptyPolls)
	}
	if stats.AvgBatchSize <= 0 {
		t.Errorf("expected positive AvgBatchSize, got %f", stats.AvgBatchSize)
	}

	if err := poller.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPoolAssign(t *testing.T) {
	verbs := &mockVerbs{}
	cfg := PollerConfig{
		PollMode:     config.PollModeEvent,
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc: func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
			return nil, nil
		},
	}

	pool, err := NewPool(verbs, 3, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}

	// Assign 6 CQs - should distribute evenly across 3 pollers
	pollerCounts := make(map[api.CQPoller]int)
	for i := 0; i < 6; i++ {
		poller, cq, err := pool.Assign()
		if err != nil {
			t.Fatalf("Assign %d failed: %v", i, err)
		}
		if cq == nil {
			t.Fatalf("Assign %d returned nil CQ", i)
		}
		// Register the CQ with a handler (simulating real usage)
		handler := &mockHandler{}
		if err := poller.(*Poller).AddCQ(cq, handler); err != nil {
			t.Fatalf("AddCQ %d failed: %v", i, err)
		}
		pollerCounts[poller]++
	}

	// Each poller should have 2 CQs (6 / 3 = 2)
	for poller, count := range pollerCounts {
		if count != 2 {
			t.Errorf("poller %v has %d CQs, expected 2", poller, count)
		}
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPoolRelease(t *testing.T) {
	verbs := &mockVerbs{}
	cfg := PollerConfig{
		PollMode:     config.PollModeEvent,
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc: func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
			return nil, nil
		},
	}

	pool, err := NewPool(verbs, 2, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}

	poller, cq, err := pool.Assign()
	if err != nil {
		t.Fatalf("Assign failed: %v", err)
	}

	handler := &mockHandler{}
	if err := poller.(*Poller).AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}

	// Verify poller has 1 CQ
	p := poller.(*Poller)
	if p.NumCQs() != 1 {
		t.Errorf("expected 1 CQ, got %d", p.NumCQs())
	}

	// Release the CQ
	if err := pool.Release(cq); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	// Verify poller has 0 CQs
	if p.NumCQs() != 0 {
		t.Errorf("expected 0 CQs after release, got %d", p.NumCQs())
	}

	// Release again should fail
	if err := pool.Release(cq); err == nil {
		t.Error("expected error for double release")
	}

	if err := pool.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestChannelHandler(t *testing.T) {
	handler := NewChannelHandler(4)

	// Send completions
	for i := 0; i < 4; i++ {
		wc := &api.WorkCompletion{
			WRID:    uint64(i),
			Status:  api.WCSuccess,
			Opcode:  api.WCSend,
			ByteLen: uint32(i * 10),
		}
		handler.OnCompletion(wc)
	}

	// Verify all received
	for i := 0; i < 4; i++ {
		select {
		case wc := <-handler.Completions():
			if wc.WRID != uint64(i) {
				t.Errorf("expected WRID %d, got %d", i, wc.WRID)
			}
		default:
			t.Fatalf("expected completion %d but channel was empty", i)
		}
	}

	// Overflow: channel is now empty but limited to 4
	for i := 0; i < 6; i++ {
		handler.OnCompletion(&api.WorkCompletion{WRID: uint64(100 + i)})
	}

	// Should have 4 in channel and 2 dropped
	if handler.Dropped() != 2 {
		t.Errorf("expected 2 dropped, got %d", handler.Dropped())
	}

	// Test error channel
	handler.OnError(nil, nil)
	select {
	case <-handler.Errors():
		// ok
	default:
		t.Error("expected error in channel")
	}
}

func TestPollerClose(t *testing.T) {
	pollFn := func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		return nil, nil
	}

	cfg := PollerConfig{
		PollMode:     config.PollModeBusy,
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc:     pollFn,
	}

	poller := NewPoller(cfg)
	handler := &mockHandler{}
	cq := &mockCQ{id: 1, size: 128}

	if err := poller.AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}

	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Let it run briefly
	time.Sleep(10 * time.Millisecond)

	// Close should return promptly
	done := make(chan struct{})
	go func() {
		if err := poller.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return within 5 seconds")
	}

	// Double start should fail
	poller2 := NewPoller(cfg)
	_ = poller2.Start(context.Background())
	if err := poller2.Start(context.Background()); err == nil {
		t.Error("expected error for double start")
	}
	_ = poller2.Close()
}

// --- Event-Driven Mode Tests ---

// eventMockCQ implements api.CompletionQueue with a pipe-based comp channel.
type eventMockCQ struct {
	id            int
	size          int
	compChannelFD int
}

func (m *eventMockCQ) Handle() unsafe.Pointer { return unsafe.Pointer(m) }
func (m *eventMockCQ) Size() int              { return m.size }
func (m *eventMockCQ) CompChannelFD() int     { return m.compChannelFD }
func (m *eventMockCQ) ReqNotify() error       { return nil }
func (m *eventMockCQ) AckEvents(nevents uint) {}

func TestPollerEventModeWithCompChannel(t *testing.T) {
	// Create a pipe to simulate a completion channel FD.
	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		t.Fatalf("pipe: %v", err)
	}
	readFD, writeFD := fds[0], fds[1]
	defer syscall.Close(writeFD)

	// Track completions injected after wake-up.
	var wakeCount atomic.Int64
	pollFn := func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		n := wakeCount.Add(1)
		if n <= 3 {
			return []api.WorkCompletion{
				{WRID: uint64(n), Status: api.WCSuccess, Opcode: api.WCRecv, ByteLen: 64},
			}, nil
		}
		return nil, nil
	}

	cfg := PollerConfig{
		PollMode:     config.PollModeEvent,
		MaxBatchSize: 32,
		SpinCount:    1024,
		PollFunc:     pollFn,
	}

	poller := NewPoller(cfg)
	handler := &mockHandler{}
	cq := &eventMockCQ{id: 1, size: 128, compChannelFD: readFD}

	if err := poller.AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}

	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Give the event waiter time to arm and drain initial completions.
	time.Sleep(50 * time.Millisecond)

	// Record poll count before wake.
	countBefore := wakeCount.Load()

	// Wait a bit — the poller should be parked (no new polls).
	time.Sleep(50 * time.Millisecond)
	countAfterWait := wakeCount.Load()
	if countAfterWait-countBefore > 2 {
		t.Errorf("poller should be parked but poll count increased: before=%d after=%d", countBefore, countAfterWait)
	}

	// Write to the pipe to simulate a CQ event — wake the poller.
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	if _, err := syscall.Write(writeFD, buf); err != nil {
		t.Fatalf("write to pipe: %v", err)
	}

	// Wait for the poller to wake and poll.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for poller to wake after pipe write")
		default:
		}
		if wakeCount.Load() > countAfterWait {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	if err := poller.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestPollerSmartModeWithCompChannel(t *testing.T) {
	// Create a pipe to simulate a completion channel FD.
	var fds [2]int
	if err := syscall.Pipe(fds[:]); err != nil {
		t.Fatalf("pipe: %v", err)
	}
	readFD, writeFD := fds[0], fds[1]
	defer syscall.Close(writeFD)

	// Phase tracking: completions during the first few polls, then empty.
	var pollCount atomic.Int64
	injectedCount := int64(5) // Return completions for the first 5 polls

	pollFn := func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		n := pollCount.Add(1)
		if n <= injectedCount {
			return []api.WorkCompletion{
				{WRID: uint64(n), Status: api.WCSuccess, Opcode: api.WCRecv, ByteLen: 32},
			}, nil
		}
		return nil, nil
	}

	cfg := PollerConfig{
		PollMode:     config.PollModeSmart,
		MaxBatchSize: 32,
		SpinCount:    50, // Low spin count for faster test
		PollFunc:     pollFn,
	}

	poller := NewPoller(cfg)
	handler := &mockHandler{}
	cq := &eventMockCQ{id: 1, size: 128, compChannelFD: readFD}

	if err := poller.AddCQ(cq, handler); err != nil {
		t.Fatalf("AddCQ failed: %v", err)
	}
	if err := poller.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the smart loop to exhaust injected completions and spin down.
	// After injectedCount + SpinCount empty polls, it should park.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timeout waiting for smart mode to spin down")
		default:
		}
		if pollCount.Load() >= injectedCount+int64(cfg.SpinCount)+1 {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Now the poller should be parked. Record count and wait.
	countBeforePark := pollCount.Load()
	time.Sleep(50 * time.Millisecond)
	countAfterPark := pollCount.Load()
	delta := countAfterPark - countBeforePark
	if delta > 3 {
		t.Errorf("smart mode should be parked: delta=%d", delta)
	}

	// Wake by writing to the pipe.
	buf := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	if _, err := syscall.Write(writeFD, buf); err != nil {
		t.Fatalf("write to pipe: %v", err)
	}

	// Verify it wakes and resumes busy-poll.
	time.Sleep(50 * time.Millisecond)
	countAfterWake := pollCount.Load()
	if countAfterWake <= countAfterPark {
		t.Error("smart mode should have resumed after wake")
	}

	if err := poller.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// --- SendCompletionHandler Tests ---

type mockSendPool struct {
	mu        sync.Mutex
	completed []*api.Buffer
}

func (p *mockSendPool) Alloc(size int) (*api.Buffer, error) {
	data := make([]byte, size)
	return &api.Buffer{Addr: unsafe.Pointer(&data[0]), Length: size}, nil
}
func (p *mockSendPool) Free(buf *api.Buffer)         {}
func (p *mockSendPool) Stats() api.BufferPoolStats    { return api.BufferPoolStats{} }
func (p *mockSendPool) Close() error                  { return nil }
func (p *mockSendPool) AcquireForSend() (*api.Buffer, error) { return p.Alloc(4096) }
func (p *mockSendPool) CompleteSend(buf *api.Buffer) {
	p.mu.Lock()
	p.completed = append(p.completed, buf)
	p.mu.Unlock()
}

func TestSendCompletionHandlerBasic(t *testing.T) {
	pool := &mockSendPool{}
	callbackCount := atomic.Int32{}
	h := NewSendCompletionHandler(pool, func() { callbackCount.Add(1) })

	// Track a send
	data := make([]byte, 64)
	buf := &api.Buffer{Addr: unsafe.Pointer(&data[0]), Length: 64}
	h.TrackSend(1, buf)

	if h.InFlight() != 1 {
		t.Fatalf("expected 1 in-flight, got %d", h.InFlight())
	}

	// Simulate completion
	h.OnCompletion(&api.WorkCompletion{WRID: 1, Status: api.WCSuccess})

	if h.InFlight() != 0 {
		t.Fatalf("expected 0 in-flight after completion, got %d", h.InFlight())
	}

	pool.mu.Lock()
	if len(pool.completed) != 1 {
		t.Fatalf("expected 1 CompleteSend call, got %d", len(pool.completed))
	}
	if pool.completed[0] != buf {
		t.Fatal("CompleteSend called with wrong buffer")
	}
	pool.mu.Unlock()

	if callbackCount.Load() != 1 {
		t.Fatalf("expected onComplete callback called once, got %d", callbackCount.Load())
	}
}

func TestSendCompletionHandlerError(t *testing.T) {
	pool := &mockSendPool{}
	h := NewSendCompletionHandler(pool, nil)

	data := make([]byte, 64)
	buf := &api.Buffer{Addr: unsafe.Pointer(&data[0]), Length: 64}
	h.TrackSend(42, buf)

	// Simulate error completion
	h.OnError(&api.WorkCompletion{WRID: 42}, nil)

	if h.InFlight() != 0 {
		t.Fatalf("expected 0 in-flight after error, got %d", h.InFlight())
	}

	pool.mu.Lock()
	if len(pool.completed) != 1 {
		t.Fatalf("expected 1 CompleteSend call on error, got %d", len(pool.completed))
	}
	pool.mu.Unlock()
}

func TestSendCompletionHandlerUnknownWRID(t *testing.T) {
	pool := &mockSendPool{}
	h := NewSendCompletionHandler(pool, nil)

	// Completion for an untracked WRID — should not panic or call CompleteSend
	h.OnCompletion(&api.WorkCompletion{WRID: 999, Status: api.WCSuccess})

	pool.mu.Lock()
	if len(pool.completed) != 0 {
		t.Fatalf("expected 0 CompleteSend calls for unknown WRID, got %d", len(pool.completed))
	}
	pool.mu.Unlock()
}

func TestSendCompletionHandlerMultipleInFlight(t *testing.T) {
	pool := &mockSendPool{}
	callbackCount := atomic.Int32{}
	h := NewSendCompletionHandler(pool, func() { callbackCount.Add(1) })

	bufs := make([]*api.Buffer, 5)
	for i := range bufs {
		data := make([]byte, 64)
		bufs[i] = &api.Buffer{Addr: unsafe.Pointer(&data[0]), Length: 64}
		h.TrackSend(uint64(i+1), bufs[i])
	}

	if h.InFlight() != 5 {
		t.Fatalf("expected 5 in-flight, got %d", h.InFlight())
	}

	// Complete out of order
	h.OnCompletion(&api.WorkCompletion{WRID: 3, Status: api.WCSuccess})
	h.OnCompletion(&api.WorkCompletion{WRID: 1, Status: api.WCSuccess})
	h.OnCompletion(&api.WorkCompletion{WRID: 5, Status: api.WCSuccess})

	if h.InFlight() != 2 {
		t.Fatalf("expected 2 in-flight, got %d", h.InFlight())
	}

	pool.mu.Lock()
	if len(pool.completed) != 3 {
		t.Fatalf("expected 3 CompleteSend calls, got %d", len(pool.completed))
	}
	pool.mu.Unlock()

	if callbackCount.Load() != 3 {
		t.Fatalf("expected 3 callbacks, got %d", callbackCount.Load())
	}
}
