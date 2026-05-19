//go:build mock || !cgo

package cq

import (
	"context"
	"sync"
	"sync/atomic"
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
