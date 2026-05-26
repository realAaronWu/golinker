// Package cq provides completion queue polling for RDMA work completions.
package cq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/config"
)

// PollFunc is the function signature for polling completions from a CQ.
// In production, this calls the C hot-path. In tests, it's mocked.
type PollFunc func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error)

// PollerConfig holds configuration for a CQ poller.
type PollerConfig struct {
	PollMode     config.PollMode
	MaxBatchSize int
	SpinCount    int
	PollFunc     PollFunc // if nil, uses default (which would call C hot-path)
}

// DefaultPollerConfig returns a PollerConfig with sensible defaults.
func DefaultPollerConfig() PollerConfig {
	return PollerConfig{
		PollMode:     config.PollModeBusy,
		MaxBatchSize: 32,
		SpinCount:    1024,
	}
}

// cqEntry holds a CQ and its associated handler.
type cqEntry struct {
	cq      api.CompletionQueue
	handler api.CompletionHandler
	file    *os.File             // os.NewFile wrapper for comp channel FD (nil if no channel)
	cancel  context.CancelFunc   // cancels the per-CQ event waiter goroutine
}

// Poller implements api.CQPoller. It polls one or more completion queues
// and dispatches work completions to registered handlers.
type Poller struct {
	cfg PollerConfig

	mu      sync.RWMutex
	entries map[api.CompletionQueue]*cqEntry

	// stats tracked with atomics for lock-free reads
	pollCycles  atomic.Uint64
	completions atomic.Uint64
	emptyPolls  atomic.Uint64
	errors      atomic.Uint64

	// lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	started atomic.Bool
	wg      sync.WaitGroup // tracks event waiter goroutines

	// eventCh is used for event-driven mode; signals when completions may be ready
	eventCh chan struct{}
}

// NewPoller creates a new CQ poller with the given configuration.
func NewPoller(cfg PollerConfig) *Poller {
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 32
	}
	if cfg.SpinCount <= 0 {
		cfg.SpinCount = 1024
	}
	return &Poller{
		cfg:     cfg,
		entries: make(map[api.CompletionQueue]*cqEntry),
		done:    make(chan struct{}),
		eventCh: make(chan struct{}, 1),
	}
}

// Start begins the poll loop. It blocks until ctx is cancelled or Close is called.
func (p *Poller) Start(ctx context.Context) error {
	if p.started.Swap(true) {
		return errors.New("cq: poller already started")
	}

	ctx, p.cancel = context.WithCancel(ctx)
	p.ctx = ctx

	go p.pollLoop(ctx)
	return nil
}

// AddCQ registers a CQ with this poller.
func (p *Poller) AddCQ(cq api.CompletionQueue, handler api.CompletionHandler) error {
	if cq == nil {
		return errors.New("cq: nil CompletionQueue")
	}
	if handler == nil {
		return errors.New("cq: nil CompletionHandler")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.entries[cq]; exists {
		return fmt.Errorf("cq: CQ already registered")
	}
	entry := &cqEntry{cq: cq, handler: handler}
	p.entries[cq] = entry

	// In event/smart mode, start the event waiter for this CQ if the poller is running.
	if p.started.Load() && p.ctx != nil &&
		(p.cfg.PollMode == config.PollModeEvent || p.cfg.PollMode == config.PollModeSmart) {
		p.startEventWaiter(entry)
	}
	return nil
}

// RemoveCQ unregisters a CQ from this poller.
func (p *Poller) RemoveCQ(cq api.CompletionQueue) error {
	if cq == nil {
		return errors.New("cq: nil CompletionQueue")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	entry, exists := p.entries[cq]
	if !exists {
		return fmt.Errorf("cq: CQ not registered")
	}
	// Stop the event waiter for this CQ.
	if entry.cancel != nil {
		entry.cancel()
	}
	if entry.file != nil {
		entry.file.Close()
		entry.file = nil
	}
	delete(p.entries, cq)
	return nil
}

// Stats returns a snapshot of the polling statistics.
func (p *Poller) Stats() api.CQPollerStats {
	completions := p.completions.Load()
	cycles := p.pollCycles.Load()

	var avgBatch float64
	if cycles > 0 && completions > 0 {
		nonEmpty := cycles - p.emptyPolls.Load()
		if nonEmpty > 0 {
			avgBatch = float64(completions) / float64(nonEmpty)
		}
	}

	return api.CQPollerStats{
		PollCycles:   cycles,
		Completions:  completions,
		EmptyPolls:   p.emptyPolls.Load(),
		Errors:       p.errors.Load(),
		AvgBatchSize: avgBatch,
	}
}

// Close stops polling and releases resources.
func (p *Poller) Close() error {
	if p.cancel != nil {
		p.cancel()
	}
	// Close all comp channel files to unblock parked event waiter goroutines.
	p.mu.RLock()
	for _, entry := range p.entries {
		if entry.file != nil {
			entry.file.Close()
			entry.file = nil
		}
	}
	p.mu.RUnlock()
	// Wait for poll loop to exit
	<-p.done
	return nil
}

// NumCQs returns the number of registered CQs. Used by the pool for load balancing.
func (p *Poller) NumCQs() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.entries)
}

// Notify signals the poller in event mode that completions may be available.
func (p *Poller) Notify() {
	select {
	case p.eventCh <- struct{}{}:
	default:
	}
}

// pollLoop runs the appropriate polling strategy based on PollMode.
func (p *Poller) pollLoop(ctx context.Context) {
	defer close(p.done)

	switch p.cfg.PollMode {
	case config.PollModeBusy:
		p.busyPollLoop(ctx)
	case config.PollModeEvent:
		p.eventPollLoop(ctx)
	case config.PollModeSmart:
		p.smartPollLoop(ctx)
	case config.PollModeUser:
		// In user mode, just wait for context cancellation.
		<-ctx.Done()
	default:
		p.busyPollLoop(ctx)
	}
}

// busyPollLoop performs tight busy-polling with runtime.Gosched hints.
func (p *Poller) busyPollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		p.pollOnce()
	}
}

// eventPollLoop uses real completion channel FDs to park goroutines via Go's netpoller.
// Each CQ with a comp channel gets its own waiter goroutine.
// CQs without a comp channel fall back to the eventCh notification mechanism.
func (p *Poller) eventPollLoop(ctx context.Context) {
	// Start event waiters for any CQs already registered.
	p.mu.RLock()
	for _, entry := range p.entries {
		p.startEventWaiter(entry)
	}
	p.mu.RUnlock()

	// Also handle CQs without comp channels via the legacy eventCh path.
	for {
		select {
		case <-ctx.Done():
			p.wg.Wait()
			return
		case <-p.eventCh:
			// Poll only CQs that don't have their own event waiter.
			p.pollNonEventCQs()
		}
	}
}

// smartPollLoop handles CQs in smart mode. CQs with comp channel FDs get
// per-CQ smart waiter goroutines (started via AddCQ/startEventWaiter).
// CQs without comp channels use the legacy eventCh-based smart polling.
func (p *Poller) smartPollLoop(ctx context.Context) {
	// Start smart waiters for any CQs already registered with comp channels.
	p.mu.RLock()
	for _, entry := range p.entries {
		p.startEventWaiter(entry)
	}
	p.mu.RUnlock()

	// For CQs without comp channels, fall back to the original smart poll logic.
	emptyRuns := 0
	for {
		select {
		case <-ctx.Done():
			p.wg.Wait()
			return
		default:
		}

		found := p.pollNonEventCQsCount()
		if found > 0 {
			emptyRuns = 0
		} else {
			emptyRuns++
			if emptyRuns >= p.cfg.SpinCount {
				// Switch to event mode: block until notified or context cancelled.
				select {
				case <-p.eventCh:
				default:
				}
				select {
				case <-ctx.Done():
					p.wg.Wait()
					return
				case <-p.eventCh:
					emptyRuns = 0
				}
			}
		}
	}
}

// pollOnce iterates all registered CQs, polls each, and dispatches completions.
// Returns the total number of completions found.
func (p *Poller) pollOnce() int {
	p.mu.RLock()
	// Copy entries to avoid holding lock during poll
	entries := make([]*cqEntry, 0, len(p.entries))
	for _, e := range p.entries {
		entries = append(entries, e)
	}
	p.mu.RUnlock()

	if len(entries) == 0 {
		p.pollCycles.Add(1)
		p.emptyPolls.Add(1)
		return 0
	}

	totalFound := 0
	for _, entry := range entries {
		totalFound += p.pollSingleCQ(entry)
	}

	p.pollCycles.Add(1)
	if totalFound == 0 {
		p.emptyPolls.Add(1)
	}

	return totalFound
}

// pollSingleCQ polls one CQ and dispatches completions to its handler.
// Returns the number of completions found.
func (p *Poller) pollSingleCQ(entry *cqEntry) int {
	pollFn := p.cfg.PollFunc
	if pollFn == nil {
		pollFn = DefaultPollFunc()
	}

	wcs, err := pollFn(entry.cq, p.cfg.MaxBatchSize)
	if err != nil {
		p.errors.Add(1)
		entry.handler.OnError(nil, err)
		return 0
	}

	found := 0
	for i := range wcs {
		wc := &wcs[i]
		if wc.Status == api.WCSuccess {
			p.completions.Add(1)
			entry.handler.OnCompletion(wc)
		} else {
			p.errors.Add(1)
			entry.handler.OnError(wc, fmt.Errorf("cq: work completion error status %d", wc.Status))
		}
		found++
	}
	return found
}

// pollNonEventCQs polls only CQs that don't have an active event waiter
// (i.e., CompChannelFD() == -1). Used by eventPollLoop for fallback.
func (p *Poller) pollNonEventCQs() {
	p.mu.RLock()
	entries := make([]*cqEntry, 0, len(p.entries))
	for _, e := range p.entries {
		if e.cq.CompChannelFD() < 0 {
			entries = append(entries, e)
		}
	}
	p.mu.RUnlock()

	totalFound := 0
	for _, entry := range entries {
		totalFound += p.pollSingleCQ(entry)
	}
	p.pollCycles.Add(1)
	if totalFound == 0 {
		p.emptyPolls.Add(1)
	}
}

// pollNonEventCQsCount is like pollNonEventCQs but returns the total found count.
// Used by smartPollLoop to drive the spin counter.
func (p *Poller) pollNonEventCQsCount() int {
	p.mu.RLock()
	entries := make([]*cqEntry, 0, len(p.entries))
	for _, e := range p.entries {
		if e.cq.CompChannelFD() < 0 {
			entries = append(entries, e)
		}
	}
	p.mu.RUnlock()

	totalFound := 0
	for _, entry := range entries {
		totalFound += p.pollSingleCQ(entry)
	}
	p.pollCycles.Add(1)
	if totalFound == 0 {
		p.emptyPolls.Add(1)
	}
	return totalFound
}

// startEventWaiter launches a goroutine that waits for completion events on
// the CQ's comp channel FD using os.NewFile and Go's netpoller.
// For CQs without a comp channel (FD == -1), this is a no-op.
// In event mode: pure event-wait loop. In smart mode: busy-poll → event-wait transitions.
func (p *Poller) startEventWaiter(entry *cqEntry) {
	fd := entry.cq.CompChannelFD()
	if fd < 0 {
		return // no comp channel — will be polled via eventCh fallback
	}

	// Dup the FD so os.File.Close() doesn't close the original comp channel FD.
	dupFD, err := syscall.Dup(fd)
	if err != nil {
		return // can't dup — fall back to eventCh
	}
	// Set non-blocking so Go's netpoller can manage it.
	_ = syscall.SetNonblock(dupFD, true)

	f := os.NewFile(uintptr(dupFD), "cq_comp_channel")
	entry.file = f

	cqCtx, cqCancel := context.WithCancel(p.ctx)
	entry.cancel = cqCancel

	p.wg.Add(1)
	if p.cfg.PollMode == config.PollModeSmart {
		go p.smartWaiterLoop(cqCtx, entry, f)
	} else {
		go p.eventWaiterLoop(cqCtx, entry, f)
	}
}

// eventWaiterLoop is the per-CQ goroutine that implements the arm-read-drain loop.
// Sequence: ReqNotify → drain (arm-then-poll) → f.Read (park) → AckEvents → drain → repeat.
func (p *Poller) eventWaiterLoop(ctx context.Context, entry *cqEntry, f *os.File) {
	defer p.wg.Done()
	buf := make([]byte, 8) // comp channel event is 8 bytes (uint64 cq_handle)

	for {
		// Check context before arming.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Arm the CQ for event notification.
		if err := entry.cq.ReqNotify(); err != nil {
			return
		}

		// Arm-then-poll: drain any completions that arrived during arm window.
		for p.pollSingleCQ(entry) > 0 {
		}
		p.pollCycles.Add(1)

		// Park: wait for the kernel to signal the comp channel FD.
		_, err := f.Read(buf)
		if err != nil {
			return // file closed or context cancelled
		}

		// Acknowledge the event.
		entry.cq.AckEvents(1)

		// Drain all pending completions.
		for p.pollSingleCQ(entry) > 0 {
		}
		p.pollCycles.Add(1)
	}
}

// smartWaiterLoop is the per-CQ goroutine for smart mode.
// It busy-polls for SpinCount iterations; if all empty, arms the CQ and parks
// on the comp channel FD. On wake, drains and returns to busy-poll.
func (p *Poller) smartWaiterLoop(ctx context.Context, entry *cqEntry, f *os.File) {
	defer p.wg.Done()
	buf := make([]byte, 8)
	emptyRuns := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// BUSY_POLL phase: poll the CQ.
		found := p.pollSingleCQ(entry)
		p.pollCycles.Add(1)
		if found > 0 {
			emptyRuns = 0
			continue
		}

		p.emptyPolls.Add(1)
		emptyRuns++
		if emptyRuns < p.cfg.SpinCount {
			continue
		}

		// Transition to EVENT_WAIT: arm then park.
		if err := entry.cq.ReqNotify(); err != nil {
			return
		}

		// Arm-then-poll: drain any completions that arrived during arm window.
		if p.pollSingleCQ(entry) > 0 {
			emptyRuns = 0
			continue // Got completions — stay in busy-poll.
		}

		// Park: wait for the kernel to signal.
		_, err := f.Read(buf)
		if err != nil {
			return // file closed or context cancelled
		}

		// Wake: acknowledge and drain.
		entry.cq.AckEvents(1)
		for p.pollSingleCQ(entry) > 0 {
		}
		p.pollCycles.Add(1)
		emptyRuns = 0 // Return to busy-poll.
	}
}
