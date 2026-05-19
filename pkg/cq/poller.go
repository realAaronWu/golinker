// Package cq provides completion queue polling for RDMA work completions.
package cq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
	cancel  context.CancelFunc
	done    chan struct{}
	started atomic.Bool

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
	p.entries[cq] = &cqEntry{cq: cq, handler: handler}
	return nil
}

// RemoveCQ unregisters a CQ from this poller.
func (p *Poller) RemoveCQ(cq api.CompletionQueue) error {
	if cq == nil {
		return errors.New("cq: nil CompletionQueue")
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.entries[cq]; !exists {
		return fmt.Errorf("cq: CQ not registered")
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

// eventPollLoop blocks until notified, then polls.
func (p *Poller) eventPollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.eventCh:
			p.pollOnce()
		}
	}
}

// smartPollLoop busy-polls for SpinCount iterations; if all empty, switches to event mode.
// Any completion resets the spin counter.
func (p *Poller) smartPollLoop(ctx context.Context) {
	emptyRuns := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		found := p.pollOnce()
		if found > 0 {
			emptyRuns = 0
		} else {
			emptyRuns++
			if emptyRuns >= p.cfg.SpinCount {
				// Switch to event mode: block until notified or context cancelled.
				// In production, the CQ fd will signal the netpoller.
				// Drain any pending notification first.
				select {
				case <-p.eventCh:
				default:
				}
				select {
				case <-ctx.Done():
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
	pollFn := p.cfg.PollFunc
	if pollFn == nil {
		// Default no-op poll function (real impl would call ibv_poll_cq)
		pollFn = func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
			return nil, nil
		}
	}

	for _, entry := range entries {
		wcs, err := pollFn(entry.cq, p.cfg.MaxBatchSize)
		if err != nil {
			p.errors.Add(1)
			entry.handler.OnError(nil, err)
			continue
		}

		for i := range wcs {
			wc := &wcs[i]
			if wc.Status == api.WCSuccess {
				p.completions.Add(1)
				entry.handler.OnCompletion(wc)
			} else {
				p.errors.Add(1)
				entry.handler.OnError(wc, fmt.Errorf("cq: work completion error status %d", wc.Status))
			}
			totalFound++
		}
	}

	p.pollCycles.Add(1)
	if totalFound == 0 {
		p.emptyPolls.Add(1)
	}

	return totalFound
}
