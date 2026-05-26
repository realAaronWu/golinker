package cq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/config"
)

// Pool implements api.CQPool, managing a pool of CQ pollers for load distribution.
type Pool struct {
	mu       sync.Mutex
	verbs    api.Verbs
	pollers  []*Poller
	cqSize   int
	pollMode config.PollMode

	// cqMap tracks which poller each CQ is registered with
	cqMap map[api.CompletionQueue]*Poller

	closed bool
}

// NewPool creates a new CQ pool with the specified number of pollers.
// Each poller is started immediately with a background context.
func NewPool(verbs api.Verbs, numPollers int, cfg PollerConfig) (*Pool, error) {
	if verbs == nil {
		return nil, errors.New("cq: nil Verbs")
	}
	if numPollers <= 0 {
		return nil, fmt.Errorf("cq: numPollers must be > 0, got %d", numPollers)
	}

	pool := &Pool{
		verbs:    verbs,
		pollers:  make([]*Poller, numPollers),
		cqSize:   4096, // default CQ size
		pollMode: cfg.PollMode,
		cqMap:    make(map[api.CompletionQueue]*Poller),
	}

	for i := 0; i < numPollers; i++ {
		poller := NewPoller(cfg)
		if err := poller.Start(context.Background()); err != nil {
			// Close already-started pollers
			for j := 0; j < i; j++ {
				_ = pool.pollers[j].Close()
			}
			return nil, fmt.Errorf("cq: failed to start poller %d: %w", i, err)
		}
		pool.pollers[i] = poller
	}

	return pool, nil
}

// Assign returns the least-loaded poller and creates a new CQ for the caller.
func (p *Pool) Assign() (api.CQPoller, api.CompletionQueue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, nil, errors.New("cq: pool is closed")
	}

	// Find poller with fewest CQs
	leastLoaded := p.pollers[0]
	minCount := leastLoaded.NumCQs()
	for _, poller := range p.pollers[1:] {
		count := poller.NumCQs()
		if count < minCount {
			minCount = count
			leastLoaded = poller
		}
	}

	// Create a new CQ via verbs — use completion channel for event/smart modes
	var cq api.CompletionQueue
	var err error
	if p.pollMode == config.PollModeEvent || p.pollMode == config.PollModeSmart {
		cq, err = p.verbs.CreateCQWithChannel(p.cqSize)
	} else {
		cq, err = p.verbs.CreateCQ(p.cqSize)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("cq: failed to create CQ: %w", err)
	}

	// Track which poller this CQ belongs to (handler will be added by the caller via AddCQ)
	p.cqMap[cq] = leastLoaded

	return leastLoaded, cq, nil
}

// Release removes a CQ from its assigned poller.
func (p *Pool) Release(cq api.CompletionQueue) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if cq == nil {
		return errors.New("cq: nil CompletionQueue")
	}

	poller, exists := p.cqMap[cq]
	if !exists {
		return errors.New("cq: CQ not found in pool")
	}

	// RemoveCQ may return error if not registered (handler not yet added)
	_ = poller.RemoveCQ(cq)
	delete(p.cqMap, cq)
	return nil
}

// Close shuts down all pollers in the pool.
func (p *Pool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	var firstErr error
	for _, poller := range p.pollers {
		if err := poller.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
