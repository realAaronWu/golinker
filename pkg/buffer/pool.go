package buffer

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/wua20/golinker/api"
)

var (
	// ErrPoolExhausted is returned when no buffers are available within the timeout.
	ErrPoolExhausted = errors.New("buffer pool exhausted: no buffers available")
	// ErrPoolClosed is returned when operations are attempted on a closed pool.
	ErrPoolClosed = errors.New("buffer pool is closed")
	// ErrBufferTooLarge is returned when the requested size exceeds the pool's buffer size.
	ErrBufferTooLarge = errors.New("requested buffer size exceeds pool buffer size")
)

// PoolConfig holds the configuration for a buffer pool.
type PoolConfig struct {
	BufferSize  int
	BufferCount int
	NUMANode    int
	AllocTimeout time.Duration // Timeout for Alloc; defaults to 100ms if zero.
}

// Pool implements api.BufferPool with a lock-free free list using a buffered channel.
type Pool struct {
	verbs      api.Verbs
	pd         api.ProtectionDomain
	cfg        PoolConfig
	freeCh     chan *api.Buffer
	allBuffers []*api.Buffer
	closed     atomic.Bool
	mu         sync.Mutex // protects Close
	poolID     int
}

var poolIDCounter int64

// NewPool creates a new buffer pool, allocating and registering all buffers.
func NewPool(verbs api.Verbs, pd api.ProtectionDomain, cfg PoolConfig) (*Pool, error) {
	if cfg.BufferSize <= 0 {
		return nil, errors.New("buffer size must be positive")
	}
	if cfg.BufferCount <= 0 {
		return nil, errors.New("buffer count must be positive")
	}
	if cfg.AllocTimeout == 0 {
		cfg.AllocTimeout = 100 * time.Millisecond
	}

	id := int(atomic.AddInt64(&poolIDCounter, 1))

	// Configure NUMA node for allocation (no-op if !numa build tag).
	SetNUMANode(cfg.NUMANode)

	p := &Pool{
		verbs:      verbs,
		pd:         pd,
		cfg:        cfg,
		freeCh:     make(chan *api.Buffer, cfg.BufferCount),
		allBuffers: make([]*api.Buffer, 0, cfg.BufferCount),
		poolID:     id,
	}

	// Allocate and register all buffers.
	for i := 0; i < cfg.BufferCount; i++ {
		addr := allocBuffer(cfg.BufferSize)
		if addr == nil {
			// Clean up already allocated buffers.
			p.cleanup()
			return nil, errors.New("failed to allocate buffer memory")
		}

		mr, err := verbs.RegMR(pd, addr, cfg.BufferSize,
			api.AccessLocalWrite|api.AccessRemoteWrite|api.AccessRemoteRead)
		if err != nil {
			freeBuffer(addr, cfg.BufferSize)
			p.cleanup()
			return nil, err
		}

		buf := &api.Buffer{
			Addr:   addr,
			Length: cfg.BufferSize,
			LKey:   mr.LKey(),
			RKey:   mr.RKey(),
			MR:     mr,
			PoolID: id,
		}

		p.allBuffers = append(p.allBuffers, buf)
		p.freeCh <- buf
	}

	return p, nil
}

// Alloc returns a buffer of at least `size` bytes from the pool.
// Returns ErrPoolExhausted if no buffer is available within the configured timeout.
func (p *Pool) Alloc(size int) (*api.Buffer, error) {
	if p.closed.Load() {
		return nil, ErrPoolClosed
	}
	if size > p.cfg.BufferSize {
		return nil, ErrBufferTooLarge
	}

	select {
	case buf := <-p.freeCh:
		return buf, nil
	case <-time.After(p.cfg.AllocTimeout):
		return nil, ErrPoolExhausted
	}
}

// Free returns a buffer to the pool.
func (p *Pool) Free(buf *api.Buffer) {
	if buf == nil || p.closed.Load() {
		return
	}
	// Non-blocking write; if channel is full, drop (shouldn't happen in correct usage).
	select {
	case p.freeCh <- buf:
	default:
	}
}

// Stats returns current pool utilization metrics.
func (p *Pool) Stats() api.BufferPoolStats {
	free := len(p.freeCh)
	total := len(p.allBuffers)
	return api.BufferPoolStats{
		TotalBuffers:    total,
		FreeBuffers:     free,
		InFlightBuffers: total - free,
		AllocatedBytes:  int64(total) * int64(p.cfg.BufferSize),
		NUMANode:        p.cfg.NUMANode,
	}
}

// Close releases all buffers and deregisters MRs.
func (p *Pool) Close() error {
	if p.closed.Swap(true) {
		return nil // already closed
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Drain the free channel.
	close(p.freeCh)
	for range p.freeCh {
		// drain
	}

	// Deregister MRs and free memory.
	var firstErr error
	for _, buf := range p.allBuffers {
		if buf.MR != nil {
			if err := p.verbs.DeregMR(buf.MR); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if buf.Addr != nil {
			freeBuffer(buf.Addr, p.cfg.BufferSize)
			buf.Addr = nil
		}
	}
	p.allBuffers = nil

	return firstErr
}

// cleanup is used during construction to release already-allocated buffers on error.
func (p *Pool) cleanup() {
	close(p.freeCh)
	for range p.freeCh {
		// drain
	}
	for _, buf := range p.allBuffers {
		if buf.MR != nil {
			_ = p.verbs.DeregMR(buf.MR)
		}
		if buf.Addr != nil {
			freeBuffer(buf.Addr, p.cfg.BufferSize)
		}
	}
	p.allBuffers = nil
}

// BufferSize returns the configured buffer size for this pool.
func (p *Pool) BufferSize() int {
	return p.cfg.BufferSize
}

// unsafeAddr returns the uintptr value for an unsafe.Pointer (for use as map key).
func unsafeAddr(ptr unsafe.Pointer) uintptr {
	return uintptr(ptr)
}
