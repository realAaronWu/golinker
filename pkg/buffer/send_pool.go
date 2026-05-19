package buffer

import (
	"sync"
	"sync/atomic"

	"github.com/wua20/golinker/api"
)

// SendPool implements api.SendBufferPool with in-flight tracking.
type SendPool struct {
	pool      *Pool
	inFlight  sync.Map    // maps uintptr(buffer addr) → struct{}
	inFlightN int64       // atomic counter for in-flight buffers
}

// NewSendPool creates a new send buffer pool wrapping the given pool.
func NewSendPool(pool *Pool) *SendPool {
	return &SendPool{
		pool: pool,
	}
}

// Alloc returns a buffer of at least `size` bytes.
func (sp *SendPool) Alloc(size int) (*api.Buffer, error) {
	return sp.pool.Alloc(size)
}

// Free returns a buffer to the pool.
func (sp *SendPool) Free(buf *api.Buffer) {
	sp.pool.Free(buf)
}

// Stats returns pool utilization metrics including in-flight count.
func (sp *SendPool) Stats() api.BufferPoolStats {
	stats := sp.pool.Stats()
	stats.InFlightBuffers = int(atomic.LoadInt64(&sp.inFlightN))
	return stats
}

// Close releases all buffers and deregisters MRs.
func (sp *SendPool) Close() error {
	return sp.pool.Close()
}

// AcquireForSend gets a buffer and marks it as in-flight.
func (sp *SendPool) AcquireForSend() (*api.Buffer, error) {
	buf, err := sp.pool.Alloc(0)
	if err != nil {
		return nil, err
	}
	sp.inFlight.Store(unsafeAddr(buf.Addr), struct{}{})
	atomic.AddInt64(&sp.inFlightN, 1)
	return buf, nil
}

// CompleteSend marks a send buffer as reclaimable and returns it to the pool.
func (sp *SendPool) CompleteSend(buf *api.Buffer) {
	if buf == nil {
		return
	}
	if _, loaded := sp.inFlight.LoadAndDelete(unsafeAddr(buf.Addr)); loaded {
		atomic.AddInt64(&sp.inFlightN, -1)
	}
	sp.pool.Free(buf)
}

// IsInFlight checks if a buffer is currently in-flight (for testing).
func (sp *SendPool) IsInFlight(buf *api.Buffer) bool {
	_, ok := sp.inFlight.Load(unsafeAddr(buf.Addr))
	return ok
}
