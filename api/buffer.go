package api

import (
	"sync/atomic"
	"unsafe"
)

// Buffer represents a registered RDMA buffer.
type Buffer struct {
	Addr   unsafe.Pointer
	Length int
	LKey   uint32
	RKey   uint32
	MR     MemoryRegion
	PoolID int // identifies which pool owns this buffer
}

// BufferPool manages allocation of registered RDMA buffers.
type BufferPool interface {
	// Alloc returns a buffer of at least `size` bytes.
	// Blocks if pool is exhausted (with configurable timeout).
	Alloc(size int) (*Buffer, error)

	// Free returns a buffer to the pool.
	Free(buf *Buffer)

	// Stats returns pool utilization metrics.
	Stats() BufferPoolStats

	// Close releases all buffers and deregisters MRs.
	Close() error
}

// SendBufferPool extends BufferPool with send-specific semantics.
type SendBufferPool interface {
	BufferPool
	// AcquireForSend gets a buffer and marks it in-flight.
	AcquireForSend() (*Buffer, error)
	// CompleteSend marks a send buffer as reclaimable.
	CompleteSend(buf *Buffer)
	// BusyFlag returns a shared atomic flag that is set when the pool is under
	// pressure (>= 50% buffers in-flight) and cleared when all are returned.
	// The aggregation engine reads this to decide between immediate and batched sends.
	BusyFlag() *atomic.Bool
}

// RecvBufferPool extends BufferPool with receive-specific semantics.
type RecvBufferPool interface {
	BufferPool
	// PostRecvBuffers pre-posts receive buffers to a QP.
	PostRecvBuffers(qp QueuePair, count int) error
	// Replenish re-posts consumed receive buffers.
	Replenish(qp QueuePair, consumed int) error
}

// BufferPoolStats contains pool utilization metrics.
type BufferPoolStats struct {
	TotalBuffers    int
	FreeBuffers     int
	InFlightBuffers int
	AllocatedBytes  int64
	NUMANode        int
}
