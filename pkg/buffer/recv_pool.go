package buffer

import (
	"fmt"

	"github.com/wua20/golinker/api"
)

// RecvPool implements api.RecvBufferPool with receive buffer management.
type RecvPool struct {
	pool  *Pool
	verbs api.Verbs
}

// NewRecvPool creates a new receive buffer pool.
func NewRecvPool(pool *Pool, verbs api.Verbs) *RecvPool {
	return &RecvPool{
		pool:  pool,
		verbs: verbs,
	}
}

// Alloc returns a buffer of at least `size` bytes.
func (rp *RecvPool) Alloc(size int) (*api.Buffer, error) {
	return rp.pool.Alloc(size)
}

// Free returns a buffer to the pool.
func (rp *RecvPool) Free(buf *api.Buffer) {
	rp.pool.Free(buf)
}

// Stats returns pool utilization metrics.
func (rp *RecvPool) Stats() api.BufferPoolStats {
	return rp.pool.Stats()
}

// Close releases all buffers and deregisters MRs.
func (rp *RecvPool) Close() error {
	return rp.pool.Close()
}

// PostRecvBuffers allocates and posts `count` receive buffers to the given QP.
func (rp *RecvPool) PostRecvBuffers(qp api.QueuePair, count int) error {
	for i := 0; i < count; i++ {
		buf, err := rp.pool.Alloc(0)
		if err != nil {
			return fmt.Errorf("PostRecvBuffers: failed to allocate buffer %d/%d: %w", i+1, count, err)
		}

		wr := &api.RecvWR{
			WRID: uint64(uintptr(buf.Addr)),
			SGList: []api.SGE{
				{
					Addr:   uint64(uintptr(buf.Addr)),
					Length: uint32(buf.Length),
					LKey:   buf.LKey,
				},
			},
		}

		if err := rp.verbs.PostRecv(qp, wr); err != nil {
			// Return the buffer since we couldn't post it.
			rp.pool.Free(buf)
			return fmt.Errorf("PostRecvBuffers: failed to post recv WR %d/%d: %w", i+1, count, err)
		}
	}
	return nil
}

// Replenish re-posts `consumed` receive buffers to the given QP.
func (rp *RecvPool) Replenish(qp api.QueuePair, consumed int) error {
	return rp.PostRecvBuffers(qp, consumed)
}
