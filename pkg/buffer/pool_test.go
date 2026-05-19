//go:build mock

package buffer

import (
	"sync"
	"testing"
	"time"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
)

func setupTestPool(t *testing.T, bufCount int) (*Pool, *rdma.MockVerbs) {
	t.Helper()
	verbs := rdma.NewMockVerbs()
	pd := &rdma.MockPD{}

	cfg := PoolConfig{
		BufferSize:   4096,
		BufferCount:  bufCount,
		NUMANode:     0,
		AllocTimeout: 50 * time.Millisecond,
	}

	pool, err := NewPool(verbs, pd, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}
	return pool, verbs
}

func TestPoolAllocFree(t *testing.T) {
	pool, _ := setupTestPool(t, 8)
	defer pool.Close()

	// Allocate all buffers.
	buffers := make([]*api.Buffer, 8)
	for i := 0; i < 8; i++ {
		buf, err := pool.Alloc(1024)
		if err != nil {
			t.Fatalf("Alloc %d failed: %v", i, err)
		}
		if buf == nil {
			t.Fatalf("Alloc %d returned nil buffer", i)
		}
		if buf.Addr == nil {
			t.Fatalf("Alloc %d returned buffer with nil Addr", i)
		}
		if buf.Length != 4096 {
			t.Fatalf("Expected buffer length 4096, got %d", buf.Length)
		}
		buffers[i] = buf
	}

	// Free all buffers.
	for _, buf := range buffers {
		pool.Free(buf)
	}

	// Allocate again - should succeed.
	for i := 0; i < 8; i++ {
		buf, err := pool.Alloc(1024)
		if err != nil {
			t.Fatalf("Re-alloc %d failed: %v", i, err)
		}
		if buf == nil {
			t.Fatalf("Re-alloc %d returned nil", i)
		}
		pool.Free(buf)
	}
}

func TestPoolConcurrent(t *testing.T) {
	pool, _ := setupTestPool(t, 64)
	defer pool.Close()

	var wg sync.WaitGroup
	wg.Add(64)

	for i := 0; i < 64; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				buf, err := pool.Alloc(512)
				if err != nil {
					// Pool may be temporarily exhausted; that's okay.
					continue
				}
				// Simulate some work.
				time.Sleep(time.Microsecond)
				pool.Free(buf)
			}
		}()
	}

	wg.Wait()

	// After all goroutines complete, all buffers should be free.
	stats := pool.Stats()
	if stats.FreeBuffers != stats.TotalBuffers {
		t.Errorf("Expected all buffers free, got %d/%d free", stats.FreeBuffers, stats.TotalBuffers)
	}
}

func TestPoolExhaustion(t *testing.T) {
	pool, _ := setupTestPool(t, 4)
	defer pool.Close()

	// Allocate all buffers.
	buffers := make([]*api.Buffer, 4)
	for i := 0; i < 4; i++ {
		buf, err := pool.Alloc(1024)
		if err != nil {
			t.Fatalf("Alloc %d failed: %v", i, err)
		}
		buffers[i] = buf
	}

	// Next alloc should fail with timeout.
	start := time.Now()
	_, err := pool.Alloc(1024)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected error on exhausted pool, got nil")
	}
	if err != ErrPoolExhausted {
		t.Fatalf("Expected ErrPoolExhausted, got: %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("Expected timeout to take ~50ms, took %v", elapsed)
	}

	// Free one and try again.
	pool.Free(buffers[0])
	buf, err := pool.Alloc(1024)
	if err != nil {
		t.Fatalf("Alloc after free failed: %v", err)
	}
	if buf == nil {
		t.Fatal("Alloc after free returned nil")
	}

	// Clean up.
	pool.Free(buf)
	for i := 1; i < 4; i++ {
		pool.Free(buffers[i])
	}
}

func TestSendPoolInFlight(t *testing.T) {
	pool, _ := setupTestPool(t, 8)
	sp := NewSendPool(pool)
	defer sp.Close()

	// Acquire a buffer for sending.
	buf, err := sp.AcquireForSend()
	if err != nil {
		t.Fatalf("AcquireForSend failed: %v", err)
	}
	if buf == nil {
		t.Fatal("AcquireForSend returned nil")
	}

	// Verify it's in-flight.
	if !sp.IsInFlight(buf) {
		t.Fatal("Buffer should be in-flight after AcquireForSend")
	}

	// Verify stats show in-flight.
	stats := sp.Stats()
	if stats.InFlightBuffers != 1 {
		t.Fatalf("Expected 1 in-flight buffer, got %d", stats.InFlightBuffers)
	}

	// Complete the send.
	sp.CompleteSend(buf)

	// Verify it's no longer in-flight.
	if sp.IsInFlight(buf) {
		t.Fatal("Buffer should not be in-flight after CompleteSend")
	}

	// Verify stats updated.
	stats = sp.Stats()
	if stats.InFlightBuffers != 0 {
		t.Fatalf("Expected 0 in-flight buffers after complete, got %d", stats.InFlightBuffers)
	}
}

func TestRecvPoolPostAndReplenish(t *testing.T) {
	verbs := rdma.NewMockVerbs()
	pd := &rdma.MockPD{}

	cfg := PoolConfig{
		BufferSize:   4096,
		BufferCount:  16,
		NUMANode:     0,
		AllocTimeout: 50 * time.Millisecond,
	}

	pool, err := NewPool(verbs, pd, cfg)
	if err != nil {
		t.Fatalf("NewPool failed: %v", err)
	}

	rp := NewRecvPool(pool, verbs)
	defer rp.Close()

	// Create a mock QP.
	qp := &rdma.MockQP{}

	// Post 4 receive buffers.
	err = rp.PostRecvBuffers(qp, 4)
	if err != nil {
		t.Fatalf("PostRecvBuffers failed: %v", err)
	}

	// Verify 4 recv WRs were posted.
	recvLog := verbs.GetPostRecvLog()
	if len(recvLog) != 4 {
		t.Fatalf("Expected 4 posted recv WRs, got %d", len(recvLog))
	}

	// Verify each WR has valid SGE.
	for i, wr := range recvLog {
		if len(wr.SGList) != 1 {
			t.Fatalf("WR %d: expected 1 SGE, got %d", i, len(wr.SGList))
		}
		if wr.SGList[0].Length != 4096 {
			t.Fatalf("WR %d: expected SGE length 4096, got %d", i, wr.SGList[0].Length)
		}
		if wr.SGList[0].LKey == 0 {
			t.Fatalf("WR %d: expected non-zero LKey", i)
		}
	}

	// Replenish 2 buffers.
	err = rp.Replenish(qp, 2)
	if err != nil {
		t.Fatalf("Replenish failed: %v", err)
	}

	// Total should now be 6 posted WRs.
	recvLog = verbs.GetPostRecvLog()
	if len(recvLog) != 6 {
		t.Fatalf("Expected 6 total posted recv WRs after replenish, got %d", len(recvLog))
	}
}

func TestPoolStats(t *testing.T) {
	pool, _ := setupTestPool(t, 10)
	defer pool.Close()

	// Initial stats.
	stats := pool.Stats()
	if stats.TotalBuffers != 10 {
		t.Fatalf("Expected TotalBuffers=10, got %d", stats.TotalBuffers)
	}
	if stats.FreeBuffers != 10 {
		t.Fatalf("Expected FreeBuffers=10, got %d", stats.FreeBuffers)
	}
	if stats.InFlightBuffers != 0 {
		t.Fatalf("Expected InFlightBuffers=0, got %d", stats.InFlightBuffers)
	}
	if stats.AllocatedBytes != int64(10*4096) {
		t.Fatalf("Expected AllocatedBytes=%d, got %d", 10*4096, stats.AllocatedBytes)
	}
	if stats.NUMANode != 0 {
		t.Fatalf("Expected NUMANode=0, got %d", stats.NUMANode)
	}

	// Allocate 3 buffers.
	bufs := make([]*api.Buffer, 3)
	for i := 0; i < 3; i++ {
		var err error
		bufs[i], err = pool.Alloc(1024)
		if err != nil {
			t.Fatalf("Alloc failed: %v", err)
		}
	}

	stats = pool.Stats()
	if stats.FreeBuffers != 7 {
		t.Fatalf("Expected FreeBuffers=7, got %d", stats.FreeBuffers)
	}
	if stats.InFlightBuffers != 3 {
		t.Fatalf("Expected InFlightBuffers=3, got %d", stats.InFlightBuffers)
	}

	// Free them back.
	for _, buf := range bufs {
		pool.Free(buf)
	}

	stats = pool.Stats()
	if stats.FreeBuffers != 10 {
		t.Fatalf("Expected FreeBuffers=10 after freeing, got %d", stats.FreeBuffers)
	}
}

func TestPoolClose(t *testing.T) {
	pool, _ := setupTestPool(t, 4)

	// Allocate a buffer.
	buf, err := pool.Alloc(1024)
	if err != nil {
		t.Fatalf("Alloc failed: %v", err)
	}

	// Return it before close.
	pool.Free(buf)

	// Close the pool.
	err = pool.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Alloc after close should fail.
	_, err = pool.Alloc(1024)
	if err != ErrPoolClosed {
		t.Fatalf("Expected ErrPoolClosed, got: %v", err)
	}

	// Double close should be safe.
	err = pool.Close()
	if err != nil {
		t.Fatalf("Double close failed: %v", err)
	}
}

func TestBufferTooLarge(t *testing.T) {
	pool, _ := setupTestPool(t, 4)
	defer pool.Close()

	// Request larger than buffer size.
	_, err := pool.Alloc(8192)
	if err != ErrBufferTooLarge {
		t.Fatalf("Expected ErrBufferTooLarge, got: %v", err)
	}
}
