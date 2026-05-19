//go:build mock

package scenarios

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
	"github.com/wua20/golinker/internal/rdma"
	"github.com/wua20/golinker/pkg/buffer"
)

// BufferPoolBench benchmarks buffer pool alloc/free throughput under
// varying contention levels.
type BufferPoolBench struct{}

func init() {
	Register("buffer-pool", &BufferPoolBench{})
}

func (b *BufferPoolBench) Run(ctx context.Context, duration, warmup time.Duration, goroutines int, hist *histogram.Histogram) (*ScenarioResult, error) {
	if goroutines == 0 {
		goroutines = runtime.GOMAXPROCS(0)
	}

	// Create mock verbs and protection domain.
	mockVerbs := rdma.NewMockVerbs()
	_ = mockVerbs.OpenDevice("mock0")
	pd, _ := mockVerbs.AllocPD()

	poolCfg := buffer.PoolConfig{
		BufferSize:   12288,
		BufferCount:  128,
		NUMANode:     0,
		AllocTimeout: 50 * time.Millisecond,
	}
	pool, err := buffer.NewPool(mockVerbs, pd, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("creating pool: %w", err)
	}
	defer pool.Close()

	fmt.Printf("  Buffer pool: %d buffers x %d bytes, %d goroutines\n",
		poolCfg.BufferCount, poolCfg.BufferSize, goroutines)

	// Warmup phase.
	fmt.Printf("  Warming up for %v...\n", warmup)
	warmupCtx, warmupCancel := context.WithTimeout(ctx, warmup)
	runAllocFree(warmupCtx, pool, goroutines, nil, nil)
	warmupCancel()
	hist.Reset()

	// Benchmark phase.
	fmt.Printf("  Running for %v...\n", duration)
	benchCtx, benchCancel := context.WithTimeout(ctx, duration)
	defer benchCancel()

	var totalOps atomic.Int64
	start := time.Now()
	runAllocFree(benchCtx, pool, goroutines, hist, &totalOps)
	elapsed := time.Since(start)

	ops := totalOps.Load()
	return &ScenarioResult{
		Scenario:      "buffer-pool",
		DurationSec:   elapsed.Seconds(),
		TotalOps:      ops,
		OpsPerSec:     float64(ops) / elapsed.Seconds(),
		LatencyP50Us:  hist.P50(),
		LatencyP99Us:  hist.P99(),
		LatencyMaxUs:  hist.Max(),
		LatencyMeanUs: hist.Mean(),
		Notes:         fmt.Sprintf("%d goroutines, %d buffers", goroutines, poolCfg.BufferCount),
	}, nil
}

func runAllocFree(ctx context.Context, pool *buffer.Pool, goroutines int, hist *histogram.Histogram, counter *atomic.Int64) {
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				start := time.Now()
				buf, err := pool.Alloc(64)
				if err != nil {
					// Pool exhausted; yield and retry.
					runtime.Gosched()
					continue
				}
				pool.Free(buf)
				if hist != nil {
					hist.Record(time.Since(start).Microseconds())
				}
				if counter != nil {
					counter.Add(1)
				}
			}
		}()
	}
	wg.Wait()
}
