package scenarios

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
)

// ChannelMutexBench compares channel-based pool vs mutex-protected pool
// to measure synchronization primitive overhead under contention.
type ChannelMutexBench struct{}

func init() {
	Register("channel-vs-mutex", &ChannelMutexBench{})
}

func (b *ChannelMutexBench) Run(ctx context.Context, duration, warmup time.Duration, goroutines int, hist *histogram.Histogram) (*ScenarioResult, error) {
	if goroutines == 0 {
		goroutines = runtime.GOMAXPROCS(0)
	}
	poolSize := 128

	fmt.Printf("  Channel vs Mutex: pool size %d, %d goroutines\n", poolSize, goroutines)

	// --- Channel-based pool ---
	fmt.Printf("  [1/2] Channel pool...\n")
	chPool := make(chan int, poolSize)
	for i := 0; i < poolSize; i++ {
		chPool <- i
	}

	warmupCtx, warmupCancel := context.WithTimeout(ctx, warmup)
	runChannelPool(warmupCtx, chPool, goroutines, nil, nil)
	warmupCancel()
	hist.Reset()

	benchCtx, benchCancel := context.WithTimeout(ctx, duration)
	var chOps atomic.Int64
	chStart := time.Now()
	runChannelPool(benchCtx, chPool, goroutines, hist, &chOps)
	benchCancel()
	chElapsed := time.Since(chStart)
	chOpsPerSec := float64(chOps.Load()) / chElapsed.Seconds()
	chP50 := hist.P50()
	chP99 := hist.P99()

	// --- Mutex-based pool ---
	fmt.Printf("  [2/2] Mutex pool...\n")
	hist.Reset()
	muPool := &mutexPool{items: make([]int, 0, poolSize)}
	for i := 0; i < poolSize; i++ {
		muPool.items = append(muPool.items, i)
	}

	warmupCtx2, warmupCancel2 := context.WithTimeout(ctx, warmup)
	runMutexPool(warmupCtx2, muPool, goroutines, nil, nil)
	warmupCancel2()
	hist.Reset()

	benchCtx2, benchCancel2 := context.WithTimeout(ctx, duration)
	var muOps atomic.Int64
	muStart := time.Now()
	runMutexPool(benchCtx2, muPool, goroutines, hist, &muOps)
	benchCancel2()
	muElapsed := time.Since(muStart)
	muOpsPerSec := float64(muOps.Load()) / muElapsed.Seconds()

	return &ScenarioResult{
		Scenario:      "channel-vs-mutex",
		DurationSec:   chElapsed.Seconds() + muElapsed.Seconds(),
		TotalOps:      chOps.Load() + muOps.Load(),
		OpsPerSec:     chOpsPerSec, // report channel ops/sec as primary metric
		LatencyP50Us:  chP50,
		LatencyP99Us:  chP99,
		LatencyMaxUs:  hist.Max(),
		LatencyMeanUs: hist.Mean(),
		Notes: fmt.Sprintf("channel: %.0f ops/sec (p50=%.1fus p99=%.1fus) | mutex: %.0f ops/sec",
			chOpsPerSec, chP50, chP99, muOpsPerSec),
	}, nil
}

type mutexPool struct {
	mu    sync.Mutex
	items []int
}

func runChannelPool(ctx context.Context, pool chan int, goroutines int, hist *histogram.Histogram, counter *atomic.Int64) {
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
				select {
				case item := <-pool:
					pool <- item
				case <-ctx.Done():
					return
				}
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

func runMutexPool(ctx context.Context, pool *mutexPool, goroutines int, hist *histogram.Histogram, counter *atomic.Int64) {
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
				pool.mu.Lock()
				if len(pool.items) > 0 {
					item := pool.items[len(pool.items)-1]
					pool.items = pool.items[:len(pool.items)-1]
					pool.mu.Unlock()
					// Simulate "use" then return.
					pool.mu.Lock()
					pool.items = append(pool.items, item)
					pool.mu.Unlock()
				} else {
					pool.mu.Unlock()
					runtime.Gosched()
					continue
				}
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
