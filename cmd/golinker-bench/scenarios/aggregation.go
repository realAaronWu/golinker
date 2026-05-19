package scenarios

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
	"github.com/wua20/golinker/pkg/message"
)

// AggregationBench benchmarks message aggregation pack/unpack throughput.
type AggregationBench struct{}

func init() {
	Register("aggregation", &AggregationBench{})
}

func (b *AggregationBench) Run(ctx context.Context, duration, warmup time.Duration, goroutines int, hist *histogram.Histogram) (*ScenarioResult, error) {
	if goroutines == 0 {
		goroutines = runtime.GOMAXPROCS(0)
	}

	msgSize := 64 // default 64B messages
	batchCount := 8 // batch 8 messages

	fmt.Printf("  Aggregation: %d x %dB messages, %d goroutines\n", batchCount, msgSize, goroutines)

	// Warmup phase.
	fmt.Printf("  Warming up for %v...\n", warmup)
	warmupCtx, warmupCancel := context.WithTimeout(ctx, warmup)
	runPackUnpack(warmupCtx, msgSize, batchCount, goroutines, nil, nil)
	warmupCancel()
	hist.Reset()

	// Benchmark phase.
	fmt.Printf("  Running for %v...\n", duration)
	benchCtx, benchCancel := context.WithTimeout(ctx, duration)
	defer benchCancel()

	var totalOps atomic.Int64
	start := time.Now()
	runPackUnpack(benchCtx, msgSize, batchCount, goroutines, hist, &totalOps)
	elapsed := time.Since(start)

	ops := totalOps.Load()
	return &ScenarioResult{
		Scenario:      "aggregation",
		DurationSec:   elapsed.Seconds(),
		TotalOps:      ops,
		OpsPerSec:     float64(ops) / elapsed.Seconds(),
		LatencyP50Us:  hist.P50(),
		LatencyP99Us:  hist.P99(),
		LatencyMaxUs:  hist.Max(),
		LatencyMeanUs: hist.Mean(),
		Notes:         fmt.Sprintf("%d goroutines, %dx%dB batch", goroutines, batchCount, msgSize),
	}, nil
}

func runPackUnpack(ctx context.Context, msgSize, batchCount, goroutines int, hist *histogram.Histogram, counter *atomic.Int64) {
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Pre-allocate messages and buffer per goroutine.
			msgs := make([][]byte, batchCount)
			for j := range msgs {
				msgs[j] = make([]byte, msgSize)
				// Fill with non-zero data to avoid compiler optimizations.
				for k := range msgs[j] {
					msgs[j][k] = byte(k)
				}
			}
			buf := make([]byte, message.BatchSize(msgs)+64) // buffer with headroom

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				start := time.Now()
				// Pack messages into wire format.
				n := message.PackBatch(buf, msgs)
				// Unpack from wire format.
				_, err := message.UnpackBatch(buf, n)
				if err != nil {
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
