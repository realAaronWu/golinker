package scenarios

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
	"github.com/wua20/golinker/pkg/cq"
	"github.com/wua20/golinker/pkg/config"
)

// CQPollBench benchmarks CQ polling throughput with mock completions.
type CQPollBench struct{}

func init() {
	Register("cq-poll", &CQPollBench{})
}

func (b *CQPollBench) Run(ctx context.Context, duration, warmup time.Duration, goroutines int, hist *histogram.Histogram) (*ScenarioResult, error) {
	if goroutines == 0 {
		goroutines = runtime.GOMAXPROCS(0)
	}

	batchSize := 32
	fmt.Printf("  CQ poll: batch size %d, %d goroutines\n", batchSize, goroutines)

	// Create a mock PollFunc that returns synthetic completions.
	var pollCount atomic.Int64
	mockPollFn := func(cqHandle api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		count := batchSize
		if count > maxWCs {
			count = maxWCs
		}
		wcs := make([]api.WorkCompletion, count)
		for i := range wcs {
			wcs[i] = api.WorkCompletion{
				WRID:    uint64(i),
				Status:  api.WCSuccess,
				Opcode:  api.WCRecv,
				ByteLen: 64,
			}
		}
		pollCount.Add(int64(count))
		return wcs, nil
	}

	pollerCfg := cq.PollerConfig{
		PollMode:     config.PollModeBusy,
		MaxBatchSize: batchSize,
		SpinCount:    1000,
		PollFunc:     mockPollFn,
	}

	// Warmup phase.
	fmt.Printf("  Warming up for %v...\n", warmup)
	warmupCtx, warmupCancel := context.WithTimeout(ctx, warmup)
	runPollBench(warmupCtx, pollerCfg, goroutines, nil, nil)
	warmupCancel()
	hist.Reset()
	pollCount.Store(0)

	// Benchmark phase.
	fmt.Printf("  Running for %v...\n", duration)
	benchCtx, benchCancel := context.WithTimeout(ctx, duration)
	defer benchCancel()

	var totalOps atomic.Int64
	start := time.Now()
	runPollBench(benchCtx, pollerCfg, goroutines, hist, &totalOps)
	elapsed := time.Since(start)

	ops := totalOps.Load()
	completions := pollCount.Load()

	return &ScenarioResult{
		Scenario:      "cq-poll",
		DurationSec:   elapsed.Seconds(),
		TotalOps:      completions,
		OpsPerSec:     float64(completions) / elapsed.Seconds(),
		LatencyP50Us:  hist.P50(),
		LatencyP99Us:  hist.P99(),
		LatencyMaxUs:  hist.Max(),
		LatencyMeanUs: hist.Mean(),
		Notes:         fmt.Sprintf("%d goroutines, batch=%d, %.0f polls/sec", goroutines, batchSize, float64(ops)/elapsed.Seconds()),
	}, nil
}

func runPollBench(ctx context.Context, cfg cq.PollerConfig, goroutines int, hist *histogram.Histogram, counter *atomic.Int64) {
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Directly call the poll function to measure raw throughput,
			// bypassing the poller goroutine machinery.
			mockCQ := &benchCQ{size: 4096}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				start := time.Now()
				_, _ = cfg.PollFunc(mockCQ, cfg.MaxBatchSize)
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

// benchCQ implements api.CompletionQueue for benchmarking purposes.
type benchCQ struct {
	size int
}

func (m *benchCQ) Handle() unsafe.Pointer { return nil }
func (m *benchCQ) Size() int              { return m.size }
