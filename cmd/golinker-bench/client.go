package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
	"github.com/wua20/golinker/cmd/golinker-bench/ratelimit"
	"github.com/wua20/golinker/cmd/golinker-bench/resources"
	"github.com/wua20/golinker/cmd/golinker-bench/scenarios"
	"github.com/wua20/golinker/internal/rdma"
)

// BenchClient represents the benchmark client.
type BenchClient struct {
	config  *BenchConfig
	hist    *histogram.Histogram
	limiter *ratelimit.Limiter
	tracker *resources.Tracker

	msgsSent atomic.Int64
	msgsRecv atomic.Int64
	errors   atomic.Int64
}

// executeClient implements the client benchmark logic.
func executeClient(cmd *cobra.Command, args []string) error {
	rdma.DebugLog = verbose

	if len(args) < 1 {
		return fmt.Errorf("scenario required (e.g., latency, throughput, buffer-pool)")
	}
	scenario := args[0]

	msgSize, _ := cmd.Flags().GetInt("message-size")
	conns, _ := cmd.Flags().GetInt("connections")
	rate, _ := cmd.Flags().GetInt("rate")
	closedLoop, _ := cmd.Flags().GetBool("closed-loop")
	goroutines, _ := cmd.Flags().GetInt("goroutines")
	if goroutines == 0 {
		goroutines = runtime.GOMAXPROCS(0)
	}

	dur, err := time.ParseDuration(duration)
	if err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}
	warmupDur, err := time.ParseDuration(warmup)
	if err != nil {
		return fmt.Errorf("invalid warmup: %w", err)
	}

	cfg := &BenchConfig{
		Addr:         addr,
		Scenario:     scenario,
		MessageSize:  msgSize,
		Connections:  conns,
		Rate:         rate,
		ClosedLoop:   closedLoop,
		Goroutines:   goroutines,
		Duration:     dur,
		Warmup:       warmupDur,
		OutputFormat: outputFmt,
		OutputFile:   outputFile,
		Verbose:      verbose,
		Pprof:        pprofFlag,
		CPUProfile:   cpuProfile,
		MemProfile:   memProfile,
		CQNumber:     cqNumber,
		PollMode:     pollMode,
		BufferSize:   bufferSize,
	}

	client := &BenchClient{
		config:  cfg,
		hist:    histogram.New(),
		tracker: resources.NewTracker(time.Second),
	}

	if rate > 0 {
		client.limiter = ratelimit.New(rate, rate/10+1)
	} else {
		client.limiter = ratelimit.Unlimited()
	}

	// CPU profiling
	if cfg.CPUProfile != "" {
		f, err := os.Create(cfg.CPUProfile)
		if err != nil {
			return fmt.Errorf("creating CPU profile: %w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("starting CPU profile: %w", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Check if this is a micro-benchmark (no server needed)
	if isMicroBenchmark(scenario) {
		return client.runMicroBenchmark(scenario)
	}

	// End-to-end scenarios need a server
	return client.runEndToEnd(scenario)
}

func isMicroBenchmark(scenario string) bool {
	switch scenario {
	case "buffer-pool", "cq-poll", "aggregation", "cgo-overhead", "mr-reg", "channel-vs-mutex":
		return true
	}
	return false
}

func (c *BenchClient) runMicroBenchmark(scenario string) error {
	fmt.Printf("golinker-bench: micro-benchmark [%s]\n", scenario)
	fmt.Printf("  Duration: %v (warmup: %v)\n", c.config.Duration, c.config.Warmup)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	// Dispatch to scenario runner from scenarios package
	runner := scenarios.Get(scenario)
	if runner == nil {
		return fmt.Errorf("unknown micro-benchmark scenario: %s (registered: %v)", scenario, scenarios.List())
	}

	c.tracker.Start()
	defer c.tracker.Stop()

	result, err := runner.Run(ctx, c.config.Duration, c.config.Warmup, c.config.Goroutines, c.hist)
	if err != nil {
		return err
	}

	// Convert scenario result to BenchResult
	benchResult := &BenchResult{
		Metadata: ResultMetadata{
			ToolVersion: version,
			Timestamp:   time.Now(),
			Scenario:    result.Scenario,
			DurationSec: result.DurationSec,
			WarmupSec:   c.config.Warmup.Seconds(),
			MessageSize: c.config.MessageSize,
			Connections: c.config.Connections,
			PollMode:    c.config.PollMode,
			GoVersion:   runtime.Version(),
			OS:          runtime.GOOS,
			Arch:        runtime.GOARCH,
		},
		Throughput: ThroughputResult{
			MessagesPerSec: result.OpsPerSec,
		},
	}

	// Generate report
	reporter := NewReporter(c.config, c.hist, c.tracker)
	return reporter.Report(benchResult)
}

func (c *BenchClient) runEndToEnd(scenario string) error {
	fmt.Printf("golinker-bench: end-to-end [%s]\n", scenario)
	fmt.Printf("  Target: %s\n", c.config.Addr)
	fmt.Printf("  Message size: %d bytes\n", c.config.MessageSize)
	fmt.Printf("  Connections: %d\n", c.config.Connections)
	fmt.Printf("  Duration: %v (warmup: %v)\n", c.config.Duration, c.config.Warmup)
	if c.config.Rate > 0 {
		fmt.Printf("  Rate limit: %d msgs/sec\n", c.config.Rate)
	}
	fmt.Printf("  Mode: closed-loop\n")

	ctx, cancel := context.WithTimeout(context.Background(), c.config.Warmup+c.config.Duration+10*time.Second)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	cfg := rdma.Config{BufSize: bufferSize, QueueDepth: queueDepth}

	// Establish connections
	conns := make([]*rdma.Conn, c.config.Connections)
	for i := range conns {
		conn, err := rdma.Dial(ctx, c.config.Addr, cfg)
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			return fmt.Errorf("connecting [%d]: %w", i, err)
		}
		conns[i] = conn
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	fmt.Printf("  %d connection(s) established\n", len(conns))

	// Start resource tracking
	c.tracker.Start()
	defer c.tracker.Stop()

	// Warmup phase
	fmt.Printf("  Warming up for %v...\n", c.config.Warmup)
	warmupHist := histogram.New()
	c.runPingPong(ctx, conns, c.config.Warmup, warmupHist)

	// Reset counters after warmup
	c.msgsSent.Store(0)
	c.msgsRecv.Store(0)
	c.errors.Store(0)

	// Benchmark phase
	fmt.Printf("  Running for %v...\n", c.config.Duration)
	start := time.Now()

	c.runPingPong(ctx, conns, c.config.Duration, c.hist)

	elapsed := time.Since(start)

	result := &BenchResult{
		Metadata: ResultMetadata{
			ToolVersion: version,
			Timestamp:   time.Now(),
			Scenario:    scenario,
			DurationSec: elapsed.Seconds(),
			WarmupSec:   c.config.Warmup.Seconds(),
			MessageSize: c.config.MessageSize,
			Connections: c.config.Connections,
			CQNumber:    c.config.CQNumber,
			PollMode:    c.config.PollMode,
			GoVersion:   runtime.Version(),
			OS:          runtime.GOOS,
			Arch:        runtime.GOARCH,
		},
		Throughput: ThroughputResult{
			MessagesPerSec:  float64(c.msgsSent.Load()) / elapsed.Seconds(),
			MegabytesPerSec: float64(c.msgsSent.Load()*int64(c.config.MessageSize)) / elapsed.Seconds() / (1024 * 1024),
		},
		Errors: ErrorResult{
			SendErrors: c.errors.Load(),
		},
	}

	reporter := NewReporter(c.config, c.hist, c.tracker)
	return reporter.Report(result)
}

// runPingPong runs a closed-loop ping-pong benchmark: one goroutine per
// connection, send then recv before sending next message.
func (c *BenchClient) runPingPong(ctx context.Context, conns []*rdma.Conn, d time.Duration, hist *histogram.Histogram) {
	benchCtx, benchCancel := context.WithTimeout(ctx, d)
	defer benchCancel()

	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(1)
		go func(conn *rdma.Conn) {
			defer wg.Done()
			payload := make([]byte, c.config.MessageSize)
			rand.Read(payload)
			recvBuf := make([]byte, bufferSize)

			for {
				select {
				case <-benchCtx.Done():
					return
				default:
				}
				c.limiter.Wait()

				start := time.Now()
				if err := conn.Send(payload); err != nil {
					c.errors.Add(1)
					fmt.Printf("  send error: %v\n", err)
					return
				}
				c.msgsSent.Add(1)

				_, err := conn.Recv(recvBuf)
				if err != nil {
					c.errors.Add(1)
					fmt.Printf("  recv error: %v\n", err)
					return
				}
				c.msgsRecv.Add(1)

				latency := time.Since(start)
				hist.Record(latency.Microseconds())
			}
		}(conn)
	}
	wg.Wait()
}
