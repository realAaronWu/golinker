package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
	"github.com/wua20/golinker/cmd/golinker-bench/ratelimit"
	"github.com/wua20/golinker/cmd/golinker-bench/resources"
	"github.com/wua20/golinker/cmd/golinker-bench/scenarios"
	"github.com/wua20/golinker/pkg/connection"
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
	fmt.Printf("  Mode: %s\n", func() string {
		if c.config.ClosedLoop {
			return "closed-loop"
		}
		return "open-loop"
	}())

	// Initialize RDMA stack
	verbs, pd, err := initVerbs(device)
	if err != nil {
		return fmt.Errorf("init verbs: %w", err)
	}

	sendPool, err := newBufferPool(verbs, pd, bufferSize, 128, numaNode)
	if err != nil {
		return fmt.Errorf("creating send buffer pool: %w", err)
	}
	_ = sendPool

	recvPool, err := newBufferPool(verbs, pd, bufferSize, 128, numaNode)
	if err != nil {
		return fmt.Errorf("creating recv buffer pool: %w", err)
	}
	_ = recvPool

	cqPool, err := newCQPool(verbs, cqNumber, pollMode)
	if err != nil {
		return fmt.Errorf("creating CQ pool: %w", err)
	}
	_ = cqPool

	// Create connection manager and establish connections
	connMgr := connection.NewManager(connection.ManagerConfig{
		Verbs:      verbs,
		QueueDepth: queueDepth,
	})

	ctx, cancel := context.WithTimeout(context.Background(), c.config.Warmup+c.config.Duration)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	conns := make([]api.Connection, c.config.Connections)
	for i := range conns {
		conn, err := connMgr.Connect(ctx, c.config.Addr)
		if err != nil {
			connMgr.Close()
			return fmt.Errorf("connecting [%d]: %w", i, err)
		}
		conns[i] = conn
	}
	defer connMgr.Close()

	// Start resource tracking
	c.tracker.Start()
	defer c.tracker.Stop()

	// Warmup phase — send traffic to warm caches and pools
	warmupHist := histogram.New()
	fmt.Printf("  Warming up for %v...\n", c.config.Warmup)
	c.runSendLoop(ctx, conns, c.config.Warmup, warmupHist)

	// Reset counters after warmup
	c.msgsSent.Store(0)
	c.msgsRecv.Store(0)
	c.errors.Store(0)

	// Benchmark phase
	fmt.Printf("  Running for %v...\n", c.config.Duration)
	start := time.Now()

	if c.config.ClosedLoop || scenario == "latency" {
		c.runClosedLoop(ctx, conns, c.config.Duration)
	} else {
		c.runOpenLoop(ctx, conns, c.config.Duration)
	}

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

// runSendLoop sends messages for the given duration, recording latency into hist.
// Used for both warmup and the open-loop benchmark phase.
func (c *BenchClient) runSendLoop(ctx context.Context, conns []api.Connection, d time.Duration, hist *histogram.Histogram) {
	loopCtx, loopCancel := context.WithTimeout(ctx, d)
	defer loopCancel()

	var wg sync.WaitGroup
	for i := 0; i < c.config.Goroutines; i++ {
		wg.Add(1)
		conn := conns[i%len(conns)]
		go func(conn api.Connection) {
			defer wg.Done()
			msg := &api.Message{Length: c.config.MessageSize}
			for {
				select {
				case <-loopCtx.Done():
					return
				default:
				}
				c.limiter.Wait()
				sendStart := time.Now()
				if err := conn.Send(msg); err != nil {
					c.errors.Add(1)
					continue
				}
				latency := time.Since(sendStart)
				hist.Record(latency.Microseconds())
				c.msgsSent.Add(1)
			}
		}(conn)
	}
	wg.Wait()
}

// runClosedLoop implements a closed-loop benchmark: one goroutine per connection,
// send then wait for response before sending the next message.
func (c *BenchClient) runClosedLoop(ctx context.Context, conns []api.Connection, d time.Duration) {
	benchCtx, benchCancel := context.WithTimeout(ctx, d)
	defer benchCancel()

	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(1)
		go func(conn api.Connection) {
			defer wg.Done()
			msg := &api.Message{Length: c.config.MessageSize}
			// Short deadline for Recv so the mock build does not hang.
			const recvTimeout = 50 * time.Millisecond
			for {
				select {
				case <-benchCtx.Done():
					return
				default:
				}
				c.limiter.Wait()
				start := time.Now()
				if err := conn.Send(msg); err != nil {
					c.errors.Add(1)
					continue
				}
				c.msgsSent.Add(1)

				// Wait for echo response; use a short per-recv deadline
				// so mock builds (where no echo arrives) don't block.
				recvCtx, recvCancel := context.WithTimeout(benchCtx, recvTimeout)
				_, err := conn.Recv(recvCtx)
				recvCancel()
				if err != nil {
					// In mock mode Recv always times out; count the
					// send latency only and continue.
					latency := time.Since(start)
					c.hist.Record(latency.Microseconds())
					continue
				}
				latency := time.Since(start)
				c.hist.Record(latency.Microseconds())
				c.msgsRecv.Add(1)
			}
		}(conn)
	}
	wg.Wait()
}

// runOpenLoop implements an open-loop benchmark: N sender goroutines
// round-robin across connections, with separate receiver goroutines
// draining responses.
func (c *BenchClient) runOpenLoop(ctx context.Context, conns []api.Connection, d time.Duration) {
	benchCtx, benchCancel := context.WithTimeout(ctx, d)
	defer benchCancel()

	var wg sync.WaitGroup

	// Sender goroutines
	for i := 0; i < c.config.Goroutines; i++ {
		wg.Add(1)
		conn := conns[i%len(conns)]
		go func(conn api.Connection) {
			defer wg.Done()
			msg := &api.Message{Length: c.config.MessageSize}
			for {
				select {
				case <-benchCtx.Done():
					return
				default:
				}
				c.limiter.Wait()
				sendStart := time.Now()
				if err := conn.Send(msg); err != nil {
					c.errors.Add(1)
					continue
				}
				latency := time.Since(sendStart)
				c.hist.Record(latency.Microseconds())
				c.msgsSent.Add(1)
			}
		}(conn)
	}

	// Receiver goroutines -- one per connection
	for _, conn := range conns {
		wg.Add(1)
		go func(conn api.Connection) {
			defer wg.Done()
			for {
				_, err := conn.Recv(benchCtx)
				if err != nil {
					return
				}
				c.msgsRecv.Add(1)
			}
		}(conn)
	}

	wg.Wait()
}
