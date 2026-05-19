package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

// ResponseMode determines how the server handles received messages.
type ResponseMode string

const (
	ModeEcho ResponseMode = "echo"
	ModeSink ResponseMode = "sink"
)

// AtomicStats tracks server statistics concurrently.
type AtomicStats struct {
	MessagesReceived atomic.Int64
	MessagesSent     atomic.Int64
	BytesReceived    atomic.Int64
	BytesSent        atomic.Int64
	Errors           atomic.Int64
}

// BenchServer represents the benchmark server.
type BenchServer struct {
	addr  string
	mode  ResponseMode
	stats *AtomicStats
}

// executeServer implements the server logic called by the server command.
func executeServer(cmd *cobra.Command, args []string) error {
	mode, _ := cmd.Flags().GetString("mode")
	if mode == "" {
		mode = "echo"
	}

	server := &BenchServer{
		addr:  addr,
		mode:  ResponseMode(mode),
		stats: &AtomicStats{},
	}

	// Start pprof if requested
	if pprofFlag {
		go func() {
			fmt.Println("pprof server on :6060")
			_ = http.ListenAndServe(":6060", nil)
		}()
	}

	// CPU profiling
	if cpuProfile != "" {
		f, err := os.Create(cpuProfile)
		if err != nil {
			return fmt.Errorf("creating CPU profile: %w", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("starting CPU profile: %w", err)
		}
		defer pprof.StopCPUProfile()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("golinker-bench server starting\n")
	fmt.Printf("  Address: %s\n", server.addr)
	fmt.Printf("  Mode: %s\n", server.mode)
	fmt.Println("  [Awaiting RDMA transport integration]")

	// Stats reporter goroutine
	go server.reportStats(ctx)

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived %v, shutting down...\n", sig)
		cancel()
	case <-ctx.Done():
	}

	server.printFinalStats()
	return nil
}

func (s *BenchServer) reportStats(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("  [stats] recv=%d sent=%d errors=%d\n",
				s.stats.MessagesReceived.Load(),
				s.stats.MessagesSent.Load(),
				s.stats.Errors.Load())
		}
	}
}

func (s *BenchServer) printFinalStats() {
	fmt.Printf("\nFinal stats:\n")
	fmt.Printf("  Messages received: %d\n", s.stats.MessagesReceived.Load())
	fmt.Printf("  Messages sent: %d\n", s.stats.MessagesSent.Load())
	fmt.Printf("  Bytes received: %d\n", s.stats.BytesReceived.Load())
	fmt.Printf("  Bytes sent: %d\n", s.stats.BytesSent.Load())
	fmt.Printf("  Errors: %d\n", s.stats.Errors.Load())
}
