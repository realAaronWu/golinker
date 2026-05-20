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
	"github.com/wua20/golinker/internal/rdma"
)

// AtomicStats tracks server statistics concurrently.
type AtomicStats struct {
	MessagesReceived atomic.Int64
	MessagesSent     atomic.Int64
	BytesReceived    atomic.Int64
	BytesSent        atomic.Int64
	Errors           atomic.Int64
}

// executeServer implements the server logic called by the server command.
func executeServer(cmd *cobra.Command, args []string) error {
	rdma.DebugLog = verbose

	mode, _ := cmd.Flags().GetString("mode")
	if mode == "" {
		mode = "echo"
	}

	stats := &AtomicStats{}

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

	ln, err := rdma.Listen(addr, rdma.Config{
		BufSize:    bufferSize,
		QueueDepth: queueDepth,
	})
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("golinker-bench server starting\n")
	fmt.Printf("  Address: %s\n", addr)
	fmt.Printf("  Mode: %s\n", mode)
	fmt.Printf("  Buffer size: %d\n", bufferSize)
	fmt.Printf("  Queue depth: %d\n", queueDepth)
	fmt.Printf("Waiting for connections...\n")

	// Stats reporter
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("  [stats] recv=%d sent=%d errors=%d\n",
					stats.MessagesReceived.Load(),
					stats.MessagesSent.Load(),
					stats.Errors.Load())
			}
		}
	}()

	// Accept loop
	go func() {
		for {
			conn, err := ln.Accept(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("  accept error: %v\n", err)
				return
			}
			fmt.Printf("  Connection established, starting %s loop\n", mode)
			go func() {
				defer conn.Close()
				if mode == "sink" {
					runSinkLoop(ctx, conn, stats)
				} else {
					runEchoLoop(ctx, conn, stats)
				}
			}()
		}
	}()

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived %v, shutting down...\n", sig)
		cancel()
	case <-ctx.Done():
	}

	fmt.Printf("\nFinal stats:\n")
	fmt.Printf("  Messages received: %d\n", stats.MessagesReceived.Load())
	fmt.Printf("  Messages sent: %d\n", stats.MessagesSent.Load())
	fmt.Printf("  Bytes received: %d\n", stats.BytesReceived.Load())
	fmt.Printf("  Bytes sent: %d\n", stats.BytesSent.Load())
	fmt.Printf("  Errors: %d\n", stats.Errors.Load())
	return nil
}

// runEchoLoop receives messages and echoes them back.
func runEchoLoop(ctx context.Context, conn *rdma.Conn, stats *AtomicStats) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := conn.RecvInPlace()
		if err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  echo recv error: %v\n", err)
			return
		}
		stats.MessagesReceived.Add(1)
		stats.BytesReceived.Add(int64(n))

		conn.CopyRecvToSend(n)
		if err := conn.SendRaw(n); err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  echo send error: %v\n", err)
			return
		}
		stats.MessagesSent.Add(1)
		stats.BytesSent.Add(int64(n))
	}
}

// runSinkLoop receives messages and sends a minimal 4-byte ACK.
func runSinkLoop(ctx context.Context, conn *rdma.Conn, stats *AtomicStats) {
	ack := []byte{0x41, 0x43, 0x4B, 0x00} // "ACK\0"
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := conn.RecvInPlace()
		if err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  sink recv error: %v\n", err)
			return
		}
		stats.MessagesReceived.Add(1)
		stats.BytesReceived.Add(int64(n))

		if err := conn.Send(ack); err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  sink send error: %v\n", err)
			return
		}
		stats.MessagesSent.Add(1)
	}
}
