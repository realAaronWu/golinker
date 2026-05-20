package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
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

	// Parse addr into host and port for CM listener
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parsing addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	// Initialize CM listener
	cmChannel, cmAcceptor, err := initCMListener(host, port)
	if err != nil {
		return fmt.Errorf("initializing CM listener: %w", err)
	}
	defer cmChannel.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("golinker-bench server starting\n")
	fmt.Printf("  Address: %s\n", addr)
	fmt.Printf("  CM listening: %s:%d\n", host, port)
	fmt.Printf("  Mode: %s\n", mode)
	fmt.Printf("  Buffer size: %d\n", bufferSize)
	fmt.Printf("  Queue depth: %d\n", queueDepth)
	fmt.Printf("Waiting for connections...\n")

	// Stats reporter goroutine
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

	// Accept connections and handle them with direct RDMA data path.
	// Each accepted connection gets its own PingPongConn and goroutine.
	//
	// We track pending connections (accepted but not yet ESTABLISHED) by
	// their CM ID pointer, so interleaved events from concurrent clients
	// are matched correctly.
	go func() {
		type pendingConn struct {
			qp api.QueuePair
		}
		pending := make(map[uintptr]*pendingConn)

		for {
			event, err := cmChannel.GetEvent(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("  CM event error: %v\n", err)
				return
			}

			switch event.Type {
			case api.EventConnectRequest:
				qp, err := cmAcceptor.AcceptConn(event.ID, nil, nil, nil, api.QueuePairConfig{
					MaxSendWR:  queueDepth,
					MaxRecvWR:  queueDepth,
					MaxSendSGE: 1,
					MaxRecvSGE: 1,
				})
				if err != nil {
					fmt.Printf("  accept error: %v\n", err)
					cmChannel.AckEvent(event)
					continue
				}
				// Track this connection; ESTABLISHED will arrive later keyed by the same CM ID.
				key := uintptr(event.ID)
				pending[key] = &pendingConn{qp: qp}
				cmChannel.AckEvent(event)

			case api.EventEstablished:
				key := uintptr(event.ID)
				cmChannel.AckEvent(event)

				pc, ok := pending[key]
				if !ok {
					fmt.Printf("  ESTABLISHED for unknown CM ID %v\n", event.ID)
					continue
				}
				delete(pending, key)

				fmt.Printf("  Connection established, starting %s loop\n", mode)

				ppConn, err := rdma.NewPingPongFromQP(pc.qp, bufferSize)
				if err != nil {
					fmt.Printf("  PingPongConn error: %v\n", err)
					continue
				}

				go func() {
					defer ppConn.Close()
					if mode == "sink" {
						runSinkLoop(ctx, ppConn, stats)
					} else {
						runEchoLoop(ctx, ppConn, stats)
					}
				}()

			default:
				fmt.Printf("  CM event: type=%d\n", event.Type)
				cmChannel.AckEvent(event)
			}
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
func runEchoLoop(ctx context.Context, pp *rdma.PingPongConn, stats *AtomicStats) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := pp.RecvInPlace()
		if err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  echo recv error: %v\n", err)
			return
		}
		stats.MessagesReceived.Add(1)
		stats.BytesReceived.Add(int64(n))

		// Echo: copy recv buffer to send buffer, then send
		pp.CopyRecvToSend(n)
		if err := pp.SendRaw(n); err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  echo send error: %v\n", err)
			return
		}
		stats.MessagesSent.Add(1)
		stats.BytesSent.Add(int64(n))
	}
}

// runSinkLoop receives messages and sends a minimal 4-byte ACK.
func runSinkLoop(ctx context.Context, pp *rdma.PingPongConn, stats *AtomicStats) {
	ack := []byte{0x41, 0x43, 0x4B, 0x00} // "ACK\0"
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := pp.RecvInPlace()
		if err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  sink recv error: %v\n", err)
			return
		}
		stats.MessagesReceived.Add(1)
		stats.BytesReceived.Add(int64(n))

		if err := pp.Send(ack); err != nil {
			stats.Errors.Add(1)
			fmt.Printf("  sink send error: %v\n", err)
			return
		}
		stats.MessagesSent.Add(1)
	}
}
