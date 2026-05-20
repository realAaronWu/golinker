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
	"github.com/wua20/golinker/pkg/buffer"
	"github.com/wua20/golinker/pkg/config"
	"github.com/wua20/golinker/pkg/connection"
	"github.com/wua20/golinker/pkg/cq"
	pkgserver "github.com/wua20/golinker/pkg/server"
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

// EchoHandler echoes received messages back to the sender.
type EchoHandler struct{ stats *AtomicStats }

// Handle returns the received message as the response.
func (h *EchoHandler) Handle(conn api.Connection, msg *api.Message) (*api.Message, error) {
	h.stats.MessagesReceived.Add(1)
	h.stats.BytesReceived.Add(int64(msg.Length))
	h.stats.MessagesSent.Add(1)
	h.stats.BytesSent.Add(int64(msg.Length))
	return msg, nil
}

// SinkHandler consumes received messages and returns a minimal acknowledgment.
type SinkHandler struct{ stats *AtomicStats }

// Handle consumes the message and returns a minimal 4-byte response.
func (h *SinkHandler) Handle(conn api.Connection, msg *api.Message) (*api.Message, error) {
	h.stats.MessagesReceived.Add(1)
	h.stats.BytesReceived.Add(int64(msg.Length))
	h.stats.MessagesSent.Add(1)
	return &api.Message{Length: 4}, nil
}

// BenchServer represents the benchmark server.
type BenchServer struct {
	addr     string
	mode     ResponseMode
	stats    *AtomicStats
	verbs    api.Verbs
	pd       api.ProtectionDomain
	sendPool *buffer.Pool
	recvPool *buffer.Pool
	cqPool   *cq.Pool
	connMgr  *connection.Manager
	srv      *pkgserver.Server
}

// newHandler returns a MessageHandler matching the server's response mode.
func (s *BenchServer) newHandler() api.MessageHandler {
	switch s.mode {
	case ModeSink:
		return &SinkHandler{stats: s.stats}
	default:
		return &EchoHandler{stats: s.stats}
	}
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

	// Initialize RDMA stack
	verbs, pd, err := initVerbs(device)
	if err != nil {
		return fmt.Errorf("initializing RDMA verbs: %w", err)
	}
	server.verbs = verbs
	server.pd = pd

	sendPool, err := newBufferPool(verbs, pd, bufferSize, 128, numaNode)
	if err != nil {
		return fmt.Errorf("creating send buffer pool: %w", err)
	}
	server.sendPool = sendPool
	defer sendPool.Close()

	recvPool, err := newBufferPool(verbs, pd, bufferSize, 128, numaNode)
	if err != nil {
		return fmt.Errorf("creating recv buffer pool: %w", err)
	}
	server.recvPool = recvPool
	defer recvPool.Close()

	cqp, err := newCQPool(verbs, cqNumber, pollMode)
	if err != nil {
		return fmt.Errorf("creating CQ pool: %w", err)
	}
	server.cqPool = cqp
	defer cqp.Close()

	// Parse addr into host and port for CM listener
	host, portStr, err := net.SplitHostPort(server.addr)
	if err != nil {
		return fmt.Errorf("parsing addr %q: %w", server.addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	// Initialize CM listener (event channel + acceptor)
	cmChannel, cmAcceptor, err := initCMListener(host, port)
	if err != nil {
		return fmt.Errorf("initializing CM listener: %w", err)
	}
	defer cmChannel.Close()

	// Assign send and recv CQs from the pool for QP creation
	_, sendCQ, err := cqp.Assign()
	if err != nil {
		return fmt.Errorf("assigning send CQ: %w", err)
	}
	_, recvCQ, err := cqp.Assign()
	if err != nil {
		return fmt.Errorf("assigning recv CQ: %w", err)
	}

	// Create connection manager with CM wiring
	connMgr := connection.NewManager(connection.ManagerConfig{
		Verbs:      verbs,
		CMChannel:  cmChannel,
		CMAcceptor: cmAcceptor,
		PD:         pd,
		SendCQ:     sendCQ,
		RecvCQ:     recvCQ,
		QPConfig: api.QueuePairConfig{
			MaxSendWR:  queueDepth,
			MaxRecvWR:  queueDepth,
			MaxSendSGE: 1,
			MaxRecvSGE: 1,
		},
		SendPool:   nil, // buffer.Pool does not implement api.SendBufferPool yet
		RecvPool:   nil, // buffer.Pool does not implement api.RecvBufferPool yet
		QueueDepth: queueDepth,
	})
	server.connMgr = connMgr
	defer connMgr.Close()

	// Build server config
	cfg := config.DefaultConfig()
	cfg.Endpoint = server.addr
	cfg.CQNumber = cqNumber
	cfg.PollMode = parsePollMode(pollMode)
	cfg.BufferSize = bufferSize

	// Create and configure the pkg/server
	srv, err := pkgserver.NewServer(cfg, pkgserver.ServerDeps{
		Verbs:   verbs,
		ConnMgr: connMgr,
		CQPool:  cqp,
		BufPool: sendPool,
	})
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}
	server.srv = srv

	srv.RegisterHandler(server.newHandler())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start CM event loop to accept incoming connections
	go connMgr.RunCMEventLoop(ctx)

	if err := srv.Start(ctx); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}
	defer srv.Stop(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("golinker-bench server starting\n")
	fmt.Printf("  Address: %s\n", server.addr)
	fmt.Printf("  CM listening: %s:%d\n", host, port)
	fmt.Printf("  Mode: %s\n", server.mode)
	fmt.Printf("  Buffer size: %d\n", bufferSize)
	fmt.Printf("  Queue depth: %d\n", queueDepth)
	fmt.Printf("  CQ pollers: %d\n", cqNumber)
	fmt.Printf("  Poll mode: %s\n", pollMode)

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
