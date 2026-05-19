// Package server implements the golinker RDMA server lifecycle.
package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/config"
)

// ServerDeps provides dependency injection for the Server.
type ServerDeps struct {
	Verbs     api.Verbs
	CMChannel api.CMEventChannel
	ConnMgr   api.ConnectionManager
	CQPool    api.CQPool
	BufPool   api.BufferPool
	SendPool  api.SendBufferPool
	RecvPool  api.RecvBufferPool
}

// Server is the top-level golinker RDMA server.
type Server struct {
	cfg      *config.Config
	handler  api.MessageHandler
	connMgr  api.ConnectionManager
	cqPool   api.CQPool
	bufPool  api.BufferPool
	sendPool api.SendBufferPool
	recvPool api.RecvBufferPool

	mu    sync.RWMutex
	conns map[uint64]api.Connection

	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started atomic.Bool
}

// NewServer creates a new Server with the given configuration and dependencies.
func NewServer(cfg *config.Config, deps ServerDeps) (*Server, error) {
	if cfg == nil {
		return nil, errors.New("server: config must not be nil")
	}
	if deps.ConnMgr == nil {
		return nil, errors.New("server: ConnectionManager dependency is required")
	}

	return &Server{
		cfg:      cfg,
		connMgr:  deps.ConnMgr,
		cqPool:   deps.CQPool,
		bufPool:  deps.BufPool,
		sendPool: deps.SendPool,
		recvPool: deps.RecvPool,
		conns:    make(map[uint64]api.Connection),
	}, nil
}

// Start begins listening and accepting connections. It is non-blocking.
func (s *Server) Start(ctx context.Context) error {
	if s.started.Load() {
		return errors.New("server: already started")
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.started.Store(true)

	s.wg.Add(1)
	go s.acceptLoop(ctx)

	return nil
}

// Stop gracefully shuts down the server, draining in-flight work within the ctx deadline.
func (s *Server) Stop(ctx context.Context) error {
	if !s.started.Load() {
		return errors.New("server: not started")
	}

	// Signal all goroutines to stop.
	s.cancel()

	// Wait for goroutines to finish or context deadline.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed.
	case <-ctx.Done():
		// Deadline reached; force-close connections.
	}

	// Close all tracked connections.
	s.mu.Lock()
	for id, conn := range s.conns {
		conn.Close()
		delete(s.conns, id)
	}
	s.mu.Unlock()

	s.started.Store(false)
	return nil
}

// RegisterHandler sets the message handler for incoming messages.
// Must be called before Start.
func (s *Server) RegisterHandler(handler api.MessageHandler) {
	s.handler = handler
}
