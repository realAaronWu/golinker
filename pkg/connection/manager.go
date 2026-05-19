package connection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/wua20/golinker/api"
)

// Errors for manager operations.
var (
	ErrManagerClosed    = errors.New("connection manager: closed")
	ErrConnectionNotFound = errors.New("connection manager: connection not found")
)

// ManagerConfig holds configuration for the connection manager.
type ManagerConfig struct {
	Verbs      api.Verbs
	CMChannel  api.CMEventChannel
	SendPool   api.SendBufferPool
	RecvPool   api.RecvBufferPool
	CQPoller   api.CQPoller
	QueueDepth int
}

// Manager implements api.ConnectionManager.
type Manager struct {
	cfg         ManagerConfig
	connections sync.Map // map[uint64]*Conn
	nextID      atomic.Uint64
	acceptCh    chan *Conn
	closed      atomic.Bool
	closeDone   chan struct{}
}

// NewManager creates a new connection manager.
func NewManager(cfg ManagerConfig) *Manager {
	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = 64
	}
	return &Manager{
		cfg:       cfg,
		acceptCh:  make(chan *Conn, 64),
		closeDone: make(chan struct{}),
	}
}

// Accept waits for and returns an incoming connection.
func (m *Manager) Accept(ctx context.Context) (api.Connection, error) {
	if m.closed.Load() {
		return nil, ErrManagerClosed
	}

	select {
	case conn, ok := <-m.acceptCh:
		if !ok {
			return nil, ErrManagerClosed
		}
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Connect initiates an outbound connection to the given address.
func (m *Manager) Connect(ctx context.Context, addr string) (api.Connection, error) {
	if m.closed.Load() {
		return nil, ErrManagerClosed
	}

	id := m.nextID.Add(1)
	deps := ConnDeps{
		Verbs:    m.cfg.Verbs,
		QP:       nil, // QP will be created by CM event handling in real implementation
		SendPool: m.cfg.SendPool,
		RecvPool: m.cfg.RecvPool,
	}

	conn := NewConn(id, addr, deps)
	conn.SetState(api.StateConnecting)

	// In a real implementation, we would:
	// 1. Resolve the address via CM
	// 2. Wait for route resolution
	// 3. Create QP and connect
	// For mock/test purposes, transition directly to Connected
	conn.SetState(api.StateConnected)

	m.connections.Store(id, conn)
	return conn, nil
}

// GetConnection retrieves a connection by ID.
func (m *Manager) GetConnection(id uint64) (api.Connection, bool) {
	val, ok := m.connections.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Conn), true
}

// Close shuts down all connections.
func (m *Manager) Close() error {
	if m.closed.Swap(true) {
		return nil // already closed
	}

	// Close the accept channel
	close(m.acceptCh)

	// Close all connections
	m.connections.Range(func(key, value any) bool {
		conn := value.(*Conn)
		conn.Close()
		m.connections.Delete(key)
		return true
	})

	close(m.closeDone)
	return nil
}

// allocateConnID generates a new unique connection ID.
func (m *Manager) allocateConnID() uint64 {
	return m.nextID.Add(1)
}

// storeConnection stores a connection in the map.
func (m *Manager) storeConnection(conn *Conn) {
	m.connections.Store(conn.ID(), conn)
}

// Ensure Manager implements api.ConnectionManager.
var _ api.ConnectionManager = (*Manager)(nil)
