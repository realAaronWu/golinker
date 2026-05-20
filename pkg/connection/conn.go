package connection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// Errors for connection operations.
var (
	ErrNotConnected   = errors.New("connection: not in connected state")
	ErrConnClosed     = errors.New("connection: closed")
	ErrRecvQueueFull  = errors.New("connection: receive queue full")
)

// ConnDeps holds dependencies for a connection.
type ConnDeps struct {
	Verbs    api.Verbs
	QP       api.QueuePair
	SendPool api.SendBufferPool
	RecvPool api.RecvBufferPool
	CMID     unsafe.Pointer // rdma_cm_id for this connection
	Dialer   api.CMDialer   // for disconnect on close (may be nil)
}

// Conn implements api.Connection for a single RDMA connection.
type Conn struct {
	id         uint64
	remoteAddr string
	deps       ConnDeps

	state atomic.Int32

	mu             sync.Mutex
	stateCallbacks []func(old, new api.ConnectionState)

	recvCh chan *api.Message
	done   chan struct{}
}

// NewConn creates a new connection in StateInit.
func NewConn(id uint64, remoteAddr string, deps ConnDeps) *Conn {
	c := &Conn{
		id:         id,
		remoteAddr: remoteAddr,
		deps:       deps,
		recvCh:     make(chan *api.Message, 128),
		done:       make(chan struct{}),
	}
	c.state.Store(int32(api.StateInit))
	return c
}

// ID returns the connection identifier.
func (c *Conn) ID() uint64 {
	return c.id
}

// RemoteAddr returns the remote address string.
func (c *Conn) RemoteAddr() string {
	return c.remoteAddr
}

// State returns the current connection state (lock-free).
func (c *Conn) State() api.ConnectionState {
	return api.ConnectionState(c.state.Load())
}

// Send posts a send work request. Only valid in StateConnected.
func (c *Conn) Send(msg *api.Message) error {
	if c.State() != api.StateConnected {
		return ErrNotConnected
	}

	wr := &api.SendWR{
		WRID:   c.id,
		Opcode: 0, // IBV_WR_SEND
	}

	if msg.Buffer != nil {
		wr.SGList = []api.SGE{
			{
				Addr:   uint64(uintptr(msg.Buffer.Addr)),
				Length: uint32(msg.Length),
				LKey:   msg.Buffer.LKey,
			},
		}
	}

	wr.ImmData = msg.ImmData

	return c.deps.Verbs.PostSend(c.deps.QP, wr)
}

// Recv blocks until a message is received or the context is cancelled.
func (c *Conn) Recv(ctx context.Context) (*api.Message, error) {
	select {
	case msg, ok := <-c.recvCh:
		if !ok {
			return nil, ErrConnClosed
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrConnClosed
	}
}

// Close initiates graceful disconnect.
func (c *Conn) Close() error {
	currentState := c.State()
	if currentState == api.StateClosed || currentState == api.StateDraining {
		return nil
	}

	// Best-effort CM disconnect before draining
	if c.deps.Dialer != nil && c.deps.CMID != nil {
		_ = c.deps.Dialer.Disconnect(c.deps.CMID)
	}

	// Transition to Draining
	c.transition(api.StateDraining)

	// Close the done channel to unblock any Recv calls
	select {
	case <-c.done:
		// already closed
	default:
		close(c.done)
	}

	// Transition to Closed
	c.transition(api.StateClosed)
	return nil
}

// OnStateChange registers a callback for state transitions.
func (c *Conn) OnStateChange(fn func(old, new api.ConnectionState)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stateCallbacks = append(c.stateCallbacks, fn)
}

// Transition changes the connection state and fires callbacks.
func (c *Conn) transition(newState api.ConnectionState) {
	oldState := api.ConnectionState(c.state.Swap(int32(newState)))
	if oldState == newState {
		return
	}

	c.mu.Lock()
	callbacks := make([]func(old, new api.ConnectionState), len(c.stateCallbacks))
	copy(callbacks, c.stateCallbacks)
	c.mu.Unlock()

	for _, fn := range callbacks {
		fn(oldState, newState)
	}
}

// SetState transitions to a new state (exported for manager use).
func (c *Conn) SetState(newState api.ConnectionState) {
	c.transition(newState)
}

// DeliverRecv delivers a received message to the connection's receive channel.
// Called by the completion handler.
func (c *Conn) DeliverRecv(msg *api.Message) {
	select {
	case c.recvCh <- msg:
	default:
		// Drop message if channel is full (backpressure)
	}
}

// Ensure Conn implements api.Connection.
var _ api.Connection = (*Conn)(nil)
