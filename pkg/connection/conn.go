package connection

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/cq"
	"github.com/wua20/golinker/pkg/message"
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

// Conn implements api.Connection and api.CompletionHandler for a single RDMA connection.
type Conn struct {
	id         uint64
	remoteAddr string
	deps       ConnDeps

	state atomic.Int32

	mu             sync.Mutex
	stateCallbacks []func(old, new api.ConnectionState)

	recvCh chan *api.Message
	done   chan struct{}

	// Send path: tracks in-flight send buffers via WRID
	sendHandler *cq.SendCompletionHandler
	nextWRID    atomic.Uint64

	// Aggregation: batches messages under load, immediate when idle
	aggregator *message.Aggregator

	// Recv path: tracks posted recv buffers (WRID → backing slice)
	recvBufsMu sync.Mutex
	recvBufs   map[uint64][]byte
}

// NewConn creates a new connection in StateInit.
func NewConn(id uint64, remoteAddr string, deps ConnDeps) *Conn {
	c := &Conn{
		id:         id,
		remoteAddr: remoteAddr,
		deps:       deps,
		recvCh:     make(chan *api.Message, 128),
		done:       make(chan struct{}),
		recvBufs:   make(map[uint64][]byte),
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

// SetSendHandler registers the send completion handler for buffer tracking.
func (c *Conn) SetSendHandler(h *cq.SendCompletionHandler) {
	c.sendHandler = h
}

// GetSendHandler returns the send completion handler (for testing/integration).
func (c *Conn) GetSendHandler() api.CompletionHandler {
	if c.sendHandler == nil {
		return nil
	}
	return c.sendHandler
}

// TrackRecvBuffer manually registers a recv buffer for WRID-based lookup.
// Used by tests and by PostRecvBuffers.
func (c *Conn) TrackRecvBuffer(wrid uint64, data []byte) {
	c.recvBufsMu.Lock()
	c.recvBufs[wrid] = data
	c.recvBufsMu.Unlock()
}

// Send posts a send work request. Only valid in StateConnected.
// The caller must have already packed the wire format into msg.Buffer.
// Send assigns a unique WRID, sets IBV_SEND_SIGNALED, and tracks the buffer
// for release when the CQ completion arrives.
func (c *Conn) Send(msg *api.Message) error {
	if c.State() != api.StateConnected {
		return ErrNotConnected
	}

	wrid := c.nextWRID.Add(1)

	// Track the buffer for completion-driven release
	if c.sendHandler != nil && msg.Buffer != nil {
		c.sendHandler.TrackSend(wrid, msg.Buffer)
	}

	wr := &api.SendWR{
		WRID:      wrid,
		Opcode:    0, // IBV_WR_SEND
		SendFlags: api.SendSignaled,
		ImmData:   msg.ImmData,
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

	if err := c.deps.Verbs.PostSend(c.deps.QP, wr); err != nil {
		// Untrack on failure so the buffer isn't leaked in the handler
		if c.sendHandler != nil && msg.Buffer != nil {
			c.sendHandler.OnError(&api.WorkCompletion{WRID: wrid}, err)
		}
		return err
	}
	return nil
}

// SetAggregator configures the aggregation engine for this connection.
// Must be called before the connection enters StateConnected.
func (c *Conn) SetAggregator(agg *message.Aggregator) {
	c.aggregator = agg
}

// SendPayload sends application data through the aggregation layer.
// Routes through the aggregator if configured; otherwise falls back to
// an immediate send using the send buffer pool.
func (c *Conn) SendPayload(data []byte) error {
	if c.State() != api.StateConnected {
		return ErrNotConnected
	}
	if c.aggregator != nil {
		return c.aggregator.Send(data)
	}
	// Fallback: immediate send without aggregator
	if c.deps.SendPool == nil {
		return errors.New("connection: no send pool configured")
	}
	buf, err := c.deps.SendPool.AcquireForSend()
	if err != nil {
		return err
	}
	totalNeeded := message.CmdHeaderSize + message.AppHeaderSize + len(data)
	if totalNeeded > buf.Length {
		c.deps.SendPool.CompleteSend(buf)
		return message.ErrBufferTooSmall
	}
	dest := unsafe.Slice((*byte)(buf.Addr), buf.Length)
	n := message.PackSingle(dest, data)
	return c.Send(&api.Message{Buffer: buf, Length: n})
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

// PostRecvBuffers allocates recv buffers, tracks their WRID→data mapping,
// and posts them to the QP. Must be called before the connection is advertised
// as usable (design rule: recv WRs posted before StateConnected).
func (c *Conn) PostRecvBuffers(count int) error {
	if c.deps.RecvPool == nil || c.deps.QP == nil {
		return nil
	}
	for i := 0; i < count; i++ {
		buf, err := c.deps.RecvPool.Alloc(0)
		if err != nil {
			return err
		}

		data := unsafe.Slice((*byte)(buf.Addr), buf.Length)
		wrid := uint64(uintptr(buf.Addr))

		c.recvBufsMu.Lock()
		c.recvBufs[wrid] = data
		c.recvBufsMu.Unlock()

		wr := &api.RecvWR{
			WRID: wrid,
			SGList: []api.SGE{{
				Addr:   uint64(uintptr(buf.Addr)),
				Length: uint32(buf.Length),
				LKey:   buf.LKey,
			}},
		}
		if err := c.deps.Verbs.PostRecv(c.deps.QP, wr); err != nil {
			c.recvBufsMu.Lock()
			delete(c.recvBufs, wrid)
			c.recvBufsMu.Unlock()
			c.deps.RecvPool.Free(buf)
			return err
		}
	}
	return nil
}

// OnCompletion handles a work completion from the CQ poller.
// For recv completions: looks up the tracked buffer, unpacks wire format,
// delivers messages, and re-posts one buffer.
func (c *Conn) OnCompletion(wc *api.WorkCompletion) {
	if wc.Opcode != api.WCRecv && wc.Opcode != api.WCRecvRdmaWithImm {
		return // send completions handled by SendCompletionHandler
	}

	if wc.ByteLen == 0 {
		c.replenishRecv()
		return
	}

	// Look up the recv buffer by WRID
	c.recvBufsMu.Lock()
	data, ok := c.recvBufs[wc.WRID]
	delete(c.recvBufs, wc.WRID)
	c.recvBufsMu.Unlock()

	if !ok {
		c.OnError(wc, errors.New("connection: recv completion for unknown WRID"))
		c.replenishRecv()
		return
	}

	messages, err := message.UnpackBatch(data[:wc.ByteLen], int(wc.ByteLen))
	if err != nil {
		c.OnError(wc, err)
		c.replenishRecv()
		return
	}

	for _, payload := range messages {
		msgBuf := make([]byte, len(payload))
		copy(msgBuf, payload)
		msg := &api.Message{
			Buffer: &api.Buffer{
				Addr:   unsafe.Pointer(&msgBuf[0]),
				Length: len(msgBuf),
			},
			Length: len(msgBuf),
		}
		if wc.HasIMM {
			msg.ImmData = wc.IMM
		}
		c.DeliverRecv(msg)
	}

	c.replenishRecv()
}

// OnError handles a work completion error (required by api.CompletionHandler).
func (c *Conn) OnError(_ *api.WorkCompletion, _ error) {
	// TODO: log error, potentially transition to error state
}

// replenishRecv re-posts one recv buffer to keep the receive pipeline full.
func (c *Conn) replenishRecv() {
	_ = c.PostRecvBuffers(1)
}

// Ensure Conn implements api.Connection and api.CompletionHandler.
var _ api.Connection = (*Conn)(nil)
var _ api.CompletionHandler = (*Conn)(nil)
