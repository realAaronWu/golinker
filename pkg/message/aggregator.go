package message

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// DefaultBufferSize is the default maximum buffer capacity.
const DefaultBufferSize = 12288

// DefaultSendThreshold is the default flush threshold.
const DefaultSendThreshold = 9216

// AggregatorConfig holds configuration for the Aggregator.
type AggregatorConfig struct {
	BufferSize      int  // max buffer capacity (default 12288)
	SendThreshold   int  // flush threshold (default 9216)
	EnableAggregate bool // if false, always immediate send
}

// AggregatorStats contains aggregator metrics.
type AggregatorStats struct {
	Flushed      uint64 // total number of flushes performed
	Sent         uint64 // total number of messages sent
	PendingCount int    // current number of pending messages
	PendingSize  int    // current total bytes of pending messages
}

// Aggregator batches small messages for efficient RDMA SEND.
type Aggregator struct {
	mu              sync.Mutex
	pending         [][]byte      // messages waiting to be flushed
	pendingSize     int           // total bytes of pending messages (payload only)
	ongoingSends    atomic.Int32  // number of in-flight sends
	isBusy          *atomic.Bool  // shared signal from buffer pool
	sendPool        api.SendBufferPool
	conn            api.Connection
	bufferSize      int           // max buffer capacity
	sendThreshold   int           // flush threshold
	enableAggregate bool          // whether aggregation is enabled
	flushed         atomic.Uint64 // metrics: total flushes
	sent            atomic.Uint64 // metrics: total messages sent
}

// NewAggregator creates a new Aggregator with the given connection, buffer pool, and config.
func NewAggregator(conn api.Connection, sendPool api.SendBufferPool, cfg AggregatorConfig) *Aggregator {
	bufSize := cfg.BufferSize
	if bufSize <= 0 {
		bufSize = DefaultBufferSize
	}
	threshold := cfg.SendThreshold
	if threshold <= 0 {
		threshold = DefaultSendThreshold
	}

	a := &Aggregator{
		sendPool:        sendPool,
		conn:            conn,
		bufferSize:      bufSize,
		sendThreshold:   threshold,
		enableAggregate: cfg.EnableAggregate,
		isBusy:          &atomic.Bool{},
	}
	return a
}

// SetBusy sets the isBusy signal (typically called by the buffer pool monitor).
func (a *Aggregator) SetBusy(busy *atomic.Bool) {
	a.mu.Lock()
	a.isBusy = busy
	a.mu.Unlock()
}

// Send is the main entry point. If !isBusy or !enableAggregate, sends immediately.
// Otherwise, aggregates and checks flush triggers.
func (a *Aggregator) Send(data []byte) error {
	if !a.enableAggregate || !a.isBusy.Load() {
		return a.immediateSend(data)
	}
	return a.aggregateSend(data)
}

// immediateSend performs a direct send without batching.
func (a *Aggregator) immediateSend(data []byte) error {
	buf, err := a.sendPool.AcquireForSend()
	if err != nil {
		return err
	}

	totalNeeded := BatchHeaderSize + MsgHeaderSize + len(data)
	if totalNeeded > buf.Length {
		a.sendPool.CompleteSend(buf)
		return ErrBufferTooSmall
	}

	dest := unsafe.Slice((*byte)(buf.Addr), buf.Length)
	n := PackSingle(dest, data)

	msg := &api.Message{
		Buffer: buf,
		Length: n,
	}

	a.ongoingSends.Add(1)
	a.sent.Add(1)
	return a.conn.Send(msg)
}

// aggregateSend adds data to the pending batch and checks flush triggers.
func (a *Aggregator) aggregateSend(data []byte) error {
	a.mu.Lock()

	msgFramedSize := MsgHeaderSize + len(data)

	// Overflow trigger: if adding this message would exceed buffer capacity,
	// flush existing pending messages first.
	currentBatchSize := BatchHeaderSize + a.pendingSize
	if a.pendingSize > 0 && currentBatchSize+msgFramedSize > a.bufferSize {
		if err := a.flushLocked(); err != nil {
			a.mu.Unlock()
			return err
		}
	}

	// Append message to pending
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	a.pending = append(a.pending, dataCopy)
	a.pendingSize += msgFramedSize

	// Threshold trigger: pendingSize + BatchHeaderSize > sendThreshold
	if BatchHeaderSize+a.pendingSize > a.sendThreshold {
		err := a.flushLocked()
		a.mu.Unlock()
		return err
	}

	// Idle trigger: if no ongoing sends, flush immediately
	if a.ongoingSends.Load() == 0 {
		err := a.flushLocked()
		a.mu.Unlock()
		return err
	}

	a.mu.Unlock()
	return nil
}

// Flush acquires a send buffer, packs all pending messages, and posts a send.
// Caller must NOT hold mu.
func (a *Aggregator) Flush() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.flushLocked()
}

// flushLocked performs the flush while holding mu.
func (a *Aggregator) flushLocked() error {
	if len(a.pending) == 0 {
		return nil
	}

	messages := a.pending
	a.pending = nil
	a.pendingSize = 0

	buf, err := a.sendPool.AcquireForSend()
	if err != nil {
		// Put messages back on failure
		a.pending = messages
		for _, m := range messages {
			a.pendingSize += MsgHeaderSize + len(m)
		}
		return err
	}

	dest := unsafe.Slice((*byte)(buf.Addr), buf.Length)
	n := PackBatch(dest, messages)

	msg := &api.Message{
		Buffer: buf,
		Length: n,
	}

	a.ongoingSends.Add(1)
	a.flushed.Add(1)
	a.sent.Add(uint64(len(messages)))

	return a.conn.Send(msg)
}

// OnSendComplete decrements ongoingSends. If it reaches 0 and there are
// pending messages, triggers an idle flush.
func (a *Aggregator) OnSendComplete() {
	newVal := a.ongoingSends.Add(-1)
	if newVal == 0 {
		a.mu.Lock()
		if len(a.pending) > 0 {
			_ = a.flushLocked()
		}
		a.mu.Unlock()
	}
}

// Stats returns current aggregator metrics.
func (a *Aggregator) Stats() AggregatorStats {
	a.mu.Lock()
	pendingCount := len(a.pending)
	pendingSize := a.pendingSize
	a.mu.Unlock()

	return AggregatorStats{
		Flushed:      a.flushed.Load(),
		Sent:         a.sent.Load(),
		PendingCount: pendingCount,
		PendingSize:  pendingSize,
	}
}
