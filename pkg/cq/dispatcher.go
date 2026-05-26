package cq

import (
	"sync"
	"sync/atomic"

	"github.com/wua20/golinker/api"
)

// ChannelHandler implements api.CompletionHandler by sending completions
// to Go channels. This is useful for integration with higher-level code
// that prefers channel-based consumption.
type ChannelHandler struct {
	completions chan *api.WorkCompletion
	errors      chan error
	dropped     atomic.Uint64
}

// NewChannelHandler creates a new ChannelHandler with the specified buffer sizes.
func NewChannelHandler(bufSize int) *ChannelHandler {
	if bufSize <= 0 {
		bufSize = 64
	}
	return &ChannelHandler{
		completions: make(chan *api.WorkCompletion, bufSize),
		errors:      make(chan error, bufSize),
	}
}

// Completions returns the read-only channel for successful completions.
func (h *ChannelHandler) Completions() <-chan *api.WorkCompletion {
	return h.completions
}

// Errors returns the read-only channel for errors.
func (h *ChannelHandler) Errors() <-chan error {
	return h.errors
}

// Dropped returns the number of completions dropped due to full channel.
func (h *ChannelHandler) Dropped() uint64 {
	return h.dropped.Load()
}

// OnCompletion sends the work completion to the completions channel.
// If the channel is full, the completion is dropped and the drop counter incremented.
func (h *ChannelHandler) OnCompletion(wc *api.WorkCompletion) {
	select {
	case h.completions <- wc:
	default:
		h.dropped.Add(1)
	}
}

// OnError sends the error to the errors channel.
// If the channel is full, the error is dropped.
func (h *ChannelHandler) OnError(wc *api.WorkCompletion, err error) {
	select {
	case h.errors <- err:
	default:
		// Drop if full
	}
}

// Compile-time interface check.
var _ api.CompletionHandler = (*ChannelHandler)(nil)

// SendCompletionHandler routes send completions back to the SendPool and
// an optional callback (typically Aggregator.OnSendComplete). It maintains
// a WRID → Buffer mapping so the correct buffer is released on completion.
type SendCompletionHandler struct {
	sendPool   api.SendBufferPool
	onComplete func() // called after each send completion (e.g. Aggregator.OnSendComplete)

	mu      sync.Mutex
	wridMap map[uint64]*api.Buffer
}

// NewSendCompletionHandler creates a handler that bridges CQ completions to SendPool.
func NewSendCompletionHandler(sendPool api.SendBufferPool, onComplete func()) *SendCompletionHandler {
	return &SendCompletionHandler{
		sendPool:   sendPool,
		onComplete: onComplete,
		wridMap:    make(map[uint64]*api.Buffer),
	}
}

// TrackSend records a buffer associated with a WRID for later release.
func (h *SendCompletionHandler) TrackSend(wrid uint64, buf *api.Buffer) {
	h.mu.Lock()
	h.wridMap[wrid] = buf
	h.mu.Unlock()
}

// OnCompletion handles a successful send completion by releasing the buffer
// back to the pool and calling the onComplete callback.
func (h *SendCompletionHandler) OnCompletion(wc *api.WorkCompletion) {
	h.mu.Lock()
	buf, ok := h.wridMap[wc.WRID]
	if ok {
		delete(h.wridMap, wc.WRID)
	}
	h.mu.Unlock()

	if ok && buf != nil {
		h.sendPool.CompleteSend(buf)
		if h.onComplete != nil {
			h.onComplete()
		}
	}
}

// OnError handles a send completion error by releasing the buffer.
func (h *SendCompletionHandler) OnError(wc *api.WorkCompletion, err error) {
	if wc == nil {
		return
	}
	h.mu.Lock()
	buf, ok := h.wridMap[wc.WRID]
	if ok {
		delete(h.wridMap, wc.WRID)
	}
	h.mu.Unlock()

	if ok && buf != nil {
		h.sendPool.CompleteSend(buf)
	}
}

// SetOnComplete replaces the onComplete callback. Used to wire aggregator
// after initial handler creation.
func (h *SendCompletionHandler) SetOnComplete(fn func()) {
	h.mu.Lock()
	h.onComplete = fn
	h.mu.Unlock()
}

// InFlight returns the number of tracked in-flight sends.
func (h *SendCompletionHandler) InFlight() int {
	h.mu.Lock()
	n := len(h.wridMap)
	h.mu.Unlock()
	return n
}

var _ api.CompletionHandler = (*SendCompletionHandler)(nil)
