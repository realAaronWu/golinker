package cq

import (
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
