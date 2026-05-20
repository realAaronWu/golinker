//go:build mock

package rdma

import (
	"context"
	"fmt"
)

// Listener is a mock RDMA listener for build-tag mock.
type Listener struct {
	cfg    Config
	addr   string
	closed bool
}

// Listen creates a mock listener (no real RDMA).
func Listen(addr string, cfg Config) (*Listener, error) {
	return &Listener{cfg: cfg, addr: addr}, nil
}

// Accept returns a mock Conn with a loopback PingPongConn.
func (ln *Listener) Accept(ctx context.Context) (*Conn, error) {
	if ln.closed {
		return nil, fmt.Errorf("listener closed")
	}
	// Block until context is cancelled (mock has no real connections).
	<-ctx.Done()
	return nil, ctx.Err()
}

// Close stops the mock listener.
func (ln *Listener) Close() error {
	ln.closed = true
	return nil
}

// Addr returns the address the listener is bound to.
func (ln *Listener) Addr() string {
	return ln.addr
}
