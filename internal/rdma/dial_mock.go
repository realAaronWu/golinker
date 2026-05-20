//go:build mock

package rdma

import (
	"context"
)

// Dial creates a mock RDMA connection (loopback, no real RDMA).
func Dial(ctx context.Context, addr string, cfg Config) (*Conn, error) {
	pp, err := NewPingPongFromQP(&MockQP{qpNum: 1}, cfg.BufSize)
	if err != nil {
		return nil, err
	}
	return &Conn{PingPongConn: pp}, nil
}
