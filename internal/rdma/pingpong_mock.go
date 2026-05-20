//go:build mock

package rdma

import (
	"fmt"

	"github.com/wua20/golinker/api"
)

// PingPongConn is a mock implementation for build tag mock.
// It simulates send/recv with immediate loopback (no real RDMA).
type PingPongConn struct {
	bufSize int
	buf     []byte
}

// NewPingPongFromQP creates a mock PingPongConn.
func NewPingPongFromQP(qp api.QueuePair, bufSize int) (*PingPongConn, error) {
	return &PingPongConn{
		bufSize: bufSize,
		buf:     make([]byte, bufSize),
	}, nil
}

func (p *PingPongConn) Send(data []byte) error {
	copy(p.buf, data)
	return nil
}

func (p *PingPongConn) Recv(dst []byte) (int, error) {
	n := copy(dst, p.buf)
	return n, nil
}

func (p *PingPongConn) SendRaw(length int) error {
	return nil
}

func (p *PingPongConn) RecvInPlace() (int, error) {
	return 0, fmt.Errorf("mock: no data")
}

func (p *PingPongConn) CopyRecvToSend(n int) {}

func (p *PingPongConn) Close() error {
	return nil
}
