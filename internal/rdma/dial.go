//go:build !mock

package rdma

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/wua20/golinker/api"
)

// Dial establishes an RDMA connection to addr (host:port) and returns a
// ready-to-use *Conn. It performs the full CM handshake internally:
// resolve-addr → create PD/CQs → resolve-route → create QP → connect → ESTABLISHED.
//
// Usage:
//
//	conn, err := rdma.Dial(ctx, "10.246.191.103:8629", rdma.DefaultConfig())
//	defer conn.Close()
//	conn.Send(payload)
//	n, err := conn.Recv(buf)
func Dial(ctx context.Context, addr string, cfg Config) (*Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("parsing addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	dialer := &RealCMDialer{}
	qp, _, err := dialer.Dial(ctx, host, port, nil, nil, nil, apiQPConfig(cfg))
	if err != nil {
		dialer.Close()
		return nil, fmt.Errorf("CM dial %s:%d: %w", host, port, err)
	}

	pp, err := NewPingPongFromQP(qp, cfg.BufSize)
	if err != nil {
		dialer.Close()
		return nil, fmt.Errorf("PingPongConn: %w", err)
	}

	conn := &Conn{
		PingPongConn: pp,
		cleanup:      func() { dialer.Close() },
	}
	debugf("Dial: connection ready to %s", addr)
	return conn, nil
}

func apiQPConfig(cfg Config) api.QueuePairConfig {
	return api.QueuePairConfig{
		MaxSendWR:  cfg.QueueDepth,
		MaxRecvWR:  cfg.QueueDepth,
		MaxSendSGE: 1,
		MaxRecvSGE: 1,
	}
}
