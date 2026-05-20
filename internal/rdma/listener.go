//go:build !mock

package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lnuma
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
*/
import "C"

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// Listener accepts incoming RDMA connections on a CM event channel.
// It encapsulates the full CM accept handshake (CONNECT_REQUEST →
// create PD/CQs/QP → rdma_accept → ESTABLISHED → PingPongConn).
//
// Usage:
//
//	ln, err := rdma.Listen("0.0.0.0:8629", rdma.DefaultConfig())
//	defer ln.Close()
//	for {
//	    conn, err := ln.Accept(ctx)
//	    go handleConn(conn)
//	}
type Listener struct {
	ch  *RealCMEventChannel
	cfg Config

	// pending tracks connections that have been accepted (rdma_accept sent)
	// but haven't received ESTABLISHED yet. Keyed by CM ID pointer.
	pending map[uintptr]api.QueuePair
}

// Listen binds to addr (host:port) and starts listening for RDMA connections.
func Listen(addr string, cfg Config) (*Listener, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("parsing addr %q: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("invalid port %q: %w", portStr, err)
	}

	ch := &RealCMEventChannel{}
	if err := ch.Listen(context.Background(), host, port); err != nil {
		return nil, fmt.Errorf("CM listen %s:%d: %w", host, port, err)
	}

	debugf("Listener: ready on %s", addr)
	return &Listener{ch: ch, cfg: cfg, pending: make(map[uintptr]api.QueuePair)}, nil
}

// Accept waits for the next fully-established RDMA connection and returns
// a ready-to-use *Conn. It handles the CM event state machine internally:
// CONNECT_REQUEST → AcceptConn → ESTABLISHED → PingPongConn.
func (ln *Listener) Accept(ctx context.Context) (*Conn, error) {
	for {
		event, err := ln.ch.GetEvent(ctx)
		if err != nil {
			return nil, fmt.Errorf("CM event: %w", err)
		}

		switch event.Type {
		case api.EventConnectRequest:
			qp, err := ln.ch.AcceptConn(event.ID, nil, nil, nil, api.QueuePairConfig{
				MaxSendWR:  ln.cfg.QueueDepth,
				MaxRecvWR:  ln.cfg.QueueDepth,
				MaxSendSGE: 1,
				MaxRecvSGE: 1,
			})
			if err != nil {
				ln.ch.AckEvent(event)
				debugf("Listener.Accept: AcceptConn failed: %v", err)
				continue
			}
			key := uintptr(event.ID)
			ln.pending[key] = qp
			ln.ch.AckEvent(event)
			debugf("Listener.Accept: CONNECT_REQUEST accepted, waiting ESTABLISHED")

		case api.EventEstablished:
			key := uintptr(event.ID)
			ln.ch.AckEvent(event)
			qp, ok := ln.pending[key]
			if !ok {
				debugf("Listener.Accept: ESTABLISHED for unknown CM ID %v", event.ID)
				continue
			}
			delete(ln.pending, key)

			pp, err := NewPingPongFromQP(qp, ln.cfg.BufSize)
			if err != nil {
				return nil, fmt.Errorf("PingPongConn: %w", err)
			}

			// Build a cleanup function that destroys the CM ID's QP.
			// The PD/CQs are owned by the QP and freed when the QP is destroyed.
			cmID := (*C.struct_rdma_cm_id)(unsafe.Pointer(key))
			conn := &Conn{
				PingPongConn: pp,
				cleanup: func() {
					if cmID.qp != nil {
						C.rdma_destroy_qp(cmID)
					}
				},
			}
			debugf("Listener.Accept: connection ready")
			return conn, nil

		default:
			debugf("Listener.Accept: ignoring event type=%d", event.Type)
			ln.ch.AckEvent(event)
		}
	}
}

// Close stops listening and releases all CM resources.
func (ln *Listener) Close() error {
	return ln.ch.Close()
}

// Addr returns the address the listener is bound to.
func (ln *Listener) Addr() string {
	// Not easily extractable from the C struct after bind; callers
	// already know the address they passed to Listen.
	return ""
}
