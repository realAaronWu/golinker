//go:build !mock

package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lnuma
#include <infiniband/verbs.h>
#include <stdlib.h>
#include <string.h>
#include "hotpath.h"

// golinker_poll_cq wraps ibv_poll_cq for a single work completion.
static inline int golinker_poll_cq(struct ibv_cq *cq, struct ibv_wc *wc) {
    return ibv_poll_cq(cq, 1, wc);
}
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// PingPongConn provides a simple RDMA send/recv data path on top of a
// CM-established QP. It extracts PD and CQs directly from the QP struct
// (qp->pd, qp->send_cq, qp->recv_cq), allocates registered send/recv
// buffers, and exposes blocking Send/Recv operations with CQ polling.
type PingPongConn struct {
	qp     *C.struct_ibv_qp
	pd     *C.struct_ibv_pd
	sendCQ *C.struct_ibv_cq
	recvCQ *C.struct_ibv_cq

	sendBuf unsafe.Pointer
	recvBuf unsafe.Pointer
	sendMR  *C.struct_ibv_mr
	recvMR  *C.struct_ibv_mr

	bufSize int
}

// NewPingPongFromQP creates a PingPongConn from an existing QP (typically
// returned by CMDialer.Dial or CMAcceptor.AcceptConn). It extracts PD and
// CQs from the QP's C struct, allocates send/recv buffers, registers them
// as memory regions, and pre-posts a recv WR.
func NewPingPongFromQP(qp api.QueuePair, bufSize int) (*PingPongConn, error) {
	cQP := (*C.struct_ibv_qp)(qp.Handle())

	debugf("PingPong: init from QP=%p (qp_num=%d), bufSize=%d", cQP, cQP.qp_num, bufSize)
	debugf("PingPong: PD=%p sendCQ=%p recvCQ=%p", cQP.pd, cQP.send_cq, cQP.recv_cq)

	p := &PingPongConn{
		qp:      cQP,
		pd:      cQP.pd,
		sendCQ:  cQP.send_cq,
		recvCQ:  cQP.recv_cq,
		bufSize: bufSize,
	}

	// Allocate send buffer.
	p.sendBuf = C.malloc(C.size_t(bufSize))
	if p.sendBuf == nil {
		return nil, fmt.Errorf("malloc send buffer failed")
	}
	C.memset(p.sendBuf, 0, C.size_t(bufSize))

	// Allocate recv buffer.
	p.recvBuf = C.malloc(C.size_t(bufSize))
	if p.recvBuf == nil {
		C.free(p.sendBuf)
		return nil, fmt.Errorf("malloc recv buffer failed")
	}
	C.memset(p.recvBuf, 0, C.size_t(bufSize))

	// Register send buffer.
	access := C.IBV_ACCESS_LOCAL_WRITE
	p.sendMR = C.ibv_reg_mr(p.pd, p.sendBuf, C.size_t(bufSize), C.int(access))
	if p.sendMR == nil {
		C.free(p.recvBuf)
		C.free(p.sendBuf)
		return nil, fmt.Errorf("ibv_reg_mr send buffer failed")
	}

	// Register recv buffer.
	p.recvMR = C.ibv_reg_mr(p.pd, p.recvBuf, C.size_t(bufSize), C.int(access))
	if p.recvMR == nil {
		C.ibv_dereg_mr(p.sendMR)
		C.free(p.recvBuf)
		C.free(p.sendBuf)
		return nil, fmt.Errorf("ibv_reg_mr recv buffer failed")
	}

	debugf("PingPong: sendBuf=%p sendMR(lkey=%d) recvBuf=%p recvMR(lkey=%d)",
		p.sendBuf, p.sendMR.lkey, p.recvBuf, p.recvMR.lkey)

	// Pre-post a recv WR so we're ready to receive.
	if err := p.postRecv(); err != nil {
		p.Close()
		return nil, fmt.Errorf("initial post recv: %w", err)
	}

	debugf("PingPong: ready (initial recv WR posted)")
	return p, nil
}

// postRecv posts a single recv WR using the recv buffer.
// Uses the C helper to avoid CGo pointer-in-pointer violations.
func (p *PingPongConn) postRecv() error {
	ret := C.golinker_post_recv_one(p.qp, p.recvBuf, C.uint32_t(p.bufSize), p.recvMR, 0)
	if ret != 0 {
		return fmt.Errorf("ibv_post_recv failed: %d", ret)
	}
	return nil
}

// Send copies data into the send buffer and posts a signaled send WR.
// It busy-polls the send CQ until the send completes.
// Uses golinker_post_send_single C helper to avoid CGo pointer violations.
func (p *PingPongConn) Send(data []byte) error {
	length := len(data)
	if length > p.bufSize {
		length = p.bufSize
	}
	if length > 0 {
		C.memcpy(p.sendBuf, unsafe.Pointer(&data[0]), C.size_t(length))
	}

	ret := C.golinker_post_send_single(p.qp, p.sendBuf, C.uint32_t(length),
		p.sendMR, 1, C.IBV_SEND_SIGNALED)
	if ret != 0 {
		return fmt.Errorf("ibv_post_send failed: %d", ret)
	}

	return p.pollSendCQ()
}

// Recv busy-polls the recv CQ until a message arrives. Returns the
// number of bytes received and copies the data into dst. After
// receiving, it re-posts a recv WR for the next message.
func (p *PingPongConn) Recv(dst []byte) (int, error) {
	var wc C.struct_ibv_wc
	for {
		n := C.golinker_poll_cq(p.recvCQ, &wc)
		if n < 0 {
			return 0, fmt.Errorf("ibv_poll_cq (recv) failed: %d", n)
		}
		if n > 0 {
			if wc.status != C.IBV_WC_SUCCESS {
				return 0, fmt.Errorf("recv completion error: status=%d vendor_err=%d",
					wc.status, wc.vendor_err)
			}
			byteLen := int(wc.byte_len)
			if dst != nil && byteLen > 0 {
				copyLen := byteLen
				if copyLen > len(dst) {
					copyLen = len(dst)
				}
				C.memcpy(unsafe.Pointer(&dst[0]), p.recvBuf, C.size_t(copyLen))
			}

			// Re-post recv WR for next message.
			if err := p.postRecv(); err != nil {
				return byteLen, fmt.Errorf("repost recv: %w", err)
			}
			return byteLen, nil
		}
	}
}

// SendRaw sends the content already in the send buffer (no copy).
// Useful for echo: recv into recvBuf, CopyRecvToSend, then SendRaw.
// Uses golinker_post_send_single C helper to avoid CGo pointer violations.
func (p *PingPongConn) SendRaw(length int) error {
	if length > p.bufSize {
		length = p.bufSize
	}

	ret := C.golinker_post_send_single(p.qp, p.sendBuf, C.uint32_t(length),
		p.sendMR, 1, C.IBV_SEND_SIGNALED)
	if ret != 0 {
		return fmt.Errorf("ibv_post_send failed: %d", ret)
	}

	return p.pollSendCQ()
}

// pollSendCQ busy-polls the send CQ until one completion arrives.
func (p *PingPongConn) pollSendCQ() error {
	var wc C.struct_ibv_wc
	for {
		n := C.golinker_poll_cq(p.sendCQ, &wc)
		if n < 0 {
			return fmt.Errorf("ibv_poll_cq (send) failed: %d", n)
		}
		if n > 0 {
			if wc.status != C.IBV_WC_SUCCESS {
				return fmt.Errorf("send completion error: status=%d vendor_err=%d",
					wc.status, wc.vendor_err)
			}
			return nil
		}
	}
}

// RecvInPlace polls for a recv completion and returns the byte length.
// Data remains in the recv buffer; use CopyRecvToSend to copy it to
// the send buffer for echo.
func (p *PingPongConn) RecvInPlace() (int, error) {
	var wc C.struct_ibv_wc
	for {
		n := C.golinker_poll_cq(p.recvCQ, &wc)
		if n < 0 {
			return 0, fmt.Errorf("ibv_poll_cq (recv) failed: %d", n)
		}
		if n > 0 {
			if wc.status != C.IBV_WC_SUCCESS {
				return 0, fmt.Errorf("recv completion error: status=%d vendor_err=%d",
					wc.status, wc.vendor_err)
			}
			byteLen := int(wc.byte_len)
			if err := p.postRecv(); err != nil {
				return byteLen, fmt.Errorf("repost recv: %w", err)
			}
			return byteLen, nil
		}
	}
}

// CopyRecvToSend copies n bytes from the recv buffer to the send buffer.
func (p *PingPongConn) CopyRecvToSend(n int) {
	if n > p.bufSize {
		n = p.bufSize
	}
	C.memcpy(p.sendBuf, p.recvBuf, C.size_t(n))
}

// Close deregisters MRs and frees buffers.
func (p *PingPongConn) Close() error {
	if p.recvMR != nil {
		C.ibv_dereg_mr(p.recvMR)
		p.recvMR = nil
	}
	if p.sendMR != nil {
		C.ibv_dereg_mr(p.sendMR)
		p.sendMR = nil
	}
	if p.recvBuf != nil {
		C.free(p.recvBuf)
		p.recvBuf = nil
	}
	if p.sendBuf != nil {
		C.free(p.sendBuf)
		p.sendBuf = nil
	}
	return nil
}
