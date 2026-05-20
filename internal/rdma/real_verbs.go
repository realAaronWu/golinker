//go:build !mock

package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lnuma
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include "hotpath.h"
*/
import "C"

import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// ---------------------------------------------------------------------------
// Wrapper types implementing the api interfaces over real C RDMA objects
// ---------------------------------------------------------------------------

// RealPD wraps *C.struct_ibv_pd as api.ProtectionDomain.
type RealPD struct {
	pd *C.struct_ibv_pd
}

func (p *RealPD) Handle() unsafe.Pointer { return unsafe.Pointer(p.pd) }

// RealMR wraps *C.struct_ibv_mr as api.MemoryRegion.
type RealMR struct {
	mr *C.struct_ibv_mr
}

func (m *RealMR) Addr() unsafe.Pointer { return unsafe.Pointer(m.mr.addr) }
func (m *RealMR) Length() int           { return int(m.mr.length) }
func (m *RealMR) LKey() uint32          { return uint32(m.mr.lkey) }
func (m *RealMR) RKey() uint32          { return uint32(m.mr.rkey) }

// RealCQ wraps *C.struct_ibv_cq as api.CompletionQueue.
type RealCQ struct {
	cq   *C.struct_ibv_cq
	size int
}

func (c *RealCQ) Handle() unsafe.Pointer { return unsafe.Pointer(c.cq) }
func (c *RealCQ) Size() int              { return c.size }

// RealQP wraps *C.struct_ibv_qp as api.QueuePair.
type RealQP struct {
	qp    *C.struct_ibv_qp
	state api.QueuePairState
}

func (q *RealQP) Handle() unsafe.Pointer    { return unsafe.Pointer(q.qp) }
func (q *RealQP) QPNum() uint32             { return uint32(q.qp.qp_num) }
func (q *RealQP) State() api.QueuePairState { return q.state }

func (q *RealQP) ModifyToInit() error {
	var attr C.struct_ibv_qp_attr
	attr.qp_state = C.IBV_QPS_INIT
	attr.pkey_index = 0
	attr.port_num = 1
	attr.qp_access_flags = C.IBV_ACCESS_LOCAL_WRITE | C.IBV_ACCESS_REMOTE_WRITE | C.IBV_ACCESS_REMOTE_READ

	mask := C.int(C.IBV_QP_STATE | C.IBV_QP_PKEY_INDEX | C.IBV_QP_PORT | C.IBV_QP_ACCESS_FLAGS)
	ret := C.ibv_modify_qp(q.qp, &attr, mask)
	if ret != 0 {
		return fmt.Errorf("ibv_modify_qp to INIT failed: %d", ret)
	}
	q.state = api.QPStateInit
	return nil
}

func (q *RealQP) ModifyToRTR(destQPN uint32, destLID uint16, destGID [16]byte) error {
	var attr C.struct_ibv_qp_attr
	attr.qp_state = C.IBV_QPS_RTR
	attr.path_mtu = C.IBV_MTU_4096
	attr.dest_qp_num = C.uint32_t(destQPN)
	attr.rq_psn = 0
	attr.max_dest_rd_atomic = 1
	attr.min_rnr_timer = 12

	attr.ah_attr.dlid = C.uint16_t(destLID)
	attr.ah_attr.sl = 0
	attr.ah_attr.src_path_bits = 0
	attr.ah_attr.port_num = 1

	// If destGID is non-zero, enable GRH (required for RoCE).
	hasGID := false
	for _, b := range destGID {
		if b != 0 {
			hasGID = true
			break
		}
	}
	if hasGID {
		attr.ah_attr.is_global = 1
		for i := 0; i < 16; i++ {
			*(*byte)(unsafe.Add(unsafe.Pointer(&attr.ah_attr.grh.dgid), i)) = destGID[i]
		}
		attr.ah_attr.grh.sgid_index = 0
		attr.ah_attr.grh.hop_limit = 64
		attr.ah_attr.grh.traffic_class = 0
	}

	mask := C.int(C.IBV_QP_STATE | C.IBV_QP_AV | C.IBV_QP_PATH_MTU |
		C.IBV_QP_DEST_QPN | C.IBV_QP_RQ_PSN |
		C.IBV_QP_MAX_DEST_RD_ATOMIC | C.IBV_QP_MIN_RNR_TIMER)
	ret := C.ibv_modify_qp(q.qp, &attr, mask)
	if ret != 0 {
		return fmt.Errorf("ibv_modify_qp to RTR failed: %d", ret)
	}
	q.state = api.QPStateRTR
	return nil
}

func (q *RealQP) ModifyToRTS() error {
	var attr C.struct_ibv_qp_attr
	attr.qp_state = C.IBV_QPS_RTS
	attr.timeout = 14
	attr.retry_cnt = 7
	attr.rnr_retry = 7
	attr.sq_psn = 0
	attr.max_rd_atomic = 1

	mask := C.int(C.IBV_QP_STATE | C.IBV_QP_TIMEOUT | C.IBV_QP_RETRY_CNT |
		C.IBV_QP_RNR_RETRY | C.IBV_QP_SQ_PSN | C.IBV_QP_MAX_QP_RD_ATOMIC)
	ret := C.ibv_modify_qp(q.qp, &attr, mask)
	if ret != 0 {
		return fmt.Errorf("ibv_modify_qp to RTS failed: %d", ret)
	}
	q.state = api.QPStateRTS
	return nil
}

// ---------------------------------------------------------------------------
// RealVerbs implements api.Verbs over libibverbs / librdmacm
// ---------------------------------------------------------------------------

// RealVerbs wraps the C libibverbs functions behind the api.Verbs interface.
type RealVerbs struct {
	ctx *C.struct_ibv_context
	pd  *C.struct_ibv_pd // cached for cleanup
}

// NewRealVerbs creates an uninitialised RealVerbs. Call OpenDevice next.
func NewRealVerbs() *RealVerbs {
	return &RealVerbs{}
}

func (v *RealVerbs) OpenDevice(devName string) error {
	ctx, err := OpenDevice(devName)
	if err != nil {
		return err
	}
	v.ctx = ctx
	return nil
}

func (v *RealVerbs) AllocPD() (api.ProtectionDomain, error) {
	pd, err := AllocPD(v.ctx)
	if err != nil {
		return nil, err
	}
	v.pd = pd
	return &RealPD{pd: pd}, nil
}

func (v *RealVerbs) CreateCQ(size int) (api.CompletionQueue, error) {
	cq, err := CreateCQ(v.ctx, size)
	if err != nil {
		return nil, err
	}
	return &RealCQ{cq: cq, size: size}, nil
}

func (v *RealVerbs) CreateQP(pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, error) {
	cPD := (*C.struct_ibv_pd)(pd.Handle())
	cSendCQ := (*C.struct_ibv_cq)(sendCQ.Handle())
	cRecvCQ := (*C.struct_ibv_cq)(recvCQ.Handle())

	var attr C.struct_ibv_qp_init_attr
	attr.send_cq = cSendCQ
	attr.recv_cq = cRecvCQ
	attr.qp_type = C.IBV_QPT_RC
	attr.cap.max_send_wr = C.uint32_t(cfg.MaxSendWR)
	attr.cap.max_recv_wr = C.uint32_t(cfg.MaxRecvWR)
	attr.cap.max_send_sge = C.uint32_t(cfg.MaxSendSGE)
	attr.cap.max_recv_sge = C.uint32_t(cfg.MaxRecvSGE)
	attr.cap.max_inline_data = C.uint32_t(cfg.MaxInlineData)
	if cfg.SQSigAll {
		attr.sq_sig_all = 1
	}

	qp := C.ibv_create_qp(cPD, &attr)
	if qp == nil {
		return nil, errors.New("ibv_create_qp failed")
	}
	return &RealQP{qp: qp, state: api.QPStateReset}, nil
}

func (v *RealVerbs) RegMR(pd api.ProtectionDomain, addr unsafe.Pointer, length int, access api.AccessFlags) (api.MemoryRegion, error) {
	cPD := (*C.struct_ibv_pd)(pd.Handle())
	mr, err := RegMR(cPD, addr, length, int(access))
	if err != nil {
		return nil, err
	}
	return &RealMR{mr: mr}, nil
}

func (v *RealVerbs) DeregMR(mr api.MemoryRegion) error {
	realMR, ok := mr.(*RealMR)
	if !ok {
		return errors.New("DeregMR: expected *RealMR")
	}
	return DeregMR(realMR.mr)
}

func (v *RealVerbs) PostSend(qp api.QueuePair, wr *api.SendWR) error {
	cQP := (*C.struct_ibv_qp)(qp.Handle())

	sges := make([]C.struct_ibv_sge, len(wr.SGList))
	for i, sg := range wr.SGList {
		sges[i].addr = C.uint64_t(sg.Addr)
		sges[i].length = C.uint32_t(sg.Length)
		sges[i].lkey = C.uint32_t(sg.LKey)
	}

	var sendWR C.struct_ibv_send_wr
	sendWR.wr_id = C.uint64_t(wr.WRID)
	sendWR.next = nil
	if len(sges) > 0 {
		sendWR.sg_list = &sges[0]
	}
	sendWR.num_sge = C.int(len(sges))
	sendWR.opcode = C.enum_ibv_wr_opcode(wr.Opcode)
	sendWR.send_flags = C.uint(wr.SendFlags)

	// imm_data lives in an anonymous union; use the C helper.
	C.golinker_wr_set_imm_data(&sendWR, C.uint32_t(wr.ImmData))

	var badWR *C.struct_ibv_send_wr
	ret := C.ibv_post_send(cQP, &sendWR, &badWR)
	if ret != 0 {
		return fmt.Errorf("ibv_post_send failed: %d", ret)
	}
	return nil
}

func (v *RealVerbs) PostRecv(qp api.QueuePair, wr *api.RecvWR) error {
	cQP := (*C.struct_ibv_qp)(qp.Handle())

	sges := make([]C.struct_ibv_sge, len(wr.SGList))
	for i, sg := range wr.SGList {
		sges[i].addr = C.uint64_t(sg.Addr)
		sges[i].length = C.uint32_t(sg.Length)
		sges[i].lkey = C.uint32_t(sg.LKey)
	}

	var recvWR C.struct_ibv_recv_wr
	recvWR.wr_id = C.uint64_t(wr.WRID)
	recvWR.next = nil
	if len(sges) > 0 {
		recvWR.sg_list = &sges[0]
	}
	recvWR.num_sge = C.int(len(sges))

	var badWR *C.struct_ibv_recv_wr
	ret := C.ibv_post_recv(cQP, &recvWR, &badWR)
	if ret != 0 {
		return fmt.Errorf("ibv_post_recv failed: %d", ret)
	}
	return nil
}

func (v *RealVerbs) Close() error {
	if v.pd != nil {
		if err := DeallocPD(v.pd); err != nil {
			return err
		}
		v.pd = nil
	}
	if v.ctx != nil {
		ret := C.ibv_close_device(v.ctx)
		if ret != 0 {
			return fmt.Errorf("ibv_close_device failed: %d", ret)
		}
		v.ctx = nil
	}
	return nil
}

// Compile-time interface checks.
var (
	_ api.Verbs            = (*RealVerbs)(nil)
	_ api.ProtectionDomain = (*RealPD)(nil)
	_ api.MemoryRegion     = (*RealMR)(nil)
	_ api.CompletionQueue  = (*RealCQ)(nil)
	_ api.QueuePair        = (*RealQP)(nil)
)
