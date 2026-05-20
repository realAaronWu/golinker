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
)

// QPConfig holds configuration for QP creation via rdma_create_qp.
type QPConfig struct {
	MaxSendWR     int
	MaxRecvWR     int
	MaxSendSGE    int
	MaxRecvSGE    int
	MaxInlineData int
	SQSigAll      bool
}

// GetDeviceList returns the names of all available RDMA devices.
func GetDeviceList() ([]string, error) {
	var numDevices C.int
	devList := C.ibv_get_device_list(&numDevices)
	if devList == nil {
		return nil, errors.New("ibv_get_device_list failed: no devices found")
	}
	defer C.ibv_free_device_list(devList)

	count := int(numDevices)
	if count == 0 {
		return nil, nil
	}

	// Convert the C array of device pointers to a Go slice.
	devices := unsafe.Slice(devList, count)
	names := make([]string, count)
	for i := 0; i < count; i++ {
		names[i] = C.GoString(C.ibv_get_device_name(devices[i]))
	}
	return names, nil
}

// OpenDevice opens an RDMA device by name and returns its context.
func OpenDevice(devName string) (*C.struct_ibv_context, error) {
	var numDevices C.int
	devList := C.ibv_get_device_list(&numDevices)
	if devList == nil {
		return nil, errors.New("ibv_get_device_list failed")
	}
	defer C.ibv_free_device_list(devList)

	count := int(numDevices)
	devices := unsafe.Slice(devList, count)

	for i := 0; i < count; i++ {
		name := C.GoString(C.ibv_get_device_name(devices[i]))
		if name == devName {
			ctx := C.ibv_open_device(devices[i])
			if ctx == nil {
				return nil, fmt.Errorf("ibv_open_device failed for %s", devName)
			}
			return ctx, nil
		}
	}
	return nil, fmt.Errorf("device %s not found", devName)
}

// AllocPD allocates a protection domain.
func AllocPD(ctx *C.struct_ibv_context) (*C.struct_ibv_pd, error) {
	pd := C.ibv_alloc_pd(ctx)
	if pd == nil {
		return nil, errors.New("ibv_alloc_pd failed")
	}
	return pd, nil
}

// DeallocPD deallocates a protection domain.
func DeallocPD(pd *C.struct_ibv_pd) error {
	ret := C.ibv_dealloc_pd(pd)
	if ret != 0 {
		return fmt.Errorf("ibv_dealloc_pd failed: %d", ret)
	}
	return nil
}

// CreateCQ creates a completion queue with the given size.
func CreateCQ(ctx *C.struct_ibv_context, size int) (*C.struct_ibv_cq, error) {
	cq := C.ibv_create_cq(ctx, C.int(size), nil, nil, 0)
	if cq == nil {
		return nil, errors.New("ibv_create_cq failed")
	}
	return cq, nil
}

// DestroyCQ destroys a completion queue.
func DestroyCQ(cq *C.struct_ibv_cq) error {
	ret := C.ibv_destroy_cq(cq)
	if ret != 0 {
		return fmt.Errorf("ibv_destroy_cq failed: %d", ret)
	}
	return nil
}

// CreateQP creates a queue pair associated with the given CM ID.
func CreateQP(cmID *C.struct_rdma_cm_id, pd *C.struct_ibv_pd, sendCQ, recvCQ *C.struct_ibv_cq, cfg QPConfig) error {
	var attr C.struct_ibv_qp_init_attr
	attr.send_cq = sendCQ
	attr.recv_cq = recvCQ
	attr.qp_type = C.IBV_QPT_RC
	attr.cap.max_send_wr = C.uint32_t(cfg.MaxSendWR)
	attr.cap.max_recv_wr = C.uint32_t(cfg.MaxRecvWR)
	attr.cap.max_send_sge = C.uint32_t(cfg.MaxSendSGE)
	attr.cap.max_recv_sge = C.uint32_t(cfg.MaxRecvSGE)
	attr.cap.max_inline_data = C.uint32_t(cfg.MaxInlineData)
	if cfg.SQSigAll {
		attr.sq_sig_all = 1
	} else {
		attr.sq_sig_all = 0
	}

	ret := C.rdma_create_qp(cmID, pd, &attr)
	if ret != 0 {
		return fmt.Errorf("rdma_create_qp failed: %d", ret)
	}
	return nil
}

// DestroyQP destroys a queue pair.
func DestroyQP(qp *C.struct_ibv_qp) error {
	ret := C.ibv_destroy_qp(qp)
	if ret != 0 {
		return fmt.Errorf("ibv_destroy_qp failed: %d", ret)
	}
	return nil
}

// RegMR registers a memory region with the given access flags.
func RegMR(pd *C.struct_ibv_pd, addr unsafe.Pointer, length int, access int) (*C.struct_ibv_mr, error) {
	mr := C.ibv_reg_mr(pd, addr, C.size_t(length), C.int(access))
	if mr == nil {
		return nil, errors.New("ibv_reg_mr failed")
	}
	return mr, nil
}

// DeregMR deregisters a memory region.
func DeregMR(mr *C.struct_ibv_mr) error {
	ret := C.ibv_dereg_mr(mr)
	if ret != 0 {
		return fmt.Errorf("ibv_dereg_mr failed: %d", ret)
	}
	return nil
}

// PostSend posts a single send work request using the hotpath C function.
func PostSend(qp *C.struct_ibv_qp, buf unsafe.Pointer, size uint32, mr *C.struct_ibv_mr, wrID uint64, flags int) error {
	ret := C.golinker_post_send_single(qp, buf, C.uint32_t(size), mr, C.uint64_t(wrID), C.int(flags))
	if ret != 0 {
		return fmt.Errorf("golinker_post_send_single failed: %d", ret)
	}
	return nil
}

// PostRecv posts a single receive work request.
func PostRecv(qp *C.struct_ibv_qp, buf unsafe.Pointer, size uint32, mr *C.struct_ibv_mr, wrID uint64) error {
	var sge C.struct_ibv_sge
	sge.addr = C.uint64_t(uintptr(buf))
	sge.length = C.uint32_t(size)
	sge.lkey = mr.lkey

	var wr C.struct_ibv_recv_wr
	wr.wr_id = C.uint64_t(wrID)
	wr.next = nil
	wr.sg_list = &sge
	wr.num_sge = 1

	var badWR *C.struct_ibv_recv_wr
	ret := C.ibv_post_recv(qp, &wr, &badWR)
	if ret != 0 {
		return fmt.Errorf("ibv_post_recv failed: %d", ret)
	}
	return nil
}

// PollCQ polls the completion queue and re-posts receive buffers using the hotpath C function.
// reposts contains buffers from the previous batch to re-post; may be nil.
// Returns completed work completions.
func PollCQ(cq *C.struct_ibv_cq, maxWCs int, reposts []RepostItem) ([]WorkCompletion, error) {
	wcs := make([]C.struct_ibv_wc, maxWCs)

	var cReposts *C.repost_item_t
	var repostCount C.int
	var cRepostSlice []C.repost_item_t

	if len(reposts) > 0 {
		cRepostSlice = make([]C.repost_item_t, len(reposts))
		for i, r := range reposts {
			cRepostSlice[i].qp = (*C.struct_ibv_qp)(r.QP)
			cRepostSlice[i].buf = r.Buf
			cRepostSlice[i].size = C.uint32_t(r.Size)
			cRepostSlice[i].mr = (*C.struct_ibv_mr)(r.MR)
		}
		cReposts = &cRepostSlice[0]
		repostCount = C.int(len(reposts))
	}

	ret := C.golinker_poll_and_repost(cq, &wcs[0], C.int(maxWCs), cReposts, repostCount)
	if ret < 0 {
		return nil, fmt.Errorf("golinker_poll_and_repost failed: %d", ret)
	}

	n := int(ret)
	if n == 0 {
		return nil, nil
	}

	completions := make([]WorkCompletion, n)
	for i := 0; i < n; i++ {
		completions[i] = WorkCompletion{
			WRID:    uint64(wcs[i].wr_id),
			Status:  int(wcs[i].status),
			Opcode:  int(wcs[i].opcode),
			ByteLen: uint32(wcs[i].byte_len),
			QPN:     uint32(wcs[i].qp_num),
			ImmData: uint32(C.golinker_wc_imm_data(&wcs[i])),
		}
	}
	return completions, nil
}

// BatchPostSend posts multiple send work requests in a single ibv_post_send call
// using the hotpath C function.
func BatchPostSend(items []SendItem) error {
	if len(items) == 0 {
		return errors.New("BatchPostSend: empty items slice")
	}

	cItems := make([]C.send_item_t, len(items))
	for i, item := range items {
		cItems[i].qp = (*C.struct_ibv_qp)(item.QP)
		cItems[i].buf = item.Buf
		cItems[i].size = C.uint32_t(item.Size)
		cItems[i].mr = (*C.struct_ibv_mr)(item.MR)
		cItems[i].flags = C.int(item.Flags)
		cItems[i].wr_id = C.uint64_t(item.WRID)
	}

	ret := C.golinker_batch_post_send(&cItems[0], C.int(len(items)))
	if ret != 0 {
		return fmt.Errorf("golinker_batch_post_send failed: %d", ret)
	}
	return nil
}
