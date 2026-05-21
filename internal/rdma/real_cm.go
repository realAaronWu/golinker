//go:build !mock

package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lnuma
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdlib.h>
#include "hotpath.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/wua20/golinker/api"
)

// ---------------------------------------------------------------------------
// RealCMEventChannel – implements api.CMEventChannel + api.CMAcceptor
// ---------------------------------------------------------------------------

// RealCMEventChannel wraps an rdma_event_channel and a listen CM ID.
type RealCMEventChannel struct {
	ch       *C.struct_rdma_event_channel
	listenID *C.struct_rdma_cm_id
	closed   bool
}

// Listen creates an RDMA CM event channel, binds to the given address and port,
// and starts listening for incoming connections.
func (r *RealCMEventChannel) Listen(ctx context.Context, addr string, port int) error {
	debugf("Listen: creating event channel")
	ch, err := CreateEventChannel()
	if err != nil {
		return fmt.Errorf("create event channel: %w", err)
	}
	r.ch = ch

	debugf("Listen: creating CM ID")
	id, err := CreateID(ch)
	if err != nil {
		DestroyEventChannel(ch)
		r.ch = nil
		return fmt.Errorf("create id: %w", err)
	}
	r.listenID = id

	debugf("Listen: binding to %s:%d", addr, port)
	if err := BindAddr(id, addr, port); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		r.listenID = nil
		r.ch = nil
		return fmt.Errorf("bind addr: %w", err)
	}

	debugf("Listen: starting listen (backlog=128)")
	if err := cmListen(id, 128); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		r.listenID = nil
		r.ch = nil
		return fmt.Errorf("listen: %w", err)
	}

	debugf("Listen: ready on %s:%d", addr, port)
	return nil
}

// GetEvent waits for the next CM event from the event channel.
// The returned event's Opaque field holds the raw C event for deferred ack.
// NOTE: Uses C.rdma_get_cm_event directly instead of the Go GetCMEvent wrapper
// because the wrapper auto-acks the event.
func (r *RealCMEventChannel) GetEvent(ctx context.Context) (*api.CMEvent, error) {
	var event *C.struct_rdma_cm_event
	ret := C.rdma_get_cm_event(r.ch, &event)
	if ret != 0 {
		return nil, fmt.Errorf("rdma_get_cm_event failed: %d", ret)
	}

	evType := api.CMEventType(mapCMEventType(event.event))

	return &api.CMEvent{
		Type:   evType,
		ID:     unsafe.Pointer(event.id),
		Opaque: unsafe.Pointer(event),
	}, nil
}

// AckEvent acknowledges a CM event previously returned by GetEvent.
func (r *RealCMEventChannel) AckEvent(event *api.CMEvent) error {
	raw := (*C.struct_rdma_cm_event)(event.Opaque)
	ret := C.rdma_ack_cm_event(raw)
	if ret != 0 {
		return fmt.Errorf("rdma_ack_cm_event failed: %d", ret)
	}
	return nil
}

// Close destroys the listen CM ID and the event channel.
func (r *RealCMEventChannel) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.listenID != nil {
		DestroyID(r.listenID)
		r.listenID = nil
	}
	if r.ch != nil {
		DestroyEventChannel(r.ch)
		r.ch = nil
	}
	return nil
}

// AcceptConn creates a QP on the incoming CM ID, accepts the connection,
// and returns the ready QueuePair.
// Note: the pd, sendCQ, recvCQ parameters are ignored for real RDMA because
// rdma_create_qp requires resources from the CM ID's own ibv_context.
// PD and CQs are created from the incoming CM ID's verbs context.
func (r *RealCMEventChannel) AcceptConn(cmID unsafe.Pointer, pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, error) {
	id := (*C.struct_rdma_cm_id)(cmID)
	debugf("AcceptConn: CM ID=%p, verbs=%p", id, id.verbs)

	// Create PD and CQs from the incoming CM ID's verbs context.
	// The incoming CM ID (from CONNECT_REQUEST) has id->verbs set by the kernel.
	cmPD := C.ibv_alloc_pd(id.verbs)
	if cmPD == nil {
		return nil, fmt.Errorf("ibv_alloc_pd on CM context failed")
	}

	cqSize := cfg.MaxSendWR
	if cfg.MaxRecvWR > cqSize {
		cqSize = cfg.MaxRecvWR
	}
	if cqSize < 128 {
		cqSize = 128
	}

	cmSendCQ := C.ibv_create_cq(id.verbs, C.int(cqSize), nil, nil, 0)
	if cmSendCQ == nil {
		C.ibv_dealloc_pd(cmPD)
		return nil, fmt.Errorf("ibv_create_cq (send) on CM context failed")
	}

	cmRecvCQ := C.ibv_create_cq(id.verbs, C.int(cqSize), nil, nil, 0)
	if cmRecvCQ == nil {
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		return nil, fmt.Errorf("ibv_create_cq (recv) on CM context failed")
	}

	qpCfg := QPConfig{
		MaxSendWR:     cfg.MaxSendWR,
		MaxRecvWR:     cfg.MaxRecvWR,
		MaxSendSGE:    cfg.MaxSendSGE,
		MaxRecvSGE:    cfg.MaxRecvSGE,
		MaxInlineData: cfg.MaxInlineData,
		SQSigAll:      cfg.SQSigAll,
	}

	debugf("AcceptConn: creating QP (send_wr=%d recv_wr=%d)", cfg.MaxSendWR, cfg.MaxRecvWR)
	if err := CreateQP(id, cmPD, cmSendCQ, cmRecvCQ, qpCfg); err != nil {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		return nil, fmt.Errorf("create qp: %w", err)
	}

	debugf("AcceptConn: calling rdma_accept")
	if err := Accept(id, nil); err != nil {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		return nil, fmt.Errorf("accept: %w", err)
	}

	debugf("AcceptConn: done, QP=%p qp_num=%d", id.qp, id.qp.qp_num)
	return &RealQP{qp: id.qp, state: api.QPStateRTS}, nil
}

// ---------------------------------------------------------------------------
// RealCMDialer – implements api.CMDialer
// ---------------------------------------------------------------------------

// RealCMDialer handles client-side RDMA connection establishment.
// Each Dial creates its own event channel and CM ID. PD and CQs are
// created from the CM ID's verbs context (set after address resolution)
// because rdma_create_qp requires all resources on the same ibv_context.
type RealCMDialer struct {
	// channels tracks event channels created by Dial() calls so they can
	// be cleaned up in Close(). Each Dial creates its own channel; the
	// channel must outlive the CM ID (rdma_destroy_id needs it).
	channels []*C.struct_rdma_event_channel
	// ids tracks CM IDs created by Dial() for cleanup.
	ids []*C.struct_rdma_cm_id
}

// Dial performs the full RDMA CM connection handshake:
// resolve-addr -> create PD/CQs from CM context -> resolve-route -> create-QP -> connect.
// Note: the pd, sendCQ, recvCQ parameters are ignored for real RDMA because
// rdma_create_qp requires resources from the CM ID's own ibv_context.
func (d *RealCMDialer) Dial(ctx context.Context, addr string, port int, pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, unsafe.Pointer, error) {
	debugf("Dial: target=%s:%d", addr, port)

	ch, err := CreateEventChannel()
	if err != nil {
		return nil, nil, fmt.Errorf("create event channel: %w", err)
	}

	id, err := CreateID(ch)
	if err != nil {
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("create id: %w", err)
	}

	// Step 1: Resolve address.
	debugf("Dial: resolving address %s:%d", addr, port)
	if err := ResolveAddr(id, addr, port, 2000); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("resolve addr: %w", err)
	}

	// Wait for ADDR_RESOLVED.
	var event *C.struct_rdma_cm_event
	ret := C.rdma_get_cm_event(ch, &event)
	if ret != 0 {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("rdma_get_cm_event (addr): %d", ret)
	}
	if mapCMEventType(event.event) != CMEventAddrResolved {
		C.rdma_ack_cm_event(event)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("unexpected event: wanted ADDR_RESOLVED, got %d", event.event)
	}
	C.rdma_ack_cm_event(event)

	// Step 2: Create PD and CQs from the CM ID's verbs context.
	debugf("Dial: ADDR_RESOLVED, creating PD/CQs from verbs=%p", id.verbs)
	// After addr resolution, id->verbs is set. All QP resources must come
	// from this context; using a separately-opened ibv_context will fail.
	cmPD := C.ibv_alloc_pd(id.verbs)
	if cmPD == nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("ibv_alloc_pd on CM context failed")
	}

	cqSize := cfg.MaxSendWR
	if cfg.MaxRecvWR > cqSize {
		cqSize = cfg.MaxRecvWR
	}
	if cqSize < 128 {
		cqSize = 128
	}

	cmSendCQ := C.ibv_create_cq(id.verbs, C.int(cqSize), nil, nil, 0)
	if cmSendCQ == nil {
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("ibv_create_cq (send) on CM context failed")
	}

	cmRecvCQ := C.ibv_create_cq(id.verbs, C.int(cqSize), nil, nil, 0)
	if cmRecvCQ == nil {
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("ibv_create_cq (recv) on CM context failed")
	}

	// Step 3: Resolve route.
	debugf("Dial: resolving route")
	if err := ResolveRoute(id, 2000); err != nil {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("resolve route: %w", err)
	}

	// Wait for ROUTE_RESOLVED.
	ret = C.rdma_get_cm_event(ch, &event)
	if ret != 0 {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("rdma_get_cm_event (route): %d", ret)
	}
	if mapCMEventType(event.event) != CMEventRouteResolved {
		C.rdma_ack_cm_event(event)
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("unexpected event: wanted ROUTE_RESOLVED, got %d", event.event)
	}
	C.rdma_ack_cm_event(event)

	// Step 4: Create QP on the CM ID using CM-context PD and CQs.
	debugf("Dial: ROUTE_RESOLVED, creating QP")
	qpCfg := QPConfig{
		MaxSendWR:     cfg.MaxSendWR,
		MaxRecvWR:     cfg.MaxRecvWR,
		MaxSendSGE:    cfg.MaxSendSGE,
		MaxRecvSGE:    cfg.MaxRecvSGE,
		MaxInlineData: cfg.MaxInlineData,
		SQSigAll:      cfg.SQSigAll,
	}
	if err := CreateQP(id, cmPD, cmSendCQ, cmRecvCQ, qpCfg); err != nil {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("create qp: %w", err)
	}

	// Step 5: Connect.
	debugf("Dial: QP created (qp_num=%d), connecting", id.qp.qp_num)
	if err := Connect(id, nil); err != nil {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("connect: %w", err)
	}

	// Wait for ESTABLISHED.
	ret = C.rdma_get_cm_event(ch, &event)
	if ret != 0 {
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("rdma_get_cm_event (established): %d", ret)
	}
	if mapCMEventType(event.event) != CMEventEstablished {
		C.rdma_ack_cm_event(event)
		C.ibv_destroy_cq(cmRecvCQ)
		C.ibv_destroy_cq(cmSendCQ)
		C.ibv_dealloc_pd(cmPD)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("unexpected event: wanted ESTABLISHED, got %d", event.event)
	}
	C.rdma_ack_cm_event(event)

	debugf("Dial: ESTABLISHED, connection ready (qp_num=%d)", id.qp.qp_num)

	// Track for cleanup in Close().
	d.channels = append(d.channels, ch)
	d.ids = append(d.ids, id)

	return &RealQP{qp: id.qp, state: api.QPStateRTS}, unsafe.Pointer(id), nil
}

// Disconnect disconnects an RDMA connection.
func (d *RealCMDialer) Disconnect(cmID unsafe.Pointer) error {
	return Disconnect((*C.struct_rdma_cm_id)(cmID))
}

// Close destroys all CM IDs and event channels created by Dial() calls.
// CM IDs must be destroyed before their event channels.
func (d *RealCMDialer) Close() error {
	var firstErr error
	for _, id := range d.ids {
		if err := DestroyID(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	d.ids = nil
	for _, ch := range d.channels {
		DestroyEventChannel(ch)
	}
	d.channels = nil
	return firstErr
}

// Compile-time interface checks.
var (
	_ api.CMEventChannel = (*RealCMEventChannel)(nil)
	_ api.CMAcceptor     = (*RealCMEventChannel)(nil)
	_ api.CMDialer       = (*RealCMDialer)(nil)
)
