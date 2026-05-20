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
	ch, err := CreateEventChannel()
	if err != nil {
		return fmt.Errorf("create event channel: %w", err)
	}
	r.ch = ch

	id, err := CreateID(ch)
	if err != nil {
		DestroyEventChannel(ch)
		r.ch = nil
		return fmt.Errorf("create id: %w", err)
	}
	r.listenID = id

	if err := BindAddr(id, addr, port); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		r.listenID = nil
		r.ch = nil
		return fmt.Errorf("bind addr: %w", err)
	}

	if err := Listen(id, 128); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		r.listenID = nil
		r.ch = nil
		return fmt.Errorf("listen: %w", err)
	}

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
func (r *RealCMEventChannel) AcceptConn(cmID unsafe.Pointer, pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, error) {
	id := (*C.struct_rdma_cm_id)(cmID)
	cPD := (*C.struct_ibv_pd)(pd.Handle())
	cSendCQ := (*C.struct_ibv_cq)(sendCQ.Handle())
	cRecvCQ := (*C.struct_ibv_cq)(recvCQ.Handle())

	qpCfg := QPConfig{
		MaxSendWR:     cfg.MaxSendWR,
		MaxRecvWR:     cfg.MaxRecvWR,
		MaxSendSGE:    cfg.MaxSendSGE,
		MaxRecvSGE:    cfg.MaxRecvSGE,
		MaxInlineData: cfg.MaxInlineData,
		SQSigAll:      cfg.SQSigAll,
	}

	if err := CreateQP(id, cPD, cSendCQ, cRecvCQ, qpCfg); err != nil {
		return nil, fmt.Errorf("create qp: %w", err)
	}

	if err := Accept(id, nil); err != nil {
		return nil, fmt.Errorf("accept: %w", err)
	}

	return &RealQP{qp: id.qp}, nil
}

// ---------------------------------------------------------------------------
// RealCMDialer – implements api.CMDialer
// ---------------------------------------------------------------------------

// RealCMDialer handles client-side RDMA connection establishment.
// Each Dial creates its own event channel and CM ID.
type RealCMDialer struct{}

// Dial performs the full RDMA CM connection handshake:
// resolve-addr -> resolve-route -> create-QP -> connect.
func (d *RealCMDialer) Dial(ctx context.Context, addr string, port int, pd api.ProtectionDomain, sendCQ, recvCQ api.CompletionQueue, cfg api.QueuePairConfig) (api.QueuePair, unsafe.Pointer, error) {
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

	// Step 2: Resolve route.
	if err := ResolveRoute(id, 2000); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("resolve route: %w", err)
	}

	// Wait for ROUTE_RESOLVED.
	ret = C.rdma_get_cm_event(ch, &event)
	if ret != 0 {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("rdma_get_cm_event (route): %d", ret)
	}
	if mapCMEventType(event.event) != CMEventRouteResolved {
		C.rdma_ack_cm_event(event)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("unexpected event: wanted ROUTE_RESOLVED, got %d", event.event)
	}
	C.rdma_ack_cm_event(event)

	// Step 3: Create QP on the CM ID.
	cPD := (*C.struct_ibv_pd)(pd.Handle())
	cSendCQ := (*C.struct_ibv_cq)(sendCQ.Handle())
	cRecvCQ := (*C.struct_ibv_cq)(recvCQ.Handle())
	qpCfg := QPConfig{
		MaxSendWR:     cfg.MaxSendWR,
		MaxRecvWR:     cfg.MaxRecvWR,
		MaxSendSGE:    cfg.MaxSendSGE,
		MaxRecvSGE:    cfg.MaxRecvSGE,
		MaxInlineData: cfg.MaxInlineData,
		SQSigAll:      cfg.SQSigAll,
	}
	if err := CreateQP(id, cPD, cSendCQ, cRecvCQ, qpCfg); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("create qp: %w", err)
	}

	// Step 4: Connect.
	if err := Connect(id, nil); err != nil {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("connect: %w", err)
	}

	// Wait for ESTABLISHED.
	ret = C.rdma_get_cm_event(ch, &event)
	if ret != 0 {
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("rdma_get_cm_event (established): %d", ret)
	}
	if mapCMEventType(event.event) != CMEventEstablished {
		C.rdma_ack_cm_event(event)
		DestroyID(id)
		DestroyEventChannel(ch)
		return nil, nil, fmt.Errorf("unexpected event: wanted ESTABLISHED, got %d", event.event)
	}
	C.rdma_ack_cm_event(event)

	return &RealQP{qp: id.qp}, unsafe.Pointer(id), nil
}

// Disconnect disconnects an RDMA connection.
func (d *RealCMDialer) Disconnect(cmID unsafe.Pointer) error {
	return Disconnect((*C.struct_rdma_cm_id)(cmID))
}

// Close is a no-op; each Dial creates its own resources.
func (d *RealCMDialer) Close() error {
	return nil
}

// Compile-time interface checks.
var (
	_ api.CMEventChannel = (*RealCMEventChannel)(nil)
	_ api.CMAcceptor     = (*RealCMEventChannel)(nil)
	_ api.CMDialer       = (*RealCMDialer)(nil)
)
