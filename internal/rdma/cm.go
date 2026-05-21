//go:build !mock

package rdma

/*
#cgo LDFLAGS: -libverbs -lrdmacm -lnuma
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "hotpath.h"
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// CMEventType identifies the type of CM event.
type CMEventType int

const (
	CMEventAddrResolved    CMEventType = iota
	CMEventRouteResolved
	CMEventConnectRequest
	CMEventEstablished
	CMEventDisconnected
	CMEventRejected
	CMEventDeviceRemoval
)

// CreateEventChannel creates an RDMA CM event channel.
func CreateEventChannel() (*C.struct_rdma_event_channel, error) {
	ch := C.rdma_create_event_channel()
	if ch == nil {
		return nil, fmt.Errorf("rdma_create_event_channel failed")
	}
	return ch, nil
}

// DestroyEventChannel destroys an RDMA CM event channel.
func DestroyEventChannel(ch *C.struct_rdma_event_channel) {
	C.rdma_destroy_event_channel(ch)
}

// CreateID creates an RDMA CM identifier with RDMA_PS_TCP.
func CreateID(ch *C.struct_rdma_event_channel) (*C.struct_rdma_cm_id, error) {
	var id *C.struct_rdma_cm_id
	ret := C.rdma_create_id(ch, &id, nil, C.RDMA_PS_TCP)
	if ret != 0 {
		return nil, fmt.Errorf("rdma_create_id failed: %d", ret)
	}
	return id, nil
}

// DestroyID destroys an RDMA CM identifier.
func DestroyID(id *C.struct_rdma_cm_id) error {
	ret := C.rdma_destroy_id(id)
	if ret != 0 {
		return fmt.Errorf("rdma_destroy_id failed: %d", ret)
	}
	return nil
}

// BindAddr binds an RDMA CM ID to the specified address and port.
func BindAddr(id *C.struct_rdma_cm_id, addr string, port int) error {
	var sockAddr C.struct_sockaddr_in
	C.memset(unsafe.Pointer(&sockAddr), 0, C.size_t(unsafe.Sizeof(sockAddr)))
	sockAddr.sin_family = C.AF_INET
	sockAddr.sin_port = C.htons(C.uint16_t(port))

	if addr == "" || addr == "0.0.0.0" {
		sockAddr.sin_addr.s_addr = C.htonl(C.INADDR_ANY)
	} else {
		cAddr := C.CString(addr)
		defer C.free(unsafe.Pointer(cAddr))
		ret := C.inet_pton(C.AF_INET, cAddr, unsafe.Pointer(&sockAddr.sin_addr))
		if ret != 1 {
			return fmt.Errorf("invalid address: %s", addr)
		}
	}

	ret := C.rdma_bind_addr(id, (*C.struct_sockaddr)(unsafe.Pointer(&sockAddr)))
	if ret != 0 {
		return fmt.Errorf("rdma_bind_addr failed: %d", ret)
	}
	return nil
}

// cmListen starts listening for incoming connections on the given CM ID.
func cmListen(id *C.struct_rdma_cm_id, backlog int) error {
	ret := C.rdma_listen(id, C.int(backlog))
	if ret != 0 {
		return fmt.Errorf("rdma_listen failed: %d", ret)
	}
	return nil
}

// ResolveAddr resolves the destination address for a connection.
func ResolveAddr(id *C.struct_rdma_cm_id, addr string, port int, timeoutMs int) error {
	var sockAddr C.struct_sockaddr_in
	C.memset(unsafe.Pointer(&sockAddr), 0, C.size_t(unsafe.Sizeof(sockAddr)))
	sockAddr.sin_family = C.AF_INET
	sockAddr.sin_port = C.htons(C.uint16_t(port))

	cAddr := C.CString(addr)
	defer C.free(unsafe.Pointer(cAddr))
	ret := C.inet_pton(C.AF_INET, cAddr, unsafe.Pointer(&sockAddr.sin_addr))
	if ret != 1 {
		return fmt.Errorf("invalid address: %s", addr)
	}

	rc := C.rdma_resolve_addr(id, nil, (*C.struct_sockaddr)(unsafe.Pointer(&sockAddr)), C.int(timeoutMs))
	if rc != 0 {
		return fmt.Errorf("rdma_resolve_addr failed: %d", rc)
	}
	return nil
}

// ResolveRoute resolves the route for a connection.
func ResolveRoute(id *C.struct_rdma_cm_id, timeoutMs int) error {
	ret := C.rdma_resolve_route(id, C.int(timeoutMs))
	if ret != 0 {
		return fmt.Errorf("rdma_resolve_route failed: %d", ret)
	}
	return nil
}

// Connect initiates an RDMA connection with optional private data.
// retry_count=3 gives ~30s total CM timeout (vs 7→~500s).
func Connect(id *C.struct_rdma_cm_id, privateData []byte) error {
	var params C.struct_rdma_conn_param
	C.memset(unsafe.Pointer(&params), 0, C.size_t(unsafe.Sizeof(params)))
	params.initiator_depth = 1
	params.responder_resources = 1
	params.retry_count = 3
	params.rnr_retry_count = 7

	if len(privateData) > 0 {
		params.private_data = unsafe.Pointer(&privateData[0])
		params.private_data_len = C.uint8_t(len(privateData))
	}

	ret := C.rdma_connect(id, &params)
	if ret != 0 {
		return fmt.Errorf("rdma_connect failed: %d", ret)
	}
	return nil
}

// Accept accepts an incoming RDMA connection with optional private data.
func Accept(id *C.struct_rdma_cm_id, privateData []byte) error {
	var params C.struct_rdma_conn_param
	C.memset(unsafe.Pointer(&params), 0, C.size_t(unsafe.Sizeof(params)))
	params.initiator_depth = 1
	params.responder_resources = 1

	if len(privateData) > 0 {
		params.private_data = unsafe.Pointer(&privateData[0])
		params.private_data_len = C.uint8_t(len(privateData))
	}

	ret := C.rdma_accept(id, &params)
	if ret != 0 {
		return fmt.Errorf("rdma_accept failed: %d", ret)
	}
	return nil
}

// Disconnect disconnects an RDMA connection.
func Disconnect(id *C.struct_rdma_cm_id) error {
	ret := C.rdma_disconnect(id)
	if ret != 0 {
		return fmt.Errorf("rdma_disconnect failed: %d", ret)
	}
	return nil
}

// GetCMEvent waits for and returns the next CM event from the event channel.
// Returns the event type, the associated CM ID, and any error.
func GetCMEvent(ch *C.struct_rdma_event_channel) (CMEventType, *C.struct_rdma_cm_id, error) {
	var event *C.struct_rdma_cm_event
	ret := C.rdma_get_cm_event(ch, &event)
	if ret != 0 {
		return 0, nil, fmt.Errorf("rdma_get_cm_event failed: %d", ret)
	}

	id := event.id
	evType := mapCMEventType(event.event)

	// Acknowledge the event immediately.
	C.rdma_ack_cm_event(event)

	return evType, id, nil
}

// AckCMEvent acknowledges a CM event. This is exposed for cases where
// the caller needs explicit control over event acknowledgment.
func AckCMEvent(event *C.struct_rdma_cm_event) error {
	ret := C.rdma_ack_cm_event(event)
	if ret != 0 {
		return fmt.Errorf("rdma_ack_cm_event failed: %d", ret)
	}
	return nil
}

// mapCMEventType maps a C rdma_cm_event_type to the Go CMEventType.
func mapCMEventType(ev C.enum_rdma_cm_event_type) CMEventType {
	switch ev {
	case C.RDMA_CM_EVENT_ADDR_RESOLVED:
		return CMEventAddrResolved
	case C.RDMA_CM_EVENT_ROUTE_RESOLVED:
		return CMEventRouteResolved
	case C.RDMA_CM_EVENT_CONNECT_REQUEST:
		return CMEventConnectRequest
	case C.RDMA_CM_EVENT_ESTABLISHED:
		return CMEventEstablished
	case C.RDMA_CM_EVENT_DISCONNECTED:
		return CMEventDisconnected
	case C.RDMA_CM_EVENT_REJECTED:
		return CMEventRejected
	case C.RDMA_CM_EVENT_DEVICE_REMOVAL:
		return CMEventDeviceRemoval
	default:
		return CMEventDisconnected
	}
}
