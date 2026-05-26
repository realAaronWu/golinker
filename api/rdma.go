package api

import (
	"context"
	"unsafe"
)

// ProtectionDomain wraps ibv_pd.
type ProtectionDomain interface {
	Handle() unsafe.Pointer
}

// MemoryRegion wraps ibv_mr.
type MemoryRegion interface {
	Addr() unsafe.Pointer
	Length() int
	LKey() uint32
	RKey() uint32
}

// CompletionQueue wraps ibv_cq.
type CompletionQueue interface {
	Handle() unsafe.Pointer
	Size() int
	// CompChannelFD returns the file descriptor of the CQ's completion channel,
	// or -1 if the CQ was created without a completion channel.
	CompChannelFD() int
	// ReqNotify arms the CQ to generate a completion event on the next work
	// completion. Must be called before parking on the comp channel FD.
	// Returns nil on CQs without a completion channel (no-op).
	ReqNotify() error
	// AckEvents acknowledges nevents completion events previously received
	// from the completion channel. Must be called before destroying the CQ.
	// No-op on CQs without a completion channel.
	AckEvents(nevents uint)
}

// QueuePair wraps ibv_qp.
type QueuePair interface {
	Handle() unsafe.Pointer
	QPNum() uint32
	State() QueuePairState
	ModifyToInit() error
	ModifyToRTR(destQPN uint32, destLID uint16, destGID [16]byte) error
	ModifyToRTS() error
}

// Verbs provides access to RDMA verbs operations.
type Verbs interface {
	OpenDevice(devName string) error
	AllocPD() (ProtectionDomain, error)
	CreateCQ(size int) (CompletionQueue, error)
	// CreateCQWithChannel creates a CQ with an associated completion channel.
	// The returned CQ's CompChannelFD() will return a valid FD (>= 0) for use
	// with event-driven or smart polling modes.
	CreateCQWithChannel(size int) (CompletionQueue, error)
	CreateQP(pd ProtectionDomain, sendCQ, recvCQ CompletionQueue, cfg QueuePairConfig) (QueuePair, error)
	RegMR(pd ProtectionDomain, addr unsafe.Pointer, length int, access AccessFlags) (MemoryRegion, error)
	DeregMR(mr MemoryRegion) error
	PostSend(qp QueuePair, wr *SendWR) error
	PostRecv(qp QueuePair, wr *RecvWR) error
	Close() error
}

// CMEventChannel wraps rdma_cm event handling.
type CMEventChannel interface {
	Listen(ctx context.Context, addr string, port int) error
	GetEvent(ctx context.Context) (*CMEvent, error)
	AckEvent(event *CMEvent) error
	Close() error
}

// QueuePairConfig holds configuration for QP creation.
type QueuePairConfig struct {
	MaxSendWR    int
	MaxRecvWR    int
	MaxSendSGE   int
	MaxRecvSGE   int
	MaxInlineData int
	SQSigAll     bool
}

// QueuePairState represents the state of a queue pair.
type QueuePairState int

const (
	QPStateReset QueuePairState = iota
	QPStateInit
	QPStateRTR
	QPStateRTS
	QPStateSQD
	QPStateSQErr
	QPStateErr
)

// AccessFlags specifies memory region access permissions.
type AccessFlags int

const (
	AccessLocalWrite   AccessFlags = 1 << iota
	AccessRemoteWrite
	AccessRemoteRead
	AccessRemoteAtomic
)

// Send flag constants (matching ibv_send_flags).
const (
	SendSignaled  = 1 << 0 // IBV_SEND_SIGNALED
	SendSolicited = 1 << 1 // IBV_SEND_SOLICITED
	SendInline    = 1 << 2 // IBV_SEND_INLINE
)

// SendWR represents a send work request.
type SendWR struct {
	WRID      uint64
	SGList    []SGE
	Opcode    int
	SendFlags int
	ImmData   uint32
}

// RecvWR represents a receive work request.
type RecvWR struct {
	WRID   uint64
	SGList []SGE
}

// SGE represents a scatter/gather entry.
type SGE struct {
	Addr   uint64
	Length uint32
	LKey   uint32
}

// CMEvent represents a connection manager event.
type CMEvent struct {
	Type        CMEventType
	ID          unsafe.Pointer // rdma_cm_id for this event
	PrivateData []byte
	Opaque      unsafe.Pointer // implementation-specific (e.g., raw C event for deferred ack)
}

// CMAcceptor handles server-side RDMA connection acceptance.
// On a CONNECT_REQUEST event the acceptor creates a QP on the incoming CM ID,
// calls rdma_accept, and returns the ready QueuePair.
type CMAcceptor interface {
	AcceptConn(cmID unsafe.Pointer, pd ProtectionDomain, sendCQ, recvCQ CompletionQueue, cfg QueuePairConfig) (QueuePair, error)
}

// CMDialer handles client-side RDMA connection establishment.
// It performs the full resolve-addr → resolve-route → create-QP → connect
// handshake and blocks until the connection is ESTABLISHED.
type CMDialer interface {
	Dial(ctx context.Context, addr string, port int, pd ProtectionDomain, sendCQ, recvCQ CompletionQueue, cfg QueuePairConfig) (QueuePair, unsafe.Pointer, error)
	Disconnect(cmID unsafe.Pointer) error
	Close() error
}

// CMEventType identifies the type of CM event.
type CMEventType int

const (
	EventAddrResolved    CMEventType = iota
	EventRouteResolved
	EventConnectRequest
	EventEstablished
	EventDisconnected
	EventRejected
	EventDeviceRemoval
)

// WCStatus represents work completion status codes.
type WCStatus int

const (
	WCSuccess      WCStatus = iota
	WCLocLenErr
	WCLocQPOpErr
	WCLocProtErr
	WCWRFlushErr
	WCMWBindErr
	WCBadRespErr
	WCLocAccessErr
	WCRemInvReqErr
	WCRemAccessErr
	WCRemOpErr
	WCRetryExcErr
	WCRnrRetryErr
)

// WCOpcode represents work completion opcodes.
type WCOpcode int

const (
	WCSend          WCOpcode = iota
	WCRdmaWrite
	WCRdmaRead
	WCRecv
	WCRecvRdmaWithImm
)
