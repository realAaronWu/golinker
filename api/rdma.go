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
	ID          unsafe.Pointer
	PrivateData []byte
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
