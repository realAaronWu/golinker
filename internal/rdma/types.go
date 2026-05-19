//go:build !mock

package rdma

import "unsafe"

// WorkCompletion mirrors ibv_wc.
type WorkCompletion struct {
	WRID    uint64
	Status  int
	Opcode  int
	ByteLen uint32
	QPN     uint32
	ImmData uint32
}

// SendWR mirrors ibv_send_wr fields used by golinker.
type SendWR struct {
	WRID      uint64
	SGList    []SGE
	NumSGE    int
	Opcode    int
	SendFlags int
	ImmData   uint32
}

// RecvWR mirrors ibv_recv_wr.
type RecvWR struct {
	WRID   uint64
	SGList []SGE
	NumSGE int
}

// SGE mirrors ibv_sge.
type SGE struct {
	Addr   uint64
	Length uint32
	LKey   uint32
}

// WC status codes.
const (
	IBV_WC_SUCCESS        = 0
	IBV_WC_LOC_LEN_ERR    = 1
	IBV_WC_LOC_QP_OP_ERR  = 2
	IBV_WC_LOC_EEC_OP_ERR = 3
	IBV_WC_LOC_PROT_ERR   = 4
	IBV_WC_WR_FLUSH_ERR   = 5
	IBV_WC_MW_BIND_ERR    = 6
	IBV_WC_BAD_RESP_ERR   = 7
	IBV_WC_LOC_ACCESS_ERR = 8
	IBV_WC_REM_INV_REQ_ERR = 9
	IBV_WC_REM_ACCESS_ERR  = 10
	IBV_WC_REM_OP_ERR      = 11
	IBV_WC_RETRY_EXC_ERR   = 12
	IBV_WC_RNR_RETRY_ERR   = 13
)

// WC opcodes.
const (
	IBV_WC_SEND               = 0
	IBV_WC_RDMA_WRITE         = 1
	IBV_WC_RDMA_READ          = 2
	IBV_WC_RECV               = 128
	IBV_WC_RECV_RDMA_WITH_IMM = 129
)

// Send flags.
const (
	IBV_SEND_SIGNALED = 1
	IBV_SEND_INLINE   = 8
)

// Access flags.
const (
	IBV_ACCESS_LOCAL_WRITE   = 1
	IBV_ACCESS_REMOTE_READ   = 2
	IBV_ACCESS_REMOTE_WRITE  = 4
	IBV_ACCESS_REMOTE_ATOMIC = 8
)

// RepostItem is used for the C hot-path repost operations.
type RepostItem struct {
	QP   unsafe.Pointer
	Buf  unsafe.Pointer
	Data unsafe.Pointer
	Size uint32
	MR   unsafe.Pointer
}

// SendItem is used for batch send operations.
type SendItem struct {
	QP    unsafe.Pointer
	Buf   unsafe.Pointer
	Size  uint32
	MR    unsafe.Pointer
	Flags int
	WRID  uint64
}
