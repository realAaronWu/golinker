//go:build !mock

package cq

import (
	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
)

// C opcode values from ibv_wc_opcode.
const (
	ibvWCSend             = 0
	ibvWCRdmaWrite        = 1
	ibvWCRdmaRead         = 2
	ibvWCRecv             = 128
	ibvWCRecvRdmaWithImm  = 129
)

// convertOpcode maps C ibv_wc_opcode to api.WCOpcode.
func convertOpcode(cop int) api.WCOpcode {
	switch cop {
	case ibvWCSend:
		return api.WCSend
	case ibvWCRdmaWrite:
		return api.WCRdmaWrite
	case ibvWCRdmaRead:
		return api.WCRdmaRead
	case ibvWCRecv:
		return api.WCRecv
	case ibvWCRecvRdmaWithImm:
		return api.WCRecvRdmaWithImm
	default:
		return api.WCOpcode(cop)
	}
}

// convertStatus maps C ibv_wc_status to api.WCStatus.
// The api enum omits IBV_WC_LOC_EEC_OP_ERR (3) which is obsolete,
// so values >= 3 need adjustment.
func convertStatus(cst int) api.WCStatus {
	if cst <= 2 {
		return api.WCStatus(cst)
	}
	// C values 3+ shift down by 1 to match api enum (which skips LOC_EEC_OP_ERR)
	if cst == 3 {
		// LOC_EEC_OP_ERR — map to generic error (use LocQPOpErr as closest)
		return api.WCLocQPOpErr
	}
	return api.WCStatus(cst - 1)
}

// DefaultPollFunc returns a PollFunc that calls the C hot-path via rdma.PollCQByHandle.
func DefaultPollFunc() PollFunc {
	return func(cq api.CompletionQueue, maxWCs int) ([]api.WorkCompletion, error) {
		wcs, err := rdma.PollCQByHandle(cq.Handle(), maxWCs)
		if err != nil {
			return nil, err
		}
		if len(wcs) == 0 {
			return nil, nil
		}
		result := make([]api.WorkCompletion, len(wcs))
		for i := range wcs {
			result[i] = api.WorkCompletion{
				WRID:    wcs[i].WRID,
				Status:  convertStatus(wcs[i].Status),
				Opcode:  convertOpcode(wcs[i].Opcode),
				ByteLen: wcs[i].ByteLen,
				QPN:     wcs[i].QPN,
				IMM:     wcs[i].ImmData,
				HasIMM:  wcs[i].ImmData != 0,
			}
		}
		return result, nil
	}
}
