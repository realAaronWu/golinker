package rdma

import "log"

// DebugLog controls diagnostic logging for RDMA stage boundaries.
// Set to true before calling any RDMA functions to enable verbose tracing.
var DebugLog bool

func debugf(format string, args ...any) {
	if DebugLog {
		log.Printf("[rdma] "+format, args...)
	}
}
