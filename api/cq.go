package api

import "context"

// WorkCompletion represents a single ibv_wc.
type WorkCompletion struct {
	WRID    uint64
	Status  WCStatus
	Opcode  WCOpcode
	ByteLen uint32
	QPN     uint32
	IMM     uint32
	HasIMM  bool
}

// CompletionHandler processes completed work requests.
type CompletionHandler interface {
	OnCompletion(wc *WorkCompletion)
	OnError(wc *WorkCompletion, err error)
}

// CQPoller polls one or more completion queues.
type CQPoller interface {
	// Start begins the poll loop (blocks until ctx is cancelled).
	Start(ctx context.Context) error
	// AddCQ registers a CQ with this poller.
	AddCQ(cq CompletionQueue, handler CompletionHandler) error
	// RemoveCQ unregisters a CQ.
	RemoveCQ(cq CompletionQueue) error
	// Stats returns polling metrics.
	Stats() CQPollerStats
	// Close stops polling and releases resources.
	Close() error
}

// CQPool manages a pool of CQ pollers for load distribution.
type CQPool interface {
	// Assign returns the least-loaded poller for a new connection.
	Assign() (CQPoller, CompletionQueue, error)
	// Release returns a CQ to the pool.
	Release(cq CompletionQueue) error
	// Close shuts down all pollers.
	Close() error
}

// CQPollerStats contains polling metrics.
type CQPollerStats struct {
	PollCycles   uint64
	Completions  uint64
	EmptyPolls   uint64
	Errors       uint64
	AvgBatchSize float64
}
