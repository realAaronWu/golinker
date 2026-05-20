package rdma

// PollMode controls how CQ completions are harvested.
type PollMode int

const (
	// PollBusy spins on ibv_poll_cq in a tight loop.
	// Lowest latency, highest CPU.
	PollBusy PollMode = iota

	// PollEvent uses completion channels (ibv_get_cq_event) to sleep
	// until the NIC signals a completion. Lowest CPU, higher latency.
	PollEvent

	// PollAdaptive starts event-driven and switches to busy-poll when
	// the completion rate exceeds a threshold.
	PollAdaptive
)

// Config holds parameters for RDMA connections.
type Config struct {
	// BufSize is the size of each send/recv buffer in bytes.
	BufSize int

	// QueueDepth is the max number of outstanding send/recv WRs.
	QueueDepth int

	// Poll controls how CQ completions are harvested.
	// Currently only PollBusy is implemented.
	Poll PollMode
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		BufSize:    12288,
		QueueDepth: 128,
		Poll:       PollBusy,
	}
}
