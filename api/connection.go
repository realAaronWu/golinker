package api

import "context"

// ConnectionState represents the RDMA connection state machine.
type ConnectionState int

const (
	StateInit       ConnectionState = iota
	StateConnecting
	StateConnected
	StateDraining
	StateClosed
	StateError
)

// MessageFlags defines flags for message transmission.
type MessageFlags uint32

// Message is the wire-format unit.
type Message struct {
	Buffer  *Buffer
	Length  int
	Flags   MessageFlags
	ImmData uint32
}

// Connection represents a single RDMA connection (one QP).
type Connection interface {
	ID() uint64
	RemoteAddr() string
	State() ConnectionState

	// Send posts a send WR. Non-blocking; completion via CompletionHandler.
	Send(msg *Message) error

	// Recv returns the next received message (blocks).
	Recv(ctx context.Context) (*Message, error)

	// Close initiates graceful disconnect.
	Close() error

	// OnStateChange registers a callback for state transitions.
	OnStateChange(fn func(old, new ConnectionState))
}

// ConnectionManager handles connection lifecycle.
type ConnectionManager interface {
	// Accept processes an incoming connection request.
	Accept(ctx context.Context) (Connection, error)

	// Connect initiates an outbound connection.
	Connect(ctx context.Context, addr string) (Connection, error)

	// GetConnection retrieves a connection by ID.
	GetConnection(id uint64) (Connection, bool)

	// Close shuts down all connections.
	Close() error
}

// Server is the top-level RDMA server.
type Server interface {
	// Start begins listening and accepting connections.
	Start(ctx context.Context) error
	// Stop gracefully shuts down the server.
	Stop(ctx context.Context) error
	// RegisterHandler sets the message handler for incoming messages.
	RegisterHandler(handler MessageHandler)
}

// MessageHandler processes incoming messages.
type MessageHandler interface {
	Handle(conn Connection, msg *Message) (*Message, error)
}
