package connection

import (
	"context"
	"fmt"

	"github.com/wua20/golinker/api"
)

// RunCMEventLoop processes CM events in a loop until the context is cancelled.
// This should be run in a goroutine.
func (m *Manager) RunCMEventLoop(ctx context.Context) error {
	for {
		event, err := m.cfg.CMChannel.GetEvent(ctx)
		if err != nil {
			// Context cancelled is a normal shutdown
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("cm event loop: get event: %w", err)
		}

		m.handleCMEvent(event)

		// Always ack the event
		if ackErr := m.cfg.CMChannel.AckEvent(event); ackErr != nil {
			// Log but don't return - continue processing events
			_ = ackErr
		}
	}
}

// handleCMEvent processes a single CM event.
func (m *Manager) handleCMEvent(event *api.CMEvent) {
	switch event.Type {
	case api.EventConnectRequest:
		m.handleConnectRequest(event)

	case api.EventEstablished:
		m.handleEstablished(event)

	case api.EventDisconnected:
		m.handleDisconnected(event)

	case api.EventRejected:
		m.handleRejected(event)

	case api.EventAddrResolved:
		// Address resolution completed - part of outbound connect flow
		// In full implementation, would trigger route resolution

	case api.EventRouteResolved:
		// Route resolution completed - part of outbound connect flow
		// In full implementation, would trigger connection request

	default:
		// Unknown event type - ignore
	}
}

// handleConnectRequest processes an incoming connection request.
func (m *Manager) handleConnectRequest(event *api.CMEvent) {
	if m.closed.Load() {
		return
	}

	id := m.allocateConnID()
	deps := ConnDeps{
		Verbs:    m.cfg.Verbs,
		QP:       nil, // Will be set up during connection establishment
		SendPool: m.cfg.SendPool,
		RecvPool: m.cfg.RecvPool,
	}

	conn := NewConn(id, remoteAddrFromEvent(event), deps)
	conn.SetState(api.StateConnecting)

	m.storeConnection(conn)

	// Offer the connection on the accept channel (non-blocking to avoid deadlock)
	select {
	case m.acceptCh <- conn:
	default:
		// Accept channel full - connection will still be available via GetConnection
	}
}

// handleEstablished transitions a connection to Connected state.
func (m *Manager) handleEstablished(event *api.CMEvent) {
	// Find the connection associated with this event
	// In a real implementation, we'd use the CM ID to find the connection
	// For now, iterate and find the most recent connecting connection
	m.connections.Range(func(key, value any) bool {
		conn := value.(*Conn)
		if conn.State() == api.StateConnecting {
			conn.SetState(api.StateConnected)
			return false // stop iteration
		}
		return true
	})
}

// handleDisconnected transitions a connection to Closed state.
func (m *Manager) handleDisconnected(event *api.CMEvent) {
	m.connections.Range(func(key, value any) bool {
		conn := value.(*Conn)
		if conn.State() == api.StateConnected {
			conn.Close()
			return false // stop iteration
		}
		return true
	})
}

// handleRejected transitions a connection to Error state.
func (m *Manager) handleRejected(event *api.CMEvent) {
	m.connections.Range(func(key, value any) bool {
		conn := value.(*Conn)
		if conn.State() == api.StateConnecting {
			conn.SetState(api.StateError)
			return false // stop iteration
		}
		return true
	})
}

// remoteAddrFromEvent extracts a remote address from a CM event.
// In a real implementation, this would parse the event's connection info.
func remoteAddrFromEvent(event *api.CMEvent) string {
	if event.PrivateData != nil && len(event.PrivateData) > 0 {
		return string(event.PrivateData)
	}
	return "unknown"
}
