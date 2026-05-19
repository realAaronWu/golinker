package server

import (
	"context"

	"github.com/wua20/golinker/api"
)

// handleConn runs in a goroutine per connection, receiving and processing messages.
func (s *Server) handleConn(ctx context.Context, conn api.Connection) {
	defer s.wg.Done()
	defer s.removeConn(conn.ID())

	for {
		msg, err := conn.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown
			}
			return // connection closed or error
		}

		if s.handler != nil {
			resp, err := s.handler.Handle(conn, msg)
			if err != nil {
				continue
			}
			if resp != nil {
				_ = conn.Send(resp)
			}
		}
	}
}

// removeConn removes a connection from the tracked connections map.
func (s *Server) removeConn(id uint64) {
	s.mu.Lock()
	delete(s.conns, id)
	s.mu.Unlock()
}
