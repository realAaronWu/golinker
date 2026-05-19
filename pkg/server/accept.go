package server

import (
	"context"
)

// acceptLoop runs in a goroutine, accepting incoming connections from the ConnectionManager.
func (s *Server) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.connMgr.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return // shutdown
			}
			continue // transient error, retry
		}

		s.mu.Lock()
		s.conns[conn.ID()] = conn
		s.mu.Unlock()

		s.wg.Add(1)
		go s.handleConn(ctx, conn)
	}
}
