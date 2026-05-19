package message

import (
	"unsafe"

	"github.com/wua20/golinker/api"
)

// ImmediateSender sends messages without batching for lowest latency.
type ImmediateSender struct {
	sendPool api.SendBufferPool
	conn     api.Connection
}

// NewImmediateSender creates a new ImmediateSender.
func NewImmediateSender(conn api.Connection, sendPool api.SendBufferPool) *ImmediateSender {
	return &ImmediateSender{
		sendPool: sendPool,
		conn:     conn,
	}
}

// Send acquires a buffer, packs a single message, and posts a send immediately.
func (s *ImmediateSender) Send(data []byte) error {
	buf, err := s.sendPool.AcquireForSend()
	if err != nil {
		return err
	}

	// Pack the message into the buffer as a single-message batch
	totalNeeded := BatchHeaderSize + MsgHeaderSize + len(data)
	if totalNeeded > buf.Length {
		s.sendPool.CompleteSend(buf)
		return ErrBufferTooSmall
	}

	// Write directly into the registered buffer memory
	dest := unsafe.Slice((*byte)(buf.Addr), buf.Length)
	n := PackSingle(dest, data)

	msg := &api.Message{
		Buffer: buf,
		Length: n,
	}

	return s.conn.Send(msg)
}

// OnSendComplete returns the buffer to the pool after send completion.
func (s *ImmediateSender) OnSendComplete(buf *api.Buffer) {
	s.sendPool.CompleteSend(buf)
}
