package message

import (
	"encoding/binary"
	"errors"
)

// Wire format header (per message within a batch):
// [4 bytes: message length (uint32 little-endian)] [N bytes: payload]
//
// An aggregated buffer looks like:
// [batch header: 4 bytes count] [msg1_len][msg1_data][msg2_len][msg2_data]...

const (
	// BatchHeaderSize is the size of the batch header (uint32 message count).
	BatchHeaderSize = 4
	// MsgHeaderSize is the per-message length prefix size (uint32).
	MsgHeaderSize = 4
)

var (
	// ErrBufferTooSmall indicates the buffer is too small for the data.
	ErrBufferTooSmall = errors.New("message: buffer too small")
	// ErrInvalidBatch indicates a malformed batch buffer.
	ErrInvalidBatch = errors.New("message: invalid batch format")
)

// PackBatch writes batch header + length-prefixed messages into buf.
// Returns total bytes written.
func PackBatch(buf []byte, messages [][]byte) int {
	offset := 0

	// Write batch header: message count
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(messages)))
	offset += BatchHeaderSize

	// Write each message with length prefix
	for _, msg := range messages {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(msg)))
		offset += MsgHeaderSize
		copy(buf[offset:], msg)
		offset += len(msg)
	}

	return offset
}

// UnpackBatch parses a batch buffer into individual messages.
// length is the total number of valid bytes in data.
func UnpackBatch(data []byte, length int) ([][]byte, error) {
	if length < BatchHeaderSize {
		return nil, ErrInvalidBatch
	}

	count := binary.LittleEndian.Uint32(data[0:BatchHeaderSize])
	offset := BatchHeaderSize

	messages := make([][]byte, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+MsgHeaderSize > length {
			return nil, ErrInvalidBatch
		}
		msgLen := int(binary.LittleEndian.Uint32(data[offset : offset+MsgHeaderSize]))
		offset += MsgHeaderSize

		if offset+msgLen > length {
			return nil, ErrInvalidBatch
		}
		msg := make([]byte, msgLen)
		copy(msg, data[offset:offset+msgLen])
		messages = append(messages, msg)
		offset += msgLen
	}

	return messages, nil
}

// PackSingle writes a single message as a count=1 batch. Returns bytes written.
func PackSingle(buf []byte, msg []byte) int {
	return PackBatch(buf, [][]byte{msg})
}

// BatchSize calculates the total buffer space needed for a batch of messages.
func BatchSize(messages [][]byte) int {
	size := BatchHeaderSize
	for _, msg := range messages {
		size += MsgHeaderSize + len(msg)
	}
	return size
}
