package message

import (
	"encoding/binary"
	"errors"
	"time"
)

// Command header constants
const (
	CmdHeaderSize = 12 // 4B type + 8B reserved
	AppHeaderSize = 12 // 8B timestamp + 4B size
)

// Command types per design.md §11
const (
	CmdPostSend       uint32 = 230
	CmdReadInvitation uint32 = 231
	CmdReadComplete   uint32 = 232
	CmdWriteRequest   uint32 = 233
	CmdWriteApprove   uint32 = 234
	CmdHeartbeat      uint32 = 235
)

var (
	// ErrBufferTooSmall indicates the buffer is too small for the data.
	ErrBufferTooSmall = errors.New("message: buffer too small")
	// ErrInvalidBatch indicates a malformed batch buffer.
	ErrInvalidBatch = errors.New("message: invalid batch format")
	// ErrUnknownCommand indicates an unrecognized command type.
	ErrUnknownCommand = errors.New("message: unknown command type")
)

// EncodeCmdHeader writes a 12-byte command header at buf[0:12].
func EncodeCmdHeader(buf []byte, cmdType uint32) {
	binary.BigEndian.PutUint32(buf[0:4], cmdType)
	// reserved 8 bytes: zero them
	binary.BigEndian.PutUint32(buf[4:8], 0)
	binary.BigEndian.PutUint32(buf[8:12], 0)
}

// DecodeCmdHeader reads the command type from a 12-byte header.
func DecodeCmdHeader(buf []byte) (uint32, error) {
	if len(buf) < CmdHeaderSize {
		return 0, ErrInvalidBatch
	}
	return binary.BigEndian.Uint32(buf[0:4]), nil
}

// EncodeAppHeader writes a 12-byte app message header.
func EncodeAppHeader(buf []byte, timestamp uint64, size uint32) {
	binary.BigEndian.PutUint64(buf[0:8], timestamp)
	binary.BigEndian.PutUint32(buf[8:12], size)
}

// DecodeAppHeader reads timestamp and message size from a 12-byte app header.
func DecodeAppHeader(buf []byte) (timestamp uint64, size uint32, err error) {
	if len(buf) < AppHeaderSize {
		return 0, 0, ErrInvalidBatch
	}
	timestamp = binary.BigEndian.Uint64(buf[0:8])
	size = binary.BigEndian.Uint32(buf[8:12])
	return timestamp, size, nil
}

// PackBatch writes a PostSend command header followed by N app messages into buf.
// Each message gets a 12-byte app header (timestamp + size) followed by its payload.
// Returns total bytes written.
func PackBatch(buf []byte, messages [][]byte) int {
	offset := 0

	// Write command header (type = PostSend)
	EncodeCmdHeader(buf[offset:], CmdPostSend)
	offset += CmdHeaderSize

	now := uint64(time.Now().UnixNano())

	// Write each message with app header
	for _, msg := range messages {
		EncodeAppHeader(buf[offset:], now, uint32(len(msg)))
		offset += AppHeaderSize
		copy(buf[offset:], msg)
		offset += len(msg)
	}

	return offset
}

// UnpackBatch parses a buffer containing a command header + N app messages.
// Returns the individual message payloads (without headers).
// length is the total number of valid bytes in data.
func UnpackBatch(data []byte, length int) ([][]byte, error) {
	if length < CmdHeaderSize {
		return nil, ErrInvalidBatch
	}

	cmdType, err := DecodeCmdHeader(data)
	if err != nil {
		return nil, err
	}
	if cmdType != CmdPostSend {
		return nil, ErrUnknownCommand
	}

	offset := CmdHeaderSize
	var messages [][]byte

	for offset < length {
		if offset+AppHeaderSize > length {
			return nil, ErrInvalidBatch
		}
		_, msgSize, err := DecodeAppHeader(data[offset:])
		if err != nil {
			return nil, err
		}
		offset += AppHeaderSize

		if offset+int(msgSize) > length {
			return nil, ErrInvalidBatch
		}
		msg := make([]byte, msgSize)
		copy(msg, data[offset:offset+int(msgSize)])
		messages = append(messages, msg)
		offset += int(msgSize)
	}

	return messages, nil
}

// PackSingle writes a single message as a PostSend with one app message.
// Returns total bytes written.
func PackSingle(buf []byte, msg []byte) int {
	return PackBatch(buf, [][]byte{msg})
}

// BatchSize calculates the total buffer space needed for a batch of messages.
func BatchSize(messages [][]byte) int {
	size := CmdHeaderSize
	for _, msg := range messages {
		size += AppHeaderSize + len(msg)
	}
	return size
}
