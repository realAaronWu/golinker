package main

import (
	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/buffer"
	"github.com/wua20/golinker/pkg/config"
	"github.com/wua20/golinker/pkg/cq"
)

func newBufferPool(verbs api.Verbs, pd api.ProtectionDomain, bufSize, count, numaNode int) (*buffer.Pool, error) {
	cfg := buffer.PoolConfig{
		BufferSize:  bufSize,
		BufferCount: count,
		NUMANode:    numaNode,
	}
	return buffer.NewPool(verbs, pd, cfg)
}

func newCQPool(verbs api.Verbs, numPollers int, pollModeStr string) (*cq.Pool, error) {
	cfg := cq.PollerConfig{
		PollMode:     parsePollMode(pollModeStr),
		MaxBatchSize: 32,
		SpinCount:    1024,
	}
	return cq.NewPool(verbs, numPollers, cfg)
}

func parsePollMode(s string) config.PollMode {
	switch s {
	case "busy":
		return config.PollModeBusy
	case "event":
		return config.PollModeEvent
	case "smart":
		return config.PollModeSmart
	case "user":
		return config.PollModeUser
	default:
		return config.PollModeBusy
	}
}
