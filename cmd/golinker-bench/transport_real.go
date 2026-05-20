//go:build !mock

package main

import (
	"context"
	"fmt"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
)

func initCMListener(addr string, port int) (api.CMEventChannel, api.CMAcceptor, error) {
	ch := &rdma.RealCMEventChannel{}
	if err := ch.Listen(context.Background(), addr, port); err != nil {
		return nil, nil, fmt.Errorf("CM listen on %s:%d: %w", addr, port, err)
	}
	// RealCMEventChannel implements both CMEventChannel and CMAcceptor
	return ch, ch, nil
}

func initCMDialer() (api.CMDialer, error) {
	return &rdma.RealCMDialer{}, nil
}
