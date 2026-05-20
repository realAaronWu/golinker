//go:build !mock

package main

import (
	"context"
	"fmt"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
)

func initVerbs(deviceName string) (api.Verbs, api.ProtectionDomain, error) {
	if deviceName == "" {
		names, err := rdma.GetDeviceList()
		if err != nil {
			return nil, nil, fmt.Errorf("listing RDMA devices: %w", err)
		}
		if len(names) == 0 {
			return nil, nil, fmt.Errorf("no RDMA devices found")
		}
		deviceName = names[0]
	}

	v := rdma.NewRealVerbs()
	if err := v.OpenDevice(deviceName); err != nil {
		return nil, nil, fmt.Errorf("opening device %s: %w", deviceName, err)
	}
	pd, err := v.AllocPD()
	if err != nil {
		v.Close()
		return nil, nil, fmt.Errorf("allocating PD on %s: %w", deviceName, err)
	}
	return v, pd, nil
}

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
