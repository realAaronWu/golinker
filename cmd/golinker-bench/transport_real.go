//go:build !mock

package main

import (
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
	return nil, nil, fmt.Errorf("real RDMA verbs adapter not yet wired for device %s", deviceName)
}
