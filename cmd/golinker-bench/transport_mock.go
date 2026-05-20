//go:build mock

package main

import (
	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/internal/rdma"
)

func initVerbs(deviceName string) (api.Verbs, api.ProtectionDomain, error) {
	v := rdma.NewMockVerbs()
	pd, err := v.AllocPD()
	if err != nil {
		return nil, nil, err
	}
	return v, pd, nil
}

func initCMListener(addr string, port int) (api.CMEventChannel, api.CMAcceptor, error) {
	ch := rdma.NewMockCMEventChannel(64)
	return ch, rdma.NewMockCMAcceptor(), nil
}

func initCMDialer() (api.CMDialer, error) {
	return rdma.NewMockCMDialer(), nil
}
