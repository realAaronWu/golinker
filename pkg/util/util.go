// Package util provides utility functions for golinker.
package util

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DetectNUMANode reads the NUMA node for the given RDMA device from sysfs.
// Path: /sys/class/infiniband/<device>/device/numa_node
// Returns -1 if detection fails (e.g., device not found or not on Linux).
func DetectNUMANode(deviceName string) int {
	if deviceName == "" {
		return -1
	}
	path := filepath.Join("/sys/class/infiniband", deviceName, "device", "numa_node")
	data, err := os.ReadFile(path)
	if err != nil {
		return -1
	}
	node, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return -1
	}
	// -1 from sysfs means "no NUMA info available"
	if node < 0 {
		return -1
	}
	return node
}

// ListRDMADevices returns the names of available RDMA devices from sysfs.
func ListRDMADevices() []string {
	entries, err := os.ReadDir("/sys/class/infiniband")
	if err != nil {
		return nil
	}
	var devices []string
	for _, e := range entries {
		devices = append(devices, e.Name())
	}
	return devices
}

// FormatNUMAInfo returns a human-readable string about NUMA configuration.
func FormatNUMAInfo(deviceName string, configuredNode int) string {
	detected := DetectNUMANode(deviceName)
	if detected >= 0 {
		if configuredNode != detected {
			return fmt.Sprintf("NUMA: device %s on node %d (config says %d, using detected)",
				deviceName, detected, configuredNode)
		}
		return fmt.Sprintf("NUMA: device %s on node %d", deviceName, detected)
	}
	return fmt.Sprintf("NUMA: node %d (from config, device detection unavailable)", configuredNode)
}
