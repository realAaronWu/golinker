package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// RDMACounters holds RDMA hardware performance counters.
type RDMACounters struct {
	TXPackets uint64
	RXPackets uint64
	TXBytes   uint64
	RXBytes   uint64
}

// ReadCounters reads RDMA hardware counters from sysfs.
// Returns zero counters without error if sysfs is not available.
func ReadCounters(device string, port int) (*RDMACounters, error) {
	base := fmt.Sprintf("/sys/class/infiniband/%s/ports/%d/counters", device, port)
	c := &RDMACounters{}

	// Read each counter file, ignore errors (sysfs may not exist)
	c.TXPackets = readSysfsCounter(base, "port_xmit_packets")
	c.RXPackets = readSysfsCounter(base, "port_rcv_packets")
	c.TXBytes = readSysfsCounter(base, "port_xmit_data")
	c.RXBytes = readSysfsCounter(base, "port_rcv_data")

	return c, nil
}

// DeltaCounters computes the difference between two counter snapshots.
func DeltaCounters(before, after *RDMACounters) *RDMACounters {
	if before == nil || after == nil {
		return &RDMACounters{}
	}
	return &RDMACounters{
		TXPackets: after.TXPackets - before.TXPackets,
		RXPackets: after.RXPackets - before.RXPackets,
		TXBytes:   after.TXBytes - before.TXBytes,
		RXBytes:   after.RXBytes - before.RXBytes,
	}
}

func readSysfsCounter(base, name string) uint64 {
	data, err := os.ReadFile(fmt.Sprintf("%s/%s", base, name))
	if err != nil {
		return 0
	}
	val, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return val
}
