package main

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/wua20/golinker/internal/rdma"
)

// executePing tests RDMA CM connectivity by establishing a connection,
// sending one message, receiving the echo, and disconnecting.
// Useful for diagnosing CM-level issues before running full benchmarks.
func executePing(cmd *cobra.Command, args []string) error {
	rdma.DebugLog = true // always verbose for ping

	timeout, _ := cmd.Flags().GetInt("timeout")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	fmt.Printf("golinker-bench ping\n")
	fmt.Printf("  Target: %s\n", addr)
	fmt.Printf("  Timeout: %ds\n", timeout)
	fmt.Println()

	// Step 1: Dial
	fmt.Printf("[1/4] Connecting...\n")
	start := time.Now()
	cfg := rdma.Config{BufSize: 4096, QueueDepth: 16}
	conn, err := rdma.Dial(ctx, addr, cfg)
	if err != nil {
		return fmt.Errorf("connect failed after %v: %w", time.Since(start), err)
	}
	defer conn.Close()
	connectTime := time.Since(start)
	fmt.Printf("  Connected in %v\n", connectTime)

	// Step 2: Send
	fmt.Printf("[2/4] Sending 64-byte ping...\n")
	payload := make([]byte, 64)
	for i := range payload {
		payload[i] = byte(i)
	}
	start = time.Now()
	if err := conn.Send(payload); err != nil {
		return fmt.Errorf("send failed: %w", err)
	}
	sendTime := time.Since(start)
	fmt.Printf("  Sent in %v\n", sendTime)

	// Step 3: Recv
	fmt.Printf("[3/4] Waiting for echo...\n")
	recvBuf := make([]byte, 4096)
	start = time.Now()
	n, err := conn.Recv(recvBuf)
	if err != nil {
		return fmt.Errorf("recv failed: %w", err)
	}
	rtt := time.Since(start)
	fmt.Printf("  Received %d bytes in %v\n", n, rtt)

	// Step 4: Verify
	fmt.Printf("[4/4] Verifying...\n")
	match := true
	for i := 0; i < len(payload) && i < n; i++ {
		if recvBuf[i] != payload[i] {
			match = false
			break
		}
	}
	if match && n == len(payload) {
		fmt.Printf("  Echo payload matches!\n")
	} else {
		fmt.Printf("  WARNING: echo mismatch (sent %d bytes, got %d)\n", len(payload), n)
	}

	fmt.Println()
	fmt.Printf("Results:\n")
	fmt.Printf("  Connect: %v\n", connectTime)
	fmt.Printf("  RTT:     %v\n", sendTime+rtt)
	fmt.Printf("  Status:  OK\n")

	return nil
}
