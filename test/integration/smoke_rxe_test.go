//go:build integration

// Package integration — real-verbs end-to-end smoke test (VERIFY-001).
//
// Unlike smoke_test.go (which runs against mock verbs under `-tags mock`),
// this test exercises the *real* RDMA data path via libibverbs/librdmacm and
// is meant to run over SoftRoCE (the `rdma_rxe` kernel module) in CI.
//
// It validates the proven PingPongConn data path described in design.md §14.4
// (rdma.Listen / rdma.Dial → Conn.Send / Conn.Recv), satisfying the §14.5
// Rule 5 minimum gate: client.Send("hello") → server.Recv() == "hello".
//
// NOTE: This does NOT exercise the modular pkg/ stack (aggregation, CQ poller,
// buffer pools). Per design.md §14.6 that path is not yet wired to real verbs;
// validating it on rxe is tracked separately (VERIFY-002).
//
// Run: go test -tags integration -v -timeout 120s ./test/integration/
// Requires: a working RDMA device (e.g. SoftRoCE rxe0). The server/client
// address is taken from $GOLINKER_RXE_IP (the rxe netdev's IPv4), falling back
// to the first non-loopback global-unicast IPv4 on the host.
package integration

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/wua20/golinker/internal/rdma"
)

const rxeTestPort = 18629

// rdmaTestHost returns the IPv4 address the RDMA CM should bind/connect to.
func rdmaTestHost(t *testing.T) string {
	t.Helper()
	if h := os.Getenv("GOLINKER_RXE_IP"); h != "" {
		return h
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("enumerating interfaces: %v", err)
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip4 := ipnet.IP.To4()
		if ip4 == nil || ip4.IsLoopback() || !ip4.IsGlobalUnicast() {
			continue
		}
		return ip4.String()
	}
	t.Skip("no non-loopback IPv4 address found for RDMA bind; set GOLINKER_RXE_IP")
	return ""
}

// recvString runs conn.Recv (which busy-polls and blocks) in a goroutine and
// returns the result over a channel so the caller can bound it with a timeout.
func recvString(conn *rdma.Conn, n int) (<-chan string, <-chan error) {
	out := make(chan string, 1)
	errc := make(chan error, 1)
	go func() {
		buf := make([]byte, n)
		got, err := conn.Recv(buf)
		if err != nil {
			errc <- err
			return
		}
		out <- string(buf[:got])
	}()
	return out, errc
}

func TestRxeSmoke_RoundTrip(t *testing.T) {
	host := rdmaTestHost(t)
	addr := net.JoinHostPort(host, strconv.Itoa(rxeTestPort))
	cfg := rdma.DefaultConfig()

	ln, err := rdma.Listen(addr, cfg)
	if err != nil {
		t.Fatalf("Listen(%s): %v", addr, err)
	}
	defer ln.Close()

	// Accept in the background; it blocks until the client connects.
	type acceptResult struct {
		conn *rdma.Conn
		err  error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		c, e := ln.Accept(ctx)
		acceptCh <- acceptResult{c, e}
	}()

	// Client dials the server.
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	client, err := rdma.Dial(dialCtx, addr, cfg)
	if err != nil {
		t.Fatalf("Dial(%s): %v", addr, err)
	}
	defer client.Close()

	var server *rdma.Conn
	select {
	case res := <-acceptCh:
		if res.err != nil {
			t.Fatalf("Accept: %v", res.err)
		}
		server = res.conn
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for Accept")
	}
	defer server.Close()

	// Forward: client → server.
	srvRecv, srvErr := recvString(server, 64)
	if err := client.Send([]byte("hello")); err != nil {
		t.Fatalf("client.Send: %v", err)
	}
	select {
	case got := <-srvRecv:
		if got != "hello" {
			t.Fatalf("server received %q, want %q", got, "hello")
		}
	case err := <-srvErr:
		t.Fatalf("server.Recv: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for server to receive")
	}

	// Reverse: server → client.
	cliRecv, cliErr := recvString(client, 64)
	if err := server.Send([]byte("world")); err != nil {
		t.Fatalf("server.Send: %v", err)
	}
	select {
	case got := <-cliRecv:
		if got != "world" {
			t.Fatalf("client received %q, want %q", got, "world")
		}
	case err := <-cliErr:
		t.Fatalf("client.Recv: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for client to receive")
	}
}
