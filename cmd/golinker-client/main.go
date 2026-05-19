package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wua20/golinker/pkg/config"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file")
	serverAddr := flag.String("server", "localhost", "server address")
	port := flag.Int("port", 0, "server port (overrides config)")
	numMessages := flag.Int("n", 100, "number of messages to send")
	messageSize := flag.Int("size", 64, "message size in bytes")
	flag.Parse()

	// Load config
	var cfg *config.Config
	if *configPath != "" {
		var err error
		cfg, err = config.LoadFromFile(*configPath)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
	} else {
		cfg = config.DefaultConfig()
	}

	if *port != 0 {
		cfg.Port = *port
	}

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	target := fmt.Sprintf("%s:%d", *serverAddr, cfg.Port)
	fmt.Printf("golinker client connecting to %s\n", target)
	fmt.Printf("  Messages: %d, size: %d bytes\n", *numMessages, *messageSize)

	// TODO: Wire up real connection when pkg/connection is integrated
	// For now, simulate a send loop to demonstrate the CLI structure
	fmt.Println("golinker client ready (awaiting full integration with pkg/connection)")

	start := time.Now()
	for i := 0; i < *numMessages; i++ {
		select {
		case <-ctx.Done():
			fmt.Printf("\nInterrupted after %d messages\n", i)
			return
		default:
		}
		// Simulated send (will be replaced with real RDMA send)
		_ = make([]byte, *messageSize)
	}
	elapsed := time.Since(start)

	fmt.Printf("Completed %d messages in %v\n", *numMessages, elapsed)
	fmt.Printf("  Throughput: %.0f msgs/sec\n", float64(*numMessages)/elapsed.Seconds())
}
