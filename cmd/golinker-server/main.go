package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/wua20/golinker/api"
	"github.com/wua20/golinker/pkg/config"
)

func main() {
	configPath := flag.String("config", "", "path to config YAML file")
	listenAddr := flag.String("addr", "", "listen address (overrides config)")
	port := flag.Int("port", 0, "listen port (overrides config)")
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

	// Apply CLI overrides
	if *listenAddr != "" {
		cfg.Endpoint = *listenAddr
	}
	if *port != 0 {
		cfg.Port = *port
	}

	// Setup signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Log configuration
	fmt.Printf("golinker server starting on %s:%d\n", cfg.Endpoint, cfg.Port)
	fmt.Printf("  CQ pollers: %d, poll mode: %d\n", cfg.CQNumber, cfg.PollMode)
	fmt.Printf("  Buffer size: %d, queue depth: %d\n", cfg.BufferSize, cfg.QueueDepth)

	// Create echo handler
	handler := &echoHandler{}
	_ = handler // will be used when server package is wired

	// TODO: Wire up real server components when pkg/server is integrated
	// For now, demonstrate config loading and signal handling
	fmt.Println("golinker server ready (awaiting full integration with pkg/server)")

	select {
	case sig := <-sigCh:
		fmt.Printf("\nReceived signal %v, shutting down...\n", sig)
		cancel()
	case <-ctx.Done():
	}

	fmt.Println("golinker server stopped")
}

// echoHandler echoes received messages back to the sender.
type echoHandler struct{}

func (h *echoHandler) Handle(conn api.Connection, msg *api.Message) (*api.Message, error) {
	// Echo: return the same message back
	return msg, nil
}
