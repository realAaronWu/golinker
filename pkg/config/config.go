// Package config provides configuration structures and validation for golinker.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// PollMode represents the CQ polling strategy.
type PollMode int

const (
	// PollModeBusy uses a tight busy-poll loop for lowest latency.
	PollModeBusy PollMode = 0
	// PollModeEvent uses Go netpoller on CQ completion channel FD.
	PollModeEvent PollMode = 1
	// PollModeSmart uses busy-poll with adaptive fallback (default).
	PollModeSmart PollMode = 2
	// PollModeUser allows the application to call Poll() explicitly.
	PollModeUser PollMode = 3
)

// Config holds all configuration for a golinker instance.
type Config struct {
	// Network
	Endpoint      string `yaml:"endpoint"`
	Port          int    `yaml:"port"`
	DevicePostfix string `yaml:"device_postfix"`

	// CQ
	CQNumber      int      `yaml:"cq_number"`
	PollMode      PollMode `yaml:"poll_mode"`
	InitialCQSize int      `yaml:"initial_cq_size"`
	MaxCQSize     int      `yaml:"max_cq_size"`

	// Buffers
	BufferSize            int   `yaml:"buffer_size"`
	BufferSendThreshold   int   `yaml:"buffer_send_threshold"`
	QueueDepth            int   `yaml:"queue_depth"`
	NUMANode              int   `yaml:"numa_node"`
	EnableAggregate       bool  `yaml:"enable_aggregate"`
	BufferMemoryThreshold int64 `yaml:"buffer_memory_threshold_mb"`
	MaxLargeBufferCap     int64 `yaml:"max_large_buffer_cap_mb"`

	// Timeouts
	ConnectTimeout          time.Duration `yaml:"connect_timeout"`
	HeartbeatInterval       time.Duration `yaml:"heartbeat_interval"`
	ConnectionIdleHeartbeat time.Duration `yaml:"connection_idle_heartbeat"`
	ConnectionIdleExpire    time.Duration `yaml:"connection_idle_expire"`
	LargeBufferMaxLive      time.Duration `yaml:"large_buffer_max_liveness"`
	BufferMonitorCycle      time.Duration `yaml:"buffer_monitor_cycle"`
}

// DefaultConfig returns a Config with all default values set.
func DefaultConfig() *Config {
	return &Config{
		Port:                    8629,
		CQNumber:                2,
		PollMode:                PollModeSmart,
		InitialCQSize:           4096,
		MaxCQSize:               16384,
		BufferSize:              12288,
		BufferSendThreshold:     9216,
		QueueDepth:              128,
		NUMANode:                0,
		EnableAggregate:         true,
		BufferMemoryThreshold:   3072,
		MaxLargeBufferCap:       1024,
		ConnectTimeout:          10 * time.Second,
		HeartbeatInterval:       5 * time.Second,
		ConnectionIdleHeartbeat: 290 * time.Second,
		ConnectionIdleExpire:    300 * time.Second,
		LargeBufferMaxLive:      5 * time.Second,
		BufferMonitorCycle:      3 * time.Second,
	}
}

// Validate checks the configuration for invalid values.
// It returns a descriptive error for the first failing check.
func (c *Config) Validate() error {
	if c.Port <= 0 || c.Port >= 65536 {
		return fmt.Errorf("config: port must be > 0 and < 65536, got %d", c.Port)
	}
	if c.CQNumber <= 0 {
		return fmt.Errorf("config: cq_number must be > 0, got %d", c.CQNumber)
	}
	if c.InitialCQSize <= 0 {
		return fmt.Errorf("config: initial_cq_size must be > 0, got %d", c.InitialCQSize)
	}
	if c.MaxCQSize < c.InitialCQSize {
		return fmt.Errorf("config: max_cq_size (%d) must be >= initial_cq_size (%d)", c.MaxCQSize, c.InitialCQSize)
	}
	if c.BufferSize <= 0 {
		return fmt.Errorf("config: buffer_size must be > 0, got %d", c.BufferSize)
	}
	if c.BufferSendThreshold <= 0 {
		return fmt.Errorf("config: buffer_send_threshold must be > 0, got %d", c.BufferSendThreshold)
	}
	if c.BufferSendThreshold >= c.BufferSize {
		return fmt.Errorf("config: buffer_send_threshold (%d) must be < buffer_size (%d)", c.BufferSendThreshold, c.BufferSize)
	}
	if c.QueueDepth <= 0 {
		return fmt.Errorf("config: queue_depth must be > 0, got %d", c.QueueDepth)
	}
	if c.QueueDepth&(c.QueueDepth-1) != 0 {
		return fmt.Errorf("config: queue_depth must be a power of 2, got %d", c.QueueDepth)
	}
	if c.ConnectTimeout <= 0 {
		return fmt.Errorf("config: connect_timeout must be > 0, got %v", c.ConnectTimeout)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("config: heartbeat_interval must be > 0, got %v", c.HeartbeatInterval)
	}
	if c.ConnectionIdleHeartbeat <= 0 {
		return fmt.Errorf("config: connection_idle_heartbeat must be > 0, got %v", c.ConnectionIdleHeartbeat)
	}
	if c.ConnectionIdleExpire <= 0 {
		return fmt.Errorf("config: connection_idle_expire must be > 0, got %v", c.ConnectionIdleExpire)
	}
	if c.ConnectionIdleExpire <= c.ConnectionIdleHeartbeat {
		return fmt.Errorf("config: connection_idle_expire (%v) must be > connection_idle_heartbeat (%v)", c.ConnectionIdleExpire, c.ConnectionIdleHeartbeat)
	}
	if c.LargeBufferMaxLive <= 0 {
		return fmt.Errorf("config: large_buffer_max_liveness must be > 0, got %v", c.LargeBufferMaxLive)
	}
	if c.BufferMonitorCycle <= 0 {
		return fmt.Errorf("config: buffer_monitor_cycle must be > 0, got %v", c.BufferMonitorCycle)
	}
	return nil
}

// LoadFromFile reads a YAML configuration file, unmarshals it into a Config,
// and validates the result.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read file %s: %w", path, err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}
