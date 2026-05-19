package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig_IsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("DefaultConfig() should be valid, got error: %v", err)
	}
}

func TestValidate_InvalidPort(t *testing.T) {
	tests := []struct {
		name string
		port int
	}{
		{"zero port", 0},
		{"negative port", -1},
		{"port too high", 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Port = tt.port
			if err := cfg.Validate(); err == nil {
				t.Errorf("Validate() should fail for port=%d", tt.port)
			}
		})
	}
}

func TestValidate_BufferThreshold(t *testing.T) {
	tests := []struct {
		name      string
		threshold int
		bufSize   int
		wantErr   bool
	}{
		{"threshold equals buffer size", 12288, 12288, true},
		{"threshold exceeds buffer size", 13000, 12288, true},
		{"valid threshold", 9216, 12288, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.BufferSendThreshold = tt.threshold
			cfg.BufferSize = tt.bufSize
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("Validate() should fail for threshold=%d, bufSize=%d", tt.threshold, tt.bufSize)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() should pass for threshold=%d, bufSize=%d, got: %v", tt.threshold, tt.bufSize, err)
			}
		})
	}
}

func TestValidate_QueueDepthPowerOf2(t *testing.T) {
	tests := []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{"not power of 2", 100, true},
		{"power of 2", 128, false},
		{"power of 2 small", 1, false},
		{"power of 2 large", 256, false},
		{"not power of 2 odd", 63, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.QueueDepth = tt.depth
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("Validate() should fail for QueueDepth=%d", tt.depth)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() should pass for QueueDepth=%d, got: %v", tt.depth, err)
			}
		})
	}
}

func TestValidate_Timeouts(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(c *Config)
		wantErr bool
	}{
		{"zero connect_timeout", func(c *Config) { c.ConnectTimeout = 0 }, true},
		{"zero heartbeat_interval", func(c *Config) { c.HeartbeatInterval = 0 }, true},
		{"zero connection_idle_heartbeat", func(c *Config) { c.ConnectionIdleHeartbeat = 0 }, true},
		{"zero connection_idle_expire", func(c *Config) { c.ConnectionIdleExpire = 0 }, true},
		{"zero large_buffer_max_liveness", func(c *Config) { c.LargeBufferMaxLive = 0 }, true},
		{"zero buffer_monitor_cycle", func(c *Config) { c.BufferMonitorCycle = 0 }, true},
		{"negative connect_timeout", func(c *Config) { c.ConnectTimeout = -1 * time.Second }, true},
		{"idle_expire equals idle_heartbeat", func(c *Config) {
			c.ConnectionIdleHeartbeat = 300 * time.Second
			c.ConnectionIdleExpire = 300 * time.Second
		}, true},
		{"idle_expire less than idle_heartbeat", func(c *Config) {
			c.ConnectionIdleHeartbeat = 300 * time.Second
			c.ConnectionIdleExpire = 200 * time.Second
		}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(cfg)
			err := cfg.Validate()
			if tt.wantErr && err == nil {
				t.Errorf("Validate() should fail for %s", tt.name)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("Validate() should pass for %s, got: %v", tt.name, err)
			}
		})
	}
}

func TestValidate_CQSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxCQSize = 100
	cfg.InitialCQSize = 200
	if err := cfg.Validate(); err == nil {
		t.Error("Validate() should fail when MaxCQSize < InitialCQSize")
	}
}

func TestLoadFromFile(t *testing.T) {
	yamlContent := `
endpoint: "192.168.1.10"
port: 9000
device_postfix: "mlx5"
cq_number: 4
poll_mode: 1
initial_cq_size: 2048
max_cq_size: 8192
buffer_size: 8192
buffer_send_threshold: 6144
queue_depth: 64
numa_node: 1
enable_aggregate: false
buffer_memory_threshold_mb: 2048
max_large_buffer_cap_mb: 512
connect_timeout: 15s
heartbeat_interval: 3s
connection_idle_heartbeat: 250s
connection_idle_expire: 260s
large_buffer_max_liveness: 10s
buffer_monitor_cycle: 5s
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	cfg, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile() returned error: %v", err)
	}

	if cfg.Endpoint != "192.168.1.10" {
		t.Errorf("Endpoint = %q, want %q", cfg.Endpoint, "192.168.1.10")
	}
	if cfg.Port != 9000 {
		t.Errorf("Port = %d, want %d", cfg.Port, 9000)
	}
	if cfg.DevicePostfix != "mlx5" {
		t.Errorf("DevicePostfix = %q, want %q", cfg.DevicePostfix, "mlx5")
	}
	if cfg.CQNumber != 4 {
		t.Errorf("CQNumber = %d, want %d", cfg.CQNumber, 4)
	}
	if cfg.PollMode != PollModeEvent {
		t.Errorf("PollMode = %d, want %d", cfg.PollMode, PollModeEvent)
	}
	if cfg.InitialCQSize != 2048 {
		t.Errorf("InitialCQSize = %d, want %d", cfg.InitialCQSize, 2048)
	}
	if cfg.MaxCQSize != 8192 {
		t.Errorf("MaxCQSize = %d, want %d", cfg.MaxCQSize, 8192)
	}
	if cfg.BufferSize != 8192 {
		t.Errorf("BufferSize = %d, want %d", cfg.BufferSize, 8192)
	}
	if cfg.BufferSendThreshold != 6144 {
		t.Errorf("BufferSendThreshold = %d, want %d", cfg.BufferSendThreshold, 6144)
	}
	if cfg.QueueDepth != 64 {
		t.Errorf("QueueDepth = %d, want %d", cfg.QueueDepth, 64)
	}
	if cfg.NUMANode != 1 {
		t.Errorf("NUMANode = %d, want %d", cfg.NUMANode, 1)
	}
	if cfg.EnableAggregate != false {
		t.Errorf("EnableAggregate = %v, want %v", cfg.EnableAggregate, false)
	}
	if cfg.BufferMemoryThreshold != 2048 {
		t.Errorf("BufferMemoryThreshold = %d, want %d", cfg.BufferMemoryThreshold, 2048)
	}
	if cfg.MaxLargeBufferCap != 512 {
		t.Errorf("MaxLargeBufferCap = %d, want %d", cfg.MaxLargeBufferCap, 512)
	}
	if cfg.ConnectTimeout != 15*time.Second {
		t.Errorf("ConnectTimeout = %v, want %v", cfg.ConnectTimeout, 15*time.Second)
	}
	if cfg.HeartbeatInterval != 3*time.Second {
		t.Errorf("HeartbeatInterval = %v, want %v", cfg.HeartbeatInterval, 3*time.Second)
	}
	if cfg.ConnectionIdleHeartbeat != 250*time.Second {
		t.Errorf("ConnectionIdleHeartbeat = %v, want %v", cfg.ConnectionIdleHeartbeat, 250*time.Second)
	}
	if cfg.ConnectionIdleExpire != 260*time.Second {
		t.Errorf("ConnectionIdleExpire = %v, want %v", cfg.ConnectionIdleExpire, 260*time.Second)
	}
	if cfg.LargeBufferMaxLive != 10*time.Second {
		t.Errorf("LargeBufferMaxLive = %v, want %v", cfg.LargeBufferMaxLive, 10*time.Second)
	}
	if cfg.BufferMonitorCycle != 5*time.Second {
		t.Errorf("BufferMonitorCycle = %v, want %v", cfg.BufferMonitorCycle, 5*time.Second)
	}
}

func TestLoadFromFile_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("{{{{not valid yaml::::"), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	_, err := LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile() should return error for invalid YAML")
	}
}

func TestLoadFromFile_FileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("LoadFromFile() should return error for non-existent file")
	}
}

func TestLoadFromFile_InvalidConfig(t *testing.T) {
	yamlContent := `
port: 0
cq_number: 1
buffer_size: 1024
buffer_send_threshold: 512
queue_depth: 128
connect_timeout: 5s
heartbeat_interval: 5s
connection_idle_heartbeat: 290s
connection_idle_expire: 300s
large_buffer_max_liveness: 5s
buffer_monitor_cycle: 3s
`
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.yaml")
	if err := os.WriteFile(path, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	_, err := LoadFromFile(path)
	if err == nil {
		t.Error("LoadFromFile() should return error for invalid config (port=0)")
	}
}
