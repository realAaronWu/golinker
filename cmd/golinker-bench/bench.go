package main

import (
	"time"
)

// BenchConfig holds all benchmark configuration.
type BenchConfig struct {
	// Server
	Addr       string
	ServerMode string // "echo" or "sink"

	// Client
	Scenario    string
	MessageSize int
	Connections int
	Rate        int // msgs/sec, 0 = unlimited
	ClosedLoop  bool
	Goroutines  int

	// Timing
	Duration time.Duration
	Warmup   time.Duration

	// Transport
	CQNumber   int
	PollMode   string
	BufferSize int
	BatchSize  int

	// Output
	OutputFormat string // "text", "json", "csv"
	OutputFile  string
	Verbose     bool

	// Profiling
	Pprof      bool
	CPUProfile string
	MemProfile string
}

// BenchResult holds the output of a benchmark run.
type BenchResult struct {
	Metadata   ResultMetadata   `json:"metadata"`
	Latency    LatencyResult    `json:"latency"`
	Throughput ThroughputResult `json:"throughput"`
	Resources  ResourceResult   `json:"resources"`
	Errors     ErrorResult      `json:"errors"`
}

type ResultMetadata struct {
	ToolVersion  string    `json:"tool_version"`
	Timestamp    time.Time `json:"timestamp"`
	Scenario     string    `json:"scenario"`
	DurationSec  float64   `json:"duration_sec"`
	WarmupSec    float64   `json:"warmup_sec"`
	MessageSize  int       `json:"message_size"`
	Connections  int       `json:"connections"`
	CQNumber     int       `json:"cq_number"`
	PollMode     string    `json:"poll_mode"`
	GolinkerVersion string `json:"golinker_version"`
	GoVersion    string    `json:"go_version"`
	Hostname     string    `json:"hostname"`
	OS           string    `json:"os"`
	Arch         string    `json:"arch"`
}

type LatencyResult struct {
	Unit    string  `json:"unit"` // "microseconds"
	P50     float64 `json:"p50"`
	P75     float64 `json:"p75"`
	P90     float64 `json:"p90"`
	P99     float64 `json:"p99"`
	P999    float64 `json:"p999"`
	P9999   float64 `json:"p9999"`
	Max     float64 `json:"max"`
	Min     float64 `json:"min"`
	Mean    float64 `json:"mean"`
	StdDev  float64 `json:"stddev"`
	Samples int64   `json:"samples"`
}

type ThroughputResult struct {
	MessagesPerSec  float64 `json:"messages_per_sec"`
	MegabytesPerSec float64 `json:"megabytes_per_sec"`
}

type ResourceResult struct {
	CPUPercent   float64 `json:"cpu_percent"`
	RSSMB        float64 `json:"rss_mb"`
	RSSPeakMB    float64 `json:"rss_peak_mb"`
	Goroutines   int     `json:"goroutines"`
	GCPauseP99Us float64 `json:"gc_pause_p99_us"`
}

type ErrorResult struct {
	SendErrors       int64 `json:"send_errors"`
	RecvErrors       int64 `json:"recv_errors"`
	TimeoutErrors    int64 `json:"timeout_errors"`
	ConnectionErrors int64 `json:"connection_errors"`
}

// OutputFormat constants
const (
	FormatText = "text"
	FormatJSON = "json"
	FormatCSV  = "csv"
)
