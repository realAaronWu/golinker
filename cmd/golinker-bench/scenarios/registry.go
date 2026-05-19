// Package scenarios implements micro-benchmark scenarios for golinker components.
// These benchmarks run without real RDMA hardware using mock mode.
package scenarios

import (
	"context"
	"time"

	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
)

// ScenarioResult is the result type for micro-benchmark scenarios.
type ScenarioResult struct {
	Scenario      string
	DurationSec   float64
	TotalOps      int64
	OpsPerSec     float64
	LatencyP50Us  float64
	LatencyP99Us  float64
	LatencyMaxUs  float64
	LatencyMeanUs float64
	Notes         string
}

// Runner is the interface that micro-benchmark scenarios implement.
type Runner interface {
	Run(ctx context.Context, duration, warmup time.Duration, goroutines int, hist *histogram.Histogram) (*ScenarioResult, error)
}

var registry = map[string]Runner{}

// Register adds a scenario to the registry.
func Register(name string, runner Runner) {
	registry[name] = runner
}

// Get returns a scenario by name, or nil if not found.
func Get(name string) Runner {
	return registry[name]
}

// List returns all registered scenario names.
func List() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}
