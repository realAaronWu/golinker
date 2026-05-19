package histogram

import (
	"sync"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

// Histogram wraps hdrhistogram with thread-safety and convenience methods.
type Histogram struct {
	mu sync.Mutex
	h  *hdr.Histogram
}

// New creates a histogram with range 1μs to 10s at 3 significant figures.
func New() *Histogram {
	return &Histogram{
		h: hdr.New(1, 10_000_000, 3), // 1μs to 10s
	}
}

// NewWithRange creates a histogram with custom range (values in microseconds).
func NewWithRange(minValue, maxValue int64, sigFigs int) *Histogram {
	return &Histogram{
		h: hdr.New(minValue, maxValue, sigFigs),
	}
}

// Record records a latency value in microseconds (thread-safe).
func (h *Histogram) Record(valueUs int64) {
	h.mu.Lock()
	h.h.RecordValue(valueUs)
	h.mu.Unlock()
}

// Percentile returns the value at a given percentile (0-100).
func (h *Histogram) Percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return float64(h.h.ValueAtQuantile(p))
}

// P50 returns the 50th percentile.
func (h *Histogram) P50() float64 { return h.Percentile(50) }

// P75 returns the 75th percentile.
func (h *Histogram) P75() float64 { return h.Percentile(75) }

// P90 returns the 90th percentile.
func (h *Histogram) P90() float64 { return h.Percentile(90) }

// P99 returns the 99th percentile.
func (h *Histogram) P99() float64 { return h.Percentile(99) }

// P999 returns the 99.9th percentile.
func (h *Histogram) P999() float64 { return h.Percentile(99.9) }

// P9999 returns the 99.99th percentile.
func (h *Histogram) P9999() float64 { return h.Percentile(99.99) }

// Max returns the maximum recorded value.
func (h *Histogram) Max() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return float64(h.h.Max())
}

// Min returns the minimum recorded value.
func (h *Histogram) Min() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return float64(h.h.Min())
}

// Mean returns the mean value.
func (h *Histogram) Mean() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.h.Mean()
}

// StdDev returns the standard deviation.
func (h *Histogram) StdDev() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.h.StdDev()
}

// TotalCount returns the number of recorded values.
func (h *Histogram) TotalCount() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.h.TotalCount()
}

// Merge adds values from another histogram into this one.
func (h *Histogram) Merge(other *Histogram) {
	h.mu.Lock()
	defer h.mu.Unlock()
	other.mu.Lock()
	defer other.mu.Unlock()
	h.h.Merge(other.h)
}

// Reset clears all recorded values.
func (h *Histogram) Reset() {
	h.mu.Lock()
	h.h.Reset()
	h.mu.Unlock()
}
