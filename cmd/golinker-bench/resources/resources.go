// Package resources provides system resource tracking for benchmarks.
package resources

import (
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Sample holds a point-in-time resource snapshot.
type Sample struct {
	Timestamp  time.Time
	RSSKB      uint64
	HeapBytes  uint64
	NumGC      uint32
	Goroutines int
}

// Tracker periodically samples resource usage.
type Tracker struct {
	interval time.Duration
	mu       sync.Mutex
	samples  []Sample
	peakRSS  atomic.Uint64
	done     chan struct{}
	running  bool
	startCPU cpuTimes
}

type cpuTimes struct {
	user uint64
	sys  uint64
}

// NewTracker creates a resource tracker that samples at the given interval.
func NewTracker(interval time.Duration) *Tracker {
	if interval <= 0 {
		interval = time.Second
	}
	return &Tracker{
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start begins periodic sampling.
func (t *Tracker) Start() {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return
	}
	t.running = true
	t.done = make(chan struct{})
	t.startCPU = readProcessCPU()
	t.mu.Unlock()

	// Take initial sample.
	t.sample()

	go func() {
		ticker := time.NewTicker(t.interval)
		defer ticker.Stop()
		for {
			select {
			case <-t.done:
				return
			case <-ticker.C:
				t.sample()
			}
		}
	}()
}

// Stop halts periodic sampling.
func (t *Tracker) Stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.running {
		return
	}
	t.running = false
	close(t.done)
}

func (t *Tracker) sample() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	s := Sample{
		Timestamp:  time.Now(),
		RSSKB:      ms.Sys / 1024,
		HeapBytes:  ms.HeapAlloc,
		NumGC:      ms.NumGC,
		Goroutines: runtime.NumGoroutine(),
	}

	t.mu.Lock()
	t.samples = append(t.samples, s)
	t.mu.Unlock()

	// Track peak.
	for {
		old := t.peakRSS.Load()
		if s.RSSKB <= old {
			break
		}
		if t.peakRSS.CompareAndSwap(old, s.RSSKB) {
			break
		}
	}
}

// Current returns the latest resource sample.
func (t *Tracker) Current() Sample {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return Sample{
		Timestamp:  time.Now(),
		RSSKB:      ms.Sys / 1024,
		HeapBytes:  ms.HeapAlloc,
		NumGC:      ms.NumGC,
		Goroutines: runtime.NumGoroutine(),
	}
}

// PeakRSSMB returns the peak RSS observed in megabytes.
func (t *Tracker) PeakRSSMB() float64 {
	return float64(t.peakRSS.Load()) / 1024.0
}

// CPUPercent estimates CPU usage over the given elapsed duration.
func (t *Tracker) CPUPercent(elapsed time.Duration) float64 {
	end := readProcessCPU()
	userDelta := end.user - t.startCPU.user
	sysDelta := end.sys - t.startCPU.sys
	totalTicks := userDelta + sysDelta
	ticksPerSec := uint64(100) // typically 100 on Linux
	cpuSeconds := float64(totalTicks) / float64(ticksPerSec)
	if elapsed.Seconds() <= 0 {
		return 0
	}
	return (cpuSeconds / elapsed.Seconds()) * 100.0
}

// Samples returns a copy of all collected samples.
func (t *Tracker) Samples() []Sample {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Sample, len(t.samples))
	copy(out, t.samples)
	return out
}

// readProcessCPU reads process CPU times from /proc/self/stat.
func readProcessCPU() cpuTimes {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return cpuTimes{}
	}
	fields := strings.Fields(string(data))
	if len(fields) < 15 {
		return cpuTimes{}
	}
	utime, _ := strconv.ParseUint(fields[13], 10, 64)
	stime, _ := strconv.ParseUint(fields[14], 10, 64)
	return cpuTimes{user: utime, sys: stime}
}
