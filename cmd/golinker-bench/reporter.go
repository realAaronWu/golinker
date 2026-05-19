package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/wua20/golinker/cmd/golinker-bench/histogram"
	"github.com/wua20/golinker/cmd/golinker-bench/resources"
)

// Reporter generates benchmark output in various formats.
type Reporter struct {
	config  *BenchConfig
	hist    *histogram.Histogram
	tracker *resources.Tracker
}

// NewReporter creates a reporter.
func NewReporter(cfg *BenchConfig, hist *histogram.Histogram, tracker *resources.Tracker) *Reporter {
	return &Reporter{config: cfg, hist: hist, tracker: tracker}
}

// Report outputs the benchmark result in the configured format.
func (r *Reporter) Report(result *BenchResult) error {
	// Fill in latency from histogram
	if r.hist.TotalCount() > 0 {
		result.Latency = LatencyResult{
			Unit:    "microseconds",
			P50:     r.hist.P50(),
			P75:     r.hist.P75(),
			P90:     r.hist.P90(),
			P99:     r.hist.P99(),
			P999:    r.hist.P999(),
			P9999:   r.hist.P9999(),
			Max:     r.hist.Max(),
			Min:     r.hist.Min(),
			Mean:    r.hist.Mean(),
			StdDev:  r.hist.StdDev(),
			Samples: r.hist.TotalCount(),
		}
	}

	// Fill in resources
	if r.tracker != nil {
		elapsed := time.Duration(result.Metadata.DurationSec * float64(time.Second))
		result.Resources = ResourceResult{
			CPUPercent: r.tracker.CPUPercent(elapsed),
			RSSMB:      float64(r.tracker.Current().RSSKB) / 1024.0,
			RSSPeakMB:  r.tracker.PeakRSSMB(),
			Goroutines: runtime.NumGoroutine(),
		}
	}

	// Get output writer
	var w io.Writer = os.Stdout
	if r.config.OutputFile != "" {
		f, err := os.Create(r.config.OutputFile)
		if err != nil {
			return fmt.Errorf("creating output file: %w", err)
		}
		defer f.Close()
		w = f
	}

	switch r.config.OutputFormat {
	case FormatJSON:
		return r.reportJSON(w, result)
	case FormatCSV:
		return r.reportCSV(w, result)
	default:
		return r.reportText(w, result)
	}
}

func (r *Reporter) reportText(w io.Writer, result *BenchResult) error {
	sep := strings.Repeat("=", 65)
	fmt.Fprintf(w, "\n%s\n", sep)
	fmt.Fprintf(w, "  golinker-bench: %s | %dB messages | %s\n",
		result.Metadata.Scenario, result.Metadata.MessageSize, result.Metadata.PollMode)
	fmt.Fprintf(w, "%s\n\n", sep)

	fmt.Fprintf(w, "Duration:     %.2fs (warmup: %.2fs)\n",
		result.Metadata.DurationSec, result.Metadata.WarmupSec)
	fmt.Fprintf(w, "Messages:     %d samples\n\n", result.Latency.Samples)

	if result.Latency.Samples > 0 {
		fmt.Fprintf(w, "Latency Distribution:\n")
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintf(tw, "  p50:\t%.1fus\n", result.Latency.P50)
		fmt.Fprintf(tw, "  p75:\t%.1fus\n", result.Latency.P75)
		fmt.Fprintf(tw, "  p90:\t%.1fus\n", result.Latency.P90)
		fmt.Fprintf(tw, "  p99:\t%.1fus\n", result.Latency.P99)
		fmt.Fprintf(tw, "  p99.9:\t%.1fus\n", result.Latency.P999)
		fmt.Fprintf(tw, "  p99.99:\t%.1fus\n", result.Latency.P9999)
		fmt.Fprintf(tw, "  max:\t%.1fus\n", result.Latency.Max)
		fmt.Fprintf(tw, "  mean:\t%.1fus\n", result.Latency.Mean)
		fmt.Fprintf(tw, "  stddev:\t%.1fus\n", result.Latency.StdDev)
		tw.Flush()
		fmt.Fprintln(w)
	}

	if result.Throughput.MessagesPerSec > 0 {
		fmt.Fprintf(w, "Throughput:\n")
		fmt.Fprintf(w, "  Messages: %.0f msg/sec\n", result.Throughput.MessagesPerSec)
		fmt.Fprintf(w, "  Data:     %.1f MB/sec\n", result.Throughput.MegabytesPerSec)
		fmt.Fprintln(w)
	}

	fmt.Fprintf(w, "Resource Usage:\n")
	fmt.Fprintf(w, "  CPU:        %.0f%%\n", result.Resources.CPUPercent)
	fmt.Fprintf(w, "  RSS:        %.0f MB (peak: %.0f MB)\n", result.Resources.RSSMB, result.Resources.RSSPeakMB)
	fmt.Fprintf(w, "  Goroutines: %d\n", result.Resources.Goroutines)

	fmt.Fprintf(w, "\n%s\n", sep)
	return nil
}

func (r *Reporter) reportJSON(w io.Writer, result *BenchResult) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func (r *Reporter) reportCSV(w io.Writer, result *BenchResult) error {
	fmt.Fprintf(w, "metric,value\n")
	fmt.Fprintf(w, "scenario,%s\n", result.Metadata.Scenario)
	fmt.Fprintf(w, "duration_sec,%.2f\n", result.Metadata.DurationSec)
	fmt.Fprintf(w, "message_size,%d\n", result.Metadata.MessageSize)
	fmt.Fprintf(w, "samples,%d\n", result.Latency.Samples)
	fmt.Fprintf(w, "p50_us,%.1f\n", result.Latency.P50)
	fmt.Fprintf(w, "p99_us,%.1f\n", result.Latency.P99)
	fmt.Fprintf(w, "p999_us,%.1f\n", result.Latency.P999)
	fmt.Fprintf(w, "max_us,%.1f\n", result.Latency.Max)
	fmt.Fprintf(w, "mean_us,%.1f\n", result.Latency.Mean)
	fmt.Fprintf(w, "throughput_msg_sec,%.0f\n", result.Throughput.MessagesPerSec)
	fmt.Fprintf(w, "throughput_mb_sec,%.1f\n", result.Throughput.MegabytesPerSec)
	fmt.Fprintf(w, "cpu_pct,%.0f\n", result.Resources.CPUPercent)
	fmt.Fprintf(w, "rss_mb,%.0f\n", result.Resources.RSSMB)
	return nil
}
