package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// runReport implements the report/comparison logic.
func runReport(cmd *cobra.Command, args []string) error {
	comparePath, _ := cmd.Flags().GetString("compare")
	threshold, _ := cmd.Flags().GetFloat64("threshold")

	if comparePath == "" {
		fmt.Println("Usage: golinker-bench report --compare <baseline.json> <current.json>")
		return nil
	}

	if len(args) < 1 {
		return fmt.Errorf("current results file required as argument")
	}

	baseline, err := loadResult(comparePath)
	if err != nil {
		return fmt.Errorf("loading baseline: %w", err)
	}

	current, err := loadResult(args[0])
	if err != nil {
		return fmt.Errorf("loading current: %w", err)
	}

	return compareResults(os.Stdout, baseline, current, threshold)
}

func loadResult(path string) (*BenchResult, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var result BenchResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func compareResults(w io.Writer, baseline, current *BenchResult, threshold float64) error {
	sep := strings.Repeat("=", 65)
	fmt.Fprintf(w, "\n%s\n", sep)
	fmt.Fprintf(w, "  Comparison Report: baseline vs current\n")
	fmt.Fprintf(w, "%s\n\n", sep)

	fmt.Fprintf(w, "Baseline: %s (%s)\n", baseline.Metadata.Scenario, baseline.Metadata.Timestamp.Format("2006-01-02"))
	fmt.Fprintf(w, "Current:  %s (%s)\n\n", current.Metadata.Scenario, current.Metadata.Timestamp.Format("2006-01-02"))

	type row struct {
		metric    string
		base      float64
		curr      float64
		unit      string
		higherBad bool // if true, increase is regression
	}

	rows := []row{
		{"Latency p50", baseline.Latency.P50, current.Latency.P50, "us", true},
		{"Latency p99", baseline.Latency.P99, current.Latency.P99, "us", true},
		{"Latency p999", baseline.Latency.P999, current.Latency.P999, "us", true},
		{"Throughput", baseline.Throughput.MessagesPerSec, current.Throughput.MessagesPerSec, "msg/s", false},
		{"CPU usage", baseline.Resources.CPUPercent, current.Resources.CPUPercent, "%", true},
		{"RSS", baseline.Resources.RSSMB, current.Resources.RSSMB, "MB", true},
	}

	fmt.Fprintf(w, "%-18s %10s %10s %8s %8s\n", "Metric", "Baseline", "Current", "Delta", "Status")
	fmt.Fprintf(w, "%s\n", strings.Repeat("-", 60))

	hasRegression := false
	for _, r := range rows {
		if r.base == 0 && r.curr == 0 {
			continue
		}
		delta := 0.0
		if r.base != 0 {
			delta = ((r.curr - r.base) / r.base) * 100.0
		}

		status := "PASS"
		if r.higherBad {
			if math.Abs(delta) > threshold {
				status = "FAIL"
				hasRegression = true
			} else if math.Abs(delta) > threshold*0.6 {
				status = "WARN"
			}
		} else {
			// For throughput, decrease is bad
			if delta < -threshold {
				status = "FAIL"
				hasRegression = true
			} else if delta < -threshold*0.6 {
				status = "WARN"
			}
		}

		fmt.Fprintf(w, "%-18s %8.1f%s %8.1f%s %+6.1f%% %8s\n",
			r.metric, r.base, r.unit, r.curr, r.unit, delta, status)
	}

	fmt.Fprintf(w, "\nLegend: PASS (< %.0f%%) | WARN (%.0f-%.0f%%) | FAIL (> %.0f%%)\n",
		threshold*0.6, threshold*0.6, threshold, threshold)

	if hasRegression {
		fmt.Fprintf(w, "\nResult: REGRESSION DETECTED\n")
		return fmt.Errorf("regression detected (threshold: %.0f%%)", threshold)
	}
	fmt.Fprintf(w, "\nResult: PASS\n")
	return nil
}
