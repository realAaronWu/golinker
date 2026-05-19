package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "dev"

	// Global flags
	addr       string
	duration   string
	warmup     string
	outputFmt  string
	outputFile string
	verbose    bool
	pprofFlag  bool
	cpuProfile string
	memProfile string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "golinker-bench",
		Short: "golinker RDMA RPC benchmark tool",
		Long:  "Performance and load testing tool for golinker transport layer",
	}

	// Persistent (global) flags
	rootCmd.PersistentFlags().StringVar(&addr, "addr", "0.0.0.0:8629", "server address")
	rootCmd.PersistentFlags().StringVar(&duration, "duration", "30s", "test duration")
	rootCmd.PersistentFlags().StringVar(&warmup, "warmup", "5s", "warmup period")
	rootCmd.PersistentFlags().StringVar(&outputFmt, "output", "text", "output format: text, json, csv")
	rootCmd.PersistentFlags().StringVar(&outputFile, "output-file", "", "write results to file (default: stdout)")
	rootCmd.PersistentFlags().BoolVar(&verbose, "verbose", false, "enable verbose logging")
	rootCmd.PersistentFlags().BoolVar(&pprofFlag, "pprof", false, "enable pprof on :6060")
	rootCmd.PersistentFlags().StringVar(&cpuProfile, "cpu-profile", "", "write CPU profile to file")
	rootCmd.PersistentFlags().StringVar(&memProfile, "mem-profile", "", "write memory profile to file")

	// Add subcommands
	rootCmd.AddCommand(newServerCmd())
	rootCmd.AddCommand(newClientCmd())
	rootCmd.AddCommand(newReportCmd())

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newServerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "server",
		Short: "Start benchmark server (echo or sink mode)",
		RunE:  executeServer,
	}
	cmd.Flags().String("mode", "echo", "server mode: echo or sink")
	return cmd
}

func newClientCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client [scenario]",
		Short: "Run benchmark client with specified scenario",
		Args:  cobra.MinimumNArgs(1),
		RunE:  executeClient,
	}
	cmd.Flags().Int("message-size", 64, "message size in bytes")
	cmd.Flags().Int("connections", 1, "number of connections")
	cmd.Flags().Int("rate", 0, "target send rate (0=unlimited)")
	cmd.Flags().Bool("closed-loop", false, "use closed-loop sending")
	cmd.Flags().Int("goroutines", 0, "sender goroutines (0=GOMAXPROCS)")
	return cmd
}

func newReportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "report",
		Short: "Generate or compare benchmark reports",
		RunE:  runReport,
	}
	cmd.Flags().String("compare", "", "baseline results JSON for comparison")
	cmd.Flags().Float64("threshold", 5.0, "regression threshold %")
	return cmd
}
