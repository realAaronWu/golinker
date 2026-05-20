# golinker Benchmark Tool Design

## 1. Overview

### Purpose

`golinker-bench` is a dedicated performance and load testing tool for golinker, the Go RDMA RPC transport library. It provides reproducible, statistically rigorous benchmarks that validate performance characteristics across subsystems and end-to-end paths.

### Goals

- **Validate performance parity**: Confirm golinker achieves within 1–3% overhead of raw RDMA verbs
- **Detect regressions**: Automated comparison against baseline results; flag any degradation > 5%
- **Isolate subsystem performance**: Micro-benchmarks for CGo overhead, buffer pools, CQ polling, aggregation, and memory registration independently
- **Generate actionable reports**: HDR histograms, throughput curves, resource usage, and diff tables in human-readable and machine-parseable formats
- **Support CI pipelines**: Run on SoftRoCE (rxe) for correctness, real RDMA hardware for perf gates

### Non-Goals

- Not a general-purpose load testing framework (use for golinker only)
- Not a correctness test suite (that belongs in `_test.go` files)
- Not a production monitoring tool (no long-lived daemon mode)
- Does not test application-layer RPC semantics (only transport layer)

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      golinker-bench                             │
├─────────────┬─────────────┬──────────────┬─────────────────┤
│   Server    │   Client    │  Controller  │    Reporter     │
│   Mode      │   Mode      │  Mode        │                 │
├─────────────┼─────────────┼──────────────┼─────────────────┤
│ Echo/Sink   │ Load Gen    │ Multi-node   │ HDR Histogram   │
│ Responder   │ Rate Control│ Orchestrator │ JSON/CSV/Text   │
│             │ Coordinated │              │ Comparison      │
│             │ Omission    │              │ pprof export    │
└─────────────┴─────────────┴──────────────┴─────────────────┘
         │            │              │
         └────────────┴──────────────┘
                      │
              ┌───────┴───────┐
              │  golinker core   │
              │  (transport)  │
              └───────────────┘
```

### Server Mode

The server listens for incoming RDMA connections and operates in one of two response modes:

- **Echo**: Receives a message, immediately sends it back (for latency measurement)
- **Sink**: Receives messages, discards payload, sends minimal ACK (for throughput measurement)

The server is intentionally minimal—no application logic—to measure pure transport overhead.

```go
// cmd/golinker-bench/server.go
type BenchServer struct {
    listener   *golinker.Listener
    mode       ResponseMode  // echo | sink
    stats      *AtomicStats  // concurrent-safe counters
    cqNumber   int
    pollMode   golinker.PollMode
}
```

### Client Mode

The client generates load with precise rate control and collects latency samples:

- **Open-loop**: Sends at a fixed rate regardless of response arrival (proper coordinated omission handling)
- **Closed-loop**: Sends next request only after previous response (useful for max-throughput discovery)
- **Ramp**: Linearly increases rate to find saturation point

```go
// cmd/golinker-bench/client.go
type BenchClient struct {
    connections []*golinker.Connection
    histogram   *hdrhistogram.Histogram
    rateLimiter *RateLimiter
    scenario    Scenario
    config      *BenchConfig
}
```

### Controller Mode

Orchestrates multi-node benchmarks for scaling tests:

- Starts servers on remote nodes via SSH
- Launches clients with coordinated start times
- Collects and merges results from all nodes
- Generates aggregate report

```go
// cmd/golinker-bench/controller.go
type Controller struct {
    nodes      []NodeConfig
    scenario   Scenario
    results    chan *NodeResult
}

type NodeConfig struct {
    Addr     string
    Role     string // "server" | "client"
    SSHKey   string
}
```

### Reporter

Collects raw measurements and produces output in multiple formats:

- Merges HDR histograms from multiple clients
- Computes summary statistics (p50, p99, p999, max, mean, stddev)
- Calculates throughput (msgs/sec, MB/sec)
- Reads baseline JSON for comparison mode
- Outputs text tables, JSON, or CSV

```go
// cmd/golinker-bench/reporter.go
type Reporter struct {
    histogram  *hdrhistogram.Histogram
    throughput *ThroughputTracker
    resources  *ResourceTracker  // CPU, RSS, goroutines
    baseline   *BenchResult      // for comparison
    format     OutputFormat
}
```

---

## 3. Benchmark Scenarios

### Micro-benchmarks

These isolate individual subsystem performance. They run in-process (no network) where possible.

#### `bench_cgo_overhead`

Measures the cost of crossing the Go→C boundary.

| Sub-test | Description |
|----------|-------------|
| `empty_call` | C function that does nothing; measures pure CGo transition cost |
| `single_verb` | Post one send/recv WR via C helper |
| `batch_verb_4` | Post batch of 4 WRs in one CGo call |
| `batch_verb_16` | Post batch of 16 WRs in one CGo call |
| `batch_verb_64` | Post batch of 64 WRs in one CGo call |

**Expected output**: ns/op for each, demonstrating amortization benefit of batching.

#### `bench_buffer_pool`

Measures buffer allocation/free throughput under contention.

| Sub-test | Description |
|----------|-------------|
| `goroutines_1` | Single goroutine alloc/free loop |
| `goroutines_2` | 2 goroutines competing |
| `goroutines_4` | 4 goroutines competing |
| `goroutines_8` | 8 goroutines competing |
| `goroutines_16` | 16 goroutines competing |

**Metrics**: ops/sec, avg latency, contention ratio (time waiting / time working).

#### `bench_cq_poll`

CQ poll throughput at various batch sizes with pre-posted completions.

| Sub-test | Description |
|----------|-------------|
| `batch_1` | Poll 1 CQE at a time |
| `batch_4` | Poll up to 4 CQEs |
| `batch_16` | Poll up to 16 CQEs |
| `batch_32` | Poll up to 32 CQEs |
| `batch_64` | Poll up to 64 CQEs |

**Metrics**: CQEs/sec, ns per CQE, batch efficiency (actual batch size / requested).

#### `bench_aggregation`

Measures overhead of message aggregation (serialize + copy into shared buffer).

| Sub-test | Description |
|----------|-------------|
| `single_64B` | One 64B message, no aggregation |
| `agg_2x64B` | 2 messages aggregated into one send |
| `agg_4x64B` | 4 messages aggregated |
| `agg_8x64B` | 8 messages aggregated |
| `agg_mixed` | Mixed sizes (64B + 256B + 1KB) aggregated |

**Metrics**: ns/message, ns/byte, effective throughput vs non-aggregated baseline.

#### `bench_mr_reg`

Memory registration/deregistation latency (critical for large-message path).

| Sub-test | Description |
|----------|-------------|
| `reg_4KB` | Register 4KB region |
| `reg_64KB` | Register 64KB region |
| `reg_1MB` | Register 1MB region |
| `reg_16MB` | Register 16MB region |
| `dereg` | Deregister previously registered region |

**Metrics**: μs per operation, variance across runs.

#### `bench_channel_vs_mutex`

Compares channel-based pool vs mutex-protected pool under contention.

| Sub-test | Description |
|----------|-------------|
| `channel_low` | Channel pool, 2 goroutines, low contention |
| `channel_high` | Channel pool, 16 goroutines, high contention |
| `mutex_low` | Mutex pool, 2 goroutines, low contention |
| `mutex_high` | Mutex pool, 16 goroutines, high contention |

**Metrics**: ops/sec, p99 latency, CPU utilization.

---

### End-to-End Benchmarks

These require a running server and measure complete round-trip or one-way performance.

#### `bench_latency`

Round-trip latency at various message sizes. Uses echo mode.

| Message Size | Target p50 | Target p99 |
|-------------|------------|------------|
| 64B | < 5μs | < 20μs |
| 256B | < 6μs | < 25μs |
| 1KB | < 8μs | < 30μs |
| 4KB | < 12μs | < 40μs |
| 12KB | < 15μs | < 50μs |

**Configuration**: Closed-loop, single connection, 1M iterations after warmup.

#### `bench_throughput`

Maximum messages per second at fixed message size. Uses sink mode.

| Message Size | Target |
|-------------|--------|
| 64B | > 5M msgs/sec per CQ |
| 256B | > 4M msgs/sec per CQ |
| 1KB | > 2M msgs/sec per CQ |

**Configuration**: Open-loop, unlimited rate, measure saturation point. Multiple connections (scaled to saturate).

#### `bench_bandwidth`

Maximum data throughput at large message sizes. Exercises RDMA read path.

| Message Size | Expected |
|-------------|----------|
| 64KB | Near line-rate |
| 256KB | Near line-rate |
| 1MB | Near line-rate |

**Configuration**: Open-loop, multiple connections, measure GB/sec.

#### `bench_mixed`

Mixed message sizes drawn from a Zipf distribution:
- 80% of messages: 64B–256B (small, inline path)
- 15% of messages: 1KB–12KB (buffer path)
- 5% of messages: 64KB–1MB (large message / RDMA read path)

**Metrics**: Per-size-bucket latency histograms, aggregate throughput.

#### `bench_connections`

Throughput scaling with connection count:

| Connections | Measurement |
|-------------|-------------|
| 1 | Baseline throughput |
| 10 | Per-connection and aggregate |
| 100 | Per-connection and aggregate |
| 1000 | Per-connection and aggregate |

**Metrics**: Aggregate msgs/sec, per-connection msgs/sec, fairness index (Jain's fairness).

#### `bench_cq_scaling`

Throughput scaling with CQ count:

| CQ Count | Measurement |
|----------|-------------|
| 1 | Baseline |
| 2 | Expected ~2x |
| 4 | Expected ~4x |
| 8 | Expected ~6-8x (diminishing) |

**Metrics**: Aggregate msgs/sec, per-CQ msgs/sec, CPU cores utilized.

---

### Stress Tests

Long-running tests that validate stability and detect resource leaks.

#### `stress_sustained`

- **Duration**: 1 hour
- **Load**: Fixed rate at 80% of measured saturation
- **Metrics**: Latency histogram per minute (detect drift), tail stability (p999 over time)
- **Pass criteria**: p99 must not increase by more than 2x from minute 5 to minute 60

#### `stress_burst`

- **Pattern**: 10 seconds at 0, 10 seconds at max rate, repeat for 30 minutes
- **Metrics**: Time-to-recover (latency returns to baseline after burst), dropped messages
- **Pass criteria**: Recovery within 100ms of burst end

#### `stress_memory`

- **Duration**: 1 hour at moderate load
- **Metrics**: RSS sampled every second, heap profile at start/end
- **Pass criteria**: RSS growth < 1MB/hour after initial stabilization (no leaks)
- **Method**: `runtime.ReadMemStats()` + `/proc/self/status` for RSS

#### `stress_reconnect`

- **Pattern**: Connect, send 100 messages, disconnect. Repeat 10,000 times.
- **Metrics**: Connection setup time, resource cleanup (goroutines, file descriptors)
- **Pass criteria**: No goroutine leak (count at end == count at start ± 2), no FD leak

---

## 4. Measurement Methodology

### HDR Histogram

All latency measurements use High Dynamic Range (HDR) Histograms:

- **Range**: 1μs to 10s (covers both fast-path and timeout scenarios)
- **Precision**: 3 significant digits
- **Library**: `github.com/HdrHistogram/hdrhistogram-go`
- **Merge**: Histograms from multiple goroutines/clients are merged for aggregate view

```go
histogram := hdrhistogram.New(1, 10_000_000, 3) // 1μs to 10s, 3 sig figs
```

### Warmup Period

- Default warmup: 5 seconds (configurable via `--warmup`)
- All samples during warmup are discarded
- Warmup ensures: JIT inlining, buffer pools pre-allocated, CQ polling threads hot, OS page tables populated

### Coordinated Omission Correction

The benchmark client uses **open-loop** sending by default:

```go
// Open-loop: send at fixed intervals regardless of response
ticker := time.NewTicker(interval)
for range ticker.C {
    sendTime := time.Now()
    send(msg)
    // Response tracked asynchronously
}

// When response arrives:
latency := responseTime - originalSendTime  // NOT time-since-last-response
```

For closed-loop tests (where coordinated omission is intentional), the flag `--closed-loop` opts in explicitly.

### CPU and Memory Profiling

- **pprof integration**: `--pprof` flag starts a pprof HTTP server on `:6060`
- **CPU profile**: Captured for the duration of the test, written to `cpu.prof`
- **Memory profile**: Heap snapshot at end of test, written to `mem.prof`
- **Block profile**: Optional, measures goroutine contention
- **Trace**: `--trace` flag captures execution trace for `go tool trace`

```go
if config.Pprof {
    go func() {
        http.ListenAndServe(":6060", nil)
    }()
}

if config.CPUProfile != "" {
    f, _ := os.Create(config.CPUProfile)
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
}
```

### RDMA Hardware Counters

When available, read hardware performance counters via sysfs:

- `/sys/class/infiniband/<dev>/ports/<port>/counters/`
  - `port_xmit_data` — bytes transmitted
  - `port_rcv_data` — bytes received
  - `port_xmit_packets` — packets transmitted
  - `port_rcv_packets` — packets received
- Sampled at start and end of benchmark; delta reported

### Comparison Mode (baseline vs current)

To validate performance parity:

1. Run the same scenario against a baseline build → save as baseline JSON
2. Run same scenario against golinker (Go server) → save as current JSON
3. `golinker-bench report --compare baseline.json current.json` generates diff

```
┌─────────────────────────────────────────────────────────────────┐
│ Comparison: baseline vs current                                    │
├──────────────┬──────────┬──────────┬─────────┬─────────────────┤
│ Metric       │ Baseline │ Current  │ Delta   │ Status          │
├──────────────┼──────────┼──────────┼─────────┼─────────────────┤
│ p50 latency  │ 4.2μs    │ 4.3μs    │ +2.4%   │ ✓ PASS (< 3%)  │
│ p99 latency  │ 15.1μs   │ 15.8μs   │ +4.6%   │ ⚠ WARN (< 5%)  │
│ throughput   │ 5.2M/s   │ 5.1M/s   │ -1.9%   │ ✓ PASS (< 3%)  │
│ CPU usage    │ 340%     │ 360%     │ +5.9%   │ ✗ FAIL (> 5%)   │
└──────────────┴──────────┴──────────┴─────────┴─────────────────┘
```

---

## 5. CLI Interface

### Command Structure

```
golinker-bench [mode] [scenario] [flags]
```

### Modes

| Mode | Description |
|------|-------------|
| `server` | Start benchmark server (echo or sink) |
| `client` | Run benchmark client with specified scenario |
| `report` | Generate or compare reports from saved results |

### Scenarios

| Category | Scenarios |
|----------|-----------|
| End-to-end | `latency`, `throughput`, `bandwidth`, `mixed`, `connections`, `cq-scaling` |
| Micro | `cgo-overhead`, `buffer-pool`, `cq-poll`, `aggregation`, `mr-reg`, `channel-vs-mutex` |
| Stress | `sustained`, `burst`, `memory`, `reconnect` |

### Flags

```
Global Flags:
  --addr string          Server address (default "0.0.0.0:8629")
  --duration duration    Test duration (default 30s)
  --warmup duration      Warmup period (default 5s)
  --output string        Output format: text, json, csv (default "text")
  --output-file string   Write results to file (default: stdout)
  --verbose              Enable verbose logging
  --pprof                Enable pprof server on :6060
  --cpu-profile string   Write CPU profile to file
  --mem-profile string   Write memory profile to file
  --trace string         Write execution trace to file

Client Flags:
  --message-size int     Message size in bytes (default 64)
  --connections int      Number of connections (default 1)
  --rate int             Target send rate, 0=unlimited (default 0)
  --closed-loop          Use closed-loop (synchronous) sending
  --goroutines int       Number of sender goroutines (default: GOMAXPROCS)

Transport Flags:
  --cq-number int        Number of completion queues (default 2)
  --poll-mode string     CQ poll mode: busy, event, smart, user (default "busy")
  --buffer-size int      Send/recv buffer size in bytes (default 12288)
  --batch-size int       Max messages per aggregation batch (default 16)

Report Flags:
  --compare string       Path to baseline results JSON for comparison
  --threshold float      Regression threshold percentage (default 5.0)

Server Flags:
  --mode string          Response mode: echo, sink (default "echo")
```

### Usage Examples

```bash
# Start a benchmark server in echo mode
golinker-bench server --addr 0.0.0.0:8629 --poll-mode busy --cq-number 4

# Run latency benchmark with 64B messages
golinker-bench client latency --addr 10.0.0.1:8629 --message-size 64 --duration 60s

# Run throughput test, save results
golinker-bench client throughput --addr 10.0.0.1:8629 --message-size 64 \
  --connections 8 --output json --output-file results/throughput_64B.json

# Run micro-benchmark (no server needed)
golinker-bench client cgo-overhead --duration 10s --output text

# Compare against baseline
golinker-bench report --compare baseline/latency_64B.json results/latency_64B.json

# Run stress test with profiling enabled
golinker-bench client sustained --addr 10.0.0.1:8629 --duration 1h \
  --rate 4000000 --pprof --cpu-profile stress_cpu.prof

# Run connection scaling test
golinker-bench client connections --addr 10.0.0.1:8629 \
  --connections 1,10,100,1000 --message-size 64 --duration 30s

# Run CQ scaling test
golinker-bench client cq-scaling --addr 10.0.0.1:8629 \
  --cq-number 1,2,4,8 --message-size 64 --connections 32
```

---

## 6. Output Format

### Text Output (default)

```
═══════════════════════════════════════════════════════════════
  golinker-bench: latency | 64B messages | busy poll | 1 conn
═══════════════════════════════════════════════════════════════

Duration:     30.00s (warmup: 5.00s)
Messages:     142,857,143 sent / 142,857,143 received (0 errors)

Latency Distribution:
  p50:      4.1μs
  p75:      5.2μs
  p90:      7.8μs
  p99:      14.3μs
  p99.9:    28.7μs
  p99.99:   95.2μs
  max:      412.0μs
  mean:     4.8μs
  stddev:   3.1μs

Throughput:
  Messages: 4,761,905 msg/sec
  Data:     290.4 MB/sec

Resource Usage:
  CPU:      198% (2.0 cores)
  RSS:      84 MB (peak: 91 MB)
  Goroutines: 12

RDMA Counters:
  TX packets: 142,857,143
  RX packets: 142,857,143
  TX bytes:   9.14 GB
  RX bytes:   9.14 GB

═══════════════════════════════════════════════════════════════
```

### JSON Output

```json
{
  "metadata": {
    "tool_version": "0.1.0",
    "timestamp": "2025-01-15T10:30:00Z",
    "scenario": "latency",
    "duration_sec": 30,
    "warmup_sec": 5,
    "message_size": 64,
    "connections": 1,
    "cq_number": 2,
    "poll_mode": "busy",
    "golinker_version": "0.3.0",
    "go_version": "go1.23.0",
    "rdma_device": "mlx5_0",
    "os": "linux",
    "arch": "amd64",
    "hostname": "bench-node-01"
  },
  "latency": {
    "unit": "microseconds",
    "p50": 4.1,
    "p75": 5.2,
    "p90": 7.8,
    "p99": 14.3,
    "p999": 28.7,
    "p9999": 95.2,
    "max": 412.0,
    "min": 2.8,
    "mean": 4.8,
    "stddev": 3.1,
    "samples": 142857143,
    "histogram_base64": "<encoded HDR histogram for later merge>"
  },
  "throughput": {
    "messages_per_sec": 4761905,
    "megabytes_per_sec": 290.4
  },
  "resources": {
    "cpu_percent": 198.0,
    "rss_mb": 84,
    "rss_peak_mb": 91,
    "goroutines": 12,
    "gc_pause_p99_us": 120
  },
  "rdma_counters": {
    "tx_packets": 142857143,
    "rx_packets": 142857143,
    "tx_bytes": 9814286016,
    "rx_bytes": 9814286016
  },
  "errors": {
    "send_errors": 0,
    "recv_errors": 0,
    "timeout_errors": 0,
    "connection_errors": 0
  }
}
```

### CSV Output

For time-series data (throughput over time, latency percentiles per interval):

```csv
timestamp_ms,interval_sec,messages,msg_per_sec,p50_us,p99_us,p999_us,cpu_pct,rss_mb
5000,1,4750000,4750000,4.1,14.2,28.5,197,84
6000,1,4780000,4780000,4.0,14.1,27.9,198,84
7000,1,4762000,4762000,4.1,14.5,29.1,197,84
...
```

### Comparison Report

When `--compare` is provided:

```
═══════════════════════════════════════════════════════════════
  Comparison Report: baseline vs current
═══════════════════════════════════════════════════════════════

Baseline: results/baseline_latency_64B.json (2025-01-10)
Current:  results/golinker_latency_64B.json (2025-01-15)

┌──────────────────┬──────────┬──────────┬─────────┬────────┐
│ Metric           │ Baseline │ Current  │ Delta % │ Status │
├──────────────────┼──────────┼──────────┼─────────┼────────┤
│ Latency p50      │   4.0μs  │   4.1μs  │  +2.5%  │ PASS   │
│ Latency p99      │  13.8μs  │  14.3μs  │  +3.6%  │ PASS   │
│ Latency p999     │  27.0μs  │  28.7μs  │  +6.3%  │ FAIL   │
│ Throughput       │  4.9M/s  │  4.76M/s │  -2.9%  │ PASS   │
│ CPU usage        │   190%   │   198%   │  +4.2%  │ WARN   │
│ RSS              │   78MB   │   84MB   │  +7.7%  │ FAIL   │
└──────────────────┴──────────┴──────────┴─────────┴────────┘

Legend: PASS (< 3%) | WARN (3-5%) | FAIL (> 5%)

Exit code: 1 (regression detected)
```

---

## 7. CI Integration

### Pipeline Stages

```yaml
# .github/workflows/benchmark.yml
name: Performance Benchmarks

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  bench-softroce:
    name: "Benchmarks (SoftRoCE)"
    runs-on: self-hosted-rdma
    steps:
      - uses: actions/checkout@v4

      - name: Setup SoftRoCE
        run: |
          sudo modprobe rdma_rxe
          sudo rdma link add rxe0 type rxe netdev eth0

      - name: Build benchmark tool
        run: go build -o golinker-bench ./cmd/golinker-bench/

      - name: Run micro-benchmarks
        run: |
          ./golinker-bench client cgo-overhead --duration 10s --output json \
            --output-file results/cgo_overhead.json
          ./golinker-bench client buffer-pool --duration 10s --output json \
            --output-file results/buffer_pool.json

      - name: Run latency benchmark
        run: |
          ./golinker-bench server --addr 127.0.0.1:8629 &
          sleep 2
          ./golinker-bench client latency --addr 127.0.0.1:8629 \
            --message-size 64 --duration 30s --output json \
            --output-file results/latency_64B.json
          kill %1

      - name: Compare with baseline
        run: |
          ./golinker-bench report \
            --compare baselines/latency_64B.json results/latency_64B.json \
            --threshold 10.0  # SoftRoCE has higher variance, use 10%

      - name: Upload results
        uses: actions/upload-artifact@v4
        with:
          name: bench-results-${{ github.sha }}
          path: results/

  bench-hardware:
    name: "Benchmarks (ConnectX-6)"
    runs-on: [self-hosted, rdma-hardware]
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4

      - name: Build
        run: go build -o golinker-bench ./cmd/golinker-bench/

      - name: Full benchmark suite
        run: |
          ./golinker-bench server --addr 0.0.0.0:8629 --cq-number 4 &
          SERVER_PID=$!
          sleep 2

          for size in 64 256 1024 4096 12288; do
            ./golinker-bench client latency --addr 10.0.0.2:8629 \
              --message-size $size --duration 60s --output json \
              --output-file results/latency_${size}B.json
          done

          ./golinker-bench client throughput --addr 10.0.0.2:8629 \
            --message-size 64 --connections 8 --duration 60s --output json \
            --output-file results/throughput_64B.json

          kill $SERVER_PID

      - name: Performance gate
        run: |
          EXIT=0
          for f in results/*.json; do
            baseline="baselines/$(basename $f)"
            if [ -f "$baseline" ]; then
              ./golinker-bench report --compare "$baseline" "$f" --threshold 5.0 || EXIT=1
            fi
          done
          exit $EXIT

      - name: Update baselines (main only)
        if: github.ref == 'refs/heads/main' && success()
        run: |
          cp results/*.json baselines/
          git add baselines/
          git commit -m "Update benchmark baselines [skip ci]" || true
          git push
```

### Performance Gates

| Metric | SoftRoCE Threshold | Hardware Threshold |
|--------|-------------------|-------------------|
| Latency p50 | 10% | 5% |
| Latency p99 | 15% | 5% |
| Throughput | 10% | 5% |
| CPU usage | 20% | 10% |
| RSS | 20% | 10% |

SoftRoCE thresholds are relaxed because:
- Software emulation adds variable overhead
- Useful for correctness and major regressions only
- Real performance validation happens on hardware

### Historical Tracking

Results are stored as JSON artifacts and optionally pushed to a time-series store:

```go
// cmd/golinker-bench/ci.go
type HistoricalResult struct {
    CommitSHA   string      `json:"commit_sha"`
    Branch      string      `json:"branch"`
    Timestamp   time.Time   `json:"timestamp"`
    Scenario    string      `json:"scenario"`
    Result      BenchResult `json:"result"`
}
```

A companion script (`scripts/bench_history.py`) can plot performance trends:

```bash
# Generate trend plot for latency over last 100 commits
python scripts/bench_history.py --metric latency_p99 --last 100 --output trend.png
```

---

## 8. End-to-End RDMA Transport Integration

### Overview

This section describes the transport integration that wires the benchmark tool's
server and client modes to the golinker core library, enabling real two-node RDMA
benchmarks. The existing micro-benchmarks (buffer-pool, cq-poll, aggregation,
channel-vs-mutex) remain in-process; the end-to-end scenarios (`latency`,
`throughput`, `bandwidth`, `mixed`) now use real RDMA connections via the
`pkg/server`, `pkg/connection`, and `pkg/message` packages.

### Architecture: Two-Node RDMA Benchmark

```
Node A (Server)                         Node B (Client)
┌───────────────────────┐               ┌───────────────────────┐
│  golinker-bench server│               │  golinker-bench client│
│                       │               │                       │
│  ┌─────────────────┐  │   RDMA RC     │  ┌─────────────────┐  │
│  │ pkg/server      │◄─┼───────────────┼──│ pkg/connection   │  │
│  │  .Start()       │  │   QP(s)       │  │  .Connect()      │  │
│  │  .RegisterHandler│ │               │  │  .Send() / .Recv()│ │
│  └────────┬────────┘  │               │  └────────┬────────┘  │
│           │           │               │           │           │
│  ┌────────▼────────┐  │               │  ┌────────▼────────┐  │
│  │ Echo / Sink     │  │               │  │ Load Generator   │  │
│  │ MessageHandler  │  │               │  │ + HDR Histogram  │  │
│  └─────────────────┘  │               │  └─────────────────┘  │
│                       │               │                       │
│  pkg/buffer (pools)   │               │  pkg/buffer (pools)   │
│  pkg/cq    (pollers)  │               │  pkg/cq    (pollers)  │
│  pkg/config           │               │  pkg/config           │
└───────────────────────┘               └───────────────────────┘
```

### Server Transport Integration

The benchmark server creates a full golinker server stack:

```go
// cmd/golinker-bench/server.go (transport integration)
func (s *BenchServer) startRDMA(ctx context.Context) error {
    cfg := config.DefaultConfig()
    cfg.Endpoint = s.addr
    cfg.CQNumber = s.cqNumber
    cfg.PollMode = parsePollMode(s.pollMode)
    cfg.BufferSize = s.bufferSize
    cfg.EnableAggregate = true

    // Initialize RDMA verbs (real or mock based on build tag)
    verbs, pd := initVerbs(cfg)

    // Create buffer pools
    sendPool := buffer.NewPool(verbs, pd, sendPoolConfig(cfg))
    recvPool := buffer.NewPool(verbs, pd, recvPoolConfig(cfg))

    // Create CQ pool and connection manager
    cqPool := cq.NewPool(verbs, cfg.CQNumber, pollerConfig(cfg))
    connMgr := connection.NewManager(managerConfig(verbs, sendPool, recvPool, cqPool))

    // Create and start server
    srv := server.NewServer(cfg, serverDeps(verbs, connMgr, cqPool, sendPool, recvPool))
    srv.RegisterHandler(s.newHandler())  // echo or sink
    return srv.Start(ctx)
}
```

**Echo Handler** — receives a message, sends it back (for latency measurement):

```go
type EchoHandler struct{ stats *AtomicStats }

func (h *EchoHandler) Handle(conn api.Connection, msg *api.Message) (*api.Message, error) {
    h.stats.MessagesReceived.Add(1)
    h.stats.BytesReceived.Add(int64(msg.Length))
    h.stats.MessagesSent.Add(1)
    h.stats.BytesSent.Add(int64(msg.Length))
    return msg, nil  // echo back same message
}
```

**Sink Handler** — receives messages, sends minimal ACK (for throughput measurement):

```go
type SinkHandler struct{ stats *AtomicStats; ackBuf []byte }

func (h *SinkHandler) Handle(conn api.Connection, msg *api.Message) (*api.Message, error) {
    h.stats.MessagesReceived.Add(1)
    h.stats.BytesReceived.Add(int64(msg.Length))
    h.stats.MessagesSent.Add(1)
    return &api.Message{Buffer: h.ackBuf, Length: 4}, nil  // minimal ACK
}
```

### Client Transport Integration

The benchmark client establishes real RDMA connections and measures true RTT:

```go
// cmd/golinker-bench/client.go (transport integration)
func (c *BenchClient) connectRDMA(ctx context.Context) ([]api.Connection, error) {
    cfg := config.DefaultConfig()
    cfg.CQNumber = c.config.CQNumber
    cfg.PollMode = parsePollMode(c.config.PollMode)
    cfg.BufferSize = c.config.BufferSize

    verbs, pd := initVerbs(cfg)
    sendPool := buffer.NewPool(verbs, pd, sendPoolConfig(cfg))
    recvPool := buffer.NewPool(verbs, pd, recvPoolConfig(cfg))
    cqPool := cq.NewPool(verbs, cfg.CQNumber, pollerConfig(cfg))
    connMgr := connection.NewManager(managerConfig(verbs, sendPool, recvPool, cqPool))

    conns := make([]api.Connection, c.config.Connections)
    for i := range conns {
        conn, err := connMgr.Connect(ctx, c.config.Addr)
        if err != nil { return nil, err }
        conns[i] = conn
    }
    return conns, nil
}
```

**Latency measurement (closed-loop, echo mode):**

```go
func (c *BenchClient) benchLatency(ctx context.Context, conn api.Connection) {
    payload := make([]byte, c.config.MessageSize)
    for {
        select {
        case <-ctx.Done(): return
        default:
        }
        c.limiter.Wait()
        start := time.Now()
        conn.Send(&api.Message{Buffer: payload, Length: uint32(len(payload))})
        _, err := conn.Recv(ctx)      // blocks until echo returns
        if err != nil { c.errors.Add(1); continue }
        latency := time.Since(start)
        c.hist.Record(latency.Microseconds())
        c.msgsSent.Add(1)
        c.msgsRecv.Add(1)
    }
}
```

**Throughput measurement (open-loop, sink mode):**

```go
func (c *BenchClient) benchThroughput(ctx context.Context, conns []api.Connection) {
    // Sender goroutines: round-robin across connections
    for i := 0; i < c.config.Goroutines; i++ {
        conn := conns[i % len(conns)]
        go func() {
            payload := make([]byte, c.config.MessageSize)
            for {
                select {
                case <-ctx.Done(): return
                default:
                }
                c.limiter.Wait()
                start := time.Now()
                conn.Send(&api.Message{Buffer: payload, Length: uint32(len(payload))})
                c.msgsSent.Add(1)
                latency := time.Since(start)
                c.hist.Record(latency.Microseconds())
            }
        }()
    }
    // Receiver goroutines: drain ACKs
    for _, conn := range conns {
        go func(c api.Connection) {
            for {
                _, err := c.Recv(ctx)
                if err != nil { return }
                c.msgsRecv.Add(1)
            }
        }(conn)
    }
}
```

### New CLI Flags

The following transport flags are added to both server and client:

```
Transport Flags:
  --device string      RDMA device name (default: auto-detect first device)
  --cq-number int      Number of completion queues (default 2)
  --poll-mode string   CQ poll mode: busy, event, smart, user (default "busy")
  --buffer-size int    Send/recv buffer size in bytes (default 12288)
  --batch-size int     Max messages per aggregation batch (default 16)
  --queue-depth int    QP send/recv queue depth, must be power of 2 (default 128)
  --numa-node int      NUMA node for buffer allocation (default -1 = auto)
```

### RDMA Hardware Counter Collection

Read hardware performance counters from sysfs at benchmark start and end:

```go
// cmd/golinker-bench/rdma_counters.go
type RDMACounters struct {
    TXPackets uint64
    RXPackets uint64
    TXBytes   uint64
    RXBytes   uint64
}

func ReadCounters(device string, port int) (*RDMACounters, error) {
    base := fmt.Sprintf("/sys/class/infiniband/%s/ports/%d/counters", device, port)
    // Read port_xmit_data, port_rcv_data, port_xmit_packets, port_rcv_packets
}
```

### Metrics Collected

For each end-to-end benchmark run:

| Metric Category | Metrics | Source |
|----------------|---------|--------|
| **Latency** | p50, p75, p90, p99, p99.9, p99.99, max, min, mean, stddev | HDR Histogram of actual RTT |
| **Throughput (TPS)** | messages/sec (send and recv rates) | Atomic counters / elapsed time |
| **Data Throughput** | MB/sec | messages/sec * message_size |
| **Resource Usage** | CPU%, RSS MB, goroutines, GC pause p99 | /proc/self/stat, runtime.ReadMemStats |
| **RDMA HW Counters** | TX/RX packets, TX/RX bytes | /sys/class/infiniband sysfs delta |
| **Errors** | send errors, recv errors, timeouts, connection errors | Atomic error counters |

### Two-Node Usage

```bash
# Node A: Start benchmark server in echo mode
golinker-bench server --addr 0.0.0.0:8629 --mode echo \
  --device mlx5_0 --poll-mode busy --cq-number 4

# Node B: Run latency benchmark
golinker-bench client latency --addr 10.0.0.1:8629 \
  --device mlx5_0 --message-size 64 --duration 60s \
  --output json --output-file latency_64B.json

# Node B: Run throughput benchmark
golinker-bench client throughput --addr 10.0.0.1:8629 \
  --device mlx5_0 --message-size 64 --connections 8 \
  --duration 60s --output json --output-file throughput_64B.json

# Node B: Run bandwidth benchmark (large messages)
golinker-bench client bandwidth --addr 10.0.0.1:8629 \
  --device mlx5_0 --message-size 65536 --connections 4 \
  --duration 60s --output json --output-file bandwidth_64KB.json

# Local: Compare results against baseline
golinker-bench report --compare baseline/latency_64B.json latency_64B.json
```

### Build Tags

The transport integration supports two build modes:

- **`go build ./...`** — Full build with real RDMA (requires libibverbs, librdmacm)
- **`go build -tags mock ./...`** — Mock build for development/CI without RDMA hardware

The `initVerbs()` function selects the appropriate implementation at compile time.

---

## 8.5. CM-Based RDMA Connection Setup

This section documents the Connection Manager (CM) based RDMA connection
establishment used by `golinker-bench` for real two-node benchmarks.

### Architecture

CM-based connections use the `rdma_cm` library to manage the full RDMA
connection lifecycle. Three key interfaces bridge the Go layer and the
underlying C rdma_cm calls:

| Interface | Role | Implementation |
|-----------|------|----------------|
| `api.CMEventChannel` | Listens for CM events (connect requests, established, disconnected) | `RealCMEventChannel` (real) / `MockCMEventChannel` (mock) |
| `api.CMAcceptor` | Accepts incoming connections: creates QP, calls `rdma_accept` | `RealCMEventChannel` (implements both) / `MockCMAcceptor` |
| `api.CMDialer` | Client-side dial: resolve addr/route, create QP, connect | `RealCMDialer` / `MockCMDialer` |

### Server Connection Flow

```
Client                          Server (RealCMEventChannel)
  │                                │
  │  rdma_connect()                │ rdma_listen() on addr:port
  │ ─────────────────────────────► │
  │                                │ GetEvent() → CONNECT_REQUEST
  │                                │   event.ID = incoming CM ID
  │                                │
  │                                │ CMAcceptor.AcceptConn():
  │                                │   1. rdma_create_qp(cmID, PD, sendCQ, recvCQ)
  │                                │   2. rdma_accept(cmID)
  │                                │   3. return RealQP
  │                                │
  │  ◄──── ESTABLISHED ──────────► │ GetEvent() → ESTABLISHED
  │                                │   conn.SetState(StateConnected)
  │                                │
  │        data exchange           │
  │  ◄═══════════════════════════► │
  │                                │
  │  rdma_disconnect()             │ GetEvent() → DISCONNECTED
  │ ─────────────────────────────► │   conn.Close()
```

### Client Connection Flow (RealCMDialer.Dial)

The `Dial()` method performs a complete 10-step handshake, blocking until
the connection is established or an error occurs:

```
Step 1:  rdma_create_event_channel()     — per-connection event channel
Step 2:  rdma_create_id(ch)              — allocate CM ID
Step 3:  rdma_resolve_addr(id, addr, port, 2000ms)
Step 4:  rdma_get_cm_event() → ADDR_RESOLVED
Step 5:  rdma_resolve_route(id, 2000ms)
Step 6:  rdma_get_cm_event() → ROUTE_RESOLVED
Step 7:  rdma_create_qp(id, PD, sendCQ, recvCQ, cfg)
Step 8:  rdma_connect(id)
Step 9:  rdma_get_cm_event() → ESTABLISHED
Step 10: return (QP, cmID)
```

On error at any step, previously allocated resources are cleaned up before
returning. The returned `cmID` (`unsafe.Pointer`) is stored in `ConnDeps`
for later disconnect.

### Connection State Machine with CM Events

```
                    ┌──────────┐
                    │ StateInit │
                    └─────┬────┘
                          │ CONNECT_REQUEST (server) or Dial() (client)
                    ┌─────▼──────────┐
                    │ StateConnecting │
                    └─────┬──────────┘
                          │ ESTABLISHED
                    ┌─────▼──────────┐
                    │ StateConnected  │ ← data exchange happens here
                    └─────┬──────────┘
                          │ DISCONNECTED or Close()
                    ┌─────▼──────────┐
                    │ StateDraining   │ → Dialer.Disconnect(cmID)
                    └─────┬──────────┘
                          │
                    ┌─────▼──────────┐
                    │ StateClosed     │
                    └────────────────┘

                    ┌────────────────┐
                    │ StateError      │ ← REJECTED event
                    └────────────────┘
```

### Disconnect and Cleanup

When `conn.Close()` is called:

1. If `Dialer` and `CMID` are both non-nil, call `Dialer.Disconnect(cmID)`
   (best-effort `rdma_disconnect`)
2. Transition to `StateDraining`
3. Close the done channel to unblock pending `Recv()` calls
4. Transition to `StateClosed`

The server side detects disconnection via the CM event loop
(`DISCONNECTED` event) and cleans up the corresponding connection.

### Connection Manager CM Wiring

The `connection.Manager` integrates CM via its config:

```go
connection.ManagerConfig{
    CMChannel:  cmChannel,   // server: event channel for incoming connections
    CMAcceptor: cmAcceptor,  // server: accepts connections, creates QPs
    CMDialer:   cmDialer,    // client: dials outbound connections
    PD:         pd,          // protection domain for QP creation
    SendCQ:     sendCQ,      // shared send completion queue
    RecvCQ:     recvCQ,      // shared recv completion queue
    QPConfig:   api.QueuePairConfig{...}, // QP sizing
}
```

- **Server path**: `RunCMEventLoop(ctx)` goroutine processes events;
  `handleConnectRequest` calls `CMAcceptor.AcceptConn()` and delivers
  the connection via the accept channel.
- **Client path**: `Connect(ctx, "host:port")` calls `CMDialer.Dial()`
  which blocks until `ESTABLISHED`, then returns a ready-to-use connection.
- **Lookup**: `cmIDConns` (`sync.Map`) provides O(1) lookup from CM ID to
  connection for established/disconnected/rejected events. Falls back to
  legacy scan for mock mode.

### Configuration

The `--device` flag specifies an IB device name (e.g., `mlx5_0`), **not** an
IP address. Use `ibv_devices` to list available devices:

```bash
# List RDMA devices
ibv_devices

# Start server on specific device
golinker-bench server --addr 0.0.0.0:8629 --device mlx5_0

# Client connects to server IP (resolved via rdma_resolve_addr)
golinker-bench client latency --addr 10.0.0.1:8629 --device mlx5_0
```

### Build Tags

| Tag | Transport | Use Case |
|-----|-----------|----------|
| (none) | Real RDMA | Production benchmarks on RDMA hardware |
| `mock` | Mock verbs + mock CM | Development, CI, unit tests |

Mock mode uses `MockCMEventChannel`, `MockCMAcceptor`, and `MockCMDialer`
which provide in-memory fakes that satisfy the interfaces without requiring
RDMA hardware.

---

## 9. Implementation Plan

### Phase 1: Foundation [DONE]

**Goal**: CLI framework, HDR histograms, micro-benchmarks, reporting.

- [x] Project structure: `cmd/golinker-bench/main.go`, CLI parsing with `cobra`
- [x] HDR histogram integration (`hdrhistogram-go`)
- [x] Text, JSON, CSV output formatters
- [x] Basic resource tracking (CPU, RSS, goroutines)
- [x] Token bucket rate limiter
- [x] Baseline comparison / regression detection
- [x] `bench_buffer_pool` — multi-goroutine alloc/free contention
- [x] `bench_cq_poll` — CQ polling throughput with mock completions
- [x] `bench_aggregation` — message pack/unpack overhead
- [x] `bench_channel_vs_mutex` — synchronization primitive comparison

### Phase 2: End-to-End RDMA Transport Integration [CURRENT]

**Goal**: Wire server and client modes to golinker core for real two-node benchmarks.

- [x] Server transport: initialize RDMA stack (verbs, PD, buffer pools, CQ pool, connection manager)
- [x] Echo handler: receive message, send back immediately
- [x] Sink handler: receive message, send minimal ACK
- [x] Client transport: establish RDMA connections to remote server
- [x] New transport CLI flags (--device, --queue-depth, --numa-node)
- [x] RealVerbs adapter (libibverbs CGo bindings)
- [x] CM-based connection setup (RealCMEventChannel, RealCMDialer, CMAcceptor)
- [x] Server CM event loop wiring (RunCMEventLoop, initCMListener)
- [x] Client CM dialer wiring (initCMDialer, ManagerConfig.CMDialer)
- [x] RDMA hardware counter collection (sysfs reader)
- [ ] `bench_latency` scenario: closed-loop RTT measurement via echo mode
- [ ] `bench_throughput` scenario: open-loop max msg/sec via sink mode
- [ ] Integration test: server + client on same host via SoftRoCE (rxe)

**Deliverable**: `golinker-bench server` on Node A + `golinker-bench client latency` on Node B produces real RDMA latency/throughput/TPS metrics.

### Phase 3: Advanced Scenarios + Stress Tests

**Goal**: Connection scaling, CQ scaling, stress tests, profiling integration.

- [ ] `bench_bandwidth` — large message throughput
- [ ] `bench_mixed` — Zipf distribution message sizes
- [ ] `bench_connections` — scaling with connection count
- [ ] `bench_cq_scaling` — scaling with CQ count
- [ ] `stress_sustained` — 1-hour fixed-rate stability
- [ ] `stress_burst` — burst/recovery pattern
- [ ] `stress_memory` — RSS/heap leak detection
- [ ] `stress_reconnect` — rapid connect/disconnect
- [ ] pprof integration (CPU, memory, block, trace)
- [ ] Warmup period (discard early samples from histogram)

### Phase 4: Controller + CI

**Goal**: Multi-node orchestration and CI regression pipeline.

- [ ] Controller mode: SSH-based remote node management
- [ ] Multi-client histogram merging
- [ ] CI pipeline YAML (SoftRoCE + hardware gates)
- [ ] Performance trend tracking
- [ ] Documentation

---

## Appendix: Project Structure

```
cmd/golinker-bench/
├── main.go              # CLI entry point, cobra commands
├── server.go            # Benchmark server (echo/sink)
├── client.go            # Benchmark client (load generator)
├── controller.go        # Multi-node orchestration
├── reporter.go          # Output formatting (text/json/csv)
├── comparison.go        # Baseline comparison logic
├── scenarios/
│   ├── latency.go       # End-to-end latency benchmark
│   ├── throughput.go    # End-to-end throughput benchmark
│   ├── bandwidth.go     # Large message bandwidth
│   ├── mixed.go         # Mixed workload (Zipf)
│   ├── connections.go   # Connection scaling
│   ├── cq_scaling.go    # CQ scaling
│   ├── cgo_overhead.go  # CGo micro-benchmark
│   ├── buffer_pool.go   # Buffer pool micro-benchmark
│   ├── cq_poll.go       # CQ poll micro-benchmark
│   ├── aggregation.go   # Aggregation micro-benchmark
│   ├── mr_reg.go        # MR registration micro-benchmark
│   ├── channel_mutex.go # Channel vs mutex comparison
│   ├── sustained.go     # Sustained load stress test
│   ├── burst.go         # Burst stress test
│   ├── memory.go        # Memory leak stress test
│   └── reconnect.go     # Reconnection stress test
├── histogram/
│   └── hdr.go           # HDR histogram wrapper + merge
├── ratelimit/
│   └── limiter.go       # Token bucket rate limiter
├── resources/
│   └── tracker.go       # CPU/RSS/goroutine monitoring
└── rdma/
    └── counters.go      # RDMA hardware counter reader
```

### Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/spf13/cobra` | CLI framework |
| `github.com/HdrHistogram/hdrhistogram-go` | Latency histograms |
| `golang.org/x/sync/errgroup` | Goroutine management |
| `runtime/pprof` | CPU/memory profiling |
| `net/http/pprof` | HTTP profiling endpoint |

### Build

```bash
cd cmd/golinker-bench
go build -o golinker-bench .

# Or with version info:
go build -ldflags "-X main.version=$(git describe --tags)" -o golinker-bench .
```
