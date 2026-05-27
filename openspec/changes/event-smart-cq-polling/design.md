## Context

golinker's CQ poller (`pkg/cq/poller.go`) already has the Go-level control flow for all four polling modes. The `Poller` struct has `busyPollLoop`, `eventPollLoop`, and `smartPollLoop` methods with correct state machine logic (spin counting, event channel blocking, mode transitions). However, these are wired to mock primitives:

- `eventCh` is a `chan struct{}` that nothing in the production path signals.
- `PollFunc` defaults to a no-op that returns `nil, nil`.
- `api.CompletionQueue` only exposes `Handle()` and `Size()` — no completion channel FD.
- `api.Verbs.CreateCQ()` creates a CQ without an associated completion channel.

The C hot-path (`internal/rdma/hotpath.c`) has `golinker_poll_and_repost()` ready but is never called by the Go poller. The design doc (§6) specifies the complete event-driven architecture using `ibv_req_notify_cq` and Go's netpoller on the `comp_channel` FD.

**Current state of `internal/rdma/verbs.go`**: Has `CreateCQ` binding but no completion channel functions. The mock build (`mock_verbs.go`) provides `MockCQ` without `CompChannelFD()`.

## Goals / Non-Goals

**Goals:**
- Wire event-driven CQ polling to real RDMA completion channel primitives
- Wire smart-mode adaptive polling with real busy-poll → event fallback
- Connect the default `PollFunc` to the C hot-path for all modes (not just event/smart)
- Maintain full backward compatibility: mock tests, busy mode, user mode all unchanged
- Verify on SoftRoCE that event mode consumes near-zero CPU when idle

**Non-Goals:**
- Dynamic CQ resizing (separate change, design §10)
- CQ pool rebalancing or connection migration between CQs
- Prometheus metrics for CQ polling (separate metrics change)
- NUMA-aware CQ thread pinning (separate change)
- Changing the `pollOnce()` dispatch logic or `CompletionHandler` interface

## Decisions

### D1: Extend `api.CompletionQueue` with `CompChannelFD() int`

**Decision**: Add `CompChannelFD() int` to the `api.CompletionQueue` interface. Returns `-1` when no completion channel is associated (busy mode, mock builds).

**Rationale**: The Go netpoller needs a file descriptor to park the goroutine. The alternative — passing the FD through `PollerConfig` — would couple the poller to a specific CQ's FD, breaking the multi-CQ-per-poller design.

**Alternatives considered**:
- *Separate `EventableCompletionQueue` interface*: Would require type assertions in the poller. Rejected for complexity.
- *Store FD in `Poller` directly*: Breaks when one poller manages multiple CQs with different FDs. Rejected.

### D2: Add `CreateCQWithChannel` to `api.Verbs`

**Decision**: Add `CreateCQWithChannel(size int) (CompletionQueue, error)` to `api.Verbs`. This creates a CQ with an associated `ibv_comp_channel`. The existing `CreateCQ(size int)` remains for backward compatibility (creates CQ without channel — suitable for busy/user mode).

**Rationale**: Not all CQs need a completion channel. Busy-poll mode doesn't use one, and creating unnecessary channels wastes kernel resources (each channel is an FD + eventfd).

### D3: Add C bindings via new Go wrapper functions, not new C hot-path code

**Decision**: Add Go-level CGo wrapper functions in `internal/rdma/verbs.go` for:
- `ibv_create_comp_channel(ctx)` → `CreateCompChannel()`
- `ibv_create_cq_ex()` or `ibv_create_cq()` with channel param → used by `CreateCQWithChannel()`
- `ibv_req_notify_cq(cq, solicited_only=0)` → `ReqNotifyCQ(cq)`
- `ibv_get_cq_event(channel, &cq, &ctx)` → `GetCQEvent(channel)` (blocking)
- `ibv_ack_cq_events(cq, nevents)` → `AckCQEvents(cq, n)`

These are NOT hot-path functions — they run once per event wake-up, not per-completion. Therefore plain CGo wrappers are fine (no batching needed). The existing `golinker_poll_and_repost()` in `hotpath.c` remains the hot-path for draining completions.

**Rationale**: Separate hot-path (C) from control-path (Go CGo wrappers). Adding these to `hotpath.c` would unnecessarily complicate the performance-critical C code.

### D4: Event mode uses `os.NewFile()` + `f.Read()` for netpoller integration

**Decision**: Wrap the completion channel FD with `os.NewFile()`, then use `f.Read()` to park the goroutine in Go's netpoller. On wake: call `AckCQEvents`, drain CQ via `PollFunc`, re-arm with `ReqNotifyCQ`.

**Sequence**:
```
1. ReqNotifyCQ(cq)                     // arm CQ notification
2. f.Read(eventBuf)                    // goroutine parks — zero CPU
3. --- NIC delivers completion ---
4. --- kernel signals comp_channel FD ---
5. --- Go netpoller wakes goroutine ---
6. AckCQEvents(cq, 1)                  // ack the event
7. loop { PollFunc(cq, batchSize) }    // drain all pending completions
8. goto 1                              // re-arm and park again
```

**Rationale**: This matches the design doc §6.4 exactly. Go's netpoller uses `epoll` internally, which multiplexes the CQ FD alongside all other network I/O — no dedicated thread needed.

**Alternative considered**: Using `ibv_get_cq_event()` (blocking C call) in a dedicated goroutine. Rejected because it would pin an OS thread via CGo blocking semantics, defeating the purpose of event mode.

### D5: Smart mode arms CQ notification only when transitioning to event-wait

**Decision**: Smart mode calls `ReqNotifyCQ` only after `SpinCount` empty polls, right before parking on the FD. It does NOT re-arm on every poll cycle (that would add unnecessary CGo overhead during busy periods).

**State machine**:
```
BUSY_POLL:
  PollFunc(cq) → got completions → process → stay BUSY_POLL, reset idle_count
  PollFunc(cq) → empty → idle_count++
  idle_count >= SpinCount → ReqNotifyCQ(cq) → transition to EVENT_WAIT

EVENT_WAIT:
  f.Read(eventBuf) → wake
  AckCQEvents(cq, 1)
  drain CQ via PollFunc
  → transition to BUSY_POLL, reset idle_count
```

### D6: Real `PollFunc` via build tags

**Decision**: Create `pkg/cq/pollfunc_real.go` (`//go:build !mock`) that provides a `RealPollFunc` calling `golinker_poll_and_repost()`. Create `pkg/cq/pollfunc_mock.go` (`//go:build mock`) that provides the no-op default. The `CQPool` constructor selects the appropriate one based on build tag.

**Rationale**: Keeps the build-tag split consistent with `pkg/buffer/alloc_cgo.go` / `alloc_mock.go`. Tests inject their own `PollFunc` via `PollerConfig` as before.

### D7: One `os.File` per CQ, owned by the poller

**Decision**: When the poller starts in event or smart mode, it wraps each CQ's `CompChannelFD()` into an `os.File` and stores it in `cqEntry`. On `Close()`, it closes the files. CQs with `CompChannelFD() == -1` are skipped (not event-capable) and always busy-polled.

This means a single poller can manage a mix of event-capable and non-event-capable CQs. In practice, all CQs will be created with `CreateCQWithChannel` when the config mode is event or smart.

## Risks / Trade-offs

**[Risk: Missed completions during re-arm window]** → Between draining the CQ and re-arming with `ReqNotifyCQ`, a completion could arrive and be missed until the next event. **Mitigation**: After `ReqNotifyCQ`, do one more `PollFunc` drain. If it finds completions, process them and loop without parking. This is the standard "arm-then-poll" pattern from the ibverbs documentation.

**[Risk: `os.NewFile` FD lifetime management]** → `os.NewFile` takes ownership of the FD. If the CQ or comp channel is destroyed while the file is open, behavior is undefined. **Mitigation**: `Poller.Close()` closes the `os.File` before the CQ is destroyed. Document that CQ destruction must happen after poller shutdown.

**[Risk: epoll overhead on many CQs]** → Each CQ's comp channel FD is added to the Go process's epoll set. With the default 2 CQs this is trivial. Even 100 CQs would be fine for epoll. **Mitigation**: None needed for foreseeable scale.

**[Trade-off: Smart mode spin count tuning]** → The default `SpinCount=1024` is a guess from the design doc. Too low = unnecessary event overhead, too high = wasted CPU. **Mitigation**: The value is configurable via `pkg/config`. A benchmark in the verification change will establish the right default for SoftRoCE and real hardware.

**[Trade-off: Two CQ creation paths]** → Having both `CreateCQ` and `CreateCQWithChannel` adds API surface. **Mitigation**: `CQPool` will call the appropriate one based on `PollMode` config, so callers don't need to decide. The split avoids forcing a comp channel on busy-mode users who don't need one.
