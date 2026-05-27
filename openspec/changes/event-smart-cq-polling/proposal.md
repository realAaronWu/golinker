## Why

The CQ poller (`pkg/cq/poller.go`) has Go-level control flow for all four polling modes (busy, event, smart, user), but event and smart modes are **not wired to real RDMA primitives**. Specifically:

- The `eventCh` channel is a plain Go channel that nothing signals in production — event mode blocks forever unless `Notify()` is called manually.
- There are no CGo bindings for `ibv_create_comp_channel`, `ibv_req_notify_cq`, `ibv_get_cq_event`, or `ibv_ack_cq_events`.
- The `api.CompletionQueue` interface has no way to expose the completion channel file descriptor needed by Go's netpoller.
- The default `PollFunc` is a no-op stub; busy mode also doesn't call the C hot-path `golinker_poll_and_repost` in the real (non-mock) build.
- Smart mode's fallback to "event wait" just blocks on the same inert Go channel.

This means only busy-poll mode works in production, burning 100% CPU per CQ core even when idle. The design (§6) specifies event mode at ~20-50μs latency with near-zero idle CPU, and smart mode as the default at ~5-15μs with proportional CPU — both are critical for production deployments where not every connection justifies a dedicated core.

## What Changes

- **New CGo bindings** for CQ completion channel lifecycle: `ibv_create_comp_channel`, `ibv_req_notify_cq`, `ibv_get_cq_event`, `ibv_ack_cq_events`.
- **Extended `api.CompletionQueue` interface** with `CompChannelFD() int` to expose the completion channel FD for Go's netpoller.
- **Extended `api.Verbs` interface** with `CreateCQWithChannel` to create CQs bound to a completion channel.
- **Real `PollFunc` implementation** (`!mock` build tag) that calls `golinker_poll_and_repost` from the C hot-path — wires all modes to actual RDMA polling.
- **Event mode rewrite**: Replace `eventCh` blocking with `os.NewFile(compChannelFD)` + Go netpoller (`f.Read()` parks the goroutine at zero CPU), then drain CQ on wake, then re-arm with `ibv_req_notify_cq`.
- **Smart mode rewrite**: Busy-poll loop with real `PollFunc`; on SpinCount empty polls, arm CQ notification and park on comp channel FD; wake on completion, reset spin counter.
- **Mock implementations** updated: `MockCQ` gains `CompChannelFD()` returning a pipe FD for testability; mock `PollFunc` is injected via `PollerConfig` as today.

## Capabilities

### New Capabilities
- `event-cq-polling`: Event-driven CQ polling mode using `ibv_req_notify_cq` + Go netpoller on the completion channel FD. Near-zero idle CPU with ~20-50μs wake latency.
- `smart-cq-polling`: Adaptive CQ polling mode that busy-polls after receiving work, then falls back to event-driven after configurable idle threshold. Proportional CPU usage.
- `real-poll-func`: Production `PollFunc` implementation (`!mock` build) that calls `golinker_poll_and_repost` from the C hot-path, wiring all four CQ modes to actual RDMA completions.

### Modified Capabilities
<!-- No existing specs to modify -->

## Impact

- **API changes**: `api.CompletionQueue` gains `CompChannelFD() int`; `api.Verbs` gains `CreateCQWithChannel`. Both are additive (no breaking changes to existing callers since busy mode doesn't use the FD).
- **Affected packages**: `api/`, `internal/rdma/` (new C bindings + Go wrappers), `pkg/cq/` (poller rewrite for event/smart), `pkg/cq/pool.go` (pass comp channel to CQ creation).
- **Build tags**: Real implementation gated behind `!mock`; mock build unaffected.
- **Testing**: Existing mock tests continue to work (PollFunc injection). New tests need a pipe-based mock for `CompChannelFD()` and SoftRoCE integration tests for real hardware path.
- **Config**: No new config fields — `PollMode`, `SpinCount`, `CQNumber` already exist and are sufficient.
