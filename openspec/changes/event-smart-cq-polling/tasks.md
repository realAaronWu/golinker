> **Reconciliation note (2026-06-22):** All implementation tasks below are complete
> and verified (`go build -tags mock ./...` clean, `go test -tags mock -race ./...`
> green). Two organizational deviations from the original task text:
> - The CGo completion-channel bindings (tasks 2.1–2.4) were implemented as methods on
>   `RealCQ`/`RealVerbs` in `internal/rdma/real_verbs.go` (`ReqNotify`, `AckEvents`,
>   `CreateCQWithChannel`) rather than as standalone functions in `verbs.go`. The
>   underlying verb calls (`ibv_create_comp_channel`, `ibv_create_cq` with channel,
>   `ibv_req_notify_cq`, `ibv_ack_cq_events`) are all present.
> - The API method is `AckEvents`/`ReqNotify` (not `AckCQEvents`/`ReqNotifyCQ`).
>
> Task 7.3 (manual SoftRoCE verification) is **not done** — no real-verbs/SoftRoCE run
> has been performed yet. It is tracked as part of the outstanding validation gap.

## 1. API Extensions

- [x] 1.1 Add `CompChannelFD() int` to `api.CompletionQueue` interface in `api/rdma.go`
- [x] 1.2 Add `CreateCQWithChannel(size int) (CompletionQueue, error)` to `api.Verbs` interface in `api/rdma.go`
- [x] 1.3 Update `MockCQ` in `internal/rdma/mock_verbs.go` to implement `CompChannelFD()` returning `-1` (default) or a pipe FD (when event testing is needed)
- [x] 1.4 Update `RealCQ` in `internal/rdma/real_verbs.go` to implement `CompChannelFD()` returning the actual comp channel FD
- [x] 1.5 Verify `go build -tags mock ./...` compiles cleanly after interface changes

## 2. CGo Bindings for Completion Channel

- [x] 2.1 Add `CreateCompChannel(ctx unsafe.Pointer) (int, error)` Go wrapper — calls `ibv_create_comp_channel`, returns the FD *(implemented inline within `RealVerbs.CreateCQWithChannel` in `real_verbs.go`)*
- [x] 2.2 Add `CreateCQWithCompChannel(ctx unsafe.Pointer, size int, channelFD int) (unsafe.Pointer, error)` Go wrapper — calls `ibv_create_cq` with the comp channel *(implemented inline within `RealVerbs.CreateCQWithChannel`)*
- [x] 2.3 Add `ReqNotifyCQ(cq unsafe.Pointer) error` Go wrapper — calls `ibv_req_notify_cq(cq, 0)` *(implemented as `RealCQ.ReqNotify`)*
- [x] 2.4 Add `AckCQEvents(cq unsafe.Pointer, nevents int)` Go wrapper — calls `ibv_ack_cq_events` *(implemented as `RealCQ.AckEvents`)*
- [x] 2.5 Implement `CreateCQWithChannel` in `RealVerbs` using the new wrappers (create comp channel, create CQ with channel, store FD in `RealCQ`)
- [x] 2.6 Implement `CreateCQWithChannel` in `MockVerbs` — create a `pipe(2)` pair, return the read FD via `CompChannelFD()` so tests can trigger wake-ups by writing to the write FD

## 3. Real PollFunc

- [x] 3.1 Create `pkg/cq/pollfunc_real.go` (`//go:build !mock`) with a PollFunc that calls `golinker_poll_and_repost()` and converts C work completions to `[]api.WorkCompletion`
- [x] 3.2 Create `pkg/cq/pollfunc_mock.go` (`//go:build mock`) with `MockPollFunc` (the existing no-op default)
- [x] 3.3 Add `DefaultPollFunc() PollFunc` function in each build-tag file that returns the appropriate implementation
- [x] 3.4 Update `pollOnce()` in `poller.go` to call `DefaultPollFunc()` when `cfg.PollFunc == nil` instead of inline no-op
- [x] 3.5 Write unit test: inject known completions via mock Verbs, verify the PollFunc returns them correctly

## 4. Event-Driven Poll Loop

- [x] 4.1 Add per-CQ comp-channel FD tracking to the `Poller` (FD wrappers via `os.NewFile`)
- [x] 4.2 Rewrite `eventPollLoop()`: for each CQ with `CompChannelFD() >= 0`, wrap FD with `os.NewFile`, call `ReqNotify`, then enter loop: `f.Read()` → `AckEvents` → drain via `pollSingleCQ()` → `ReqNotify` → repeat
- [x] 4.3 Implement the arm-then-poll pattern: after `ReqNotify`, drain once more to catch completions that arrived during the re-arm window
- [x] 4.4 Handle `Close()`: close all `os.File` wrappers, unblock parked goroutines via context cancellation
- [x] 4.5 Handle CQs with `CompChannelFD() == -1` in event mode: fall back to busy-poll for those CQs
- [x] 4.6 Write unit test with pipe-based mock: create CQ with pipe FD, verify poller parks, write to pipe, verify poller wakes and polls

## 5. Smart-Mode Poll Loop

- [x] 5.1 Rewrite `smartPollLoop()`: busy-poll with `pollSingleCQ()`, count empties, after `SpinCount` call `ReqNotify` + `f.Read()` to park, wake → drain → reset counter → resume busy-poll
- [x] 5.2 Handle the arm-then-poll pattern in smart mode (post-arm drain before parking)
- [x] 5.3 Handle CQs without comp channel in smart mode: pure busy-poll fallback (never arm/park)
- [x] 5.4 Write unit test: verify mode transitions — inject completions to keep busy-poll active, stop injecting to trigger event-wait transition, resume injecting to verify wake-up and return to busy-poll

## 6. CQPool Integration

- [x] 6.1 Update `CQPool.Assign()` to call `CreateCQWithChannel` when `PollMode` is `Event` or `Smart`, and `CreateCQ` when `Busy` or `User`
- [x] 6.2 Pass `PollMode` to `PoolConfig` so the pool knows which CQ creation method to use
- [x] 6.3 Write unit test: verify `Assign()` returns CQ with `CompChannelFD() >= 0` in event mode, `-1` in busy mode

## 7. Verification

- [x] 7.1 Run full test suite: `go test -tags mock -race -count=1 ./...` — all existing tests pass
- [x] 7.2 Run build check: `go build -tags mock ./...` — clean compilation
- [ ] 7.3 Manual SoftRoCE verification (if available): start server in event mode, send messages, verify completions arrive and CPU usage is near-zero when idle — **outstanding** (no real-verbs run performed yet)
