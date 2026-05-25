## 1. API Extensions

- [ ] 1.1 Add `CompChannelFD() int` to `api.CompletionQueue` interface in `api/rdma.go`
- [ ] 1.2 Add `CreateCQWithChannel(size int) (CompletionQueue, error)` to `api.Verbs` interface in `api/rdma.go`
- [ ] 1.3 Update `MockCQ` in `internal/rdma/mock_verbs.go` to implement `CompChannelFD()` returning `-1` (default) or a pipe FD (when event testing is needed)
- [ ] 1.4 Update `RealCQ` in `internal/rdma/real_verbs.go` to implement `CompChannelFD()` returning the actual comp channel FD
- [ ] 1.5 Verify `go build -tags mock ./...` compiles cleanly after interface changes

## 2. CGo Bindings for Completion Channel

- [ ] 2.1 Add `CreateCompChannel(ctx unsafe.Pointer) (int, error)` Go wrapper in `internal/rdma/verbs.go` — calls `ibv_create_comp_channel`, returns the FD
- [ ] 2.2 Add `CreateCQWithCompChannel(ctx unsafe.Pointer, size int, channelFD int) (unsafe.Pointer, error)` Go wrapper — calls `ibv_create_cq` with the comp channel
- [ ] 2.3 Add `ReqNotifyCQ(cq unsafe.Pointer) error` Go wrapper — calls `ibv_req_notify_cq(cq, 0)`
- [ ] 2.4 Add `AckCQEvents(cq unsafe.Pointer, nevents int)` Go wrapper — calls `ibv_ack_cq_events`
- [ ] 2.5 Implement `CreateCQWithChannel` in `RealVerbs` using the new wrappers (create comp channel, create CQ with channel, store FD in `RealCQ`)
- [ ] 2.6 Implement `CreateCQWithChannel` in `MockVerbs` — create a `pipe(2)` pair, return the read FD via `CompChannelFD()` so tests can trigger wake-ups by writing to the write FD

## 3. Real PollFunc

- [ ] 3.1 Create `pkg/cq/pollfunc_real.go` (`//go:build !mock`) with `RealPollFunc` that calls `golinker_poll_and_repost()` and converts C work completions to `[]api.WorkCompletion`
- [ ] 3.2 Create `pkg/cq/pollfunc_mock.go` (`//go:build mock`) with `MockPollFunc` (the existing no-op default)
- [ ] 3.3 Add `DefaultPollFunc() PollFunc` function in each build-tag file that returns the appropriate implementation
- [ ] 3.4 Update `pollOnce()` in `poller.go` to call `DefaultPollFunc()` when `cfg.PollFunc == nil` instead of inline no-op
- [ ] 3.5 Write unit test: inject known completions via mock Verbs, verify `RealPollFunc` (mock build) returns them correctly

## 4. Event-Driven Poll Loop

- [ ] 4.1 Add `compFiles map[api.CompletionQueue]*os.File` field to `Poller` struct for tracking FD wrappers
- [ ] 4.2 Rewrite `eventPollLoop()`: for each CQ with `CompChannelFD() >= 0`, wrap FD with `os.NewFile`, call `ReqNotifyCQ`, then enter loop: `f.Read()` → `AckCQEvents` → drain via `pollOnce()` → `ReqNotifyCQ` → repeat
- [ ] 4.3 Implement the arm-then-poll pattern: after `ReqNotifyCQ`, call `pollOnce()` one more time to catch completions that arrived during re-arm window
- [ ] 4.4 Handle `Close()`: close all `os.File` wrappers in `compFiles`, unblock parked goroutines via context cancellation
- [ ] 4.5 Handle CQs with `CompChannelFD() == -1` in event mode: fall back to busy-poll for those CQs (log a warning)
- [ ] 4.6 Write unit test with pipe-based mock: create CQ with pipe FD, verify poller parks (no CPU spin), write to pipe, verify poller wakes and polls

## 5. Smart-Mode Poll Loop

- [ ] 5.1 Rewrite `smartPollLoop()`: replace `eventCh` blocking with real comp channel FD parking — busy-poll with `pollOnce()`, count empties, after `SpinCount` call `ReqNotifyCQ` + `f.Read()` to park, wake → drain → reset counter → resume busy-poll
- [ ] 5.2 Handle the arm-then-poll pattern in smart mode (same as event mode — post-arm drain before parking)
- [ ] 5.3 Handle CQs without comp channel in smart mode: pure busy-poll fallback (never arm/park)
- [ ] 5.4 Write unit test: verify mode transitions — inject completions to keep busy-poll active, stop injecting to trigger event-wait transition, resume injecting to verify wake-up and return to busy-poll

## 6. CQPool Integration

- [ ] 6.1 Update `CQPool.Assign()` to call `CreateCQWithChannel` when `PollMode` is `Event` or `Smart`, and `CreateCQ` when `Busy` or `User`
- [ ] 6.2 Pass `PollMode` to `PoolConfig` so the pool knows which CQ creation method to use
- [ ] 6.3 Write unit test: verify `Assign()` returns CQ with `CompChannelFD() >= 0` in event mode, `-1` in busy mode

## 7. Verification

- [ ] 7.1 Run full test suite: `go test -tags mock -race -count=1 ./...` — all existing tests must pass
- [ ] 7.2 Run build check: `go build -tags mock ./...` — clean compilation
- [ ] 7.3 Manual SoftRoCE verification (if available): start server in event mode, send messages, verify completions arrive and CPU usage is near-zero when idle
