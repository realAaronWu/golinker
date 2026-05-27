## ADDED Requirements

### Requirement: CQ creation with completion channel
The system SHALL support creating a CompletionQueue with an associated completion channel via `Verbs.CreateCQWithChannel(size int)`. The returned CQ SHALL expose the completion channel file descriptor via `CompChannelFD() int`.

#### Scenario: Create CQ with completion channel
- **WHEN** `CreateCQWithChannel(4096)` is called on a valid Verbs instance
- **THEN** a CompletionQueue is returned with `CompChannelFD() >= 0` and `Size() == 4096`

#### Scenario: CQ without channel returns sentinel FD
- **WHEN** `CreateCQ(4096)` is called (the existing method without channel)
- **THEN** the returned CompletionQueue has `CompChannelFD() == -1`

### Requirement: CQ notification arm and ack bindings
The system SHALL provide Go wrappers for `ibv_req_notify_cq` and `ibv_ack_cq_events` in the `internal/rdma` package. These are control-path functions (not hot-path) and use standard CGo calls.

#### Scenario: Arm CQ notification
- **WHEN** `ReqNotifyCQ(cq)` is called on a CQ that has a completion channel
- **THEN** the function returns nil and the CQ is armed for the next completion event

#### Scenario: Ack CQ events
- **WHEN** `AckCQEvents(cq, 1)` is called after receiving a CQ event
- **THEN** the function acknowledges the event without error

### Requirement: Event-driven poll loop parks on comp channel FD
The poller in `PollModeEvent` SHALL wrap the CQ's completion channel FD with `os.NewFile()` and use `f.Read()` to park the goroutine in Go's netpoller. The goroutine SHALL consume near-zero CPU while parked.

#### Scenario: Event mode parks when idle
- **WHEN** the poller is started in `PollModeEvent` with no pending completions
- **THEN** the poll goroutine is parked (no CPU consumption) until a completion arrives on the CQ

#### Scenario: Event mode wakes on completion
- **WHEN** the poller is parked in event mode and a work completion arrives on the CQ
- **THEN** the kernel signals the completion channel FD, Go's netpoller wakes the goroutine, and the poller drains all pending completions via `PollFunc`

### Requirement: Event mode re-arms notification after draining
After waking from an event and draining the CQ, the poller SHALL re-arm the CQ notification with `ReqNotifyCQ` and do one additional `PollFunc` drain before parking again. This prevents missed completions during the re-arm window.

#### Scenario: No missed completions during re-arm
- **WHEN** the poller wakes, drains the CQ, calls `ReqNotifyCQ`, and a new completion arrives between re-arm and park
- **THEN** the poller's post-arm drain catches the new completion and processes it before parking

### Requirement: Event mode cleans up os.File on close
The poller SHALL close all `os.File` wrappers for CQ completion channel FDs when `Close()` is called, before CQ destruction occurs.

#### Scenario: Graceful shutdown of event mode
- **WHEN** `Poller.Close()` is called while in event mode
- **THEN** the poll loop exits, all `os.File` wrappers are closed, and no FD leaks occur
