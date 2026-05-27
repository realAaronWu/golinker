## ADDED Requirements

### Requirement: Production PollFunc calls C hot-path
In non-mock builds (`//go:build !mock`), the system SHALL provide a `RealPollFunc` that calls `golinker_poll_and_repost()` from `internal/rdma/hotpath.c`. This function polls the CQ for up to `maxWCs` completions and returns them as `[]api.WorkCompletion`.

#### Scenario: RealPollFunc returns completions
- **WHEN** `RealPollFunc` is called on a CQ that has pending completions
- **THEN** it returns a slice of `WorkCompletion` structs with correct WRID, Status, Opcode, ByteLen, and QPN fields

#### Scenario: RealPollFunc returns empty on idle CQ
- **WHEN** `RealPollFunc` is called on a CQ with no pending completions
- **THEN** it returns an empty slice and nil error

#### Scenario: RealPollFunc propagates CQ errors
- **WHEN** the underlying `ibv_poll_cq` returns a negative errno
- **THEN** `RealPollFunc` returns a non-nil error wrapping the errno

### Requirement: Mock PollFunc for test builds
In mock builds (`//go:build mock`), the default `PollFunc` SHALL be a no-op that returns `nil, nil`. Tests SHALL continue to inject custom `PollFunc` via `PollerConfig.PollFunc`.

#### Scenario: Mock build default PollFunc
- **WHEN** a `Poller` is created in a mock build without explicit `PollFunc`
- **THEN** `pollOnce()` uses the no-op default and returns 0 completions

#### Scenario: Test-injected PollFunc overrides default
- **WHEN** a test sets `PollerConfig.PollFunc` to a custom function
- **THEN** `pollOnce()` calls the custom function regardless of build tag

### Requirement: CQPool selects CQ creation method based on PollMode
The `CQPool` SHALL call `CreateCQWithChannel` when `PollMode` is `Event` or `Smart`, and `CreateCQ` (without channel) when `PollMode` is `Busy` or `User`.

#### Scenario: Event mode gets channel-backed CQ
- **WHEN** `CQPool.Assign()` is called with `PollMode = PollModeEvent`
- **THEN** the returned CQ has `CompChannelFD() >= 0`

#### Scenario: Busy mode gets plain CQ
- **WHEN** `CQPool.Assign()` is called with `PollMode = PollModeBusy`
- **THEN** the returned CQ has `CompChannelFD() == -1`

### Requirement: Build-tag split follows existing pattern
The real and mock `PollFunc` implementations SHALL use the same build-tag pattern as `pkg/buffer/alloc_cgo.go` (`//go:build !mock`) and `pkg/buffer/alloc_mock.go` (`//go:build mock`).

#### Scenario: Mock build compiles without RDMA headers
- **WHEN** `go build -tags mock ./...` is run on a system without libibverbs
- **THEN** the build succeeds and all CQ poller tests pass with the mock PollFunc
