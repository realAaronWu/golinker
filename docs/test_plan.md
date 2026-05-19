# golinker Test Plan

## 1. Testing Strategy Overview

golinker uses a layered testing approach to ensure correctness, reliability, and performance of the RDMA RPC transport:

| Layer | Scope | Dependencies | Run Frequency |
|-------|-------|--------------|---------------|
| **Unit Tests** | Per-package, isolated logic | Mocked RDMA verbs, no hardware | Every commit |
| **Integration Tests** | Cross-package, real RDMA ops | SoftRoCE (rxe) kernel module | Every PR / nightly |
| **Fault Injection Tests** | Error paths, failure recovery | SoftRoCE + injected failures | Nightly |
| **Performance Regression** | Latency/throughput gates | Real or SoftRoCE RDMA | Weekly / release |

**Principles:**
- Unit tests mock all external dependencies (libibverbs, librdmacm) via interfaces
- Integration tests use SoftRoCE (`rxe` kernel module) — no real RDMA hardware required in CI
- Fault injection tests use wrapper interfaces to inject errors at precise points
- Performance tests run in controlled environments with pinned CPU/memory

---

## 2. Unit Tests by Package

### 2.1 `pkg/buffer/`

#### SendPool

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestSendPool_AllocFree_Cycle` | Allocate a buffer, use it, free it back to pool | Buffer returned to channel, pool size unchanged |
| `TestSendPool_Exhaustion_Blocks` | Allocate all buffers (queue depth=128), next alloc blocks | Goroutine blocks until a buffer is freed; unblocks on free |
| `TestSendPool_BusyFlag_AtThreshold` | Allocate 65 of 128 buffers (>50%) | `is_busy` flag set to `true` |
| `TestSendPool_BusyFlag_ClearsBelow` | Free buffers until below 50% | `is_busy` flag set to `false` |
| `TestSendPool_ConcurrentAccess` | 100 goroutines alloc/free in tight loop | No races (run with `-race`), no panics, pool size stable |
| `TestSendPool_RecalibrateStuck` | Mark buffer as in-flight, wait past recalibrate timeout | Buffer reclaimed by pool, available for reuse |
| `TestSendPool_ChannelCapacity` | Create pool with queue depth N | Channel capacity == N |

#### RecvPool

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestRecvPool_PrePostBuffers` | Initialize pool | All buffers posted as receive WRs (mock verbs records calls) |
| `TestRecvPool_RepostAfterCompletion` | Simulate recv completion, call repost | Buffer re-posted via `ibv_post_recv` mock |
| `TestRecvPool_RepostBatch` | Complete multiple receives, repost in batch | All buffers re-posted, correct WR chain |

#### LargeBuffer

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestLargeBuffer_Alloc` | Request 1MB large buffer | Buffer allocated, registered as MR, tracked in map |
| `TestLargeBuffer_CapacityExceeded` | Allocate buffers totaling >1GB | Returns error (capacity exceeded) |
| `TestLargeBuffer_ExactlyAtCapacity` | Allocate buffers totaling exactly 1GB | Succeeds; next 1-byte alloc fails |
| `TestLargeBuffer_ExpiryAfterTimeout` | Allocate buffer, advance clock 5s+ | Buffer freed, MR deregistered |
| `TestLargeBuffer_NoExpiryBeforeTimeout` | Allocate buffer, check at 4s | Buffer still alive |
| `TestLargeBuffer_ConcurrentAlloc` | 50 goroutines allocate large buffers | Total never exceeds 1GB cap, no races |

#### NUMA Allocation

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestNUMA_CorrectNode` | Allocate on specified NUMA node | Memory bound to correct node (verify via mock) |
| `TestNUMA_FallbackToAny` | Specify invalid NUMA node | Falls back to any-node allocation without error |
| `TestNUMA_Disabled` | NUMA not available on system | Allocates normally, no panic |

---

### 2.2 `pkg/cq/`

#### Poll Modes

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestCQ_BusyPoll_ReturnsCompletions` | Inject completions into mock CQ, use busy-poll | Completions returned immediately, no sleep |
| `TestCQ_BusyPoll_EmptyCQ` | No completions available | Returns 0 completions, loops immediately |
| `TestCQ_EventPoll_WakesOnNotify` | Arm CQ notification, inject completion | Blocks until notify, then returns completion |
| `TestCQ_EventPoll_SpuriousWake` | Arm CQ, wake with no completions | Re-arms and blocks again |
| `TestCQ_SmartPoll_TransitionBusyToEvent` | Start busy, no completions for threshold | Transitions to event-driven mode |
| `TestCQ_SmartPoll_TransitionEventToBusy` | In event mode, receive burst of completions | Transitions to busy-poll mode |
| `TestCQ_UserPoll_ManualTrigger` | Call poll explicitly | Returns current completions, no background loop |

#### Batch Processing

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestCQ_Batch_ZeroCompletions` | Poll with empty CQ | Returns slice of length 0, no error |
| `TestCQ_Batch_SingleCompletion` | One completion pending | Returns slice of length 1 |
| `TestCQ_Batch_MaxBatch` | CQ full to batch size limit | Returns exactly max_batch completions |
| `TestCQ_Batch_RepostInSameCall` | Batch poll + repost via C hot-path | Single CGo boundary crossing, recv buffers reposted |

#### Error Handling

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestCQ_FatalError_SetsFatalFlag` | Inject `IBV_WC_FATAL_ERR` | Fatal flag set, polling stops |
| `TestCQ_FatalError_CallsCallback` | Fatal error with registered callback | Callback invoked with error details |
| `TestCQ_WorkCompletionError_NonFatal` | Inject `IBV_WC_REM_ACCESS_ERR` | Error reported, polling continues |

#### CQ Pool

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestCQPool_RoundRobinAssignment` | Assign 4 connections to pool of 2 CQs | Each CQ gets 2 connections |
| `TestCQPool_ResizeOnOverflow` | Exceed max connections per CQ | New CQ created, connections rebalanced |
| `TestCQPool_Cleanup` | Close all connections on a CQ | CQ destroyed when empty |

---

### 2.3 `pkg/connection/`

#### Connect 8 Phases

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestConnect_AllPhasesSuccess` | Full 8-phase connect with mock CM | Connection established, state = connected |
| `TestConnect_Phase1_ResolveAddrTimeout` | Timeout during address resolution | Error returned, resources cleaned up |
| `TestConnect_Phase2_ResolveRouteTimeout` | Timeout during route resolution | Error returned, resources cleaned up |
| `TestConnect_Phase3_CreateQPFailure` | QP creation fails | Error returned, CM ID destroyed |
| `TestConnect_Phase4_ConnectTimeout` | `rdma_connect` times out | Error returned, QP destroyed |
| `TestConnect_Phase5_Rejected` | Server rejects connection | `RDMA_CM_EVENT_REJECTED` handled, error returned |
| `TestConnect_Phase6_MRRegistrationFail` | Memory registration fails post-connect | Connection torn down, error propagated |
| `TestConnect_Phase7_BufferPostFail` | Initial recv buffer post fails | Connection torn down, error propagated |
| `TestConnect_Phase8_HandshakeTimeout` | Application-level handshake times out | Connection closed gracefully |
| `TestConnect_ContextCancellation` | Cancel context mid-connect | Connect aborts, no goroutine leak |

#### CM Event Loop

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestCM_EventAddrResolved` | Inject addr resolved event | Transitions to route resolution |
| `TestCM_EventRouteResolved` | Inject route resolved event | Calls `rdma_connect` |
| `TestCM_EventEstablished` | Inject established event | State = connected, ready for traffic |
| `TestCM_EventDisconnected` | Inject disconnect event | Cleanup triggered, state = disconnected |
| `TestCM_EventRejected` | Inject rejected event | Error callback invoked, cleanup triggered |
| `TestCM_EventTimedWait` | Inject timewait exit | Resources fully freed |
| `TestCM_UnknownEvent` | Inject unknown event type | Logged, no crash |

#### Connection Pool

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestConnPool_AddRemove` | Add connection, remove by ID | Pool size changes correctly |
| `TestConnPool_Range` | Add 5 connections, iterate | All 5 visited exactly once |
| `TestConnPool_ConcurrentAccess` | 50 goroutines add/remove/range | No races, no panics |
| `TestConnPool_DuplicateAdd` | Add same ID twice | Returns error or updates entry |
| `TestConnPool_RemoveNonexistent` | Remove ID not in pool | No panic, returns not-found |

#### Close

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestConnection_Close_Idempotent` | Call Close() 3 times | Only first call executes cleanup (sync.Once) |
| `TestConnection_Close_CleansResources` | Close active connection | QP destroyed, MRs deregistered, buffers freed, CM ID destroyed |
| `TestConnection_Close_DrainsInflight` | Close with in-flight sends | Waits for completions before destroying QP |

---

### 2.4 `pkg/message/`

#### Aggregation

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestAggregation_ThresholdTrigger` | Enqueue messages until byte threshold hit | Flush triggered, aggregated message sent |
| `TestAggregation_IdleTrigger` | Enqueue one message, wait idle timeout | Flush triggered by timer |
| `TestAggregation_OverflowTrigger` | Enqueue message that would exceed buffer size | Current batch flushed, oversized message starts new batch |
| `TestAggregation_MultipleMsgsOneBatch` | Enqueue 5 small messages rapidly | All 5 in one aggregated send |
| `TestAggregation_ResetAfterFlush` | Flush, then enqueue more | New batch starts fresh |

#### Wire Format

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestWireFormat_EncodeCommandHeader` | Encode command type 230 | 12 bytes: 4B type (little-endian) + 8B reserved zeros |
| `TestWireFormat_DecodeCommandHeader` | Decode 12-byte buffer | Correct type extracted (230-235 range) |
| `TestWireFormat_EncodeMessageHeader` | Encode timestamp + size | 12 bytes: 8B timestamp (big-endian) + 4B size (big-endian) |
| `TestWireFormat_DecodeMessageHeader` | Decode 12-byte message header | Correct timestamp and size |
| `TestWireFormat_CommandTypes_AllValid` | Encode/decode each type 230-235 | Round-trip matches for all |
| `TestWireFormat_InvalidCommandType` | Decode type outside 230-235 | Returns error |
| `TestWireFormat_LargeBufferPayload` | Encode message referencing RDMA read | Correct rkey, remote addr, length in payload |

#### Edge Cases

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestMessage_ExactlyBufferSized` | Message exactly fills send buffer | Sent without aggregation, no overflow |
| `TestMessage_EmptyPayload` | Zero-length message body | Headers still valid, size field = 0 |
| `TestMessage_MaxSize` | Message at max allowed size | Routed to large buffer (RDMA read) path |
| `TestMessage_HeaderOnly` | Command with no message body | Valid parse, no data portion |

---

### 2.5 `pkg/health/`

#### Heartbeat

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestHeartbeat_SendsAtIdleThreshold` | No activity for 290s (mocked clock) | Heartbeat command (type 233) sent |
| `TestHeartbeat_NoSendBeforeThreshold` | Activity at 289s | No heartbeat sent |
| `TestHeartbeat_ExpiresAtExpireThreshold` | No response for 300s | Connection marked expired, close initiated |
| `TestHeartbeat_ResetOnActivity` | Send data at 289s | Idle timer resets, no heartbeat at 290s absolute |
| `TestHeartbeat_ContextCancellation` | Cancel health monitor context | Goroutine exits cleanly, no leak |
| `TestHeartbeat_ResponseResetsExpiry` | Receive heartbeat response at 299s | Expiry timer reset |

#### Buffer Monitor

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestBufferMonitor_RecalibrateStuck` | Buffer marked in-flight beyond timeout | Buffer reclaimed to pool |
| `TestBufferMonitor_CleanExpiredLargeBuffers` | Large buffer past 5s expiry | Buffer freed, MR deregistered |
| `TestBufferMonitor_HealthyBuffersUntouched` | All buffers within normal bounds | No recalibration or cleanup |

---

### 2.6 `pkg/config/`

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestConfig_DefaultValues` | Load empty config | All fields have documented defaults (port=0, queue_depth=128, etc.) |
| `TestConfig_YAMLParsing` | Load valid YAML file | All fields populated correctly |
| `TestConfig_InvalidPort` | Port = -1 or 99999 | Validation error returned |
| `TestConfig_NegativeTimeout` | Heartbeat timeout = -5s | Validation error returned |
| `TestConfig_ZeroQueueDepth` | Queue depth = 0 | Validation error (must be > 0) |
| `TestConfig_PartialYAML` | YAML with only some fields | Missing fields get defaults |
| `TestConfig_UnknownFields` | YAML with extra unknown keys | Ignored or warning (no error) |
| `TestConfig_EnvironmentOverride` | Env var overrides YAML value | Env var takes precedence |

---

### 2.7 `pkg/server/`

| Test Name | Description | Assertions |
|-----------|-------------|------------|
| `TestServer_StartStop` | Start server, stop it | Listener closed, no goroutine leaks |
| `TestServer_AcceptConnection` | Client connects to listening server | Connection added to pool, handler invoked |
| `TestServer_AcceptMultiple` | 10 clients connect concurrently | All 10 accepted, pool size = 10 |
| `TestServer_RejectAfterShutdown` | Stop server, then client tries to connect | Connection refused |
| `TestServer_ListenAddressInUse` | Start two servers on same port | Second returns error |

---

## 3. Integration Tests

All integration tests require SoftRoCE (`rxe`) and use build tag `//go:build integration`.

| Test Name | Description | Validates |
|-----------|-------------|-----------|
| `TestInteg_FullLifecycle` | Listen → connect → send 1 msg → recv → disconnect | End-to-end happy path |
| `TestInteg_Bidirectional` | Client sends msg, server responds | Both directions work on same connection |
| `TestInteg_MultipleConnections` | 5 clients connect to 1 server | Server handles concurrent connections |
| `TestInteg_LargeMessage_RDMARead` | Send 1MB message (triggers RDMA read flow) | Large buffer allocated, read invitation sent, data transferred |
| `TestInteg_ConnectionPool_ConcurrentSenders` | 10 goroutines send on pool of 3 connections | Messages interleaved correctly, no corruption |
| `TestInteg_ServerGracefulShutdown` | Send msg, initiate shutdown, verify msg completes | In-flight messages delivered before close |
| `TestInteg_CQResize_UnderLoad` | Start with 1 CQ, add connections until resize | New CQ created, traffic continues uninterrupted |
| `TestInteg_Aggregation_EndToEnd` | Send 20 small msgs rapidly | Receiver gets all 20, possibly aggregated |
| `TestInteg_Heartbeat_KeepsAlive` | Connect, idle for 300s+ (accelerated clock) | Heartbeats exchanged, connection stays alive |
| `TestInteg_ReconnectAfterDisconnect` | Connect, disconnect, reconnect | Second connection succeeds, fresh state |
| `TestInteg_MaxMessageSize` | Send message at exactly max threshold | Delivered via RDMA read path |
| `TestInteg_ZeroLengthMessage` | Send empty message | Received with zero-length payload |

---

## 4. Fault Injection Tests

| Test Name | Injected Fault | Expected Behavior |
|-----------|---------------|-------------------|
| `TestFault_NetworkPartition` | Drop all CM events after connect | Heartbeat expires, connection closed after 300s |
| `TestFault_CQFatalError` | Return `IBV_WC_FATAL_ERR` from poll | CQ marks fatal, connection closed, error callback |
| `TestFault_MRRegistrationFailure` | `ibv_reg_mr` returns NULL | Connect fails gracefully, error propagated |
| `TestFault_BufferExhaustion` | Hold all 128 buffers, attempt send | Send blocks until timeout or buffer freed |
| `TestFault_Phase1_Timeout` | `rdma_resolve_addr` never completes | Connect returns timeout error after deadline |
| `TestFault_Phase2_Timeout` | `rdma_resolve_route` never completes | Connect returns timeout error after deadline |
| `TestFault_Phase3_Timeout` | `rdma_connect` never gets event | Connect returns timeout error after deadline |
| `TestFault_Phase4_Rejection` | Server sends reject | Client gets rejection error, cleans up |
| `TestFault_HeartbeatTimeout` | Suppress heartbeat responses | Connection expired after 300s |
| `TestFault_LargeBufferAtCapacity` | Fill 1GB of large buffers, request more | Allocation returns capacity error |
| `TestFault_DeviceHotRemove` | Close device FD mid-operation | All connections on device closed, errors propagated |
| `TestFault_QPErrorState` | Transition QP to error state | Connection detects error, initiates close |
| `TestFault_PartialRecv` | Completion with fewer bytes than header | Parse error handled, connection not crashed |
| `TestFault_DoubleFree` | Free same buffer twice | Second free is no-op or returns error, no corruption |
| `TestFault_SlowConsumer` | Receiver processes messages slowly | Sender backpressure via busy flag, no buffer overflow |

---

## 5. Performance Regression Tests

### Latency Gates

| Metric | Threshold | Conditions |
|--------|-----------|------------|
| p50 latency | < 5 us | 64B message, single connection, loopback |
| p99 latency | < 20 us | 64B message, single connection, loopback |
| p99.9 latency | < 50 us | 64B message, single connection, loopback |
| p50 latency (1KB) | < 8 us | 1KB message, single connection, loopback |
| p99 latency (1KB) | < 30 us | 1KB message, single connection, loopback |

### Throughput Gates

| Metric | Threshold | Conditions |
|--------|-----------|------------|
| Small message throughput | > 5M msgs/sec | 64B messages, single CQ, saturated |
| Medium message throughput | > 2M msgs/sec | 1KB messages, single CQ |
| Aggregated throughput | > 8M msgs/sec | 64B messages with aggregation enabled |
| Multi-connection throughput | > 15M msgs/sec | 64B, 4 connections, 4 CQs |

### Micro-benchmarks

| Metric | Threshold | Test Function |
|--------|-----------|---------------|
| Buffer pool alloc+free | < 200 ns | `BenchmarkSendPool_AllocFree` |
| CGo batch poll overhead | < 100 ns/call | `BenchmarkCGo_BatchPoll` |
| Message header encode | < 20 ns | `BenchmarkMessageHeader_Encode` |
| Message header decode | < 20 ns | `BenchmarkMessageHeader_Decode` |
| Aggregation flush (10 msgs) | < 500 ns | `BenchmarkAggregation_Flush10` |
| Connection pool lookup | < 50 ns | `BenchmarkConnPool_Lookup` |

### Stability Gates

| Metric | Threshold | Duration |
|--------|-----------|----------|
| RSS memory stability | < 5% growth | 1M messages |
| Goroutine count stability | No growth | 1M messages |
| CQ average batch size | > 10 completions/poll | Under saturation |
| Buffer pool utilization | < 80% average | Sustained load |

---

## 6. Test Infrastructure

### 6.1 SoftRoCE (rxe) Setup

```bash
# Load rxe kernel module
sudo modprobe rdma_rxe

# Add rxe device on loopback or eth interface
sudo rdma link add rxe0 type rxe netdev lo

# Verify
rdma link show
ibv_devices
```

### 6.2 Docker Container for CI

```dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    libibverbs-dev \
    librdmacm-dev \
    rdma-core \
    iproute2 \
    perftest \
    golang-1.21

# Load rxe module (requires --privileged)
RUN modprobe rdma_rxe && \
    rdma link add rxe0 type rxe netdev lo
```

```yaml
# CI job (GitHub Actions example)
integration-tests:
  runs-on: ubuntu-latest
  container:
    image: golinker-test:latest
    options: --privileged
  steps:
    - run: modprobe rdma_rxe && rdma link add rxe0 type rxe netdev lo
    - run: go test -tags integration -race -timeout 300s ./...
```

### 6.3 Test Fixtures

```go
// testutil/fixture.go

// SetupServerClient creates a connected server-client pair for integration tests.
// Cleans up on t.Cleanup().
func SetupServerClient(t *testing.T) (server *server.Server, client *connection.Connection) { ... }

// SetupConnectionPool creates a pool of N connections to a test server.
func SetupConnectionPool(t *testing.T, n int) *connection.Pool { ... }

// MockClock provides a controllable time source for heartbeat/expiry tests.
type MockClock struct { ... }
```

### 6.4 Mocking Strategy

All RDMA operations are accessed through interfaces, enabling unit tests without hardware:

```go
// internal/rdma/verbs.go
type Verbs interface {
    RegMR(pd *PD, addr uintptr, length int, access int) (*MR, error)
    DeregMR(mr *MR) error
    PostSend(qp *QP, wr *SendWR) error
    PostRecv(qp *QP, wr *RecvWR) error
    PollCQ(cq *CQ, maxEntries int) ([]WorkCompletion, error)
    CreateQP(pd *PD, init *QPInit) (*QP, error)
    DestroyQP(qp *QP) error
}

// internal/rdma/cm.go
type CM interface {
    CreateID(ch *EventChannel, portSpace int) (*CMID, error)
    ResolveAddr(id *CMID, src, dst *net.TCPAddr, timeout time.Duration) error
    ResolveRoute(id *CMID, timeout time.Duration) error
    Connect(id *CMID, params *ConnParam) error
    Listen(id *CMID, backlog int) error
    GetEvent(ch *EventChannel) (*CMEvent, error)
    AckEvent(event *CMEvent) error
    Disconnect(id *CMID) error
}
```

Mock implementations generated with `mockgen` or hand-written:

```go
// internal/rdma/mock_verbs.go
type MockVerbs struct {
    RegMRFunc   func(...) (*MR, error)
    PostSendFunc func(...) error
    // ...
}
```

### 6.5 Build Tags

```go
//go:build integration

package integration_test
// Tests requiring SoftRoCE / real RDMA hardware
```

```go
//go:build !integration

package buffer_test
// Unit tests that run without RDMA (mocked)
```

### 6.6 Running Tests

```bash
# Unit tests only (no RDMA required)
go test ./... -race -count=1

# Integration tests (requires rxe)
go test -tags integration ./test/integration/ -race -timeout 300s

# Performance benchmarks
go test -tags integration -bench=. -benchtime=10s ./test/bench/

# Fault injection tests
go test -tags integration -run TestFault ./test/fault/ -timeout 600s

# Coverage report
go test ./... -coverprofile=coverage.out -covermode=atomic
go tool cover -html=coverage.out -o coverage.html
```

---

## 7. Coverage Goals

### Target Coverage by Package

| Package | Line Coverage Target | Branch Coverage Target | Notes |
|---------|---------------------|----------------------|-------|
| `pkg/buffer/` | 95% | 90% | Critical path — all alloc/free/threshold logic |
| `pkg/cq/` | 90% | 85% | Polling modes, batch logic |
| `pkg/connection/` | 90% | 85% | All 8 connect phases, close path |
| `pkg/message/` | 95% | 90% | Wire format must be perfect |
| `pkg/health/` | 90% | 85% | Timer-driven, use mock clock |
| `pkg/config/` | 95% | 90% | Validation logic fully covered |
| `pkg/server/` | 85% | 80% | Accept loop, lifecycle |
| `internal/rdma/` | 70% | 60% | CGo wrappers — limited to interface boundaries |

### Coverage Enforcement

- **Unit tests alone**: 90%+ line coverage across `pkg/` packages
- **Integration tests**: All happy paths exercised + critical error paths (connection failure, CQ error, buffer exhaustion)
- **Fault injection**: Every `error` return path in production code exercised at least once
- **CI gate**: Coverage drop > 2% from main branch fails the build

### Uncoverable Code

The following are excluded from coverage targets:
- CGo bridge code in `internal/rdma/` (tested via integration tests)
- Platform-specific NUMA code (tested only on Linux with NUMA topology)
- Fatal/panic paths (tested via subprocess tests with `exec.Command`)

---

## Appendix: Test File Organization

```
golinker/
├── pkg/
│   ├── buffer/
│   │   ├── send_pool_test.go
│   │   ├── recv_pool_test.go
│   │   ├── large_buffer_test.go
│   │   └── numa_test.go
│   ├── cq/
│   │   ├── poll_modes_test.go
│   │   ├── batch_test.go
│   │   ├── error_test.go
│   │   └── pool_test.go
│   ├── connection/
│   │   ├── connect_test.go
│   │   ├── cm_events_test.go
│   │   ├── pool_test.go
│   │   └── close_test.go
│   ├── message/
│   │   ├── aggregation_test.go
│   │   ├── wire_format_test.go
│   │   └── edge_cases_test.go
│   ├── health/
│   │   ├── heartbeat_test.go
│   │   └── buffer_monitor_test.go
│   ├── config/
│   │   └── config_test.go
│   └── server/
│       └── server_test.go
├── test/
│   ├── integration/
│   │   ├── lifecycle_test.go
│   │   ├── large_message_test.go
│   │   ├── concurrent_test.go
│   │   └── shutdown_test.go
│   ├── fault/
│   │   ├── network_test.go
│   │   ├── cq_error_test.go
│   │   ├── buffer_test.go
│   │   └── device_test.go
│   ├── bench/
│   │   ├── latency_test.go
│   │   ├── throughput_test.go
│   │   └── micro_test.go
│   └── testutil/
│       ├── fixture.go
│       ├── mock_clock.go
│       └── rxe_setup.go
└── docs/
    └── test_plan.md   ← this file
```
