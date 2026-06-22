# golinker Execution Plan v2: Completing the Design

## Overview

This plan covers the implementation of all design features documented in
`design.md` that were missed or left unwired during Phases 1-4. It addresses
the structural issues identified in `railyard_retro.md` by:

1. Introducing **Flow tickets** for cross-cutting data path wiring
2. Introducing **Verify tickets** for behavioral acceptance tests
3. Introducing **Conform tickets** for design conformance audits
4. Requiring **SoftRoCE integration tests** as phase exit gates
5. Providing a **design-to-ticket traceability matrix** (Section 10)

### Lanes

| Lane | Prefix | Purpose |
|------|--------|---------|
| Domain | `DOMAIN-1xx` | Feature implementation within a single package |
| Flow | `FLOW-0xx` | Cross-cutting data path wiring (touches multiple packages) |
| Verify | `VERIFY-0xx` | Integration tests, benchmark validation, hardware testing |
| Conform | `CONFORM-0xx` | Design conformance audits after each phase |

### Numbering

Tickets continue from the Phase 1-4 numbering. Domain tickets start at
DOMAIN-100 to avoid collision with the existing DOMAIN-001 through DOMAIN-009.

---

## 1. Phase 5: Wire Format + Data Path Wiring

**Goal**: Fix the wire format to match `design.md` §11, then wire the existing
`pkg/` modules into a working data path that matches `PingPongConn` behavior.

**Why sequential**: Flow tickets touch multiple packages and must see the final
state of all modules. Cannot parallelize safely.

**Exit gate**: `client.Send("hello") → server.Recv() == "hello"` over SoftRoCE.

### Tickets

#### DOMAIN-100: Wire Format Implementation

**Package**: `pkg/message/`

**Description**: Rewrite `wire.go` to implement the wire format from `design.md`
§11. The current implementation uses `[4B count][4B len][payload]` with
little-endian encoding. The design specifies:

- 12-byte command header: `type(4B uint32) + reserved(8B)`, values 230-235
- 12-byte app message header: `timestamp(8B uint64 BE) + msg_size(4B uint32 BE)`
- Aggregated layout: one command header followed by N app messages
- Big-endian encoding for app headers

**Files to modify**:
- `pkg/message/wire.go` — rewrite encoder/decoder
- `pkg/message/aggregator.go` — update to use new wire format
- `pkg/message/immediate.go` — update to use new wire format
- `pkg/message/aggregator_test.go` — update test expectations

**Acceptance criteria**:
- [ ] `Encode([]Message) → []byte` produces correct binary layout per §11
- [ ] `Decode([]byte) → []Message` round-trips correctly
- [ ] Command types: PostSend(230), ReadInvitation(231), ReadComplete(232),
      WriteRequest(233), WriteApprove(234), Heartbeat(235)
- [ ] Big-endian encoding for app headers
- [ ] `go test ./pkg/message/ -race` passes
- [ ] Fuzz test: random messages encode/decode without loss

**Design reference**: `design.md` §11.1, §11.2, §11.3

---

#### DOMAIN-101: Send/Recv Buffer Pool Wiring

**Package**: `pkg/buffer/`

**Description**: The buffer pool code exists but has never been connected to real
RDMA operations. Fix the pool to:

1. Allocate a single contiguous region per pool (128 x 12KB = 1.5MB)
2. Register the entire region as one MR (`ibv_reg_mr`)
3. Slice into 128 `Buffer` structs with correct LKey/RKey
4. `SendPool.AcquireForSend()` → returns buffer from free list
5. `SendPool.CompleteSend()` → returns buffer to free list
6. `RecvPool.PostRecvBuffers(qp, count)` → pre-posts recv WRs to QP
7. `RecvPool.Replenish(qp, buffers)` → re-posts consumed recv buffers

**Key design requirement**: The `is_busy_` flag. When free buffers drop below
50% of capacity, set an atomic boolean. Reset when all buffers are returned.
This flag drives aggregation mode switching (§5).

**Files to modify**:
- `pkg/buffer/pool.go` — single contiguous allocation + single MR
- `pkg/buffer/send_pool.go` — busy flag, CompleteSend wiring
- `pkg/buffer/recv_pool.go` — PostRecvBuffers via `golinker_post_recv_one()`
- `pkg/buffer/pool_test.go` — add tests for busy flag, MR lifecycle

**Acceptance criteria**:
- [ ] Single `ibv_reg_mr` per pool (not per buffer)
- [ ] `IsBusy()` returns true when <50% free, false when all returned
- [ ] `PostRecvBuffers()` calls `golinker_post_recv_one()` for each buffer
- [ ] `go test -tags mock ./pkg/buffer/ -race` passes
- [ ] Benchmark: alloc/free cycle <200ns

**Design reference**: `design.md` §7.1, §8.1, §8.3

---

#### FLOW-001: Wire Send Data Path

**Packages**: `pkg/buffer/`, `pkg/connection/`, `pkg/cq/`

**Description**: Wire the send data path end-to-end:

```
SendPool.AcquireForSend()
  → Copy message into buffer (with wire format headers)
  → ibv_post_send(buffer, IBV_SEND_SIGNALED)
  → CQ poll reaps send completion
  → SendPool.CompleteSend(buffer)
```

**Specific fixes required**:
1. `Connection.Send()` must call `SendPool.AcquireForSend()` to get a buffer
2. `Connection.Send()` must use `wire.Encode()` to write headers into buffer
3. `Connection.Send()` must set `IBV_SEND_SIGNALED` on every send
4. `Connection.Send()` must track in-flight sends (`ongoingSend` counter)
5. `CQPoller` must route send completions back to `SendPool.CompleteSend()`
6. Connection must register with CQPoller before first send

**Files to modify**:
- `pkg/connection/conn.go` — fix `Send()` to use pool + wire format + signaling
- `pkg/cq/dispatcher.go` — route send completions to pool
- `pkg/server/handler.go` — ensure CQ registration before sends

**Acceptance criteria**:
- [ ] Send path works end-to-end with real verbs (rxe)
- [ ] Send queue does not exhaust (completions reaped correctly)
- [ ] Buffer pool occupancy returns to 100% after all sends complete
- [ ] `go test -tags mock ./pkg/connection/ -race` passes

**Design reference**: `design.md` §7.1, §14.2 (Rule 2, Rule 4)

---

#### FLOW-002: Wire Recv Data Path

**Packages**: `pkg/buffer/`, `pkg/connection/`, `pkg/cq/`

**Description**: Wire the receive data path end-to-end:

```
RecvPool.PostRecvBuffers(qp, queueDepth)     (at connection setup)
  → NIC writes into pre-posted recv buffer
  → CQ poll reaps recv completion
  → Connection.DeliverRecv(buffer)
  → Application processes message
  → RecvPool.Replenish(qp, [buffer])         (re-post to NIC)
```

**Specific fixes required**:
1. `Connection.Accept()` and `Connection.Connect()` must call
   `RecvPool.PostRecvBuffers(qp, queueDepth)` before transitioning to
   `StateConnected`
2. `CQPoller` must route recv completions to `Connection.DeliverRecv()`
3. `Connection.Recv()` must block until `DeliverRecv()` delivers a buffer
4. After application reads the buffer, `Replenish()` re-posts it
5. Wire format: `wire.Decode()` parses the received buffer

**Files to modify**:
- `pkg/connection/conn.go` — add DeliverRecv(), call PostRecvBuffers at setup
- `pkg/connection/manager.go` — wire PostRecvBuffers into Accept/Connect
- `pkg/cq/dispatcher.go` — route recv completions to connection
- `pkg/buffer/recv_pool.go` — Replenish() calls golinker_post_recv_one()

**Acceptance criteria**:
- [ ] Recv buffers pre-posted before connection advertised as usable
- [ ] No RNR NAK (receiver has buffers ready when sender transmits)
- [ ] Buffer recycling works: recv → deliver → repost → recv again
- [ ] `go test -tags mock ./pkg/connection/ -race` passes

**Design reference**: `design.md` §7.1, §14.2 (Rule 1, Rule 3, Rule 4)

---

#### VERIFY-001: End-to-End Smoke Test (SoftRoCE)

**Description**: Integration test that validates the complete data path over
SoftRoCE (rxe kernel module). This is the Phase 5 exit gate.

**Test scenario**:
```
1. Server starts, listens on localhost:port
2. Client connects
3. Client sends "hello" (64 bytes with wire format headers)
4. Server receives, verifies content == "hello"
5. Server sends "world" back
6. Client receives, verifies content == "world"
7. Both sides close gracefully
```

**Files to create**:
- `test/integration/smoke_test.go` (build tag: `//go:build integration`)

**Acceptance criteria**:
- [ ] Test passes on SoftRoCE (`rxe` kernel module)
- [ ] Wire format headers present and correctly parsed
- [ ] No resource leaks (MRs deregistered, buffers freed, QPs destroyed)
- [ ] Test completes in <5 seconds

**Design reference**: `design.md` §14.5 (Rule 5)

---

#### CONFORM-001: Phase 5 Design Conformance Audit

**Description**: After all Phase 5 tickets are complete, the Architect performs a
design conformance check:

| Check | design.md § | Verify |
|-------|------------|--------|
| Wire format matches | §11.1, §11.2, §11.3 | Binary dump of encoded message matches spec |
| Send path complete | §7.1 | All 5 operations present in send flow |
| Recv path complete | §7.1 | All 5 operations present in recv flow |
| Buffer lifecycle closed-loop | §14.5 Rule 4 | No buffer leaks after 1000 send/recv cycles |
| Signaling correct | §14.5 Rule 2 | Every send has IBV_SEND_SIGNALED |
| Recv WRs pre-posted | §14.5 Rule 1 | PostRecvBuffers called before StateConnected |

**Acceptance criteria**:
- [ ] All checks pass
- [ ] Any discrepancies documented and ticketed for immediate fix

---

### Phase 5 Partition Strategy

```
DOMAIN-100 (wire format)     ─── can run in parallel ───  DOMAIN-101 (buffer pool)
         │                                                        │
         └──────────────── both complete ────────────────────────┘
                                    │
                              FLOW-001 (send path)
                                    │
                              FLOW-002 (recv path)
                                    │
                              VERIFY-001 (smoke test)
                                    │
                              CONFORM-001 (audit)
```

- DOMAIN-100 and DOMAIN-101 are **parallelizable** (different packages, no overlap)
- FLOW-001 and FLOW-002 are **sequential** after both domain tickets complete
- VERIFY-001 runs after both flow tickets
- CONFORM-001 runs last

**Estimated runners**: 2 parallel (Phase 5a) + 2 sequential (Phase 5b) = 4 total

---

## 2. Phase 6: Aggregation + CQ Modes + NUMA

**Goal**: Implement the three core performance optimizations from the design.

**Exit gate**: Benchmark shows aggregation activates under load; event-driven
CQ mode works; NUMA allocation on correct node.

### Tickets

#### DOMAIN-102: Message Aggregation Engine

**Package**: `pkg/message/`

**Description**: Implement adaptive message aggregation per `design.md` §5.
The existing `aggregator.go` has the basic structure but uses the wrong wire
format and has never been connected to real sends.

**Requirements**:
1. **Busy flag integration**: Read `SendPool.IsBusy()` to decide mode
2. **Immediate mode** (not busy): One message = one RDMA SEND
3. **Aggregate mode** (busy): Accumulate messages into linked list, flush on:
   - Threshold: `pendingSize + headerSize > SendThreshold` (default 9KB)
   - Overflow: Next message would exceed `BufferSize` (12KB)
   - Idle: `ongoingSends == 0` (no in-flight sends)
4. **Flush operation**: Acquire send buffer, write command header (type=230),
   pack N app messages (each with 12B app header), post send
5. **Wire format**: Use the corrected wire format from DOMAIN-100

**Files to modify**:
- `pkg/message/aggregator.go` — rewrite flush logic, integrate busy flag
- `pkg/message/immediate.go` — rewrite to use correct wire format
- `pkg/message/aggregator_test.go` — test all 3 flush triggers

**Acceptance criteria**:
- [ ] Under low load (IsBusy=false): messages sent immediately (1:1)
- [ ] Under high load (IsBusy=true): messages batched, flush on threshold/overflow/idle
- [ ] Idle trigger fires within ~5-10μs (no timer needed)
- [ ] Wire format headers correct in both modes
- [ ] `go test -tags mock ./pkg/message/ -race` passes

**Design reference**: `design.md` §5

---

#### DOMAIN-103: Event-Driven and Adaptive CQ Polling

**Package**: `pkg/cq/`

**Description**: Implement the remaining CQ polling modes from `design.md` §6.
Currently only busy-poll works.

**Modes to implement**:

| Mode | Algorithm | Latency | CPU |
|------|-----------|---------|-----|
| Busy (exists) | Tight `ibv_poll_cq` loop | ~2-5μs | 100% |
| Event (new) | `ibv_req_notify_cq` + Go netpoller on comp_channel FD | ~20-50μs | Near-zero idle |
| Smart (new) | Busy-poll N rounds after last completion; fall back to event if N empty polls | ~5-15μs | Proportional |

**Event mode implementation**:
1. Create completion channel (`ibv_create_comp_channel`)
2. Request notification (`ibv_req_notify_cq`)
3. Use Go's netpoller (`os.NewFile` + `poll.FD`) to wait on the channel FD
4. When notified: drain CQ via `ibv_poll_cq`, re-arm notification
5. Integrate with `context.Context` for cancellation

**Smart mode implementation**:
1. After receiving completions, busy-poll for up to `smartSpinCount` (default 5)
   empty rounds
2. If all empty: fall back to event mode (req_notify + netpoller wait)
3. On next completion: reset to busy-poll phase
4. This captures bursty traffic without wasting CPU when idle

**Files to modify**:
- `pkg/cq/poller.go` — implement Event and Smart modes
- `pkg/cq/poller_test.go` — test mode switching, latency characteristics

**Acceptance criteria**:
- [ ] Event mode: near-zero CPU when idle, wakes on completion
- [ ] Smart mode: transitions between busy and event based on traffic
- [ ] All modes: completions delivered correctly to handlers
- [ ] Config: `PollMode` selects mode at startup
- [ ] `go test -tags mock ./pkg/cq/ -race` passes

**Design reference**: `design.md` §6

---

#### DOMAIN-104: NUMA-Aware Memory Allocation

**Package**: `pkg/buffer/`

**Description**: Replace `C.malloc()` with `C.numa_alloc_onnode()` for buffer
pool allocation. Allocate on the NUMA node local to the RDMA NIC.

**Implementation**:
1. Auto-detect RDMA device's NUMA node:
   - Read `/sys/class/infiniband/<device>/device/numa_node`
   - Fallback to config `NUMANode` field (default 0)
2. Replace `C.malloc(size)` with `C.numa_alloc_onnode(size, node)`
3. Replace `C.free(ptr)` with `C.numa_free(ptr, size)`
4. Page-align allocations (`posix_memalign` or NUMA library alignment)
5. Keep `C.malloc` fallback when NUMA is unavailable (build tag or runtime check)

**Files to modify**:
- `pkg/buffer/alloc_cgo.go` — NUMA-aware allocator
- `pkg/buffer/alloc_mock.go` — mock stays with Go heap
- `pkg/buffer/pool.go` — pass NUMA node to allocator

**Acceptance criteria**:
- [ ] Buffers allocated on correct NUMA node (verify via `/proc/self/numa_maps`)
- [ ] Fallback to `C.malloc` when libnuma unavailable
- [ ] `go test -tags mock ./pkg/buffer/ -race` passes (mock uses Go heap)
- [ ] Benchmark: no regression in alloc/free cycle time

**Design reference**: `design.md` §8.2

---

#### FLOW-003: Wire Aggregation Into Send Path

**Packages**: `pkg/message/`, `pkg/connection/`, `pkg/buffer/`

**Description**: Connect the aggregation engine to the real send path:

```
Application → Connection.Send(msg)
  → if not busy: Immediate.Send(msg)     → AcquireForSend → wire.Encode → PostSend
  → if busy:     Aggregator.Enqueue(msg)  → flush triggers → AcquireForSend → wire.Encode(batch) → PostSend
```

**Files to modify**:
- `pkg/connection/conn.go` — route Send() through Aggregator or Immediate
- `pkg/message/aggregator.go` — call real PostSend on flush
- `pkg/message/immediate.go` — call real PostSend

**Acceptance criteria**:
- [ ] Low load: messages sent 1:1 (immediate mode)
- [ ] High load: messages batched (aggregation mode)
- [ ] Transition between modes driven by `SendPool.IsBusy()`
- [ ] Integration test: 1000 messages at high rate → aggregation activates

**Design reference**: `design.md` §5

---

#### VERIFY-002: Aggregation + CQ Mode Benchmark

**Description**: Use `golinker-bench` to validate performance characteristics.

**Benchmark scenarios**:
1. **Aggregation activation**: Send at increasing rate, verify batch size >1
   when throughput exceeds ~50% of buffer pool capacity
2. **CQ mode comparison**: Run same workload under busy/event/smart modes,
   verify latency and CPU usage match design expectations:
   - Busy: ~2-5μs p50, 100% CPU
   - Event: ~20-50μs p50, near-zero idle CPU
   - Smart: ~5-15μs p50, proportional CPU
3. **NUMA validation**: Compare throughput with NUMA-local vs NUMA-remote
   allocation (expect >10% difference on multi-socket)

**Files to modify**:
- `cmd/golinker-bench/client.go` — add aggregation stats output
- `cmd/golinker-bench/server.go` — add CQ mode selection flag

**Acceptance criteria**:
- [ ] Aggregation batch size >1 under high load
- [ ] CQ event mode CPU <5% when idle
- [ ] Smart mode latency between busy and event modes
- [ ] NUMA-local allocation shows measurable improvement

**Design reference**: `design.md` §5, §6, §8.2

---

#### CONFORM-002: Phase 6 Design Conformance Audit

| Check | design.md § | Verify |
|-------|------------|--------|
| Aggregation uses 3 flush triggers | §5 | Unit test per trigger |
| Busy flag threshold = 50% | §5 | IsBusy fires at 64/128 buffers |
| No timer-based flushing | §5 | Idle trigger only, no time.After |
| Event mode uses comp_channel | §6 | ibv_req_notify_cq called |
| Smart mode spin count | §6 | Configurable, default 5 |
| NUMA node auto-detected | §8.2 | Read from sysfs |

---

### Phase 6 Partition Strategy

```
DOMAIN-102 (aggregation) ── parallel ── DOMAIN-103 (CQ modes) ── parallel ── DOMAIN-104 (NUMA)
         │                                       │                                   │
         └────────────────── all complete ───────┴───────────────────────────────────┘
                                    │
                              FLOW-003 (wire aggregation)
                                    │
                              VERIFY-002 (benchmark)
                                    │
                              CONFORM-002 (audit)
```

- DOMAIN-102, DOMAIN-103, DOMAIN-104 are **fully parallelizable** (separate packages)
- FLOW-003 runs after DOMAIN-102 + Phase 5 flow tickets
- VERIFY-002 and CONFORM-002 run last

**Estimated runners**: 3 parallel + 2 sequential = 5 total

---

## 3. Phase 7: Large Message Protocol

**Goal**: Implement RDMA READ protocol for messages exceeding buffer size.

**Exit gate**: 1MB message transferred via RDMA READ; benchmark validates
throughput.

### Tickets

#### DOMAIN-105: Large Buffer Allocator

**Package**: `pkg/buffer/`

**Description**: Implement dynamic large-buffer allocation with global memory
cap per `design.md` §7.4.

**Requirements**:
1. `AllocLargeBuffer(pd, size)` → allocate page-aligned, NUMA-aware buffer
2. `ibv_reg_mr()` with `IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ`
3. Global atomic counter: `totalLargeBufMem`
4. Cap check: if `totalLargeBufMem + size > MaxLargeBufferCap` (default 1GB),
   return `ErrLargeBufferCapacityExceeded`
5. `FreeLargeBuffer()` → deregister MR, free memory, decrement counter
6. Large buffers are NOT pooled — each transfer allocates/frees its own

**Files to create**:
- `pkg/buffer/large_buffer.go` — allocator, global counter, cap enforcement
- `pkg/buffer/large_buffer_test.go` — tests for cap, concurrent alloc/free

**Acceptance criteria**:
- [ ] Allocations page-aligned and NUMA-aware (when available)
- [ ] Global cap enforced (concurrent allocations don't exceed limit)
- [ ] `ibv_reg_mr` called with correct access flags for RDMA READ
- [ ] `FreeLargeBuffer` deregisters MR and decrements counter
- [ ] `go test -tags mock ./pkg/buffer/ -race` passes

**Design reference**: `design.md` §7.4

---

#### DOMAIN-106: RDMA READ Protocol

**Packages**: `pkg/message/`, `pkg/connection/`

**Description**: Implement the large-message RDMA READ protocol from §7.2-7.3.

**Protocol flow**:
```
SENDER                                RECEIVER
1. Detect msg > BufferSize
2. AllocLargeBuffer(size)
3. ibv_reg_mr(LOCAL_WRITE|REMOTE_READ)
4. Copy payload into large buffer
5. Send ReadInvitation (cmd=231):
   {remote_addr(8B), size(4B), rkey(4B)}
   via normal RDMA SEND (12KB buffer)
                                      6. Receive ReadInvitation
                                      7. AllocLargeBuffer(size)
                                      8. ibv_reg_mr(LOCAL_WRITE)
                                      9. ibv_post_read(remote_addr,
                                                       local_buf, size, rkey)
                                      10. Poll IBV_WC_RDMA_READ completion
                                      11. Parse payload
                                      12. FreeLargeBuffer (target)
                                      13. Send ReadComplete (cmd=232):
                                          {addr(8B), success(1B)}
14. Receive ReadComplete
15. FreeLargeBuffer (source)
```

**New RDMA verb needed**: `ibv_post_read` — must be added to:
- `internal/rdma/verbs.go` — CGo wrapper
- `internal/rdma/hotpath.h` / `hotpath.c` — if batching needed (likely not for large msgs)
- `internal/rdma/types.go` — `IBV_WR_RDMA_READ` opcode constant

**Files to create/modify**:
- `pkg/message/large.go` — sender-side: detect, alloc, send invitation
- `pkg/message/large_recv.go` — receiver-side: receive invitation, post read, send complete
- `internal/rdma/verbs.go` — add PostRead wrapper
- `internal/rdma/types.go` — add RDMA READ opcode + access flags

**Acceptance criteria**:
- [ ] Messages >12KB routed to RDMA READ path
- [ ] Messages <=12KB still use RDMA SEND path
- [ ] Invitation uses standard 12KB buffer (does not block pool)
- [ ] Large buffers freed on both sides after completion
- [ ] Handles failure: receiver sends ReadComplete with success=false
- [ ] `go test -tags mock` passes (mock path simulates invitation/complete)

**Design reference**: `design.md` §7.2, §7.3, §7.4

---

#### FLOW-004: Wire Large Message Path

**Packages**: `pkg/message/`, `pkg/connection/`, `pkg/buffer/`, `pkg/cq/`

**Description**: Wire the RDMA READ protocol into the connection:

1. `Connection.Send()` checks message size against `BufferSize`
2. If large: route to `large.SendLarge()` → invitation → wait for completion
3. Receiver side: `CQPoller` routes `ReadInvitation` command to `large_recv`
4. `large_recv` performs `ibv_post_read` and polls for `IBV_WC_RDMA_READ`
5. On completion: parse payload, send `ReadComplete`, free buffers

**Files to modify**:
- `pkg/connection/conn.go` — size-based routing in Send()
- `pkg/cq/dispatcher.go` — handle IBV_WC_RDMA_READ completions

**Acceptance criteria**:
- [ ] Integration test: send 1MB message → received correctly via RDMA READ
- [ ] Small messages still use SEND path (no regression)
- [ ] No large buffer leaks after 100 transfers

**Design reference**: `design.md` §7.2

---

#### VERIFY-003: Large Message Benchmark

**Description**: Benchmark large-message throughput using `golinker-bench`.

**Scenarios**:
1. **Large message throughput**: Send 64KB, 256KB, 1MB, 4MB messages; measure
   throughput (GB/s)
2. **Mixed traffic**: 95% small (1KB), 5% large (256KB); verify small message
   latency not impacted (head-of-line blocking prevention)
3. **Large buffer cap**: Exceed cap with concurrent large transfers; verify
   graceful failure (ErrLargeBufferCapacityExceeded)

**Acceptance criteria**:
- [ ] 1MB messages: >2 GB/s throughput
- [ ] Mixed traffic: small message p99 latency <50μs (no HOL blocking)
- [ ] Cap enforcement: no OOM under concurrent large transfers

**Design reference**: `design.md` §7.3

---

#### CONFORM-003: Phase 7 Design Conformance Audit

| Check | design.md § | Verify |
|-------|------------|--------|
| Invitation format: {addr(8B), size(4B), rkey(4B)} | §11 cmd=231 | Binary dump matches |
| ReadComplete format: {addr(8B), success(1B)} | §11 cmd=232 | Binary dump matches |
| Large buffers NOT pooled | §7.4 | Each transfer allocs/frees |
| Access flags: REMOTE_READ on source MR | §7.2 | ibv_reg_mr call verified |
| Global cap from config MaxLargeBufferCap | §7.4 | Config wired to allocator |
| Size threshold = BufferSize (12KB) | §7.2 | Messages >12KB → READ path |

---

### Phase 7 Partition Strategy

```
DOMAIN-105 (large buffer alloc)  ── parallel ──  ibv_post_read verb addition
         │                                               │
         └──────────── both complete ────────────────────┘
                              │
                      DOMAIN-106 (RDMA READ protocol)
                              │
                      FLOW-004 (wire large message path)
                              │
                      VERIFY-003 (benchmark)
                              │
                      CONFORM-003 (audit)
```

- DOMAIN-105 and the verb addition are parallelizable
- DOMAIN-106 depends on DOMAIN-105 (needs large buffer allocator)
- Everything else is sequential

**Estimated runners**: 2 parallel + 3 sequential = 5 total

---

## 4. Phase 8: Self-Healing Infrastructure

**Goal**: Implement buffer monitor, heartbeat, and dynamic CQ resizing.

**Exit gate**: Stuck buffers recovered automatically; idle connections heartbeated;
CQ overflow handled without message loss.

### Tickets

#### DOMAIN-107: Buffer Monitor

**Package**: `pkg/health/`

**Description**: Implement the buffer monitor thread from `design.md` §10.2.
Runs as a goroutine, sweeps every `BufferMonitorCycle` (default 3s).

**Four monitoring actions per sweep**:

1. **Expired large buffer removal**: Scan large buffer map for entries older
   than `LargeBufferMaxLive` (5s). Free stuck buffers from failed transfers.
2. **Stuck send detection**: If `ongoingLargeSend > 0` for >10s with no
   progress, reset the counter to unblock the connection.
3. **Send pool recalibration**: Scan send pools for buffers occupied >60s.
   Force-return to free pool. Reset `is_busy_` if all buffers are free but
   flag is stuck.
4. **Busy counter validation**: If global `busyConnCount > 0` but no connections
   are actually busy, reset to zero.

**Files to create/modify**:
- `pkg/health/buffer_monitor.go` — monitor goroutine, 4 sweep actions
- `pkg/health/buffer_monitor_test.go` — test each sweep action in isolation

**Acceptance criteria**:
- [ ] Expired large buffers freed after LargeBufferMaxLive
- [ ] Stuck send counter reset after 10s stall
- [ ] Stuck send buffers force-returned after 60s
- [ ] Busy flag corrected when stuck
- [ ] Monitor goroutine stops on context cancellation
- [ ] `go test -tags mock ./pkg/health/ -race` passes

**Design reference**: `design.md` §10.2

---

#### DOMAIN-108: Heartbeat / Connection Liveness

**Package**: `pkg/health/`

**Description**: Implement heartbeat monitor from `design.md` §10.1.
Runs as a goroutine, checks every `HeartbeatInterval` (default 5s).

**Two actions per sweep**:

1. **Idle connection expiration**: If `idleDuration > ConnectionIdleExpire`
   (300s), close the connection.
2. **Heartbeat sending**: If initiator and
   `idleDuration > ConnectionIdleHeartbeat` (290s), send a heartbeat
   (cmd=235, type=request). Receiver responds with heartbeat (type=response).
   Both reset idle timers.

**Wire format**: Command header with type=235 (Heartbeat), payload: 1 byte
(0=request, 1=response).

**Files to create/modify**:
- `pkg/health/heartbeat.go` — monitor goroutine, idle tracking, heartbeat send/recv
- `pkg/health/heartbeat_test.go` — test idle expiration, heartbeat round-trip

**Acceptance criteria**:
- [ ] Idle connection closed after ConnectionIdleExpire seconds
- [ ] Heartbeat sent after ConnectionIdleHeartbeat seconds
- [ ] Heartbeat response resets idle timer on both sides
- [ ] Heartbeat uses wire format command type 235
- [ ] Monitor goroutine stops on context cancellation
- [ ] `go test -tags mock ./pkg/health/ -race` passes

**Design reference**: `design.md` §10.1

---

#### DOMAIN-109: Dynamic CQ Resizing

**Package**: `pkg/cq/`

**Description**: Handle CQ overflow by dynamically resizing. From §10.3:

1. Register for async events on the device context (`ibv_get_async_event`)
2. On `IBV_EVENT_CQ_ERR`: mark CQ as fatal
3. Create new CQ with 2x size (up to `MaxCQSize`, default 16384)
4. Migrate all connections from old CQ to new CQ
5. Destroy old CQ

**Files to create/modify**:
- `pkg/cq/pool.go` — add ResizeCQ() method, async event goroutine
- `pkg/cq/poller.go` — handle CQ fatal flag, transition to new CQ
- `internal/rdma/verbs.go` — add `ibv_get_async_event` wrapper (if missing)
- `pkg/cq/poller_test.go` — test CQ resize scenario

**Acceptance criteria**:
- [ ] CQ overflow triggers resize (4096 → 8192 → 16384)
- [ ] Connections migrated to new CQ without message loss
- [ ] Resize capped at MaxCQSize
- [ ] `go test -tags mock ./pkg/cq/ -race` passes

**Design reference**: `design.md` §10.3

---

#### VERIFY-004: Self-Healing Integration Test

**Description**: Integration tests for self-healing features.

**Test scenarios**:
1. **Buffer monitor**: Simulate stuck large buffer (hold for >5s), verify
   monitor frees it
2. **Heartbeat**: Open connection, wait 295s (or use short config), verify
   heartbeat exchanged. Wait 305s, verify connection closed.
3. **CQ resize**: (If possible on rxe) Fill CQ to capacity, verify resize
   triggers and no completions are lost

**Files to create**:
- `test/integration/health_test.go` (build tag: `//go:build integration`)

**Acceptance criteria**:
- [ ] Buffer monitor frees stuck buffers
- [ ] Heartbeat keeps connections alive
- [ ] Idle connections closed after expiry
- [ ] CQ resize recovers from overflow

**Design reference**: `design.md` §10

---

#### CONFORM-004: Phase 8 Design Conformance Audit

| Check | design.md § | Verify |
|-------|------------|--------|
| Buffer monitor: 4 sweep actions | §10.2 | All 4 actions in code |
| Buffer monitor cycle = config value | §10.2 | Uses BufferMonitorCycle |
| Heartbeat interval = config value | §10.1 | Uses HeartbeatInterval |
| Idle heartbeat at 290s, expire at 300s | §10.1 | 10s gap for RTT |
| Heartbeat wire format = cmd 235 | §11 | Matches spec |
| CQ resize doubles size | §10.3 | 4096 → 8192 → 16384 |
| CQ resize capped at MaxCQSize | §10.3 | Config wired |

---

### Phase 8 Partition Strategy

```
DOMAIN-107 (buffer monitor) ── parallel ── DOMAIN-108 (heartbeat) ── parallel ── DOMAIN-109 (CQ resize)
         │                                        │                                       │
         └──────────────── all complete ──────────┴───────────────────────────────────────┘
                                    │
                              VERIFY-004 (integration test)
                                    │
                              CONFORM-004 (audit)
```

- All 3 domain tickets are **fully parallelizable** (separate files, no overlap)
- VERIFY-004 and CONFORM-004 run after all domain tickets

**Estimated runners**: 3 parallel + 2 sequential = 5 total

---

## 5. Phase 9: Full System Integration + Performance Validation

**Goal**: Wire everything together, run full benchmark suite, validate against
design performance targets.

### Tickets

#### FLOW-005: Full System Composition

**Description**: Wire all Phase 5-8 components into a single server binary:

```go
func NewServer(cfg *config.Config) (*Server, error) {
    verbs := rdma.NewVerbs(cfg.DeviceName)
    pd, _ := verbs.AllocPD()
    numaNode := detectNUMANode(cfg)

    bufPool := buffer.NewPool(pd, cfg.Buffer, numaNode)
    sendPool := buffer.NewSendPool(bufPool)
    recvPool := buffer.NewRecvPool(bufPool)

    cqPool := cq.NewPool(verbs, cfg.CQ)

    connMgr := connection.NewManager(connection.Deps{
        Verbs: verbs, PD: pd,
        SendPool: sendPool, RecvPool: recvPool,
        CQPool: cqPool, Config: cfg.Connection,
    })

    aggregator := message.NewAggregator(cfg.Message, sendPool)
    heartbeat := health.NewHeartbeat(cfg.Health, connMgr)
    bufMonitor := health.NewBufferMonitor(cfg.Health, sendPool, bufPool)

    return &Server{...}, nil
}
```

**Acceptance criteria**:
- [ ] Server binary starts with all subsystems active
- [ ] Client binary connects and exchanges messages
- [ ] golinker-bench runs latency and throughput scenarios
- [ ] Graceful shutdown: heartbeat stops, buffers drained, CQs destroyed

**Design reference**: `design.md` §3 (Architecture), `execution_plan.md` §5

---

#### VERIFY-005: Full Performance Validation

**Description**: Run the complete `golinker-bench` suite against design targets.

| Metric | Target (design.md §13) | Measurement |
|--------|----------------------|-------------|
| Small msg latency (64B) | <5μs p50 | `golinker-bench --scenario latency --size 64` |
| Small msg latency (1KB) | <8μs p50 | `golinker-bench --scenario latency --size 1024` |
| Throughput (64B) | >5M msgs/sec | `golinker-bench --scenario throughput --size 64` |
| Throughput (1KB) | >2M msgs/sec | `golinker-bench --scenario throughput --size 1024` |
| Large msg bandwidth (1MB) | >2 GB/s | `golinker-bench --scenario bandwidth --size 1048576` |
| Buffer alloc/free | <200ns | `golinker-bench --scenario micro --sub buffer-pool` |
| CQ poll batch (32 WCs) | Single CGo call | Code inspection |
| Memory overhead vs C++ | <20% more | RSS comparison |

**Acceptance criteria**:
- [ ] All targets met on real RDMA hardware (ConnectX-5/6)
- [ ] Results documented in `docs/benchmark_results.md`
- [ ] If targets not met: profile, identify bottleneck, create fix ticket

**Design reference**: `design.md` §13

---

#### CONFORM-005: Final Design Conformance Audit

**Description**: Complete design-to-implementation audit. Walk every section of
`design.md` and verify the implementation matches.

| design.md § | Feature | Check |
|------------|---------|-------|
| §1 | Problem statement | N/A (context only) |
| §2 | Design principles | Verify: minimal C (~300 LOC), context-native, wire compatible |
| §3 | Architecture | Package structure matches diagram |
| §4 | Hot-path CGo | Three C functions exist and are used |
| §5 | Aggregation | 3 flush triggers, busy flag at 50%, no timers |
| §6 | CQ polling | 4 modes work, smart mode spin count configurable |
| §7.1 | Small message send/recv | Buffer pool wired, zero per-msg allocation |
| §7.2-7.4 | Large message RDMA READ | Full protocol works, buffers freed |
| §8.1 | Off-heap allocation | All RDMA buffers via C.malloc/numa_alloc |
| §8.2 | NUMA-aware placement | Buffers on NIC-local node |
| §8.3 | Single-MR-per-pool | One ibv_reg_mr per pool, not per buffer |
| §9 | Connection lifecycle | 8-phase handshake, goroutine-per-connection |
| §10.1 | Heartbeat | 290s heartbeat, 300s expire, cmd=235 |
| §10.2 | Buffer monitor | 4 sweep actions, 3s cycle |
| §10.3 | Dynamic CQ resize | Double on overflow, capped at MaxCQSize |
| §11 | Wire format | 12B cmd + 12B app, BE, values 230-235 |
| §12 | Tradeoff analysis | N/A (analysis only) |
| §13 | Performance targets | Benchmark results match targets |
| §14 | Data path rules | All 5 rules satisfied |
| Appendix A | Config reference | All config fields wired and used |

---

## 6. Testing Strategy

### 6.1 Unit Tests (Mock Mode)

Every domain ticket includes mock-mode unit tests. These verify internal logic
without RDMA hardware.

| Package | Test Focus | Build Tag |
|---------|-----------|-----------|
| `pkg/message/` | Wire format encode/decode, aggregation flush triggers | `mock` |
| `pkg/buffer/` | Pool alloc/free, busy flag, large buffer cap | `mock` |
| `pkg/cq/` | Mode switching, completion dispatch | `mock` |
| `pkg/connection/` | State machine, send/recv wiring | `mock` |
| `pkg/health/` | Monitor sweep actions, heartbeat timing | `mock` |

**Command**: `go test -tags mock -race ./...`

### 6.2 Integration Tests (SoftRoCE)

Flow tickets and verify tickets include integration tests that run over SoftRoCE
(rxe kernel module).

| Test | What It Validates | Build Tag |
|------|------------------|-----------|
| Smoke test | Send "hello" → recv "hello" | `integration` |
| Aggregation test | Batch size >1 under load | `integration` |
| Large message test | 1MB via RDMA READ | `integration` |
| Heartbeat test | Connection kept alive / expired | `integration` |
| Buffer monitor test | Stuck buffers recovered | `integration` |
| CQ resize test | Overflow handled | `integration` |

**Command**: `go test -tags integration -v ./test/integration/`

**SoftRoCE setup**:
```bash
modprobe rdma_rxe
rdma link add rxe0 type rxe netdev eth0
```

### 6.3 Hardware Benchmarks

Verify tickets include `golinker-bench` runs on real RDMA hardware.

| Scenario | Hardware Required | Metrics |
|----------|------------------|---------|
| Latency (64B, 1KB) | 2 nodes, ConnectX-5+ | p50, p99, p99.9 |
| Throughput (64B, 1KB) | 2 nodes, ConnectX-5+ | msgs/sec |
| Bandwidth (1MB) | 2 nodes, ConnectX-5+ | GB/s |
| Mixed traffic | 2 nodes, ConnectX-5+ | small-msg p99 under large-msg load |

**Command**: `golinker-bench --mode client --addr <server>:<port> --scenario <name>`

### 6.4 Design Conformance Audits

After each phase, a CONFORM ticket audits the implementation against `design.md`.
The audit is a checklist (see each CONFORM ticket above). Any discrepancy is
documented and ticketed for immediate fix before the next phase begins.

**Process**:
1. Architect reads relevant `design.md` sections
2. Architect reads implemented code
3. For each design point: does the code match? (binary diff for wire format,
   code review for algorithms, config inspection for parameters)
4. Discrepancies → new tickets (DOMAIN-1xx or FLOW-0xx)
5. Fix tickets resolved before next phase begins

---

## 7. Runner Context Improvements

Based on the retrospective, runner prompts now include:

### 7.1 Flow Participation Context

Each runner prompt includes the end-to-end flow their code participates in:

> "Your code participates in the **send data path**:
> `SendPool.AcquireForSend() → wire.Encode(headers) → ibv_post_send(SIGNALED) → CQ poll → SendPool.CompleteSend()`
>
> Your implementation of `Connection.Send()` must:
> 1. Acquire a buffer from SendPool (not accept nil Buffer)
> 2. Encode wire format headers per §11
> 3. Set IBV_SEND_SIGNALED on every send
> 4. Increment ongoingSend counter"

### 7.2 RDMA Semantic Requirements

Each runner prompt includes the relevant RDMA semantics:

> "RDMA requires these 5 operations before data can flow:
> 1. Buffer allocation in stable memory (not Go heap)
> 2. Memory registration (ibv_reg_mr)
> 3. Recv WR posting (ibv_post_recv) — BEFORE any sends
> 4. Send signaling (IBV_SEND_SIGNALED)
> 5. CQ polling (ibv_poll_cq)
>
> Your code is responsible for operations #3 and #5. Verify they happen."

### 7.3 Wire Format Specification

Each runner prompt includes the exact wire format (not a simplified version):

> "Wire format (design.md §11):
> - Command header: 12 bytes — type(4B uint32) + reserved(8B zeros)
> - App message header: 12 bytes — timestamp(8B uint64 BE) + size(4B uint32 BE)
> - PostSend command (type=230): one command header + N app messages
> - Big-endian encoding for all multi-byte fields in app header
>
> Do NOT use little-endian. Do NOT use varint encoding."

---

## 8. Timeline Estimate

| Phase | Tickets | Parallel Runners | Sequential Steps | Est. Duration |
|-------|---------|-----------------|-----------------|---------------|
| Phase 5 | 6 | 2 (DOMAIN-100, 101) | 4 (FLOW-001, 002, VERIFY-001, CONFORM-001) | ~1 week |
| Phase 6 | 6 | 3 (DOMAIN-102, 103, 104) | 3 (FLOW-003, VERIFY-002, CONFORM-002) | ~1 week |
| Phase 7 | 6 | 2 (DOMAIN-105 + verbs) | 4 (DOMAIN-106, FLOW-004, VERIFY-003, CONFORM-003) | ~1.5 weeks |
| Phase 8 | 5 | 3 (DOMAIN-107, 108, 109) | 2 (VERIFY-004, CONFORM-004) | ~1 week |
| Phase 9 | 3 | 0 | 3 (FLOW-005, VERIFY-005, CONFORM-005) | ~1 week |
| **Total** | **26** | **10 parallel** | **16 sequential** | **~5.5 weeks** |

---

## 9. Ticket Summary

### All Tickets by Phase

| Phase | Ticket | Type | Title | Parallel? |
|-------|--------|------|-------|-----------|
| 5 | DOMAIN-100 | Domain | Wire format implementation (§11) | Yes (with 101) |
| 5 | DOMAIN-101 | Domain | Send/recv buffer pool wiring (§7.1, §8) | Yes (with 100) |
| 5 | FLOW-001 | Flow | Wire send data path | No |
| 5 | FLOW-002 | Flow | Wire recv data path | No |
| 5 | VERIFY-001 | Verify | End-to-end smoke test (SoftRoCE) | No |
| 5 | CONFORM-001 | Conform | Phase 5 design conformance audit | No |
| 6 | DOMAIN-102 | Domain | Message aggregation engine (§5) | Yes (with 103, 104) |
| 6 | DOMAIN-103 | Domain | Event-driven + adaptive CQ polling (§6) | Yes (with 102, 104) |
| 6 | DOMAIN-104 | Domain | NUMA-aware memory allocation (§8.2) | Yes (with 102, 103) |
| 6 | FLOW-003 | Flow | Wire aggregation into send path | No |
| 6 | VERIFY-002 | Verify | Aggregation + CQ mode benchmark | No |
| 6 | CONFORM-002 | Conform | Phase 6 design conformance audit | No |
| 7 | DOMAIN-105 | Domain | Large buffer allocator (§7.4) | Yes (with verb add) |
| 7 | DOMAIN-106 | Domain | RDMA READ protocol (§7.2-7.3) | No (needs 105) |
| 7 | FLOW-004 | Flow | Wire large message path | No |
| 7 | VERIFY-003 | Verify | Large message benchmark | No |
| 7 | CONFORM-003 | Conform | Phase 7 design conformance audit | No |
| 8 | DOMAIN-107 | Domain | Buffer monitor (§10.2) | Yes (with 108, 109) |
| 8 | DOMAIN-108 | Domain | Heartbeat / connection liveness (§10.1) | Yes (with 107, 109) |
| 8 | DOMAIN-109 | Domain | Dynamic CQ resizing (§10.3) | Yes (with 107, 108) |
| 8 | VERIFY-004 | Verify | Self-healing integration test | No |
| 8 | CONFORM-004 | Conform | Phase 8 design conformance audit | No |
| 9 | FLOW-005 | Flow | Full system composition | No |
| 9 | VERIFY-005 | Verify | Full performance validation | No |
| 9 | CONFORM-005 | Conform | Final design conformance audit | No |

### Ticket Counts by Lane

| Lane | Count | Purpose |
|------|-------|---------|
| Domain | 10 | Feature implementation |
| Flow | 5 | Cross-cutting data path wiring |
| Verify | 5 | Integration tests + benchmarks |
| Conform | 5 | Design conformance audits |
| **Total** | **26** | |

---

## 10. Design-to-Ticket Traceability Matrix

This matrix maps every section of `design.md` to the ticket(s) responsible for
its implementation, and tracks completion status.

| design.md § | Section Title | Ticket(s) | Phase | Status |
|------------|---------------|-----------|-------|--------|
| §1 | The Problem | — | — | N/A (context) |
| §2 | Design Principles | CONFORM-005 | 9 | Pending |
| §3 | Architecture | FLOW-005 | 9 | Pending |
| §4.1 | CGo Hot-Path: poll_and_repost | SYSTEM-003 | 1 | **Done** (hotpath.c) |
| §4.2 | CGo Hot-Path: batch_post_send | SYSTEM-003 | 1 | **Done** (hotpath.c) |
| §4.3 | CGo Hot-Path: post_send | SYSTEM-003 | 1 | **Done** (hotpath.c) |
| §5.1 | Aggregation: busy flag | DOMAIN-102 | 6 | **Done** (send_pool.go, aggregator.go) |
| §5.2 | Aggregation: threshold trigger | DOMAIN-102 | 6 | **Done** (aggregator.go) |
| §5.3 | Aggregation: overflow trigger | DOMAIN-102 | 6 | **Done** (aggregator.go) |
| §5.4 | Aggregation: idle trigger | DOMAIN-102 | 6 | **Done** (aggregator.go) |
| §5.5 | Aggregation: no timer | DOMAIN-102, CONFORM-002 | 6 | **Done** (no time.After in flush path) |
| §6.1 | CQ Polling: busy mode | DOMAIN-006 | 2 | **Done** (poller.go) |
| §6.2 | CQ Polling: event mode | DOMAIN-103 | 6 | **Done** (poller.go eventWaiterLoop) |
| §6.3 | CQ Polling: smart mode | DOMAIN-103 | 6 | **Done** (poller.go smartWaiterLoop) |
| §6.4 | CQ Polling: user mode | DOMAIN-103 | 6 | **Done** (poller.go PollModeUser) |
| §6.5 | CQ Pool: round-robin | DOMAIN-006 | 2 | **Done** (pool.go) |
| §7.1 | Small msg: send path | FLOW-001 | 5 | **Done** (mock-validated; not yet on real verbs) |
| §7.1 | Small msg: recv path | FLOW-002 | 5 | **Done** (mock-validated; not yet on real verbs) |
| §7.1 | Small msg: buffer pools | DOMAIN-101 | 5 | **Done** (send_pool.go, recv_pool.go) |
| §7.2 | Large msg: RDMA READ protocol | DOMAIN-106 | 7 | Pending |
| §7.3 | Large msg: HOL blocking prevention | DOMAIN-106, VERIFY-003 | 7 | Pending |
| §7.4 | Large msg: buffer lifecycle | DOMAIN-105 | 7 | Pending |
| §8.1 | Memory: off-heap allocation | DOMAIN-005 | 2 | **Done** (alloc_cgo.go) |
| §8.2 | Memory: NUMA-aware placement | DOMAIN-104 | 6 | **Done** (alloc_numa_cgo.go, util.go) |
| §8.3 | Memory: single-MR-per-pool | DOMAIN-101 | 5 | **Done** (pool.go) |
| §9.1 | Connection: goroutine-per-connection | DOMAIN-007 | 2 | **Done** (conn.go) |
| §9.2 | Connection: 8-phase handshake | Manual fix | Post-4 | **Done** (real_cm.go) |
| §9.3 | Connection: server accept | Manual fix | Post-4 | **Done** (listener.go) |
| §9.4 | Connection: pool (sync.Map) | DOMAIN-007 | 2 | **Done** (manager.go) |
| §10.1 | Health: heartbeat | DOMAIN-108 | 8 | Pending |
| §10.2 | Health: buffer monitor | DOMAIN-107 | 8 | Pending |
| §10.3 | Health: dynamic CQ resize | DOMAIN-109 | 8 | Pending |
| §11.1 | Wire: command header (12B) | DOMAIN-100 | 5 | **Done** (wire.go) |
| §11.2 | Wire: app message header (12B BE) | DOMAIN-100 | 5 | **Done** (wire.go) |
| §11.3 | Wire: aggregated layout | DOMAIN-100 | 5 | **Done** (wire.go) |
| §12 | Tradeoff Analysis | — | — | N/A (analysis) |
| §13 | Performance Targets | VERIFY-005 | 9 | Pending |
| §14.1 | Data path: cross-cutting flow | FLOW-001, FLOW-002 | 5 | **Done** (mock-validated) |
| §14.2 | Data path: 5 required operations | VERIFY-001 | 5 | **Done** (mock-validated) |
| §14.5 | Data path: Rule 1 (recv before usable) | FLOW-002 | 5 | **Done** (mock-validated) |
| §14.5 | Data path: Rule 2 (send signaling) | FLOW-001 | 5 | **Done** (mock-validated) |
| §14.5 | Data path: Rule 3 (CQ before verbs) | FLOW-001, FLOW-002 | 5 | **Done** (mock-validated) |
| §14.5 | Data path: Rule 4 (closed-loop buffers) | FLOW-001, FLOW-002 | 5 | **Done** (mock-validated) |
| §14.5 | Data path: Rule 5 (test with real verbs) | VERIFY-001 | 5 | **Pending** (SoftRoCE/real-verbs run not yet performed) |
| App. A | Config: all fields wired | CONFORM-005 | 9 | Pending |

### Traceability Statistics

*(Recounted 2026-06-22 from the matrix rows above; the previous "43 total" figure
was a miscount — there are 45 tracked rows.)*

| Status | Count | Percentage |
|--------|-------|------------|
| **Done** (Phases 1-6) | 32 | 71% |
| **Pending** (Phases 7-9 + real-verbs validation) | 11 | 24% |
| **N/A** (context/analysis only) | 2 | 4% |
| **Total design points** | 45 | 100% |

> **Validation caveat:** Phase 5 (wire format, buffer pools, send/recv data paths)
> and Phase 6 (aggregation, event/smart CQ, NUMA) are marked Done on the basis of
> mock-mode unit/integration tests passing. No SoftRoCE or real-RDMA run has yet
> exercised the data path (§14.5 Rule 5 remains Pending), so these are
> "implemented + mock-validated," not "hardware-proven."

### Coverage by Phase

| Phase | Design Points Covered | Status | Key Features |
|-------|----------------------|--------|--------------|
| 1-4 | 10 | Done | Hot-path C, CGo wrappers, CQ busy-poll, off-heap alloc, connection lifecycle |
| 5 | 13 | Done (mock) | Wire format, buffer pools, send/recv data paths, RDMA rules 1-4 |
| 6 | 9 | Done (mock) | Aggregation (3 triggers + busy flag), event/smart/user CQ modes, NUMA |
| 7 | 3 | Pending | RDMA READ protocol, large buffers, HOL prevention |
| 8 | 3 | Pending | Heartbeat, buffer monitor, dynamic CQ resize |
| 9 | 5 | Pending | Full composition, performance targets, config wiring, final audit, real-verbs validation |

---

## 11. Design Conformance Process

To prevent future design drift, the following process applies to all phases:

### 11.1 Pre-Phase: Ticket Generation Audit

Before any phase begins, the Architect:
1. Re-reads all relevant `design.md` sections
2. Verifies every design point has a ticket (using the traceability matrix)
3. Identifies any gaps and creates additional tickets
4. Reviews runner context packets for design-accurate specifications

### 11.2 Runner Execution: Design References

Every runner prompt includes:
- Exact `design.md` section references (e.g., "implement §5.2")
- Verbatim specifications (wire format bytes, algorithm pseudocode)
- Explicitly called-out anti-patterns ("Do NOT use LE encoding")
- Cross-module flow context ("Your code participates in the send data path...")

### 11.3 Post-Phase: Conformance Audit

After each phase:
1. CONFORM ticket executed (see CONFORM-001 through CONFORM-005)
2. Each checklist item verified by reading code, not just running tests
3. Discrepancies documented with:
   - Which design.md § was violated
   - What the code does vs what the design says
   - Severity: Critical (data path broken) / Major (semantics wrong) / Minor (cosmetic)
4. Critical/Major discrepancies → new tickets, resolved before next phase
5. Traceability matrix updated: design point status changes from Pending to Done

### 11.4 Continuous: Traceability Matrix Maintenance

The traceability matrix (Section 10) is the living document. After each ticket
completion:
1. Update status from Pending to Done
2. Add commit hash for traceability
3. Add any new design points discovered during implementation
4. Flag any design points that were intentionally deviated from (with rationale)

---

## Appendix: Comparison with Phases 1-4 Execution Plan

| Aspect | v1 (Phases 1-4) | v2 (Phases 5-9) |
|--------|-----------------|-----------------|
| Ticket lanes | 2 (System, Domain) | 4 (Domain, Flow, Verify, Conform) |
| Cross-cutting tickets | 0 | 5 (Flow lane) |
| Integration tests | 0 | 5 (Verify lane) |
| Design conformance audits | 0 | 5 (Conform lane) |
| Behavioral acceptance criteria | No | Yes (data flows end-to-end) |
| Wire format in runner context | Simplified (LE) | Exact (BE, per §11) |
| RDMA semantic context in prompt | No | Yes (5 required operations) |
| Flow participation in prompt | No | Yes (end-to-end path described) |
| Traceability matrix | No | Yes (43 design points tracked) |
| SoftRoCE testing | No | Phase exit gate |
| Hardware benchmarking | No | VERIFY-005 |
