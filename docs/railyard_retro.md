# Railyard Retrospective: golinker Phases 1-4

## 1. Executive Summary

Railyard dispatched 17 runner sub-agents across 4 phases, producing ~8,098 LOC,
65 files, and 52 passing tests with zero merge conflicts. By every metric the
framework tracks — compilation, test pass rate, race-detector cleanliness — the
project was a success.

Yet when it came time to send a single byte over RDMA, **nothing worked.**

The design document (Section 14.3) admits this directly:

> Nobody was assigned the cross-cutting wiring that connects these modules into
> a working data path.

A `PingPongConn` bypass had to be built from scratch to prove data could flow
at all. Five independently correct packages (`pkg/buffer/`, `pkg/cq/`,
`pkg/connection/`, `pkg/message/`, `pkg/server/`) had never been wired together
and still have not been.

This retrospective analyzes the root causes, catalogs every design point that
was missed or diverged, and proposes structural changes to prevent recurrence.

---

## 2. What Went Well

| Aspect | Evidence |
|--------|----------|
| Parallel execution | 3-4 runners per phase, zero merge conflicts |
| Interface isolation | Frozen `api/` contracts enabled independent work |
| Mock testing | 52 tests, all PASS, `-race` clean across 6 packages |
| Phase gating | Architect verified build after each phase before committing |
| Context handover | Self-contained runner prompts with exact interface contracts |
| Build system | Mock/real build-tag split works cleanly |
| Hot-path C code | `hotpath.c` (202 lines) implements correct batched verbs |
| CM lifecycle | `real_cm.go` correctly handles Listen/Accept/Dial |
| PingPongConn | Self-contained data path validates all 5 RDMA operations |

Railyard excels at **constructing independent modules in parallel**. The
framework's file-level isolation rule and frozen interface contracts are
genuinely effective at eliminating merge conflicts and enabling concurrent work.

---

## 3. What Went Wrong

### 3.1 Package-Boundary Decomposition Missed Cross-Cutting Flows

Railyard's core principle — "no two runners ever edit the same file" — is a
conflict-avoidance strategy, not a correctness strategy. The RDMA data path is
inherently cross-cutting:

```
buffer.AcquireForSend()
  → connection.PostSend()
    → cq.PollCompletion()
      → buffer.CompleteSend()
```

This flow touches 3 packages in sequence. No single runner owned it.

The execution plan assigned:
- Agent 2A → `pkg/buffer/` (implements `PostRecvBuffers()` but never calls it)
- Agent 2B → `pkg/cq/` (implements `OnCompletion()` but no one registers)
- Agent 2C → `pkg/connection/` (implements `Send()` but without `IBV_SEND_SIGNALED`)

Each runner's acceptance criteria was "my tests pass." All tests passed. None
tested the cross-module contract.

### 3.2 Mock Testing Created a False Sense of Completeness

Every runner used mock dependencies:
- `PostSend()` → no-op in mock
- `PostRecv()` → no-op in mock
- `PollCQ()` → returns whatever the test injects

Mock tests proved **internal logic** was correct. They could not prove **data
would flow**. The 5 RDMA-critical operations (buffer alloc, MR registration,
recv WR posting, send signaling, CQ polling) were all no-ops in mock mode.

### 3.3 Phase 3 "Integration" Integrated Lifecycle, Not Data Path

The execution plan called Phase 3 the "Integration Layer." But it integrated:
- Message aggregation (batching logic — correct but disconnected from real sends)
- Server lifecycle (accept/stop — correct but no recv buffers posted)
- CLI binaries (flag parsing — no actual RDMA operations)

The word "integration" was misleading. It meant "compose the lifecycle," not
"wire the data path."

### 3.4 The Architect Never Tested End-to-End

After each phase, the Architect verified:
- `go build -tags mock ./...` — compilation
- `go test -tags mock -race ./...` — unit tests
- No merge conflicts

The Architect never ran an integration test that sent a message from client to
server, even over SoftRoCE/rxe. Acceptance criteria were **structural** (builds,
tests pass) not **behavioral** (data flows).

### 3.5 Tickets Were Derived From Package Layout, Not Design Sections

The execution plan derives tickets from the dependency DAG of Go packages. This
approach misses features that span packages or exist only as protocol-level
requirements.

Example: `design.md` Section 7.2 (RDMA READ for Large Messages) describes a
complete protocol with invitation messages, dynamic MR registration, and
completion notifications. **No ticket was ever created for it.** The feature
simply fell through the cracks because it doesn't map to a single package.

### 3.6 Wire Format Diverged From Design

`pkg/message/wire.go` implements a `[4-byte count][4-byte len][payload]` format
with little-endian encoding. The design doc (Section 11) specifies:
- 12-byte command header: `type(4B) + reserved(8B)`
- 12-byte app message header: `timestamp(8B BE) + size(4B BE)`
- Big-endian encoding

The runner was given a simplified wire format spec instead of the actual design.
No conformance check caught the divergence.

---

## 4. Root Cause: Structural Blind Spot

Railyard decomposes by package. RDMA correctness decomposes by data flow.

```
Railyard sees this:           RDMA needs this:
┌─────────────┐               ┌────────────────────────────────────┐
│ pkg/buffer/  │               │ alloc → reg_mr → post_recv →       │
├─────────────┤               │ poll_cq → deliver → repost         │
│ pkg/cq/     │  (5 boxes)   │                                    │
├─────────────┤               │ acquire → copy → post_send →       │
│ pkg/connect/ │               │ poll_cq → complete → release       │
├─────────────┤               │                                    │
│ pkg/message/ │               │ aggregation → wire_format →        │
├─────────────┤               │ large_msg_invitation → rdma_read → │
│ pkg/server/  │               │ read_complete → free               │
└─────────────┘               └────────────────────────────────────┘
  5 independent boxes            4 cross-cutting flows
```

Five independent boxes vs. four cross-cutting flows. The framework optimized
for the former. The correctness lives in the latter.

---

## 5. Complete Gap Inventory

### 5.1 Designed But Not Implemented

| # | design.md § | Feature | Ticket | Status |
|---|------------|---------|--------|--------|
| 1 | §5 | Message aggregation (busy flag, 3 flush triggers) | DOMAIN-008 | Code exists in `pkg/message/aggregator.go` but uses wrong wire format and is never called from real connection |
| 2 | §6 | Event-driven CQ polling | DOMAIN-006 | `pkg/cq/poller.go` has mode enum but event/adaptive modes are stubs |
| 3 | §6 | Adaptive (smart) CQ polling | DOMAIN-006 | Same — only busy-poll implemented |
| 4 | §7.1 | Send/recv buffer pools (128 x 12KB) | DOMAIN-005 | `pkg/buffer/` exists but `PostRecvBuffers()` never called; pools never wired to real RDMA |
| 5 | §7.2-7.4 | RDMA READ large-message protocol | **None** | Not implemented. No invitation/completion code. No `ibv_post_read`. |
| 6 | §7.4 | Large buffer allocator with global cap | **None** | Not implemented. Config fields exist (`MaxLargeBufferCap`) but no allocator code. |
| 7 | §8.2 | NUMA-aware memory allocation | **None** | Not implemented. `PingPongConn` uses `C.malloc()`. |
| 8 | §10.1 | Heartbeat / connection liveness | Phase 4 deferred | `pkg/health/health.go` is placeholder only |
| 9 | §10.2 | Buffer monitor (stuck buffer recovery) | Phase 4 deferred | Same placeholder |
| 10 | §10.3 | Dynamic CQ resizing | **None** | Not implemented. No async event handler. |
| 11 | §11 | Wire format (12B cmd + 12B app headers, BE) | DOMAIN-008 | **Diverged**: `wire.go` uses 4B LE length-prefix, not design's format |

### 5.2 Implemented But Not Wired

| # | Module | What Exists | What's Missing |
|---|--------|-------------|---------------|
| 1 | `pkg/buffer/recv_pool.go` | `PostRecvBuffers()` method | Never called during connection setup |
| 2 | `pkg/cq/dispatcher.go` | `OnCompletion()` dispatch | Connection never registers as handler |
| 3 | `pkg/connection/conn.go` | `Send()` method | Sends without `IBV_SEND_SIGNALED`; expects pre-registered buffer in `msg.Buffer` |
| 4 | `pkg/message/aggregator.go` | Aggregation engine | Uses wrong wire format; never calls real RDMA send |
| 5 | `pkg/server/handler.go` | Per-connection recv loop | Never receives real completions |

### 5.3 Correctly Implemented

| # | Component | Status |
|---|-----------|--------|
| 1 | `internal/rdma/hotpath.c` | Correct batched poll+repost, batch-send, single-send |
| 2 | `internal/rdma/verbs.go` + `cm.go` | Correct CGo wrappers for all verbs and CM functions |
| 3 | `internal/rdma/real_cm.go` | Correct Listen/Accept/Dial with proper CM event handling |
| 4 | `internal/rdma/pingpong.go` | Correct self-contained data path (all 5 RDMA operations) |
| 5 | `internal/rdma/listener.go` + `dial.go` | Correct high-level Listen/Dial/Conn API |
| 6 | `pkg/config/config.go` | Correct config with all design fields, validation, YAML parsing |
| 7 | `internal/rdma/debug.go` | Diagnostic logging with DebugLog flag |

---

## 6. Lessons Learned

### Lesson 1: Conflict avoidance ≠ correctness

Zero merge conflicts is a process metric, not a quality metric. A project can
have zero conflicts and zero working features.

### Lesson 2: Mock-only testing is necessary but not sufficient

Mock tests prove internal logic. Integration tests prove the system works.
Without the latter, 52 passing tests mean nothing for RDMA correctness.

### Lesson 3: "Integration" must include data path, not just lifecycle

Starting, stopping, accepting, and connecting are lifecycle operations. Sending
and receiving data is the data path. Both must be integrated.

### Lesson 4: Design conformance must be checked, not assumed

The wire format diverged silently. Nobody compared `wire.go` against design.md
Section 11. Design conformance requires an explicit review step.

### Lesson 5: Tickets must trace to design sections, not just packages

If a design section has no ticket, it won't get implemented. A traceability
matrix from design sections to tickets would have caught the RDMA READ gap,
the NUMA gap, the wire format gap, and the CQ resize gap.

### Lesson 6: Cross-cutting flows need dedicated "Flow" tickets

Package-boundary tickets build modules. Flow tickets wire them together. Both
are needed. The execution plan had only the former.

---

## 7. Recommendations

### 7.1 Add a "Flow" Lane

Add a third ticket lane alongside System and Domain:

```
FLOW-001: Wire send data path (buffer → connection → CQ → buffer)
FLOW-002: Wire recv data path (post_recv → CQ → deliver → repost)
FLOW-003: Wire large-message path (invitation → RDMA READ → complete)
```

Flow tickets are explicitly not parallelizable with the packages they touch.
They run after packages are built and are allowed to modify multiple packages.

### 7.2 Require Behavioral Acceptance Criteria

Current: "go build passes, go test passes, -race clean."

Required: **"client.Send('hello') → server.Recv() == 'hello' over SoftRoCE."**

This single test exercises all 5 RDMA operations. It should be the exit gate
for any phase that touches the data path.

### 7.3 Generate Tickets From Design Sections

For each numbered section in `design.md`, either generate a ticket or explicitly
mark it as "deferred to Phase N" with a rationale. Verify coverage via a
traceability matrix before execution begins.

### 7.4 Add Design Conformance Review After Each Phase

After each phase, the Architect diffs implemented behavior against `design.md`:
- Does the wire format match §11?
- Does aggregation match §5?
- Are all config fields from Appendix A wired?

This review blocks the next phase.

### 7.5 Add a PingPong Validation Gate

```
Gate 0: PingPong works (CM → QP → buffer → MR → post_recv → send → recv)
Gate 1: Foundation (packages build)
Gate 2: Core modules (unit tests pass)
Gate 3: Modular data path matches PingPong behavior
Gate 4: Production features (aggregation, large messages, health)
```

Gate 3 is what was missing. Without it, you can have 52 passing tests and zero
bytes on the wire.

### 7.6 Runner Context Must Include Flow Participation

Current runner prompt: "Here are the interfaces you implement. Build and test."

Improved: "Here are the interfaces you implement. Here is the **end-to-end
flow** your code participates in. Your code must ensure [specific RDMA
semantic]. Your test must verify [cross-module invariant]."

---

## 8. Action Items

| # | Action | Owner | Deliverable |
|---|--------|-------|-------------|
| 1 | Create execution_plan_v2.md with Flow tickets and behavioral gates | Architect | `docs/execution_plan_v2.md` |
| 2 | Create design-to-ticket traceability matrix | Architect | In execution_plan_v2.md |
| 3 | Fix wire format to match design §11 | Runner | Modified `pkg/message/wire.go` |
| 4 | Wire data path end-to-end | Runner (Flow ticket) | Integration test passes on rxe |
| 5 | Implement all 9 missing design features | Runners (phased) | Per-feature tickets |
| 6 | Add design conformance review step to Railyard process | Architect | Updated SKILL.md or process doc |
| 7 | Add SoftRoCE integration test to CI gate | Architect | CI configuration |

---

## Appendix A: Execution Record Summary

| Phase | Agents | Files | LOC | Tests | Conflicts |
|-------|--------|-------|-----|-------|-----------|
| Phase 1, Batch 1 | 4 | 20 | ~700 | 10 | 0 |
| Phase 1, Batch 2 | 4 | 6 | ~1,060 | 0 | 0 |
| Phase 2 | 3 | 14 | ~2,873 | 24 | 0 |
| Phase 3 | 3 | 11 | ~1,692 | 18 | 0 |
| Phase 4 (Bench) | 3 | 14 | ~1,773 | 0 | 0 |
| **Total** | **17** | **65** | **~8,098** | **52** | **0** |

## Appendix B: Git History (Phases 1-4)

```
beb4144 Benchmark tool (golinker-bench): core framework + micro-benchmarks
7b27851 Update execution_record.md with Phase 3 details
f649261 Phase 3: Integration layer (message aggregation, server, CLI)
0f815de Phase 2: Core module implementations (buffer, cq, connection)
8ba1077 Phase 1: Foundation infrastructure and interface contracts
d7008f0 Rewrite design.md as standalone architecture document
b3147bc Initial project scaffold with design documents
```

## Appendix C: Post-Railyard Manual Fixes

After the Railyard phases completed, the following manual work was required:

1. **Data path audit** — Full trace of CM → PD → CQ → QP → buffer → send/recv
   verified correct in `PingPongConn` path (commit `dad4e16`)
2. **Three bugs fixed** — Client event channel leak, server concurrent accept
   race, RealQP state wrong after CM accept (commit `dad4e16`)
3. **Diagnostic logging** — `debug.go` with `DebugLog` flag (commit `dad4e16`)
4. **High-level API** — `rdma.Listen/Dial/Conn` hiding CM complexity, benchmark
   server/client rewritten to use it (commit `5dd4d3b`)

These fixes were necessary because the Railyard-built code could not actually
transfer data. The `PingPongConn` + high-level API now provides a working (but
simple) data path. The modular `pkg/` path remains unwired.
