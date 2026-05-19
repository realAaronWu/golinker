# golinker Execution Plan: Multi-Agent Parallel Build

## Overview

This document defines the execution strategy for building `golinker` (~6,000-8,000 Go LOC), a high-performance RDMA RPC library, using 3-4 parallel AI coding agents. The plan partitions work along module boundaries, provides upfront interface contracts to eliminate blocking dependencies, and defines a merge strategy that preserves correctness.

---

## 1. Parallel Agent Strategy

### Partitioning Principle

Work is divided along **package boundaries** in the Go project. Each agent owns one or more packages exclusively — no two agents ever edit the same file. Inter-module communication is defined entirely through Go interfaces established before parallel work begins.

### Dependency DAG

```
Phase 1 (sequential, single agent)
    │
    ├── internal/rdma/   (CGo bindings + C hot-path)
    ├── api/             (public interfaces)
    └── pkg/config/      (configuration structs)
    │
    ▼
Phase 2 (parallel, 3 agents)
    ┌───────────────┬────────────────┬──────────────────┐
    │ Agent 2A      │ Agent 2B       │ Agent 2C         │
    │ pkg/buffer/   │ pkg/cq/        │ pkg/connection/  │
    │               │                │                  │
    └───────┬───────┴────────┬───────┴────────┬─────────┘
            │                │                │
            ▼                ▼                ▼
Phase 3 (sequential or 2 agents)
    │
    ├── pkg/message/     (aggregation layer)
    ├── pkg/server/      (server lifecycle)
    └── cmd/golinker-server, cmd/golinker-client
    │
    ▼
Phase 4 (parallel, 2-3 agents)
    ┌──────────────┬────────────────┬───────────────┐
    │ Agent 4A     │ Agent 4B       │ Agent 4C      │
    │ pkg/health/  │ pkg/metrics/   │ test/         │
    │              │ cmd/golinker-bench│               │
    └──────────────┴────────────────┴───────────────┘
```

### Conflict Avoidance Rules

1. Each agent operates in its own branch (`agent/<phase>-<module>`).
2. Shared interfaces live in `api/` and are **frozen** before Phase 2 begins.
3. Agents import interfaces, never concrete types from other agents' packages.
4. Integration points are validated by compiling against interface stubs.

---

## 2. Phase Plan

### Phase 1 — Foundation (1 agent, ~1 week)

**Agent 1** builds the substrate all other agents depend on.

| Deliverable | Package | Description |
|---|---|---|
| CGo bindings | `internal/rdma/` | Thin wrappers around `ibv_*` and `rdma_cm_*` verbs, plus the ~300-line C hot-path (`cq_poll_batch`, `post_send_batch`) |
| Public interfaces | `api/` | All inter-module contracts (see Section 4) |
| Config types | `pkg/config/` | `ServerConfig`, `ConnectionConfig`, `BufferConfig`, `CQConfig` structs with validation |
| Build system | `go.mod`, `Makefile` | CGo flags, NUMA linkage, build tags for mock vs real RDMA |

**Exit criteria:** `go build ./...` succeeds; unit tests for config parsing pass; CGo smoke test calls `ibv_get_device_list` (or mock).

---

### Phase 2 — Core Modules (3 agents parallel, ~2-3 weeks)

#### Agent 2A — Buffer Pools (`pkg/buffer/`)

Implements: buffer pool, send pool, receive pool, NUMA-aware allocation

| Deliverable | Description |
|---|---|
| `pool.go` | Core buffer pool with `C.malloc`/`numa_alloc_onnode` allocation |
| `send_pool.go` | Lock-free send buffer pool (replaces sp_queue-based pool) |
| `recv_pool.go` | Receive buffer pool with pre-posted WRs |
| `numa.go` | NUMA-aware allocation strategy |
| `pool_test.go` | Unit tests: alloc/free cycles, concurrent access, OOM handling |

#### Agent 2B — Completion Queue (`pkg/cq/`)

Implements: CQ poller, CQ pool, 4 polling modes

| Deliverable | Description |
|---|---|
| `poller.go` | CQ poller using C hot-path (`cq_poll_batch`) with adaptive polling/blocking |
| `pool.go` | CQ pool distributing connections across pollers |
| `dispatcher.go` | Dispatches completions to connection handlers via Go channels |
| `poller_test.go` | Unit tests: mock completions, throughput benchmarks |

#### Agent 2C — Connection Management (`pkg/connection/`)

Implements: connection lifecycle, CM events, connection pool

| Deliverable | Description |
|---|---|
| `conn.go` | RDMA connection lifecycle (init, connect, disconnect, destroy) |
| `cm_events.go` | CM event loop (replaces epoll-based event handling) |
| `qp.go` | QP creation and configuration |
| `conn_test.go` | Unit tests: state machine transitions, error injection |

---

### Phase 3 — Integration Layer (1-2 agents, ~2 weeks)

Depends on all Phase 2 outputs.

| Deliverable | Package | Description |
|---|---|---|
| Message aggregation | `pkg/message/` | Implements aggregation engine — batches small messages, manages send/recv WR posting |
| Server lifecycle | `pkg/server/` | Implements server listener — accept loop, connection registry, graceful shutdown |
| CLI binaries | `cmd/` | Minimal server/client that exercise the full stack |

---

### Phase 4 — Polish (2-3 agents parallel, ~1-2 weeks)

| Agent | Packages | Description |
|---|---|---|
| 4A | `pkg/health/` | Heartbeat (`rdma_heartbeat.cpp`), buffer monitor (`rdma_buffer_monitor.cpp`), liveness probes |
| 4B | `pkg/metrics/`, `cmd/golinker-bench/` | Prometheus integration, latency histograms, throughput counters, benchmark harness |
| 4C | `test/` | End-to-end integration tests, performance validation, chaos/fault injection |

---

## 3. Per-Agent Context Packets

### Agent 1 (Phase 1 — Foundation)

**Outputs:**
- `internal/rdma/verbs.go` — CGo wrappers
- `internal/rdma/hotpath.c` + `hotpath.h` — C polling/posting functions
- `api/interfaces.go` — all interface definitions
- `pkg/config/config.go` — config structs

**Acceptance criteria:**
- [x] `go build ./internal/rdma/` compiles with CGo
- [x] `go test ./pkg/config/` passes
- [x] Mock implementation of `rdma.Verbs` interface exists for downstream agents

---

### Agent 2A (Phase 2 — Buffer Pools)

**Interface contracts received:**
```go
// From api/ — Agent 2A must implement buffer.Pool
type BufferPool interface {
    Alloc(size int) (*Buffer, error)
    Free(buf *Buffer)
    Close() error
}

// From internal/rdma — Agent 2A may call:
type MemoryRegion interface {
    Register(pd ProtectionDomain, addr unsafe.Pointer, length int, access int) (*MR, error)
    Deregister(mr *MR) error
}
```

**Dependency stubs provided:**
- `internal/rdma/mock_verbs.go` — mock MR registration (always succeeds)
- `pkg/config/config.go` — `BufferConfig` struct

**Acceptance criteria:**
- [x] `go test ./pkg/buffer/ -race` passes
- [x] Benchmark: alloc/free cycle < 200ns (no syscall per operation)
- [x] Memory is allocated outside Go heap (`C.malloc` or `numa_alloc`)
- [x] Pool handles concurrent access from 64+ goroutines

---

### Agent 2B (Phase 2 — Completion Queue)

**Interface contracts received:**
```go
// From api/ — Agent 2B must implement cq.Poller
type CQPoller interface {
    Poll(ctx context.Context) error    // long-running poll loop
    Register(cq *CompletionQueue, handler CompletionHandler) error
    Unregister(cq *CompletionQueue) error
    Close() error
}

type CompletionHandler interface {
    OnSendComplete(wr WorkRequest, status int)
    OnRecvComplete(wr WorkRequest, status int)
}
```

**Dependency stubs provided:**
- `internal/rdma/mock_verbs.go` — mock CQ creation/destruction
- `internal/rdma/hotpath.h` — C function signatures for `cq_poll_batch`

**Acceptance criteria:**
- [x] `go test ./pkg/cq/ -race` passes
- [x] Poll loop uses C hot-path (no per-WC CGo crossing)
- [x] Adaptive polling: busy-spin under load, `ibv_req_notify_cq` when idle
- [x] CQ pool distributes connections with even load balancing

---

### Agent 2C (Phase 2 — Connection Management)

**Interface contracts received:**
```go
// From api/ — Agent 2C must implement connection.Connection
type Connection interface {
    ID() uint64
    Send(msg *Message) error
    Close() error
    State() ConnectionState
    OnStateChange(fn func(ConnectionState))
}

type ConnectionManager interface {
    Accept(cmID *CMIdentifier) (Connection, error)
    Connect(addr string) (Connection, error)
    Close() error
}
```

**Dependency stubs provided:**
- `internal/rdma/mock_verbs.go` — mock QP, CM ID operations
- `pkg/config/config.go` — `ConnectionConfig` struct
- Mock `buffer.Pool` — returns dummy buffers
- Mock `cq.Poller` — accepts registrations, delivers fake completions

**Acceptance criteria:**
- [x] `go test ./pkg/connection/ -race` passes
- [x] Connection state machine: `Init → Connecting → Connected → Draining → Closed`
- [x] CM event goroutine correctly handles: CONNECT_REQUEST, ESTABLISHED, DISCONNECTED
- [x] Graceful shutdown drains in-flight sends before destroying QP

---

## 4. Interface Contracts

These interfaces are defined in `api/` during Phase 1 and are **immutable** during Phases 2-3.

### `api/rdma.go` — Low-Level RDMA Abstractions

```go
package api

import (
    "context"
    "unsafe"
)

// ProtectionDomain wraps ibv_pd.
type ProtectionDomain interface {
    Handle() unsafe.Pointer
}

// MemoryRegion wraps ibv_mr.
type MemoryRegion interface {
    Addr() unsafe.Pointer
    Length() int
    LKey() uint32
    RKey() uint32
}

// CompletionQueue wraps ibv_cq.
type CompletionQueue interface {
    Handle() unsafe.Pointer
    Size() int
}

// QueuePair wraps ibv_qp.
type QueuePair interface {
    Handle() unsafe.Pointer
    QPNum() uint32
    State() QueuePairState
    ModifyToInit() error
    ModifyToRTR(destQPN uint32, destLID uint16, destGID [16]byte) error
    ModifyToRTS() error
}

// Verbs provides access to RDMA verbs operations.
type Verbs interface {
    OpenDevice(devName string) error
    AllocPD() (ProtectionDomain, error)
    CreateCQ(size int) (CompletionQueue, error)
    CreateQP(pd ProtectionDomain, sendCQ, recvCQ CompletionQueue, cfg QueuePairConfig) (QueuePair, error)
    RegMR(pd ProtectionDomain, addr unsafe.Pointer, length int, access AccessFlags) (MemoryRegion, error)
    DeregMR(mr MemoryRegion) error
    PostSend(qp QueuePair, wr *SendWR) error
    PostRecv(qp QueuePair, wr *RecvWR) error
    Close() error
}

// CMEventChannel wraps rdma_cm event handling.
type CMEventChannel interface {
    Listen(ctx context.Context, addr string, port int) error
    GetEvent(ctx context.Context) (*CMEvent, error)
    AckEvent(event *CMEvent) error
    Close() error
}
```

### `api/buffer.go` — Buffer Pool Interface

```go
package api

import "unsafe"

// Buffer represents a registered RDMA buffer.
type Buffer struct {
    Addr   unsafe.Pointer
    Length int
    LKey   uint32
    RKey   uint32
    MR     MemoryRegion
    PoolID int // identifies which pool owns this buffer
}

// BufferPool manages allocation of registered RDMA buffers.
type BufferPool interface {
    // Alloc returns a buffer of at least `size` bytes.
    // Blocks if pool is exhausted (with configurable timeout).
    Alloc(size int) (*Buffer, error)

    // Free returns a buffer to the pool.
    Free(buf *Buffer)

    // Stats returns pool utilization metrics.
    Stats() BufferPoolStats

    // Close releases all buffers and deregisters MRs.
    Close() error
}

// SendBufferPool extends BufferPool with send-specific semantics.
type SendBufferPool interface {
    BufferPool
    // AcquireForSend gets a buffer and marks it in-flight.
    AcquireForSend() (*Buffer, error)
    // CompleteSend marks a send buffer as reclaimable.
    CompleteSend(buf *Buffer)
}

// RecvBufferPool extends BufferPool with receive-specific semantics.
type RecvBufferPool interface {
    BufferPool
    // PostRecvBuffers pre-posts receive buffers to a QP.
    PostRecvBuffers(qp QueuePair, count int) error
    // Replenish re-posts consumed receive buffers.
    Replenish(qp QueuePair, consumed int) error
}

type BufferPoolStats struct {
    TotalBuffers     int
    FreeBuffers      int
    InFlightBuffers  int
    AllocatedBytes   int64
    NUMANode         int
}
```

### `api/cq.go` — Completion Queue Interface

```go
package api

import "context"

// WorkCompletion represents a single ibv_wc.
type WorkCompletion struct {
    WRID      uint64
    Status    WCStatus
    Opcode    WCOpcode
    ByteLen   uint32
    QPN       uint32
    IMM       uint32
    HasIMM    bool
}

// CompletionHandler processes completed work requests.
type CompletionHandler interface {
    OnCompletion(wc *WorkCompletion)
    OnError(wc *WorkCompletion, err error)
}

// CQPoller polls one or more completion queues.
type CQPoller interface {
    // Start begins the poll loop (blocks until ctx is cancelled).
    Start(ctx context.Context) error
    // AddCQ registers a CQ with this poller.
    AddCQ(cq CompletionQueue, handler CompletionHandler) error
    // RemoveCQ unregisters a CQ.
    RemoveCQ(cq CompletionQueue) error
    // Stats returns polling metrics.
    Stats() CQPollerStats
    // Close stops polling and releases resources.
    Close() error
}

// CQPool manages a pool of CQ pollers for load distribution.
type CQPool interface {
    // Assign returns the least-loaded poller for a new connection.
    Assign() (CQPoller, CompletionQueue, error)
    // Release returns a CQ to the pool.
    Release(cq CompletionQueue) error
    // Close shuts down all pollers.
    Close() error
}

type CQPollerStats struct {
    PollCycles    uint64
    Completions   uint64
    EmptyPolls    uint64
    Errors        uint64
    AvgBatchSize  float64
}
```

### `api/connection.go` — Connection Interface

```go
package api

import "context"

// ConnectionState represents the RDMA connection state machine.
type ConnectionState int

const (
    StateInit         ConnectionState = iota
    StateConnecting
    StateConnected
    StateDraining
    StateClosed
    StateError
)

// Message is the wire-format unit.
type Message struct {
    Buffer  *Buffer
    Length  int
    Flags   MessageFlags
    ImmData uint32
}

// Connection represents a single RDMA connection (one QP).
type Connection interface {
    ID() uint64
    RemoteAddr() string
    State() ConnectionState

    // Send posts a send WR. Non-blocking; completion via CompletionHandler.
    Send(msg *Message) error

    // Recv returns the next received message (blocks).
    Recv(ctx context.Context) (*Message, error)

    // Close initiates graceful disconnect.
    Close() error

    // OnStateChange registers a callback for state transitions.
    OnStateChange(fn func(old, new ConnectionState))
}

// ConnectionManager handles connection lifecycle.
type ConnectionManager interface {
    // Accept processes an incoming connection request.
    Accept(ctx context.Context) (Connection, error)

    // Connect initiates an outbound connection.
    Connect(ctx context.Context, addr string) (Connection, error)

    // GetConnection retrieves a connection by ID.
    GetConnection(id uint64) (Connection, bool)

    // Close shuts down all connections.
    Close() error
}

// Server is the top-level RDMA server.
type Server interface {
    // Start begins listening and accepting connections.
    Start(ctx context.Context) error
    // Stop gracefully shuts down the server.
    Stop(ctx context.Context) error
    // RegisterHandler sets the message handler for incoming messages.
    RegisterHandler(handler MessageHandler)
}

// MessageHandler processes incoming messages.
type MessageHandler interface {
    Handle(conn Connection, msg *Message) (*Message, error)
}
```

---

## 5. Merge Strategy

### Assembly Order

```
1. Phase 1 output → main branch (foundation)
2. Agent 2A branch → merge first (no deps on 2B/2C)
3. Agent 2B branch → merge second (no deps on 2A/2C)
4. Agent 2C branch → merge third (may import buffer/cq packages)
5. Integration compilation check: `go build ./...`
6. Phase 3 work begins on main
7. Phase 4 branches merge after Phase 3
```

### Integration Validation

After each merge:

```bash
# Step 1: Compilation
go build ./...

# Step 2: Unit tests (all packages)
go test ./... -race -count=1

# Step 3: Interface compliance (compile-time)
# Each package has a file like:
// var _ api.BufferPool = (*pool.DefaultPool)(nil)

# Step 4: Integration smoke test (Phase 3+)
go test ./test/integration/ -tags=integration -v
```

### Composition Pattern

Modules are composed via dependency injection in `pkg/server/`:

```go
func NewServer(cfg *config.ServerConfig) (*Server, error) {
    verbs := rdma.NewVerbs(cfg.DeviceName)
    pd, _ := verbs.AllocPD()

    bufPool := buffer.NewPool(pd, cfg.Buffer)
    sendPool := buffer.NewSendPool(bufPool)
    recvPool := buffer.NewRecvPool(bufPool)

    cqPool := cq.NewPool(verbs, cfg.CQ)

    connMgr := connection.NewManager(connection.Deps{
        Verbs:    verbs,
        PD:       pd,
        SendPool: sendPool,
        RecvPool: recvPool,
        CQPool:   cqPool,
        Config:   cfg.Connection,
    })

    return &Server{
        verbs:   verbs,
        connMgr: connMgr,
        bufPool: bufPool,
        cqPool:  cqPool,
    }, nil
}
```

---

## 6. Risk Mitigation

### Interface Drift

| Risk | Mitigation |
|---|---|
| Agent discovers interface is insufficient mid-implementation | Agent documents needed changes in `INTERFACE_CHANGE_REQUEST.md` in their branch. Coordinator reviews and issues amendment to all affected agents. |
| Two agents need incompatible changes | Coordinator creates adapter layer rather than modifying frozen interface. |
| Interface is over-specified | Interfaces use minimal method sets. Agents can define package-internal extended interfaces. |

**Protocol:** If an agent needs to change an `api/` interface:
1. Agent writes a proposal in their branch: `docs/interface_change_<module>.md`
2. Coordinator evaluates impact on other agents
3. If approved, coordinator pushes amendment to all active branches via cherry-pick
4. 24-hour freeze on affected interfaces after amendment

### CGo Build Issues

| Risk | Mitigation |
|---|---|
| CGo compilation fails on agent's environment | Provide Docker image with pre-installed `libibverbs-dev`, `librdmacm-dev`, `libnuma-dev` |
| C hot-path has undefined behavior | Hot-path is only ~300 LOC; reviewed and tested with AddressSanitizer before agents start |
| Linking errors with NUMA | Build tags: `//go:build !numa` fallback uses `C.malloc` |

**Build environment spec:**
```dockerfile
FROM golang:1.22-bookworm
RUN apt-get update && apt-get install -y \
    libibverbs-dev librdmacm-dev libnuma-dev \
    rdma-core ibverbs-utils
ENV CGO_ENABLED=1
```

### RDMA Device Availability

| Risk | Mitigation |
|---|---|
| No physical RDMA NIC for testing | All packages work against mock `api.Verbs` implementation |
| `rxe` (soft-RoCE) kernel module unavailable | Integration tests gated behind `//go:build integration` tag; CI uses `rxe` |
| Mock doesn't catch real hardware bugs | Phase 4 Agent 4C runs tests on hardware cluster; issues filed as bugs |

**Testing tiers:**
1. **Unit tests** (always run): Use mock interfaces, no hardware needed
2. **Soft-RDMA tests** (`-tags=softrdma`): Use `rxe` kernel module
3. **Hardware tests** (`-tags=hwtest`): Require real Mellanox/Intel NICs

### Agent Coordination Failures

| Risk | Mitigation |
|---|---|
| Agent produces incompatible output | Compile-time interface checks catch immediately at merge |
| Agent stalls or produces low quality | Each phase has hard time-box; fallback is coordinator completes module |
| Agents duplicate utility code | `pkg/util/` is owned exclusively by Phase 1; agents import, never duplicate |

---

## 7. Timeline

Assuming 3 parallel agents available from Week 2 onward.

```
Week 1:  Phase 1 — Foundation (Agent 1)
         ├── Day 1-2: internal/rdma/ CGo bindings + C hot-path
         ├── Day 3-4: api/ interface definitions
         └── Day 5:   pkg/config/ + build system + mock verbs

Week 2:  Phase 2 — Core (3 agents parallel)
         ├── Agent 2A: Buffer pool core (alloc/free, MR registration)
         ├── Agent 2B: CQ poller (C hot-path integration, adaptive polling)
         └── Agent 2C: Connection state machine (QP lifecycle, CM events)

Week 3:  Phase 2 continued
         ├── Agent 2A: Send/recv pool specializations, NUMA support
         ├── Agent 2B: CQ pool, load balancing, dispatcher channels
         └── Agent 2C: Connection manager, graceful shutdown, reconnect

Week 4:  Phase 2 wrap-up + merge
         ├── All agents: Polish unit tests, fix race conditions
         ├── Coordinator: Merge 2A → 2B → 2C into main
         └── Integration compilation verification

Week 5:  Phase 3 — Integration (1-2 agents)
         ├── Message aggregation layer (smart batching logic)
         ├── Immediate send path (bypass aggregation for large msgs)
         └── Server accept loop + connection registry

Week 6:  Phase 3 continued
         ├── Server lifecycle (graceful shutdown, drain)
         ├── cmd/golinker-server + cmd/golinker-client (basic functionality)
         └── First end-to-end test: client → server → echo

Week 7:  Phase 4 — Polish (3 agents parallel)
         ├── Agent 4A: Health monitoring (heartbeat, buffer monitor)
         ├── Agent 4B: Prometheus metrics + cmd/golinker-bench
         └── Agent 4C: Integration test suite, performance validation

Week 8:  Phase 4 wrap-up + final integration
         ├── All agents: Bug fixes from integration testing
         ├── Performance tuning (benchmark-driven)
         └── Documentation, README, deployment guide
```

### Milestone Gates

| Gate | Criteria | Week |
|---|---|---|
| G1: Foundation Ready | `go build ./...` passes; mock verbs work | End of W1 |
| G2: Core Modules Done | All Phase 2 unit tests pass with `-race` | End of W4 |
| G3: End-to-End | Client-server echo works over soft-RDMA | End of W6 |
| G4: Production Ready | All tests pass; benchmarks meet targets; interop verified | End of W8 |

### Performance Targets (Gate G4)

| Metric | Target | Measurement |
|---|---|---|
| Small message latency (64B) | < 5 us (same as C++) | `cmd/golinker-bench` |
| Throughput (1KB messages) | > 10M msgs/sec | `cmd/golinker-bench` |
| Buffer alloc/free | < 200ns | `go test -bench` |
| CQ poll batch (32 WCs) | Single CGo call | Code inspection |
| Memory overhead vs C++ | < 20% more | RSS comparison |

---

## Appendix: File Mapping (C++ → Go)

| C++ Source | Go Package | Notes |
|---|---|---|
| `rdma_connection.cpp` | `pkg/connection/` | State machine + QP management |
| `rdma_server.cpp` | `pkg/server/` + `pkg/connection/` | Split: listener vs connection |
| `rdma_completion_queue.cpp` | `pkg/cq/` | Direct port + C hot-path |
| `rdma_cq_pool.cpp` | `pkg/cq/` | Pool logic |
| `rdma_buffer_pool.cpp` | `pkg/buffer/` | Core pool |
| `rdma_send_buffer_lock_pool.cpp` | `pkg/buffer/` | Send specialization |
| `rdma_receive_buffer_pool.cpp` | `pkg/buffer/` | Recv specialization |
| `rdma_numa_buffer.cpp` | `pkg/buffer/` | NUMA allocator |
| `rdma_smart_aggregate_connection.cpp` | `pkg/message/` | Batching logic |
| `rdma_immediate_connection.cpp` | `pkg/message/` | Fast path |
| `rdma_heartbeat.cpp` | `pkg/health/` | Keep-alive |
| `rdma_buffer_monitor.cpp` | `pkg/health/` | Pool health checks |
| `sp_queue.h`, `sp_executor.h` | Go channels | No direct port needed |
| `mt_hash_map.h` | `sync.Map` | No direct port needed |
| Custom metrics | `pkg/metrics/` | Prometheus-based |
