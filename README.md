# swim-gossip-core
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/swim-gossip-core/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/swim-gossip-core/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/swim-gossip-core) [![Stars](https://img.shields.io/github/stars/sohamshewani/swim-gossip-core?style=flat)](https://github.com/sohamshewani/swim-gossip-core)

## Executive Summary & Problem Space

`swim-gossip-core` is an enterprise-grade, zero-allocation-path implementation of the SWIM (Structured Weakly-Consistent Infection-Style Process Group Membership) protocol in Python. Designed for hyperscale distributed environments, it resolves traditional peer-to-peer discovery bottlenecks:
- **Linear Scaling Overhead**: Eliminates centralized heartbeats, bounding steady-state network overhead to $O(1)$ per node regardless of cluster size.
- **False-Positive Partition Storms**: Replaces binary dead/alive states with an adaptive, phi-accrual suspicion mechanism (`src/swim/suspicion.py`) that scales suspicion timeouts dynamically based on historical packet loss and round-trip times.
- **State Convergence Drift**: Integrates causal vector clocks (`src/swim/vector_clock.py`) and monotonic incarnation numbering to guarantee deterministic state reconciliation without distributed consensus bottlenecks (e.g., Paxos/Raft).
- **Garbage Collection Latency Spikes**: Employs deterministic memory pooling and object reclamation (`src/swim/allocator.py`) to prevent dynamic runtime allocations within the hot-path network and state-transition loops.

---

## Structural ASCII Architecture / Dataflow Diagram

```
 +-----------------------------------------------------------------------------------+
 |                             NETWORK & IO LAYER                                    |
 +-----------------------------------------------------------------------------------+
          |                                                       ^
          v (Inbound UDP / Encrypted Datagrams)                   | (Outbound Pings & Probes)
 +-------------------------+                               +-------------------------+
 |   concurrency.py        |                               |      engine.py          |
 |  (AsyncIO Event Loop)   |                               |  (SWIM Engine Core)     |
 +-------------------------+                               +-------------------------+
          |                                                       ^
          | Dispatches Raw Payload                                | Pulls Piggyback Meta
          v                                                       |
 +-----------------------------------------------------------------------------------+
 |                              STATE RECONCILIATION LAYER                           |
 +-----------------------------------------------------------------------------------+
          |                                                       ^
          v                                                       |
 +-------------------------+     Updates      +------------------+-------------------+
 |   vector_clock.py       +----------------->+                  types.py            |
 | (Causal Causality Map)  |                  | (Node State, Incarnations, Metadata) |
 +-------------------------+                  +------------------+-------------------+
                                                                  |
                                                                  v Checks Health
                                              +------------------+-------------------+
                                              |      suspicion.py                    |
                                              | (Phi-Accrual Failure Detector)       |
                                              +------------------+-------------------+
                                                                  |
                                                                  v Allocates Buffers
                                              +------------------+-------------------+
                                              |      allocator.py                    |
                                              | (Zero-Alloc Object Pooling Engine)   |
                                              +--------------------------------------+
```

---

## Concurrency Model & Memory Invariants

`swim-gossip-core` enforces strict thread safety and determinism across concurrent network and computation routines:
- **AsyncIO & Lock-Free State Boundaries**: Network IO operations (`src/swim/concurrency.py`) execute asynchronously via non-blocking event loops, segregating IO routines from deterministic state mutations. Critical state maps in `src/swim/engine.py` are guarded by reentrant async primitives to prevent race conditions during concurrent ping-req acknowledgments.
- **Memory Ownership & Zero-Allocation Hot Paths**: To circumvent Python's Garbage Collector (GC) latency spikes during high-throughput gossip phases, `src/swim/allocator.py` implements pre-allocated memory pools and object recycling. Buffers used for serialization and deserialization of gossip messages are leased and returned deterministically.
- **Monotonic Invariants**: Incarnation numbers and vector clocks operate under strict monotonicity guarantees. A node state update is rejected if the incoming vector clock is causally dominated or equal to the local state unless accompanied by a strictly higher incarnation number, neutralizing stale packet injections and out-of-order delivery anomalies.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Target Path | Time Complexity | Space Complexity | Estimated Latency (P99) |
| :--- | :--- | :--- | :--- | :--- |
| **State Merge** | `src/swim/engine.py` | $O(N)$ where $N = \text{piggyback size}$ | $O(1)$ (Pooled allocation) | $< 120\mu s$ |
| **Vector Clock Resolution** | `src/swim/vector_clock.py` | $O(K)$ where $K = \text{cluster size}$ | $O(1)$ | $< 45\mu s$ |
| **Suspicion Evaluation** | `src/swim/suspicion.py` | $O(1)$ | $O(1)$ | $< 15\mu s$ |
| **Buffer Allocation** | `src/swim/allocator.py` | $O(1)$ (Free-list lookup) | $O(P)$ ($\text{Pool capacity}$) | $< 2\mu s$ |
| **Full Cluster Ping Cycle**| `src/swim/engine.py` | $O(1)$ per node interval | $O(K)$ | Node-Interval Dependent |

---

## Security Architecture & Threat Model

- **Input Boundaries & Sanitization**: All inbound network payloads undergo strict schema validation and structural parsing within hardened boundaries (`src/swim/types.py`) before touching internal maps. Malformed, truncated, or oversized datagrams are dropped at the socket reader level.
- **Memory Corruption & Overflow Protections**: Python's memory model inherently prevents buffer overflows, but `swim-gossip-core` defends against semantic memory exhaustion (e.g., state-table bloating attacks via Sybil nodes) by enforcing strict capacity caps on local membership tables and dropping unauthenticated or un-incarnated state mutations.
- **Threat Vectors Mitigated**:
  - *Stale State Injection*: Mitigated via monotonic incarnation counters and cryptographic-grade vector clock validation.
  - *Denial of Service (DoS) via Gossip Flooding*: Mitigated by rate-limiting piggyback disseminations and bounded allocator pools (`src/swim/allocator.py`).
  - *Split-Brain Partitioning*: Mitigated through adaptive suspicion thresholds (`src/swim/suspicion.py`), reducing false-positive failure declarations in lossy networks.

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
Ensure you have Python 3.10+ and `poetry` installed on your host system.

### 1. Environment Initialization & Dependency Resolution
```bash
poetry install --with dev,bench
```

### 2. Static Analysis & Test Verification (Invariants)
Execute the invariant test suite to verify vector clock convergence, suspicion timeouts, and state-machine transitions:
```bash
poetry run pytest tests/test_invariants.py -v --tb=short
```

### 3. Performance & Micro-Benchmarks
Execute the engine benchmarking harness (`benchmarks/bench_engine.py`) to profile allocation efficiency and merge throughput under simulated cluster scales:
```bash
poetry run python benchmarks/bench_engine.py --nodes=1000 --iterations=5000