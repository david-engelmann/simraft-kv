# How simraft-kv Compares

`simraft-kv` combines a zero-dependency sans-I/O Raft consensus core (`simraft-core`), a deterministic virtual-clock chaos simulator and linearizability verifier (`simraft-sim`), and a reference durable TCP key-value server and client (`simraft-node` and the `simraft_kv` Python SDK) in one workspace.

This page explains how `simraft-kv` relates to established Raft libraries and deterministic simulation frameworks, where it shines, and what it deliberately leaves out of scope.

## Summary matrix

| Dimension | `simraft-kv` | `tikv/raft-rs` | `openraft` | `etcd/raft` (Go) |
| --- | --- | --- | --- | --- |
| Core execution model | Pure sans-I/O (`Node::step(Event) -> Vec<Action>`) | Sans-I/O (`RawNode::step` + `ready`) | Async trait runtime (`Tokio` / `AsyncRuntime`) | Sans-I/O (`Node.Step` + `Ready` channel) |
| Core runtime dependencies | `std` only (zero crates) | `protobuf`, `slog`, `thiserror`, `bytes` | `tokio`, `futures`, `serde`, `tracing`, `anyerror` | Go stdlib + `protobuf` |
| `#![forbid(unsafe_code)]` | All 5 workspace crates | Partial | Yes (core crate) | N/A (Go) |
| Built-in deterministic chaos simulator | Included (`simraft-sim`, single-seed replay, `--chaos`) | External / user-supplied | External / async mock | Basic in-package network test harness |
| Built-in linearizability checker | Per-key Wing and Gong (`simraft-sim::liz`) | External (Jepsen / porcupine) | External | External (porcupine) |
| Linearizable reads | Clock-free `ReadIndex` (`R-01..R-04`, `R-06`) | `ReadIndex` + optional lease read | `ReadIndex` + leader lease | `ReadIndex` + optional lease read |
| Membership changes | Single-server (`AddLearner`, `PromoteVoter`, `RemoveVoter`) | Joint consensus + simple changes | Joint consensus | Joint consensus + simple changes |
| Wire & disk formats | Fixed little-endian `"SRKV"` / `"SRSN"` with CRC-32 & SHA-256 | Protobuf (`eraftpb`) | Generic `serde` / user-defined | Protobuf (`raftpb`) |
| Reference TCP server & Python client | Included (`simraft-node` + `simraft_kv` on PyPI) | Not included (bring your own) | Example stores | Full `etcd` server |

## Detailed comparisons

### `tikv/raft-rs`

`tikv/raft-rs` is TiKV's battle-tested Rust port of `etcd/raft`, powering large-scale multi-Raft production clusters.

- **Choose `tikv/raft-rs`** when you need joint-consensus arbitrary membership transitions, Protobuf compatibility with existing `eraftpb` ecosystems, or battle-hardened multi-terabyte production lineage at TiKV scale.
- **Choose `simraft-kv`** when you want a zero-dependency `std`-only core (`simraft-core`), explicit `Event -> Vec<Action>` stepping without a split `step`/`ready`/`advance` lifecycle, verified per-step heap allocation budgets (`L-03`), and an integrated deterministic chaos simulator + Wing and Gong linearizability oracle out of the box.

### `openraft`

`openraft` is a popular async Rust Raft framework parameterized over storage, network, and runtime traits.

- **Choose `openraft`** when your application is already structured around Tokio async traits, `serde` models, and joint-consensus dynamic cluster topologies, and you prefer high-level async method calls over driving a state machine loop.
- **Choose `simraft-kv`** when deterministic simulation, single-seed failure replay, zero async/runtime lock-in, and byte-pinned non-`serde` wire and snapshot schemas matter most. Because `simraft-core` has no async runtime traits or background tasks, `simraft-sim` steps thousands of virtual-clock cluster ticks per second on one OS thread with guaranteed cross-platform determinism (`D-01`, `D-02`).

### `etcd/raft`

`etcd/raft` popularized the sans-I/O state-machine architecture for Raft in Go (used by etcd, CockroachDB, and Docker Swarm).

- `simraft-core` follows the same philosophical separation between pure consensus logic and driver-owned I/O, adapted to idiomatic Rust value semantics (`Event` in, `Vec<Action>` out) with explicit durability watermarks (`Event::Persisted`) that fence premature vote, append, commit, and `ReadIndex` releases inside the core (`T-02`, `R-06`) rather than relying solely on caller discipline around `Ready` batches.

### FoundationDB simulation and TigerBeetle VOPR

FoundationDB pioneered testing distributed storage inside a single-threaded deterministic simulator with seeded network and disk faults, an approach extended by TigerBeetle's VOPR harness.

`simraft-sim` brings this verification model directly into a compact, inspectable Rust workspace:

- **Pinned PRNG**: internal SplitMix64 seed expansion into xoshiro256starstar, immune to upstream `rand` crate changes (`D-01`).
- **Realistic storage faults**: `SimDisk` models distinct volatile page-cache writes and fsynced durable state (`H-03`), alongside torn log record CRC-32 truncation (`H-05`) and corrupt snapshot SHA-256 rejection (`H-06`).
- **Hostile network faults**: asymmetric directed cuts, chain partitions, isolated nodes, packet drops/duplicates, variable latency, virtual clock jumps, and wire bit-flips (`H-01`, `H-04`).
- **Continuous oracles + bounded healing**: checks `S-01..S-05` after every simulator event, lifts faults at `heal_start_tick`, verifies leader and state digest convergence (`H-02`), and runs an exact memoized Wing and Gong linearizability check (`R-05`, `L-04`).

## Explicit non-goals and trade-offs

`simraft-kv` intentionally scopes out several features to keep every consensus path auditable against [INVARIANTS.md](INVARIANTS.md):

1. **Single-server membership changes instead of joint consensus.** Adding a learner (`AddLearner`), promoting a caught-up learner (`PromoteVoter`), and removing a voter (`RemoveVoter`) execute one server at a time (`M-01..M-05`). Multi-server changes are performed as sequential single-server steps rather than two-phase joint quorums.
2. **Clock-free `ReadIndex` instead of clock-bound leader leases.** Reads require one heartbeat quorum round (zero log writes, `L-01`) rather than assuming bounded physical clock drift (`R-04`).
3. **Single Raft group in `simraft-node`.** `simraft-node` runs one Raft consensus group per cluster rather than a multi-Raft range router or distributed SQL layer.
4. **Transport security delegated to infrastructure.** `simraft-node` uses unauthenticated TCP sockets and relies on private networking, WireGuard, or mTLS proxies in untrusted networks (see [SECURITY.md](../SECURITY.md)).
