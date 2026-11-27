# simraft-kv

<p>
  <img class="theme-light-only" src="docs/media/simraft-lockup-light.svg" alt="simraft-kv" height="50">
  <img class="theme-dark-only" src="docs/media/simraft-lockup-dark.svg" alt="simraft-kv" height="50">
</p>

`simraft-kv` is a replicated key-value store built around a pure sans-I/O Raft consensus core (`simraft-core`), a deterministic single-seed chaos and linearizability simulator (`simraft-sim`), a durable TCP server and client (`simraft-node`), and a zero-dependency Python SDK (`simraft_kv`).

<p>
  <img src="docs/media/money-shot.gif" alt="simraft-kv five-node chaos partition sweep, per-event safety oracles, Wing and Gong linearizability verdict, and live TCP cluster" width="860">
</p>

## One Pure Core, Two Drivers

<p>
  <img class="theme-light-only" src="docs/media/dual-driver-chaos-light.svg" alt="How simraft-sim and simraft-node both drive the identical std-only simraft-core, simraft-kv, and simraft-wire crates, plus recorded latency and chaos baselines" width="880">
  <img class="theme-dark-only" src="docs/media/dual-driver-chaos-dark.svg" alt="How simraft-sim and simraft-node both drive the identical std-only simraft-core, simraft-kv, and simraft-wire crates, plus recorded latency and chaos baselines" width="880">
</p>

`simraft-core` and `simraft-kv` depend only on the Rust standard library (`#![forbid(unsafe_code)]`) and perform zero network, filesystem, or clock I/O. Both `simraft-sim` and `simraft-node` drive `Node::step(Event) -> Vec<Action>` and frame peer messages through `simraft-wire` (`"SRKV"`).

## Durability-Gated Quorum & Clock-Free `ReadIndex`

<p>
  <img class="theme-light-only" src="docs/media/step-quorum-timeline-light.svg" alt="Space-time protocol sequence inside simraft-core::Node::step showing durability-gated quorum commitment (T-02, S-06) and clock-free ReadIndex reads (R-01..R-04) across a partitioned 3-node cluster" width="880">
  <img class="theme-dark-only" src="docs/media/step-quorum-timeline-dark.svg" alt="Space-time protocol sequence inside simraft-core::Node::step showing durability-gated quorum commitment (T-02, S-06) and clock-free ReadIndex reads (R-01..R-04) across a partitioned 3-node cluster" width="880">
</p>

- **Durability watermarks (`T-02`, `B-02`, `R-06`).** Vote grants, `AppendEntries` acknowledgments, coalesced leader sends, single-voter commits, and `ReadReady` notifications wait on explicit `Event::Persisted` acknowledgments.
- **Operational Raft protocol.** Pre-Vote with leader recency (`T-03..T-05`), Figure 8 current-term commitment (`S-06`), per-term fast conflict backup (`S-08`), lease-free `ReadIndex` (`R-01..R-04`), chunked SHA-256 `InstallSnapshot` (`N-01..N-08`), and single-server membership changes (`M-01..M-05`).
- **Continuous safety & linearizability verification.** `simraft-sim --chaos` checks `S-01..S-05` on every event, verifies post-heal convergence (`H-02`), and runs a memoized per-key Wing and Gong linearizability oracle (`R-05`).

## Chapters in This Book

- **[Quickstart](./quickstart.md):** Install `simraft-sim`, `simraft-node`, or `simraft-kv` (PyPI) and run a chaos sweep or three-node localhost cluster.
- **[CLI, TCP Cluster, and Python SDK](./cli-and-python.md):** Simulator flags, JSON output, daemon & client exit codes, and `SimRaftClient` usage.
- **[Architecture](docs/ARCHITECTURE.md) & [Invariants](docs/INVARIANTS.md):** State machine rules, `"SRKV"` / `"SRSN"` / `"SRHS"` / `"SRLG"` schemas, and invariant traceability.
- **[Comparison](docs/COMPARISON.md) & [FAQ](docs/FAQ.md):** Trade-offs against `tikv/raft-rs`, `openraft`, and `etcd/raft`, plus operational answers.
