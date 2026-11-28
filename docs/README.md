# simraft-kv Documentation Index

Protocol specifications, invariant traceability, and crate guides for the `simraft-kv` sans-I/O Raft workspace. The rendered documentation book is published at <https://david-engelmann.github.io/simraft-kv/>.

## Protocol Specification & Trade-offs

| Document | Contents |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Roles and state transitions, Pre-Vote with leader recency, fast conflict backup, Figure 8 commitment, clock-free `ReadIndex`, chunked snapshots, single-server membership, `"SRKV"` / `"SRSN"` byte layouts, and `--chaos` simulation model. |
| [INVARIANTS.md](INVARIANTS.md) | Traceability matrix (`S-01..S-10`, `T-01..T-06`, `R-01..R-06`, `C-01..C-04`, `M-01..M-05`, `N-01..N-08`, `H-01..H-09`, `D-01..D-03`, `L-01..L-04`, `A-01..A-09`, `X-01..X-03`) naming every enforcing oracle, property test, and benchmark. |
| [COMPARISON.md](COMPARISON.md) | Architectural comparison with `tikv/raft-rs`, `openraft`, `etcd/raft`, and FoundationDB / TigerBeetle simulation testing. |
| [FAQ.md](FAQ.md) | Answers on durability watermarks (`Event::Persisted`), `ReadIndex` vs leases, session deduplication, and TLS deployment scope. |

## Crate, CLI, and Agent Guides

| Document | Contents |
| --- | --- |
| [Root README](../README.md) | Sans-I/O `Node::step` contract, quorum sequence diagram, `--chaos` simulation, TCP cluster commands, and Python `simraft_kv` SDK. |
| [simraft-core README](../simraft-core/README.md) | Zero-dependency consensus state machine (`Node`, `Event`, `Action`, `Config`, `ClusterConfig`). |
| [simraft-kv crate README](../simraft-kv/README.md) | Deterministic `KvStateMachine`, LRU session table, and SHA-256 verified `"SRSN"` snapshots. |
| [simraft-wire README](../simraft-wire/README.md) | Bounded 16 MiB `"SRKV"` peer message frame, log entry, and cluster config codec. |
| [simraft-node README](../simraft-node/README.md) · [simraft-sim README](../simraft-sim/README.md) | Durable TCP server/client CLI and deterministic `--chaos` simulator / Wing & Gong checker. |
| [examples/README.md](../examples/README.md) · [AGENTS.md](../AGENTS.md) | Runnable chaos sweep, trace replay, and localhost cluster scripts plus agent integration guide. |

## Project Records & Policies

| Document | Contents |
| --- | --- |
| [DECISIONS.md](../DECISIONS.md) · [SECURITY.md](../SECURITY.md) | Architectural trade-off records, defensive frame/session caps, and vulnerability reporting. |
| [CONTRIBUTING.md](../CONTRIBUTING.md) · [ROADMAP.md](ROADMAP.md) · [CHANGELOG.md](../CHANGELOG.md) | Determinism source guards, local verification gates, milestone records, and release notes. |
