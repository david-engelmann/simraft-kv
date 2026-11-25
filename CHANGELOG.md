# Changelog

All notable changes to the `simraft-kv` workspace are documented in
this file. The format follows Keep a Changelog and this project
adheres to Semantic Versioning.

## [0.1.0] - 2026-11-25

Initial public release of all five workspace crates at version
`0.1.0` (`simraft-core`, `simraft-wire`, `simraft-kv`,
`simraft-node`, and `simraft-sim`).

### Added

- `simraft-core` zero-dependency sans-I/O Raft consensus state
  machine supporting Pre-Vote leader election, leader recency checks,
  fast conflict log backup, Figure 8 commitment, single-voter
  durability-gated commitment (`R-06`), leader send coalescing
  (`B-02`), single-server membership changes (`AddLearner`,
  `PromoteVoter`, `RemoveVoter`), chunked `InstallSnapshot` transfer,
  clock-free linearizable reads via `ReadIndex`, uncommitted log tail
  backpressure (`Config::max_uncommitted_entries`), disjoint
  `ClusterConfig` validation, and `Node::stats` inspection (`O-01`).
- `simraft-wire` canonical message frame, log entry, and cluster
  configuration codec with 16 MiB frame bounds, streaming header
  validation (`inspect_frame_header`), and domain-separated IEEE
  CRC-32 integrity checks (`"SRKV/frame/v1"`).
- `simraft-kv` deterministic `BTreeMap` replicated key-value state
  machine with configurable bounded client session deduplication
  (`KvStateMachine::with_max_sessions`), 16 MiB command and snapshot
  payload bounds, strict trailing-byte rejection, and domain-separated
  FIPS 180-4 SHA-256 snapshot verification (`"simraft-kv/snapshot/v1"`).
- `simraft-node` TCP and file-backed Raft server runtime and CLI
  client (`serve` and `client put/get/delete`), stateful
  `simraft_node::Client` session API, collision-free per-call session
  identifiers in `run_client`, bounded peer and inbox channels, socket
  read and write timeouts, snapshot persistence fencing (`A-05` and
  `A-06`), and torn log tail truncation, mid-burst leader kill
  durability, and wiped-member rejoin via snapshot (`A-07` through
  `A-09`).
- `simraft-sim` single-threaded deterministic simulation harness,
  seeded chaos profile (`--chaos` and `FaultConfig::chaos`), virtual
  clock and priority event queue, simulated volatile and durable disk
  model, five per-event safety oracles, bounded healing and leader
  convergence checks (`H-02`), domain-separated state and log digests
  (`"simraft-kv/state-digest/v1"` and `"simraft-kv/log-digest/v1"`),
  and per-key Wing and Gong linearizability verification.
- Public documentation index (`docs/README.md`), per-crate `README.md`
  files, light and dark SVG lockups and architecture and baseline
  diagrams, a `1200x630` social card, and a real-output
  `docs/media/money-shot.gif` walkthrough.
