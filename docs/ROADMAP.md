# simraft-kv Roadmap and Delivery History

This document tracks the delivered milestones, engineering lanes, and
point releases across all five workspace crates (`simraft-core`,
`simraft-wire`, `simraft-kv`, `simraft-node`, and `simraft-sim`) as
well as the Python SDK (`simraft-kv` on PyPI).

Every core milestone and engineering lane landed in two stages. Stage
A introduced the public interfaces and failing tests, and Stage B
implemented the minimal design that passed those tests and
synchronized `docs/ARCHITECTURE.md` and `docs/INVARIANTS.md` to the
code as built.

## Completed in v0.2.1

- Added the pure-stdlib `"SRCL"` v1 linearizable TCP client
  (`SimRaftClient`, `ClientReply`, `encode_client_request`,
  `decode_client_response`, and `find_node_binary`) to the Python SDK
  (`python/simraft_kv/__init__.py`), supporting automatic leader
  redirect discovery, candidate list leader promotion, and transient
  retry with preserved `(client_id, seq)` session deduplication.
- Added unit, mock TCP redirect and deduplication, and live 3-node
  `simraft-node serve` failover tests to `python/tests/test_sdk.py`,
  and updated `ci.yml` to build both `simraft-sim` and `simraft-node`
  before running the Python SDK suite.
- Upgraded workspace dependencies (`sha2 0.11`, `criterion 0.8`, and
  `clap 4.6.10`) while preserving the `1.88` MSRV gate.
- Added `.github/workflows/verify-published.yml` to smoke-test
  published `0.2.1` crates.io libraries, CLI binaries, and PyPI
  packages in clean environments, and pinned `actions/checkout` to
  `v7.0.1` across GitHub Actions workflows.
- Converted all root `README.md` media and documentation links to
  canonical GitHub URLs so PyPI renders images and repository links
  without broken relative paths.

## Completed in v0.2.0

- Unified the five-crate workspace release (`simraft-core`,
  `simraft-wire`, `simraft-kv`, `simraft-node`, and `simraft-sim`) at
  version `0.2.0`.
- Enforced single-voter durability-gated commitment (`R-06`) and
  persisted-entry watermark clamping across follower log truncations
  (`T-02`) so neither single-node commits nor stale persistence
  acknowledgments can bypass durable storage.
- Reduced single-entry `AppendEntries` heap allocations inside
  `Node::step` to `4` within `APPEND_BUDGET = 7` (`L-03`), alongside
  `0` allocations on `Tick` and `2` on `RequestVote`.
- Completed live multi-process TCP durability and recovery test suites
  for torn log tail truncation, mid-burst leader kill durability, and
  wiped-member snapshot rejoin (`A-07` through `A-09`), plus post-heal
  leader and state digest convergence checks (`H-02`) in
  `simraft-sim`.
- Shipped the typed Python chaos-sweep SDK (`simraft-kv` on PyPI with
  5-platform maturin wheels) and GitHub Actions OIDC Trusted
  Publishing workflows for crates.io, PyPI, and 5-platform GitHub
  Releases.

## Completed in v0.1.0

### Milestones M1 through M4

- `M1` delivered the zero-dependency sans-I/O `simraft-core` state
  machine with Pre-Vote elections, leader recency checks, fast
  conflict log backup, monotonic `match_index`, and timing ratio
  validation.
- `M2` delivered Figure 8 commitment, clock-free linearizable reads
  via `ReadIndex`, and the `simraft-kv` state machine with two-layer
  client session deduplication.
- `M3` delivered `simraft-kv` snapshot payloads with domain-separated
  SHA-256 verification, chunked `InstallSnapshot` transfer, and
  single-server membership changes (`AddLearner`, `PromoteVoter`,
  and `RemoveVoter`).
- `M4` delivered the single-threaded `simraft-sim` virtual-clock
  harness, simulated volatile and durable disks, five per-event
  safety oracles, Wing and Gong per-key linearizability checking,
  and automated CI seed sweeps.

### Lane 1 (`simraft-wire` and `simraft-node`)

- Extracted `simraft-wire` so the simulator and live TCP adapter
  share one 16 MiB bounded CRC-32 frame codec.
- Built `simraft-node` with durable CRC-checked hard-state and log
  files, atomic SHA-256 verified snapshot persistence (`A-05` and
  `A-06`), bounded channels and socket timeouts, and the `serve` and
  `client` CLI subcommands (`A-01` through `A-04`).

### Lane 2 (Adversarial simulation depth)

- Added in-flight wire bit-flip corruption (`H-04`), durable log
  corruption truncation (`H-05`), durable snapshot corruption
  rejection (`H-06`), rolling cluster restarts under load (`H-07`),
  partitions across snapshot transfers (`H-08`), learner catch-up
  under load (`H-09`), the `--chaos` fault profile on `simraft-sim`,
  and the 100,000-seed weekly chaos sweep.

### Lane 3 (Performance and resource bounds)

- Coalesced held leader `AppendEntries` sends per follower on
  persist acknowledgment (`B-02`), added 10,000-entry follower
  catch-up benchmarks (`B-01`), scaled the linearizability checker
  benchmark to 100,000 operations across 64 keys (`L-04`), tightened
  `step_allocations` to `APPEND_BUDGET = 7` (`L-03`), and enforced
  commit latency bounds (`L-02`).

### Lane 4 (Operability and embedding)

- Shipped `Node::stats` (`O-01`) for read-only inspection of role,
  term, leader, log cursors, and peer replication progress, and
  `simraft-core/examples/embed.rs` (`O-02`) as a single-file
  in-memory embedding reference.

## Parked beyond v0.2.1

Joint consensus membership changes remain intentionally parked
(`docs/ARCHITECTURE.md` section 14 A2). Single-server membership
changes serialize cleanly behind a single integer invariant (`M-01`)
and compute every quorum against one configuration (`M-05`). Any
future exploration of joint consensus or authenticated TLS transport
requires an explicit architectural decision update in `DECISIONS.md`
and dedicated rows in `docs/INVARIANTS.md`.
