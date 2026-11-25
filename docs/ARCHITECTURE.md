# simraft-kv Architecture

This document specifies the as-built `v0.1.0` architecture across all
five crates in the `simraft-kv` workspace. Any change to protocol
contracts or byte formats requires updating `DECISIONS.md`, this
specification, and the corresponding verification rows in
`docs/INVARIANTS.md`.

## 1. Purpose and scope

`simraft-kv` is a replicated key-value store built on a pure sans-I/O
Raft consensus core. Two architectural rules shape every component.

1. Consensus state transitions are pure functions of explicit inputs.
   `simraft-core` reads no wall clock, opens no socket, touches no
   file, spawns no thread, and draws no operating system entropy.
   Time, network I/O, durable storage, and timer offsets enter as
   `Event` values and exit as `Action` values.
2. Because the core is pure, an entire cluster executes inside a
   single-threaded deterministic simulator (`simraft-sim`) driven by
   one 64-bit seed while the production TCP and file adapter
   (`simraft-node`) runs the identical core and wire codec across OS
   processes.

In scope are leader election with Pre-Vote and leader recency checks,
log replication with fast conflict backup, Figure 8 commitment and
single-voter durability-gated commitment, clock-free linearizable
reads via `ReadIndex`, bounded two-layer client session
deduplication, snapshots with chunked `InstallSnapshot` transfer,
single-server membership changes, deterministic chaos simulation with
per-event safety oracles and Wing and Gong linearizability checking,
and the TCP and file-backed node server and client CLI.

Out of scope are Byzantine fault tolerance, multi-threaded core
consensus, joint consensus membership changes, clock-bound leader
leases, stale reads, and built-in TLS or authentication.

## 2. System overview

```text
               Propose / ReadIndex / ProposeConfig
                                |
                                v
  +-------------------------------------------------------------+
  | Driver (simraft-sim Harness or simraft-node runtime)        |
  | owns clock, sockets or packet queue, disk files or SimDisk  |
  +-------------------------------------------------------------+
        | Event                                ^ Action
        v                                      |
  +-------------------------------------------------------------+
  | simraft-core  Node::step(Event) -> Vec<Action>              |
  | roles, log, commit rule, ReadIndex rounds, membership       |
  +-------------------------------------------------------------+
        | Apply (index, LogEntry)              ^ snapshot bytes
        v                                      |
  +-------------------------------------------------------------+
  | simraft-kv  KvStateMachine                                  |
  | BTreeMap store, bounded sessions, command & snapshot codecs |
  +-------------------------------------------------------------+
```

The driver owns transport, storage, and scheduling while
`simraft-core` and `simraft-kv` own consensus and state-machine
decisions. Both `simraft-sim` and `simraft-node` speak `simraft-wire`
frames on peer links and section 7.3 record formats on durable
storage.

## 3. Crate and module boundaries

All five crates live at the workspace root under `Cargo.toml`
(`edition = "2024"`, `rust-version = "1.88"`).

| Crate | Kind | External dependencies | Responsibility |
| --- | --- | --- | --- |
| `simraft-core` | library | none (`std` only) | Sans-I/O Raft consensus state machine (`Node::step`) and `Node::stats`. |
| `simraft-wire` | library | `crc32fast` | Canonical 16 MiB bounded `"SRKV"` message frame, entry, and config codec. |
| `simraft-kv` | library | none (`std` only) | `KvStateMachine`, bounded session table, canonical command codec, and FIPS 180-4 SHA-256 verified snapshot codec. |
| `simraft-node` | library and binary | `clap`, `crc32fast` | Durable file store, TCP peer and client runtime, `Client` library API, and `serve` and `client` CLI subcommands. |
| `simraft-sim` | library and binary | `clap`, `crc32fast`, `sha2` | Deterministic virtual-clock harness, `--chaos` fault injector, safety oracles, linearizability checker, and CLI. |

Dependency edges flow strictly one way. `simraft-core` depends only
on `std`. `simraft-kv` and `simraft-wire` depend on `simraft-core`.
`simraft-node` and `simraft-sim` depend on `simraft-core`,
`simraft-kv`, and `simraft-wire`. `criterion` and `proptest` appear
solely in `[dev-dependencies]`. Every crate root carries
`#![forbid(unsafe_code)]` and denies `clippy::all`,
`clippy::pedantic`, `clippy::nursery`, `clippy::todo`, and
`clippy::unimplemented`.

### 3.1 `simraft-core` modules

| Module | Contents |
| --- | --- |
| `config` | `Config` timing and capacity bounds and `Config::validate` |
| `ids` | `NodeId`, `ClientId`, `Term`, `LogIndex`, and `Tick` types |
| `error` | `ConfigError`, `ProtocolError`, and `SnapshotError` with full diagnostic fields |
| `message` | Structured Raft RPC request and response types (`Message`) |
| `event` | `Event` and `PersistKind` inputs to `Node::step` |
| `action` | `Action`, `ClientResult`, and `ConfigResult` outputs |
| `log` | Contiguous `Log` storage offset by snapshot `base_index` and `base_term` |
| `snapshot` | `SnapshotMeta` and sequential chunk receiver state (`IncomingSnapshot`) |
| `membership` | `ClusterConfig`, `ConfigChange`, and `ConfigState` single-server rules |
| `node` | `Node`, `Role`, `NodeStats`, `PeerStats`, `ReadIndex` round tracking, `Node::step`, and `Node::stats` |

### 3.2 `simraft-kv` modules

| Module | Contents |
| --- | --- |
| `command` | `KvOp`, `CommandResult`, and 16 MiB bounded canonical command and result codecs |
| `error` | `KvError` diagnostic error type with offsets, bounds, and hex digests |
| `machine` | `KvStateMachine` (`BTreeMap` store and bounded LRU-by-apply session table) |
| `snapshot` | `SnapshotParts` and `"SRSN"` snapshot codec with self-contained FIPS 180-4 SHA-256 |

### 3.3 `simraft-wire` and `simraft-node` modules

| Crate and module | Contents |
| --- | --- |
| `simraft-wire::lib` | `encode_frame`, `decode_frame`, `inspect_frame_header`, entry and config codecs, and `WireError` |
| `simraft-node::store` | `FileStore` durable `"SRHS"`, `"SRLG"`, and `"SRSS"` files with atomic writes and torn-tail truncation |
| `simraft-node::runtime` | `serve` loop, `Driver`, bounded `sync_channel` peer and inbox queues, socket timeouts, and `FrameBuffer` |
| `simraft-node::client` | Stateful `Client`, one-shot collision-free `run_client`, and bounded TCP client wire protocol |
| `simraft-node::main` | `simraft-node` binary (`serve` and `client put/get/delete`) |

### 3.4 `simraft-sim` modules

| Module | Contents |
| --- | --- |
| `rng` | Deterministic SplitMix64 seed expansion into xoshiro256starstar |
| `clock` | Priority event queue ordered by `(delivery_tick, insertion_seq)` |
| `disk` | `SimDisk` with separate volatile and fsynced durable regions |
| `faults` | `FaultConfig`, `FaultConfig::chaos`, `DirectedCut`, and scripted actions |
| `harness` | Multi-node simulation loop, compaction, healing checks, and domain-separated digests |
| `oracle` | Per-event safety oracles (`S-01` through `S-05`) and monotonicity checks |
| `history` | Client invocation and completion history recorder |
| `liz` | Per-key Wing and Gong linearizability checker with visited-state memoization |
| `trace` | Canonical trace formatter and domain-separated SHA-256 helper |
| `main` | `simraft-sim` CLI (`--seed`, `--seeds`, `--chaos`, `--json`, `--trace`, `--nodes`, `--ticks`) |

## 4. The sans-I/O protocol

### 4.1 Events (`simraft-core::Event`)

```text
Event::Tick { now: Tick }
Event::Propose { client_id: ClientId, seq: u64, payload: Vec<u8> }
Event::ProposeConfig { change: ConfigChange }
Event::ReadIndex { request_id: u64, key: Vec<u8> }
Event::Message { from: NodeId, msg: Message }
Event::Persisted { kind: PersistKind, up_to_index: LogIndex }
Event::SnapshotPersisted { meta: SnapshotMeta }
Event::SnapshotLoaded { meta: SnapshotMeta, data: Vec<u8> }
Event::Restored { hard_state: HardState, entries: Vec<LogEntry>, snapshot: Option<SnapshotMeta> }
```

`Tick` counts virtual or monotonic milliseconds supplied by the
driver. `Event::Persisted` acknowledges that prior
`Action::PersistHardState` or `Action::PersistEntries` durable writes
finished fsyncing. A node withholds dependent vote responses, append
responses, single-voter commits, held leader replication sends, and
`ReadReady` notifications until matching persistence is acknowledged
(`T-02` and `R-06`).

When multiple proposals arrive before `PersistEntries` completes, the
leader coalesces held `AppendEntries` sends per follower on release
so only the newest cumulative batch is transmitted (`B-02`).

### 4.2 Actions (`simraft-core::Action`)

```text
Action::Send { to: NodeId, msg: Message }
Action::PersistHardState { current_term: Term, voted_for: Option<NodeId> }
Action::PersistEntries { from_index: LogIndex, entries: Vec<LogEntry> }
Action::PersistSnapshot { meta: SnapshotMeta, data: Vec<u8> }
Action::LoadSnapshot { meta: SnapshotMeta }
Action::Apply { index: LogIndex, entry: LogEntry }
Action::ClientResponse { client_id: ClientId, seq: u64, result: ClientResult }
Action::ConfigResponse { change: ConfigChange, result: ConfigResult }
Action::ReadReady { request_id: u64, key: Vec<u8>, read_index: LogIndex }
Action::ReadRejected { request_id: u64, leader: Option<NodeId> }
Action::StepDown { term: Term }
```

`ClientResult` variants report `Committed`,
`NotLeader { leader: Option<NodeId> }`, or
`Rejected { error: ProtocolError }` (emitted when the uncommitted log
tail reaches `Config::max_uncommitted_entries`). Drivers also call
`Node::poll_timeout` to jump directly to the next scheduled timer
deadline and `Node::stats` to inspect operating state without mutating
the node (`O-01`).

### 4.3 Determinism rules

- `simraft-core`, `simraft-kv`, `simraft-wire`, and `simraft-sim`
  contain zero `HashMap`, `HashSet`, `SystemTime`, `Instant::now`,
  `thread_rng`, or `OsRng` references (`D-02`). Ordered `BTreeMap`
  and `BTreeSet` collections guarantee identical iteration order on
  every platform.
- Protocol logic uses integer arithmetic only without floating point.
- `Node::step` is total across every role and message interleaving,
  never panicking on stale, duplicated, reordered, or out-of-range
  protocol inputs.

## 5. Core data structures

- **Persistent state** covers `current_term`, `voted_for`, durable
  `LogEntry` records, and optional `SnapshotMeta`.
- **Log storage** holds a contiguous `Vec<LogEntry>` preceded by
  `base_index` and `base_term`. Entry `i` resides at slice index
  `i - base_index - 1`. Term lookups and conflict-boundary scans run
  over contiguous memory without per-entry heap nodes.
- **Cluster configuration** (`ClusterConfig`) stores disjoint
  non-empty `voters: BTreeSet<NodeId>` and `learners: BTreeSet<NodeId>`
  validated by `ClusterConfig::new` (`ConfigError::EmptyVoters` and
  `ConfigError::OverlappingMember`). `ConfigState` recomputes the
  committed and latest configurations from the snapshot base and log
  suffix after every log or commit change.

## 6. Roles, elections, replication, and commitment

Roles are `Follower`, `PreCandidate`, `Candidate`, and `Leader`.
Learners run the `Follower` transition rules with voting and election
campaigns disabled (`M-04`).

### 6.1 Pre-Vote and leader recency

Every election timeout enters `PreCandidate` first and broadcasts
`RequestPreVote` for `current_term + 1` without mutating persistent
state (`T-03` and `T-04`). A voter grants a pre-vote only when the
candidate log is at least as up to date by `(last_log_term,
last_log_index)` (`S-10`), the hypothetical term is at least
`current_term + 1`, and the voter has not heard from a current-term
leader within `min_election_timeout` (`T-05`). Only after collecting
a pre-vote quorum does a candidate persist `current_term + 1` and
`voted_for = Some(self)` and send `RequestVote`.

### 6.2 Replication, fast backup, and verified prefix commit

Leaders bound `AppendEntries` batches by
`Config::max_entries_per_append` (default 64) and
`Config::max_append_bytes` (default 1 MiB). Followers reject
mismatched `(prev_log_index, prev_log_term)` with fast-backup
`(conflict_index, conflict_term)` hints so leaders back up one term
per round trip (`S-08`), and leaders update `match_index`
monotonically (`S-07`).

When a follower accepts an `AppendEntries` request, it advances
`commit_index` to `min(leader_commit, verified_up_to)` where
`verified_up_to` is `prev_log_index + entries.len()` (or
`log.base_index()` when `prev_log_index < log.base_index()`), never
`log.last_index()`. An empty heartbeat therefore never commits
unverified divergent entries sitting past `prev_log_index` (`S-05`).

### 6.3 Figure 8 and single-voter commitment

Leaders count replicas only for entries from `current_term` (`S-06`)
and append an immediate `NoOp` entry upon election. The leader counts
its own replica at `entries_durable_up_to` once its current-term hard
state has been acknowledged durable (`hard_gen_acked >=
hard_gen_sent`), and `on_persisted` runs `advance_commit` and
`try_release_reads` directly. Consequently, a single-voter cluster
commits and serves reads as soon as its local persistence completes
while strictly preserving `T-02` (`R-06`).

## 7. Byte-level schemas and domain separation

All multi-byte integers use little-endian encoding. Wire frames,
client requests and responses, and KV command and snapshot payloads
enforce a strict 16 MiB maximum payload bound (`16 * 1024 * 1024`
bytes) and reject trailing bytes (`E-01` and `A-04`).

### 7.1 Wire frame (`simraft-wire`)

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 4 | ASCII magic `"SRKV"` |
| 4 | 2 | Format version `u16 = 1` |
| 6 | 1 | Message kind (`1..=8`) |
| 7 | 1 | Flags `u8 = 0` |
| 8 | 8 | Sender `term: u64` |
| 16 | 8 | Sender `from: u64` |
| 24 | 8 | Receiver `to: u64` |
| 32 | 4 | `body_len: u32` (at most `16,777,216`) |
| 36 | `body_len` | Canonical RPC body |
| `end - 4` | 4 | IEEE CRC-32 over `"SRKV/frame/v1"` plus header and body |

### 7.2 Log entry, cluster config, and command payloads

Log entries encode `term: u64` and `kind: u8` (`0` = `NoOp`, `1` =
`Command`, `2` = `Config`). `ClusterConfig` writes voter and learner
`NodeId` lists in ascending order. `simraft-kv` commands encode `Put`
(`tag = 0`, `key_len: u32`, `key`, `value_len: u32`, `value`) and
`Delete` (`tag = 1`, `key_len: u32`, `key`), and command outcomes
encode `Ack` (`0`), `Value` (`1`), or `Error` (`2`).

### 7.3 Durable disk records (`simraft-sim` and `simraft-node`)

- **Hard state** (`"SRHS"`, version `1`) stores `record_len`,
  `current_term`, `voted_for`, and IEEE CRC-32 under domain
  `"SRKV/hard/v1"`.
- **Log records** (`"SRLG"`, version `1`) store `record_len`,
  `index`, encoded entry bytes, and IEEE CRC-32 under domain
  `"SRKV/log/v1"`. Torn tail records truncate cleanly at recovery
  with full diagnostic reporting (`H-05` and `A-07`).
- **Adapter snapshot file** (`simraft-node::FileStore`) wraps the
  section 7.4 KV snapshot payload with `"SRSS"` metadata and IEEE
  CRC-32 under domain `"SRKV/snap/v1"` via write-temp, fsync, and
  atomic rename.

### 7.4 KV snapshot payload (`simraft-kv`)

The `"SRSN"` version `1` payload writes `last_included_index`,
`last_included_term`, canonical `ClusterConfig`, ascending `BTreeMap`
key-value pairs, ascending client session entries, and a trailing
32-byte FIPS 180-4 SHA-256 digest over domain
`"simraft-kv/snapshot/v1"` plus all preceding bytes (`N-03`).

### 7.5 Domain separation tags

| Domain prefix | Algorithm | Purpose |
| --- | --- | --- |
| `"simraft-kv/state-digest/v1"` | SHA-256 | Final simulator KV state digest |
| `"simraft-kv/log-digest/v1"` | SHA-256 | Simulator per-node and final log digest |
| `"simraft-kv/snapshot/v1"` | SHA-256 | `simraft-kv` snapshot payload integrity |
| `"SRKV/frame/v1"` | CRC-32 | `simraft-wire` peer message frame integrity |
| `"SRKV/hard/v1"` | CRC-32 | Durable hard-state record integrity |
| `"SRKV/log/v1"` | CRC-32 | Durable log record integrity |
| `"SRKV/snap/v1"` | CRC-32 | `simraft-node` snapshot file envelope |

## 8. Clock-free linearizable reads (`ReadIndex`)

`ReadIndex` uses zero wall-clock or virtual-clock leases (`R-04`). A
leader queues reads until an entry from `current_term` commits
(`R-01`), records `read_index = commit_index`, assigns a monotonic
`read_round` (`R-03`), confirms leadership via quorum heartbeat
echoes (or local durability on a single-voter cluster, `R-06`), and
emits `Action::ReadReady` once `last_applied >= read_index` (`R-02`).

## 9. Single-server membership changes

At most one configuration entry may remain uncommitted (`M-01`), and
newly elected leaders reject `ProposeConfig` until their current-term
`NoOp` commits (`M-02`). Supported transitions are `AddLearner`,
`PromoteVoter` (requiring `match_index >= commit_index`, `M-04`), and
`RemoveVoter` (stepping down only after self-removal commits, `M-03`).
Every quorum uses the single newest configuration in the node log
(`M-05`).

## 10. Client sessions, deduplication, and backpressure

1. **In-flight deduplication and tail cap** (`simraft-core`) maps
   `(ClientId, seq)` across uncommitted entries so retries attach
   without appending duplicate entries (`C-02`). When
   `last_index - commit_index >= Config::max_uncommitted_entries`,
   new proposals are rejected immediately with
   `ProtocolError::TooManyUncommitted`.
2. **Committed session table** (`simraft-kv`) caches
   `(last_seq, CommandResult)` for up to `Config::max_sessions`
   clients via `KvStateMachine::with_max_sessions` (`C-01` and
   `C-03`), evicting
   the least recently applied session when full and rejecting
   sequence gaps (`seq > last_seq + 1`) with `KvError::SessionGap`
   (`C-04`).
3. **Client session generation** (`simraft-node`) combines a
   per-process mixed prefix with an atomic monotonic counter so
   rapid `run_client` calls never collide on `(ClientId, seq)`, while
   `simraft_node::Client` preserves one session across sequential
   operations (`A-01`).

## 11. Configuration validation

`Config::validate` rejects zero intervals or capacity limits,
`min_election_timeout < 10 * heartbeat_interval` (`T-01`), and
`max_election_timeout < 2 * min_election_timeout`, naming both
offending values in `ConfigError`.

## 12. Deterministic simulation (`simraft-sim`)

- **Virtual clock and PRNG** run on a single thread using an internal
  SplitMix64 and xoshiro256starstar generator so seeds preserve exact
  behavior across Rust and dependency upgrades (`D-01`).
- **Chaos profile** (`FaultConfig::chaos` and `--chaos`) injects
  seeded packet drops, duplicates, variable latency, 15 per mille
  frame bit-flip corruption, crashes wiping volatile disk state,
  virtual clock jumps, and intermittent asymmetric, chain, and
  single-node isolation partitions (`H-01` through `H-09`).
- **Bounded healing** removes faults at `heal_start_tick`, bounds
  delivery latency below `min_election_timeout / 4`, verifies that a
  single stable leader converges within `3 * max_election_timeout`
  ticks (`RunReport::leader_converged`), verifies log and KV state
  convergence across all active configuration members, and checks
  per-key Wing and Gong linearizability (`H-02` and `R-05`).

## 13. Diagnostic error model

Every error enum (`ConfigError`, `ProtocolError`, `SnapshotError`,
`KvError`, `WireError`, and `NodeError`) implements
`std::error::Error` and `Display` with concrete byte offsets, terms,
log indexes, expected bounds, and digests (`E-01`).

## 14. Rejected alternatives

1. **Async or multi-threaded consensus core** was rejected because OS
   thread scheduling and wall clocks prevent single-seed deterministic
   simulation and exact failure replay.
2. **Joint consensus membership** was rejected in favor of
   single-server changes so every quorum consults one configuration
   and serialization is enforced by a single index comparison.
3. **Clock-bound leader leases** were rejected because virtual and
   real-world clock jumps or pauses can silently violate
   linearizability whereas quorum-confirmed `ReadIndex` depends only
   on message accounting.
4. **Tree-allocated log storage** was rejected in favor of a
   contiguous snapshot-offset `Vec<LogEntry>` matching sequential
   replication and prefix compaction access patterns.
5. **Wrapping an external Raft crate** was rejected so every
   consensus transition matches the pure `Event` and `Action`
   contract and maps directly to `docs/INVARIANTS.md`.
6. **Derived generic serialization** (`serde` wire or disk layouts)
   was rejected so canonical byte layouts stay fixed across compiler
   and library versions, and `simraft-sim` formats its compact JSON
   summaries directly without pulling `serde_json` into the workspace.

## 15. Verification summary

Automated gates in `.github/workflows/ci.yml` and
`.github/workflows/weekly-sweep.yml` run `rustfmt`, `clippy` with
warnings denied, `rustdoc` with warnings denied, MSRV `1.88`
verification, property suites (`proptest`), simulator scenarios and
`--chaos` sweeps, `simraft-node` multi-process end-to-end and crash
recovery suites, Criterion benchmarks including the thread-local
counting allocator gate in `simraft-core/benches/step_allocations.rs`
(`L-03`), Markdown typography guards, and the four-crate
nondeterminism identifier guard (`D-02`).

## 16. References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable
  Consensus Algorithm*, USENIX ATC 2014.
- Diego Ongaro, *Consensus, Bridging Theory and Practice*, Stanford
  University doctoral dissertation, 2014.
- Jepsen, FoundationDB deterministic simulation testing, and
  TigerBeetle VOPR fault-injection and bounded-healing architectures.
- Jeannette M. Wing and C. Gong, *Testing and Verifying Concurrent
  Objects*, Journal of Parallel and Distributed Computing, 1993.
