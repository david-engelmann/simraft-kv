# simraft-kv Invariants and Traceability Matrix

Status: Milestone 0 design. This is the normative list of formal invariants
for simraft-kv. Every invariant has exactly one row here, and every row names
the exact property test, fault oracle, tamper test, or benchmark that
enforces it. Naming follows the code that Milestones 1 to 4 will create:
property tests live in tests/ of the named crate, simulator oracles live in
simraft-sim/src/oracle.rs, simulator scenario tests live in
simraft-sim/tests/, and benchmarks live in benches/ of the named crate.
A claim in docs/ARCHITECTURE.md with no row here is not a guarantee.

Categories: SAF safety, DUR durability, LIV liveness under stated bounds,
LAT latency and resource bounds, DET determinism, ERR diagnostics.

## 1. Core Raft safety

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| S-01 | Election Safety (SAF): at most one leader per term across all nodes, checked after every tick. Two nodes are never both Leader with the same current_term. | Oracle `oracle::election_safety` run every simulator tick; property test `simraft-core/tests/property_election.rs::one_leader_per_term` over randomized vote interleavings. | M1, oracle M4 |
| S-02 | Leader Append-Only (SAF): a leader never overwrites or deletes entries in its own log. A leader's log changes only by appends at the tail and by snapshot base advancement. | Oracle `oracle::leader_append_only`, which snapshots each leader's log per tick and diffs prefixes; property test `simraft-core/tests/property_log.rs::leader_log_is_append_only`. | M1, oracle M4 |
| S-03 | Log Matching (SAF): if two logs contain an entry at the same index with the same term, the logs are identical in all entries up to that index. | Oracle `oracle::log_matching` every tick over all node pairs; property test `simraft-core/tests/property_log.rs::matching_index_term_implies_matching_prefix`. | M1, oracle M4 |
| S-04 | Leader Completeness (SAF): if an entry is committed in a term, that entry is present in the log of every leader for every higher term. | Oracle `oracle::leader_completeness` every tick; simulator scenario `simraft-sim/tests/scenario_commit_survival.rs` (commit, partition, force successive elections, compare logs). | M1, oracle M4 |
| S-05 | State Machine Safety (SAF): no two nodes apply different commands at the same log index. Applied command sequences are prefix comparable on all nodes at all times. | Oracle `oracle::state_machine_safety` comparing applied vectors every tick; property test `simraft-kv/tests/property_apply.rs::apply_sequences_are_prefix_comparable`. | M2, oracle M4 |
| S-06 | Figure 8 commitment (SAF): a leader advances commit_index by counting replicas only for an entry from its own current term. Earlier-term entries commit only as the prefix of a committed current-term entry. commit_index never decreases on any node. | Property test `simraft-core/tests/property_commit.rs::no_commit_by_counting_for_prior_term_entries`, including the paper's Figure 8 five-node sequence as a fixed regression case `figure8_replication_sequence_never_commits_s2_entry`; oracle assertion on monotonic commit_index every tick. | M2 |
| S-07 | Monotonic match_index (SAF): a leader never moves a follower's match_index backwards, including when AppendEntriesResponses arrive reordered, duplicated, or delayed past newer responses. next_index is never set below match_index + 1 by a stale response. | Property test `simraft-core/tests/property_progress.rs::stale_append_response_never_regresses_match_index` with explicitly reordered response sequences; simulator hold-and-release fault scenario `simraft-sim/tests/scenario_reordered_responses.rs`. | M1 |
| S-08 | Conflict fast backup correctness (SAF): on AppendEntries rejection, conflict_index and conflict_term follow the ARCHITECTURE section 6.2 definitions, and the leader's resulting next_index never skips an index the follower is missing and never lands inside a term the follower does not have without restarting at that term's first index. | Property test `simraft-core/tests/property_log.rs::conflict_hint_lands_on_term_boundary` over randomized divergent logs; convergence bound checked in the simulator (a divergent follower of T conflicting terms converges in at most T + 1 AppendEntries rounds in a fault-free window). | M1 |
| S-09 | Vote uniqueness (SAF): a node votes for at most one candidate per term, and that vote survives crash and restart. | Property test `simraft-core/tests/property_election.rs::one_vote_per_term_across_restart` (persist, crash, restore, second request in the same term). | M1 |
| S-10 | Up-to-date voting restriction (SAF): a vote or pre-vote is granted only to a candidate whose log is at least as up to date by (last_log_term, last_log_index) comparison. | Property test `simraft-core/tests/property_election.rs::stale_log_candidate_is_refused` over the full comparison truth table. | M1 |

## 2. Timing, persistence, and Pre-Vote

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| T-01 | Timing ratio validation (SAF, config): Config::validate rejects any config where min_election_timeout < 10 * heartbeat_interval or max_election_timeout < 2 * min_election_timeout, and rejects zero values for either, with both offending values in the error. | Unit and property tests `simraft-core/tests/property_config.rs::invalid_timing_ratios_are_rejected` and `::error_names_both_values`. | M1 |
| T-02 | Persist before response (DUR): current_term, voted_for, and newly appended log entries reach durable storage before the vote, append, or read response that depends on them is emitted. The core emits Persist actions and withholds the dependent response until the matching Persisted event arrives. | Simulator crash fault: `simraft-sim/tests/scenario_crash_before_persist_ack.rs` kills a node between append and Persisted and asserts no response was emitted and no committed entry is lost; structural test `simraft-core/tests/property_persist.rs::no_response_before_persist_ack` over the action stream. | M1 |
| T-03 | Pre-Vote purity (SAF): granting or denying a RequestPreVote never increments the voter's persistent current_term, never writes voted_for, and never resets the voter's election timer. | Property test `simraft-core/tests/property_prevote.rs::prevote_leaves_hard_state_and_timer_untouched`, asserting hard state bytes and deadline equality before and after. | M1 |
| T-04 | Pre-Vote hypothetical term (SAF): RequestPreVote carries current_term + 1 without the candidate incrementing its own persistent current_term. A candidate becomes Candidate only after a pre-vote majority, and only then persists term + 1 and its self-vote. | Property test `simraft-core/tests/property_prevote.rs::candidate_term_increments_only_after_prevote_majority`. | M1 |
| T-05 | Leader recency denial (LIV): a voter that accepted AppendEntries from a leader in the current term within its minimum election timeout denies pre-votes, so a partitioned or lagging node cannot disrupt a healthy leader by rejoining. | Simulator scenario `simraft-sim/tests/scenario_disruptive_rejoin.rs`: isolate one node for many election timeouts, heal, assert the original leader's term and leadership are unchanged and no election occurred. | M1, scenario M4 |
| T-06 | Randomized election timeouts (LIV): per-node election deadlines differ by a deterministic offset drawn from [min_election_timeout, max_election_timeout), so simultaneous timeouts and repeated split votes are not systematic. | Property test `simraft-core/tests/property_config.rs::deadline_offsets_stay_in_range_and_vary_by_node`; split-vote recovery asserted in simulator scenarios by leader emergence during healing (H-02). | M1 |

## 3. Commitment, ReadIndex, and reads

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| R-01 | Current-term gate (SAF): a leader never serves a ReadIndex read before it has committed an entry in its own current term. Requests arriving earlier queue until the election NoOp commits or the leader steps down. | Property test `simraft-core/tests/property_readindex.rs::read_waits_for_current_term_commit`; simulator scenario with a read issued in the same tick as the election. | M2 |
| R-02 | Quorum confirmation (SAF): a ReadIndex read is served only after heartbeat acknowledgments carrying its read round cover a quorum of the current config, and only after the leader's last_applied reaches the recorded read_index. | Property test `simraft-core/tests/property_readindex.rs::read_requires_quorum_round_acks`; fault scenario `simraft-sim/tests/scenario_stale_leader_read.rs` partitions the leader before the round completes and asserts the read never returns a value. | M2 |
| R-03 | Monotonic round tokens (SAF): read_round is strictly increasing within a leader term, tokens are scoped by (leader_id, term, round), and acks from an earlier term or a deposed leader never confirm a later read. Delayed acks from before a partition cannot confirm a read issued after it. | Property test `simraft-core/tests/property_readindex.rs::cross_term_acks_never_confirm_reads` with delayed and replayed responses. | M2 |
| R-04 | Clock-free reads (SAF): ReadIndex correctness uses no clock, lease, or timeout value. A virtual clock jump of any size, forward or to a value far past all deadlines, mid-read changes which reads are pending, never which values they may return. | Simulator scenario `simraft-sim/tests/scenario_clock_jump_read.rs` injects jumps larger than max_election_timeout during pending reads and runs the linearizability checker over the full history. | M2, scenario M4 |
| R-05 | Linearizable client histories (SAF): under the full chaos matrix, every completed client operation history recorded by the simulator is linearizable per key under the Wing and Gong checker, with pending operations treated as optionally linearized. An Unknown checker verdict fails the run. | Checker `simraft-sim/src/liz.rs`; scenario suite `simraft-sim/tests/scenario_linearizability.rs` across the CI seed ranges; checker self-tests `simraft-sim/tests/checker_verdicts.rs` pin known linearizable and non-linearizable histories (stale read, phantom value, lost update) to their expected verdicts. | M4 |


## 4. Client deduplication

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| C-01 | Committed dedup (SAF): a retried proposal whose (client_id, seq) is at or below the session's last_seq returns the cached response and never re-executes the operation. Put and Delete effects appear exactly once in the state machine. | Property test `simraft-kv/tests/property_session.rs::retry_returns_cached_response_without_reapplying` over randomized retry interleavings. | M2 |
| C-02 | In-flight dedup (SAF): a retry that arrives while the original entry is appended but uncommitted does not append a second log entry. Both requesters receive the same response when the single entry commits and applies. | Property test `simraft-core/tests/property_dedup.rs::inflight_retry_appends_no_second_entry`; simulator duplicate-proposal workload in `simraft-sim/tests/scenario_linearizability.rs`. | M2 |
| C-03 | Bounded session memory (LAT, SAF): session state holds at most one (last_seq, cached_response) pair per client and at most Config::max_sessions clients. Eviction removes only committed sessions, least recently applied first. A retry of an evicted pair may re-execute; this is the documented client contract, tested rather than hidden. | Property test `simraft-kv/tests/property_session.rs::session_table_never_exceeds_cap` and `::eviction_takes_only_committed_sessions`, which assert the table length and per-client entry count directly after every operation in the sequence. | M2 |
| C-04 | Sequence gap rejection (SAF, ERR): an applied entry with seq greater than last_seq + 1 is rejected as a protocol error naming the client_id, the expected seq, and the received seq. Sequence state survives snapshot install unchanged. | Unit test `simraft-kv/tests/property_session.rs::seq_gap_is_an_error_with_context`; snapshot round-trip test in N-03. | M2 |

## 5. Membership changes

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| M-01 | One uncommitted change (SAF): at most one config entry is ever uncommitted. A leader rejects ProposeConfig while last_config_index > commit_index, with both indexes in the error. | Property test `simraft-core/tests/property_membership.rs::second_config_change_rejected_while_first_uncommitted`. | M3 |
| M-02 | NoOp before config (SAF): a newly elected leader rejects config proposals until its current-term NoOp entry has committed. | Property test `simraft-core/tests/property_membership.rs::config_rejected_before_leader_noop_commits`. | M3 |
| M-03 | Commit-then-step-down (SAF): a leader that removes itself steps down only after the removal entry commits, and continues to replicate until it does. It never steps down on append and never remains leader in a config that excludes it once the removal is committed. | Simulator scenario `simraft-sim/tests/scenario_self_removal.rs` asserting role per tick around the commit point, then a fresh election in the new config. | M3 |
| M-04 | Learner semantics (SAF): learners never vote, never start elections, and never count toward any quorum (election, commit, or ReadIndex). PromoteVoter rejects a learner whose match_index is below the leader's commit_index, naming both indexes. | Property tests `simraft-core/tests/property_membership.rs::learners_never_vote_or_count` and `::promotion_requires_caught_up_learner`. | M3 |
| M-05 | Single-config quorums (SAF): every quorum decision (election win, commit advancement, ReadIndex confirmation) is computed against exactly one ClusterConfig, the newest whose entry exists in the deciding node's log. | Oracle `oracle::single_config_quorum` spot-checks decisions during simulator runs; code-level assertion in the quorum function exercised by `simraft-core/tests/property_membership.rs::quorum_uses_latest_single_config`. | M3 |

## 6. Snapshots and InstallSnapshot

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| N-01 | Automatic snapshot fallback (SAF, LIV): when a follower's next_index falls to or below the leader's snapshot base index, the leader sends InstallSnapshot instead of AppendEntries, without waiting for a failed append round first. | Property test `simraft-core/tests/property_snapshot.rs::leader_switches_to_snapshot_at_base_boundary`. | M3 |
| N-02 | Sequential chunks (SAF): snapshot chunks are accepted only in strictly increasing offset order, each chunk's offset equal to the bytes already buffered for that snapshot identity. An out-of-order, duplicated, or gapped chunk is rejected with the expected and received offsets. A chunk for a newer snapshot identity discards the older partial buffer exactly once, so leader changes cannot livelock the transfer. | Property test `simraft-core/tests/property_snapshot.rs::chunks_require_exact_sequential_offsets` and `::new_snapshot_identity_discards_stale_buffer`. | M3 |
| N-03 | Snapshot integrity and round trip (DUR): a snapshot whose SHA-256 digest (domain "simraft-kv/snapshot/v1") does not match is rejected whole and never installed, with expected and actual digests in the error. A valid snapshot installs store and session state identical to the snapshotted node: same digest input bytes, same dedup behavior afterward. | Tamper test `simraft-kv/tests/tamper_snapshot.rs::single_byte_flip_is_rejected_with_digests`; round-trip property test `simraft-kv/tests/property_snapshot_kv.rs::install_restores_store_and_sessions`. | M3 |
| N-04 | Prefix retention rule (SAF): on snapshot install, a follower retains the log suffix after last_included_index only when its log holds an entry at last_included_index with term equal to last_included_term; otherwise it discards its entire log. It never retains entries at or below the snapshot base. | Property test `simraft-core/tests/property_snapshot.rs::matching_prefix_retained_divergent_log_discarded` over both branches. | M3 |
| N-05 | Compaction safety (SAF): log truncation and snapshot base advancement never remove an uncommitted entry and never remove entries at or above commit_index on any path. last_applied and commit_index never exceed the snapshot base after install by more than the retained suffix length, and never decrease. | Oracle structural assertions every tick; property test `simraft-core/tests/property_snapshot.rs::compaction_never_touches_committed_or_uncommitted_boundary`. | M3 |

## 7. Chaos, healing, and determinism

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| H-01 | Chaos survival (SAF): during the chaos phase (asymmetric partitions including one-way cuts A->B and chains A->B, B->C with A and C cut both ways, drops, duplication, reordering, crashes that wipe un-fsynced volatile storage, and clock jumps beyond max_election_timeout) no safety oracle ever fires. Liveness is not asserted during chaos. | Scenario suite `simraft-sim/tests/scenario_chaos_matrix.rs` over the CI seed ranges (1,000 seeds per PR, 10,000 on main, DECISIONS Q6); oracles per section 1 run every tick of every run. | M4 |
| H-02 | Bounded healing (LIV): after partitions heal, drops stop, and latency is bounded below min_election_timeout / 4, the cluster reaches exactly one stable leader within 3 * max_election_timeout ticks, that leader stays leader for the rest of the phase, and all live nodes converge to identical log digests and identical KV state digests before the phase budget ends. | Healing assertions in `simraft-sim/tests/scenario_chaos_matrix.rs` and `simraft-sim/tests/scenario_disruptive_rejoin.rs`; failure prints the seed, the phase tick budget, and the divergent digests. | M4 |
| H-03 | Crash recovery (DUR): a crashed node restarts with exactly its durable state (hard state, fsynced log prefix, installed snapshot) and nothing from its volatile buffer. It never votes twice in a term it voted in before the crash (S-09) and never acknowledges entries it lost. | Disk model in `simraft-sim/src/disk.rs`; scenario `simraft-sim/tests/scenario_crash_matrix.rs` crashing every node at every phase of an append in rotation. | M4 |
| D-01 | Cross-process seed determinism (DET): two runs of the simulator in separate OS processes with the same SIM_SEED produce byte-identical trace files and byte-identical final state digests. | Test `simraft-sim/tests/determinism_cross_process.rs` spawns the simraft-sim binary twice per seed in a sample of seeds and diffs stdout traces and digest lines byte for byte; CI runs the same check in the seed sweep job. | M4 |
| D-02 | No hidden nondeterminism (DET): simraft-core, simraft-kv, and the deterministic paths of simraft-sim contain no HashMap or HashSet iteration, no SystemTime or Instant reads, and no OS randomness. All collections in protocol paths are BTreeMap or BTreeSet. | CI lint job greps the three crates' sources for the forbidden identifiers and fails on any hit outside an allow-listed comment; the grep list itself is under test (a planted violation in a scratch copy must be caught, gate blindness check). | M1 onward |
| D-03 | Seed visibility and replay (DET): every simulation and property run prints its active seed (SIM_SEED env or --seed flag) on its first output line, and any failing seed from CI can be replayed locally with that single value to the identical failure. | CLI contract test `simraft-sim/tests/cli_contract.rs::seed_is_printed_and_replays_failure`; CONTRIBUTING documents the workflow from M4. | M4 |

## 8. Latency, resource, and diagnostic bounds

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| L-01 | ReadIndex cost bound (LAT): a ReadIndex read costs at most one heartbeat round of messages (one AppendEntries and one response per config member) and zero log appends and zero persists on the leader. Measured, not assumed. | Benchmark `simraft-sim/benches/readindex_cost.rs` counting messages, appends, and persists per read under a counting driver; a count above the bound fails the benchmark gate. | M2, bench M4 |
| L-02 | Commit latency bound (LAT): in a fault-free window, an entry proposed to a healthy leader commits within 2 heartbeat intervals of virtual time (one round trip) plus one tick of scheduling, and applies within one further tick. | Simulator assertion in `simraft-sim/tests/scenario_commit_latency.rs` measuring virtual ticks from Propose to Apply across cluster sizes 1, 3, and 5. | M4 |
| L-03 | Step allocation budget (LAT): Node::step allocates within a measured per-event-class budget (AppendEntries handling may allocate proportional to the batch it accepts; Tick, vote, and response handling allocate at most a small constant number of allocations, verified with a counting allocator). | Benchmark `simraft-core/benches/step_allocations.rs` using a thread-local counting allocator in the bench harness; budgets are recorded in the bench file and a regression fails CI's `cargo bench --workspace -- --test` job. | M4 |
| L-04 | Checker tractability (LAT): the per-key linearizability checker decides histories of 10,000 operations over up to 64 keys within the CI step budget, by partitioning and visited-state memoization. Unpartitioned whole-history checking is never used on multi-key histories. | Benchmark `simraft-sim/benches/checker_scale.rs` with a generated 10,000-operation history; the benchmark records visited-state counts so a memoization regression shows as state explosion, not just wall time. | M4 |
| B-01 | Replication throughput baseline (LAT): criterion benchmarks report AppendEntries replication throughput in entries per second and step latency per event class, with inputs and outputs wrapped in black_box and Throughput::Elements or Throughput::Bytes configured per group. Numbers land in the README only from a recorded run, never estimated. | `simraft-core/benches/replication.rs` and `simraft-sim/benches/` suites; CI runs `cargo bench --workspace -- --test` on every PR. | M4 |
| E-01 | Diagnostic errors (ERR): every domain error (ConfigError, DecodeError, ProtocolError, SnapshotError) carries its specified context fields (offsets, indexes, terms, expected versus actual values), and Display output includes them. No error presented to a caller is a bare variant name. | Unit tests per error type, for example `simraft-core/tests/error_context.rs::config_error_names_both_timeout_values`, `::decode_error_names_offset_and_lengths`, `::snapshot_error_names_expected_and_received_offsets`. | M1 onward |

## 9. Adversarial edge case register

Each case names the invariant that decides it. The simulator scenario suite
and the property tests above must each contain the named case; this register
is the checklist they are audited against at every milestone gate.

1. Rejoining partitioned node with an inflated local view but a stale log
   attempts an election against a healthy leader. Decided by T-03, T-05.
2. Two candidates split a vote, both time out, and retry through Pre-Vote
   repeatedly. Liveness by T-06 and H-02; safety by S-01.
3. Figure 8: an entry replicated to a minority in term 2, a new leader in
   term 3 without it, the original leader returns in term 4. The entry is
   never counted-committed in term 4. Decided by S-06.
4. AppendEntriesResponse for batch N+1 arrives before batch N. match_index
   moves once, to the higher value, and never back. Decided by S-07.
5. A follower's log is longer than the leader's and entirely divergent past
   the snapshot base. The leader falls back to InstallSnapshot. N-01, N-04.
6. Chunk 3 of a snapshot arrives before chunk 2, then the leader changes
   and a new snapshot starts at chunk 0. The stale buffer is dropped once,
   the gap is rejected with offsets, the new transfer completes. N-02.
7. A client retries a Put while the first attempt is in the leader's
   uncommitted tail, then retries again after commit, then after a leader
   change. Exactly one execution, three identical responses. C-01, C-02.
8. A ReadIndex request lands in the tick between election and NoOp commit,
   the leader is partitioned mid-round, and a delayed ack from the old
   round arrives after healing under a new leader. No stale value is ever
   returned. R-01, R-02, R-03.
9. The virtual clock jumps forward by 100 * max_election_timeout in one
   step during a pending read and during an election. Safety unchanged;
   elections may restart; reads resolve only by quorum accounting. R-04.
10. A node crashes after appending entries but before its Persisted event,
    having sent no response. On restart it must not remember the entries.
    T-02, H-03.
11. A leader removes itself while a client write is in flight in the same
    batch window. The write resolves under the old config's quorum, then
    the leader steps down post-commit and the new config elects. M-03.
12. AddLearner for a node that is currently partitioned, then PromoteVoter
    before it catches up. Promotion is rejected with both indexes; after
    healing and catch-up, promotion succeeds. M-04.
13. A second config change is proposed while the first is one append away
    from commit. Rejected immediately, not queued silently. M-01.
14. A torn final log record (truncated bytes, bad CRC) is found on restart.
    Recovery truncates at that record, reports offset, index, and CRC
    values, and the node rejoins without the entry. ARCHITECTURE 7.3, E-01.
15. A snapshot with one flipped byte in the store section arrives complete
    and in order. Digest verification rejects it whole; the follower keeps
    its old log and base. N-03.
16. Duplicate delivery of the same AppendEntries batch to a follower
    (network duplication). The follower's log gains the entries once; the
    duplicate is recognized as already present and answered idempotently.
    S-03, H-01.
17. Session eviction boundary: a client retries its oldest operation after
    more than max_sessions other clients have run. Re-execution is the
    documented contract; the test asserts the bound and the behavior, and
    no other client's dedup is disturbed. C-03.
18. Empty cluster edge: a single-node config commits its own entries,
    serves ReadIndex with a quorum of one (itself, after its current-term
    commit), and removes itself only into an empty config, which is
    rejected as a config error (a config must keep at least one voter).
    M-01, R-01, T-01 family (validation).

## 10. How to read this matrix at a milestone gate

For each milestone, the rows whose Milestone column names it (or an earlier
milestone not yet landed) must have their named tests present, running, and
passing at the stated CI volume: PROPTEST_CASES = 1,000 per PR and 10,000
on main, simulator seed sweeps at the same volumes via SIM_SEED ranges.
A row may not be marked landed on the strength of a test that merely
compiles; the test must be able to fail (Stage A of each milestone lands
the tests failing against unimplemented logic first, per ARCHITECTURE
section 16).
