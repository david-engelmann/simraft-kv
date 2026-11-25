# simraft-kv Invariants and Traceability Matrix

This document is the normative list of formal invariants for
`simraft-kv` at `v0.2.0` across all five workspace crates
(`simraft-core`, `simraft-wire`, `simraft-kv`, `simraft-node`, and
`simraft-sim`). Every invariant occupies one row and names the exact
property test, simulator oracle, scenario test, tamper test, or
benchmark that enforces it in the repository tree. Any behavioral
claim in `docs/ARCHITECTURE.md` without a row here is not a verified
guarantee.

Invariant classes cover safety (`SAF`), linearizability (`LIN`),
durability (`DUR`), liveness under stated bounds (`LIV`), latency
and resource bounds (`LAT`), determinism (`DET`), wire and embedding
integration (`INT`), and diagnostics (`ERR` and `DIA`).

## 1. Core Raft safety

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| S-01 | Election Safety (SAF): at most one leader per term across all nodes, checked after every event. Two nodes are never both `Leader` with the same `current_term`. | Oracle `oracle::election_safety` run every simulator event; property test `simraft-core/tests/property_election.rs::one_leader_per_term` over randomized vote interleavings. | M1, oracle M4 |
| S-02 | Leader Append-Only (SAF): a leader never overwrites or deletes entries in its own log within a leadership tenure. A leader log changes only by appends at the tail and by snapshot base advancement. | Oracle `oracle::leader_append_only` snapshotting each leader log per event; property test `simraft-core/tests/property_log.rs::leader_log_is_append_only`; tenure scoping regression `simraft-sim/tests/regression_seed_912.rs`. | M1, oracle M4 |
| S-03 | Log Matching (SAF): if two logs contain an entry at the same index with the same term, the logs are identical in all entries up to that index. | Oracle `oracle::log_matching` every event over all node pairs; property test `simraft-core/tests/property_log.rs::matching_index_term_implies_matching_prefix`. | M1, oracle M4 |
| S-04 | Leader Completeness (SAF): if an entry is committed in a term, that entry is present in the log of every leader for every higher term, or covered by that leader's snapshot at or below its base index with a matching base term. | Oracle `oracle::leader_completeness` every event plus unit tests in `simraft-sim/src/oracle.rs`; simulator scenario `simraft-sim/tests/scenario_commit_survival.rs`. | M1, oracle M4 |
| S-05 | State Machine Safety (SAF): no two nodes apply different commands at the same log index, and heartbeats or below-base appends never commit unverified divergent follower tail entries. | Oracle `oracle::state_machine_safety` every event; tests `simraft-core/tests/property_commit.rs::same_index_applies_same_entry_on_all_nodes`, `::heartbeat_never_commits_unverified_divergent_tail`, and `::append_below_base_never_commits_unverified_divergent_tail`. | M2, v0.1.0 |
| S-06 | Figure 8 commitment (SAF): a leader advances `commit_index` by counting replicas only for an entry from its own current term. Earlier-term entries commit only as the prefix of a committed current-term entry, and `commit_index` never decreases on any node. | Property and regression tests `simraft-core/tests/property_commit.rs::figure8_old_term_entry_not_committed_by_counting`, `::current_term_entry_commits_by_counting`, and `::figure8_overwritten_entry_never_applies`; structural monotonicity check in `simraft-sim/src/oracle.rs`. | M2 |
| S-07 | Monotonic `match_index` (SAF): a leader never moves a follower `match_index` backwards when `AppendEntriesResponse` messages arrive reordered, duplicated, or delayed, and `next_index` never sits below `match_index + 1`. | Property tests `simraft-core/tests/property_progress.rs::stale_append_response_never_regresses_match_index` and `::arbitrary_reordered_responses_keep_match_and_next_monotonic`; per-tenure check in `simraft-sim/src/oracle.rs` under `simraft-sim/tests/scenario_chaos_matrix.rs`. | M1, v0.1.0 |
| S-08 | Conflict fast backup correctness (SAF): on `AppendEntries` rejection, `conflict_index` and `conflict_term` follow `docs/ARCHITECTURE.md` section 6.2 and never skip a missing index or land inside an unmatched term without restarting at its boundary. | Property tests `simraft-core/tests/property_log.rs::conflict_hint_lands_on_term_boundary` and `::conflict_hint_always_identifies_exact_term_boundary`. | M1, v0.1.0 |
| S-09 | Vote uniqueness (SAF): a node votes for at most one candidate per term, and that vote survives crash and restart. | Test `simraft-core/tests/property_election.rs::one_vote_per_term_across_restart`. | M1 |
| S-10 | Up-to-date voting restriction (SAF): a vote or pre-vote is granted only to a candidate whose log is at least as up to date by `(last_log_term, last_log_index)` comparison. | Property tests `simraft-core/tests/property_election.rs::stale_log_candidate_is_refused` and `::up_to_date_voting_rule_holds_over_arbitrary_tips`. | M1, v0.1.0 |

## 2. Timing, persistence, and Pre-Vote

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| T-01 | Timing ratio and limit validation (SAF): `Config::validate` rejects any config where `min_election_timeout < 10 * heartbeat_interval`, `max_election_timeout < 2 * min_election_timeout`, or any limit is zero, naming the offending values, and `ClusterConfig::new` rejects empty voters or overlapping voters and learners. | Property and unit tests `simraft-core/tests/property_config.rs::invalid_timing_ratios_are_rejected`, `::zero_limits_are_rejected`, `::error_names_both_values`, and `simraft-core/tests/error_context.rs::empty_and_overlapping_voters_errors_are_explicit`. | M1, v0.1.0 |
| T-02 | Persist before response (DUR): `current_term`, `voted_for`, and newly appended log entries reach durable storage before any dependent vote, append, snapshot, commit, or read response is emitted, including across log truncations, divergent snapshot installs, and non-voter term updates. | Structural tests `simraft-core/tests/property_persist.rs::no_response_before_persist_ack`, `::leader_does_not_advertise_unpersisted_entries`, `::truncation_and_snapshot_clamp_durable_watermark_and_ignore_stale_acks`, `::non_voter_and_snapshot_higher_term_persist_before_response`, and `simraft-core/tests/single_voter.rs::single_voter_waits_for_both_hard_state_and_entries_persists_before_commit_or_read`; crash scenario `simraft-sim/tests/scenario_crash_matrix.rs`. | M1, v0.1.0 |
| T-03 | Pre-Vote purity (SAF): granting or denying a `RequestPreVote` never increments persistent `current_term`, never writes `voted_for`, and never resets the voter election timer. | Test `simraft-core/tests/property_prevote.rs::prevote_leaves_hard_state_and_timer_untouched`. | M1 |
| T-04 | Pre-Vote hypothetical term (SAF): `RequestPreVote` carries `current_term + 1` without incrementing persistent `current_term`; a node enters `Candidate` and persists `term + 1` only after winning a pre-vote majority. | Test `simraft-core/tests/property_prevote.rs::candidate_term_increments_only_after_prevote_majority`. | M1 |
| T-05 | Leader recency denial (LIV): a voter that accepted `AppendEntries` from a leader in the current term within `min_election_timeout` denies pre-votes so a partitioned node cannot disrupt a healthy leader upon rejoining. | Core test `simraft-core/tests/property_prevote.rs::prevote_denied_when_leader_recently_heard_from` and simulator scenario `simraft-sim/tests/scenario_disruptive_rejoin.rs`. | M1, M4 |
| T-06 | Randomized election timeouts (LIV): per-node election deadlines differ by a deterministic offset in `[min_election_timeout, max_election_timeout)` so simultaneous timeouts do not livelock. | Test `simraft-core/tests/property_config.rs::deadline_offsets_stay_in_range_and_vary_by_node` and leader convergence in `H-02`. | M1 |

## 3. Commitment, ReadIndex, and reads

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| R-01 | Current-term gate (SAF): a leader never serves a `ReadIndex` read before committing an entry in its own current term; earlier reads queue until the election `NoOp` commits or leadership is lost. | Test `simraft-core/tests/property_readindex.rs::read_waits_for_current_term_commit`. | M2 |
| R-02 | Quorum and apply confirmation (SAF): a `ReadIndex` read is served only after heartbeat acknowledgments carrying its round cover a quorum of the current config and `last_applied >= read_index`. | Tests `simraft-core/tests/property_readindex.rs::read_waits_for_current_term_commit` and `simraft-kv/tests/property_dedup.rs::read_waits_for_applied_index`; partition and failover workload in `simraft-sim/tests/scenario_linearizability.rs`. | M2, M4 |
| R-03 | Monotonic round tokens (SAF): `read_round` increases strictly within a leader term, tokens are scoped by `(leader_id, term, round)`, and stale acks never confirm later reads. | Tests `simraft-core/tests/property_readindex.rs::round_tokens_increase_and_stale_rounds_do_not_release` and `::read_index_never_goes_backwards`. | M2 |
| R-04 | Clock-free reads (SAF): `ReadIndex` correctness uses no clock, lease, or timeout value, and arbitrary virtual clock jumps never permit stale reads. | Scenario `simraft-sim/tests/scenario_clock_jump_read.rs`. | M2, M4 |
| R-05 | Linearizable client histories (SAF): under chaos faults, every completed client history is linearizable per key under the Wing and Gong checker, and an `Unknown` verdict fails the run. | Checker `simraft-sim/src/liz.rs`; suites `simraft-sim/tests/scenario_linearizability.rs`, `simraft-sim/tests/checker_verdicts.rs`, and `simraft-sim/tests/regression_seed_15623.rs`. | M4 |
| R-06 | Single-voter commitment (LIV, DUR): a cluster of one voter commits and applies its leader entries and releases `ReadIndex` reads once its own hard state and entries are durable (`T-02`), both from genesis and after shrinking from two voters to one via `RemoveVoter`. | Core tests `simraft-core/tests/single_voter.rs::single_voter_leader_commits_and_applies`, `::single_voter_waits_for_both_hard_state_and_entries_persists_before_commit_or_read`, and `::two_voter_cluster_shrinking_to_one_voter_commits_only_after_persist`. | v0.1.0 |

## 4. Client deduplication and backpressure

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| C-01 | Committed dedup (SAF): a retried proposal whose `(client_id, seq)` is at or below the session `last_seq` returns the cached response (or stale marker) without re-executing `Put` or `Delete`. | Property tests `simraft-kv/tests/property_dedup.rs::committed_duplicate_returns_cached_response`, `::older_duplicate_is_stale`, and `::dedup_and_session_cap_hold_over_randomized_interleavings`. | M2, v0.1.0 |
| C-02 | In-flight dedup and uncommitted bound (SAF, LAT): a retry matching an uncommitted log entry attaches without appending a duplicate entry, and new proposals past `Config::max_uncommitted_entries` are rejected with `ProtocolError::TooManyUncommitted`. | Property tests `simraft-core/tests/property_dedup.rs::duplicate_propose_attaches_to_inflight_entry`, `::different_seq_appends_normally`, `::inflight_retries_never_duplicate_entries_and_all_waiters_receive_commit`, and `::max_uncommitted_entries_backpressures_new_proposals`. | M2, v0.1.0 |
| C-03 | Bounded session memory (LAT, SAF): session state holds at most `Config::max_sessions` clients (`KvStateMachine::with_max_sessions` wired in `simraft-sim` and `simraft-node`), evicting least recently applied sessions first. | Property tests `simraft-kv/tests/property_dedup.rs::session_table_is_bounded_and_eviction_allows_reexecution` and `::dedup_and_session_cap_hold_over_randomized_interleavings`. | M2, v0.1.0 |
| C-04 | Sequence gap rejection (SAF, ERR): an applied entry with `seq > last_seq + 1` is rejected with `client_id`, `expected`, and `received` sequence numbers, and session state survives snapshot install. | Tests `simraft-kv/tests/property_dedup.rs::sequence_gap_is_rejected_and_never_applied` and `simraft-kv/tests/property_snapshot_kv.rs::snapshot_roundtrip_is_canonical_and_preserves_dedup`. | M2, v0.1.0 |

## 5. Membership changes

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| M-01 | One uncommitted change (SAF): at most one config entry is ever uncommitted; `ProposeConfig` while `last_config_index > commit_index` is rejected with both indexes. | Test `simraft-core/tests/property_membership.rs::second_config_change_rejected_while_first_uncommitted`. | M3 |
| M-02 | NoOp before config (SAF): a newly elected leader rejects config proposals until its current-term `NoOp` entry commits. | Test `simraft-core/tests/property_membership.rs::config_rejected_before_leader_noop_commits`. | M3 |
| M-03 | Commit-then-step-down (SAF): a leader removing itself steps down only after the removal entry commits and continues replicating until commit. | Core test `simraft-core/tests/property_membership.rs::leader_steps_down_only_after_self_removal_commits` and scenario `simraft-sim/tests/scenario_self_removal.rs`. | M3, M4 |
| M-04 | Learner semantics (SAF): learners never vote, never campaign, and never count toward quorums; `PromoteVoter` rejects a learner whose `match_index < commit_index` with both indexes. | Tests `simraft-core/tests/property_membership.rs::learners_never_vote_or_count` and `::promotion_requires_caught_up_learner`. | M3 |
| M-05 | Single-config quorums (SAF): every quorum decision uses the single newest `ClusterConfig` present in the deciding node log. | Core test `simraft-core/tests/property_membership.rs::quorum_uses_latest_single_config` and safety oracles in `simraft-sim/src/oracle.rs` under `simraft-sim/tests/scenario_self_removal.rs` and `simraft-sim/tests/scenario_chaos_matrix.rs`. | M3, M4 |

## 6. Snapshots and InstallSnapshot

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| N-01 | Automatic snapshot fallback (SAF, LIV): when a follower `next_index` falls at or below the leader snapshot base index, the leader sends `InstallSnapshot` immediately. | Test `simraft-core/tests/property_snapshot.rs::leader_switches_to_snapshot_at_base_boundary`. | M3 |
| N-02 | Sequential chunks (SAF): snapshot chunks are accepted only in exact sequential offset order, and a newer snapshot identity discards a stale partial buffer cleanly. | Tests `simraft-core/tests/property_snapshot.rs::chunks_require_exact_sequential_offsets` and `::new_snapshot_identity_discards_stale_buffer`. | M3 |
| N-03 | Snapshot integrity and canonical round trip (DUR): a snapshot failing SHA-256 verification (`"simraft-kv/snapshot/v1"`) or carrying trailing bytes is rejected whole, and valid snapshots restore identical store and session state. | Tamper test `simraft-kv/tests/tamper_snapshot.rs::single_byte_flip_is_rejected_with_digests`; property tests `simraft-kv/tests/property_snapshot_kv.rs::install_restores_store_and_sessions` and `::snapshot_roundtrip_is_canonical_and_preserves_dedup`. | M3, v0.1.0 |
| N-04 | Prefix retention rule (SAF): on snapshot install, a follower retains the suffix past `last_included_index` only when its entry at `last_included_index` matches `last_included_term`, discarding the log otherwise. | Test `simraft-core/tests/property_snapshot.rs::matching_prefix_retained_divergent_log_discarded`. | M3 |
| N-05 | Compaction safety (SAF): log compaction never removes uncommitted entries or entries above `commit_index`, and `commit_index` and `last_applied` never decrease. | Oracle structural assertions in `simraft-sim/src/oracle.rs`; test `simraft-core/tests/property_snapshot.rs::compaction_never_touches_committed_or_uncommitted_boundary`. | M3, M4 |
| N-06 | Snapshot relay restart (LIV): an unacknowledged completed snapshot relay restarts from offset 0 on the next send so a previously partitioned peer catches up without waiting for a new compaction. | Test `simraft-core/tests/property_snapshot.rs::a_completed_unacknowledged_relay_restarts_from_offset_zero` and scenario `simraft-sim/tests/scenario_partition_snapshot.rs::partition_during_snapshot_transfer_still_converges`. | Lane 2 |
| N-07 | Boot snapshot install (SAF): a node restored with a durable snapshot installs its boundary before serving so base, commit, applied, and cluster config reflect the snapshot immediately. | Tests `simraft-core/tests/property_snapshot.rs::a_restored_snapshot_installs_its_boundary_over_the_recovered_log` and `::a_restored_snapshot_covering_the_whole_log_restores_an_empty_suffix`. | Lane 2 |
| N-08 | Store restore alignment (DUR): recovered log entries begin at `snapshot.last_included_index + 1` and subsequent persists stay at their absolute log indexes. | Test `simraft-core/tests/property_snapshot.rs::a_restored_snapshot_installs_its_boundary_over_the_recovered_log` and unit tests in `simraft-sim/src/disk.rs`. | Lane 2 |

## 7. Chaos, healing, and determinism

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| H-01 | Chaos survival (SAF): under asymmetric and chain partitions, drops, duplicates, frame bit flips, crashes, and extreme clock jumps (`FaultConfig::chaos` and `--chaos`), no safety oracle ever fires. | Suite `simraft-sim/tests/scenario_chaos_matrix.rs` and CI `--chaos` sweeps across PR, `main`, and weekly 100,000-seed shards. | M4, v0.1.0 |
| H-02 | Bounded healing (LIV): once faults cease at `heal_start_tick` and latency is bounded below `min_election_timeout / 4`, a single stable leader emerges within `3 * max_election_timeout` ticks (`RunReport::leader_converged`) and all active members converge in log and state digests (`"simraft-kv/log-digest/v1"` and `"simraft-kv/state-digest/v1"`). | Enforced in `simraft-sim/src/harness.rs` (`Harness::finish` and `RunReport::passed`) across `simraft-sim/tests/scenario_chaos_matrix.rs` and `simraft-sim/tests/scenario_disruptive_rejoin.rs`. | M4, v0.1.0 |
| H-03 | Crash recovery (DUR): a crashed node restarts with only fsynced hard state, log prefix, and snapshot, losing volatile state without violating `S-09` or `T-02`. | Disk model `simraft-sim/src/disk.rs` and scenario `simraft-sim/tests/scenario_crash_matrix.rs`. | M4 |
| H-04 | Frame corruption rejection (SAF): bit-flipped frames and oversized (> 16 MiB) frames are rejected by magic, length, and CRC-32 checks (`"SRKV/frame/v1"`) without stepping into `Node`. | Scenario `simraft-sim/tests/scenario_frame_corruption.rs`, `simraft-sim/tests/scenario_seed_sweep.rs::frame_corruption_holds_across_swept_seeds`, and unit tests in `simraft-wire/src/lib.rs`. | Lane 2, v0.1.0 |
| H-05 | Log corruption truncation (DUR): a corrupt durable log record fails CRC-32 (`"SRKV/log/v1"`), truncates at that record with full diagnostic reporting, and catches up cleanly from the leader. | Unit tests in `simraft-sim/src/disk.rs`, `simraft-sim/tests/scenario_disk_corruption.rs::corrupt_log_record_truncates_on_restore_and_the_cluster_heals`, and `simraft-sim/tests/scenario_seed_sweep.rs::log_corruption_holds_across_swept_seeds`. | Lane 2 |
| H-06 | Snapshot corruption rejection (DUR): a corrupt durable snapshot payload fails SHA-256 verification at restore, leaves the log base untouched, and replays the durable log instead. | Scenario `simraft-sim/tests/scenario_disk_corruption.rs::corrupt_snapshot_is_rejected_on_restore_and_the_log_replays` and `simraft-sim/tests/scenario_seed_sweep.rs::snapshot_corruption_holds_across_swept_seeds`. | Lane 2 |
| H-07 | Rolling restart (LIV): crashing and restarting every node in rotation under continuous client load loses no acknowledged write and preserves linearizability. | Scenario `simraft-sim/tests/scenario_rolling_restart.rs` and `simraft-sim/tests/scenario_seed_sweep.rs::rolling_restart_holds_across_swept_seeds`. | Lane 2 |
| H-08 | Partition during snapshot transfer (SAF, LIV): a follower partitioned across compaction catches up via `InstallSnapshot` upon healing and installs the snapshot into its state machine. | Scenario `simraft-sim/tests/scenario_partition_snapshot.rs` and `simraft-sim/tests/scenario_seed_sweep.rs::partition_snapshot_holds_across_swept_seeds`. | Lane 2 |
| H-09 | Learner catch-up under load (LIV): a learner replicates and applies committed entries under load without voting or campaigning and converges to the leader applied index. | Scenario `simraft-sim/tests/scenario_learner_catchup.rs` and `simraft-sim/tests/scenario_seed_sweep.rs::learner_catchup_holds_across_swept_seeds`. | Lane 2 |
| D-01 | Cross-process seed determinism (DET): two separate OS process invocations of `simraft-sim` with the same seed emit byte-identical traces and domain-separated state and log digests. | Test `simraft-sim/tests/determinism_cross_process.rs` and `simraft-sim/tests/cli_contract.rs::chaos_flag_runs_and_replays_deterministically`. | M4, v0.1.0 |
| D-02 | No hidden nondeterminism (DET): `simraft-core`, `simraft-kv`, `simraft-wire`, and `simraft-sim` contain no `HashMap`, `HashSet`, `SystemTime`, `Instant::now`, `thread_rng`, or `OsRng` outside comments. | CI guard in `.github/workflows/ci.yml` plus source-scan and gate-blindness tests `simraft-sim/tests/determinism_guard.rs::deterministic_crates_contain_no_forbidden_nondeterminism_identifiers` and `::determinism_guard_catches_planted_violations_including_braced_imports`. | M1, v0.1.0 |
| D-03 | Seed visibility and replay (DET): every simulator run prints its active seed on its first output line and replays deterministically from that seed. | CLI contract tests `simraft-sim/tests/cli_contract.rs::seed_is_printed_and_replays_failure_free` and `::chaos_flag_runs_and_replays_deterministically`; payload byte stability in `simraft-kv/tests/command_codec.rs`. | M4, v0.1.0 |

## 8. Latency, resource, and diagnostic bounds

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| L-01 | ReadIndex cost bound (LAT): a `ReadIndex` read costs at most one heartbeat round per peer and zero log appends or persists on the leader. | Benchmark gate `simraft-sim/benches/readindex_cost.rs`. | M2, M4 |
| L-02 | Commit latency bound (LAT): in a fault-free window, a proposal to a healthy leader commits within two heartbeat intervals plus one scheduling tick across cluster sizes 1, 3, and 5. | Scenario test `simraft-sim/tests/scenario_commit_latency.rs::fault_free_commit_latency_is_bounded`. | M4, v0.1.0 |
| L-03 | Step allocation budget (LAT): `Node::step` stays within verified thread-local allocation budgets (`TICK_BUDGET = 4`, `VOTE_BUDGET = 4`, `APPEND_BUDGET = 7`). | Benchmark gate `simraft-core/benches/step_allocations.rs` executed by `cargo bench --workspace --locked -- --test`. | M4, v0.1.0 |
| L-04 | Checker tractability (LAT): the per-key linearizability checker decides 100,000-operation histories across 64 keys with visited-state memoization. | Benchmark gate `simraft-sim/benches/checker_scale.rs`. | M4, Lane 3 |
| B-01 | Replication throughput baseline (LAT): Criterion suites measure replication throughput and catch-up latency at 10,000 entries with `black_box` inputs and outputs. | Suite `simraft-core/benches/replication.rs`. | M4, Lane 3 |
| B-02 | Leader batching (LAT): proposals appended before a persist ack release as one coalesced `AppendEntries` round per follower. | Core tests in `simraft-core/tests/batching.rs` and `propose_commit_batch_32` in `simraft-core/benches/replication.rs`. | Lane 3 |
| E-01 | Diagnostic errors and payload caps (ERR): every domain error (`ConfigError`, `ProtocolError`, `SnapshotError`, `KvError`, `WireError`) formats clean context fields without duplicate prefixes or indentation gaps, and codecs reject payloads above 16 MiB and trailing bytes. | Tests `simraft-core/tests/error_context.rs::config_error_names_both_timeout_values`, `::membership_and_backpressure_rejections_name_the_deciding_values`, `::snapshot_rejection_names_identity_and_offsets`, `simraft-kv/tests/command_codec.rs::truncated_payload_names_field_and_offset`, `::payloads_above_sixteen_mib_are_rejected_without_panicking`, `::trailing_bytes_are_rejected_for_commands_and_results`, and `simraft-kv/tests/property_dedup.rs::apply_gap_and_replay_are_reported`. | M1, v0.1.0 |

## 9. Adversarial edge case register

1. Rejoining partitioned node with a stale log attempts an election
   against a healthy leader (`T-03` and `T-05`).
2. Split-vote candidates time out and retry through Pre-Vote with
   staggered deterministic offsets (`T-06`, `H-02`, and `S-01`).
3. Figure 8 sequence where an old-term entry sits on a majority in a
   later term and must never be counted-committed directly (`S-06`).
4. `AppendEntriesResponse` for batch `N + 1` arrives before batch `N`
   without regressing `match_index` or `next_index` (`S-07`).
5. Divergent follower log behind a compacted leader snapshot base
   falls back automatically to `InstallSnapshot` (`N-01` and `N-04`).
6. Out-of-order snapshot chunk followed by a leader change drops the
   stale partial buffer once and completes the new snapshot (`N-02`).
7. Client retries a `Put` while uncommitted, again after commit, and
   again after failover, executing once (`C-01` and `C-02`).
8. `ReadIndex` issued before `NoOp` commit followed by a partition
   and delayed ack never returns stale state (`R-01`, `R-02`, `R-03`).
9. Virtual clock jump of `100 * max_election_timeout` mid-read
   preserves linearizability (`R-04`).
10. Crash between `AppendEntries` memory append and `Persisted`
    discards un-fsynced entries cleanly (`T-02` and `H-03`).
11. Leader self-removal commits under the old quorum before stepping
    down (`M-03`).
12. `PromoteVoter` on a lagging learner is rejected with both indexes
    and succeeds once caught up (`M-04`).
13. Concurrent second config proposal while the first is uncommitted
    is rejected immediately (`M-01`).
14. Torn final log record on disk truncates at the bad CRC record
    and rejoins cleanly (`H-05` and `A-07`).
15. Single flipped byte in a snapshot payload is rejected whole by
    SHA-256 verification (`N-03`, `H-06`, and `A-09`).
16. Duplicate `AppendEntries` delivery is answered idempotently
    without duplicating log entries (`S-03` and `H-01`).
17. Empty `AppendEntries` heartbeat with `leader_commit` above
    `prev_log_index` never commits unverified divergent follower tail
    entries (`S-05`).
18. Single-voter cluster commits and serves `ReadIndex` only after
    durable persistence and rejects shrinking to zero voters (`R-06`,
    `T-01`, and `T-02`).

## 10. Production node adapter (`simraft-node`)

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| A-01 | Acknowledged-write durability and session uniqueness (SAF): writes acknowledged by `simraft-node` survive leader restart, rapid sequential `run_client` calls assign distinct sessions without dropping writes, and local payload rejections never poison `Client` sequence numbers. | `simraft-node/tests/cluster_end_to_end.rs::writes_survive_leader_restart`, `::rapid_sequential_run_client_puts_all_persist_without_collision`, `simraft-node/src/client.rs::tests::consecutive_client_ids_never_collide`, and `::oversized_write_does_not_poison_client_sequence`. | Lane 1, v0.1.0 |
| A-02 | Failover read freshness (LIN): after killing a leader and electing a successor, reads through any surviving member return the latest committed value, and missing-key CLI reads exit with code `1`. | `simraft-node/tests/cluster_end_to_end.rs::read_after_failover_returns_latest_committed` and `::cli_get_distinguishes_missing_key_from_ok_value`. | Lane 1, v0.1.0 |
| A-03 | Restart recovery and parked-write completion (LIV): a restarted lagging node catches up and serves current state, and stale or failed applies complete parked writes immediately without hanging. | `simraft-node/tests/cluster_end_to_end.rs::restarted_node_catches_up` and `simraft-node/src/runtime.rs::tests::duplicate_stale_and_failed_applies_complete_parked_writes_without_hanging`. | Lane 1, v0.1.0 |
| A-04 | Wire compatibility and frame header bounds (INT, LAT): peer TCP links carry `simraft-wire` frames across arbitrary byte splits and reject bad magic or > 16 MiB headers from the 36-byte header without large allocations. | `simraft-node/tests/frame_compat.rs::frames_reassemble_at_any_split`, `::back_to_back_frames_come_out_in_order`, and `::frame_buffer_rejects_bad_magic_and_four_gigabyte_header_immediately`. | Lane 1, v0.1.0 |
| A-05 | Snapshot persist cycle (SAF): `simraft-node` completes the snapshot persist cycle on both local compaction and received `InstallSnapshot` so a caught-up node serves snapshotted state immediately without restarting. | `simraft-node/tests/snapshot_install.rs::snapshot_receiver_serves_snapshotted_state_after_promotion`. | Lane 1 |
| A-06 | Snapshot serving fence (SAF): `simraft-node` serves a snapshot for a boundary only from durable bytes at that exact boundary or a live state machine at that exact applied index, never leaking later state under an earlier boundary. | Unit test `simraft-node/src/runtime.rs::tests::snapshot_rebuild_never_serves_later_state_under_an_earlier_boundary`. | Lane 1 |
| A-07 | Torn log tail recovery (DUR): a node whose durable log file ends in a torn record truncates at the first bad record on boot, rejoins the live cluster, and serves intact prefix and subsequent writes. | End-to-end test `simraft-node/tests/torn_log_recovery.rs::torn_log_tail_boots_truncates_and_rejoins` and `simraft-node/src/store.rs::tests::torn_tail_is_truncated_and_reported`. | Lane 2, v0.1.0 |
| A-08 | Leader kill mid-burst durability (DUR): killing the leader during a continuous client write burst loses zero acknowledged writes across failover. | End-to-end test `simraft-node/tests/torn_log_recovery.rs::acknowledged_writes_survive_leader_kill_mid_burst`. | Lane 2, v0.1.0 |
| A-09 | Wiped member convergence (SAF): a member whose data directory is wiped rejoins fresh under the same node ID, catches up via snapshot under concurrent load, and serves full cluster state after promotion. | End-to-end test `simraft-node/tests/torn_log_recovery.rs::wiped_member_rejoins_fresh_and_converges_under_load`. | Lane 2, v0.1.0 |

## 11. Operability and embedding

| ID | Invariant | Enforced by | Milestone |
| --- | --- | --- | --- |
| O-01 | Stats snapshot accuracy (DIA): `Node::stats` reports role, current term, acknowledged leader, commit and applied indexes, last log index, snapshot base, and leader peer progress matching individual node accessors. | Tests `simraft-core/tests/stats.rs::a_fresh_node_reports_a_zeroed_follower_snapshot`, `::an_elected_leader_reports_its_term_and_peer_progress`, `::commit_and_applied_advance_together_on_leader_and_follower`, and `::a_follower_reports_no_peer_progress`. | Lane 4 |
| O-02 | Embedding example (INT): a single-file example embeds `simraft-core` with an in-memory transport, commits a write, and reads it back on every node using only public sans-I/O APIs. | Example `simraft-core/examples/embed.rs` run in CI via `cargo run -p simraft-core --example embed --locked`. | Lane 4 |

## 12. Verification gate rules

Every row in this matrix is enforced by automated tests and
benchmarks in `.github/workflows/ci.yml` and
`.github/workflows/weekly-sweep.yml`. Pull requests run
`PROPTEST_CASES=1000` and 1,000 `--chaos` simulator seeds, pushes to
`main` run `PROPTEST_CASES=10000` and 10,000 `--chaos` seeds, and
weekly sweeps run 100,000 `--chaos` seeds plus widened Lane 2
scenario sweeps.
