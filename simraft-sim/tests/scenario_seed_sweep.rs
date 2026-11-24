//! Seed sweep of the Lane 2 adversarial scenarios
//! (invariants H-04 to H-09). Each scenario landed at one
//! hand-picked seed; this file reruns every one of them at
//! its original seed and across a batch of swept seeds, with
//! the scripted choreography and fault profile unchanged and
//! only the run's seeded stream (timers, latency, fault
//! draws, workload) varying. The M4 sweep of the default
//! scenario found a real oracle defect at seed 912 that no
//! hand-picked seed had reached; the adversarial scenarios
//! deserve the same pressure. A failing seed here is a
//! finding: it names the scenario and the seed, and it is
//! fixed or adjudicated with evidence, never deleted.
//!
//! The first sweep produced two findings. At seeds 7900 and
//! 8100 the S-04 leader completeness oracle fired on a
//! partitioned leader still sitting in term 1 while the
//! majority committed in term 2; the property binds higher
//! terms only, and the oracle is now scoped to its statement
//! (with unit pins in `simraft-sim/src/oracle.rs`). And the
//! partition scenario's snapshot-received evidence is
//! emergent rather than scripted, so it is asserted over
//! the batch below instead of per seed.

use simraft_core::{LogIndex, NodeId};
use simraft_sim::faults::{DirectedCut, FaultConfig, ScriptedOp};
use simraft_sim::{Harness, RunReport, SimConfig};

fn trace_has(report: &RunReport, needle: &str) -> bool {
    report
        .trace
        .lines()
        .iter()
        .any(|line| line.contains(needle))
}

fn trace_count(report: &RunReport, needle: &str) -> usize {
    report
        .trace
        .lines()
        .iter()
        .filter(|line| line.contains(needle))
        .count()
}

fn assert_passed(report: &RunReport, scenario: &str, seed: u64) {
    assert!(
        report.passed(),
        "{scenario} seed {seed}: the run must pass (converged, no oracle failure, linearizable)"
    );
}

/// The seeds a scenario runs at: its original seed plus a
/// batch of swept seeds from `batch_base`. The weekly sweep
/// sets `SIM_SWEEP_WIDE=1` to widen every batch from 7 to 100
/// seeds; the per-pull-request suite keeps the small batch
/// so its runtime stays bounded.
fn sweep_seeds(original: u64, batch_base: u64) -> Vec<u64> {
    let wide = std::env::var("SIM_SWEEP_WIDE").is_ok_and(|value| value == "1");
    let batch: u64 = if wide { 100 } else { 7 };
    let mut seeds = vec![original];
    seeds.extend((0..batch).map(|offset| batch_base + offset));
    seeds
}

/// H-04: the frame corruption scenario's configuration at
/// any seed (300 per mille corruption, crash and clock
/// faults off, as the original scenario documents).
fn frame_corruption_config(seed: u64) -> SimConfig {
    let faults = FaultConfig {
        corrupt_per_mille: 300,
        crash_per_mille: 0,
        clock_jump_per_mille: 0,
        ..FaultConfig::default()
    };
    SimConfig {
        seed,
        faults,
        ..SimConfig::default()
    }
}

#[test]
fn frame_corruption_holds_across_swept_seeds() {
    for seed in sweep_seeds(77, 7700) {
        let report = Harness::new(frame_corruption_config(seed)).run();
        assert!(
            trace_count(&report, "frame_corrupted") > 0,
            "frame corruption seed {seed}: the run must actually corrupt frames (H-04 evidence)"
        );
        assert_passed(&report, "frame corruption", seed);
    }
}

/// H-05: the log corruption scenario's configuration at any
/// seed (the corruption and the crash stay scripted at the
/// same ticks; only the seeded stream varies).
fn log_corruption_config(seed: u64) -> SimConfig {
    SimConfig {
        seed,
        chaos_ticks: 6_000,
        heal_ticks: 4_000,
        faults: FaultConfig::none(),
        scripted: vec![
            (
                3_000,
                ScriptedOp::CorruptLog {
                    node: NodeId(3),
                    index: LogIndex(5),
                },
            ),
            (3_010, ScriptedOp::Crash(NodeId(3))),
        ],
        ..SimConfig::default()
    }
}

#[test]
fn log_corruption_holds_across_swept_seeds() {
    for seed in sweep_seeds(78, 7800) {
        let report = Harness::new(log_corruption_config(seed)).run();
        assert!(
            trace_has(&report, "corrupt_log n3 index 5 flipped=true"),
            "log corruption seed {seed}: the scripted corruption must land on a real record"
        );
        assert!(
            trace_has(&report, "log_truncated n3"),
            "log corruption seed {seed}: the restore must report the torn record (H-05 evidence)"
        );
        assert_passed(&report, "log corruption", seed);
        let applied: Vec<LogIndex> = report
            .final_applied
            .iter()
            .map(|(_, index)| *index)
            .collect();
        assert!(
            applied.iter().all(|index| *index == applied[0]),
            "log corruption seed {seed}: every node ends at the same applied index: {applied:?}"
        );
    }
}

/// H-06: the snapshot corruption scenario's configuration
/// at any seed (node 3 partitioned away, healed into a
/// snapshot, then its durable snapshot corrupted and the
/// node crashed on the same tick).
fn snapshot_corruption_config(seed: u64) -> SimConfig {
    let mut faults = FaultConfig::none();
    for (from, to) in [(1, 3), (3, 1), (2, 3), (3, 2)] {
        faults.partitions.push(DirectedCut {
            from: NodeId(from),
            to: NodeId(to),
            start: 1_200,
            end: 4_500,
        });
    }
    SimConfig {
        seed,
        chaos_ticks: 8_000,
        heal_ticks: 4_000,
        faults,
        scripted: vec![
            (6_000, ScriptedOp::CorruptSnapshot { node: NodeId(3) }),
            (6_000, ScriptedOp::Crash(NodeId(3))),
        ],
        ..SimConfig::default()
    }
}

#[test]
fn snapshot_corruption_holds_across_swept_seeds() {
    for seed in sweep_seeds(79, 7900) {
        let report = Harness::new(snapshot_corruption_config(seed)).run();
        assert!(
            trace_has(&report, "corrupt_snapshot n3 flipped=true"),
            "snapshot corruption seed {seed}: node 3 must hold a durable snapshot by tick 6000"
        );
        assert!(
            trace_has(&report, "snapshot_rejected n3"),
            "snapshot corruption seed {seed}: the restore must reject the corrupt snapshot \
             (H-06 evidence)"
        );
        assert_passed(&report, "snapshot corruption", seed);
    }
}

/// H-07: the rolling restart scenario's configuration at
/// any seed (every node crashed in turn under load).
fn rolling_restart_config(seed: u64) -> SimConfig {
    SimConfig {
        seed,
        chaos_ticks: 9_000,
        heal_ticks: 4_000,
        faults: FaultConfig::none(),
        scripted: vec![
            (2_000, ScriptedOp::Crash(NodeId(1))),
            (4_500, ScriptedOp::Crash(NodeId(2))),
            (7_000, ScriptedOp::Crash(NodeId(3))),
        ],
        ..SimConfig::default()
    }
}

#[test]
fn rolling_restart_holds_across_swept_seeds() {
    for seed in sweep_seeds(80, 8000) {
        let report = Harness::new(rolling_restart_config(seed)).run();
        assert_passed(&report, "rolling restart", seed);
        let completed = report
            .history
            .records()
            .iter()
            .filter(|record| record.result.is_some())
            .count();
        assert!(
            completed >= 40,
            "rolling restart seed {seed}: client work must keep completing across the \
             restarts, got {completed}"
        );
        assert_eq!(
            trace_count(&report, "crash n"),
            3,
            "rolling restart seed {seed}: each node crashed exactly once"
        );
    }
}

/// H-08: the partition during snapshot transfer scenario's
/// configuration at any seed.
fn partition_snapshot_config(seed: u64) -> SimConfig {
    let mut faults = FaultConfig::none();
    for (from, to) in [(1, 3), (3, 1), (2, 3), (3, 2)] {
        faults.partitions.push(DirectedCut {
            from: NodeId(from),
            to: NodeId(to),
            start: 1_500,
            end: 6_000,
        });
    }
    SimConfig {
        seed,
        chaos_ticks: 8_000,
        heal_ticks: 4_000,
        faults,
        ..SimConfig::default()
    }
}

#[test]
fn partition_snapshot_holds_across_swept_seeds() {
    // The snapshot-received evidence is asserted over the
    // batch, not per seed: whether the leader's base passes
    // the isolated follower during the partition window
    // depends on the stream-driven write volume, and a seed
    // where the follower instead catches up by appends after
    // the heal is a legitimate outcome (seed 8100 converges
    // that way, applied 396, no snapshot anywhere in the
    // run). The per-seed contract is the invariant content:
    // convergence at one applied index, well past the
    // compaction threshold. The original seed's per-seed
    // evidence stays pinned by the scenario's own file.
    let mut any_snapshot_received = false;
    for seed in sweep_seeds(81, 8100) {
        let report = Harness::new(partition_snapshot_config(seed)).run();
        any_snapshot_received |= trace_has(&report, "snapshot_received n3");
        assert_passed(&report, "partition snapshot", seed);
        let applied: Vec<LogIndex> = report
            .final_applied
            .iter()
            .map(|(_, index)| *index)
            .collect();
        assert!(
            applied.iter().all(|index| *index == applied[0]),
            "partition snapshot seed {seed}: every node ends at the same applied index: \
             {applied:?}"
        );
        assert!(
            applied[0].0 >= 32,
            "partition snapshot seed {seed}: the run must have written well past the \
             compaction threshold, got {}",
            applied[0].0
        );
    }
    assert!(
        any_snapshot_received,
        "partition snapshot: at least one swept seed must exercise the snapshot path \
         (H-08 evidence over the batch)"
    );
}

/// H-09: the learner catch-up scenario's configuration at
/// any seed (a genesis learner under a three-client load).
fn learner_catchup_config(seed: u64) -> SimConfig {
    SimConfig {
        seed,
        learners: vec![NodeId(4)],
        clients: 3,
        chaos_ticks: 6_000,
        heal_ticks: 3_000,
        faults: FaultConfig::none(),
        ..SimConfig::default()
    }
}

#[test]
fn learner_catchup_holds_across_swept_seeds() {
    for seed in sweep_seeds(82, 8200) {
        let report = Harness::new(learner_catchup_config(seed)).run();
        assert_passed(&report, "learner catch-up", seed);
        let learner_applied = report
            .final_applied
            .iter()
            .find(|(id, _)| *id == NodeId(4))
            .map(|(_, index)| *index)
            .expect("the learner is part of the run");
        let leader_applied = report
            .final_applied
            .iter()
            .map(|(_, index)| *index)
            .max()
            .expect("the run has nodes");
        assert_eq!(
            learner_applied, leader_applied,
            "learner catch-up seed {seed}: the learner ends fully caught up with the leader"
        );
        assert!(
            learner_applied.0 >= 32,
            "learner catch-up seed {seed}: the load must be real (past the compaction \
             threshold), got {}",
            learner_applied.0
        );
    }
}
