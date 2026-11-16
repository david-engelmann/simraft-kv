//! Disk corruption (post-M4 Lane 2, invariants H-05 and H-06):
//! a flipped byte in a durable log record truncates that node's
//! log at the record on restore, and a flipped byte in a durable
//! snapshot payload gets the snapshot rejected on restore. In
//! both cases the node rejoins, catches up, and the cluster
//! converges with a linearizable history.

use simraft_core::{LogIndex, NodeId};
use simraft_sim::faults::{DirectedCut, FaultConfig, ScriptedOp};
use simraft_sim::{Harness, SimConfig};

fn trace_has(report: &simraft_sim::RunReport, needle: &str) -> bool {
    report
        .trace
        .lines()
        .iter()
        .any(|line| line.contains(needle))
}

#[test]
fn corrupt_log_record_truncates_on_restore_and_the_cluster_heals() {
    let config = SimConfig {
        seed: 78,
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
    };
    let report = Harness::new(config).run();
    assert!(
        trace_has(&report, "corrupt_log n3 index 5 flipped=true"),
        "the scripted corruption must land on a real record"
    );
    assert!(
        trace_has(&report, "log_truncated n3"),
        "the restore must report the torn record (H-05 evidence)"
    );
    assert!(
        report.passed(),
        "the truncated node must catch up and the cluster converge"
    );
    let applied: Vec<LogIndex> = report
        .final_applied
        .iter()
        .map(|(_, index)| *index)
        .collect();
    assert!(
        applied.iter().all(|index| *index == applied[0]),
        "every node ends at the same applied index: {applied:?}"
    );
}

#[test]
fn corrupt_snapshot_is_rejected_on_restore_and_the_log_replays() {
    // Node 3 is partitioned away while the leader writes and
    // compacts, healed long enough to receive and persist a
    // snapshot, then its durable snapshot is corrupted and the
    // node crashes: the restore must reject the snapshot and
    // replay the log the disk still holds.
    let mut faults = FaultConfig::none();
    for (from, to) in [(1, 3), (3, 1), (2, 3), (3, 2)] {
        faults.partitions.push(DirectedCut {
            from: NodeId(from),
            to: NodeId(to),
            start: 1_200,
            end: 4_500,
        });
    }
    let config = SimConfig {
        seed: 79,
        chaos_ticks: 8_000,
        heal_ticks: 4_000,
        faults,
        scripted: vec![
            (6_000, ScriptedOp::CorruptSnapshot { node: NodeId(3) }),
            (6_010, ScriptedOp::Crash(NodeId(3))),
        ],
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        trace_has(&report, "corrupt_snapshot n3 flipped=true"),
        "node 3 must hold a durable snapshot by tick 6000"
    );
    assert!(
        trace_has(&report, "snapshot_rejected n3"),
        "the restore must reject the corrupt snapshot (H-06 evidence)"
    );
    assert!(
        report.passed(),
        "the node must replay its log, catch up, and converge"
    );
}
