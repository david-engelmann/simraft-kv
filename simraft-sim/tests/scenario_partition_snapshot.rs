//! Partition during snapshot transfer (post-M4 Lane 2,
//! invariant H-08): a follower is cut off while the leader
//! writes far past it and compacts, so the follower's only way
//! back is a snapshot. The partition spans the window in which
//! the leader first tries to relay that snapshot; after the
//! partition heals, the transfer completes, the follower
//! installs it, and the cluster converges.

use simraft_core::NodeId;
use simraft_sim::faults::{DirectedCut, FaultConfig};
use simraft_sim::{Harness, SimConfig};

#[test]
fn partition_during_snapshot_transfer_still_converges() {
    let mut faults = FaultConfig::none();
    for (from, to) in [(1, 3), (3, 1), (2, 3), (3, 2)] {
        faults.partitions.push(DirectedCut {
            from: NodeId(from),
            to: NodeId(to),
            start: 1_500,
            end: 6_000,
        });
    }
    let config = SimConfig {
        seed: 81,
        chaos_ticks: 8_000,
        heal_ticks: 4_000,
        faults,
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        report
            .trace
            .lines()
            .iter()
            .any(|line| line.contains("snapshot_received n3")),
        "node 3 must actually receive a snapshot (H-08 evidence), not merely catch up by appends"
    );
    assert!(
        report.passed(),
        "the follower must install the snapshot after the heal and converge"
    );
    let applied: Vec<_> = report.final_applied.iter().map(|(_, i)| *i).collect();
    assert!(
        applied.iter().all(|index| *index == applied[0]),
        "the snapshotted follower ends at the same applied index: {applied:?}"
    );
    assert!(
        applied[0].0 >= 32,
        "the run must have written well past the compaction threshold, got {}",
        applied[0].0
    );
}
