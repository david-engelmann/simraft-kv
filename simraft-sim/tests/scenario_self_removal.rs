//! Self-removal through the simulator (invariant M-03): the
//! leader proposes its own removal, steps down only after the
//! entry commits, and the remaining cluster elects a successor
//! and converges without it.

use simraft_sim::faults::{FaultConfig, ScriptedOp};
use simraft_sim::{Harness, SimConfig};

#[test]
fn leader_removes_itself_and_the_cluster_converges() {
    let config = SimConfig {
        seed: 71,
        faults: FaultConfig::none(),
        scripted: vec![(2_000, ScriptedOp::RemoveLeader)],
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(report.oracle_failure.is_none());
    assert!(report.passed(), "the run must converge after self-removal");
    let removed = report
        .leader_history
        .first()
        .map(|(_, leader, _)| *leader)
        .expect("a leader must have been elected before the removal");
    assert_ne!(
        report.final_leader,
        Some(removed),
        "M-03: the removed leader must not lead at the end"
    );
    assert!(
        report.leader_history.len() >= 2,
        "M-03: a successor must have been elected"
    );
}
