//! Learner catch-up under load (post-M4 Lane 2, invariant
//! H-09): a fourth node joins the genesis configuration as a
//! learner while three clients drive steady load. The learner
//! never votes, but it replicates and applies everything the
//! voters commit, and it ends the run at the same applied index
//! as the leader, part of the converged member set.

use simraft_core::NodeId;
use simraft_sim::faults::FaultConfig;
use simraft_sim::{Harness, SimConfig};

#[test]
fn genesis_learner_catches_up_under_load() {
    let config = SimConfig {
        seed: 82,
        learners: vec![NodeId(4)],
        clients: 3,
        chaos_ticks: 6_000,
        heal_ticks: 3_000,
        faults: FaultConfig::none(),
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        report.passed(),
        "the run must converge with the learner in the member set"
    );
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
        "the learner ends fully caught up with the leader"
    );
    assert!(
        learner_applied.0 >= 32,
        "the load must be real (past the compaction threshold), got {}",
        learner_applied.0
    );
}
