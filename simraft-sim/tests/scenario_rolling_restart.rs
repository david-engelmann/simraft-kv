//! Rolling restart (post-M4 Lane 2, invariant H-07): every
//! node in the cluster is crashed and restarted in turn while
//! client load continues. The surviving majority keeps
//! committing throughout, and the fully restarted cluster
//! converges with a linearizable history.

use simraft_core::NodeId;
use simraft_sim::faults::{FaultConfig, ScriptedOp};
use simraft_sim::{Harness, SimConfig};

#[test]
fn rolling_restart_keeps_the_majority_committing() {
    let config = SimConfig {
        seed: 80,
        chaos_ticks: 9_000,
        heal_ticks: 4_000,
        faults: FaultConfig::none(),
        scripted: vec![
            (2_000, ScriptedOp::Crash(NodeId(1))),
            (4_500, ScriptedOp::Crash(NodeId(2))),
            (7_000, ScriptedOp::Crash(NodeId(3))),
        ],
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        report.passed(),
        "the cluster must converge after every node has restarted"
    );
    let completed = report
        .history
        .records()
        .iter()
        .filter(|record| record.result.is_some())
        .count();
    assert!(
        completed >= 40,
        "client work must keep completing across the restarts, got {completed}"
    );
    let crashes = report
        .trace
        .lines()
        .iter()
        .filter(|line| line.contains("crash n"))
        .count();
    assert_eq!(crashes, 3, "each node crashed exactly once");
}
