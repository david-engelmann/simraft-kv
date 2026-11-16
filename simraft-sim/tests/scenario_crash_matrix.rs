//! Crash recovery (invariant H-03): under a heavy crash rate,
//! nodes restart with exactly their durable state and the cluster
//! still converges; the oracles (including election safety, which
//! a double vote would break) never fire.

use simraft_sim::faults::FaultConfig;
use simraft_sim::{Harness, SimConfig};

#[test]
fn frequent_crashes_preserve_safety_and_converge() {
    let faults = FaultConfig {
        crash_per_mille: 8,
        ..FaultConfig::default()
    };
    let config = SimConfig {
        seed: 21,
        faults,
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        report.oracle_failure.is_none(),
        "H-03: no oracle may fire under crashes, got {:?}",
        report.oracle_failure
    );
    assert!(report.passed(), "H-03: the cluster must still converge");
}

#[test]
fn scripted_crash_of_each_node_in_rotation() {
    use simraft_core::NodeId;
    use simraft_sim::faults::ScriptedOp;
    let config = SimConfig {
        seed: 22,
        faults: FaultConfig::none(),
        scripted: vec![
            (1_000, ScriptedOp::Crash(NodeId(1))),
            (2_000, ScriptedOp::Crash(NodeId(2))),
            (3_000, ScriptedOp::Crash(NodeId(3))),
        ],
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(report.oracle_failure.is_none());
    assert!(
        report.passed(),
        "every crashed node must recover and converge"
    );
}
