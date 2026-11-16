//! The chaos matrix (invariants H-01 and H-02): asymmetric
//! partitions (one-way cuts, chains, full isolation), drops,
//! duplication, reordering, crashes, and clock jumps during the
//! chaos phase must never fire a safety oracle, and the healing
//! phase must converge to one stable leader with identical logs
//! and state digests and a linearizable history.

use simraft_core::NodeId;
use simraft_sim::faults::{DirectedCut, FaultConfig};
use simraft_sim::{Harness, SimConfig};

fn run(seed: u64, faults: FaultConfig) -> simraft_sim::RunReport {
    let config = SimConfig {
        seed,
        faults,
        ..SimConfig::default()
    };
    Harness::new(config).run()
}

const fn cut(from: u64, to: u64, end: u64) -> DirectedCut {
    DirectedCut {
        from: NodeId(from),
        to: NodeId(to),
        start: 200,
        end,
    }
}

#[test]
fn one_way_cut_heals_and_converges() {
    // Node 1 can send to node 2 but never hears back for most of
    // the chaos phase.
    let faults = FaultConfig {
        partitions: vec![cut(2, 1, 3_500)],
        ..FaultConfig::default()
    };
    let report = run(11, faults);
    assert!(
        report.oracle_failure.is_none(),
        "H-01: no oracle may fire, got {:?}",
        report.oracle_failure
    );
    assert!(report.passed(), "H-02: the run must heal and converge");
}

#[test]
fn chain_partition_heals_and_converges() {
    // A->B and B->C flow; A and C are cut in both directions.
    let faults = FaultConfig {
        partitions: vec![cut(1, 3, 3_500), cut(3, 1, 3_500)],
        ..FaultConfig::default()
    };
    let report = run(12, faults);
    assert!(report.oracle_failure.is_none());
    assert!(report.passed(), "H-02: the chain must heal and converge");
}

#[test]
fn full_isolation_heals_and_converges() {
    let faults = FaultConfig {
        partitions: vec![
            cut(1, 3, 3_500),
            cut(3, 1, 3_500),
            cut(2, 3, 3_500),
            cut(3, 2, 3_500),
        ],
        ..FaultConfig::default()
    };
    let report = run(13, faults);
    assert!(report.oracle_failure.is_none());
    assert!(report.passed(), "H-02: the isolated node must rejoin");
}
