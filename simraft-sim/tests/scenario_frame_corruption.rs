//! Frame corruption (post-M4 Lane 2, invariant H-04): with a
//! third of all chaos-phase frames carrying a flipped bit, every
//! corrupted frame is rejected by the receiver's frame checks
//! and never stepped into a node, no safety oracle fires, and
//! the cluster still heals and converges.

use simraft_sim::faults::FaultConfig;
use simraft_sim::{Harness, SimConfig};

#[test]
fn corrupted_frames_are_rejected_and_the_cluster_heals() {
    let faults = FaultConfig {
        corrupt_per_mille: 300,
        ..FaultConfig::default()
    };
    let config = SimConfig {
        seed: 77,
        faults,
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    let corrupted = report
        .trace
        .lines()
        .iter()
        .filter(|line| line.contains("frame_corrupted"))
        .count();
    assert!(
        corrupted > 0,
        "the run must actually corrupt frames (H-04 evidence)"
    );
    assert!(
        report.oracle_failure.is_none(),
        "no oracle may fire under frame corruption: {:?}",
        report.oracle_failure
    );
    assert!(
        report.passed(),
        "the cluster must heal and converge after the chaos phase"
    );
}
