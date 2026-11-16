//! The commit latency bound (invariant L-02): in a fault-free
//! window, an entry proposed to a healthy leader commits within
//! two heartbeat intervals plus one tick, and applies within one
//! further tick. The harness measures Propose-to-Apply latency in
//! virtual ticks for every completed client write.

use simraft_sim::faults::FaultConfig;
use simraft_sim::{Harness, SimConfig};

#[test]
fn fault_free_commit_latency_is_bounded() {
    let core = simraft_core::Config::default();
    let bound = 2 * core.heartbeat_interval + 1;
    let config = SimConfig {
        seed: 81,
        faults: FaultConfig::none(),
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(report.passed());
    assert!(
        report.max_commit_latency_ticks > 0,
        "the workload must complete writes to measure"
    );
    assert!(
        report.max_commit_latency_ticks <= bound,
        "L-02: worst commit latency {} exceeds the bound {}",
        report.max_commit_latency_ticks,
        bound
    );
}
