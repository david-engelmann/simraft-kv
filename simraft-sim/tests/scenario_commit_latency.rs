//! The commit latency bound (invariant L-02): in a fault-free
//! window, an entry proposed to a healthy leader commits within
//! two heartbeat intervals plus one tick, and applies within one
//! further tick, across cluster sizes 1, 3, and 5.

use simraft_sim::faults::FaultConfig;
use simraft_sim::{Harness, SimConfig};

#[test]
fn fault_free_commit_latency_is_bounded() {
    let core = simraft_core::Config::default();
    let bound = 2 * core.heartbeat_interval + 1;
    for node_count in [1_usize, 3, 5] {
        let config = SimConfig {
            seed: 81,
            node_count,
            faults: FaultConfig::none(),
            ..SimConfig::default()
        };
        let report = Harness::new(config).run();
        assert!(
            report.passed(),
            "L-02: fault-free cluster of {node_count} nodes must pass"
        );
        assert!(
            report.max_commit_latency_ticks > 0,
            "the workload must complete writes to measure ({node_count} nodes)"
        );
        assert!(
            report.max_commit_latency_ticks <= bound,
            "L-02 ({node_count} nodes): worst commit latency {} exceeds the bound {}",
            report.max_commit_latency_ticks,
            bound
        );
    }
}
