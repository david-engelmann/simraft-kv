//! The disruptive rejoin (invariant T-05): a node isolated for
//! many election timeouts cannot disrupt the healthy leader when
//! it rejoins; the original leader's term and leadership survive.

use simraft_core::NodeId;
use simraft_sim::faults::{DirectedCut, FaultConfig};
use simraft_sim::{Harness, SimConfig};

#[test]
fn isolated_node_rejoins_without_disruption() {
    let mut faults = FaultConfig::none();
    // Node 3 is fully isolated from tick 2500 (after the first
    // election has certainly settled: the longest election timeout
    // is 1000 ticks) until the end of the chaos phase at 8000.
    for (from, to) in [(1, 3), (3, 1), (2, 3), (3, 2)] {
        faults.partitions.push(DirectedCut {
            from: NodeId(from),
            to: NodeId(to),
            start: 2_500,
            end: 8_000,
        });
    }
    let config = SimConfig {
        seed: 51,
        chaos_ticks: 8_000,
        faults,
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(report.passed(), "the run must converge after the rejoin");
    let (first_tick, first_leader, first_term) = report
        .leader_history
        .first()
        .copied()
        .expect("a leader must have been elected");
    assert!(
        first_tick < 2_500,
        "the first leader predates the isolation"
    );
    assert_eq!(
        report.final_leader,
        Some(first_leader),
        "T-05: the original leader keeps leadership after the rejoin"
    );
    assert_eq!(
        report.final_leader_term,
        Some(first_term),
        "T-05: no new election may have occurred (term unchanged)"
    );
}
