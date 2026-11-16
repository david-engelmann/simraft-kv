//! Linearizable client histories under the chaos matrix
//! (invariant R-05): the recorded history of every run must check
//! out; an Unknown verdict fails the test, never passes it.

use simraft_sim::liz::Verdict;
use simraft_sim::{Harness, SimConfig};

#[test]
fn chaos_histories_are_linearizable() {
    for seed in [31_u64, 32, 33] {
        let config = SimConfig {
            seed,
            ..SimConfig::default()
        };
        let report = Harness::new(config).run();
        assert!(
            !report.history.is_empty(),
            "seed {seed}: the workload must record operations"
        );
        assert_eq!(
            report.verdict,
            Verdict::Linearizable,
            "seed {seed}: the history must be linearizable"
        );
        assert!(report.passed(), "seed {seed}: the run must pass in full");
    }
}
