//! Regression pin for seed 912, the leader append-only
//! oracle's cross-tenure over-claim (invariant S-02). The
//! node led term 1, stepped down, had its uncommitted tail
//! legitimately overwritten while a follower, then led
//! term 2, and the oracle compared its new log against the
//! snapshot kept from the earlier tenure, failing during
//! healing. Append-only binds within one tenure: the
//! oracle now drops its remembered snapshot the moment a
//! node is observed in a non-leader role. This seed passes
//! with the tenure-scoped check, and pinning it keeps the
//! scoping from regressing silently.

use simraft_sim::{Harness, SimConfig};

#[test]
fn seed_912_history_is_linearizable() {
    let config = SimConfig {
        seed: 912,
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        report.passed(),
        "seed 912 must pass with the tenure-scoped append-only oracle"
    );
}
