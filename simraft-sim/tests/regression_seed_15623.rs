//! Regression pin for the seed 15623 history fabrication
//! (invariant R-05). The harness completed a write record
//! with a default Ack when the leader's commit response
//! raced the state machine outcomes: the leader's own
//! machine was replaying after a restart and gapped the
//! apply, the followers' applies landed a tick later, and
//! at response time no outcome existed, so the record
//! showed a success the machine never returned (it had
//! rejected the command with a session gap, and no node
//! ever executed the write). The recorded history was not
//! linearizable even though the cluster behaved correctly.
//! The harness now parks such a record until an apply
//! records the real outcome, and failed applies record
//! their error as well. This seed passes with the honest
//! history.

use simraft_sim::{Harness, SimConfig};

#[test]
fn seed_15623_history_is_linearizable() {
    let config = SimConfig {
        seed: 15623,
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(
        report.passed(),
        "seed 15623 must pass with an honestly recorded history"
    );
}
