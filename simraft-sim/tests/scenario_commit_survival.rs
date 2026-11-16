//! Leader completeness end to end (invariant S-04): entries
//! committed under one leader survive that leader's crash and a
//! forced successor election; the final digests agree on every
//! node, which is only possible if the committed entries are in
//! every later leader's log.

use simraft_core::NodeId;
use simraft_sim::faults::{FaultConfig, ScriptedOp};
use simraft_sim::{Harness, SimConfig};

#[test]
fn committed_entries_survive_the_leaders_crash() {
    let config = SimConfig {
        seed: 41,
        faults: FaultConfig::none(),
        scripted: vec![(2_500, ScriptedOp::Crash(NodeId(1)))],
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(report.oracle_failure.is_none());
    assert!(
        report.converged,
        "S-04: all nodes must converge to identical digests"
    );
    assert!(report.passed());
}
