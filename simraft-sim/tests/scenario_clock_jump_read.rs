//! Clock-free reads (invariant R-04): a virtual clock jump far
//! past every deadline, landing while reads are pending, changes
//! which reads are pending, never which values they may return;
//! the full history still checks linearizable.

use simraft_sim::faults::{FaultConfig, ScriptedOp};
use simraft_sim::liz::Verdict;
use simraft_sim::{Harness, SimConfig};

#[test]
fn huge_clock_jump_mid_read_stays_linearizable() {
    let config = SimConfig {
        seed: 61,
        faults: FaultConfig {
            clock_jump_per_mille: 20,
            max_clock_jump: 200_000,
            ..FaultConfig::default()
        },
        scripted: vec![(2_000, ScriptedOp::JumpClock(500_000))],
        ..SimConfig::default()
    };
    let report = Harness::new(config).run();
    assert!(report.oracle_failure.is_none());
    assert_eq!(
        report.verdict,
        Verdict::Linearizable,
        "R-04: clock jumps must not change what reads may return"
    );
    assert!(report.passed());
}
