//! Property tests for configuration validation and election timer
//! randomization (invariants T-01 and T-06).

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use proptest::prelude::*;
use simraft_core::{Config, ConfigError, Node, NodeId};

fn config_with(heartbeat: u64, min: u64, max: u64) -> Config {
    Config {
        heartbeat_interval: heartbeat,
        min_election_timeout: min,
        max_election_timeout: max,
        ..Config::default()
    }
}

proptest! {
    #[test]
    fn invalid_timing_ratios_are_rejected(
        heartbeat in 1_u64..10_000,
        min in 0_u64..20_000,
        max in 0_u64..40_000,
    ) {
        let cfg = config_with(heartbeat, min, max);
        let legal = min >= 10 * heartbeat && max >= 2 * min && max > min;
        if legal {
            prop_assert!(cfg.validate().is_ok(), "legal config rejected: {cfg:?}");
        } else {
            prop_assert!(cfg.validate().is_err(), "illegal config accepted: {cfg:?}");
        }
    }

    #[test]
    fn zero_limits_are_rejected(which in 0_usize..5) {
        let mut cfg = Config::default();
        match which {
            0 => cfg.max_entries_per_append = 0,
            1 => cfg.max_append_bytes = 0,
            2 => cfg.snapshot_chunk_bytes = 0,
            3 => cfg.max_sessions = 0,
            _ => cfg.max_uncommitted_entries = 0,
        }
        let rejected = matches!(cfg.validate(), Err(ConfigError::ZeroValue { .. }));
        prop_assert!(rejected, "zero limit must be rejected as ZeroValue");
    }
}

#[test]
fn error_names_both_values() {
    let err = config_with(50, 100, 1_000).validate().unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("100") && text.contains("50"),
        "ratio error must name both values, got: {text}"
    );
    match err {
        ConfigError::ElectionTimeoutRatio {
            min_election_timeout: 100,
            heartbeat_interval: 50,
            required_min: 500,
        } => {}
        other => panic!("unexpected error variant: {other:?}"),
    }
}

#[test]
fn deadline_offsets_stay_in_range_and_vary_by_node() {
    let cluster = common::all_voters(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let cfg = Config::default();
    let mut deadlines = Vec::new();
    for id in 1_u64..=10 {
        let node = Node::new(NodeId(id), cfg.clone(), cluster.clone(), 42).unwrap();
        let deadline = node.poll_timeout().expect("a follower has a deadline");
        assert!(
            (cfg.min_election_timeout..cfg.max_election_timeout).contains(&deadline),
            "deadline {deadline} outside the configured election window"
        );
        deadlines.push(deadline);
    }
    deadlines.sort_unstable();
    deadlines.dedup();
    assert!(
        deadlines.len() > 1,
        "election deadlines must vary by node, got one value for all ten nodes"
    );
}
