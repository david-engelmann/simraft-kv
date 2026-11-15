//! Tests that domain errors carry their diagnostic context
//! (invariant E-01, config error portion).

use simraft_core::{ClusterConfig, Config, ConfigError};

#[test]
fn config_error_names_both_timeout_values() {
    let cfg = Config {
        heartbeat_interval: 25,
        min_election_timeout: 100,
        max_election_timeout: 1_000,
        ..Config::default()
    };
    let text = cfg.validate().unwrap_err().to_string();
    assert!(text.contains("100"), "missing the min value: {text}");
    assert!(text.contains("25"), "missing the heartbeat value: {text}");
    assert!(text.contains("250"), "missing the required bound: {text}");
}

#[test]
fn range_ratio_error_names_both_values_and_bound() {
    let cfg = Config {
        heartbeat_interval: 10,
        min_election_timeout: 500,
        max_election_timeout: 900,
        ..Config::default()
    };
    match cfg.validate().unwrap_err() {
        ConfigError::ElectionRangeRatio {
            min_election_timeout: 500,
            max_election_timeout: 900,
            required_min_max: 1_000,
        } => {}
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn zero_value_error_names_the_field() {
    let cfg = Config {
        heartbeat_interval: 0,
        ..Config::default()
    };
    let text = cfg.validate().unwrap_err().to_string();
    assert!(
        text.contains("heartbeat_interval"),
        "error must name the field: {text}"
    );
}

#[test]
fn empty_voters_error_is_explicit() {
    let err = ClusterConfig::new(std::iter::empty(), std::iter::empty()).unwrap_err();
    assert_eq!(err, ConfigError::EmptyVoters);
    assert!(err.to_string().contains("at least one voter"));
}

#[test]
fn membership_rejections_name_the_deciding_values() {
    use simraft_core::{LogIndex, NodeId, ProtocolError};
    let pending = ProtocolError::ConfigChangePending {
        last_config_index: LogIndex(7),
        commit_index: LogIndex(3),
    }
    .to_string();
    assert!(pending.contains("index 7"), "got: {pending}");
    assert!(pending.contains("commit index 3"), "got: {pending}");
    let noop = ProtocolError::LeaderNoopUncommitted {
        noop_index: LogIndex(1),
        commit_index: LogIndex::ZERO,
    }
    .to_string();
    assert!(noop.contains("NoOp at index 1"), "got: {noop}");
    assert!(noop.contains("commit index 0"), "got: {noop}");
    let caught = ProtocolError::LearnerNotCaughtUp {
        node: NodeId(4),
        match_index: LogIndex(2),
        required: LogIndex(5),
    }
    .to_string();
    assert!(caught.contains("learner 4"), "got: {caught}");
    assert!(caught.contains("match index 2"), "got: {caught}");
    assert!(caught.contains("required index 5"), "got: {caught}");
}

#[test]
fn snapshot_rejection_names_identity_and_offsets() {
    use simraft_core::{LogIndex, SnapshotError, Term};
    let err = SnapshotError::ChunkOffset {
        last_included_index: LogIndex(9),
        last_included_term: Term(3),
        expected_offset: 12,
        received_offset: 40,
    }
    .to_string();
    assert!(err.contains("index 9"), "got: {err}");
    assert!(err.contains("term 3"), "got: {err}");
    assert!(err.contains("offset 40"), "got: {err}");
    assert!(err.contains("expected offset 12"), "got: {err}");
}
