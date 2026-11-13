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
