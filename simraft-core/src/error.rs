//! Domain error types for simraft-core.
//!
//! Every error carries concrete diagnostic context (the offending values,
//! the required bounds, the conflicting fields), never a bare variant
//! name. See invariant E-01 in docs/INVARIANTS.md.

use core::fmt;

/// A cluster configuration failed validation.
///
/// Returned by `Config::validate` and by `Node::new`. Each variant names
/// the fields involved and the values that conflict, so an operator can
/// fix the configuration without reading the validation code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A field that must be positive was zero.
    ZeroValue {
        /// The field that was zero.
        field: &'static str,
    },
    /// `min_election_timeout` is below `10 * heartbeat_interval`.
    ElectionTimeoutRatio {
        /// The configured minimum election timeout, in ticks.
        min_election_timeout: u64,
        /// The configured heartbeat interval, in ticks.
        heartbeat_interval: u64,
        /// The smallest legal minimum election timeout for this heartbeat
        /// interval, in ticks.
        required_min: u64,
    },
    /// `max_election_timeout` is below `2 * min_election_timeout`.
    ElectionRangeRatio {
        /// The configured minimum election timeout, in ticks.
        min_election_timeout: u64,
        /// The configured maximum election timeout, in ticks.
        max_election_timeout: u64,
        /// The smallest legal maximum election timeout, in ticks.
        required_min_max: u64,
    },
    /// `max_election_timeout` does not exceed `min_election_timeout`, so
    /// the randomized election window is empty.
    ElectionRangeOrder {
        /// The configured minimum election timeout, in ticks.
        min_election_timeout: u64,
        /// The configured maximum election timeout, in ticks.
        max_election_timeout: u64,
    },
    /// A proposed cluster configuration has no voters.
    EmptyVoters,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroValue { field } => {
                write!(f, "config field {field} must be positive, got 0")
            }
            Self::ElectionTimeoutRatio {
                min_election_timeout,
                heartbeat_interval,
                required_min,
            } => write!(
                f,
                "min_election_timeout {min_election_timeout} is below 10 * heartbeat_interval \
                 {heartbeat_interval}; required minimum is {required_min}"
            ),
            Self::ElectionRangeRatio {
                min_election_timeout,
                max_election_timeout,
                required_min_max,
            } => write!(
                f,
                "max_election_timeout {max_election_timeout} is below 2 * min_election_timeout \
                 {min_election_timeout}; required minimum is {required_min_max}"
            ),
            Self::ElectionRangeOrder {
                min_election_timeout,
                max_election_timeout,
            } => write!(
                f,
                "max_election_timeout {max_election_timeout} must exceed \
                 min_election_timeout {min_election_timeout}"
            ),
            Self::EmptyVoters => {
                write!(f, "cluster configuration must keep at least one voter")
            }
        }
    }
}

impl std::error::Error for ConfigError {}
