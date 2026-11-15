//! Domain error types for simraft-core.
//!
//! Every error carries concrete diagnostic context (the offending values,
//! the required bounds, the conflicting fields), never a bare variant
//! name. See invariant E-01 in docs/INVARIANTS.md.

use crate::ids::{LogIndex, NodeId, Term};
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

/// A protocol-level rejection with the deciding values named.
///
/// The request was well formed but the protocol rules forbid it in
/// the current state. Every variant names the values that decided
/// the rejection (invariant E-01, docs/ARCHITECTURE.md section 13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// A configuration change was proposed while an earlier one is
    /// still uncommitted (invariant M-01).
    ConfigChangePending {
        /// The log index of the newest, still uncommitted config entry.
        last_config_index: LogIndex,
        /// The leader's commit index when the change was rejected.
        commit_index: LogIndex,
    },
    /// A configuration change was proposed before the leader's
    /// current-term `NoOp` entry committed (invariant M-02).
    LeaderNoopUncommitted {
        /// The index of the leader's election `NoOp` entry.
        noop_index: LogIndex,
        /// The leader's commit index when the change was rejected.
        commit_index: LogIndex,
    },
    /// A learner was proposed for promotion before it had replicated
    /// through the leader's commit index (invariant M-04).
    LearnerNotCaughtUp {
        /// The learner proposed for promotion.
        node: NodeId,
        /// The learner's match index on the leader.
        match_index: LogIndex,
        /// The index the learner must reach before promotion.
        required: LogIndex,
    },
    /// An `AddLearner` or `PromoteVoter` named a node that is already
    /// a voter.
    AlreadyVoter {
        /// The node named by the change.
        node: NodeId,
    },
    /// A `PromoteVoter` named a node that is not a learner.
    NotLearner {
        /// The node named by the change.
        node: NodeId,
    },
    /// A change named a node that is not a member in either role.
    NotMember {
        /// The node named by the change.
        node: NodeId,
    },
    /// A `RemoveVoter` named the last remaining voter; a cluster may
    /// shrink to one voter, never to none.
    LastVoterRemoval {
        /// The node named by the change.
        node: NodeId,
    },
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigChangePending {
                last_config_index,
                commit_index,
            } => write!(
                f,
                "config change rejected: entry at index {last_config_index} is still \
                 uncommitted (commit index {commit_index})"
            ),
            Self::LeaderNoopUncommitted {
                noop_index,
                commit_index,
            } => write!(
                f,
                "config change rejected: leader NoOp at index {noop_index} has not \
                 committed (commit index {commit_index})"
            ),
            Self::LearnerNotCaughtUp {
                node,
                match_index,
                required,
            } => write!(
                f,
                "learner {node} cannot be promoted: match index {match_index} is below \
                 the required index {required}"
            ),
            Self::AlreadyVoter { node } => {
                write!(f, "node {node} is already a voter")
            }
            Self::NotLearner { node } => {
                write!(f, "node {node} is not a learner")
            }
            Self::NotMember { node } => {
                write!(f, "node {node} is not a cluster member")
            }
            Self::LastVoterRemoval { node } => {
                write!(
                    f,
                    "removing node {node} would leave the cluster with no voters"
                )
            }
        }
    }
}

impl std::error::Error for ProtocolError {}

/// A snapshot transfer failed. The variant names the snapshot identity
/// and the offsets that decided the failure (docs/ARCHITECTURE.md
/// section 13).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotError {
    /// A chunk arrived at an offset other than the number of bytes
    /// already buffered for its snapshot identity (invariant N-02).
    ChunkOffset {
        /// The snapshot's last included index.
        last_included_index: LogIndex,
        /// The snapshot's last included term.
        last_included_term: Term,
        /// The offset the follower was ready to accept.
        expected_offset: u64,
        /// The offset the chunk actually carried.
        received_offset: u64,
    },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChunkOffset {
                last_included_index,
                last_included_term,
                expected_offset,
                received_offset,
            } => write!(
                f,
                "snapshot chunk for (index {last_included_index}, term {last_included_term}) \
                 arrived at offset {received_offset}, expected offset {expected_offset}"
            ),
        }
    }
}

impl std::error::Error for SnapshotError {}
