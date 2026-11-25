//! Cluster configuration representation and single-server membership
//! change rules (`AddLearner`, `PromoteVoter`, `RemoveVoter`).
//!
//! Every quorum decision is computed against exactly one
//! `ClusterConfig` (invariant M-05).

use crate::error::{ConfigError, ProtocolError};
use crate::ids::LogIndex;
use crate::ids::NodeId;
use std::collections::BTreeSet;

/// The membership of the cluster: disjoint voter and learner sets.
///
/// A node appears in at most one set. Learners receive replication but
/// never vote, never start elections, and never count toward any quorum.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ClusterConfig {
    /// Nodes that vote and count toward quorums.
    pub voters: BTreeSet<NodeId>,
    /// Nodes that replicate without voting.
    pub learners: BTreeSet<NodeId>,
}

impl ClusterConfig {
    /// Build a configuration from voter and learner id lists.
    ///
    /// # Errors
    ///
    /// Returns `ConfigError::EmptyVoters` when the voter list is empty
    /// (a cluster may shrink to one voter, never to none) and
    /// `ConfigError::OverlappingMember` when a node appears in both
    /// `voters` and `learners`.
    pub fn new(
        voters: impl IntoIterator<Item = NodeId>,
        learners: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, ConfigError> {
        let cfg = Self {
            voters: voters.into_iter().collect(),
            learners: learners.into_iter().collect(),
        };
        if cfg.voters.is_empty() {
            return Err(ConfigError::EmptyVoters);
        }
        if let Some(&node) = cfg.voters.intersection(&cfg.learners).next() {
            return Err(ConfigError::OverlappingMember { node });
        }
        Ok(cfg)
    }

    /// The number of voters that makes a quorum of this configuration.
    #[must_use]
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Whether `id` may vote in this configuration.
    #[must_use]
    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    /// Whether `id` is a member in either role.
    #[must_use]
    pub fn contains(&self, id: NodeId) -> bool {
        self.voters.contains(&id) || self.learners.contains(&id)
    }

    /// Every member, voters and learners, in ascending id order.
    pub fn members(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.voters.union(&self.learners).copied()
    }
}

/// One single-server membership change (docs/ARCHITECTURE.md section
/// 9). Each change is carried by exactly one config log entry; at most
/// one change is ever uncommitted (invariant M-01).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChange {
    /// The node joins as a learner: it receives replication and
    /// snapshots but never votes and never counts toward a quorum.
    AddLearner {
        /// The node joining as a learner.
        node: NodeId,
    },
    /// A caught-up learner becomes a voter.
    PromoteVoter {
        /// The learner being promoted.
        node: NodeId,
    },
    /// A voter or learner leaves the cluster.
    RemoveVoter {
        /// The member being removed.
        node: NodeId,
    },
}

impl ClusterConfig {
    /// The configuration produced by applying `change` to this one.
    ///
    /// This checks the set-level rules only (who may be added,
    /// promoted, or removed). The leader-side rules that need log
    /// state (one uncommitted change, the `NoOp` gate, promotion
    /// catch-up) are enforced by `Node` before it appends the entry.
    ///
    /// # Errors
    ///
    /// Returns the `ProtocolError` naming the rule the change breaks.
    pub fn apply_change(&self, change: ConfigChange) -> Result<Self, ProtocolError> {
        let mut next = self.clone();
        match change {
            ConfigChange::AddLearner { node } => {
                if self.voters.contains(&node) {
                    return Err(ProtocolError::AlreadyVoter { node });
                }
                next.learners.insert(node);
            }
            ConfigChange::PromoteVoter { node } => {
                if self.voters.contains(&node) {
                    return Err(ProtocolError::AlreadyVoter { node });
                }
                if !self.learners.contains(&node) {
                    return Err(ProtocolError::NotLearner { node });
                }
                next.learners.remove(&node);
                next.voters.insert(node);
            }
            ConfigChange::RemoveVoter { node } => {
                if self.voters.contains(&node) {
                    if self.voters.len() == 1 {
                        return Err(ProtocolError::LastVoterRemoval { node });
                    }
                    next.voters.remove(&node);
                } else if self.learners.contains(&node) {
                    next.learners.remove(&node);
                } else {
                    return Err(ProtocolError::NotMember { node });
                }
            }
        }
        Ok(next)
    }
}

/// The node's view of cluster membership (section 5.3).
///
/// Holds the newest configuration whose entry exists in the log, the
/// newest committed configuration, and the index of the newest
/// config entry. Every quorum decision uses exactly one of these
/// configurations, never a blend (invariant M-05).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigState {
    /// The newest configuration from the log, committed or not.
    pub latest: ClusterConfig,
    /// The newest committed configuration.
    pub committed: ClusterConfig,
    /// The log index of the newest config entry, or 0 when the log
    /// holds no config entry (the construction configuration rules).
    pub last_config_index: LogIndex,
}

impl ConfigState {
    /// A state where the construction configuration is both the
    /// latest and the committed configuration.
    #[must_use]
    pub fn new(initial: ClusterConfig) -> Self {
        Self {
            committed: initial.clone(),
            latest: initial,
            last_config_index: LogIndex::ZERO,
        }
    }
}
