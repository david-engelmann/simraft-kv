//! Cluster configuration representation.
//!
//! At M1 the configuration is static: it is supplied at node construction
//! and never changes. Single-server configuration changes (AddLearner,
//! PromoteVoter, RemoveVoter) land at M3 and extend this module; the
//! representation and the quorum arithmetic they rely on live here from
//! the start, so every quorum decision at every milestone is computed
//! against exactly one `ClusterConfig` (invariant M-05).

use crate::error::ConfigError;
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
    /// Returns `ConfigError::EmptyVoters` when the voter list is empty: a
    /// cluster may shrink to one voter, never to none.
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
