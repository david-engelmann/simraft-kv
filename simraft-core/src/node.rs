//! The Raft node: a pure sans-I/O state machine.
//!
//! `Node::step` consumes one `Event` and returns the `Action` values the
//! driver must execute. The node reads no clock, opens no socket, and
//! touches no disk; time arrives as `Event::Tick`, messages as
//! `Event::Message`, and durable writes complete as `Event::Persisted`.
//!
//! Roles and transitions follow docs/ARCHITECTURE.md section 6. Every
//! election runs through Pre-Vote first: a node never increments its
//! persistent term merely because its election timer fired.

use crate::action::Action;
use crate::config::Config;
use crate::error::ConfigError;
use crate::event::Event;
use crate::ids::{LogIndex, NodeId, Term, Tick};
use crate::log::Log;
use crate::membership::ClusterConfig;
use std::collections::{BTreeMap, BTreeSet};

/// The role a node currently plays. Learner is a membership status, not
/// a role: a learner runs the follower transitions with voting disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Follows the leader, votes, and replicates.
    Follower,
    /// Running a pre-vote for a hypothetical next term. No persistent
    /// state has changed.
    PreCandidate,
    /// Running a real election in its term.
    Candidate,
    /// Leading the cluster for its term.
    Leader,
}

/// Per-peer replication progress, held by a leader.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Progress {
    /// The index of the next entry to send this peer.
    pub next_index: LogIndex,
    /// The highest index known to be held contiguously by this peer.
    /// Never decreases (invariant S-07).
    pub match_index: LogIndex,
}

/// An action held back until the durable writes it depends on have been
/// acknowledged (invariant T-02).
#[derive(Debug, Clone)]
pub(crate) struct PendingAction {
    /// The hard-state persist generation that must be acknowledged.
    pub need_hard_gen: u64,
    /// The entries persist watermark that must be reached.
    pub need_entries_up_to: LogIndex,
    /// The term the action was created in. Dropped if the node's term
    /// has moved on by the time it is released.
    pub term: Term,
    /// The action to release.
    pub action: Action,
}

/// One Raft node.
#[derive(Debug)]
pub struct Node {
    pub(crate) id: NodeId,
    pub(crate) config: Config,
    pub(crate) cluster: ClusterConfig,
    pub(crate) seed: u64,
    pub(crate) role: Role,
    pub(crate) current_term: Term,
    pub(crate) voted_for: Option<NodeId>,
    pub(crate) log: Log,
    pub(crate) commit_index: LogIndex,
    pub(crate) last_applied: LogIndex,
    pub(crate) leader_id: Option<NodeId>,
    pub(crate) now: Tick,
    pub(crate) election_deadline: Tick,
    pub(crate) heartbeat_deadline: Tick,
    pub(crate) election_epoch: u64,
    pub(crate) last_leader_contact: Option<Tick>,
    pub(crate) prevote_term: Term,
    pub(crate) prevotes_received: BTreeSet<NodeId>,
    pub(crate) votes_received: BTreeSet<NodeId>,
    pub(crate) progress: BTreeMap<NodeId, Progress>,
    pub(crate) pending: Vec<PendingAction>,
    pub(crate) hard_gen_sent: u64,
    pub(crate) hard_gen_acked: u64,
    pub(crate) entries_durable_up_to: LogIndex,
}

impl Node {
    /// Create a fresh follower with an empty log in term 0.
    ///
    /// `seed` is the driver-supplied deterministic seed for this node's
    /// election timer offsets. It is not OS randomness: the same seed and
    /// the same event sequence always produce the same deadlines.
    ///
    /// # Errors
    ///
    /// Returns the `ConfigError` from `Config::validate` when the timing
    /// configuration is invalid.
    pub fn new(
        id: NodeId,
        config: Config,
        cluster: ClusterConfig,
        seed: u64,
    ) -> Result<Self, ConfigError> {
        config.validate()?;
        let mut node = Self {
            id,
            config,
            cluster,
            seed,
            role: Role::Follower,
            current_term: Term::ZERO,
            voted_for: None,
            log: Log::new(),
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            leader_id: None,
            now: 0,
            election_deadline: 0,
            heartbeat_deadline: 0,
            election_epoch: 0,
            last_leader_contact: None,
            prevote_term: Term::ZERO,
            prevotes_received: BTreeSet::new(),
            votes_received: BTreeSet::new(),
            progress: BTreeMap::new(),
            pending: Vec::new(),
            hard_gen_sent: 0,
            hard_gen_acked: 0,
            entries_durable_up_to: LogIndex::ZERO,
        };
        node.reset_election_deadline();
        Ok(node)
    }

    /// This node's id.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// The node's current role.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// The node's current term.
    #[must_use]
    pub const fn current_term(&self) -> Term {
        self.current_term
    }

    /// Who this node voted for in its current term, if anyone.
    #[must_use]
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// The node's commit index.
    #[must_use]
    pub const fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// The highest index this node has emitted an `Action::Apply` for.
    #[must_use]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// The node's log, for drivers, oracles, and tests.
    #[must_use]
    pub const fn log(&self) -> &Log {
        &self.log
    }

    /// The current cluster configuration.
    #[must_use]
    pub const fn cluster(&self) -> &ClusterConfig {
        &self.cluster
    }

    /// The node's current best hint for the leader's identity.
    #[must_use]
    pub const fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    /// A leader's replication progress for `peer`, as
    /// `(next_index, match_index)`. `None` when this node is not leading
    /// or has no progress entry for the peer.
    #[must_use]
    pub fn progress(&self, peer: NodeId) -> Option<(LogIndex, LogIndex)> {
        self.progress
            .get(&peer)
            .map(|p| (p.next_index, p.match_index))
    }

    /// The next tick at which the driver must wake this node with an
    /// `Event::Tick`, if it has a deadline pending. Leaders return their
    /// heartbeat deadline; everyone else returns their election deadline.
    #[must_use]
    pub const fn poll_timeout(&self) -> Option<Tick> {
        match self.role {
            Role::Leader => Some(self.heartbeat_deadline),
            _ => Some(self.election_deadline),
        }
    }

    /// Advance the state machine by one event, returning the actions the
    /// driver must execute, in order.
    pub fn step(&mut self, event: Event) -> Vec<Action> {
        let _ = event;
        Vec::new()
    }

    /// Deterministic election timeout offset for the current epoch, in
    /// `[0, max_election_timeout - min_election_timeout)`.
    pub(crate) fn timer_offset(&self) -> u64 {
        let span = self
            .config
            .max_election_timeout
            .saturating_sub(self.config.min_election_timeout);
        if span == 0 {
            return 0;
        }
        splitmix64(
            self.seed
                ^ self.id.0.wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ self.election_epoch.wrapping_mul(0xBF58_476D_1CE4_E5B9),
        ) % span
    }

    /// Recompute the election deadline from the current tick and epoch,
    /// advancing the epoch so consecutive deadlines differ.
    pub(crate) fn reset_election_deadline(&mut self) {
        self.election_epoch += 1;
        self.election_deadline = self
            .now
            .saturating_add(self.config.min_election_timeout)
            .saturating_add(self.timer_offset());
    }
}

/// SplitMix64 finalizer: a small deterministic mix used for election
/// timer offsets. Not a general-purpose RNG; the simulator owns the real
/// seeded stream.
pub(crate) const fn splitmix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}
