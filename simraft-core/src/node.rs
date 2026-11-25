//! The Raft node: a pure sans-I/O state machine.
//!
//! `Node::step` consumes one `Event` and returns the `Action` values the
//! driver must execute. The node reads no clock, opens no socket, and
//! touches no disk; time arrives as `Event::Tick`, messages as
//! `Event::Message`, and durable writes complete as `Event::Persisted`.
//!
//! Roles and transitions follow docs/ARCHITECTURE.md section 6. Every
//! election runs through Pre-Vote first: a node never increments its
//! persistent term merely because its election timer fired. Responses
//! whose correctness depends on durable state are held in a pending
//! queue and released only by the matching `Event::Persisted`
//! (invariant T-02).

use crate::action::{Action, ClientResult, ConfigResult};
use crate::config::Config;
use crate::error::{ConfigError, ProtocolError};
use crate::event::{Event, PersistKind};
use crate::ids::{ClientId, LogIndex, NodeId, Term, Tick};
use crate::log::{EntryKind, Log, LogEntry};
use crate::membership::{ClusterConfig, ConfigChange, ConfigState};
use crate::message::Message;
use crate::snapshot::{IncomingSnapshot, LoadedSnapshot, SnapshotMeta};
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
    /// In-flight snapshot relay to this peer, when it is being caught
    /// up by `InstallSnapshot` instead of `AppendEntries`: the
    /// snapshot's (last included index, last included term) and the
    /// next chunk offset to send.
    pub snapshot: Option<(LogIndex, Term, u64)>,
}

/// One peer's replication progress, as reported in a stats
/// snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerStats {
    /// The index of the next entry to send this peer.
    pub next_index: LogIndex,
    /// The highest index known to be held contiguously by this peer.
    pub match_index: LogIndex,
}

/// A node's whole operating state at one instant, as data.
///
/// Returned by `Node::stats`. Every field is the value the node's
/// own accessor of the same meaning reports at the same instant,
/// so a snapshot taken around a step explains the step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStats {
    /// The role the node currently plays.
    pub role: Role,
    /// The node's current term.
    pub current_term: Term,
    /// The leader this node currently acknowledges, if any.
    pub leader: Option<NodeId>,
    /// The highest index known to be committed.
    pub commit_index: LogIndex,
    /// The highest index applied to the state machine.
    pub last_applied: LogIndex,
    /// The last index in the log.
    pub last_log_index: LogIndex,
    /// The log base: the index the latest snapshot covers.
    pub snapshot_index: LogIndex,
    /// Per-peer replication progress on a leader; empty otherwise.
    pub peers: BTreeMap<NodeId, PeerStats>,
}

/// An action held back until the durable writes it depends on have been
/// acknowledged (invariant T-02).
#[derive(Debug, Clone)]
pub(crate) struct PendingAction {
    /// The hard-state persist generation that must be acknowledged.
    pub need_hard_gen: u64,
    /// The entries persist watermark that must be reached.
    pub need_entries_up_to: LogIndex,
    /// The term the action was created in. Held `Send` actions are
    /// dropped if the node's term has moved on when they are released;
    /// applies and client responses are term independent and always
    /// release once their persist needs are met.
    pub term: Term,
    /// The action to release.
    pub action: Action,
}

/// One in-flight `ReadIndex` round on a leader: the requests batched
/// into it, the followers that have echoed its round token, and the
/// first current-term index that must be committed before any request
/// in the batch may be released (docs/ARCHITECTURE.md section 8).
#[derive(Debug, Clone)]
pub(crate) struct ReadBatch {
    /// The round token carried on heartbeats for this batch.
    pub round: u64,
    /// The first log index of the leader's current term.
    pub gate_index: LogIndex,
    /// Voters that have echoed `round`, including the leader itself.
    pub acks: BTreeSet<NodeId>,
    /// The pending reads, as (request id, key) pairs.
    pub requests: Vec<(u64, Vec<u8>)>,
}

/// One Raft node.
#[derive(Debug)]
pub struct Node {
    pub(crate) id: NodeId,
    pub(crate) config: Config,
    pub(crate) config_state: ConfigState,
    pub(crate) initial_config: ClusterConfig,
    pub(crate) base_config: ClusterConfig,
    pub(crate) pending_config: Option<(LogIndex, ConfigChange)>,
    pub(crate) pending_install: Option<SnapshotMeta>,
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
    pub(crate) read_round_counter: u64,
    pub(crate) pending_reads: Option<ReadBatch>,
    pub(crate) last_read_round: u64,
    pub(crate) inflight: BTreeMap<(ClientId, u64), LogIndex>,
    pub(crate) attached: BTreeMap<LogIndex, u64>,
    pub(crate) incoming_snapshot: Option<IncomingSnapshot>,
    pub(crate) loaded_snapshot: Option<LoadedSnapshot>,
    pub(crate) hard_gen_sent: u64,
    pub(crate) hard_gen_acked: u64,
    pub(crate) entries_durable_up_to: LogIndex,
    pub(crate) unacked_entry_persists: u64,
    pub(crate) stale_entry_acks: u64,
}

impl Node {
    /// Create a fresh follower with an empty log in term 0.
    ///
    /// `seed` is the driver-supplied deterministic seed for this node's
    /// election timer offsets. It is not OS randomness: the same seed
    /// and the same event sequence always produce the same deadlines.
    ///
    /// # Errors
    ///
    /// Returns the `ConfigError` from `Config::validate` when the
    /// timing configuration is invalid.
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
            config_state: ConfigState::new(cluster.clone()),
            initial_config: cluster.clone(),
            base_config: cluster,
            pending_config: None,
            pending_install: None,
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
            read_round_counter: 0,
            pending_reads: None,
            last_read_round: 0,
            inflight: BTreeMap::new(),
            attached: BTreeMap::new(),
            incoming_snapshot: None,
            loaded_snapshot: None,
            hard_gen_sent: 0,
            hard_gen_acked: 0,
            entries_durable_up_to: LogIndex::ZERO,
            unacked_entry_persists: 0,
            stale_entry_acks: 0,
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
        &self.config_state.latest
    }

    /// The log index of the newest config entry in this node's log, or
    /// 0 when the log holds none (the construction configuration
    /// rules). A leader rejects a new config change while this index
    /// is above its commit index (invariant M-01).
    #[must_use]
    pub const fn last_config_index(&self) -> LogIndex {
        self.config_state.last_config_index
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

    /// The node's whole operating state as data, for drivers,
    /// dashboards, and tests (invariant O-01).
    ///
    /// The snapshot gathers the values the individual accessors
    /// report at this instant into one comparable struct, so a
    /// driver can log, diff, or assert on a node's state without
    /// stepping it. The `peers` map carries a leader's replication
    /// progress and is empty in every other role, where progress
    /// is not tracked.
    #[must_use]
    pub fn stats(&self) -> NodeStats {
        let peers = if self.role == Role::Leader {
            self.progress
                .iter()
                .map(|(peer, progress)| {
                    (
                        *peer,
                        PeerStats {
                            next_index: progress.next_index,
                            match_index: progress.match_index,
                        },
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        NodeStats {
            role: self.role,
            current_term: self.current_term,
            leader: self.leader_id,
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            last_log_index: self.log.last_index(),
            snapshot_index: self.log.base_index(),
            peers,
        }
    }

    /// The next tick at which the driver must wake this node with an
    /// `Event::Tick`, if it has a deadline pending. Leaders return
    /// their heartbeat deadline; everyone else returns their election
    /// deadline.
    #[must_use]
    pub const fn poll_timeout(&self) -> Option<Tick> {
        match self.role {
            Role::Leader => Some(self.heartbeat_deadline),
            _ => Some(self.election_deadline),
        }
    }

    /// Advance the state machine by one event, returning the actions
    /// the driver must execute, in order.
    pub fn step(&mut self, event: Event) -> Vec<Action> {
        let mut out = Vec::new();
        match event {
            Event::Tick { now } => self.on_tick(now, &mut out),
            Event::Propose {
                client_id,
                seq,
                payload,
            } => self.on_propose(client_id, seq, payload, &mut out),
            Event::ReadIndex { request_id, key } => {
                self.on_read_index(request_id, key, &mut out);
            }
            Event::ProposeConfig { change } => self.on_propose_config(change, &mut out),
            Event::SnapshotLoaded { meta, data } => {
                self.loaded_snapshot = Some(LoadedSnapshot { meta, data });
            }
            Event::SnapshotPersisted { meta } => self.on_snapshot_persisted(&meta),
            Event::Message { from, msg } => self.on_message(from, msg, &mut out),
            Event::Persisted { kind, up_to_index } => {
                self.on_persisted(kind, up_to_index, &mut out);
            }
            Event::Restored {
                hard_state,
                entries,
                snapshot,
            } => self.on_restored(
                hard_state.current_term,
                hard_state.voted_for,
                &entries,
                snapshot.as_ref(),
            ),
        }
        out
    }

    // -- timers -------------------------------------------------------

    /// Deterministic election timeout offset for the current epoch, in
    /// `[0, max_election_timeout - min_election_timeout)`.
    pub(crate) const fn timer_offset(&self) -> u64 {
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
    pub(crate) const fn reset_election_deadline(&mut self) {
        self.election_epoch += 1;
        self.election_deadline = self
            .now
            .saturating_add(self.config.min_election_timeout)
            .saturating_add(self.timer_offset());
    }

    // -- persistence plumbing ----------------------------------------

    /// Emit a hard-state persist for the current state and return its
    /// generation number.
    fn emit_persist_hard(&mut self, out: &mut Vec<Action>) -> u64 {
        self.hard_gen_sent += 1;
        out.push(Action::PersistHardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        });
        self.hard_gen_sent
    }

    /// Emit a log-entries persist action and count it in flight so
    /// pre-truncation acknowledgments cannot advance durability past a
    /// rewritten suffix.
    fn emit_persist_entries(
        &mut self,
        from_index: LogIndex,
        entries: Vec<LogEntry>,
        out: &mut Vec<Action>,
    ) {
        self.unacked_entry_persists += 1;
        out.push(Action::PersistEntries {
            from_index,
            entries,
        });
    }

    /// Release `action` now if its persist needs are already met, else
    /// hold it in the pending queue.
    fn defer_or_emit(
        &mut self,
        out: &mut Vec<Action>,
        action: Action,
        need_hard_gen: u64,
        need_entries_up_to: LogIndex,
    ) {
        if need_hard_gen <= self.hard_gen_acked && need_entries_up_to <= self.entries_durable_up_to
        {
            out.push(action);
        } else {
            self.pending.push(PendingAction {
                need_hard_gen,
                need_entries_up_to,
                term: self.current_term,
                action,
            });
        }
    }

    /// Whether a held action can be released now: its persist needs
    /// are met, and it is not a send from an older term (a stale send
    /// is dropped, never released and never re-queued).
    fn releasable(&self, held: &PendingAction) -> bool {
        let stale_send =
            matches!(held.action, Action::Send { .. }) && held.term != self.current_term;
        !stale_send
            && held.need_hard_gen <= self.hard_gen_acked
            && held.need_entries_up_to <= self.entries_durable_up_to
    }

    /// Release every held action whose persist needs are now met.
    /// Held sends from an older term are dropped instead: their content
    /// is stale by construction.
    ///
    /// Batching (invariant B-02): when one release flushes several
    /// held `AppendEntries` sends to the same follower, only the last
    /// is emitted. A send is built from the follower's next index at
    /// build time, and that index only moves on real
    /// acknowledgements or on rejection backoff, so the last send
    /// covers every entry an earlier one carried that the follower
    /// still needs, and any range it does not cover is re-offered by
    /// the next heartbeat or proposal. No proposal waits any longer
    /// than the persist acknowledgement it already waits for. Other
    /// send kinds are never coalesced: `InstallSnapshot` chunks in
    /// particular carry their own sequencing.
    fn flush_pending(&mut self, out: &mut Vec<Action>) {
        // The pending index of the last releasable AppendEntries
        // send per follower: the only one of its run worth emitting.
        let mut last_append: BTreeMap<NodeId, usize> = BTreeMap::new();
        for (index, held) in self.pending.iter().enumerate() {
            if !self.releasable(held) {
                continue;
            }
            if let Action::Send {
                to,
                msg: Message::AppendEntries { .. },
            } = &held.action
            {
                last_append.insert(*to, index);
            }
        }
        let pending = std::mem::take(&mut self.pending);
        for (index, held) in pending.into_iter().enumerate() {
            if !self.releasable(&held) {
                let stale_send =
                    matches!(held.action, Action::Send { .. }) && held.term != self.current_term;
                if !stale_send {
                    self.pending.push(held);
                }
                continue;
            }
            let superseded_append = matches!(
                &held.action,
                Action::Send {
                    to,
                    msg: Message::AppendEntries { .. },
                } if last_append.get(to) != Some(&index)
            );
            if superseded_append {
                continue;
            }
            out.push(held.action);
        }
    }

    fn on_persisted(&mut self, kind: PersistKind, up_to_index: LogIndex, out: &mut Vec<Action>) {
        match kind {
            PersistKind::HardState => {
                self.hard_gen_acked = self
                    .hard_gen_acked
                    .saturating_add(1)
                    .min(self.hard_gen_sent);
            }
            PersistKind::Entries => {
                if self.stale_entry_acks > 0 {
                    self.stale_entry_acks -= 1;
                    self.unacked_entry_persists = self.unacked_entry_persists.saturating_sub(1);
                } else {
                    let capped = up_to_index.min(self.log.last_index());
                    if capped >= self.log.last_index() {
                        self.unacked_entry_persists = 0;
                    } else {
                        self.unacked_entry_persists = self.unacked_entry_persists.saturating_sub(1);
                    }
                    if capped > self.entries_durable_up_to {
                        self.entries_durable_up_to = capped;
                    }
                }
            }
        }
        self.flush_pending(out);
        if self.role == Role::Leader {
            self.advance_commit(out);
            self.try_release_reads(out);
        }
    }

    fn on_restored(
        &mut self,
        term: Term,
        voted_for: Option<NodeId>,
        entries: &[LogEntry],
        snapshot: Option<&SnapshotMeta>,
    ) {
        self.current_term = term;
        self.voted_for = voted_for;
        self.log = Log::new();
        // The recovered entries begin above the snapshot
        // boundary (invariant N-08): a store returns only the
        // entries its durable snapshot does not cover, so the
        // fresh log's base advances to the boundary before the
        // suffix is appended and every recovered entry lands at
        // its absolute index.
        if let Some(meta) = snapshot {
            self.log
                .advance_base(meta.last_included_index, meta.last_included_term);
        }
        self.log.append(entries);
        self.role = Role::Follower;
        self.commit_index = LogIndex::ZERO;
        self.last_applied = LogIndex::ZERO;
        self.leader_id = None;
        self.last_leader_contact = None;
        self.progress.clear();
        self.pending.clear();
        self.pending_reads = None;
        self.last_read_round = 0;
        self.inflight.clear();
        self.attached.clear();
        self.votes_received.clear();
        self.prevotes_received.clear();
        self.base_config = self.initial_config.clone();
        self.pending_config = None;
        self.pending_install = None;
        self.incoming_snapshot = None;
        self.loaded_snapshot = None;
        self.recompute_config();
        // A recovered snapshot installs its boundary at restore
        // (invariant N-07): the commit and applied cursors jump
        // to the boundary and the boundary's cluster config
        // takes over. The commit bound that guards a live
        // compaction (N-05) does not apply here: the snapshot is
        // already durable, and the recovered commit index starts
        // at zero by construction.
        if let Some(meta) = snapshot {
            self.install_snapshot(meta);
        }
        self.entries_durable_up_to = self.log.last_index();
        self.unacked_entry_persists = 0;
        self.stale_entry_acks = 0;
        self.hard_gen_sent += 1;
        self.hard_gen_acked = self.hard_gen_sent;
        self.reset_election_deadline();
    }

    // -- role transitions ---------------------------------------------

    /// Move to follower in `term`, clearing leader and election state.
    /// Emits `Action::StepDown` when leaving the leader role.
    fn set_term(&mut self, term: Term, out: &mut Vec<Action>) {
        if self.role == Role::Leader {
            out.push(Action::StepDown { term });
            self.fail_pending_reads(out);
        }
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = None;
        self.progress.clear();
        self.votes_received.clear();
        self.prevotes_received.clear();
        self.inflight.clear();
        self.attached.clear();
        self.pending_config = None;
        self.last_read_round = 0;
    }

    /// Rebuild `config_state` from the log and the commit index: the
    /// newest config entry in the log is the latest configuration,
    /// the newest at or below `commit_index` is the committed one,
    /// and the snapshot base configuration rules when the log holds
    /// no config entry above the base (invariant M-05's single source
    /// of truth).
    fn recompute_config(&mut self) {
        let mut latest: &ClusterConfig = &self.base_config;
        let mut committed: &ClusterConfig = &self.base_config;
        let mut last_config_index = self.log.base_index();
        let base = self.log.base_index().0;
        for (offset, entry) in self.log.entries().iter().enumerate() {
            if let EntryKind::Config { config } = &entry.kind {
                let index = LogIndex(base + 1 + offset as u64);
                latest = config;
                last_config_index = index;
                if index <= self.commit_index {
                    committed = config;
                }
            }
        }
        if self.config_state.latest == *latest
            && self.config_state.committed == *committed
            && self.config_state.last_config_index == last_config_index
        {
            return;
        }
        self.config_state = ConfigState {
            latest: latest.clone(),
            committed: committed.clone(),
            last_config_index,
        };
    }

    /// Step down after this leader's own removal from the committed
    /// configuration (invariant M-03): the removal has replicated and
    /// committed, so leadership ends here, never earlier.
    fn step_down_after_removal(&mut self, out: &mut Vec<Action>) {
        out.push(Action::StepDown {
            term: self.current_term,
        });
        self.fail_pending_reads(out);
        self.role = Role::Follower;
        self.leader_id = None;
        self.progress.clear();
        self.inflight.clear();
        self.attached.clear();
    }

    /// Bookkeeping for a config entry that has just applied: answer
    /// the pending proposal, sync leader progress to the new
    /// membership, and step down if this leader was removed.
    fn on_config_applied(
        &mut self,
        index: LogIndex,
        config: &ClusterConfig,
        out: &mut Vec<Action>,
    ) {
        self.recompute_config();
        if let Some((pending_index, change)) = self.pending_config.take() {
            if pending_index == index {
                self.defer_or_emit(
                    out,
                    Action::ConfigResponse {
                        change,
                        result: ConfigResult::Committed,
                    },
                    0,
                    index,
                );
            } else {
                self.pending_config = Some((pending_index, change));
            }
        }
        if self.role != Role::Leader {
            return;
        }
        let next = self.log.last_index().next();
        for member in config.members() {
            if member != self.id && !self.progress.contains_key(&member) {
                self.progress.insert(
                    member,
                    Progress {
                        next_index: next,
                        match_index: LogIndex::ZERO,
                        snapshot: None,
                    },
                );
            }
        }
        self.progress.retain(|peer, _| config.contains(*peer));
        if !config.is_voter(self.id) {
            self.step_down_after_removal(out);
        }
    }

    /// Reject every read in the pending `ReadIndex` batch: this node is
    /// no longer the leader, so none of them can be confirmed.
    fn fail_pending_reads(&mut self, out: &mut Vec<Action>) {
        if let Some(batch) = self.pending_reads.take() {
            for (request_id, _key) in batch.requests {
                out.push(Action::ReadRejected {
                    request_id,
                    leader: None,
                });
            }
        }
    }

    /// Release the pending `ReadIndex` batch when a quorum has echoed
    /// its round token and the commit gate (the first current-term
    /// index) is committed. The read index is the commit index at
    /// release, which never moves backwards across batches (R-03).
    fn try_release_reads(&mut self, out: &mut Vec<Action>) {
        if self.hard_gen_acked < self.hard_gen_sent {
            return;
        }
        let Some(batch) = &self.pending_reads else {
            return;
        };
        if batch.acks.len() < self.config_state.latest.quorum()
            || self.commit_index < batch.gate_index
        {
            return;
        }
        let read_index = self.commit_index;
        let batch = self.pending_reads.take().expect("batch checked above");
        for (request_id, key) in batch.requests {
            out.push(Action::ReadReady {
                request_id,
                key,
                read_index,
            });
        }
    }

    /// Whether the candidate log tip `(last_term, last_index)` is at
    /// least as up to date as this node's log.
    fn log_up_to_date(&self, last_term: Term, last_index: LogIndex) -> bool {
        let my_term = self.log.last_term();
        last_term > my_term || (last_term == my_term && last_index >= self.log.last_index())
    }

    fn start_prevote(&mut self, out: &mut Vec<Action>) {
        self.role = Role::PreCandidate;
        self.leader_id = None;
        self.prevote_term = self.current_term.next();
        self.prevotes_received = BTreeSet::from([self.id]);
        self.reset_election_deadline();
        if self.prevotes_received.len() >= self.config_state.latest.quorum() {
            self.start_election(out);
            return;
        }
        let msg = Message::RequestPreVote {
            term: self.prevote_term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for voter in self.config_state.latest.voters.iter().copied() {
            if voter != self.id {
                out.push(Action::Send {
                    to: voter,
                    msg: msg.clone(),
                });
            }
        }
    }

    fn start_election(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Candidate;
        self.current_term = self.prevote_term;
        self.voted_for = Some(self.id);
        self.votes_received = BTreeSet::from([self.id]);
        self.reset_election_deadline();
        let generation = self.emit_persist_hard(out);
        if self.votes_received.len() >= self.config_state.latest.quorum() {
            self.become_leader(out);
            return;
        }
        let msg = Message::RequestVote {
            term: self.current_term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        let voters: Vec<NodeId> = self.config_state.latest.voters.iter().copied().collect();
        for voter in voters {
            if voter != self.id {
                self.defer_or_emit(
                    out,
                    Action::Send {
                        to: voter,
                        msg: msg.clone(),
                    },
                    generation,
                    LogIndex::ZERO,
                );
            }
        }
    }

    fn become_leader(&mut self, out: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.last_leader_contact = Some(self.now);
        self.heartbeat_deadline = self.now.saturating_add(self.config.heartbeat_interval);
        let next = self.log.last_index().next();
        self.progress = self
            .config_state
            .latest
            .members()
            .filter(|&m| m != self.id)
            .map(|m| {
                (
                    m,
                    Progress {
                        next_index: next,
                        match_index: LogIndex::ZERO,
                        snapshot: None,
                    },
                )
            })
            .collect();
        // The election NoOp: the first entry this leader may count
        // toward commitment, and the entry ReadIndex waits for.
        let entries = vec![LogEntry {
            term: self.current_term,
            kind: EntryKind::NoOp,
        }];
        let first = self.log.append(&entries);
        self.emit_persist_entries(first, entries, out);
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append(peer, out);
        }
    }

    // -- ticking and proposals ----------------------------------------

    fn on_tick(&mut self, now: Tick, out: &mut Vec<Action>) {
        self.now = self.now.max(now);
        match self.role {
            Role::Leader => {
                if self.now >= self.heartbeat_deadline {
                    self.heartbeat_deadline =
                        self.now.saturating_add(self.config.heartbeat_interval);
                    let peers: Vec<NodeId> = self.progress.keys().copied().collect();
                    for peer in peers {
                        self.send_append(peer, out);
                    }
                }
            }
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                if self.now >= self.election_deadline && self.config_state.latest.is_voter(self.id)
                {
                    self.start_prevote(out);
                }
            }
        }
    }

    /// Handle a `ReadIndex` request (docs/ARCHITECTURE.md section 8):
    /// batch the read into the pending round, or open a new round with
    /// a fresh round token and a heartbeat broadcast. Only the leader
    /// serves reads.
    fn on_read_index(&mut self, request_id: u64, key: Vec<u8>, out: &mut Vec<Action>) {
        if self.role != Role::Leader {
            out.push(Action::ReadRejected {
                request_id,
                leader: self.leader_id,
            });
            return;
        }
        if let Some(batch) = &mut self.pending_reads {
            batch.requests.push((request_id, key));
            return;
        }
        let Some(gate_index) = self.log.first_index_of_term(self.current_term) else {
            // A leader always holds an entry of its own term (its
            // election NoOp); reject defensively if that ever breaks.
            out.push(Action::ReadRejected {
                request_id,
                leader: self.leader_id,
            });
            return;
        };
        self.read_round_counter += 1;
        let mut acks = BTreeSet::new();
        acks.insert(self.id);
        self.pending_reads = Some(ReadBatch {
            round: self.read_round_counter,
            gate_index,
            acks,
            requests: vec![(request_id, key)],
        });
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append(peer, out);
        }
        self.try_release_reads(out);
    }

    fn on_propose(
        &mut self,
        client_id: ClientId,
        seq: u64,
        payload: Vec<u8>,
        out: &mut Vec<Action>,
    ) {
        if self.role != Role::Leader {
            out.push(Action::ClientResponse {
                client_id,
                seq,
                result: ClientResult::NotLeader {
                    leader: self.leader_id,
                },
            });
            return;
        }
        // In-flight deduplication (C-02): a retry of a proposal that
        // is still uncommitted attaches to the in-flight entry. It
        // appends nothing and is answered when the entry applies.
        if let Some(&index) = self.inflight.get(&(client_id, seq))
            && index > self.commit_index
        {
            *self.attached.entry(index).or_insert(0) += 1;
            return;
        }
        let uncommitted =
            usize::try_from(self.log.last_index().0.saturating_sub(self.commit_index.0))
                .unwrap_or(usize::MAX);
        if uncommitted >= self.config.max_uncommitted_entries {
            out.push(Action::ClientResponse {
                client_id,
                seq,
                result: ClientResult::Rejected {
                    error: ProtocolError::TooManyUncommitted {
                        uncommitted,
                        limit: self.config.max_uncommitted_entries,
                    },
                },
            });
            return;
        }
        let entry = LogEntry {
            term: self.current_term,
            kind: EntryKind::Command {
                client_id,
                seq,
                payload,
            },
        };
        let entries = vec![entry];
        let first = self.log.append(&entries);
        self.inflight.insert((client_id, seq), first);
        self.emit_persist_entries(first, entries, out);
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append(peer, out);
        }
    }

    // -- replication (leader side) -------------------------------------

    /// Build the `AppendEntries` message this leader would send `peer`
    /// now, from its current progress.
    fn build_append(&self, peer: NodeId, read_round: u64) -> Message {
        let next = self
            .progress
            .get(&peer)
            .map_or_else(|| self.log.last_index().next(), |p| p.next_index);
        let prev = LogIndex(next.0.saturating_sub(1));
        let prev_term = self.log.term_at(prev).unwrap_or(Term::ZERO);
        let entries = self.log.entries_from(
            next,
            self.config.max_entries_per_append,
            self.config.max_append_bytes,
        );
        Message::AppendEntries {
            term: self.current_term,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
            read_round,
        }
    }

    /// Emit an `AppendEntries` to `peer`, holding it back if it carries
    /// entries that are not yet durable locally (T-02).
    fn send_append(&mut self, peer: NodeId, out: &mut Vec<Action>) {
        // A peer whose next index has fallen to or below the snapshot
        // base is caught up by InstallSnapshot, not AppendEntries
        // (invariant N-01).
        let next = self
            .progress
            .get(&peer)
            .map_or_else(|| self.log.last_index().next(), |prog| prog.next_index);
        if self.log.base_index() > LogIndex::ZERO && next <= self.log.base_index() {
            self.send_snapshot_chunk(peer, out);
            return;
        }
        let round = self.pending_reads.as_ref().map_or(0, |batch| batch.round);
        let msg = self.build_append(peer, round);
        let need = if let Message::AppendEntries {
            prev_log_index,
            entries,
            ..
        } = &msg
            && !entries.is_empty()
        {
            LogIndex(prev_log_index.0 + entries.len() as u64)
        } else {
            LogIndex::ZERO
        };
        self.defer_or_emit(
            out,
            Action::Send { to: peer, msg },
            self.hard_gen_sent,
            need,
        );
    }

    /// The metadata of the snapshot this node's log base represents.
    fn base_meta(&self) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: self.log.base_index(),
            last_included_term: self.log.base_term(),
            config: self.base_config.clone(),
        }
    }

    /// Send the next `InstallSnapshot` chunk to `peer`, or ask the
    /// driver to load the base snapshot when its bytes are not in
    /// memory (docs/ARCHITECTURE.md section 9, snapshot relay).
    ///
    /// A relay that was sent in full but never answered (the
    /// peer's `InstallSnapshotResponse` clears the relay state)
    /// restarts from offset 0 on the next send: the peer may
    /// have missed every chunk, for example across a partition,
    /// and resending only the empty tail chunk would wedge it
    /// until the next compaction (invariant N-06).
    fn send_snapshot_chunk(&mut self, peer: NodeId, out: &mut Vec<Action>) {
        let meta = self.base_meta();
        let Some(loaded) = &self.loaded_snapshot else {
            out.push(Action::LoadSnapshot { meta });
            return;
        };
        if loaded.meta != meta {
            out.push(Action::LoadSnapshot { meta });
            return;
        }
        let loaded = loaded.clone();
        let chunk = self.config.snapshot_chunk_bytes;
        let Some(prog) = self.progress.get_mut(&peer) else {
            return;
        };
        let offset = match prog.snapshot {
            Some((index, term, next_offset))
                if index == meta.last_included_index
                    && term == meta.last_included_term
                    && next_offset > 0
                    && next_offset < loaded.data.len() as u64 =>
            {
                next_offset
            }
            _ => 0,
        };
        let start = usize::try_from(offset)
            .unwrap_or(usize::MAX)
            .min(loaded.data.len());
        let end = start.saturating_add(chunk).min(loaded.data.len());
        let done = end == loaded.data.len();
        prog.snapshot = Some((
            meta.last_included_index,
            meta.last_included_term,
            end as u64,
        ));
        out.push(Action::Send {
            to: peer,
            msg: Message::InstallSnapshot {
                term: self.current_term,
                last_included_index: meta.last_included_index,
                last_included_term: meta.last_included_term,
                config: meta.config,
                offset,
                data: loaded.data[start..end].to_vec(),
                done,
            },
        });
    }

    /// Handle an `InstallSnapshot` chunk (docs/ARCHITECTURE.md
    /// sections 7.4 and 9): assemble chunks for exactly one snapshot
    /// identity at exactly the buffered offsets (N-02), then hand the
    /// assembled payload to the driver to persist; the install itself
    /// happens when the persist is acknowledged (T-02's shape).
    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per InstallSnapshot field keeps the handler aligned with the message layout"
    )]
    fn on_install_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        meta: SnapshotMeta,
        offset: u64,
        data: &[u8],
        done: bool,
        out: &mut Vec<Action>,
    ) {
        if term < self.current_term {
            out.push(Action::Send {
                to: from,
                msg: Message::InstallSnapshotResponse {
                    term: self.current_term,
                    match_index: self.log.base_index(),
                },
            });
            return;
        }
        let need_hard = if term > self.current_term {
            self.set_term(term, out);
            self.emit_persist_hard(out)
        } else if self.role == Role::Leader {
            out.push(Action::StepDown { term });
            self.fail_pending_reads(out);
            self.role = Role::Follower;
            self.progress.clear();
            self.inflight.clear();
            self.attached.clear();
            self.pending_config = None;
            0
        } else {
            self.role = Role::Follower;
            0
        };
        self.leader_id = Some(from);
        self.last_leader_contact = Some(self.now);
        self.reset_election_deadline();

        // A snapshot at or below the base is already installed here.
        if meta.last_included_index <= self.log.base_index() {
            let response = Action::Send {
                to: from,
                msg: Message::InstallSnapshotResponse {
                    term: self.current_term,
                    match_index: self.log.base_index(),
                },
            };
            self.defer_or_emit(out, response, need_hard, LogIndex::ZERO);
            return;
        }
        // One buffer for one snapshot identity: a chunk for a
        // different snapshot discards the old buffer exactly once.
        let fresh = self
            .incoming_snapshot
            .as_ref()
            .is_none_or(|incoming| incoming.meta != meta);
        if fresh {
            self.incoming_snapshot = Some(IncomingSnapshot::new(meta.clone()));
        }
        let expected = self
            .incoming_snapshot
            .as_ref()
            .map_or(0, IncomingSnapshot::expected_offset);
        if offset != expected {
            // Rejected: the buffer keeps exactly what it held, and
            // the response reports the pre-install base, so the
            // leader learns nothing was installed (invariant N-02;
            // the rejection carries SnapshotError::ChunkOffset's
            // values on the driver side).
            let response = Action::Send {
                to: from,
                msg: Message::InstallSnapshotResponse {
                    term: self.current_term,
                    match_index: self.log.base_index(),
                },
            };
            self.defer_or_emit(out, response, need_hard, LogIndex::ZERO);
            return;
        }
        if let Some(incoming) = &mut self.incoming_snapshot {
            incoming.data.extend_from_slice(data);
        }
        if done && let Some(incoming) = self.incoming_snapshot.take() {
            self.pending_install = Some(meta.clone());
            out.push(Action::PersistSnapshot {
                meta,
                data: incoming.data,
            });
        }
        let response = Action::Send {
            to: from,
            msg: Message::InstallSnapshotResponse {
                term: self.current_term,
                match_index: self.log.base_index(),
            },
        };
        self.defer_or_emit(out, response, need_hard, LogIndex::ZERO);
    }

    /// Handle an `InstallSnapshotResponse`: a follower whose reported
    /// match reaches the snapshot base has installed it, so its relay
    /// state clears and normal replication resumes.
    fn on_snapshot_response(
        &mut self,
        from: NodeId,
        term: Term,
        match_index: LogIndex,
        out: &mut Vec<Action>,
    ) {
        if term > self.current_term {
            self.set_term(term, out);
            let _ = self.emit_persist_hard(out);
            return;
        }
        if self.role != Role::Leader || term < self.current_term {
            return;
        }
        let mut advance = false;
        if let Some(prog) = self.progress.get_mut(&from) {
            if match_index > prog.match_index {
                prog.match_index = match_index;
                advance = true;
            }
            if prog.next_index < prog.match_index.next() {
                prog.next_index = prog.match_index.next();
            }
            if match_index >= self.log.base_index() {
                prog.snapshot = None;
            }
        }
        if advance {
            self.advance_commit(out);
        }
    }

    /// A snapshot the driver persisted is installed now: a received
    /// snapshot installs unconditionally, and this node's own
    /// snapshot compacts the log only up to the commit index, never
    /// past it (invariant N-05). The loaded bytes stay in memory so a
    /// leader can relay them to lagging peers.
    fn on_snapshot_persisted(&mut self, meta: &SnapshotMeta) {
        if self.pending_install.as_ref() == Some(meta) {
            self.pending_install = None;
            self.install_snapshot(meta);
            return;
        }
        if let Some(loaded) = &self.loaded_snapshot
            && loaded.meta == *meta
            && meta.last_included_index <= self.commit_index
        {
            self.install_snapshot(meta);
        }
    }

    /// Install `meta`'s snapshot boundary into the log: retain the
    /// suffix when the entry at the boundary matches the snapshot's
    /// term, discard the whole log otherwise (N-04), and jump the
    /// commit and apply cursors to the boundary (section 4.2).
    fn install_snapshot(&mut self, meta: &SnapshotMeta) {
        let index = meta.last_included_index;
        if index < self.log.base_index() {
            return;
        }
        if self.log.term_at(index) == Some(meta.last_included_term) {
            if self.entries_durable_up_to < index {
                self.entries_durable_up_to = index;
            }
        } else {
            self.log.clear_entries();
            self.entries_durable_up_to = index;
            self.stale_entry_acks = self.unacked_entry_persists;
        }
        self.log.advance_base(index, meta.last_included_term);
        if self.commit_index < index {
            self.commit_index = index;
        }
        if self.last_applied < index {
            self.last_applied = index;
        }
        self.base_config = meta.config.clone();
        self.inflight.retain(|_, entry_index| *entry_index > index);
        self.attached.retain(|entry_index, _| *entry_index > index);
        if let Some(incoming) = &self.incoming_snapshot
            && incoming.meta.last_included_index <= index
        {
            self.incoming_snapshot = None;
        }
        self.recompute_config();
    }

    /// Advance the leader's commit index by counting replicas, but only
    /// for an entry from the leader's current term (the Figure 8 rule,
    /// invariant S-06), then propagate applies and the new commit.
    fn advance_commit(&mut self, out: &mut Vec<Action>) {
        if self.hard_gen_acked < self.hard_gen_sent {
            return;
        }
        let mut held: Vec<u64> = self
            .config_state
            .latest
            .voters
            .iter()
            .map(|&v| {
                if v == self.id {
                    self.entries_durable_up_to.0
                } else {
                    self.progress.get(&v).map_or(0, |p| p.match_index.0)
                }
            })
            .collect();
        held.sort_unstable();
        let quorum = self.config_state.latest.quorum();
        let candidate = LogIndex(held[held.len() - quorum]);
        if candidate > self.commit_index && self.log.term_at(candidate) == Some(self.current_term) {
            self.commit_index = candidate;
            self.emit_applies(out);
            self.try_release_reads(out);
            let peers: Vec<NodeId> = self.progress.keys().copied().collect();
            for peer in peers {
                self.send_append(peer, out);
            }
        }
    }

    /// Emit applies (and the client responses attached to command
    /// entries) for everything newly committed, holding any whose
    /// entries are not yet durable (T-02).
    fn emit_applies(&mut self, out: &mut Vec<Action>) {
        while self.last_applied < self.commit_index {
            self.last_applied = self.last_applied.next();
            let index = self.last_applied;
            let Some(entry) = self.log.entry_at(index).cloned() else {
                break;
            };
            let applied_config = match &entry.kind {
                EntryKind::Config { config } => Some(config.clone()),
                _ => None,
            };
            let applied_command = match &entry.kind {
                EntryKind::Command { client_id, seq, .. } => Some((*client_id, *seq)),
                _ => None,
            };
            self.defer_or_emit(out, Action::Apply { index, entry }, 0, index);
            if let Some(config) = applied_config {
                self.on_config_applied(index, &config, out);
            }
            if let Some((client_id, seq)) = applied_command {
                self.defer_or_emit(
                    out,
                    Action::ClientResponse {
                        client_id,
                        seq,
                        result: ClientResult::Committed,
                    },
                    0,
                    index,
                );
                // In-flight dedup bookkeeping (C-02): the entry has
                // applied, so the (client, seq) leaves the in-flight
                // table, and every attached retry gets the same
                // response as the original.
                self.inflight.remove(&(client_id, seq));
                if let Some(extra) = self.attached.remove(&index) {
                    for _ in 0..extra {
                        self.defer_or_emit(
                            out,
                            Action::ClientResponse {
                                client_id,
                                seq,
                                result: ClientResult::Committed,
                            },
                            0,
                            index,
                        );
                    }
                }
            }
        }
    }

    // -- message dispatch ----------------------------------------------

    fn on_message(&mut self, from: NodeId, msg: Message, out: &mut Vec<Action>) {
        match msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(from, term, last_log_index, last_log_term, out),
            Message::RequestVoteResponse { term, vote_granted } => {
                self.on_vote_response(from, term, vote_granted, out);
            }
            Message::RequestPreVote {
                term,
                last_log_index,
                last_log_term,
            } => self.on_request_prevote(from, term, last_log_index, last_log_term, out),
            Message::RequestPreVoteResponse { term, vote_granted } => {
                self.on_prevote_response(from, term, vote_granted, out);
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            } => self.on_append_entries(
                from,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
                out,
            ),
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                read_round,
            } => self.on_append_response(
                from,
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                read_round,
                out,
            ),
            Message::InstallSnapshot {
                term,
                last_included_index,
                last_included_term,
                config,
                offset,
                data,
                done,
            } => self.on_install_snapshot(
                from,
                term,
                SnapshotMeta {
                    last_included_index,
                    last_included_term,
                    config,
                },
                offset,
                &data,
                done,
                out,
            ),
            Message::InstallSnapshotResponse { term, match_index } => {
                self.on_snapshot_response(from, term, match_index, out);
            }
        }
    }

    /// Handle a membership change proposal (docs/ARCHITECTURE.md
    /// section 9). The leader gates the change on its election `NoOp`
    /// (M-02), on any earlier uncommitted change (M-01), on the
    /// set-level rules, and on learner catch-up for promotions
    /// (M-04), then appends one config entry. The proposal is
    /// answered when that entry applies (see `on_config_applied`), or
    /// immediately when any gate rejects it.
    fn on_propose_config(&mut self, change: ConfigChange, out: &mut Vec<Action>) {
        let reject = |out: &mut Vec<Action>, error: ProtocolError| {
            out.push(Action::ConfigResponse {
                change,
                result: ConfigResult::Rejected { error },
            });
        };
        if self.role != Role::Leader {
            out.push(Action::ConfigResponse {
                change,
                result: ConfigResult::NotLeader {
                    leader: self.leader_id,
                },
            });
            return;
        }
        if let Some(noop_index) = self.log.first_index_of_term(self.current_term)
            && self.commit_index < noop_index
        {
            reject(
                out,
                ProtocolError::LeaderNoopUncommitted {
                    noop_index,
                    commit_index: self.commit_index,
                },
            );
            return;
        }
        if self.config_state.last_config_index > self.commit_index {
            reject(
                out,
                ProtocolError::ConfigChangePending {
                    last_config_index: self.config_state.last_config_index,
                    commit_index: self.commit_index,
                },
            );
            return;
        }
        let new_config = match self.config_state.latest.apply_change(change) {
            Ok(config) => config,
            Err(error) => {
                reject(out, error);
                return;
            }
        };
        if let ConfigChange::PromoteVoter { node } = change {
            let matched = self
                .progress
                .get(&node)
                .map_or(LogIndex::ZERO, |prog| prog.match_index);
            if matched < self.commit_index {
                reject(
                    out,
                    ProtocolError::LearnerNotCaughtUp {
                        node,
                        match_index: matched,
                        required: self.commit_index,
                    },
                );
                return;
            }
        }
        let entries = vec![LogEntry {
            term: self.current_term,
            kind: EntryKind::Config { config: new_config },
        }];
        let first = self.log.append(&entries);
        self.pending_config = Some((first, change));
        self.emit_persist_entries(first, entries, out);
        self.recompute_config();
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append(peer, out);
        }
    }

    fn on_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
        out: &mut Vec<Action>,
    ) {
        if term < self.current_term {
            out.push(Action::Send {
                to: from,
                msg: Message::RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                },
            });
            return;
        }
        let mut hard_changed = if term > self.current_term {
            self.set_term(term, out);
            true
        } else {
            false
        };
        // Learners never vote (M-04): they answer, but never grant,
        // and they never record a vote for themselves. If the higher
        // term changed persistent hard state, hold the response until
        // HardState persistence is acknowledged (T-02).
        if !self.config_state.latest.is_voter(self.id) {
            let response = Action::Send {
                to: from,
                msg: Message::RequestVoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                },
            };
            if hard_changed {
                let generation = self.emit_persist_hard(out);
                self.defer_or_emit(out, response, generation, LogIndex::ZERO);
            } else {
                out.push(response);
            }
            return;
        }
        let grant = (self.voted_for.is_none() || self.voted_for == Some(from))
            && self.log_up_to_date(last_log_term, last_log_index);
        if grant {
            if self.voted_for != Some(from) {
                self.voted_for = Some(from);
                hard_changed = true;
            }
            self.reset_election_deadline();
        }
        let response = Action::Send {
            to: from,
            msg: Message::RequestVoteResponse {
                term: self.current_term,
                vote_granted: grant,
            },
        };
        if hard_changed {
            let generation = self.emit_persist_hard(out);
            self.defer_or_emit(out, response, generation, LogIndex::ZERO);
        } else {
            out.push(response);
        }
    }

    fn on_vote_response(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        out: &mut Vec<Action>,
    ) {
        if term > self.current_term {
            self.set_term(term, out);
            let _ = self.emit_persist_hard(out);
            return;
        }
        if self.role == Role::Candidate
            && term == self.current_term
            && vote_granted
            && self.config_state.latest.is_voter(from)
        {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.config_state.latest.quorum() {
                self.become_leader(out);
            }
        }
    }

    fn on_request_prevote(
        &self,
        from: NodeId,
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
        out: &mut Vec<Action>,
    ) {
        // Pre-Vote purity (T-03): this handler reads state and replies.
        // It never writes hard state and never resets the election timer.
        let leader_recent = self.role == Role::Leader
            || (self.leader_id.is_some()
                && self.last_leader_contact.is_some_and(|t| {
                    self.now.saturating_sub(t) < self.config.min_election_timeout
                }));
        let granted = term > self.current_term
            && self.log_up_to_date(last_log_term, last_log_index)
            && !leader_recent;
        out.push(Action::Send {
            to: from,
            msg: Message::RequestPreVoteResponse {
                term: self.current_term,
                vote_granted: granted,
            },
        });
    }

    fn on_prevote_response(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        out: &mut Vec<Action>,
    ) {
        if term > self.current_term {
            self.set_term(term, out);
            let _ = self.emit_persist_hard(out);
            return;
        }
        if self.role == Role::PreCandidate
            && vote_granted
            && self.config_state.latest.is_voter(from)
        {
            self.prevotes_received.insert(from);
            if self.prevotes_received.len() >= self.config_state.latest.quorum() {
                self.start_election(out);
            }
        }
    }

    /// Answer an `AppendEntries` whose previous index sits below
    /// this node's snapshot base: the entries it could carry are
    /// already installed here, so only the commit index can advance,
    /// and the response reports the base as the matched index.
    fn on_append_below_base(
        &mut self,
        from: NodeId,
        leader_commit: LogIndex,
        read_round: u64,
        hard_changed: bool,
        out: &mut Vec<Action>,
    ) {
        let new_commit = leader_commit.min(self.log.base_index());
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.emit_applies(out);
        }
        if read_round > self.last_read_round {
            self.last_read_round = read_round;
        }
        let response = Action::Send {
            to: from,
            msg: Message::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: self.log.base_index(),
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: self.last_read_round,
            },
        };
        if hard_changed {
            let generation = self.emit_persist_hard(out);
            self.defer_or_emit(out, response, generation, LogIndex::ZERO);
        } else {
            out.push(response);
        }
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per AppendEntries field keeps the handler aligned with the message layout"
    )]
    fn on_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        read_round: u64,
        out: &mut Vec<Action>,
    ) {
        if term < self.current_term {
            out.push(Action::Send {
                to: from,
                msg: Message::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: LogIndex::ZERO,
                    conflict_index: LogIndex::ZERO,
                    conflict_term: Term::ZERO,
                    read_round: 0,
                },
            });
            return;
        }
        let hard_changed = if term > self.current_term {
            self.set_term(term, out);
            true
        } else if self.role == Role::Leader {
            out.push(Action::StepDown { term });
            self.fail_pending_reads(out);
            self.role = Role::Follower;
            self.progress.clear();
            self.inflight.clear();
            self.attached.clear();
            self.pending_config = None;
            false
        } else {
            self.role = Role::Follower;
            false
        };
        self.leader_id = Some(from);
        self.last_leader_contact = Some(self.now);
        self.reset_election_deadline();

        // The leader is replicating from below our snapshot base:
        // everything up to the base is already installed here, so the
        // batch verifies trivially and only the commit can advance.
        if prev_log_index < self.log.base_index() {
            self.on_append_below_base(from, leader_commit, read_round, hard_changed, out);
            return;
        }

        // Consistency check with the fast-backup conflict hint (S-08).
        let rejection = if prev_log_index > self.log.last_index() {
            Some((self.log.last_index().next(), Term::ZERO))
        } else if self.log.term_at(prev_log_index) != Some(prev_log_term) {
            let conflict_term = self.log.term_at(prev_log_index).unwrap_or(Term::ZERO);
            let conflict_index = self
                .log
                .first_index_of_term(conflict_term)
                .unwrap_or(prev_log_index);
            Some((conflict_index, conflict_term))
        } else {
            None
        };
        if let Some((conflict_index, conflict_term)) = rejection {
            let response = Action::Send {
                to: from,
                msg: Message::AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    match_index: LogIndex::ZERO,
                    conflict_index,
                    conflict_term,
                    read_round: 0,
                },
            };
            if hard_changed {
                let generation = self.emit_persist_hard(out);
                self.defer_or_emit(out, response, generation, LogIndex::ZERO);
            } else {
                out.push(response);
            }
            return;
        }

        let batch_len = entries.len() as u64;
        let need_entries = self.merge_and_commit(prev_log_index, entries, leader_commit, out);

        // ReadIndex bookkeeping (R-04): remember the highest round
        // token this leader has shown us and echo it back, so the
        // leader can count this node toward that round's quorum.
        if read_round > self.last_read_round {
            self.last_read_round = read_round;
        }
        let matched = LogIndex(prev_log_index.0 + batch_len);
        let response = Action::Send {
            to: from,
            msg: Message::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: matched,
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: self.last_read_round,
            },
        };
        if hard_changed {
            let generation = self.emit_persist_hard(out);
            self.defer_or_emit(out, response, generation, need_entries);
        } else {
            self.defer_or_emit(out, response, 0, need_entries);
        }
    }

    /// Merge an accepted `AppendEntries` batch into the log, persist the
    /// changed suffix, and advance the commit index as far as this
    /// batch verifies. Returns the entries persist watermark the
    /// caller's response depends on (zero when nothing changed).
    fn merge_and_commit(
        &mut self,
        prev_log_index: LogIndex,
        mut entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        out: &mut Vec<Action>,
    ) -> LogIndex {
        let batch_len = entries.len() as u64;
        // Keep matching entries, truncate and rewrite at the first
        // conflict, append what is new.
        let mut changed_from: Option<(LogIndex, usize)> = None;
        for (offset, entry) in entries.iter().enumerate() {
            let index = LogIndex(prev_log_index.0 + 1 + offset as u64);
            match self.log.entry_at(index) {
                Some(existing) if existing.term == entry.term => {}
                Some(_) => {
                    self.log.truncate_from(index);
                    self.entries_durable_up_to =
                        self.entries_durable_up_to.min(LogIndex(index.0 - 1));
                    self.stale_entry_acks = self.unacked_entry_persists;
                    self.log.append(&entries[offset..]);
                    changed_from = Some((index, offset));
                    break;
                }
                None => {
                    self.log.append(&entries[offset..]);
                    changed_from = Some((index, offset));
                    break;
                }
            }
        }
        let mut need_entries = LogIndex::ZERO;
        if let Some((from_index, offset)) = changed_from {
            need_entries = self.log.last_index();
            if offset > 0 {
                entries.drain(..offset);
            }
            self.emit_persist_entries(from_index, entries, out);
        }
        // Commit advancement bounded strictly by what this batch verified.
        let verified_up_to = LogIndex(prev_log_index.0 + batch_len);
        let new_commit = leader_commit.min(verified_up_to);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.emit_applies(out);
        }
        self.recompute_config();
        need_entries
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one argument per AppendEntriesResponse field keeps the handler aligned with the message layout"
    )]
    fn on_append_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        conflict_index: LogIndex,
        conflict_term: Term,
        read_round: u64,
        out: &mut Vec<Action>,
    ) {
        if term > self.current_term {
            self.set_term(term, out);
            let _ = self.emit_persist_hard(out);
            return;
        }
        if self.role != Role::Leader || term < self.current_term {
            return;
        }
        if success {
            let mut advance = false;
            if let Some(prog) = self.progress.get_mut(&from) {
                if match_index > prog.match_index {
                    prog.match_index = match_index;
                    advance = true;
                }
                if prog.next_index < prog.match_index.next() {
                    prog.next_index = prog.match_index.next();
                }
                let cap = self.log.last_index().next();
                if prog.next_index > cap {
                    prog.next_index = cap;
                }
            }
            if advance {
                self.advance_commit(out);
            }
            // A successful response echoes the follower's latest round
            // token; count it toward the pending ReadIndex round and
            // release the batch if the commit gate also holds.
            if read_round != 0
                && let Some(batch) = &mut self.pending_reads
                && batch.round == read_round
                && self.config_state.latest.is_voter(from)
            {
                batch.acks.insert(from);
            }
            self.try_release_reads(out);
            return;
        }
        // Fast log backup from the conflict hint (S-08), never moving
        // next_index below match_index + 1 (S-07) and never above the
        // tail + 1.
        if conflict_index == LogIndex::ZERO && conflict_term == Term::ZERO {
            return;
        }
        let candidate = if conflict_term > Term::ZERO {
            self.log
                .last_index_of_term(conflict_term)
                .map_or(conflict_index, LogIndex::next)
        } else {
            conflict_index
        };
        let mut retry = false;
        if let Some(prog) = self.progress.get_mut(&from) {
            let floor = prog.match_index.next();
            let cap = self.log.last_index().next();
            let new_next = candidate.max(floor).min(cap);
            if new_next < prog.next_index {
                prog.next_index = new_next;
                retry = true;
            }
        }
        if retry {
            self.send_append(from, out);
        }
    }
}

/// `SplitMix64` finalizer: a small deterministic mix used for election
/// timer offsets. Not a general-purpose RNG; the simulator owns the real
/// seeded stream.
pub(crate) const fn splitmix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}
