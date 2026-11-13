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

use crate::action::{Action, ClientResult};
use crate::config::Config;
use crate::error::ConfigError;
use crate::event::{Event, PersistKind};
use crate::ids::{ClientId, LogIndex, NodeId, Term, Tick};
use crate::log::{EntryKind, Log, LogEntry};
use crate::membership::ClusterConfig;
use crate::message::Message;
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
    /// The term the action was created in. Held `Send` actions are
    /// dropped if the node's term has moved on when they are released;
    /// applies and client responses are term independent and always
    /// release once their persist needs are met.
    pub term: Term,
    /// The action to release.
    pub action: Action,
}

/// One in-flight ReadIndex round on a leader: the requests batched
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
    pub(crate) read_round_counter: u64,
    pub(crate) pending_reads: Option<ReadBatch>,
    pub(crate) last_read_round: u64,
    pub(crate) inflight: BTreeMap<(ClientId, u64), LogIndex>,
    pub(crate) attached: BTreeMap<LogIndex, u64>,
    pub(crate) hard_gen_sent: u64,
    pub(crate) hard_gen_acked: u64,
    pub(crate) entries_durable_up_to: LogIndex,
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
            read_round_counter: 0,
            pending_reads: None,
            last_read_round: 0,
            inflight: BTreeMap::new(),
            attached: BTreeMap::new(),
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
            Event::Message { from, msg } => self.on_message(from, msg, &mut out),
            Event::Persisted { kind, up_to_index } => {
                self.on_persisted(kind, up_to_index, &mut out);
            }
            Event::Restored {
                hard_state,
                entries,
            } => self.on_restored(hard_state.current_term, hard_state.voted_for, &entries),
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

    /// Release every held action whose persist needs are now met.
    /// Held sends from an older term are dropped instead: their content
    /// is stale by construction.
    fn flush_pending(&mut self, out: &mut Vec<Action>) {
        let pending = std::mem::take(&mut self.pending);
        for held in pending {
            let stale_send =
                matches!(held.action, Action::Send { .. }) && held.term != self.current_term;
            if stale_send {
                continue;
            }
            if held.need_hard_gen <= self.hard_gen_acked
                && held.need_entries_up_to <= self.entries_durable_up_to
            {
                out.push(held.action);
            } else {
                self.pending.push(held);
            }
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
                if up_to_index > self.entries_durable_up_to {
                    self.entries_durable_up_to = up_to_index;
                }
            }
        }
        self.flush_pending(out);
    }

    fn on_restored(&mut self, term: Term, voted_for: Option<NodeId>, entries: &[LogEntry]) {
        self.current_term = term;
        self.voted_for = voted_for;
        self.log = Log::new();
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
        self.entries_durable_up_to = self.log.last_index();
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
        }
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = None;
        self.progress.clear();
        self.votes_received.clear();
        self.prevotes_received.clear();
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
        if self.prevotes_received.len() >= self.cluster.quorum() {
            self.start_election(out);
            return;
        }
        let msg = Message::RequestPreVote {
            term: self.prevote_term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        for voter in self.cluster.voters.iter().copied() {
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
        if self.votes_received.len() >= self.cluster.quorum() {
            self.become_leader(out);
            return;
        }
        let msg = Message::RequestVote {
            term: self.current_term,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };
        let voters: Vec<NodeId> = self.cluster.voters.iter().copied().collect();
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
            .cluster
            .members()
            .filter(|&m| m != self.id)
            .map(|m| {
                (
                    m,
                    Progress {
                        next_index: next,
                        match_index: LogIndex::ZERO,
                    },
                )
            })
            .collect();
        // The election NoOp: the first entry this leader may count
        // toward commitment, and the entry ReadIndex waits for at M2.
        let entries = vec![LogEntry {
            term: self.current_term,
            kind: EntryKind::NoOp,
        }];
        let first = self.log.append(&entries);
        out.push(Action::PersistEntries {
            from_index: first,
            entries,
        });
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
                if self.now >= self.election_deadline && self.cluster.is_voter(self.id) {
                    self.start_prevote(out);
                }
            }
        }
    }

    /// Handle a ReadIndex request. Stage A contract: every read is
    /// rejected; Stage B runs the heartbeat round protocol.
    fn on_read_index(&mut self, request_id: u64, _key: Vec<u8>, out: &mut Vec<Action>) {
        out.push(Action::ReadRejected {
            request_id,
            leader: self.leader_id,
        });
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
        out.push(Action::PersistEntries {
            from_index: first,
            entries,
        });
        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append(peer, out);
        }
    }

    // -- replication (leader side) -------------------------------------

    /// Build the `AppendEntries` message this leader would send `peer`
    /// now, from its current progress.
    fn build_append(&self, peer: NodeId) -> Message {
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
            read_round: 0,
        }
    }

    /// Emit an `AppendEntries` to `peer`, holding it back if it carries
    /// entries that are not yet durable locally (T-02).
    fn send_append(&mut self, peer: NodeId, out: &mut Vec<Action>) {
        let msg = self.build_append(peer);
        let mut need = LogIndex::ZERO;
        if let Message::AppendEntries {
            prev_log_index,
            entries,
            ..
        } = &msg
        {
            if !entries.is_empty() {
                need = LogIndex(prev_log_index.0 + entries.len() as u64);
            }
        }
        self.defer_or_emit(out, Action::Send { to: peer, msg }, 0, need);
    }

    /// Advance the leader's commit index by counting replicas, but only
    /// for an entry from the leader's current term (the Figure 8 rule,
    /// invariant S-06), then propagate applies and the new commit.
    fn advance_commit(&mut self, out: &mut Vec<Action>) {
        let mut held: Vec<u64> = self
            .cluster
            .voters
            .iter()
            .map(|&v| {
                if v == self.id {
                    self.log.last_index().0
                } else {
                    self.progress.get(&v).map_or(0, |p| p.match_index.0)
                }
            })
            .collect();
        held.sort_unstable();
        let quorum = self.cluster.quorum();
        let candidate = LogIndex(held[held.len() - quorum]);
        if candidate > self.commit_index && self.log.term_at(candidate) == Some(self.current_term) {
            self.commit_index = candidate;
            self.emit_applies(out);
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
            self.defer_or_emit(
                out,
                Action::Apply {
                    index,
                    entry: entry.clone(),
                },
                0,
                index,
            );
            if let EntryKind::Command { client_id, seq, .. } = entry.kind {
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
                ..
            } => self.on_append_entries(
                from,
                term,
                prev_log_index,
                prev_log_term,
                &entries,
                leader_commit,
                out,
            ),
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                ..
            } => self.on_append_response(
                from,
                term,
                success,
                match_index,
                conflict_index,
                conflict_term,
                out,
            ),
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
        if self.role == Role::Candidate && term == self.current_term && vote_granted {
            self.votes_received.insert(from);
            if self.votes_received.len() >= self.cluster.quorum() {
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
        if self.role == Role::PreCandidate && vote_granted {
            self.prevotes_received.insert(from);
            if self.prevotes_received.len() >= self.cluster.quorum() {
                self.start_election(out);
            }
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
        entries: &[LogEntry],
        leader_commit: LogIndex,
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
            self.role = Role::Follower;
            self.progress.clear();
            false
        } else {
            self.role = Role::Follower;
            false
        };
        self.leader_id = Some(from);
        self.last_leader_contact = Some(self.now);
        self.reset_election_deadline();

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

        let need_entries = self.merge_and_commit(prev_log_index, entries, leader_commit, out);
        let verified_up_to = if entries.is_empty() {
            self.log.last_index()
        } else {
            LogIndex(prev_log_index.0 + entries.len() as u64)
        };

        let matched = if entries.is_empty() {
            prev_log_index
        } else {
            verified_up_to
        };
        let response = Action::Send {
            to: from,
            msg: Message::AppendEntriesResponse {
                term: self.current_term,
                success: true,
                match_index: matched,
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: 0,
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
        entries: &[LogEntry],
        leader_commit: LogIndex,
        out: &mut Vec<Action>,
    ) -> LogIndex {
        // Keep matching entries, truncate and rewrite at the first
        // conflict, append what is new.
        let mut changed_from: Option<LogIndex> = None;
        for (offset, entry) in entries.iter().enumerate() {
            let index = LogIndex(prev_log_index.0 + 1 + offset as u64);
            match self.log.entry_at(index) {
                Some(existing) if existing.term == entry.term => {}
                Some(_) => {
                    self.log.truncate_from(index);
                    self.log.append(&entries[offset..]);
                    changed_from = Some(index);
                    break;
                }
                None => {
                    self.log.append(&entries[offset..]);
                    changed_from = Some(index);
                    break;
                }
            }
        }
        let mut need_entries = LogIndex::ZERO;
        if let Some(from_index) = changed_from {
            let suffix = self.log.entries_from(from_index, usize::MAX, usize::MAX);
            need_entries = self.log.last_index();
            out.push(Action::PersistEntries {
                from_index,
                entries: suffix,
            });
        }
        // Commit advancement bounded by what this batch verified.
        let verified_up_to = if entries.is_empty() {
            self.log.last_index()
        } else {
            LogIndex(prev_log_index.0 + entries.len() as u64)
        };
        let new_commit = leader_commit.min(verified_up_to);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.emit_applies(out);
        }
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
