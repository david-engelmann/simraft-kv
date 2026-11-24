//! The per-tick safety oracles.
//!
//! Docs/ARCHITECTURE.md section 12.5, invariants S-01 to S-05,
//! plus the structural assertions: commit and applied indexes
//! never decrease, applied never exceeds commit, and a leader's
//! match index for a peer never decreases. An oracle failure
//! stops the run and names the seed context through the harness's
//! report.

use std::collections::BTreeMap;
use std::fmt;

use simraft_core::{LogEntry, LogIndex, NodeId, Role, Term};

/// A read-only view of one node at one tick, assembled by the
/// harness from the core's accessors and the driver's records.
#[derive(Debug, Clone, Copy)]
pub struct NodeView<'a> {
    /// The node's id.
    pub id: NodeId,
    /// Its current role.
    pub role: Role,
    /// Its current term.
    pub current_term: Term,
    /// The log's snapshot base index.
    pub base_index: LogIndex,
    /// The log's snapshot base term.
    pub base_term: Term,
    /// The entries above the base; `entries[i]` sits at index
    /// `base_index + 1 + i`.
    pub entries: &'a [LogEntry],
    /// The node's commit index.
    pub commit_index: LogIndex,
    /// The node's last applied index.
    pub last_applied: LogIndex,
    /// The entries this node has applied, as (index, entry) pairs
    /// in apply order, recorded by the driver.
    pub applied: &'a [(LogIndex, LogEntry)],
    /// For a leader, its per-peer match indexes.
    pub match_indexes: &'a [(NodeId, LogIndex)],
}

impl NodeView<'_> {
    /// The last index in this node's log.
    #[must_use]
    pub const fn last_index(&self) -> LogIndex {
        LogIndex(self.base_index.0 + self.entries.len() as u64)
    }

    /// The term of the entry at `index`, when the node holds it
    /// (the base term answers for the base index itself).
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == self.base_index {
            return Some(self.base_term);
        }
        if index < self.base_index || index > self.last_index() {
            return None;
        }
        let offset = usize::try_from(index.0 - self.base_index.0 - 1).ok()?;
        self.entries.get(offset).map(|e| e.term)
    }

    /// The entry at `index`, when the node holds it above its base.
    #[must_use]
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.base_index || index > self.last_index() {
            return None;
        }
        let offset = usize::try_from(index.0 - self.base_index.0 - 1).ok()?;
        self.entries.get(offset)
    }
}

/// One oracle failure: which oracle fired and the two conflicting
/// states, in text the harness prints with the seed and tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleViolation {
    /// The oracle's name (for example `election_safety`).
    pub oracle: &'static str,
    /// The conflicting values, named.
    pub detail: String,
}

impl fmt::Display for OracleViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "oracle {} violated: {}", self.oracle, self.detail)
    }
}

impl std::error::Error for OracleViolation {}

/// State the oracles carry between ticks (previous logs for the
/// append-only diff, previous indexes for the monotonicity rules).
#[derive(Debug, Default)]
pub struct OracleState {
    leader_logs: BTreeMap<NodeId, (LogIndex, Term, Vec<LogEntry>)>,
    indexes: BTreeMap<NodeId, (LogIndex, LogIndex)>,
    /// Match monotonicity is scoped per leadership: a node's
    /// progress map legitimately resets when it wins a new term,
    /// so the recorded high-water marks key on (leader, term,
    /// peer).
    matches: BTreeMap<(NodeId, Term, NodeId), LogIndex>,
}

impl OracleState {
    /// Fresh oracle state for a new run.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Forget one node's recorded state, when the node crashes:
    /// the oracles judge within a node lifetime, and cross-lifetime
    /// safety is carried by election safety, leader completeness,
    /// and the disk model.
    pub fn forget_node(&mut self, id: NodeId) {
        self.leader_logs.remove(&id);
        self.indexes.remove(&id);
        self.matches.retain(|(leader, _, _), _| *leader != id);
    }

    /// Run every oracle over the current views.
    ///
    /// # Errors
    /// Returns the first `OracleViolation` found.
    pub fn check_all(&mut self, views: &[NodeView<'_>]) -> Result<(), OracleViolation> {
        election_safety(views)?;
        leader_append_only(views, &mut self.leader_logs)?;
        log_matching(views)?;
        leader_completeness(views)?;
        state_machine_safety(views)?;
        structural(views, self)?;
        Ok(())
    }
}

/// S-01: at most one leader per term across all nodes.
fn election_safety(views: &[NodeView<'_>]) -> Result<(), OracleViolation> {
    let mut leaders: BTreeMap<Term, NodeId> = BTreeMap::new();
    for view in views {
        if view.role == Role::Leader {
            if let Some(other) = leaders.insert(view.current_term, view.id) {
                if other != view.id {
                    return Err(OracleViolation {
                        oracle: "election_safety",
                        detail: format!(
                            "nodes {} and {} are both leader in {}",
                            other, view.id, view.current_term
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

/// S-02: within one leadership tenure, a leader never overwrites
/// or deletes its own entries; its log changes only by tail
/// appends and base advancement. Entries from an earlier tenure
/// carry no guarantee: as a follower the node may have them
/// overwritten by the current leader.
fn leader_append_only(
    views: &[NodeView<'_>],
    previous: &mut BTreeMap<NodeId, (LogIndex, Term, Vec<LogEntry>)>,
) -> Result<(), OracleViolation> {
    for view in views {
        if view.role != Role::Leader {
            // Append-only binds within one leadership tenure. Once
            // the node serves as a follower, the current leader may
            // legitimately overwrite its uncommitted tail or
            // replace its log through a snapshot, so the remembered
            // tenure snapshot is dropped here and a later tenure
            // starts fresh.
            previous.remove(&view.id);
            continue;
        }
        if let Some((prev_base, _prev_term, prev_entries)) = previous.get(&view.id) {
            // Every previously held entry still above the new base
            // must be present and identical.
            for (offset, entry) in prev_entries.iter().enumerate() {
                let index = LogIndex(prev_base.0 + 1 + offset as u64);
                if index <= view.base_index {
                    continue;
                }
                match view.entry_at(index) {
                    Some(current) if current == entry => {}
                    other => {
                        return Err(OracleViolation {
                            oracle: "leader_append_only",
                            detail: format!(
                                "leader {} changed its entry at {}: was {:?}, now {:?}",
                                view.id, index, entry, other
                            ),
                        });
                    }
                }
            }
        }
        previous.insert(
            view.id,
            (view.base_index, view.base_term, view.entries.to_vec()),
        );
    }
    Ok(())
}

/// S-03: if two logs hold an entry at the same index with the same
/// term, the logs are identical in all entries up to that index.
/// Checked in one pass per pair: once the logs diverge, no later
/// index may agree on term again.
fn log_matching(views: &[NodeView<'_>]) -> Result<(), OracleViolation> {
    for a in views {
        for b in views {
            if a.id >= b.id {
                continue;
            }
            let shared_base = a.base_index.max(b.base_index);
            let shared_last = a.last_index().min(b.last_index());
            let mut diverged_at: Option<LogIndex> = None;
            let mut index = LogIndex(shared_base.0 + 1);
            while index <= shared_last {
                if diverged_at.is_none() && a.entry_at(index) != b.entry_at(index) {
                    diverged_at = Some(index);
                }
                if let Some(diverged) = diverged_at
                    && a.term_at(index).is_some()
                    && a.term_at(index) == b.term_at(index)
                {
                    return Err(OracleViolation {
                        oracle: "log_matching",
                        detail: format!(
                            "logs of {} and {} share term at {} but diverged at {}",
                            a.id, b.id, index, diverged
                        ),
                    });
                }
                index = index.next();
            }
        }
    }
    Ok(())
}

/// S-04: an entry committed in any term is present in the log of
/// every leader of every higher term.
fn leader_completeness(views: &[NodeView<'_>]) -> Result<(), OracleViolation> {
    let mut committed: Vec<(LogIndex, Term)> = Vec::new();
    for view in views {
        let mut index = LogIndex(view.base_index.0 + 1);
        while index <= view.commit_index.min(view.last_index()) {
            if let Some(term) = view.term_at(index) {
                committed.push((index, term));
            }
            index = index.next();
        }
    }
    for view in views {
        if view.role != Role::Leader {
            continue;
        }
        for (index, term) in &committed {
            // The property binds leaders of higher terms than
            // the committing term (the S-04 statement above). A
            // leader still sitting in a lower term predates the
            // commit: partitioned away, it cannot know the entry
            // exists, and it cannot commit anything of its own
            // without a majority, so nothing is lost by the
            // entry's absence from its log. Requiring it here
            // made the oracle claim more than Raft guarantees
            // (found by the scenario seed sweep: at seeds 7900
            // and 8100 node 3 leads term 1 in isolation while
            // the majority commits in term 2).
            if *term >= view.current_term {
                continue;
            }
            // Entries at or below the leader's snapshot base are
            // held by its snapshot, not its log: below the base the
            // snapshot covers them by construction, and at the base
            // the recorded base term must match. Only entries above
            // the base are checked against the log itself. Without
            // this, a leader that compacts past a committed entry
            // reads as missing it.
            let present = match (*index).cmp(&view.base_index) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => view.base_term == *term,
                std::cmp::Ordering::Greater => view.term_at(*index) == Some(*term),
            };
            if !present {
                return Err(OracleViolation {
                    oracle: "leader_completeness",
                    detail: format!(
                        "leader {} in {} is missing the committed entry at {} from {}",
                        view.id, view.current_term, index, term
                    ),
                });
            }
        }
    }
    Ok(())
}

/// S-05: no two nodes apply different entries at the same index.
fn state_machine_safety(views: &[NodeView<'_>]) -> Result<(), OracleViolation> {
    let mut applied: BTreeMap<LogIndex, (NodeId, &LogEntry)> = BTreeMap::new();
    for view in views {
        for (index, entry) in view.applied {
            match applied.get(index) {
                Some((other, existing)) if *existing != entry => {
                    return Err(OracleViolation {
                        oracle: "state_machine_safety",
                        detail: format!(
                            "nodes {} and {} applied different entries at {}",
                            other, view.id, index
                        ),
                    });
                }
                _ => {
                    applied.insert(*index, (view.id, entry));
                }
            }
        }
    }
    Ok(())
}

/// Structural assertions: commit and applied indexes are monotone
/// per node, applied never exceeds commit, and a leader's match
/// index for a peer never decreases (S-07's oracle half).
fn structural(views: &[NodeView<'_>], state: &mut OracleState) -> Result<(), OracleViolation> {
    for view in views {
        if view.last_applied > view.commit_index {
            return Err(OracleViolation {
                oracle: "structural",
                detail: format!(
                    "node {} applied {} beyond its commit index {}",
                    view.id, view.last_applied, view.commit_index
                ),
            });
        }
        if let Some((prev_commit, prev_applied)) = state.indexes.get(&view.id) {
            if view.commit_index < *prev_commit || view.last_applied < *prev_applied {
                return Err(OracleViolation {
                    oracle: "structural",
                    detail: format!(
                        "node {} indexes moved backwards: commit {} -> {}, applied {} -> {}",
                        view.id, prev_commit, view.commit_index, prev_applied, view.last_applied
                    ),
                });
            }
        }
        state
            .indexes
            .insert(view.id, (view.commit_index, view.last_applied));
        for (peer, match_index) in view.match_indexes {
            let key = (view.id, view.current_term, *peer);
            if let Some(prev) = state.matches.get(&key) {
                if match_index < prev {
                    return Err(OracleViolation {
                        oracle: "structural",
                        detail: format!(
                            "leader {} match for {} decreased in {}: {} -> {}",
                            view.id, peer, view.current_term, prev, match_index
                        ),
                    });
                }
            }
            state.matches.insert(key, *match_index);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use simraft_core::EntryKind;

    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term: Term(term),
            kind: EntryKind::NoOp,
        }
    }

    fn view(
        id: u64,
        role: Role,
        term: u64,
        entries: &[LogEntry],
        commit_index: u64,
    ) -> NodeView<'_> {
        NodeView {
            id: NodeId(id),
            role,
            current_term: Term(term),
            base_index: LogIndex::ZERO,
            base_term: Term::ZERO,
            entries,
            commit_index: LogIndex(commit_index),
            last_applied: LogIndex::ZERO,
            applied: &[],
            match_indexes: &[],
        }
    }

    /// S-04 binds leaders of higher terms than the committing
    /// term only: a partitioned leader still sitting in term 1
    /// is not required to hold an entry the majority committed
    /// in term 2 (scenario seed sweep, seeds 7900 and 8100).
    #[test]
    fn stale_lower_term_leader_is_not_flagged() {
        let committed = [entry(2)];
        let empty: [LogEntry; 0] = [];
        let views = [
            view(1, Role::Follower, 2, &committed, 1),
            view(2, Role::Follower, 2, &committed, 1),
            view(3, Role::Leader, 1, &empty, 0),
        ];
        assert!(leader_completeness(&views).is_ok());
    }

    /// A leader of a higher term that is missing a committed
    /// entry is exactly the violation S-04 exists to catch.
    #[test]
    fn higher_term_leader_missing_a_committed_entry_is_flagged() {
        let committed = [entry(2)];
        let empty: [LogEntry; 0] = [];
        let views = [
            view(1, Role::Follower, 2, &committed, 1),
            view(2, Role::Leader, 3, &empty, 0),
        ];
        match leader_completeness(&views) {
            Ok(()) => panic!("the higher-term leader must be flagged"),
            Err(violation) => assert_eq!(violation.oracle, "leader_completeness"),
        }
    }
}
