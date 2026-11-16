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
    matches: BTreeMap<(NodeId, NodeId), LogIndex>,
}

impl OracleState {
    /// Fresh oracle state for a new run.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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

/// S-02: a leader never overwrites or deletes its own entries;
/// its log changes only by tail appends and base advancement.
fn leader_append_only(
    views: &[NodeView<'_>],
    previous: &mut BTreeMap<NodeId, (LogIndex, Term, Vec<LogEntry>)>,
) -> Result<(), OracleViolation> {
    for view in views {
        if view.role != Role::Leader {
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
fn log_matching(views: &[NodeView<'_>]) -> Result<(), OracleViolation> {
    for a in views {
        for b in views {
            if a.id >= b.id {
                continue;
            }
            let shared_base = a.base_index.max(b.base_index);
            let shared_last = a.last_index().min(b.last_index());
            let mut index = LogIndex(shared_base.0 + 1);
            while index <= shared_last {
                if a.term_at(index) == b.term_at(index) && a.term_at(index).is_some() {
                    // Terms agree here; every entry from the higher
                    // base up to here must agree too.
                    let mut check = LogIndex(shared_base.0 + 1);
                    while check <= index {
                        if a.entry_at(check) != b.entry_at(check) {
                            return Err(OracleViolation {
                                oracle: "log_matching",
                                detail: format!(
                                    "logs of {} and {} share term at {} but differ at {}",
                                    a.id, b.id, index, check
                                ),
                            });
                        }
                        check = check.next();
                    }
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
            if view.term_at(*index) != Some(*term) {
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
            let key = (view.id, *peer);
            if let Some(prev) = state.matches.get(&key) {
                if match_index < prev {
                    return Err(OracleViolation {
                        oracle: "structural",
                        detail: format!(
                            "leader {} match for {} decreased: {} -> {}",
                            view.id, peer, prev, match_index
                        ),
                    });
                }
            }
            state.matches.insert(key, *match_index);
        }
    }
    Ok(())
}
