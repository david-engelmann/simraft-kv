//! The replicated log: a contiguous entry vector behind a snapshot base.
//!
//! The entry at log index `i` lives at vector position
//! `i - base_index - 1`. Index 0 is the sentinel with term 0. Snapshot
//! support (base advancement and compaction) lands at M3; at M1 the base
//! is always the sentinel.

use crate::ids::{ClientId, LogIndex, Term};
use crate::membership::ClusterConfig;

/// What a log entry does when it applies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    /// No operation. Leaders append one on election so they hold a
    /// current-term entry they may count toward commitment.
    NoOp,
    /// A client command. The payload is the canonical command encoding
    /// produced by simraft-kv; the core treats it as opaque bytes and
    /// carries the client identity alongside for deduplication (M2).
    Command {
        /// The client that proposed the command.
        client_id: ClientId,
        /// The per-client sequence number of the proposal.
        seq: u64,
        /// The canonical command payload bytes.
        payload: Vec<u8>,
    },
    /// A cluster configuration change (used from M3).
    Config {
        /// The configuration this entry installs when it applies.
        config: ClusterConfig,
    },
}

/// One replicated log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    /// The term of the leader that created this entry.
    pub term: Term,
    /// The entry payload.
    pub kind: EntryKind,
}

impl LogEntry {
    /// An approximate encoded size of this entry, used to enforce the
    /// `AppendEntries` byte budget.
    #[must_use]
    pub fn approx_len(&self) -> usize {
        match &self.kind {
            EntryKind::NoOp => 16,
            EntryKind::Command { payload, .. } => 32 + payload.len(),
            EntryKind::Config { config } => 32 + 8 * (config.voters.len() + config.learners.len()),
        }
    }
}

/// A node's log: the snapshot base plus every entry after it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Log {
    base_index: LogIndex,
    base_term: Term,
    entries: Vec<LogEntry>,
}

impl Log {
    /// An empty log with only the sentinel base.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The index of the snapshot base (0 until snapshots land at M3).
    #[must_use]
    pub const fn base_index(&self) -> LogIndex {
        self.base_index
    }

    /// The index of the last entry, or the base index when empty.
    #[must_use]
    pub fn last_index(&self) -> LogIndex {
        LogIndex(self.base_index.0 + self.entries.len() as u64)
    }

    /// The term of the last entry, or the base term when empty.
    #[must_use]
    pub fn last_term(&self) -> Term {
        self.entries.last().map_or(self.base_term, |e| e.term)
    }

    /// The term of the entry at `index`, the base term at the base index,
    /// term 0 at the sentinel, and `None` outside the log.
    #[must_use]
    pub fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index == LogIndex::ZERO && self.base_index == LogIndex::ZERO {
            return Some(Term::ZERO);
        }
        if index == self.base_index {
            return Some(self.base_term);
        }
        if index < self.base_index || index > self.last_index() {
            return None;
        }
        let pos = self.pos_of(index);
        self.entries.get(pos).map(|e| e.term)
    }

    /// The entry at `index`, or `None` at or below the base and beyond
    /// the tail.
    #[must_use]
    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index <= self.base_index || index > self.last_index() {
            return None;
        }
        let pos = self.pos_of(index);
        self.entries.get(pos)
    }

    /// The vector position of `index`. Callers have already bounds
    /// checked `index` against the in-memory entry vector, so the
    /// offset always fits in a `usize`.
    fn pos_of(&self, index: LogIndex) -> usize {
        usize::try_from(index.0 - self.base_index.0 - 1)
            .expect("a bounds-checked log offset always fits in usize")
    }

    /// Append entries at the tail. Returns the index of the first
    /// appended entry.
    pub fn append(&mut self, new_entries: &[LogEntry]) -> LogIndex {
        let first = self.last_index().next();
        self.entries.extend_from_slice(new_entries);
        first
    }

    /// Delete every entry at or after `index`. Deleting at or below the
    /// base index is a no-op: compacted history is never resurrected.
    pub fn truncate_from(&mut self, index: LogIndex) {
        if index <= self.base_index {
            return;
        }
        let keep = self.pos_of(index);
        self.entries.truncate(keep);
    }

    /// The first index whose entry carries `term`, if any.
    #[must_use]
    pub fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        // Terms are non-decreasing along a Raft log, so the run of any
        // term is one contiguous span found by binary search.
        if term == self.base_term && self.base_index != LogIndex::ZERO {
            return Some(self.base_index);
        }
        let start = self.entries.partition_point(|e| e.term < term);
        match self.entries.get(start) {
            Some(entry) if entry.term == term => {
                Some(LogIndex(self.base_index.0 + start as u64 + 1))
            }
            _ => None,
        }
    }

    /// The last index whose entry carries `term`, if any.
    #[must_use]
    pub fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        let end = self.entries.partition_point(|e| e.term <= term);
        if end == 0 {
            if term == self.base_term && self.base_index != LogIndex::ZERO {
                return Some(self.base_index);
            }
            return None;
        }
        match self.entries.get(end - 1) {
            Some(entry) if entry.term == term => Some(LogIndex(self.base_index.0 + end as u64)),
            _ => None,
        }
    }

    /// Up to `max_count` entries starting at `index`, bounded also by the
    /// approximate byte budget `max_bytes`. Empty when `index` is past
    /// the tail.
    #[must_use]
    pub fn entries_from(
        &self,
        index: LogIndex,
        max_count: usize,
        max_bytes: usize,
    ) -> Vec<LogEntry> {
        if index <= self.base_index || index > self.last_index() {
            return Vec::new();
        }
        let start = self.pos_of(index);
        let mut out = Vec::new();
        let mut bytes = 0_usize;
        for entry in self.entries.iter().skip(start) {
            if out.len() >= max_count {
                break;
            }
            bytes += entry.approx_len();
            if !out.is_empty() && bytes > max_bytes {
                break;
            }
            out.push(entry.clone());
        }
        out
    }

    /// Every entry in index order, for oracles and tests.
    #[must_use]
    pub fn entries(&self) -> &[LogEntry] {
        &self.entries
    }
}
