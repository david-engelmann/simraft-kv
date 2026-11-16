//! The simulated disk: volatile and durable regions per node.
//!
//! Docs/ARCHITECTURE.md section 12.3. Writes land in the volatile
//! buffer first and reach the durable region only on an explicit
//! fsync. A crash drops the volatile buffer, which models the
//! failure that matters: a node that answered as if it had
//! persisted, then lost the write.

use simraft_core::{HardState, LogEntry, LogIndex, SnapshotMeta};

/// One node's simulated storage.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    durable_hard: Option<HardState>,
    volatile_hard: Option<HardState>,
    durable_entries: Vec<LogEntry>,
    volatile_entries: Vec<LogEntry>,
    durable_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
    volatile_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
}

impl SimDisk {
    /// A blank disk: nothing durable, nothing buffered.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Buffer a hard state write (not durable until `fsync_hard`).
    pub const fn write_hard(&mut self, hard: HardState) {
        self.volatile_hard = Some(hard);
    }

    /// Buffer log entries starting at `from_index` (one-based): the
    /// volatile log is truncated to `from_index - 1` entries and the
    /// new suffix appended, mirroring a real log rewrite.
    pub fn write_entries(&mut self, from_index: LogIndex, entries: &[LogEntry]) {
        let keep = usize::try_from(from_index.0.saturating_sub(1)).unwrap_or(usize::MAX);
        self.volatile_entries.truncate(keep);
        self.volatile_entries.extend_from_slice(entries);
    }

    /// Buffer a snapshot payload (not durable until
    /// `fsync_snapshot`).
    pub fn write_snapshot(&mut self, meta: SnapshotMeta, data: Vec<u8>) {
        self.volatile_snapshot = Some((meta, data));
    }

    /// Make the buffered hard state durable.
    pub const fn fsync_hard(&mut self) {
        if let Some(hard) = self.volatile_hard.take() {
            self.durable_hard = Some(hard);
        }
    }

    /// Make the buffered log durable through its current tail.
    pub fn fsync_entries(&mut self) {
        self.durable_entries.clone_from(&self.volatile_entries);
    }

    /// Make the buffered snapshot durable.
    pub fn fsync_snapshot(&mut self) {
        if let Some(snapshot) = self.volatile_snapshot.take() {
            self.durable_snapshot = Some(snapshot);
        }
    }

    /// Drop every volatile buffer: after a crash the disk holds
    /// exactly its durable state.
    pub fn crash(&mut self) {
        self.volatile_hard.clone_from(&self.durable_hard);
        self.volatile_entries.clone_from(&self.durable_entries);
        self.volatile_snapshot.clone_from(&self.durable_snapshot);
    }

    /// The durable hard state, if any was ever fsynced.
    #[must_use]
    pub fn durable_hard(&self) -> Option<HardState> {
        self.durable_hard.clone()
    }

    /// The durable log entries (index 1 first).
    #[must_use]
    pub fn durable_entries(&self) -> &[LogEntry] {
        &self.durable_entries
    }

    /// The durable snapshot, if one was fsynced.
    #[must_use]
    pub const fn durable_snapshot(&self) -> Option<&(SnapshotMeta, Vec<u8>)> {
        self.durable_snapshot.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simraft_core::{EntryKind, Term};

    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term: Term(term),
            kind: EntryKind::NoOp,
        }
    }

    #[test]
    fn unfsynced_writes_are_lost_in_a_crash() {
        let mut disk = SimDisk::new();
        disk.write_entries(LogIndex(1), &[entry(1), entry(1)]);
        disk.fsync_entries();
        disk.write_entries(LogIndex(3), &[entry(2)]);
        disk.crash();
        assert_eq!(disk.durable_entries().len(), 2);
        assert_eq!(disk.durable_entries()[1].term, Term(1));
    }

    #[test]
    fn fsynced_writes_survive_a_crash() {
        let mut disk = SimDisk::new();
        disk.write_hard(HardState {
            current_term: Term(3),
            voted_for: None,
        });
        disk.fsync_hard();
        disk.crash();
        assert_eq!(disk.durable_hard().map(|h| h.current_term), Some(Term(3)));
    }
}
