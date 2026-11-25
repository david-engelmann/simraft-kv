//! The simulated disk: volatile and durable regions per node.
//!
//! Docs/ARCHITECTURE.md sections 12.3 and 7.3. Writes land in the
//! volatile buffer first and reach the durable region only on an
//! explicit fsync. A crash drops the volatile buffer, which models
//! the failure that matters: a node that answered as if it had
//! persisted, then lost the write.
//!
//! The durable log is held as section 7.3 log records (magic
//! `SRLG`, the section 7.2 entry encoding, a CRC-32 over the
//! domain prefix `SRKV/log/v1` plus the record bytes), the same
//! byte format the simraft-node file store writes. Restoring the
//! log decodes records in index order and stops at the first bad
//! record (bad magic, bad length, checksum mismatch, or an index
//! out of sequence): the record and everything after it is a torn
//! tail, dropped and reported, exactly the recovery rule of
//! section 7.3. Post-M4 Lane 2 adds the corruption surface: a
//! scenario can flip one byte in a durable log record or in the
//! durable snapshot payload and watch the next restore detect it.
//!
//! The disk tracks the durable snapshot's boundary as its log
//! base. The durable records are never dropped: they are the
//! node's genuine history, retained whole, exactly as the
//! simraft-node file store keeps its log file. But the volatile
//! log holds only the entries above the base, and the restore
//! verifies the covered records and skips them, returning only
//! the suffix above the base (invariant N-08). Positions in the
//! volatile log and in the restore are always aligned to
//! absolute indexes through the base, so a write that lands
//! after a snapshot install is stored at its own index, never
//! shifted into the covered prefix.

use simraft_core::{HardState, LogEntry, LogIndex, SnapshotMeta};

use crate::wire::{decode_entry_bytes, encode_entry_bytes};

/// The section 7.3 log record magic.
const LOG_MAGIC: &[u8; 4] = b"SRLG";
/// The section 7.3 record version.
const RECORD_VERSION: u16 = 1;
/// Magic (4) + version (2) + record length (4) + index (8).
const HEADER_LEN: usize = 18;
/// The trailing CRC-32 length.
const CRC_LEN: usize = 4;
/// The CRC domain prefix for log records (section 7.3).
const LOG_CRC_DOMAIN: &[u8] = b"SRKV/log/v1";

/// A torn durable log record, found while restoring.
///
/// The record sits at `byte_offset`, claims `index`, and its
/// stored CRC-32 (`expected_crc`) does not match the computed
/// one (`actual_crc`). A record too damaged to read its header
/// or checksum reports zeroes for the fields it cannot supply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TornRecord {
    /// The byte offset of the bad record in the durable log.
    pub byte_offset: usize,
    /// The index the bad record claims (the next expected index
    /// when the header itself is unreadable).
    pub index: LogIndex,
    /// The CRC-32 stored in the record.
    pub expected_crc: u32,
    /// The CRC-32 computed over the record as found.
    pub actual_crc: u32,
}

/// The result of restoring the durable log: the valid record
/// prefix as entries, and the torn record that ended it, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRestore {
    /// The entries decoded from the valid records above the log
    /// base, in index order (the base plus one first).
    pub entries: Vec<LogEntry>,
    /// The first bad record, when one ended the restore.
    pub torn: Option<TornRecord>,
}

/// One node's simulated storage.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    durable_hard: Option<HardState>,
    volatile_hard: Option<HardState>,
    durable_log: Vec<u8>,
    log_base: LogIndex,
    volatile_entries: Vec<LogEntry>,
    durable_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
    volatile_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
}

/// Encode one durable log record (section 7.3) for `entry` at
/// `index`.
fn encode_log_record(index: LogIndex, entry: &LogEntry) -> Vec<u8> {
    let body =
        encode_entry_bytes(entry).expect("simulated log entry fits within 16 MiB frame limit");
    let record_len = HEADER_LEN + body.len() + CRC_LEN;
    let mut out = Vec::with_capacity(record_len);
    out.extend_from_slice(LOG_MAGIC);
    out.extend_from_slice(&RECORD_VERSION.to_le_bytes());
    out.extend_from_slice(&u32::try_from(record_len).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(&index.0.to_le_bytes());
    out.extend_from_slice(&body);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(LOG_CRC_DOMAIN);
    hasher.update(&out);
    out.extend_from_slice(&hasher.finalize().to_le_bytes());
    out
}

/// The offset and total length of the durable record claiming
/// `index`, walking record lengths without verifying checksums.
fn record_span(log: &[u8], index: LogIndex) -> Option<(usize, usize)> {
    let mut offset = 0usize;
    while offset + HEADER_LEN <= log.len() {
        let record_len = u32::from_le_bytes(log[offset + 6..offset + 10].try_into().ok()?) as usize;
        if record_len < HEADER_LEN + CRC_LEN || offset + record_len > log.len() {
            return None;
        }
        let found = u64::from_le_bytes(log[offset + 10..offset + 18].try_into().ok()?);
        if found == index.0 {
            return Some((offset, record_len));
        }
        offset += record_len;
    }
    None
}

/// The byte offset just past each complete record in `log`,
/// walking record lengths without verifying checksums. The
/// walk stops at the first record whose length field is
/// unusable or runs past the end.
fn record_ends(log: &[u8]) -> Vec<usize> {
    let mut ends = Vec::new();
    let mut offset = 0usize;
    while offset + HEADER_LEN <= log.len() {
        let Some(len_bytes) = log[offset + 6..offset + 10].try_into().ok() else {
            break;
        };
        let record_len = u32::from_le_bytes(len_bytes) as usize;
        if record_len < HEADER_LEN + CRC_LEN || offset + record_len > log.len() {
            break;
        }
        offset += record_len;
        ends.push(offset);
    }
    ends
}

/// The number of complete records at the head of `log` claiming
/// an index at or below `base`, walking record lengths without
/// verifying checksums. Records sit in index order, so the walk
/// stops at the first record above the base or the first
/// unusable length.
fn covered_records(log: &[u8], base: LogIndex) -> usize {
    let mut count = 0usize;
    let mut offset = 0usize;
    while offset + HEADER_LEN <= log.len() {
        let Some(len_bytes) = log[offset + 6..offset + 10].try_into().ok() else {
            break;
        };
        let record_len = u32::from_le_bytes(len_bytes) as usize;
        if record_len < HEADER_LEN + CRC_LEN || offset + record_len > log.len() {
            break;
        }
        let Some(index_bytes) = log[offset + 10..offset + 18].try_into().ok() else {
            break;
        };
        if u64::from_le_bytes(index_bytes) > base.0 {
            break;
        }
        count += 1;
        offset += record_len;
    }
    count
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

    /// Buffer log entries starting at `from_index`: the volatile
    /// log, which holds only the entries above the log base, is
    /// truncated to the entries below `from_index` and the new
    /// suffix appended, mirroring a real log rewrite.
    pub fn write_entries(&mut self, from_index: LogIndex, entries: &[LogEntry]) {
        let keep = usize::try_from(
            from_index
                .0
                .saturating_sub(1)
                .saturating_sub(self.log_base.0),
        )
        .unwrap_or(usize::MAX);
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
    ///
    /// An fsync makes new writes durable; it does not rewrite
    /// bytes already on the disk. The durable records the
    /// volatile log still agrees with are kept byte for byte,
    /// so a corruption in an already durable record survives an
    /// fsync of later appends, exactly as on real media.
    /// Encoding resumes at the first position where the
    /// volatile log diverges from the durable records (a
    /// rewritten suffix), or at the first record the restore
    /// cannot read when the volatile log has since been
    /// truncated back past it. Positions here are above the log
    /// base: the covered records at the head of the durable log
    /// are counted, kept byte for byte, and never enter the
    /// comparison.
    pub fn fsync_entries(&mut self) {
        let restore = self.restore_log();
        let ends = record_ends(&self.durable_log);
        let covered = covered_records(&self.durable_log, self.log_base);
        // Length of the agreeing prefix between the restored
        // durable entries and the volatile log.
        let mut keep = 0usize;
        while keep < restore.entries.len()
            && keep < self.volatile_entries.len()
            && restore.entries[keep] == self.volatile_entries[keep]
        {
            keep += 1;
        }
        if restore.torn.is_some() && keep == restore.entries.len() {
            if covered + self.volatile_entries.len() >= ends.len() {
                // The volatile log still covers every durable
                // record: nothing already durable is rewritten,
                // so the torn record and everything after it
                // stay exactly as they are on the disk, and any
                // newer entries append past them.
                keep = ends.len() - covered;
            } else if self.volatile_entries.len() > restore.entries.len() {
                // The volatile log was rewritten from the torn
                // position onward; the torn record is replaced.
                keep = restore.entries.len();
            } else {
                keep = self.volatile_entries.len();
            }
        }
        let mut bytes = if covered + keep == 0 {
            Vec::new()
        } else {
            let cut = ends.get(covered + keep - 1).copied().unwrap_or(0);
            self.durable_log[..cut].to_vec()
        };
        for (position, entry) in self.volatile_entries.iter().enumerate().skip(keep) {
            let offset = u64::try_from(position + 1).unwrap_or(u64::MAX);
            let index = LogIndex(self.log_base.0 + offset);
            bytes.extend_from_slice(&encode_log_record(index, entry));
        }
        self.durable_log = bytes;
    }

    /// Make the buffered snapshot durable.
    ///
    /// The snapshot covers the log through its boundary, so the
    /// log base advances to the boundary (invariant N-08): the
    /// volatile entries at or below it are dropped, and every
    /// later write, fsync, and restore aligns positions to
    /// absolute indexes through the new base. The durable
    /// records are kept whole; the restore verifies and skips
    /// the covered prefix. This runs in the same fsync step
    /// that steps `SnapshotPersisted` in the core, so the
    /// disk's base and the core's base jump together.
    pub fn fsync_snapshot(&mut self) {
        if let Some(snapshot) = self.volatile_snapshot.take() {
            let boundary = snapshot.0.last_included_index;
            if boundary > self.log_base {
                let drop = usize::try_from(boundary.0 - self.log_base.0).unwrap_or(usize::MAX);
                let drained = drop.min(self.volatile_entries.len());
                self.volatile_entries.drain(..drained);
                self.log_base = boundary;
            }
            self.durable_snapshot = Some(snapshot);
        }
    }

    /// Drop every volatile buffer: after a crash the disk holds
    /// exactly its durable state. The volatile log is rebuilt
    /// from the durable records, so a torn durable tail is
    /// already gone from the volatile view.
    pub fn crash(&mut self) {
        self.volatile_hard.clone_from(&self.durable_hard);
        self.volatile_entries = self.restore_log().entries;
        self.volatile_snapshot.clone_from(&self.durable_snapshot);
    }

    /// The durable hard state, if any was ever fsynced.
    #[must_use]
    pub fn durable_hard(&self) -> Option<HardState> {
        self.durable_hard.clone()
    }

    /// Restore the durable log: decode its records in index
    /// order, stopping at the first bad one.
    ///
    /// Records at or below the log base are verified and
    /// skipped: the durable snapshot covers them, and the
    /// restore returns only the entries above the base
    /// (invariant N-08). The valid suffix is the restored log;
    /// a bad record and everything after it is a torn tail,
    /// reported and dropped (section 7.3).
    ///
    /// # Panics
    ///
    /// The field reads use `expect` on slices whose lengths the
    /// guards above them establish (a record is only read after
    /// its header and total length check out), so the panics are
    /// unreachable unless those guards are changed.
    #[must_use]
    pub fn restore_log(&self) -> LogRestore {
        self.restore_from(self.log_base)
    }

    /// Restore the durable log ignoring the snapshot base:
    /// every record from index 1, verified in order. A driver
    /// whose snapshot payload failed verification at boot uses
    /// this to replay the whole log instead (invariant H-06):
    /// the snapshot is rejected, so nothing is covered.
    #[must_use]
    pub fn restore_whole_log(&self) -> LogRestore {
        self.restore_from(LogIndex::ZERO)
    }

    fn restore_from(&self, base: LogIndex) -> LogRestore {
        let mut entries = Vec::new();
        let mut offset = 0usize;
        let mut expected = base.0 + 1;
        let torn = loop {
            let rest = &self.durable_log[offset..];
            if rest.is_empty() {
                break None;
            }
            if rest.len() < HEADER_LEN + CRC_LEN {
                break Some(TornRecord {
                    byte_offset: offset,
                    index: LogIndex(expected),
                    expected_crc: 0,
                    actual_crc: 0,
                });
            }
            let record_len = u32::from_le_bytes(
                rest[6..10]
                    .try_into()
                    .expect("the length field is four bytes"),
            ) as usize;
            if &rest[0..4] != LOG_MAGIC
                || u16::from_le_bytes(
                    rest[4..6]
                        .try_into()
                        .expect("the version field is two bytes"),
                ) != RECORD_VERSION
                || record_len < HEADER_LEN + CRC_LEN
                || record_len > rest.len()
            {
                break Some(TornRecord {
                    byte_offset: offset,
                    index: LogIndex(expected),
                    expected_crc: 0,
                    actual_crc: 0,
                });
            }
            let record = &rest[..record_len];
            let index = u64::from_le_bytes(
                record[10..18]
                    .try_into()
                    .expect("the index field is eight bytes"),
            );
            let stored_crc = u32::from_le_bytes(
                record[record_len - CRC_LEN..]
                    .try_into()
                    .expect("the checksum field is four bytes"),
            );
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(LOG_CRC_DOMAIN);
            hasher.update(&record[..record_len - CRC_LEN]);
            let actual_crc = hasher.finalize();
            if stored_crc != actual_crc {
                break Some(TornRecord {
                    byte_offset: offset,
                    index: LogIndex(index),
                    expected_crc: stored_crc,
                    actual_crc,
                });
            }
            if index <= base.0 {
                // Covered by the durable snapshot: verified
                // above, skipped here. The restore returns
                // only the entries above the base.
                offset += record_len;
                continue;
            }
            if index != expected {
                break Some(TornRecord {
                    byte_offset: offset,
                    index: LogIndex(index),
                    expected_crc: stored_crc,
                    actual_crc,
                });
            }
            let body = &record[HEADER_LEN..record_len - CRC_LEN];
            let Ok(entry) = decode_entry_bytes(body) else {
                break Some(TornRecord {
                    byte_offset: offset,
                    index: LogIndex(index),
                    expected_crc: stored_crc,
                    actual_crc,
                });
            };
            entries.push(entry);
            offset += record_len;
            expected += 1;
        };
        LogRestore { entries, torn }
    }

    /// The durable log entries above the log base, in index
    /// order: the valid record suffix, as `restore_log` decodes
    /// it.
    #[must_use]
    pub fn durable_entries(&self) -> Vec<LogEntry> {
        self.restore_log().entries
    }

    /// The highest durable log index: the log base plus the
    /// number of entries the restore decodes above it.
    #[must_use]
    pub fn durable_up_to(&self) -> LogIndex {
        let count = u64::try_from(self.restore_log().entries.len()).unwrap_or(u64::MAX);
        LogIndex(self.log_base.0 + count)
    }

    /// The durable snapshot, if one was fsynced.
    #[must_use]
    pub const fn durable_snapshot(&self) -> Option<&(SnapshotMeta, Vec<u8>)> {
        self.durable_snapshot.as_ref()
    }

    /// Flip one byte inside the durable log record for `index`
    /// (post-M4 Lane 2): the record's stored checksum no longer
    /// matches, so the next restore truncates the log there.
    /// Returns false when no such record exists or its body is
    /// empty.
    pub fn corrupt_log_record(&mut self, index: LogIndex) -> bool {
        let Some((offset, record_len)) = record_span(&self.durable_log, index) else {
            return false;
        };
        let body_len = record_len - HEADER_LEN - CRC_LEN;
        if body_len == 0 {
            return false;
        }
        let at = offset + HEADER_LEN + body_len - 1;
        self.durable_log[at] ^= 0xFF;
        true
    }

    /// Flip one byte in the durable snapshot payload (post-M4
    /// Lane 2): the payload's internal checksum (the KV snapshot
    /// SHA-256) no longer verifies, so the next restore rejects
    /// the snapshot. Returns false when no durable snapshot is
    /// held.
    pub fn corrupt_snapshot(&mut self) -> bool {
        let Some((_, data)) = &mut self.durable_snapshot else {
            return false;
        };
        let Some(last) = data.last_mut() else {
            return false;
        };
        *last ^= 0xFF;
        true
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

    #[test]
    fn a_corrupt_record_truncates_the_restore_at_that_record() {
        let mut disk = SimDisk::new();
        disk.write_entries(LogIndex(1), &[entry(1), entry(1), entry(2), entry(2)]);
        disk.fsync_entries();
        assert!(disk.corrupt_log_record(LogIndex(3)));
        let restore = disk.restore_log();
        assert_eq!(restore.entries.len(), 2);
        let torn = restore.torn.expect("the corruption is reported");
        assert_eq!(torn.index, LogIndex(3));
        assert_ne!(torn.expected_crc, torn.actual_crc);
        assert!(!disk.corrupt_log_record(LogIndex(99)));
    }

    #[test]
    fn an_fsync_of_later_appends_preserves_a_corrupt_record() {
        let mut disk = SimDisk::new();
        disk.write_entries(LogIndex(1), &[entry(1), entry(1), entry(1), entry(1)]);
        disk.fsync_entries();
        assert!(disk.corrupt_log_record(LogIndex(3)));
        disk.write_entries(LogIndex(5), &[entry(2), entry(2)]);
        disk.fsync_entries();
        let restore = disk.restore_log();
        assert_eq!(restore.entries.len(), 2);
        let torn = restore.torn.expect("the corruption survives the fsync");
        assert_eq!(torn.index, LogIndex(3));
    }

    #[test]
    fn an_fsync_with_no_new_entries_preserves_a_corrupt_record() {
        let mut disk = SimDisk::new();
        disk.write_entries(LogIndex(1), &[entry(1), entry(1), entry(1), entry(1)]);
        disk.fsync_entries();
        assert!(disk.corrupt_log_record(LogIndex(3)));
        // A re-persist of the same tail (the core re-emits
        // PersistEntries for entries it already holds) fsyncs
        // with the volatile log exactly as long as the durable
        // one: the corrupt record must still survive.
        disk.fsync_entries();
        let restore = disk.restore_log();
        assert_eq!(restore.entries.len(), 2);
        let torn = restore.torn.expect("the corruption survives a no-op fsync");
        assert_eq!(torn.index, LogIndex(3));
    }

    #[test]
    fn an_fsync_rewrites_from_the_first_divergence() {
        let mut disk = SimDisk::new();
        disk.write_entries(LogIndex(1), &[entry(1), entry(1), entry(1), entry(1)]);
        disk.fsync_entries();
        disk.write_entries(LogIndex(3), &[entry(2), entry(2)]);
        disk.fsync_entries();
        let restore = disk.restore_log();
        assert!(restore.torn.is_none());
        let terms: Vec<Term> = restore.entries.iter().map(|e| e.term).collect();
        assert_eq!(terms, vec![Term(1), Term(1), Term(2), Term(2)]);
    }

    fn meta(index: u64) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(index),
            last_included_term: Term(1),
            config: simraft_core::ClusterConfig::new([simraft_core::NodeId(1)], [])
                .expect("one voter validates"),
        }
    }

    #[test]
    fn a_snapshot_fsync_rebases_the_log_base_and_keeps_later_writes_aligned() {
        let mut disk = SimDisk::new();
        disk.write_entries(LogIndex(1), &[entry(1), entry(1), entry(1), entry(1)]);
        disk.fsync_entries();
        // A snapshot whose boundary runs past everything the
        // disk ever wrote (the node restored a short log, then
        // installed a snapshot): the base advances to the
        // boundary and the restore above it is empty.
        disk.write_snapshot(meta(6), vec![9]);
        disk.fsync_snapshot();
        assert_eq!(disk.durable_up_to(), LogIndex(6));
        assert_eq!(disk.restore_log().entries.len(), 0);
        assert!(disk.restore_log().torn.is_none());
        // The next write starts at index 7 and is stored at
        // index 7, not shifted into the covered gap.
        disk.write_entries(LogIndex(7), &[entry(2), entry(2)]);
        disk.fsync_entries();
        let restore = disk.restore_log();
        assert!(restore.torn.is_none());
        assert_eq!(restore.entries.len(), 2);
        assert_eq!(restore.entries[0].term, Term(2));
        assert_eq!(disk.durable_up_to(), LogIndex(8));
        disk.crash();
        assert_eq!(disk.durable_entries().len(), 2);
        // The covered records are still on the disk, and a
        // corruption inside one is still detected on restore.
        assert!(disk.corrupt_log_record(LogIndex(2)));
        let restore = disk.restore_log();
        assert_eq!(
            restore
                .torn
                .expect("the covered corruption is reported")
                .index,
            LogIndex(2)
        );
    }

    #[test]
    fn a_snapshot_fsync_keeps_the_covered_records_and_the_suffix_aligned() {
        let mut disk = SimDisk::new();
        let entries: Vec<LogEntry> = (0..8).map(|_| entry(1)).collect();
        disk.write_entries(LogIndex(1), &entries);
        disk.fsync_entries();
        disk.write_snapshot(meta(5), vec![9]);
        disk.fsync_snapshot();
        // The restore returns only the suffix above the base.
        let restore = disk.restore_log();
        assert!(restore.torn.is_none());
        assert_eq!(restore.entries.len(), 3);
        assert_eq!(disk.durable_up_to(), LogIndex(8));
        // Later writes stay at their absolute indexes.
        disk.write_entries(LogIndex(9), &[entry(2)]);
        disk.fsync_entries();
        let restore = disk.restore_log();
        assert!(restore.torn.is_none());
        assert_eq!(restore.entries.len(), 4);
        assert_eq!(restore.entries[3].term, Term(2));
        assert_eq!(disk.durable_up_to(), LogIndex(9));
        // A corruption above the base truncates the suffix.
        assert!(disk.corrupt_log_record(LogIndex(7)));
        let restore = disk.restore_log();
        assert_eq!(restore.entries.len(), 1);
        assert_eq!(
            restore.torn.expect("the corruption is reported").index,
            LogIndex(7)
        );
    }

    #[test]
    fn a_corrupt_snapshot_byte_is_flipped_in_place() {
        let mut disk = SimDisk::new();
        assert!(!disk.corrupt_snapshot());
        disk.write_snapshot(
            SnapshotMeta {
                last_included_index: LogIndex(4),
                last_included_term: Term(1),
                config: simraft_core::ClusterConfig::new([simraft_core::NodeId(1)], [])
                    .expect("one voter validates"),
            },
            vec![1, 2, 3],
        );
        disk.fsync_snapshot();
        assert!(disk.corrupt_snapshot());
        let (_, data) = disk.durable_snapshot().expect("the snapshot is held");
        assert_eq!(data, &vec![1, 2, 0xFC]);
    }
}
