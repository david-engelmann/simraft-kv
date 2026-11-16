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

/// A torn durable log record, found while restoring: the record
/// at `byte_offset`, claiming `index`, whose stored CRC-32
/// (`expected_crc`) does not match the computed one
/// (`actual_crc`). A record too damaged to read its header or
/// checksum reports zeroes for the fields it cannot supply.
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
    /// The entries decoded from the valid records, index 1 first.
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
    volatile_entries: Vec<LogEntry>,
    durable_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
    volatile_snapshot: Option<(SnapshotMeta, Vec<u8>)>,
}

/// Encode one durable log record (section 7.3) for `entry` at
/// `index`.
fn encode_log_record(index: LogIndex, entry: &LogEntry) -> Vec<u8> {
    let body = encode_entry_bytes(entry);
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
        let record_len =
            u32::from_le_bytes(log[offset + 6..offset + 10].try_into().ok()?) as usize;
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

    /// Make the buffered log durable through its current tail:
    /// the volatile entries are encoded as section 7.3 records
    /// and replace the durable log bytes.
    pub fn fsync_entries(&mut self) {
        let mut bytes = Vec::new();
        for (position, entry) in self.volatile_entries.iter().enumerate() {
            let index = LogIndex(u64::try_from(position + 1).unwrap_or(u64::MAX));
            bytes.extend_from_slice(&encode_log_record(index, entry));
        }
        self.durable_log = bytes;
    }

    /// Make the buffered snapshot durable.
    pub fn fsync_snapshot(&mut self) {
        if let Some(snapshot) = self.volatile_snapshot.take() {
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
    /// order, stopping at the first bad one. The valid prefix is
    /// the restored log; a bad record and everything after it is
    /// a torn tail, reported and dropped (section 7.3).
    #[must_use]
    pub fn restore_log(&self) -> LogRestore {
        let mut entries = Vec::new();
        let mut offset = 0usize;
        let mut expected = 1u64;
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
            if stored_crc != actual_crc || index != expected {
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

    /// The durable log entries (index 1 first): the valid record
    /// prefix, as `restore_log` decodes it.
    #[must_use]
    pub fn durable_entries(&self) -> Vec<LogEntry> {
        self.restore_log().entries
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
