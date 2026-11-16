//! File storage for one node, in the durable record formats of
//! docs/ARCHITECTURE.md section 7.3.
//!
//! Three files live in the node's data directory. `hard_state`
//! holds one SRHS record and is replaced atomically (write to a
//! temporary file, fsync, rename, fsync the directory) on every
//! change. `log` is an append-only sequence of SRLG records, one
//! per entry, truncated from the back when the core overwrites
//! a tail. `snapshot` holds one SRSS record (the adapter's
//! snapshot container, in section 7.3's style) wrapping the
//! canonical simraft-kv snapshot payload, and is replaced
//! atomically like the hard state.
//!
//! Durability discipline: a persist call returns only after the
//! bytes it wrote are fsynced, because the core releases the
//! responses that depend on a persist only when the driver
//! acknowledges it, and the acknowledgment is the fsync.
//!
//! Recovery follows section 7.3: a log record whose CRC fails,
//! whose magic is unknown, or whose length runs past the end of
//! the file is a torn write. Recovery truncates the log at the
//! first bad record and reports it in the `Recovery` value
//! (byte offset, index, expected versus actual CRC) rather than
//! failing, because a torn tail is the expected shape of a
//! crash mid-append. A corrupt hard state or snapshot record is
//! not a torn write (both are replaced atomically) and is a
//! hard error with the values named.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use simraft_core::{HardState, LogEntry, LogIndex, NodeId, SnapshotMeta, Term};
use simraft_wire::{
    decode_config_bytes, decode_entry_bytes, encode_config_bytes, encode_entry_bytes,
};

use crate::NodeError;

const HARD_MAGIC: &[u8; 4] = b"SRHS";
const LOG_MAGIC: &[u8; 4] = b"SRLG";
const SNAP_MAGIC: &[u8; 4] = b"SRSS";
const RECORD_VERSION: u16 = 1;
const HARD_DOMAIN: &[u8] = b"SRKV/hard/v1";
const LOG_DOMAIN: &[u8] = b"SRKV/log/v1";
const SNAP_DOMAIN: &[u8] = b"SRKV/snap/v1";

fn crc32(domain: &[u8], bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize()
}

fn io(path: &Path, source: std::io::Error) -> NodeError {
    NodeError::Io {
        path: path.to_path_buf(),
        source,
    }
}

/// A torn log tail found and truncated during recovery, with
/// the values section 7.3 requires the recovery to report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornReport {
    /// The byte offset of the first bad record.
    pub byte_offset: u64,
    /// The index the bad record claimed.
    pub index: u64,
    /// The CRC stored in the record.
    pub expected_crc: u32,
    /// The CRC computed over the record's bytes.
    pub actual_crc: u32,
}

/// What recovery found in the data directory.
#[derive(Debug)]
pub struct Recovery {
    /// The durable hard state (term 0, no vote, when absent).
    pub hard_state: HardState,
    /// The snapshot, when one is durable: its identity and the
    /// canonical KV payload bytes.
    pub snapshot: Option<(SnapshotMeta, Vec<u8>)>,
    /// The log entries after the snapshot's base, in index
    /// order.
    pub entries: Vec<LogEntry>,
    /// The torn tail truncated during recovery, when one was
    /// found.
    pub torn: Option<TornReport>,
}

/// One node's durable store.
#[derive(Debug)]
pub struct FileStore {
    dir: PathBuf,
    /// (index, byte offset, total record length) per log record
    /// in the file, in file order.
    log_records: Vec<(u64, u64, u64)>,
    /// The snapshot base the log records follow.
    base_index: u64,
}

fn record_bytes(magic: [u8; 4], domain: &[u8], payload: &[u8]) -> Vec<u8> {
    let total = 4 + 2 + 4 + payload.len() + 4;
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&magic);
    out.extend_from_slice(&RECORD_VERSION.to_le_bytes());
    out.extend_from_slice(&u32::try_from(total).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(payload);
    let crc = crc32(domain, &out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Split one record off the front of `bytes`. Returns the
/// payload, the stored CRC, the computed CRC, and the record's
/// total length, or None when the header itself is incomplete.
fn split_record<'a>(domain: &[u8], bytes: &'a [u8]) -> Option<(&'a [u8], u32, u32, usize)> {
    if bytes.len() < 4 + 2 + 4 + 4 {
        return None;
    }
    let total = u32::from_le_bytes(bytes[6..10].try_into().ok()?) as usize;
    if total < 4 + 2 + 4 + 4 || bytes.len() < total {
        return None;
    }
    let stored = u32::from_le_bytes(bytes[total - 4..total].try_into().ok()?);
    let computed = crc32(domain, &bytes[..total - 4]);
    Some((&bytes[10..total - 4], stored, computed, total))
}

fn write_atomic(dir: &Path, name: &str, bytes: &[u8]) -> Result<(), NodeError> {
    let tmp = dir.join(format!("{name}.tmp"));
    let target = dir.join(name);
    {
        let mut file = File::create(&tmp).map_err(|e| io(&tmp, e))?;
        file.write_all(bytes).map_err(|e| io(&tmp, e))?;
        file.sync_all().map_err(|e| io(&tmp, e))?;
    }
    std::fs::rename(&tmp, &target).map_err(|e| io(&target, e))?;
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io(dir, e))?;
    Ok(())
}

fn read_file(path: &Path) -> Result<Option<Vec<u8>>, NodeError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io(path, e)),
    }
}

impl FileStore {
    /// Open the store in `dir` (created when absent) and recover
    /// the durable state.
    ///
    /// # Errors
    /// Returns `NodeError::CorruptStore` when the hard state or
    /// snapshot record is malformed (with the values), and
    /// `NodeError::Io` on filesystem failures.
    pub fn open(dir: &Path) -> Result<(Self, Recovery), NodeError> {
        std::fs::create_dir_all(dir).map_err(|e| io(dir, e))?;
        let hard_state = match read_file(&dir.join("hard_state"))? {
            None => HardState {
                current_term: Term(0),
                voted_for: None,
            },
            Some(bytes) => parse_hard_state(&bytes)?,
        };
        let snapshot = match read_file(&dir.join("snapshot"))? {
            None => None,
            Some(bytes) => Some(parse_snapshot(&bytes)?),
        };
        let base_index = snapshot
            .as_ref()
            .map_or(0, |(meta, _)| meta.last_included_index.0);
        let recovered = recover_log(&dir.join("log"), base_index)?;
        let (entries, records, torn) = (recovered.entries, recovered.records, recovered.torn);
        let store = Self {
            dir: dir.to_path_buf(),
            log_records: records,
            base_index,
        };
        Ok((
            store,
            Recovery {
                hard_state,
                snapshot,
                entries,
                torn,
            },
        ))
    }

    /// Persist the hard state, fsynced before returning.
    ///
    /// # Errors
    /// Returns `NodeError::Io` on filesystem failures.
    pub fn write_hard_state(
        &mut self,
        current_term: Term,
        voted_for: Option<NodeId>,
    ) -> Result<(), NodeError> {
        let mut payload = Vec::with_capacity(17);
        payload.extend_from_slice(&current_term.0.to_le_bytes());
        match voted_for {
            None => payload.push(0),
            Some(id) => {
                payload.push(1);
                payload.extend_from_slice(&id.0.to_le_bytes());
            }
        }
        write_atomic(
            &self.dir,
            "hard_state",
            &record_bytes(*HARD_MAGIC, HARD_DOMAIN, &payload),
        )
    }

    /// Persist entries per the core's storage semantics:
    /// truncate the durable log to `from_index - 1`, then store
    /// `entries` at consecutive indexes, fsynced before
    /// returning.
    ///
    /// # Errors
    /// Returns `NodeError::Io` on filesystem failures.
    pub fn persist_entries(
        &mut self,
        from_index: LogIndex,
        entries: &[LogEntry],
    ) -> Result<(), NodeError> {
        let path = self.dir.join("log");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| io(&path, e))?;
        // Truncate back to the record before from_index.
        let keep = self
            .log_records
            .iter()
            .take_while(|(index, _, _)| *index < from_index.0)
            .count();
        let truncate_at = if keep == 0 {
            0
        } else {
            let (_, offset, len) = self.log_records[keep - 1];
            offset + len
        };
        self.log_records.truncate(keep);
        file.set_len(truncate_at).map_err(|e| io(&path, e))?;
        file.seek(SeekFrom::Start(truncate_at))
            .map_err(|e| io(&path, e))?;
        let mut offset = truncate_at;
        for (i, entry) in entries.iter().enumerate() {
            let index = from_index.0 + u64::try_from(i).unwrap_or(u64::MAX);
            let mut payload = Vec::with_capacity(64);
            payload.extend_from_slice(&index.to_le_bytes());
            payload.extend_from_slice(&encode_entry_bytes(entry));
            let record = record_bytes(*LOG_MAGIC, LOG_DOMAIN, &payload);
            file.write_all(&record).map_err(|e| io(&path, e))?;
            let len = u64::try_from(record.len()).unwrap_or(u64::MAX);
            self.log_records.push((index, offset, len));
            offset += len;
        }
        file.sync_all().map_err(|e| io(&path, e))?;
        Ok(())
    }

    /// Persist a snapshot, fsynced before returning, then
    /// compact the log: records at or below the snapshot's last
    /// included index are dropped by rewriting the log with its
    /// surviving tail.
    ///
    /// # Errors
    /// Returns `NodeError::Io` on filesystem failures.
    pub fn write_snapshot(&mut self, meta: &SnapshotMeta, data: &[u8]) -> Result<(), NodeError> {
        let config = encode_config_bytes(&meta.config);
        let mut payload = Vec::with_capacity(24 + config.len() + data.len());
        payload.extend_from_slice(&meta.last_included_index.0.to_le_bytes());
        payload.extend_from_slice(&meta.last_included_term.0.to_le_bytes());
        payload.extend_from_slice(
            &u32::try_from(config.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        payload.extend_from_slice(&config);
        payload.extend_from_slice(data);
        write_atomic(
            &self.dir,
            "snapshot",
            &record_bytes(*SNAP_MAGIC, SNAP_DOMAIN, &payload),
        )?;
        // Compact the log file to the tail past the snapshot.
        let keep_from = self
            .log_records
            .iter()
            .position(|(index, _, _)| *index > meta.last_included_index.0)
            .unwrap_or(self.log_records.len());
        if keep_from > 0 {
            let path = self.dir.join("log");
            let surviving: Vec<(u64, u64, u64)> = self.log_records[keep_from..].to_vec();
            let bytes = if surviving.is_empty() {
                Vec::new()
            } else {
                let start = usize::try_from(surviving[0].1).unwrap_or(0);
                let last = surviving[surviving.len() - 1];
                let end = usize::try_from(last.1 + last.2).unwrap_or(0);
                let all = std::fs::read(&path).map_err(|e| io(&path, e))?;
                all[start..end].to_vec()
            };
            let mut records = Vec::with_capacity(surviving.len());
            let mut offset = 0_u64;
            for (index, _, len) in &surviving {
                records.push((*index, offset, *len));
                offset += len;
            }
            write_atomic(&self.dir, "log", &bytes)?;
            self.log_records = records;
        }
        self.base_index = meta.last_included_index.0;
        Ok(())
    }

    /// Read the durable snapshot back, for serving a follower
    /// the core asks this node to relay to.
    ///
    /// # Errors
    /// Returns `NodeError::CorruptStore` when the record is
    /// malformed, and `NodeError::Io` on filesystem failures.
    pub fn read_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, NodeError> {
        match read_file(&self.dir.join("snapshot"))? {
            None => Ok(None),
            Some(bytes) => Ok(Some(parse_snapshot(&bytes)?)),
        }
    }
}

const fn corrupt(detail: String) -> NodeError {
    NodeError::CorruptStore { detail }
}

fn parse_hard_state(bytes: &[u8]) -> Result<HardState, NodeError> {
    if bytes.len() < 14 || &bytes[0..4] != HARD_MAGIC {
        return Err(corrupt(format!(
            "hard state record: bad magic or length {}",
            bytes.len()
        )));
    }
    let (payload, stored, computed, total) = split_record(HARD_DOMAIN, bytes)
        .ok_or_else(|| corrupt("hard state record: incomplete header".to_string()))?;
    if stored != computed || total != bytes.len() {
        return Err(corrupt(format!(
            "hard state record: CRC stored {stored:#010x}, computed {computed:#010x}"
        )));
    }
    if payload.len() < 9 {
        return Err(corrupt("hard state record: payload too short".to_string()));
    }
    let term = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    let voted_for = match payload[8] {
        0 => None,
        1 if payload.len() >= 17 => Some(NodeId(u64::from_le_bytes(
            payload[9..17].try_into().unwrap_or([0; 8]),
        ))),
        found => {
            return Err(corrupt(format!(
                "hard state record: bad voted-for tag {found}"
            )));
        }
    };
    Ok(HardState {
        current_term: Term(term),
        voted_for,
    })
}

fn parse_snapshot(bytes: &[u8]) -> Result<(SnapshotMeta, Vec<u8>), NodeError> {
    if bytes.len() < 14 || &bytes[0..4] != SNAP_MAGIC {
        return Err(corrupt(format!(
            "snapshot record: bad magic or length {}",
            bytes.len()
        )));
    }
    let (payload, stored, computed, total) = split_record(SNAP_DOMAIN, bytes)
        .ok_or_else(|| corrupt("snapshot record: incomplete header".to_string()))?;
    if stored != computed || total != bytes.len() {
        return Err(corrupt(format!(
            "snapshot record: CRC stored {stored:#010x}, computed {computed:#010x}"
        )));
    }
    if payload.len() < 20 {
        return Err(corrupt("snapshot record: payload too short".to_string()));
    }
    let index = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
    let term = u64::from_le_bytes(payload[8..16].try_into().unwrap_or([0; 8]));
    let config_len = u32::from_le_bytes(payload[16..20].try_into().unwrap_or([0; 4])) as usize;
    if payload.len() < 20 + config_len {
        return Err(corrupt(
            "snapshot record: config runs past the payload".to_string(),
        ));
    }
    let config = decode_config_bytes(&payload[20..20 + config_len])
        .map_err(|e| corrupt(format!("snapshot record: bad config body: {e}")))?;
    Ok((
        SnapshotMeta {
            last_included_index: LogIndex(index),
            last_included_term: Term(term),
            config,
        },
        payload[20 + config_len..].to_vec(),
    ))
}

/// The outcome of scanning the log file at recovery.
struct LogRecovery {
    entries: Vec<LogEntry>,
    records: Vec<(u64, u64, u64)>,
    torn: Option<TornReport>,
}

fn recover_log(path: &Path, base_index: u64) -> Result<LogRecovery, NodeError> {
    let Some(bytes) = read_file(path)? else {
        return Ok(LogRecovery {
            entries: Vec::new(),
            records: Vec::new(),
            torn: None,
        });
    };
    let mut entries = Vec::new();
    let mut records = Vec::new();
    let mut torn = None;
    let mut offset = 0_usize;
    let mut expected_index = base_index + 1;
    while offset < bytes.len() {
        let rest = &bytes[offset..];
        if rest.len() < 14 || &rest[0..4] != LOG_MAGIC {
            torn = Some(TornReport {
                byte_offset: u64::try_from(offset).unwrap_or(u64::MAX),
                index: expected_index,
                expected_crc: 0,
                actual_crc: 0,
            });
            break;
        }
        let Some((payload, stored, computed, total)) = split_record(LOG_DOMAIN, rest) else {
            torn = Some(TornReport {
                byte_offset: u64::try_from(offset).unwrap_or(u64::MAX),
                index: expected_index,
                expected_crc: 0,
                actual_crc: 0,
            });
            break;
        };
        if stored != computed || payload.len() < 8 {
            torn = Some(TornReport {
                byte_offset: u64::try_from(offset).unwrap_or(u64::MAX),
                index: expected_index,
                expected_crc: stored,
                actual_crc: computed,
            });
            break;
        }
        let index = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
        if index <= base_index {
            // A leftover from before the snapshot (a crash
            // between the snapshot write and the log rewrite):
            // it stays in the record map so offsets stay
            // truthful, but it is not a recovered entry.
            records.push((
                index,
                u64::try_from(offset).unwrap_or(u64::MAX),
                u64::try_from(total).unwrap_or(u64::MAX),
            ));
            offset += total;
            continue;
        }
        if index != expected_index {
            torn = Some(TornReport {
                byte_offset: u64::try_from(offset).unwrap_or(u64::MAX),
                index,
                expected_crc: stored,
                actual_crc: computed,
            });
            break;
        }
        let Ok(entry) = decode_entry_bytes(&payload[8..]) else {
            torn = Some(TornReport {
                byte_offset: u64::try_from(offset).unwrap_or(u64::MAX),
                index,
                expected_crc: stored,
                actual_crc: computed,
            });
            break;
        };
        entries.push(entry);
        records.push((
            index,
            u64::try_from(offset).unwrap_or(u64::MAX),
            u64::try_from(total).unwrap_or(u64::MAX),
        ));
        expected_index += 1;
        offset += total;
    }
    if torn.is_some() {
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|e| io(path, e))?;
        file.set_len(u64::try_from(offset).unwrap_or(u64::MAX))
            .map_err(|e| io(path, e))?;
        file.sync_all().map_err(|e| io(path, e))?;
    }
    Ok(LogRecovery {
        entries,
        records,
        torn,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use simraft_core::{ClusterConfig, EntryKind};

    fn entry(term: u64) -> LogEntry {
        LogEntry {
            term: Term(term),
            kind: EntryKind::NoOp,
        }
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("simraft-node-store-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn persist_and_recover_round_trips() {
        let dir = temp_dir("roundtrip");
        let (mut store, recovery) = FileStore::open(&dir).expect("open a fresh store");
        assert_eq!(recovery.hard_state.current_term, Term(0));
        store
            .write_hard_state(Term(3), Some(NodeId(2)))
            .expect("write hard state");
        store
            .persist_entries(LogIndex(1), &[entry(1), entry(1), entry(2)])
            .expect("append entries");
        store
            .persist_entries(LogIndex(3), &[entry(3)])
            .expect("overwrite the tail");
        drop(store);
        let (_store, recovery) = FileStore::open(&dir).expect("reopen");
        assert_eq!(recovery.hard_state.current_term, Term(3));
        assert_eq!(recovery.hard_state.voted_for, Some(NodeId(2)));
        assert_eq!(recovery.entries.len(), 3);
        assert_eq!(
            recovery.entries[2].term,
            Term(3),
            "the tail was overwritten"
        );
        assert!(recovery.torn.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_is_truncated_and_reported() {
        let dir = temp_dir("torn");
        let (mut store, _) = FileStore::open(&dir).expect("open a fresh store");
        store
            .persist_entries(LogIndex(1), &[entry(1), entry(1)])
            .expect("append entries");
        drop(store);
        // Simulate a crash mid-append: garbage at the tail.
        let log_path = dir.join("log");
        let mut file = OpenOptions::new()
            .append(true)
            .open(&log_path)
            .expect("open the log");
        file.write_all(b"SRLG\x01\x00\xff\xff")
            .expect("write garbage");
        drop(file);
        let (_store, recovery) = FileStore::open(&dir).expect("reopen");
        assert_eq!(recovery.entries.len(), 2, "the good prefix survives");
        let torn = recovery.torn.expect("the torn tail is reported");
        assert_eq!(torn.index, 3, "the report names the expected index");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_compacts_the_log() {
        let dir = temp_dir("snapshot");
        let (mut store, _) = FileStore::open(&dir).expect("open a fresh store");
        store
            .persist_entries(LogIndex(1), &[entry(1), entry(1), entry(1), entry(1)])
            .expect("append entries");
        let config =
            ClusterConfig::new([NodeId(1), NodeId(2)], Vec::new()).expect("config validates");
        let meta = SnapshotMeta {
            last_included_index: LogIndex(3),
            last_included_term: Term(1),
            config,
        };
        store
            .write_snapshot(&meta, b"payload-bytes")
            .expect("write the snapshot");
        drop(store);
        let (_store, recovery) = FileStore::open(&dir).expect("reopen");
        let (found_meta, data) = recovery.snapshot.expect("the snapshot recovers");
        assert_eq!(found_meta.last_included_index, LogIndex(3));
        assert_eq!(data, b"payload-bytes");
        assert_eq!(
            recovery.entries.len(),
            1,
            "only the tail past the snapshot remains"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
