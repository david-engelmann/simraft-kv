//! Snapshot metadata and chunk transfer state.
//!
//! The core never interprets snapshot payload bytes (that is
//! simraft-kv's `snapshot_kv` layout, docs/ARCHITECTURE.md section
//! 7.4). It tracks identity and transfer progress only: which snapshot
//! a follower is buffering, how many bytes it holds, and which
//! snapshot a leader has loaded for relay to lagging followers.

use crate::ids::{LogIndex, Term};
use crate::membership::ClusterConfig;

/// The identity of a snapshot: the last log entry it covers and the
/// configuration in force at that entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    /// The highest log index included in the snapshot.
    pub last_included_index: LogIndex,
    /// The term of the entry at `last_included_index`.
    pub last_included_term: Term,
    /// The cluster configuration in force at `last_included_index`.
    pub config: ClusterConfig,
}

/// A follower's partially received snapshot. Chunks are accepted only
/// at the exact buffered length (invariant N-02); a chunk carrying a
/// different identity replaces this buffer wholesale.
#[derive(Debug, Clone)]
pub(crate) struct IncomingSnapshot {
    /// The identity of the snapshot being received.
    pub meta: SnapshotMeta,
    /// The bytes buffered so far, in offset order.
    pub data: Vec<u8>,
}

impl IncomingSnapshot {
    /// A fresh buffer for `meta` with no bytes yet.
    pub const fn new(meta: SnapshotMeta) -> Self {
        Self {
            meta,
            data: Vec::new(),
        }
    }

    /// The offset the next accepted chunk must carry.
    pub const fn expected_offset(&self) -> u64 {
        self.data.len() as u64
    }
}

/// A snapshot a node loaded from its own state machine, held by a
/// leader for relay to followers whose next index has fallen to or
/// below the snapshot base (invariant N-01), and by any node for its
/// own log compaction once the driver persists it.
#[derive(Debug, Clone)]
pub(crate) struct LoadedSnapshot {
    /// The identity of the loaded snapshot.
    pub meta: SnapshotMeta,
    /// The complete snapshot payload bytes.
    pub data: Vec<u8>,
}
