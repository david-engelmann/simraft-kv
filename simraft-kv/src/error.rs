//! Domain error types for simraft-kv.
//!
//! Every error names the offending values: byte offsets and expected
//! versus actual fields for decoding, the client and the exact expected
//! and received sequence numbers for session ordering (invariant E-01).

use core::fmt;
use simraft_core::{ClientId, LogIndex};

/// An error from the KV payload codec or the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    /// A payload ended before the field at `offset` was complete.
    Truncated {
        /// The byte offset at which decoding ran out of input.
        offset: usize,
        /// The field that was being read.
        field: &'static str,
        /// How many bytes the field needed.
        needed: usize,
        /// How many bytes remained.
        available: usize,
    },
    /// A tag byte carried a value this schema version does not assign.
    UnknownTag {
        /// The byte offset of the tag.
        offset: usize,
        /// The tag value found.
        tag: u8,
        /// What the tag encodes (for example "command result").
        context: &'static str,
    },
    /// A proposal skipped sequence numbers for its client. The command
    /// was not executed and the session did not advance.
    SessionGap {
        /// The client whose sequence skipped.
        client_id: ClientId,
        /// The sequence number that would have been accepted.
        expected: u64,
        /// The sequence number that arrived.
        received: u64,
    },
    /// Entries were offered to the state machine out of apply order.
    ApplyGap {
        /// The index the state machine expected next.
        expected: LogIndex,
        /// The index it was offered.
        received: LogIndex,
    },
    /// A snapshot's trailing SHA-256 digest does not match its
    /// payload. The snapshot is rejected whole and never installed
    /// (invariant N-03).
    SnapshotDigest {
        /// The digest stored in the snapshot (hex).
        expected: String,
        /// The digest computed over the received payload (hex).
        actual: String,
    },
    /// A snapshot payload did not start with the "SRSN" magic.
    BadSnapshotMagic {
        /// The first four bytes found, lossy-decoded.
        found: String,
    },
    /// A snapshot payload carried a configuration with no voters.
    SnapshotEmptyVoters,
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated {
                offset,
                field,
                needed,
                available,
            } => write!(
                f,
                "payload truncated at byte {offset}: field {field} needs {needed} bytes,                  {available} remain"
            ),
            Self::UnknownTag {
                offset,
                tag,
                context,
            } => write!(
                f,
                "unknown {context} tag {tag} at byte {offset}; this schema version assigns                  only the documented values"
            ),
            Self::SessionGap {
                client_id,
                expected,
                received,
            } => write!(
                f,
                "session gap for {client_id}: expected seq {expected}, received seq {received};                  the command was not executed"
            ),
            Self::ApplyGap { expected, received } => write!(
                f,
                "apply order violation: expected {expected}, received {received}"
            ),
            Self::SnapshotDigest { expected, actual } => write!(
                f,
                "snapshot digest mismatch: stored {expected}, computed {actual}; \
                 the snapshot is rejected whole"
            ),
            Self::BadSnapshotMagic { found } => {
                write!(
                    f,
                    "snapshot payload starts with {found:?}, expected \"SRSN\""
                )
            }
            Self::SnapshotEmptyVoters => {
                write!(f, "snapshot configuration has no voters")
            }
        }
    }
}

impl std::error::Error for KvError {}
