//! Domain error types for simraft-kv.
//!
//! Every error names the offending values: byte offsets and expected
//! versus actual fields for decoding, payload lengths and bounds for
//! size caps, and the client and exact expected and received sequence
//! numbers for session ordering (invariant E-01).

use core::fmt;
use simraft_core::{ClientId, LogIndex};

/// An error from the KV payload codec or the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    /// A payload or encoded field exceeded `MAX_PAYLOAD_BYTES` (16 MiB).
    PayloadTooLarge {
        /// The offending payload or field length in bytes.
        length: usize,
        /// The maximum permitted payload length in bytes.
        limit: usize,
    },
    /// Extra undecoded bytes remained after a complete payload.
    TrailingBytes {
        /// The byte offset at which trailing data began.
        offset: usize,
        /// How many undecoded bytes remained.
        extra: usize,
    },
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
    /// A snapshot payload carried an invalid cluster configuration
    /// (such as overlapping voter and learner sets).
    SnapshotInvalidConfig {
        /// The underlying configuration error text.
        detail: String,
    },
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { length, limit } => write!(
                f,
                "payload length {length} bytes exceeds limit of {limit} bytes"
            ),
            Self::TrailingBytes { offset, extra } => write!(
                f,
                "payload has {extra} trailing undecoded bytes starting at byte {offset}"
            ),
            Self::Truncated {
                offset,
                field,
                needed,
                available,
            } => write!(
                f,
                "payload truncated at byte {offset}: field {field} needs {needed} bytes, \
                 {available} remain"
            ),
            Self::UnknownTag {
                offset,
                tag,
                context,
            } => write!(
                f,
                "unknown {context} tag {tag} at byte {offset}; this schema version assigns \
                 only the documented values"
            ),
            Self::SessionGap {
                client_id,
                expected,
                received,
            } => write!(
                f,
                "session gap for {client_id}: expected seq {expected}, received seq {received}; \
                 the command was not executed"
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
            Self::SnapshotInvalidConfig { detail } => {
                write!(f, "snapshot configuration is invalid: {detail}")
            }
        }
    }
}

impl std::error::Error for KvError {}
