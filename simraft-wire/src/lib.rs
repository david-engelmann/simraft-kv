//! Canonical CRC-32 message frame codec for `simraft-kv`, shared
//! by the deterministic simulator (`simraft-sim`) and the TCP node
//! adapter (`simraft-node`).
//!
//! One codec serves both carriers of the wire format: simulator
//! packets carry real frame bytes, so the simulation tests the
//! wire format rather than structured shortcuts, and
//! `simraft-node`'s TCP transport carries the exact same bytes
//! between processes.
//!
//! Frame layout: magic `"SRKV"`, version `u16`, kind `u8`, flags
//! `u8`, term `u64`, from `u64`, to `u64`, `body_len` `u32`
//! (capped at [`MAX_FRAME_BODY_LEN`], 16 MiB), body, and a CRC-32
//! IEEE checksum over every preceding byte with the domain prefix
//! `"SRKV/frame/v1"` prepended to the CRC input.
//!
//! This crate forbids unsafe code.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;

use simraft_core::{ClusterConfig, EntryKind, LogEntry, LogIndex, Message, NodeId, Term};

/// The frame format version this codec speaks.
pub const FRAME_VERSION: u16 = 1;

/// The fixed frame header length in bytes (before the body and trailing CRC-32).
pub const HEADER_LEN: usize = 36;

/// Maximum permitted frame body length in bytes (16 MiB, docs/ARCHITECTURE.md section 7).
pub const MAX_FRAME_BODY_LEN: usize = 16 * 1024 * 1024;

/// Maximum permitted complete frame length in bytes (header + 16 MiB body + 4-byte CRC-32).
pub const MAX_FRAME_LEN: usize = HEADER_LEN + MAX_FRAME_BODY_LEN + 4;

const MAGIC: &[u8; 4] = b"SRKV";
const CRC_DOMAIN: &[u8] = b"SRKV/frame/v1";

/// A frame failed to encode or decode; every variant names the
/// deciding values (invariant E-01).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    /// A payload or frame body exceeded [`MAX_FRAME_BODY_LEN`] (16 MiB).
    PayloadTooLarge {
        /// The offending payload or body length in bytes.
        len: usize,
        /// The maximum allowed length in bytes.
        limit: usize,
    },
    /// The buffer is shorter than the frame structure requires.
    Truncated {
        /// The byte offset at which decoding ran out.
        offset: usize,
        /// The bytes the field needed.
        needed: usize,
        /// The bytes actually remaining.
        remaining: usize,
    },
    /// The magic bytes are not "SRKV".
    BadMagic {
        /// The four bytes found.
        found: [u8; 4],
    },
    /// The frame version is not 1.
    BadVersion {
        /// The version found.
        found: u16,
    },
    /// The flags byte is nonzero in version 1.
    BadFlags {
        /// The flags byte found.
        found: u8,
    },
    /// The message kind byte names no message.
    BadKind {
        /// The kind byte found.
        found: u8,
    },
    /// The CRC does not match the frame contents.
    BadCrc {
        /// The CRC stored in the frame.
        expected: u32,
        /// The CRC computed over the received bytes.
        actual: u32,
    },
    /// Bytes remain after the body is fully decoded.
    Trailing {
        /// The number of undecoded bytes.
        extra: usize,
    },
    /// A config body names overlapping or empty membership.
    BadConfig {
        /// What was wrong, with the values.
        detail: String,
    },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { len, limit } => write!(
                f,
                "frame payload length {len} bytes exceeds limit of {limit} bytes"
            ),
            Self::Truncated {
                offset,
                needed,
                remaining,
            } => write!(
                f,
                "frame truncated at offset {offset}: field needs {needed} bytes, {remaining} remain"
            ),
            Self::BadMagic { found } => {
                write!(f, "bad frame magic: found {found:?}, expected \"SRKV\"")
            }
            Self::BadVersion { found } => {
                write!(f, "bad frame version {found}, expected {FRAME_VERSION}")
            }
            Self::BadFlags { found } => {
                write!(f, "bad frame flags {found}, expected 0 in version 1")
            }
            Self::BadKind { found } => write!(f, "unknown message kind byte {found}"),
            Self::BadCrc { expected, actual } => write!(
                f,
                "frame CRC mismatch: stored {expected:#010x}, computed {actual:#010x}"
            ),
            Self::Trailing { extra } => {
                write!(f, "frame body has {extra} trailing undecoded bytes")
            }
            Self::BadConfig { detail } => write!(f, "bad config body: {detail}"),
        }
    }
}

impl std::error::Error for WireError {}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(CRC_DOMAIN);
    hasher.update(bytes);
    hasher.finalize()
}

const fn kind_byte(msg: &Message) -> u8 {
    match msg {
        Message::RequestVote { .. } => 1,
        Message::RequestVoteResponse { .. } => 2,
        Message::RequestPreVote { .. } => 3,
        Message::RequestPreVoteResponse { .. } => 4,
        Message::AppendEntries { .. } => 5,
        Message::AppendEntriesResponse { .. } => 6,
        Message::InstallSnapshot { .. } => 7,
        Message::InstallSnapshotResponse { .. } => 8,
    }
}

const fn term_of(msg: &Message) -> Term {
    match msg {
        Message::RequestVote { term, .. }
        | Message::RequestVoteResponse { term, .. }
        | Message::RequestPreVote { term, .. }
        | Message::RequestPreVoteResponse { term, .. }
        | Message::AppendEntries { term, .. }
        | Message::AppendEntriesResponse { term, .. }
        | Message::InstallSnapshot { term, .. }
        | Message::InstallSnapshotResponse { term, .. } => *term,
    }
}

fn push_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn check_len(len: usize) -> Result<u32, WireError> {
    if len > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len,
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    u32::try_from(len).map_err(|_| WireError::PayloadTooLarge {
        len,
        limit: MAX_FRAME_BODY_LEN,
    })
}

fn encode_config(out: &mut Vec<u8>, config: &ClusterConfig) -> Result<(), WireError> {
    let voter_count = check_len(config.voters.len())?;
    let learner_count = check_len(config.learners.len())?;
    let added = 8_usize
        .saturating_add(config.voters.len().saturating_mul(8))
        .saturating_add(config.learners.len().saturating_mul(8));
    let next_len = out.len().saturating_add(added);
    if next_len > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len: next_len,
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    push_u32(out, voter_count);
    for voter in &config.voters {
        push_u64(out, voter.0);
    }
    push_u32(out, learner_count);
    for learner in &config.learners {
        push_u64(out, learner.0);
    }
    Ok(())
}

fn encode_entry(out: &mut Vec<u8>, entry: &LogEntry) -> Result<(), WireError> {
    push_u64(out, entry.term.0);
    match &entry.kind {
        EntryKind::NoOp => out.push(0),
        EntryKind::Command {
            client_id,
            seq,
            payload,
        } => {
            let payload_len = check_len(payload.len())?;
            let next_len = out
                .len()
                .saturating_add(1 + 8 + 8 + 4)
                .saturating_add(payload.len());
            if next_len > MAX_FRAME_BODY_LEN {
                return Err(WireError::PayloadTooLarge {
                    len: next_len,
                    limit: MAX_FRAME_BODY_LEN,
                });
            }
            out.push(1);
            push_u64(out, client_id.0);
            push_u64(out, *seq);
            push_u32(out, payload_len);
            out.extend_from_slice(payload);
        }
        EntryKind::Config { config } => {
            out.push(2);
            encode_config(out, config)?;
        }
    }
    if out.len() > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len: out.len(),
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    Ok(())
}

/// Encode one log entry in the canonical entry encoding, for
/// carriers outside frames (durable log records).
///
/// # Errors
/// Returns `WireError::PayloadTooLarge` if the encoded entry exceeds
/// [`MAX_FRAME_BODY_LEN`] (16 MiB).
pub fn encode_entry_bytes(entry: &LogEntry) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::new();
    encode_entry(&mut out, entry)?;
    Ok(out)
}

/// Decode one log entry from its canonical encoding.
///
/// # Errors
/// Returns a `WireError` naming the failure when the bytes are
/// truncated, oversized, malformed, or have trailing content.
pub fn decode_entry_bytes(bytes: &[u8]) -> Result<LogEntry, WireError> {
    if bytes.len() > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    let mut reader = Reader::new(bytes);
    let entry = decode_entry(&mut reader)?;
    reader.finish()?;
    Ok(entry)
}

/// Encode a cluster configuration in the canonical config encoding,
/// for carriers outside frames (snapshot records).
///
/// # Errors
/// Returns `WireError::PayloadTooLarge` if the encoded configuration
/// exceeds [`MAX_FRAME_BODY_LEN`].
pub fn encode_config_bytes(config: &ClusterConfig) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::new();
    encode_config(&mut out, config)?;
    Ok(out)
}

/// Decode a cluster configuration from its canonical encoding.
///
/// # Errors
/// Returns a `WireError` naming the failure when the bytes are
/// truncated, oversized, malformed, or have trailing content.
pub fn decode_config_bytes(bytes: &[u8]) -> Result<ClusterConfig, WireError> {
    if bytes.len() > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len: bytes.len(),
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    let mut reader = Reader::new(bytes);
    let config = decode_config(&mut reader)?;
    reader.finish()?;
    Ok(config)
}

fn encode_body(msg: &Message) -> Result<Vec<u8>, WireError> {
    let mut out = Vec::new();
    match msg {
        Message::RequestVote {
            last_log_index,
            last_log_term,
            ..
        }
        | Message::RequestPreVote {
            last_log_index,
            last_log_term,
            ..
        } => {
            push_u64(&mut out, last_log_index.0);
            push_u64(&mut out, last_log_term.0);
        }
        Message::RequestVoteResponse { vote_granted, .. }
        | Message::RequestPreVoteResponse { vote_granted, .. } => {
            out.push(u8::from(*vote_granted));
        }
        Message::AppendEntries {
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            read_round,
            ..
        } => {
            let count = check_len(entries.len())?;
            push_u64(&mut out, prev_log_index.0);
            push_u64(&mut out, prev_log_term.0);
            push_u64(&mut out, leader_commit.0);
            push_u64(&mut out, *read_round);
            push_u32(&mut out, count);
            for entry in entries {
                encode_entry(&mut out, entry)?;
            }
        }
        Message::AppendEntriesResponse {
            success,
            match_index,
            conflict_index,
            conflict_term,
            read_round,
            ..
        } => {
            out.push(u8::from(*success));
            push_u64(&mut out, match_index.0);
            push_u64(&mut out, conflict_index.0);
            push_u64(&mut out, conflict_term.0);
            push_u64(&mut out, *read_round);
        }
        Message::InstallSnapshot {
            last_included_index,
            last_included_term,
            config,
            offset,
            data,
            done,
            ..
        } => {
            let chunk_len = check_len(data.len())?;
            push_u64(&mut out, last_included_index.0);
            push_u64(&mut out, last_included_term.0);
            encode_config(&mut out, config)?;
            let next_len = out
                .len()
                .saturating_add(8 + 4 + 1)
                .saturating_add(data.len());
            if next_len > MAX_FRAME_BODY_LEN {
                return Err(WireError::PayloadTooLarge {
                    len: next_len,
                    limit: MAX_FRAME_BODY_LEN,
                });
            }
            push_u64(&mut out, *offset);
            push_u32(&mut out, chunk_len);
            out.extend_from_slice(data);
            out.push(u8::from(*done));
        }
        Message::InstallSnapshotResponse { match_index, .. } => {
            push_u64(&mut out, match_index.0);
        }
    }
    if out.len() > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len: out.len(),
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    Ok(out)
}

/// Encode one message into a complete frame addressed from `from`
/// to `to`.
///
/// # Errors
/// Returns `WireError::PayloadTooLarge` naming both the body length
/// and [`MAX_FRAME_BODY_LEN`] when the encoded body exceeds 16 MiB.
pub fn encode_frame(from: NodeId, to: NodeId, msg: &Message) -> Result<Vec<u8>, WireError> {
    let body = encode_body(msg)?;
    let body_len = check_len(body.len())?;
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + 4);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    out.push(kind_byte(msg));
    out.push(0);
    push_u64(&mut out, term_of(msg).0);
    push_u64(&mut out, from.0);
    push_u64(&mut out, to.0);
    push_u32(&mut out, body_len);
    out.extend_from_slice(&body);
    let crc = crc32(&out);
    push_u32(&mut out, crc);
    Ok(out)
}

/// A byte cursor over a frame body.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let remaining = self.buf.len().saturating_sub(self.pos);
        if remaining < n {
            return Err(WireError::Truncated {
                offset: self.pos,
                needed: n,
                remaining,
            });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    const fn finish(&self) -> Result<(), WireError> {
        let extra = self.buf.len().saturating_sub(self.pos);
        if extra > 0 {
            return Err(WireError::Trailing { extra });
        }
        Ok(())
    }
}

fn decode_config(r: &mut Reader<'_>) -> Result<ClusterConfig, WireError> {
    let voter_count = r.u32()?;
    let mut voters = Vec::new();
    for _ in 0..voter_count {
        voters.push(NodeId(r.u64()?));
    }
    let learner_count = r.u32()?;
    let mut learners = Vec::new();
    for _ in 0..learner_count {
        learners.push(NodeId(r.u64()?));
    }
    ClusterConfig::new(voters, learners).map_err(|e| WireError::BadConfig {
        detail: e.to_string(),
    })
}

fn decode_entry(r: &mut Reader<'_>) -> Result<LogEntry, WireError> {
    let term = Term(r.u64()?);
    let kind = match r.u8()? {
        0 => EntryKind::NoOp,
        1 => {
            let client_id = simraft_core::ClientId(r.u64()?);
            let seq = r.u64()?;
            let len = usize::try_from(r.u32()?).unwrap_or(usize::MAX);
            if len > MAX_FRAME_BODY_LEN {
                return Err(WireError::PayloadTooLarge {
                    len,
                    limit: MAX_FRAME_BODY_LEN,
                });
            }
            let payload = r.take(len)?.to_vec();
            EntryKind::Command {
                client_id,
                seq,
                payload,
            }
        }
        2 => EntryKind::Config {
            config: decode_config(r)?,
        },
        found => {
            return Err(WireError::BadKind { found });
        }
    };
    Ok(LogEntry { term, kind })
}

/// Inspect the leading bytes of a stream buffer to determine the
/// total frame length, rejecting bad magic or oversized `body_len`
/// values before buffering the body.
///
/// Returns `None` when fewer than 4 bytes (or fewer than
/// [`HEADER_LEN`] bytes after valid magic) have arrived.
#[must_use]
pub fn inspect_frame_header(bytes: &[u8]) -> Option<Result<usize, WireError>> {
    if bytes.len() < 4 {
        return None;
    }
    let found = [bytes[0], bytes[1], bytes[2], bytes[3]];
    if &found != MAGIC {
        return Some(Err(WireError::BadMagic { found }));
    }
    if bytes.len() < HEADER_LEN {
        return None;
    }
    let body_len = u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]) as usize;
    if body_len > MAX_FRAME_BODY_LEN {
        return Some(Err(WireError::PayloadTooLarge {
            len: body_len,
            limit: MAX_FRAME_BODY_LEN,
        }));
    }
    Some(Ok(HEADER_LEN + body_len + 4))
}

/// The total length in bytes of the frame whose header begins
/// `bytes`, when valid and within [`MAX_FRAME_BODY_LEN`].
#[must_use]
pub fn frame_len(bytes: &[u8]) -> Option<usize> {
    inspect_frame_header(bytes)?.ok()
}

/// Decode a complete frame into `(from, to, message)`.
///
/// # Errors
/// Returns a `WireError` naming the failure and its values when the
/// frame is oversized, truncated, corrupt, or malformed.
pub fn decode_frame(bytes: &[u8]) -> Result<(NodeId, NodeId, Message), WireError> {
    if bytes.len() < HEADER_LEN + 4 {
        return Err(WireError::Truncated {
            offset: bytes.len(),
            needed: HEADER_LEN + 4,
            remaining: bytes.len(),
        });
    }
    if bytes.len() > MAX_FRAME_LEN {
        return Err(WireError::PayloadTooLarge {
            len: bytes.len() - HEADER_LEN - 4,
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    let mut magic = [0_u8; 4];
    magic.copy_from_slice(&bytes[..4]);
    if &magic != MAGIC {
        return Err(WireError::BadMagic { found: magic });
    }
    let body_len = usize::try_from(u32::from_le_bytes([
        bytes[32], bytes[33], bytes[34], bytes[35],
    ]))
    .unwrap_or(usize::MAX);
    if body_len > MAX_FRAME_BODY_LEN {
        return Err(WireError::PayloadTooLarge {
            len: body_len,
            limit: MAX_FRAME_BODY_LEN,
        });
    }
    let stored_crc = u32::from_le_bytes([
        bytes[bytes.len() - 4],
        bytes[bytes.len() - 3],
        bytes[bytes.len() - 2],
        bytes[bytes.len() - 1],
    ]);
    let computed_crc = crc32(&bytes[..bytes.len() - 4]);
    if stored_crc != computed_crc {
        return Err(WireError::BadCrc {
            expected: stored_crc,
            actual: computed_crc,
        });
    }
    let mut head = Reader::new(&bytes[..HEADER_LEN]);
    let _ = head.take(4)?;
    let version_bytes = head.take(2)?;
    let version = u16::from_le_bytes([version_bytes[0], version_bytes[1]]);
    if version != FRAME_VERSION {
        return Err(WireError::BadVersion { found: version });
    }
    let kind = head.u8()?;
    let flags = head.u8()?;
    if flags != 0 {
        return Err(WireError::BadFlags { found: flags });
    }
    let term = Term(head.u64()?);
    let from = NodeId(head.u64()?);
    let to = NodeId(head.u64()?);
    let _ = head.u32()?;
    let body = &bytes[HEADER_LEN..bytes.len() - 4];
    if body.len() != body_len {
        return Err(WireError::Truncated {
            offset: HEADER_LEN,
            needed: body_len,
            remaining: body.len(),
        });
    }
    let msg = decode_body(kind, term, body)?;
    Ok((from, to, msg))
}

fn decode_body(kind: u8, term: Term, body: &[u8]) -> Result<Message, WireError> {
    let mut r = Reader::new(body);
    let msg = match kind {
        1 => Message::RequestVote {
            term,
            last_log_index: LogIndex(r.u64()?),
            last_log_term: Term(r.u64()?),
        },
        2 => Message::RequestVoteResponse {
            term,
            vote_granted: r.u8()? != 0,
        },
        3 => Message::RequestPreVote {
            term,
            last_log_index: LogIndex(r.u64()?),
            last_log_term: Term(r.u64()?),
        },
        4 => Message::RequestPreVoteResponse {
            term,
            vote_granted: r.u8()? != 0,
        },
        5 => {
            let prev_log_index = LogIndex(r.u64()?);
            let prev_log_term = Term(r.u64()?);
            let leader_commit = LogIndex(r.u64()?);
            let read_round = r.u64()?;
            let count = r.u32()?;
            let mut entries = Vec::new();
            for _ in 0..count {
                entries.push(decode_entry(&mut r)?);
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_round,
            }
        }
        6 => Message::AppendEntriesResponse {
            term,
            success: r.u8()? != 0,
            match_index: LogIndex(r.u64()?),
            conflict_index: LogIndex(r.u64()?),
            conflict_term: Term(r.u64()?),
            read_round: r.u64()?,
        },
        7 => {
            let last_included_index = LogIndex(r.u64()?);
            let last_included_term = Term(r.u64()?);
            let config = decode_config(&mut r)?;
            let offset = r.u64()?;
            let len = usize::try_from(r.u32()?).unwrap_or(usize::MAX);
            if len > MAX_FRAME_BODY_LEN {
                return Err(WireError::PayloadTooLarge {
                    len,
                    limit: MAX_FRAME_BODY_LEN,
                });
            }
            let data = r.take(len)?.to_vec();
            let done = r.u8()? != 0;
            Message::InstallSnapshot {
                term,
                last_included_index,
                last_included_term,
                config,
                offset,
                data,
                done,
            }
        }
        8 => Message::InstallSnapshotResponse {
            term,
            match_index: LogIndex(r.u64()?),
        },
        found => return Err(WireError::BadKind { found }),
    };
    r.finish()?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use simraft_core::ClusterConfig;

    #[test]
    fn frame_len_reads_the_header_and_rejects_oversized_body_len() {
        let msg = Message::RequestVote {
            term: Term(2),
            last_log_index: LogIndex(9),
            last_log_term: Term(1),
        };
        let frame = encode_frame(NodeId(1), NodeId(2), &msg).expect("encodes");
        assert_eq!(frame_len(&frame), Some(frame.len()));
        assert_eq!(
            frame_len(&frame[..10]),
            None,
            "a partial header has no length"
        );
        let mut bad = frame.clone();
        bad[0] = b'X';
        assert_eq!(frame_len(&bad), None, "a wrong magic has no length");

        let mut huge = frame;
        huge[32..36].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            frame_len(&huge),
            None,
            "a 4 GiB body_len header must not be accepted by frame_len"
        );
        assert!(matches!(
            inspect_frame_header(&huge),
            Some(Err(WireError::PayloadTooLarge {
                limit: MAX_FRAME_BODY_LEN,
                ..
            }))
        ));
    }

    #[test]
    fn oversized_frame_encoding_is_rejected_with_length_and_limit() {
        let cfg = ClusterConfig::new([NodeId(1)], Vec::new()).expect("validates");
        let msg = Message::InstallSnapshot {
            term: Term(1),
            last_included_index: LogIndex(1),
            last_included_term: Term(1),
            config: cfg,
            offset: 0,
            data: vec![0_u8; MAX_FRAME_BODY_LEN + 1],
            done: true,
        };
        let err = encode_frame(NodeId(1), NodeId(2), &msg).expect_err("must reject > 16 MiB");
        match &err {
            WireError::PayloadTooLarge { len, limit } => {
                assert!(*len > MAX_FRAME_BODY_LEN);
                assert_eq!(*limit, MAX_FRAME_BODY_LEN);
            }
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
        let text = err.to_string();
        assert!(
            text.contains(&MAX_FRAME_BODY_LEN.to_string()),
            "got: {text}"
        );
    }

    #[test]
    fn every_message_kind_round_trips() {
        let cfg = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], [NodeId(4)])
            .expect("config validates");
        let messages = vec![
            Message::RequestVote {
                term: Term(2),
                last_log_index: LogIndex(9),
                last_log_term: Term(1),
            },
            Message::RequestVoteResponse {
                term: Term(2),
                vote_granted: true,
            },
            Message::RequestPreVote {
                term: Term(3),
                last_log_index: LogIndex(9),
                last_log_term: Term(1),
            },
            Message::RequestPreVoteResponse {
                term: Term(2),
                vote_granted: false,
            },
            Message::AppendEntries {
                term: Term(2),
                prev_log_index: LogIndex(4),
                prev_log_term: Term(1),
                entries: vec![
                    LogEntry {
                        term: Term(2),
                        kind: EntryKind::NoOp,
                    },
                    LogEntry {
                        term: Term(2),
                        kind: EntryKind::Config {
                            config: cfg.clone(),
                        },
                    },
                ],
                leader_commit: LogIndex(3),
                read_round: 7,
            },
            Message::AppendEntriesResponse {
                term: Term(2),
                success: true,
                match_index: LogIndex(6),
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: 7,
            },
            Message::InstallSnapshot {
                term: Term(2),
                last_included_index: LogIndex(11),
                last_included_term: Term(1),
                config: cfg,
                offset: 0,
                data: vec![1, 2, 3],
                done: true,
            },
            Message::InstallSnapshotResponse {
                term: Term(2),
                match_index: LogIndex(11),
            },
        ];
        for msg in messages {
            let frame = encode_frame(NodeId(1), NodeId(2), &msg).expect("frame encodes");
            let (from, to, back) = decode_frame(&frame).expect("frame decodes");
            assert_eq!((from, to), (NodeId(1), NodeId(2)));
            assert_eq!(back, msg);
        }
    }

    #[test]
    fn a_single_byte_flip_fails_the_crc() {
        let frame = encode_frame(
            NodeId(1),
            NodeId(2),
            &Message::RequestVoteResponse {
                term: Term(1),
                vote_granted: true,
            },
        )
        .expect("frame encodes");
        let mut corrupted = frame;
        corrupted[20] ^= 0xFF;
        assert!(matches!(
            decode_frame(&corrupted),
            Err(WireError::BadCrc { .. })
        ));
    }
}
