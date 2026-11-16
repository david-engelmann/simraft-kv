//! The section 7.1 message frame codec.
//!
//! The simulator's packets carry real frame bytes, so the
//! simulation tests the wire format and not structured shortcuts
//! (docs/ARCHITECTURE.md section 12.2). Layout: magic "SRKV",
//! version `u16`, kind `u8`, flags `u8`, term `u64`, from `u64`,
//! to `u64`, `body_len` `u32`, body, and a CRC-32 IEEE over every
//! preceding byte with the domain prefix "SRKV/frame/v1"
//! prepended to the CRC input only.
//!
//! Log entries inside `AppendEntries` bodies use the section 7.2
//! entry encoding; a Command entry's payload is the canonical
//! simraft-kv command encoding, carried with its length so the
//! frame stays faithful to the core's opaque-payload boundary.

use std::fmt;

use simraft_core::{ClusterConfig, EntryKind, LogEntry, LogIndex, Message, NodeId, Term};

/// The frame format version this codec speaks.
pub const FRAME_VERSION: u16 = 1;

const MAGIC: &[u8; 4] = b"SRKV";
const CRC_DOMAIN: &[u8] = b"SRKV/frame/v1";
const HEADER_LEN: usize = 36;

/// A frame failed to decode; every variant names the deciding
/// values (invariant E-01's discipline, applied to the wire).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
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

fn encode_config(out: &mut Vec<u8>, config: &ClusterConfig) {
    push_u32(out, u32::try_from(config.voters.len()).unwrap_or(u32::MAX));
    for voter in &config.voters {
        push_u64(out, voter.0);
    }
    push_u32(
        out,
        u32::try_from(config.learners.len()).unwrap_or(u32::MAX),
    );
    for learner in &config.learners {
        push_u64(out, learner.0);
    }
}

fn encode_entry(out: &mut Vec<u8>, entry: &LogEntry) {
    push_u64(out, entry.term.0);
    match &entry.kind {
        EntryKind::NoOp => out.push(0),
        EntryKind::Command {
            client_id,
            seq,
            payload,
        } => {
            out.push(1);
            push_u64(out, client_id.0);
            push_u64(out, *seq);
            push_u32(out, u32::try_from(payload.len()).unwrap_or(u32::MAX));
            out.extend_from_slice(payload);
        }
        EntryKind::Config { config } => {
            out.push(2);
            encode_config(out, config);
        }
    }
}

fn encode_body(msg: &Message) -> Vec<u8> {
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
            push_u64(&mut out, prev_log_index.0);
            push_u64(&mut out, prev_log_term.0);
            push_u64(&mut out, leader_commit.0);
            push_u64(&mut out, *read_round);
            push_u32(&mut out, u32::try_from(entries.len()).unwrap_or(u32::MAX));
            for entry in entries {
                encode_entry(&mut out, entry);
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
            push_u64(&mut out, last_included_index.0);
            push_u64(&mut out, last_included_term.0);
            encode_config(&mut out, config);
            push_u64(&mut out, *offset);
            push_u32(&mut out, u32::try_from(data.len()).unwrap_or(u32::MAX));
            out.extend_from_slice(data);
            out.push(u8::from(*done));
        }
        Message::InstallSnapshotResponse { match_index, .. } => {
            push_u64(&mut out, match_index.0);
        }
    }
    out
}

/// Encode one message into a complete frame addressed from `from`
/// to `to`.
#[must_use]
pub fn encode_frame(from: NodeId, to: NodeId, msg: &Message) -> Vec<u8> {
    let body = encode_body(msg);
    let mut out = Vec::with_capacity(HEADER_LEN + body.len() + 4);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    out.push(kind_byte(msg));
    out.push(0);
    push_u64(&mut out, term_of(msg).0);
    push_u64(&mut out, from.0);
    push_u64(&mut out, to.0);
    push_u32(&mut out, u32::try_from(body.len()).unwrap_or(u32::MAX));
    out.extend_from_slice(&body);
    let crc = crc32(&out);
    push_u32(&mut out, crc);
    out
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

/// Decode a complete frame into (from, to, message).
///
/// # Errors
/// Returns a `WireError` naming the failure and its values when the
/// frame is malformed in any way.
pub fn decode_frame(bytes: &[u8]) -> Result<(NodeId, NodeId, Message), WireError> {
    if bytes.len() < HEADER_LEN + 4 {
        return Err(WireError::Truncated {
            offset: bytes.len(),
            needed: HEADER_LEN + 4,
            remaining: bytes.len(),
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
    let magic_bytes = head.take(4)?;
    let mut magic = [0_u8; 4];
    magic.copy_from_slice(magic_bytes);
    if &magic != MAGIC {
        return Err(WireError::BadMagic { found: magic });
    }
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
    let body_len = usize::try_from(head.u32()?).unwrap_or(usize::MAX);
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

/// Decode a frame body of the given kind at the given term.
///
/// # Errors
/// Returns a `WireError` when the body is malformed.
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
            let frame = encode_frame(NodeId(1), NodeId(2), &msg);
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
        );
        let mut corrupted = frame;
        corrupted[20] ^= 0xFF;
        assert!(matches!(
            decode_frame(&corrupted),
            Err(WireError::BadCrc { .. })
        ));
    }
}
