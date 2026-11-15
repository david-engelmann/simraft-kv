//! The canonical command payload codec (docs/ARCHITECTURE.md 7.2).
//!
//! Log entries carry command payloads in exactly this encoding, so two
//! encodings of the same command compare byte equal:
//!
//! ```text
//! client_id u64, seq u64, op_kind u8 (0 = Put, 1 = Delete),
//! key_len u32, key bytes, value_len u32 plus value bytes (Put only)
//! ```
//!
//! Command results use the section 7.2 result encoding: tag 0 = Ack
//! (no payload), 1 = Value (`value_len` u32 plus bytes), 2 = Error
//! (`error_code` u32, `message_len` u32, message UTF-8 bytes). All integers
//! are little-endian.

use crate::error::KvError;
use simraft_core::ClientId;

/// A key-value operation carried by a command payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvOp {
    /// Set `key` to `value`.
    Put {
        /// The key.
        key: Vec<u8>,
        /// The value.
        value: Vec<u8>,
    },
    /// Remove `key`. Deleting a missing key succeeds and changes
    /// nothing.
    Delete {
        /// The key.
        key: Vec<u8>,
    },
}

/// The outcome of executing a command, in the canonical result
/// encoding. Cached per session for deduplication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandResult {
    /// The command applied (Put and Delete outcomes).
    Ack,
    /// A read-back value carried as a command outcome.
    Value(Vec<u8>),
    /// The command was rejected without applying.
    Error {
        /// A stable numeric code from `crate::machine` (never reused).
        code: u32,
        /// A human-readable explanation.
        message: String,
    },
}

/// Encode a command payload for client `client_id` with per-client
/// sequence number `seq`.
#[must_use]
pub fn encode_command(client_id: ClientId, seq: u64, op: &KvOp) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&client_id.0.to_le_bytes());
    out.extend_from_slice(&seq.to_le_bytes());
    match op {
        KvOp::Put { key, value } => {
            out.push(0);
            push_bytes(&mut out, key);
            push_bytes(&mut out, value);
        }
        KvOp::Delete { key } => {
            out.push(1);
            push_bytes(&mut out, key);
        }
    }
    out
}

/// Decode a command payload.
///
/// # Errors
///
/// Returns `KvError::Truncated` naming the field and offset when the
/// payload ends early, and `KvError::UnknownTag` for an unassigned
/// operation kind.
pub fn decode_command(payload: &[u8]) -> Result<(ClientId, u64, KvOp), KvError> {
    let mut reader = Reader {
        buf: payload,
        pos: 0,
    };
    let client_id = ClientId(reader.u64("client_id")?);
    let seq = reader.u64("seq")?;
    let kind_offset = reader.pos;
    let kind = reader.u8("op_kind")?;
    let op = match kind {
        0 => {
            let key = reader.bytes("key")?;
            let value = reader.bytes("value")?;
            KvOp::Put { key, value }
        }
        1 => KvOp::Delete {
            key: reader.bytes("key")?,
        },
        tag => {
            return Err(KvError::UnknownTag {
                offset: kind_offset,
                tag,
                context: "command op kind",
            });
        }
    };
    Ok((client_id, seq, op))
}

/// Encode a command result in the section 7.2 result encoding.
#[must_use]
pub fn encode_result(result: &CommandResult) -> Vec<u8> {
    let mut out = Vec::new();
    match result {
        CommandResult::Ack => out.push(0),
        CommandResult::Value(value) => {
            out.push(1);
            push_bytes(&mut out, value);
        }
        CommandResult::Error { code, message } => {
            out.push(2);
            out.extend_from_slice(&code.to_le_bytes());
            push_bytes(&mut out, message.as_bytes());
        }
    }
    out
}

/// Decode a command result.
///
/// # Errors
///
/// Returns `KvError::Truncated` or `KvError::UnknownTag` for malformed
/// input, naming the offset.
pub fn decode_result(payload: &[u8]) -> Result<CommandResult, KvError> {
    let mut reader = Reader {
        buf: payload,
        pos: 0,
    };
    let tag_offset = reader.pos;
    let tag = reader.u8("result tag")?;
    match tag {
        0 => Ok(CommandResult::Ack),
        1 => Ok(CommandResult::Value(reader.bytes("value")?)),
        2 => {
            let code = reader.u32("error_code")?;
            let message_bytes = reader.bytes("error message")?;
            let message = String::from_utf8_lossy(&message_bytes).into_owned();
            Ok(CommandResult::Error { code, message })
        }
        tag => Err(KvError::UnknownTag {
            offset: tag_offset,
            tag,
            context: "command result",
        }),
    }
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("payload fields fit in u32 by construction");
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
}

/// A cursor over a payload, producing offset-exact truncation errors.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize, field: &'static str) -> Result<&[u8], KvError> {
        let available = self.buf.len().saturating_sub(self.pos);
        if available < n {
            return Err(KvError::Truncated {
                offset: self.pos,
                field,
                needed: n,
                available,
            });
        }
        let slice = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    fn u8(&mut self, field: &'static str) -> Result<u8, KvError> {
        Ok(self.take(1, field)?[0])
    }

    fn u32(&mut self, field: &'static str) -> Result<u32, KvError> {
        let bytes = self.take(4, field)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().expect("take returns exactly 4 bytes"),
        ))
    }

    fn u64(&mut self, field: &'static str) -> Result<u64, KvError> {
        let bytes = self.take(8, field)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().expect("take returns exactly 8 bytes"),
        ))
    }

    fn bytes(&mut self, field: &'static str) -> Result<Vec<u8>, KvError> {
        let len = self.u32(field)? as usize;
        Ok(self.take(len, field)?.to_vec())
    }
}
