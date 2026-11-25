//! The snapshot payload codec (docs/ARCHITECTURE.md section 7.4) and
//! the SHA-256 implementation its integrity digest needs.
//!
//! simraft-kv is std-only (DECISIONS Q5, ARCHITECTURE section 3), so
//! the digest is computed by the self-contained FIPS 180-4 SHA-256 in
//! this module rather than an external crate. The digest input is the
//! domain prefix `simraft-kv/snapshot/v1` followed by every payload
//! byte before the digest itself (section 7.5); a mismatch rejects
//! the whole snapshot and a partial snapshot is never installed
//! (invariant N-03).

use crate::command::{MAX_PAYLOAD_BYTES, Reader, decode_result_from, encode_result, push_bytes};
use crate::error::KvError;
use simraft_core::{ClientId, ClusterConfig, ConfigError, LogIndex, NodeId, Term};

/// The snapshot payload magic, ASCII "SRSN".
const MAGIC: &[u8; 4] = b"SRSN";
/// The snapshot schema version written and accepted by this codec.
const VERSION: u16 = 1;
/// The digest domain prefix (docs/ARCHITECTURE.md section 7.5).
pub const SNAPSHOT_DOMAIN: &str = "simraft-kv/snapshot/v1";

/// The decoded contents of a snapshot payload: everything a state
/// machine needs to resume exactly where the snapshotted node left
/// off, including its deduplication state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotParts {
    /// The highest log index included in the snapshot.
    pub last_included_index: LogIndex,
    /// The term of the entry at `last_included_index`.
    pub last_included_term: Term,
    /// The cluster configuration in force at `last_included_index`.
    pub config: ClusterConfig,
    /// Store entries, in ascending key order.
    pub store: Vec<(Vec<u8>, Vec<u8>)>,
    /// Session records as (client, last sequence, cached response),
    /// in ascending client order.
    pub sessions: Vec<(ClientId, u64, crate::command::CommandResult)>,
}

/// Encode `parts` into the canonical snapshot payload.
///
/// The digest is appended last. Store entries and sessions are
/// written in ascending order regardless of the order they were
/// supplied in, so two encodings of the same state compare byte
/// equal.
///
/// # Errors
///
/// Returns `KvError::PayloadTooLarge` when the encoded snapshot body
/// exceeds [`MAX_PAYLOAD_BYTES`] (16 MiB).
pub fn encode_snapshot(parts: &SnapshotParts) -> Result<Vec<u8>, KvError> {
    let mut store = parts.store.clone();
    store.sort();
    let mut sessions = parts.sessions.clone();
    sessions.sort_by_key(|(client, _, _)| *client);
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&parts.last_included_index.0.to_le_bytes());
    out.extend_from_slice(&parts.last_included_term.0.to_le_bytes());
    push_ids(&mut out, parts.config.voters.iter().copied())?;
    push_ids(&mut out, parts.config.learners.iter().copied())?;
    out.extend_from_slice(&(store.len() as u64).to_le_bytes());
    for (key, value) in &store {
        push_bytes(&mut out, key)?;
        push_bytes(&mut out, value)?;
    }
    out.extend_from_slice(&(sessions.len() as u64).to_le_bytes());
    for (client, last_seq, response) in &sessions {
        let encoded_res = encode_result(response)?;
        let next_len = out
            .len()
            .saturating_add(16)
            .saturating_add(encoded_res.len());
        if next_len > MAX_PAYLOAD_BYTES {
            return Err(KvError::PayloadTooLarge {
                length: next_len,
                limit: MAX_PAYLOAD_BYTES,
            });
        }
        out.extend_from_slice(&client.0.to_le_bytes());
        out.extend_from_slice(&last_seq.to_le_bytes());
        out.extend_from_slice(&encoded_res);
    }
    if out.len() > MAX_PAYLOAD_BYTES {
        return Err(KvError::PayloadTooLarge {
            length: out.len(),
            limit: MAX_PAYLOAD_BYTES,
        });
    }
    let digest = snapshot_digest(&out);
    out.extend_from_slice(&digest);
    Ok(out)
}

fn push_ids(out: &mut Vec<u8>, ids: impl Iterator<Item = NodeId>) -> Result<(), KvError> {
    let ids: Vec<NodeId> = ids.collect();
    let next_len = out
        .len()
        .saturating_add(4)
        .saturating_add(ids.len().saturating_mul(8));
    if next_len > MAX_PAYLOAD_BYTES {
        return Err(KvError::PayloadTooLarge {
            length: next_len,
            limit: MAX_PAYLOAD_BYTES,
        });
    }
    let count = u32::try_from(ids.len()).map_err(|_| KvError::PayloadTooLarge {
        length: ids.len(),
        limit: MAX_PAYLOAD_BYTES,
    })?;
    out.extend_from_slice(&count.to_le_bytes());
    for id in ids {
        out.extend_from_slice(&id.0.to_le_bytes());
    }
    Ok(())
}

/// Decode and verify a snapshot payload.
///
/// # Errors
///
/// Returns `KvError::PayloadTooLarge` when the snapshot body exceeds
/// [`MAX_PAYLOAD_BYTES`], `KvError::SnapshotDigest` when the trailing
/// digest does not match the payload, `KvError::TrailingBytes` when
/// undecoded bytes remain before the digest, and offset-exact errors
/// for truncated or malformed bodies.
pub fn decode_snapshot(payload: &[u8]) -> Result<SnapshotParts, KvError> {
    if payload.len() < 32 {
        return Err(KvError::Truncated {
            offset: 0,
            field: "snapshot digest",
            needed: 32,
            available: payload.len(),
        });
    }
    let (body, stored) = payload.split_at(payload.len() - 32);
    if body.len() > MAX_PAYLOAD_BYTES {
        return Err(KvError::PayloadTooLarge {
            length: body.len(),
            limit: MAX_PAYLOAD_BYTES,
        });
    }
    let computed = snapshot_digest(body);
    if computed.as_slice() != stored {
        return Err(KvError::SnapshotDigest {
            expected: hex(stored),
            actual: hex(&computed),
        });
    }
    let mut reader = Reader::new(body);
    let magic = reader.take(4, "snapshot magic")?;
    if magic != MAGIC {
        return Err(KvError::BadSnapshotMagic {
            found: String::from_utf8_lossy(magic).into_owned(),
        });
    }
    let version = reader.u16("snapshot version")?;
    if version != VERSION {
        return Err(KvError::UnknownTag {
            offset: 4,
            tag: u8::try_from(version).unwrap_or(u8::MAX),
            context: "snapshot version",
        });
    }
    let last_included_index = LogIndex(reader.u64("last_included_index")?);
    let last_included_term = Term(reader.u64("last_included_term")?);
    let voters = read_ids(&mut reader, "voter")?;
    let learners = read_ids(&mut reader, "learner")?;
    let config = ClusterConfig::new(voters, learners).map_err(|err| match err {
        ConfigError::EmptyVoters => KvError::SnapshotEmptyVoters,
        other => KvError::SnapshotInvalidConfig {
            detail: other.to_string(),
        },
    })?;
    let store_count = reader.u64("store_count")?;
    let mut store = Vec::new();
    for _ in 0..store_count {
        let key = reader.bytes("store key")?;
        let value = reader.bytes("store value")?;
        store.push((key, value));
    }
    let session_count = reader.u64("session_count")?;
    let mut sessions = Vec::new();
    for _ in 0..session_count {
        let client = ClientId(reader.u64("session client_id")?);
        let last_seq = reader.u64("session last_seq")?;
        let response = decode_result_from(&mut reader)?;
        sessions.push((client, last_seq, response));
    }
    reader.finish()?;
    Ok(SnapshotParts {
        last_included_index,
        last_included_term,
        config,
        store,
        sessions,
    })
}

fn read_ids(reader: &mut Reader<'_>, kind: &'static str) -> Result<Vec<NodeId>, KvError> {
    let count = reader.u32(kind)?;
    let mut ids = Vec::new();
    for _ in 0..count {
        ids.push(NodeId(reader.u64(kind)?));
    }
    Ok(ids)
}

/// The snapshot integrity digest: SHA-256 over the domain prefix
/// followed by the payload bytes.
#[must_use]
pub fn snapshot_digest(body: &[u8]) -> [u8; 32] {
    let mut input = Vec::with_capacity(SNAPSHOT_DOMAIN.len() + body.len());
    input.extend_from_slice(SNAPSHOT_DOMAIN.as_bytes());
    input.extend_from_slice(body);
    sha256(&input)
}

/// Lowercase hex, for digest values in errors and diagnostics.
#[must_use]
pub fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

const K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// SHA-256 (FIPS 180-4) over `data`.
#[must_use]
#[allow(
    clippy::many_single_char_names,
    reason = "the compression working variables carry their FIPS 180-4 names"
)]
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut msg = data.to_vec();
    msg.push(0x80);
    while msg.len() % 64 != 56 {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());
    for block in msg.as_chunks::<64>().0 {
        let mut w = [0_u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }
    let mut out = [0_u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{hex, sha256};

    #[test]
    fn sha256_matches_fips_vectors() {
        assert_eq!(
            hex(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex(&sha256(
                b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"
            )),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }
}
