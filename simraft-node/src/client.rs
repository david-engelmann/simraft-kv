//! The client protocol between the `simraft-node` client mode
//! and a node's client listener, plus the one-shot and stateful
//! client implementations.
//!
//! A request is a length-prefixed SRCL message: a `u32` body
//! length, then magic `"SRCL"`, version `u16` = 1, op `u8`
//! (1 Put, 2 Get, 3 Delete), client id `u64`, sequence `u64`,
//! key length `u32`, the key, and for Put a value length `u32`
//! and the value. A response is a length-prefixed SRCL message:
//! magic, version, status `u8` (0 Ok, 1 Redirect, 2 Error),
//! present `u8` (for a Get, 1 when a value exists), data length
//! `u32`, and the data: the value for an Ok Get, the leader's
//! address as text for a Redirect, or the message for an Error.
//!
//! Clients retry transient failures (no leader yet, a refused or
//! dropped connection, leadership turnover) and follow redirects
//! to the leader, always reusing the same `(client_id, seq)` pair
//! for a given write so a proposal that committed just before a
//! retry is deduplicated by the session table rather than applied
//! twice.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use simraft_core::ClientId;
use simraft_kv::MAX_PAYLOAD_BYTES;

use crate::{ClientOp, NodeError};

const MAGIC: &[u8; 4] = b"SRCL";
const VERSION: u16 = 1;

/// Maximum permitted length of a framed client request or response
/// body (16 MiB payload cap plus fixed SRCL envelope overhead).
pub const MAX_CLIENT_BODY_BYTES: usize = MAX_PAYLOAD_BYTES + 64;

/// How long one client operation retries transient failures
/// before giving up.
pub const CLIENT_BUDGET: Duration = Duration::from_secs(20);

static PROCESS_SESSION_PREFIX: OnceLock<u64> = OnceLock::new();
static NEXT_CLIENT_COUNTER: AtomicU64 = AtomicU64::new(1);

const fn splitmix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Allocate a collision-free `ClientId`.
///
/// Mixes the OS process id and full 128-bit launch timestamp into a
/// per-process upper prefix and adds a strictly monotonic atomic
/// counter, so consecutive calls in the same process never collide
/// even when invoked in a tight sub-nanosecond loop.
#[must_use]
pub fn next_client_id() -> ClientId {
    let prefix = *PROCESS_SESSION_PREFIX.get_or_init(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0_u128, |d| d.as_nanos());
        let lo = u64::try_from(nanos & u128::from(u64::MAX)).unwrap_or(0);
        let hi = u64::try_from(nanos >> 64).unwrap_or(0);
        let mixed = splitmix64((u64::from(std::process::id()) << 32) ^ lo ^ hi.rotate_left(19));
        mixed & 0xFFFF_FFFF_0000_0000
    });
    let seq = NEXT_CLIENT_COUNTER.fetch_add(1, Ordering::Relaxed);
    ClientId(prefix.wrapping_add(seq))
}

/// A decoded client response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResponse {
    /// The operation completed; the value is present for a Get
    /// that found one.
    Ok(Option<Vec<u8>>),
    /// The member is not the leader; the leader's address when
    /// the member knows it.
    Redirect(Option<SocketAddr>),
    /// The operation failed on the server side.
    Error(String),
}

const fn bad(detail: String) -> NodeError {
    NodeError::Unavailable { detail }
}

/// Encode one client request.
///
/// # Errors
/// Returns `NodeError::Unavailable` when the encoded request body
/// exceeds [`MAX_CLIENT_BODY_BYTES`].
pub fn encode_request(client_id: ClientId, seq: u64, op: &ClientOp) -> Result<Vec<u8>, NodeError> {
    let body_len = match op {
        ClientOp::Put { key, value } => 31_usize
            .saturating_add(key.len())
            .saturating_add(value.len()),
        ClientOp::Get { key } | ClientOp::Delete { key } => 27_usize.saturating_add(key.len()),
    };
    if body_len > MAX_CLIENT_BODY_BYTES {
        return Err(bad(format!(
            "client request body length {body_len} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
        )));
    }
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&VERSION.to_le_bytes());
    match op {
        ClientOp::Put { key, value } => {
            if key.len().saturating_add(value.len()).saturating_add(25) > MAX_PAYLOAD_BYTES {
                return Err(bad(format!(
                    "client command payload exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
                )));
            }
            body.push(1);
            body.extend_from_slice(&client_id.0.to_le_bytes());
            body.extend_from_slice(&seq.to_le_bytes());
            body.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
            body.extend_from_slice(key);
            body.extend_from_slice(&u32::try_from(value.len()).unwrap_or(u32::MAX).to_le_bytes());
            body.extend_from_slice(value);
        }
        ClientOp::Get { key } => {
            body.push(2);
            body.extend_from_slice(&client_id.0.to_le_bytes());
            body.extend_from_slice(&seq.to_le_bytes());
            body.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
            body.extend_from_slice(key);
        }
        ClientOp::Delete { key } => {
            if key.len().saturating_add(21) > MAX_PAYLOAD_BYTES {
                return Err(bad(format!(
                    "client command payload exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
                )));
            }
            body.push(3);
            body.extend_from_slice(&client_id.0.to_le_bytes());
            body.extend_from_slice(&seq.to_le_bytes());
            body.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
            body.extend_from_slice(key);
        }
    }
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&u32::try_from(body.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Take `n` bytes at `pos` from a request body, naming the
/// field in the error when they are not there.
fn take<'a>(body: &'a [u8], pos: &mut usize, n: usize, field: &str) -> Result<&'a [u8], NodeError> {
    if body.len().saturating_sub(*pos) < n {
        return Err(bad(format!(
            "client request truncated at {field}: need {n} bytes at offset {pos}, {} remain",
            body.len().saturating_sub(*pos)
        )));
    }
    let slice = &body[*pos..*pos + n];
    *pos += n;
    Ok(slice)
}

/// Decode one client request body (without the length prefix).
///
/// # Errors
/// Returns `NodeError::Unavailable` naming the malformed field
/// when the request is not a well-formed SRCL message.
pub fn decode_request(body: &[u8]) -> Result<(ClientId, u64, ClientOp), NodeError> {
    if body.len() > MAX_CLIENT_BODY_BYTES {
        return Err(bad(format!(
            "client request body length {} exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes",
            body.len()
        )));
    }
    let mut pos = 0_usize;
    if take(body, &mut pos, 4, "magic")? != MAGIC {
        return Err(bad("client request has bad magic".to_string()));
    }
    let version = u16::from_le_bytes(
        take(body, &mut pos, 2, "version")?
            .try_into()
            .unwrap_or([0; 2]),
    );
    if version != VERSION {
        return Err(bad(format!(
            "client request version {version}, expected {VERSION}"
        )));
    }
    let op_byte = take(body, &mut pos, 1, "op")?[0];
    let client_id = u64::from_le_bytes(
        take(body, &mut pos, 8, "client id")?
            .try_into()
            .unwrap_or([0; 8]),
    );
    let seq = u64::from_le_bytes(
        take(body, &mut pos, 8, "sequence")?
            .try_into()
            .unwrap_or([0; 8]),
    );
    let key_len = u32::from_le_bytes(
        take(body, &mut pos, 4, "key length")?
            .try_into()
            .unwrap_or([0; 4]),
    ) as usize;
    if key_len > MAX_PAYLOAD_BYTES {
        return Err(bad(format!(
            "client key length {key_len} exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
        )));
    }
    let key = take(body, &mut pos, key_len, "key")?.to_vec();
    let op = match op_byte {
        1 => {
            let value_len = u32::from_le_bytes(
                take(body, &mut pos, 4, "value length")?
                    .try_into()
                    .unwrap_or([0; 4]),
            ) as usize;
            if value_len > MAX_PAYLOAD_BYTES {
                return Err(bad(format!(
                    "client value length {value_len} exceeds limit of {MAX_PAYLOAD_BYTES} bytes"
                )));
            }
            let value = take(body, &mut pos, value_len, "value")?.to_vec();
            ClientOp::Put { key, value }
        }
        2 => ClientOp::Get { key },
        3 => ClientOp::Delete { key },
        found => {
            return Err(bad(format!("unknown client op byte {found}")));
        }
    };
    if pos != body.len() {
        return Err(bad(format!(
            "client request has {} trailing bytes",
            body.len() - pos
        )));
    }
    Ok((ClientId(client_id), seq, op))
}

/// Encode one client response.
#[must_use]
pub fn encode_response(response: &ClientResponse) -> Vec<u8> {
    let (status, present, data): (u8, u8, Vec<u8>) = match response {
        ClientResponse::Ok(Some(value)) => (0, 1, value.clone()),
        ClientResponse::Ok(None) => (0, 0, Vec::new()),
        ClientResponse::Redirect(Some(addr)) => (1, 0, addr.to_string().into_bytes()),
        ClientResponse::Redirect(None) => (1, 0, Vec::new()),
        ClientResponse::Error(message) => (2, 0, message.clone().into_bytes()),
    };
    let mut body = Vec::with_capacity(12 + data.len());
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&VERSION.to_le_bytes());
    body.push(status);
    body.push(present);
    body.extend_from_slice(&u32::try_from(data.len()).unwrap_or(u32::MAX).to_le_bytes());
    body.extend_from_slice(&data);
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&u32::try_from(body.len()).unwrap_or(u32::MAX).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

/// Decode one client response body (without the length prefix).
///
/// # Errors
/// Returns `NodeError::Unavailable` naming the malformed field
/// when the response is not a well-formed SRCL message.
pub fn decode_response(body: &[u8]) -> Result<ClientResponse, NodeError> {
    if body.len() < 12 || &body[0..4] != MAGIC {
        return Err(bad(format!(
            "client response malformed: length {}, magic {:?}",
            body.len(),
            &body[0..4.min(body.len())]
        )));
    }
    let version = u16::from_le_bytes([body[4], body[5]]);
    if version != VERSION {
        return Err(bad(format!(
            "client response version {version}, expected {VERSION}"
        )));
    }
    let status = body[6];
    let present = body[7] == 1;
    let data_len = u32::from_le_bytes(body[8..12].try_into().unwrap_or([0; 4])) as usize;
    if body.len() != 12 + data_len {
        return Err(bad(format!(
            "client response length mismatch: header claims {data_len} data bytes, {} present",
            body.len() - 12
        )));
    }
    let data = &body[12..12 + data_len];
    match status {
        0 => Ok(ClientResponse::Ok(if present {
            Some(data.to_vec())
        } else {
            None
        })),
        1 if data.is_empty() => Ok(ClientResponse::Redirect(None)),
        1 => {
            let text = String::from_utf8_lossy(data).into_owned();
            let addr: SocketAddr = text
                .parse()
                .map_err(|_| bad(format!("redirect address {text:?} does not parse")))?;
            Ok(ClientResponse::Redirect(Some(addr)))
        }
        2 => Ok(ClientResponse::Error(
            String::from_utf8_lossy(data).into_owned(),
        )),
        found => Err(bad(format!("unknown client response status {found}"))),
    }
}

/// Read one length-prefixed message body from a stream.
///
/// # Errors
/// Returns `NodeError::Unavailable` on I/O failure or a length
/// that exceeds [`MAX_CLIENT_BODY_BYTES`].
pub fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>, NodeError> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|e| bad(format!("reading a message length: {e}")))?;
    let len = u32::from_le_bytes(prefix) as usize;
    if len > MAX_CLIENT_BODY_BYTES {
        return Err(bad(format!(
            "message length {len} bytes exceeds limit of {MAX_CLIENT_BODY_BYTES} bytes"
        )));
    }
    let mut body = vec![0_u8; len];
    stream
        .read_exact(&mut body)
        .map_err(|e| bad(format!("reading a message body: {e}")))?;
    Ok(body)
}

fn attempt(server: SocketAddr, request: &[u8]) -> Result<ClientResponse, NodeError> {
    let mut stream = TcpStream::connect_timeout(&server, Duration::from_secs(3))
        .map_err(|e| bad(format!("connecting to {server}: {e}")))?;
    let _ = stream.set_nodelay(true);
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(|e| bad(format!("setting a write timeout: {e}")))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| bad(format!("setting a read timeout: {e}")))?;
    stream
        .write_all(request)
        .map_err(|e| bad(format!("writing a request to {server}: {e}")))?;
    let body = read_message(&mut stream)?;
    decode_response(&body)
}

/// A stateful client session against a `simraft-node` cluster.
///
/// Reuses one [`ClientId`] across multiple operations, advancing the
/// per-client sequence number strictly monotonically on writes (`Put`
/// and `Delete`) while leaving it untouched on `ReadIndex` reads
/// (`Get`), and caching the latest leader redirect hint so sequential
/// operations avoid extra redirect hops.
#[derive(Debug)]
pub struct Client {
    id: ClientId,
    next_seq: u64,
    entry: SocketAddr,
    leader_hint: Option<SocketAddr>,
    budget: Duration,
}

impl Client {
    /// Open a new client session against `server` with a unique
    /// collision-free [`ClientId`].
    #[must_use]
    pub fn new(server: SocketAddr) -> Self {
        Self::with_id(server, next_client_id())
    }

    /// Open a client session with an explicit `id`.
    #[must_use]
    pub const fn with_id(server: SocketAddr, id: ClientId) -> Self {
        Self {
            id,
            next_seq: 0,
            entry: server,
            leader_hint: None,
            budget: CLIENT_BUDGET,
        }
    }

    /// Override the per-operation retry budget.
    #[must_use]
    pub const fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = budget;
        self
    }

    /// This session's client identifier.
    #[must_use]
    pub const fn id(&self) -> ClientId {
        self.id
    }

    /// Execute one operation within this client session.
    ///
    /// # Errors
    /// Returns `NodeError::Unavailable` when the operation exceeds
    /// the payload bound or fails to complete within the retry budget.
    pub fn execute(&mut self, op: &ClientOp) -> Result<Option<Vec<u8>>, NodeError> {
        let is_write = matches!(op, ClientOp::Put { .. } | ClientOp::Delete { .. });
        let seq = if is_write { self.next_seq + 1 } else { 0 };
        let request = encode_request(self.id, seq, op)?;
        if is_write {
            self.next_seq = seq;
        }
        let deadline = Instant::now() + self.budget;
        let mut target = self.leader_hint.unwrap_or(self.entry);
        let mut last_detail: String;
        loop {
            match attempt(target, &request) {
                Ok(ClientResponse::Ok(value)) => {
                    self.leader_hint = Some(target);
                    return Ok(value);
                }
                Ok(ClientResponse::Redirect(Some(addr))) => {
                    target = addr;
                    self.leader_hint = Some(addr);
                    last_detail = format!("redirected to {addr}");
                }
                Ok(ClientResponse::Redirect(None)) => {
                    self.leader_hint = None;
                    last_detail = format!("{target} knows no leader yet");
                    target = self.entry;
                }
                Ok(ClientResponse::Error(message)) => {
                    last_detail = format!("{target} reported: {message}");
                }
                Err(error) => {
                    self.leader_hint = None;
                    last_detail = error.to_string();
                    target = self.entry;
                }
            }
            if Instant::now() >= deadline {
                return Err(bad(format!(
                    "client operation did not complete within {}s: {last_detail}",
                    self.budget.as_secs()
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Store `value` under `key`.
    ///
    /// # Errors
    /// Returns `NodeError::Unavailable` if the write cannot complete.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), NodeError> {
        self.execute(&ClientOp::Put { key, value }).map(|_| ())
    }

    /// Linearizably read `key`.
    ///
    /// # Errors
    /// Returns `NodeError::Unavailable` if the read cannot complete.
    pub fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>, NodeError> {
        self.execute(&ClientOp::Get { key })
    }

    /// Delete `key`.
    ///
    /// # Errors
    /// Returns `NodeError::Unavailable` if the delete cannot complete.
    pub fn delete(&mut self, key: Vec<u8>) -> Result<(), NodeError> {
        self.execute(&ClientOp::Delete { key }).map(|_| ())
    }
}

/// Issue one standalone client operation against the member at
/// `server`, allocating a fresh collision-free [`ClientId`] so
/// rapid sequential `run_client` calls never collide on `(client_id,
/// seq = 1)`.
///
/// # Errors
/// Returns `NodeError::Unavailable` with the last failure's context
/// when the budget expires or the request is rejected.
pub fn run_client(server: SocketAddr, op: &ClientOp) -> Result<Option<Vec<u8>>, NodeError> {
    Client::new(server).execute(op)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn request_and_response_round_trip() {
        let op = ClientOp::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        };
        let bytes = encode_request(ClientId(7), 3, &op).expect("encodes");
        let (id, seq, decoded) = decode_request(&bytes[4..]).expect("the request decodes");
        assert_eq!(id, ClientId(7));
        assert_eq!(seq, 3);
        assert_eq!(decoded, op);
        let response =
            ClientResponse::Redirect(Some("127.0.0.1:7002".parse().expect("address parses")));
        let bytes = encode_response(&response);
        assert_eq!(
            decode_response(&bytes[4..]).expect("the response decodes"),
            response
        );
        let response = ClientResponse::Ok(Some(b"v1".to_vec()));
        let bytes = encode_response(&response);
        assert_eq!(
            decode_response(&bytes[4..]).expect("the response decodes"),
            response
        );
    }

    #[test]
    fn consecutive_client_ids_never_collide() {
        let mut ids = BTreeSet::new();
        for _ in 0..10_000 {
            assert!(ids.insert(next_client_id()), "duplicate ClientId generated");
        }
    }

    #[test]
    fn truncated_request_names_the_field() {
        let bytes = encode_request(
            ClientId(7),
            3,
            &ClientOp::Get {
                key: b"key".to_vec(),
            },
        )
        .expect("encodes");
        let err =
            decode_request(&bytes[4..bytes.len() - 2]).expect_err("a truncated request must fail");
        assert!(
            err.to_string().contains("truncated at key"),
            "the field is named: {err}"
        );
    }

    #[test]
    fn oversized_write_does_not_poison_client_sequence() {
        let addr: SocketAddr = "127.0.0.1:9".parse().expect("valid addr");
        let mut client = Client::with_id(addr, ClientId(42)).with_budget(Duration::ZERO);
        let err = client
            .put(b"k".to_vec(), vec![0_u8; MAX_PAYLOAD_BYTES])
            .expect_err("payload above 16 MiB must fail locally");
        assert!(err.to_string().contains("exceeds limit"));
        assert_eq!(
            client.next_seq, 0,
            "local payload rejection must not burn a sequence number"
        );
    }
}
