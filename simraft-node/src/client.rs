//! The client protocol between the simraft-node client mode
//! and a node's client listener, plus the client implementation.
//!
//! A request is a length-prefixed SRCL message: a `u32` total
//! length, then magic "SRCL", version `u16` = 1, op `u8`
//! (1 Put, 2 Get, 3 Delete), client id `u64`, sequence `u64`,
//! key length `u32`, the key, and for Put a value length `u32`
//! and the value. A response is a length-prefixed SRCL message:
//! magic, version, status `u8` (0 Ok, 1 Redirect, 2 Error),
//! present `u8` (for a Get, 1 when a value exists), data length
//! `u32`, and the data: the value for an Ok Get, the leader's
//! address as text for a Redirect, the message for an Error.
//!
//! The client retries transient failures (no leader yet, a
//! refused or dropped connection) against the same member and
//! follows redirects to the leader, always with the same
//! (client id, sequence) pair, so a write that committed just
//! before a retry is deduplicated by the core's session table
//! rather than applied twice. The retry budget is
//! `CLIENT_BUDGET`; after it, the last failure is returned.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use simraft_core::ClientId;

use crate::{ClientOp, NodeError};

const MAGIC: &[u8; 4] = b"SRCL";
const VERSION: u16 = 1;

/// How long one client operation retries transient failures
/// before giving up.
pub const CLIENT_BUDGET: Duration = Duration::from_secs(20);

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
#[must_use]
pub fn encode_request(client_id: ClientId, seq: u64, op: &ClientOp) -> Vec<u8> {
    let mut body = Vec::with_capacity(64);
    body.extend_from_slice(MAGIC);
    body.extend_from_slice(&VERSION.to_le_bytes());
    match op {
        ClientOp::Put { key, value } => {
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
    out
}

/// Take `n` bytes at `pos` from a request body, naming the
/// field in the error when they are not there.
fn take<'a>(body: &'a [u8], pos: &mut usize, n: usize, field: &str) -> Result<&'a [u8], NodeError> {
    if body.len() < *pos + n {
        return Err(bad(format!(
            "client request truncated at {field}: need {n} bytes at offset {pos}, {} remain",
            body.len() - *pos
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
    let key = take(body, &mut pos, key_len, "key")?.to_vec();
    let op = match op_byte {
        1 => {
            let value_len = u32::from_le_bytes(
                take(body, &mut pos, 4, "value length")?
                    .try_into()
                    .unwrap_or([0; 4]),
            ) as usize;
            let value = take(body, &mut pos, value_len, "value")?.to_vec();
            ClientOp::Put { key, value }
        }
        2 => ClientOp::Get { key },
        3 => ClientOp::Delete { key },
        found => {
            return Err(bad(format!("unknown client op byte {found}")));
        }
    };
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

/// Decode one client response body (without the length
/// prefix).
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
    let status = body[6];
    let present = body[7] == 1;
    let data_len = u32::from_le_bytes(body[8..12].try_into().unwrap_or([0; 4])) as usize;
    if body.len() < 12 + data_len {
        return Err(bad(format!(
            "client response truncated: data length {data_len}, {} remain",
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
/// that exceeds the sanity bound.
pub fn read_message(stream: &mut TcpStream) -> Result<Vec<u8>, NodeError> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(|e| bad(format!("reading a message length: {e}")))?;
    let len = u32::from_le_bytes(prefix) as usize;
    if len > 64 * 1024 * 1024 {
        return Err(bad(format!("message length {len} exceeds the bound")));
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
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .map_err(|e| bad(format!("setting a read timeout: {e}")))?;
    stream
        .write_all(request)
        .map_err(|e| bad(format!("writing a request to {server}: {e}")))?;
    let body = read_message(&mut stream)?;
    decode_response(&body)
}

/// Issue one client operation against the member at `server`,
/// following redirects and retrying transient failures within
/// `CLIENT_BUDGET`, as the module documentation describes.
///
/// # Errors
/// Returns `NodeError::Unavailable` with the last failure's
/// context when the budget expires or the server reports a
/// non-transient error.
pub fn run_client(server: SocketAddr, op: &ClientOp) -> Result<Option<Vec<u8>>, NodeError> {
    // One identity per client invocation: the process id in the
    // high half, launch-time nanoseconds in the low half. The
    // sequence is 1 for the single operation this invocation
    // issues, reused across every retry so the session table
    // deduplicates a retry of a committed write.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::from(d.subsec_nanos()));
    let client_id = ClientId((u64::from(std::process::id()) << 32) | nanos);
    let request = encode_request(client_id, 1, op);
    let deadline = Instant::now() + CLIENT_BUDGET;
    let mut target = server;
    let mut last_detail: String;
    loop {
        match attempt(target, &request) {
            Ok(ClientResponse::Ok(value)) => return Ok(value),
            Ok(ClientResponse::Redirect(Some(addr))) => {
                target = addr;
                last_detail = format!("redirected to {addr}");
            }
            Ok(ClientResponse::Redirect(None)) => {
                last_detail = format!("{target} knows no leader yet");
                // Ask the entry member again next time; it may
                // learn a leader sooner than this member.
                target = server;
            }
            Ok(ClientResponse::Error(message)) => {
                last_detail = format!("{target} reported: {message}");
                // A server error here is the not-ready family
                // (no leader, leadership lost); it is transient
                // within the budget by the same rule as a
                // redirect with no hint.
            }
            Err(error) => {
                last_detail = error.to_string();
                // A transport failure against a redirect target
                // (a leader that has since died) must not stick:
                // fall back to the entry member, which issues a
                // fresh redirect once a new leader exists.
                target = server;
            }
        }
        if Instant::now() >= deadline {
            return Err(bad(format!(
                "client operation did not complete within {}s: {last_detail}",
                CLIENT_BUDGET.as_secs()
            )));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_and_response_round_trip() {
        let op = ClientOp::Put {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        };
        let bytes = encode_request(ClientId(7), 3, &op);
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
    fn truncated_request_names_the_field() {
        let bytes = encode_request(
            ClientId(7),
            3,
            &ClientOp::Get {
                key: b"key".to_vec(),
            },
        );
        let err =
            decode_request(&bytes[4..bytes.len() - 2]).expect_err("a truncated request must fail");
        assert!(
            err.to_string().contains("truncated at key"),
            "the field is named: {err}"
        );
    }
}
