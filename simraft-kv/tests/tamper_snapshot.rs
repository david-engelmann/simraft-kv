//! Snapshot integrity tests (invariant N-03 in docs/INVARIANTS.md).
//!
//! A snapshot whose SHA-256 digest (domain `simraft-kv/snapshot/v1`)
//! does not match its payload is rejected whole, with the expected
//! and actual digests named in the error, and a snapshot body with
//! trailing undecoded bytes before a valid digest is rejected.

use simraft_core::{ClusterConfig, EntryKind, LogEntry, LogIndex, NodeId, Term};
use simraft_kv::snapshot::snapshot_digest;
use simraft_kv::{KvError, KvOp, KvStateMachine, decode_snapshot, encode_command, encode_snapshot};

fn entry(seq_client: (u64, u64), op: &KvOp) -> LogEntry {
    let (client, seq) = seq_client;
    LogEntry {
        term: Term(1),
        kind: EntryKind::Command {
            client_id: simraft_core::ClientId(client),
            seq,
            payload: encode_command(simraft_core::ClientId(client), seq, op).expect("encodes"),
        },
    }
}

fn snapshot_bytes() -> Vec<u8> {
    let mut machine = KvStateMachine::new(16);
    let put = |key: &[u8], value: &[u8]| KvOp::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    };
    machine
        .apply(LogIndex(1), &entry((7, 1), &put(b"alpha", b"one")))
        .expect("apply 1");
    machine
        .apply(LogIndex(2), &entry((7, 2), &put(b"beta", b"two")))
        .expect("apply 2");
    machine
        .apply(LogIndex(3), &entry((9, 1), &put(b"alpha", b"nine")))
        .expect("apply 3");
    let config =
        ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], std::iter::empty()).expect("config");
    encode_snapshot(&machine.snapshot_parts(LogIndex(3), Term(1), &config)).expect("snapshot")
}

#[test]
fn single_byte_flip_is_rejected_with_digests() {
    let bytes = snapshot_bytes();
    assert!(
        decode_snapshot(&bytes).is_ok(),
        "the untampered snapshot must decode"
    );

    let mut tampered = bytes.clone();
    tampered[20] ^= 0xFF;
    match decode_snapshot(&tampered) {
        Err(KvError::SnapshotDigest { expected, actual }) => {
            assert_ne!(expected, actual, "the two digests must differ");
            assert_eq!(expected.len(), 64, "expected digest is full hex SHA-256");
            assert_eq!(actual.len(), 64, "actual digest is full hex SHA-256");
            let stored = simraft_kv::snapshot::hex(&bytes[bytes.len() - 32..]);
            assert_eq!(expected, stored);
        }
        other => panic!("expected a SnapshotDigest rejection, got {other:?}"),
    }

    let mut tampered = bytes.clone();
    let last = tampered.len() - 1;
    tampered[last] ^= 0x01;
    assert!(
        matches!(
            decode_snapshot(&tampered),
            Err(KvError::SnapshotDigest { .. })
        ),
        "a tampered digest is also a rejection"
    );

    assert!(matches!(
        decode_snapshot(&bytes[..16]),
        Err(KvError::Truncated { .. })
    ));

    // Trailing bytes inside the body with a recomputed digest must be rejected.
    let mut with_trailing = bytes[..bytes.len() - 32].to_vec();
    let body_len = with_trailing.len();
    with_trailing.extend_from_slice(&[0xDE, 0xAD, 0xBE]);
    let recomputed = snapshot_digest(&with_trailing);
    with_trailing.extend_from_slice(&recomputed);
    assert_eq!(
        decode_snapshot(&with_trailing),
        Err(KvError::TrailingBytes {
            offset: body_len,
            extra: 3,
        })
    );
}
