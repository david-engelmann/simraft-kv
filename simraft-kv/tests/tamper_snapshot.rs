//! Snapshot integrity tests (invariant N-03 in docs/INVARIANTS.md).
//!
//! A snapshot whose SHA-256 digest (domain `simraft-kv/snapshot/v1`)
//! does not match its payload is rejected whole, with the expected
//! and actual digests named in the error.

use simraft_core::{ClusterConfig, EntryKind, LogEntry, LogIndex, NodeId, Term};
use simraft_kv::{KvError, KvOp, KvStateMachine, decode_snapshot, encode_command, encode_snapshot};

fn entry(seq_client: (u64, u64), op: &KvOp) -> LogEntry {
    let (client, seq) = seq_client;
    LogEntry {
        term: Term(1),
        kind: EntryKind::Command {
            client_id: simraft_core::ClientId(client),
            seq,
            payload: encode_command(simraft_core::ClientId(client), seq, op),
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
    encode_snapshot(&machine.snapshot_parts(LogIndex(3), Term(1), &config))
}

#[test]
fn single_byte_flip_is_rejected_with_digests() {
    let bytes = snapshot_bytes();
    assert!(
        decode_snapshot(&bytes).is_ok(),
        "the untampered snapshot must decode"
    );

    // Flip one payload byte (inside the store section).
    let mut tampered = bytes.clone();
    tampered[20] ^= 0xFF;
    match decode_snapshot(&tampered) {
        Err(KvError::SnapshotDigest { expected, actual }) => {
            assert_ne!(expected, actual, "the two digests must differ");
            assert_eq!(expected.len(), 64, "expected digest is full hex SHA-256");
            assert_eq!(actual.len(), 64, "actual digest is full hex SHA-256");
            // The expected digest is the one stored in the snapshot.
            let stored = simraft_kv::snapshot::hex(&bytes[bytes.len() - 32..]);
            assert_eq!(expected, stored);
        }
        other => panic!("expected a SnapshotDigest rejection, got {other:?}"),
    }

    // Flip one byte of the stored digest itself.
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

    // A truncated payload is an offset-exact error, never a panic.
    assert!(matches!(
        decode_snapshot(&bytes[..16]),
        Err(KvError::Truncated { .. })
    ));
}
