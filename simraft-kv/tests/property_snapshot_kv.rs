//! Snapshot round-trip tests (invariant N-03 in docs/INVARIANTS.md).
//!
//! A valid snapshot installs store and session state identical to
//! the snapshotted node: the same values, the same deduplication
//! behavior afterward, and a byte-identical re-encoding.

use simraft_core::{ClientId, ClusterConfig, EntryKind, LogEntry, LogIndex, NodeId, Term};
use simraft_kv::{
    ApplyOutcome, KvOp, KvStateMachine, decode_snapshot, encode_command, encode_snapshot,
};

fn entry(client: u64, seq: u64, op: &KvOp) -> LogEntry {
    LogEntry {
        term: Term(1),
        kind: EntryKind::Command {
            client_id: ClientId(client),
            seq,
            payload: encode_command(ClientId(client), seq, op),
        },
    }
}

fn put(key: &[u8], value: &[u8]) -> KvOp {
    KvOp::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

#[test]
fn install_restores_store_and_sessions() {
    let config =
        ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], std::iter::empty()).expect("config");
    let mut source = KvStateMachine::new(16);
    source
        .apply(LogIndex(1), &entry(7, 1, &put(b"a", b"1")))
        .expect("apply 1");
    source
        .apply(LogIndex(2), &entry(7, 2, &put(b"b", b"2")))
        .expect("apply 2");
    source
        .apply(
            LogIndex(3),
            &entry(9, 1, &KvOp::Delete { key: b"a".to_vec() }),
        )
        .expect("apply 3");
    source
        .apply(LogIndex(4), &entry(7, 3, &put(b"c", b"3")))
        .expect("apply 4");

    let bytes = encode_snapshot(&source.snapshot_parts(LogIndex(4), Term(1), &config));
    let parts = decode_snapshot(&bytes).expect("the snapshot decodes");
    assert_eq!(parts.last_included_index, LogIndex(4));

    let mut restored = KvStateMachine::new(16);
    restored.install_snapshot(parts);
    assert_eq!(restored.last_applied(), LogIndex(4));
    assert_eq!(restored.get(b"a"), None, "the delete is in force");
    assert_eq!(restored.get(b"b"), Some(b"2".as_slice()));
    assert_eq!(restored.get(b"c"), Some(b"3".as_slice()));
    assert_eq!(restored.session_count(), source.session_count());

    // Re-encoding the restored state reproduces the snapshot bytes.
    let reencoded = encode_snapshot(&restored.snapshot_parts(LogIndex(4), Term(1), &config));
    assert_eq!(reencoded, bytes, "round trip is byte identical");

    // Deduplication continues exactly as the source would run it: a
    // retry of client 7's last sequence is served from the cache and
    // does not re-execute.
    let outcome = restored
        .apply(LogIndex(5), &entry(7, 3, &put(b"c", b"CHANGED")))
        .expect("the retry applies as a duplicate");
    assert!(
        matches!(outcome, ApplyOutcome::Cached { .. }),
        "the restored session table deduplicates, got {outcome:?}"
    );
    assert_eq!(restored.get(b"c"), Some(b"3".as_slice()), "no re-execution");

    // And the next fresh sequence executes normally.
    let outcome = restored
        .apply(LogIndex(6), &entry(7, 4, &put(b"d", b"4")))
        .expect("the next sequence applies");
    assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
    assert_eq!(restored.get(b"d"), Some(b"4".as_slice()));
}
