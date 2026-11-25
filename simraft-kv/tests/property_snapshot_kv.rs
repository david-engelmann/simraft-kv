//! Snapshot round-trip property tests (invariant N-03 in docs/INVARIANTS.md).
//!
//! A valid snapshot installs store and session state identical to
//! the snapshotted node: the same values, the same deduplication
//! behavior afterward, and a byte-identical re-encoding.

use proptest::prelude::*;
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
            payload: encode_command(ClientId(client), seq, op).expect("command encodes"),
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

    let bytes = encode_snapshot(&source.snapshot_parts(LogIndex(4), Term(1), &config))
        .expect("snapshot encodes");
    let parts = decode_snapshot(&bytes).expect("the snapshot decodes");
    assert_eq!(parts.last_included_index, LogIndex(4));

    let mut restored = KvStateMachine::new(16);
    restored.install_snapshot(parts);
    assert_eq!(restored.last_applied(), LogIndex(4));
    assert_eq!(restored.get(b"a"), None, "the delete is in force");
    assert_eq!(restored.get(b"b"), Some(b"2".as_slice()));
    assert_eq!(restored.get(b"c"), Some(b"3".as_slice()));
    assert_eq!(restored.session_count(), source.session_count());

    let reencoded = encode_snapshot(&restored.snapshot_parts(LogIndex(4), Term(1), &config))
        .expect("reencodes");
    assert_eq!(reencoded, bytes, "round trip is byte identical");

    let outcome = restored
        .apply(LogIndex(5), &entry(7, 3, &put(b"c", b"CHANGED")))
        .expect("the retry applies as a duplicate");
    assert!(
        matches!(outcome, ApplyOutcome::Cached { .. }),
        "the restored session table deduplicates, got {outcome:?}"
    );
    assert_eq!(restored.get(b"c"), Some(b"3".as_slice()), "no re-execution");

    let outcome = restored
        .apply(LogIndex(6), &entry(7, 4, &put(b"d", b"4")))
        .expect("the next sequence applies");
    assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
    assert_eq!(restored.get(b"d"), Some(b"4".as_slice()));
}

proptest! {
    #[test]
    fn snapshot_roundtrip_is_canonical_and_preserves_dedup(
        ops in prop::collection::vec((1_u64..=5, 0_u8..6, any::<bool>(), prop::collection::vec(any::<u8>(), 0..16)), 1..30),
    ) {
        let config = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], [NodeId(4)]).expect("config");
        let mut source = KvStateMachine::with_max_sessions(16);
        let mut client_seqs = std::collections::BTreeMap::<u64, u64>::new();
        let mut idx = 0_u64;
        for (client, key_id, is_put, val) in &ops {
            let seq = client_seqs.entry(*client).or_insert(0);
            *seq += 1;
            idx += 1;
            let key = vec![b'k', *key_id];
            let kv_op = if *is_put {
                KvOp::Put { key, value: val.clone() }
            } else {
                KvOp::Delete { key }
            };
            source.apply(LogIndex(idx), &entry(*client, *seq, &kv_op)).expect("apply");
        }
        let encoded = encode_snapshot(&source.snapshot_parts(LogIndex(idx), Term(2), &config))
            .expect("encodes");
        let decoded = decode_snapshot(&encoded).expect("decodes");
        let mut restored = KvStateMachine::with_max_sessions(16);
        restored.install_snapshot(decoded);

        let reencoded = encode_snapshot(&restored.snapshot_parts(LogIndex(idx), Term(2), &config))
            .expect("reencodes");
        prop_assert_eq!(reencoded, encoded, "N-03: canonical snapshot encoding must round-trip byte identically");

        for k in 0_u8..6 {
            let key = [b'k', k];
            prop_assert_eq!(restored.get(&key), source.get(&key));
        }
        if let Some((&client, &last_seq)) = client_seqs.iter().next() {
            let dup = restored
                .apply(LogIndex(idx + 1), &entry(client, last_seq, &put(b"k0", b"MUTATED")))
                .expect("dup applies");
            prop_assert!(
                matches!(dup, ApplyOutcome::Cached { .. }),
                "expected cached session outcome, got {dup:?}"
            );
        }
    }
}
