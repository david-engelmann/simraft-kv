//! Session deduplication properties (invariants C-01, C-03, C-04) and
//! `ReadIndex` read gating at the state machine.

use simraft_core::{ClientId, EntryKind, LogEntry, LogIndex, Term};
use simraft_kv::machine::ERR_SESSION_GAP;
use simraft_kv::{ApplyOutcome, CommandResult, KvError, KvOp, KvStateMachine, encode_command};

fn cmd(client: u64, seq: u64, op: &KvOp) -> LogEntry {
    LogEntry {
        term: Term(1),
        kind: EntryKind::Command {
            client_id: ClientId(client),
            seq,
            payload: encode_command(ClientId(client), seq, op),
        },
    }
}

fn put(key: &str, value: &str) -> KvOp {
    KvOp::Put {
        key: key.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    }
}

#[test]
fn committed_duplicate_returns_cached_response() {
    let mut machine = KvStateMachine::new(10);
    let first = machine
        .apply(LogIndex(1), &cmd(1, 1, &put("k", "v1")))
        .expect("applies");
    assert_eq!(
        first,
        ApplyOutcome::Applied {
            result: CommandResult::Ack
        }
    );
    let retry = machine
        .apply(LogIndex(2), &cmd(1, 1, &put("k", "v2")))
        .expect("applies");
    assert_eq!(
        retry,
        ApplyOutcome::Cached {
            result: CommandResult::Ack
        }
    );
    assert_eq!(machine.get(b"k"), Some(b"v1".as_slice()));
}

#[test]
fn sequence_gap_is_rejected_and_never_applied() {
    let mut machine = KvStateMachine::new(10);
    let outcome = machine
        .apply(LogIndex(1), &cmd(1, 2, &put("k", "v2")))
        .expect("applies");
    match outcome {
        ApplyOutcome::Applied {
            result: CommandResult::Error { code, .. },
        } => assert_eq!(code, ERR_SESSION_GAP),
        other => panic!("a gap must produce the gap error result, got {other:?}"),
    }
    assert_eq!(machine.get(b"k"), None);
    assert_eq!(machine.session_count(), 0);
    let _ = machine
        .apply(LogIndex(2), &cmd(1, 1, &put("k", "v1")))
        .expect("applies");
    let _ = machine
        .apply(LogIndex(3), &cmd(1, 2, &put("k", "v2")))
        .expect("applies");
    assert_eq!(machine.get(b"k"), Some(b"v2".as_slice()));
}

#[test]
fn older_duplicate_is_stale() {
    let mut machine = KvStateMachine::new(10);
    let _ = machine
        .apply(LogIndex(1), &cmd(1, 1, &put("k", "v1")))
        .expect("applies");
    let _ = machine
        .apply(LogIndex(2), &cmd(1, 2, &put("k", "v2")))
        .expect("applies");
    let outcome = machine
        .apply(LogIndex(3), &cmd(1, 1, &put("k", "vX")))
        .expect("applies");
    assert_eq!(outcome, ApplyOutcome::DuplicateStale);
    assert_eq!(machine.get(b"k"), Some(b"v2".as_slice()));
}

#[test]
fn session_table_is_bounded_and_eviction_allows_reexecution() {
    let mut machine = KvStateMachine::new(2);
    let _ = machine
        .apply(LogIndex(1), &cmd(1, 1, &put("shared", "first")))
        .expect("applies");
    let _ = machine
        .apply(LogIndex(2), &cmd(2, 1, &put("a", "1")))
        .expect("applies");
    let _ = machine
        .apply(LogIndex(3), &cmd(3, 1, &put("b", "1")))
        .expect("applies");
    assert_eq!(
        machine.session_count(),
        2,
        "the table never exceeds its bound"
    );
    let outcome = machine
        .apply(LogIndex(4), &cmd(1, 1, &put("shared", "second")))
        .expect("applies");
    assert_eq!(
        outcome,
        ApplyOutcome::Applied {
            result: CommandResult::Ack
        }
    );
    assert_eq!(machine.get(b"shared"), Some(b"second".as_slice()));
    assert!(machine.session_count() <= 2);
}

#[test]
fn read_waits_for_applied_index() {
    let mut machine = KvStateMachine::new(10);
    let _ = machine
        .apply(LogIndex(1), &cmd(1, 1, &put("k", "v")))
        .expect("applies");
    assert_eq!(
        machine.read(LogIndex(2), b"k"),
        None,
        "a read pinned past the applied index must wait"
    );
    assert_eq!(machine.read(LogIndex(1), b"k"), Some(Some(b"v".as_slice())));
    assert_eq!(machine.read(LogIndex(1), b"missing"), Some(None));
}

#[test]
fn apply_gap_and_replay_are_reported() {
    let mut machine = KvStateMachine::new(10);
    match machine.apply(LogIndex(2), &cmd(1, 1, &put("k", "v"))) {
        Err(KvError::ApplyGap {
            expected: LogIndex(1),
            received: LogIndex(2),
        }) => {}
        other => panic!("expected ApplyGap, got {other:?}"),
    }
    let _ = machine
        .apply(LogIndex(1), &cmd(1, 1, &put("k", "v")))
        .expect("applies");
    let replay = machine
        .apply(LogIndex(1), &cmd(1, 1, &put("k", "v")))
        .expect("applies");
    assert_eq!(replay, ApplyOutcome::DuplicateStale);
}
