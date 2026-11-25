//! Core-side in-flight deduplication (invariant C-02): a retry of an
//! uncommitted proposal attaches to the in-flight entry, appends no
//! second entry, and receives the same response on commit.

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use proptest::prelude::*;
use simraft_core::{
    Action, ClientId, ClientResult, Config, Event, LogIndex, Message, Node, NodeId, PersistKind,
    ProtocolError, Term,
};

fn committed_leader() -> Node {
    let mut leader = Node::new(
        NodeId(1),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        53,
    )
    .unwrap();
    let _ = common::elect(&mut leader, &[NodeId(2), NodeId(3)]);
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    leader
}

fn propose(leader: &mut Node, seq: u64) -> Vec<Action> {
    leader.step(Event::Propose {
        client_id: ClientId(7),
        seq,
        payload: b"payload".to_vec(),
    })
}

#[test]
fn duplicate_propose_attaches_to_inflight_entry() {
    let mut leader = committed_leader();
    let first = propose(&mut leader, 1);
    assert!(
        first
            .iter()
            .any(|a| matches!(a, Action::PersistEntries { .. })),
        "the original proposal persists its entry"
    );
    assert_eq!(leader.log().last_index(), LogIndex(2));
    // The entry is durable locally before replication completes.
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });

    let retry = propose(&mut leader, 1);
    assert!(
        !retry
            .iter()
            .any(|a| matches!(a, Action::PersistEntries { .. })),
        "a retry of an in-flight proposal must not persist a second entry, got {retry:?}"
    );
    assert_eq!(
        leader.log().last_index(),
        LogIndex(2),
        "a retry of an in-flight proposal must not append"
    );

    let mut responses = 0;
    for peer in [2_u64, 3] {
        let actions = leader.step(Event::Message {
            from: NodeId(peer),
            msg: Message::AppendEntriesResponse {
                term: Term(1),
                success: true,
                match_index: LogIndex(2),
                conflict_index: LogIndex(0),
                conflict_term: Term(0),
                read_round: 0,
            },
        });
        responses += actions
            .iter()
            .filter(|a| {
                matches!(
                    a,
                    Action::ClientResponse {
                        client_id: ClientId(7),
                        seq: 1,
                        result: ClientResult::Committed,
                    }
                )
            })
            .count();
    }
    assert_eq!(
        responses, 2,
        "the original proposal and its attached retry each get the response"
    );
}

#[test]
fn different_seq_appends_normally() {
    let mut leader = committed_leader();
    let _ = propose(&mut leader, 1);
    let second = propose(&mut leader, 2);
    assert!(
        second
            .iter()
            .any(|a| matches!(a, Action::PersistEntries { .. })),
        "a new sequence number is a new entry"
    );
    assert_eq!(leader.log().last_index(), LogIndex(3));
}

#[test]
fn max_uncommitted_entries_backpressures_new_proposals() {
    let cfg = Config {
        max_uncommitted_entries: 3,
        ..Config::default()
    };
    let mut leader = Node::new(NodeId(1), cfg, common::all_voters(&[1, 2, 3]), 61).unwrap();
    let _ = common::elect(&mut leader, &[NodeId(2), NodeId(3)]);
    // NoOp is at index 1 (uncommitted = 1). Two proposals reach uncommitted = 3.
    for seq in 1..=2_u64 {
        let out = leader.step(Event::Propose {
            client_id: ClientId(1),
            seq,
            payload: b"ok".to_vec(),
        });
        assert!(
            out.iter()
                .any(|a| matches!(a, Action::PersistEntries { .. }))
        );
    }
    assert_eq!(leader.log().last_index(), LogIndex(3));

    // In-flight retry of an already appended proposal still attaches without appending.
    let retry = leader.step(Event::Propose {
        client_id: ClientId(1),
        seq: 2,
        payload: b"ok".to_vec(),
    });
    assert_eq!(retry, Vec::new());

    // A fresh proposal past max_uncommitted_entries (3) is rejected immediately.
    let rejected = leader.step(Event::Propose {
        client_id: ClientId(1),
        seq: 3,
        payload: b"overflow".to_vec(),
    });
    assert_eq!(
        rejected,
        vec![Action::ClientResponse {
            client_id: ClientId(1),
            seq: 3,
            result: ClientResult::Rejected {
                error: ProtocolError::TooManyUncommitted {
                    uncommitted: 3,
                    limit: 3,
                },
            },
        }]
    );
    assert_eq!(leader.log().last_index(), LogIndex(3));
}

proptest! {
    #[test]
    fn inflight_retries_never_duplicate_entries_and_all_waiters_receive_commit(
        retries_per_proposal in prop::collection::vec(0_usize..=4, 1..=8),
    ) {
        let mut leader = committed_leader();
        let mut expected_responses = 0_usize;
        for (idx, &retries) in retries_per_proposal.iter().enumerate() {
            let seq = idx as u64 + 1;
            let _ = propose(&mut leader, seq);
            for _ in 0..retries {
                let dup = propose(&mut leader, seq);
                prop_assert!(
                    !dup.iter().any(|a| matches!(a, Action::PersistEntries { .. })),
                    "C-02: in-flight retry must never persist a second entry"
                );
            }
            expected_responses += 1 + retries;
        }
        let expected_last = LogIndex(1 + retries_per_proposal.len() as u64);
        prop_assert_eq!(leader.log().last_index(), expected_last);
        let _ = leader.step(Event::Persisted {
            kind: PersistKind::Entries,
            up_to_index: expected_last,
        });
        let actions = leader.step(Event::Message {
            from: NodeId(2),
            msg: Message::AppendEntriesResponse {
                term: Term(1),
                success: true,
                match_index: expected_last,
                conflict_index: LogIndex::ZERO,
                conflict_term: Term::ZERO,
                read_round: 0,
            },
        });
        let count = actions
            .iter()
            .filter(|a| matches!(a, Action::ClientResponse { result: ClientResult::Committed, .. }))
            .count();
        prop_assert_eq!(count, expected_responses);
    }
}
