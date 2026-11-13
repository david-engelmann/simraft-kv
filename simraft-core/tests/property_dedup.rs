//! Core-side in-flight deduplication (invariant C-02): a retry of an
//! uncommitted proposal attaches to the in-flight entry, appends no
//! second entry, and receives the same response on commit.

#[allow(dead_code, reason = "each test file uses a different subset of the shared driver helpers")]
mod common;

use simraft_core::{
    Action, ClientId, ClientResult, Event, LogIndex, Message, Node, NodeId, PersistKind, Term,
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
        first.iter().any(|a| matches!(a, Action::PersistEntries { .. })),
        "the original proposal persists its entry"
    );
    assert_eq!(leader.log().last_index(), LogIndex(2));

    let retry = propose(&mut leader, 1);
    assert!(
        !retry.iter().any(|a| matches!(a, Action::PersistEntries { .. })),
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
            .filter(|a| matches!(
                a,
                Action::ClientResponse {
                    client_id: ClientId(7),
                    seq: 1,
                    result: ClientResult::Committed,
                }
            ))
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
        second.iter().any(|a| matches!(a, Action::PersistEntries { .. })),
        "a new sequence number is a new entry"
    );
    assert_eq!(leader.log().last_index(), LogIndex(3));
}
