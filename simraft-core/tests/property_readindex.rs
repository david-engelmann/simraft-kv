//! `ReadIndex` properties (invariants R-01 to R-05): the current-term
//! commit gate, quorum heartbeat confirmation, monotonic round tokens,
//! and leader-only reads.

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use simraft_core::{Action, Event, LogIndex, Message, Node, NodeId, PersistKind, Term};

fn read_rounds_in(actions: &[Action]) -> Vec<u64> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send {
                msg: Message::AppendEntries { read_round, .. },
                ..
            } => Some(*read_round),
            _ => None,
        })
        .collect()
}

fn ready_in(actions: &[Action]) -> Option<LogIndex> {
    actions.iter().find_map(|a| match a {
        Action::ReadReady { read_index, .. } => Some(*read_index),
        _ => None,
    })
}

fn uncommitted_leader() -> Node {
    let mut leader = Node::new(
        NodeId(1),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        41,
    )
    .unwrap();
    let _ = common::elect(&mut leader, &[NodeId(2), NodeId(3)]);
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    assert_eq!(leader.commit_index(), LogIndex(0));
    leader
}

const fn ae_response(from: u64, match_index: u64, read_round: u64) -> Event {
    Event::Message {
        from: NodeId(from),
        msg: Message::AppendEntriesResponse {
            term: Term(1),
            success: true,
            match_index: LogIndex(match_index),
            conflict_index: LogIndex(0),
            conflict_term: Term(0),
            read_round,
        },
    }
}

#[test]
fn read_waits_for_current_term_commit() {
    let mut leader = uncommitted_leader();
    let actions = leader.step(Event::ReadIndex {
        request_id: 1,
        key: b"k".to_vec(),
    });
    assert!(
        ready_in(&actions).is_none(),
        "a read must not be ready before the leader commits a current-term entry"
    );
    assert!(
        read_rounds_in(&actions).iter().all(|&r| r == 1),
        "the read must start round 1 on its heartbeats, got {actions:?}"
    );
    let actions = leader.step(ae_response(2, 0, 1));
    assert!(
        ready_in(&actions).is_none(),
        "round confirmation without the current-term commit must not release the read"
    );
    let actions = leader.step(ae_response(3, 1, 1));
    assert_eq!(leader.commit_index(), LogIndex(1));
    assert_eq!(
        ready_in(&actions),
        Some(LogIndex(1)),
        "the read releases once the current-term entry commits"
    );
}

#[test]
fn follower_read_is_rejected_with_leader_hint() {
    let mut harness = common::Harness::new(&[1, 2, 3], 43);
    harness.run_until(3_000, 25);
    let leader_id = harness.leaders()[0].0;
    let follower_id = [NodeId(1), NodeId(2), NodeId(3)]
        .into_iter()
        .find(|&id| id != leader_id)
        .expect("a follower exists");
    let actions = harness
        .nodes
        .get_mut(&follower_id)
        .expect("follower")
        .step(Event::ReadIndex {
            request_id: 9,
            key: b"k".to_vec(),
        });
    assert!(
        actions.iter().any(|a| match a {
            Action::ReadRejected {
                request_id: 9,
                leader: Some(l),
            } => *l == leader_id,
            _ => false,
        }),
        "a follower must reject the read with the leader hint, got {actions:?}"
    );
}

#[test]
fn round_tokens_increase_and_stale_rounds_do_not_release() {
    let mut leader = uncommitted_leader();
    let _ = leader.step(Event::ReadIndex {
        request_id: 1,
        key: b"a".to_vec(),
    });
    let _ = leader.step(ae_response(2, 1, 1));
    let _ = leader.step(ae_response(3, 1, 1));
    assert_eq!(leader.commit_index(), LogIndex(1));
    let actions = leader.step(Event::ReadIndex {
        request_id: 2,
        key: b"b".to_vec(),
    });
    assert!(
        read_rounds_in(&actions).iter().all(|&r| r == 2),
        "the second read must use round 2, got {actions:?}"
    );
    let actions = leader.step(ae_response(2, 1, 1));
    assert!(
        ready_in(&actions).is_none(),
        "a stale round echo released the second read"
    );
    let actions = leader.step(ae_response(2, 1, 2));
    assert_eq!(
        ready_in(&actions),
        Some(LogIndex(1)),
        "the second read releases on its own round quorum"
    );
}

#[test]
fn read_index_never_goes_backwards() {
    let mut leader = uncommitted_leader();
    let mut last = LogIndex(0);
    for request in 1_u64..=3 {
        let _ = leader.step(Event::ReadIndex {
            request_id: request,
            key: b"k".to_vec(),
        });
        let round = request;
        let mut released = None;
        for peer in [2_u64, 3] {
            let actions = leader.step(ae_response(peer, 1, round));
            if let Some(index) = ready_in(&actions) {
                released = Some(index);
            }
        }
        let index = released.expect("each read releases once its round is confirmed");
        assert!(
            index >= last,
            "read_index moved backwards: {index} < {last}"
        );
        last = index;
    }
}
