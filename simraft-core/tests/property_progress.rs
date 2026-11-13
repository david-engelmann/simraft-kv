//! Monotonic replication progress (invariant S-07): stale, reordered
//! `AppendEntries` responses never move `match_index` or `next_index`
//! backwards on the leader.

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use simraft_core::{Event, LogIndex, Message, Node, NodeId, Term};

fn leader_with_peers() -> Node {
    let mut node = Node::new(
        NodeId(1),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        13,
    )
    .unwrap();
    let _ = common::elect(&mut node, &[NodeId(2), NodeId(3)]);
    // Acknowledge the election NoOp persist, then append four client
    // entries so the leader's log honestly holds five entries: a
    // follower can only report a match index the leader's log covers.
    let _ = node.step(Event::Persisted {
        kind: simraft_core::PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    for seq in 1_u64..=4 {
        let _ = node.step(Event::Propose {
            client_id: simraft_core::ClientId(1),
            seq,
            payload: vec![u8::try_from(seq).expect("test sequence numbers fit in u8")],
        });
        let _ = node.step(Event::Persisted {
            kind: simraft_core::PersistKind::Entries,
            up_to_index: LogIndex(1 + seq),
        });
    }
    assert_eq!(node.log().last_index(), LogIndex(5));
    node
}

#[test]
fn stale_append_response_never_regresses_match_index() {
    let mut leader = leader_with_peers();
    let term = leader.current_term();
    assert_eq!(term, Term(1));

    // A response reporting the follower holds 5 entries.
    let _ = leader.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntriesResponse {
            term,
            success: true,
            match_index: LogIndex(5),
            conflict_index: LogIndex(0),
            conflict_term: Term(0),
            read_round: 0,
        },
    });
    assert_eq!(
        leader.progress(NodeId(2)),
        Some((LogIndex(6), LogIndex(5))),
        "next_index must sit one past match_index"
    );

    // A stale, reordered response reporting only 3 must change nothing.
    let _ = leader.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntriesResponse {
            term,
            success: true,
            match_index: LogIndex(3),
            conflict_index: LogIndex(0),
            conflict_term: Term(0),
            read_round: 0,
        },
    });
    assert_eq!(
        leader.progress(NodeId(2)),
        Some((LogIndex(6), LogIndex(5))),
        "a stale response regressed the follower's progress"
    );

    // A duplicated copy of the newest response is likewise inert.
    let _ = leader.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntriesResponse {
            term,
            success: true,
            match_index: LogIndex(5),
            conflict_index: LogIndex(0),
            conflict_term: Term(0),
            read_round: 0,
        },
    });
    assert_eq!(leader.progress(NodeId(2)), Some((LogIndex(6), LogIndex(5))));
}
