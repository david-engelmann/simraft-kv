//! Persist-before-response ordering (invariant T-02): no vote, append,
//! or replication response is emitted before the durable write it
//! depends on has been acknowledged.

#[allow(dead_code)]
mod common;

use simraft_core::{
    Action, ClientId, EntryKind, Event, LogEntry, LogIndex, Message, Node, NodeId, PersistKind,
    Term,
};

fn sends(actions: &[Action]) -> Vec<&Action> {
    actions
        .iter()
        .filter(|a| matches!(a, Action::Send { .. }))
        .collect()
}

#[test]
fn no_response_before_persist_ack() {
    // Vote path.
    let mut voter = Node::new(
        NodeId(2),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        17,
    )
    .unwrap();
    let actions = voter.step(Event::Message {
        from: NodeId(1),
        msg: Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        },
    });
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::PersistHardState { .. })),
        "granting a vote must persist hard state, got {actions:?}"
    );
    assert!(
        sends(&actions).is_empty(),
        "no vote response may be sent before the persist ack, got {actions:?}"
    );
    let released = voter.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex(0),
    });
    assert!(
        released.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestVoteResponse {
                    vote_granted: true,
                    ..
                },
                ..
            }
        )),
        "the persist ack must release the vote response, got {released:?}"
    );

    // Append path on a fresh follower.
    let mut follower = Node::new(
        NodeId(3),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        19,
    )
    .unwrap();
    let entry = LogEntry {
        term: Term(1),
        kind: EntryKind::Command {
            client_id: ClientId(1),
            seq: 1,
            payload: b"set".to_vec(),
        },
    };
    let actions = follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: vec![entry],
            leader_commit: LogIndex(0),
            read_round: 0,
        },
    });
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::PersistEntries { .. } | Action::PersistHardState { .. }
        )),
        "accepting entries must persist first, got {actions:?}"
    );
    assert!(
        sends(&actions).is_empty(),
        "no append response may be sent before the entries are durable, got {actions:?}"
    );
    // Acknowledge hard state, then the entries; the response follows the
    // entries ack (acknowledging hard state alone is not enough).
    let _ = follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex(0),
    });
    let released = follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    assert!(
        released.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::AppendEntriesResponse {
                    success: true,
                    match_index: LogIndex(1),
                    ..
                },
                ..
            }
        )),
        "the entries ack must release the append response, got {released:?}"
    );
}

#[test]
fn leader_does_not_advertise_unpersisted_entries() {
    let mut leader = Node::new(
        NodeId(1),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        23,
    )
    .unwrap();
    let _ = common::elect(&mut leader, &[NodeId(2), NodeId(3)]);
    // Acknowledge the election NoOp persist; only then may appends flow.
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let actions = leader.step(Event::Propose {
        client_id: ClientId(4),
        seq: 1,
        payload: b"v".to_vec(),
    });
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::PersistEntries { .. })),
        "a proposal must persist the new entry, got {actions:?}"
    );
    assert!(
        sends(&actions).is_empty(),
        "the leader must not send AppendEntries for an entry that is not yet durable, got {actions:?}"
    );
}
