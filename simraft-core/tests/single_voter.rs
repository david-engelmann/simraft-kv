//! R-06: a cluster of one voter commits and applies its
//! leader's entries. The leader's own durable replica
//! completes its quorum of one, so commitment never waits
//! on a follower response that cannot exist. A cluster can
//! reach one voter by configuration as well as by a
//! single-server membership change, so the lone leader's
//! progress is a liveness requirement, not an edge case.

use simraft_core::{
    Action, ClientId, ClusterConfig, Config, ConfigChange, ConfigResult, Event, LogIndex, Message,
    Node, NodeId, PersistKind, Role,
};

/// Feed a step's actions back into the node the way a
/// driver would: persistence is acknowledged at once, and
/// a message the node addresses to itself is delivered.
/// Chained actions are driven until the node is quiescent.
fn drive(node: &mut Node, actions: Vec<Action>) -> Vec<Action> {
    let mut emitted = Vec::new();
    let mut queue: std::collections::VecDeque<Action> = actions.into();
    while let Some(action) = queue.pop_front() {
        match &action {
            Action::PersistHardState { .. } => {
                queue.extend(node.step(Event::Persisted {
                    kind: PersistKind::HardState,
                    up_to_index: LogIndex::ZERO,
                }));
            }
            Action::PersistEntries {
                from_index,
                entries,
            } => {
                let last = LogIndex(from_index.0 + u64::try_from(entries.len() - 1).unwrap_or(0));
                queue.extend(node.step(Event::Persisted {
                    kind: PersistKind::Entries,
                    up_to_index: last,
                }));
            }
            Action::Send { to, msg } if *to == NodeId(1) => {
                queue.extend(node.step(Event::Message {
                    from: *to,
                    msg: msg.clone(),
                }));
            }
            _ => {}
        }
        emitted.push(action);
    }
    emitted
}

#[test]
fn single_voter_leader_commits_and_applies() {
    let cluster = ClusterConfig::new([NodeId(1)], Vec::new()).expect("cluster validates");
    let mut node = Node::new(NodeId(1), Config::default(), cluster, 7).expect("node builds");
    let initial = node.step(Event::Tick { now: 1_000_000 });
    let _ = drive(&mut node, initial);
    assert_eq!(
        node.role(),
        Role::Leader,
        "the lone voter must elect itself"
    );
    let actions = node.step(Event::Propose {
        client_id: ClientId(1),
        seq: 1,
        payload: b"one".to_vec(),
    });
    let _ = drive(&mut node, actions);
    assert_eq!(
        node.commit_index(),
        LogIndex(2),
        "R-06: the lone leader commits its NoOp and its first entry"
    );
    assert_eq!(
        node.last_applied(),
        LogIndex(2),
        "R-06: the lone leader applies what it committed"
    );
}

#[test]
fn single_voter_waits_for_both_hard_state_and_entries_persists_before_commit_or_read() {
    let cluster = ClusterConfig::new([NodeId(1)], Vec::new()).expect("cluster validates");
    let mut node = Node::new(NodeId(1), Config::default(), cluster, 11).expect("node builds");
    let election_actions = node.step(Event::Tick { now: 1_000_000 });
    assert_eq!(node.role(), Role::Leader);
    assert_eq!(
        node.commit_index(),
        LogIndex::ZERO,
        "T-02: nothing commits before PersistHardState and PersistEntries complete"
    );
    assert!(
        !election_actions
            .iter()
            .any(|a| matches!(a, Action::Apply { .. })),
        "T-02: no Apply before durability"
    );

    // Issue a ReadIndex while election persists are still in flight.
    let read_actions = node.step(Event::ReadIndex {
        request_id: 99,
        key: b"k".to_vec(),
    });
    assert!(
        !read_actions
            .iter()
            .any(|a| matches!(a, Action::ReadReady { .. })),
        "T-02 + R-01: ReadReady must wait until hard state and NoOp are durable"
    );

    // Acknowledge PersistEntries FIRST while PersistHardState is still unacked:
    // commit and ReadReady must STILL be held until hard state is durable!
    let after_entries = node.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    assert_eq!(
        node.commit_index(),
        LogIndex::ZERO,
        "T-02: entries ack alone must not commit while self-vote hard state is unpersisted"
    );
    assert!(
        !after_entries
            .iter()
            .any(|a| matches!(a, Action::ReadReady { .. } | Action::Apply { .. })),
        "T-02: neither Apply nor ReadReady may fire before PersistHardState ack"
    );

    // Now acknowledge PersistHardState: NoOp commits, applies, and releases ReadReady.
    let after_hard = node.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    assert_eq!(node.commit_index(), LogIndex(1));
    assert_eq!(node.last_applied(), LogIndex(1));
    assert!(
        after_hard.iter().any(|a| matches!(
            a,
            Action::ReadReady {
                request_id: 99,
                read_index: LogIndex(1),
                ..
            }
        )),
        "R-06: ReadReady releases once both persists complete, got {after_hard:?}"
    );
}

#[test]
fn two_voter_cluster_shrinking_to_one_voter_commits_only_after_persist() {
    let cluster =
        ClusterConfig::new([NodeId(1), NodeId(2)], Vec::new()).expect("cluster validates");
    let mut leader = Node::new(NodeId(1), Config::default(), cluster, 19).expect("node builds");

    // Elect node 1 in the 2-voter cluster.
    let _ = leader.step(Event::Tick { now: 1_000_000 });
    let _ = leader.step(Event::Message {
        from: NodeId(2),
        msg: Message::RequestPreVoteResponse {
            term: leader.current_term(),
            vote_granted: true,
        },
    });
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    let vote_term = leader.current_term();
    let _ = leader.step(Event::Message {
        from: NodeId(2),
        msg: Message::RequestVoteResponse {
            term: vote_term,
            vote_granted: true,
        },
    });
    assert_eq!(leader.role(), Role::Leader);
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let _ = leader.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntriesResponse {
            term: vote_term,
            success: true,
            match_index: LogIndex(1),
            conflict_index: LogIndex::ZERO,
            conflict_term: simraft_core::Term::ZERO,
            read_round: 0,
        },
    });
    assert_eq!(leader.commit_index(), LogIndex(1));

    // Remove node 2: latest config immediately becomes {n1} (quorum 1),
    // but index 2 must NOT commit until PersistEntries(2) is acknowledged.
    let remove_actions = leader.step(Event::ProposeConfig {
        change: ConfigChange::RemoveVoter { node: NodeId(2) },
    });
    assert_eq!(
        leader.commit_index(),
        LogIndex(1),
        "T-02: config shrink to 1 voter must not commit before PersistEntries ack"
    );
    assert!(
        !remove_actions
            .iter()
            .any(|a| matches!(a, Action::ConfigResponse { .. })),
        "no ConfigResponse before persist ack"
    );

    let persisted = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });
    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(leader.last_applied(), LogIndex(2));
    assert!(
        persisted.iter().any(|a| matches!(
            a,
            Action::ConfigResponse {
                result: ConfigResult::Committed,
                ..
            }
        )),
        "R-06: shrinking to one voter commits once durable"
    );

    // Subsequent proposals on the now-lone voter commit after persist.
    let prop = leader.step(Event::Propose {
        client_id: ClientId(7),
        seq: 1,
        payload: b"solo".to_vec(),
    });
    assert_eq!(leader.commit_index(), LogIndex(2));
    let _ = drive(&mut leader, prop);
    assert_eq!(leader.commit_index(), LogIndex(3));
    assert_eq!(leader.last_applied(), LogIndex(3));
}
