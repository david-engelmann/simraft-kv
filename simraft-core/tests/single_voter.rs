//! R-06: a cluster of one voter commits and applies its
//! leader's entries. The leader's own durable replica
//! completes its quorum of one, so commitment never waits
//! on a follower response that cannot exist. A cluster can
//! reach one voter by configuration as well as by a
//! single-server membership change, so the lone leader's
//! progress is a liveness requirement, not an edge case.
//! The second test walks the shrink path itself: a
//! three-voter leader removes the other two voters one at
//! a time, then must keep committing on its own.

use simraft_core::{
    Action, ClientId, ClusterConfig, Config, ConfigChange, Event, LogIndex, Message, Node, NodeId,
    PersistKind, Role, Term,
};

/// Feed a step's actions back into the node the way a
/// driver would: persistence is acknowledged at once, and
/// a message the node addresses to itself is delivered.
/// Chained actions are driven until the node is quiescent.
fn drive(node: &mut Node, actions: Vec<Action>) {
    let mut queue: std::collections::VecDeque<Action> = actions.into();
    while let Some(action) = queue.pop_front() {
        match action {
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
                let count = u64::try_from(entries.len().saturating_sub(1)).unwrap_or(0);
                let last = LogIndex(from_index.0 + count);
                queue.extend(node.step(Event::Persisted {
                    kind: PersistKind::Entries,
                    up_to_index: last,
                }));
            }
            Action::Send { to, msg } if to == NodeId(1) => {
                queue.extend(node.step(Event::Message { from: to, msg }));
            }
            _ => {}
        }
    }
}

#[test]
fn single_voter_leader_commits_and_applies() {
    let cluster = ClusterConfig::new([NodeId(1)], Vec::new()).expect("cluster validates");
    let mut node = Node::new(NodeId(1), Config::default(), cluster, 7).expect("node builds");
    let boot = node.step(Event::Tick { now: 1_000_000 });
    drive(&mut node, boot);
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
    drive(&mut node, actions);
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

/// Drive a step's actions for a node with peers: persistence
/// is acknowledged at once, and every message the node sends
/// gets the faithful peer reply (votes granted, appends
/// matched in full), the way a healthy cluster answers.
fn drive_cluster(node: &mut Node, actions: Vec<Action>) {
    let mut queue: std::collections::VecDeque<Action> = actions.into();
    while let Some(action) = queue.pop_front() {
        match action {
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
                let count = u64::try_from(entries.len().saturating_sub(1)).unwrap_or(0);
                let last = LogIndex(from_index.0 + count);
                queue.extend(node.step(Event::Persisted {
                    kind: PersistKind::Entries,
                    up_to_index: last,
                }));
            }
            Action::Send { to, msg } => {
                let reply = match &msg {
                    Message::RequestPreVote { term, .. } => Some(
                        Message::RequestPreVoteResponse {
                            term: *term,
                            vote_granted: true,
                        },
                    ),
                    Message::RequestVote { term, .. } => Some(Message::RequestVoteResponse {
                        term: *term,
                        vote_granted: true,
                    }),
                    Message::AppendEntries {
                        term,
                        prev_log_index,
                        entries,
                        read_round,
                        ..
                    } => Some(Message::AppendEntriesResponse {
                        term: *term,
                        success: true,
                        match_index: LogIndex(
                            prev_log_index.0 + u64::try_from(entries.len()).unwrap_or(0),
                        ),
                        conflict_index: LogIndex::ZERO,
                        conflict_term: Term::ZERO,
                        read_round: *read_round,
                    }),
                    _ => None,
                };
                if let Some(reply) = reply {
                    queue.extend(node.step(Event::Message { from: to, msg: reply }));
                }
            }
            _ => {}
        }
    }
}

#[test]
fn shrunk_to_one_voter_leader_keeps_committing() {
    let cluster = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("cluster validates");
    let mut node = Node::new(NodeId(1), Config::default(), cluster, 7).expect("node builds");
    let boot = node.step(Event::Tick { now: 1_000_000 });
    drive_cluster(&mut node, boot);
    assert_eq!(node.role(), Role::Leader, "the leader must elect");
    assert_eq!(
        node.commit_index(),
        LogIndex(1),
        "the leader NoOp commits under the three-voter configuration"
    );
    let shrink_one = node.step(Event::ProposeConfig {
        change: ConfigChange::RemoveVoter { node: NodeId(2) },
    });
    drive_cluster(&mut node, shrink_one);
    let shrink_two = node.step(Event::ProposeConfig {
        change: ConfigChange::RemoveVoter { node: NodeId(3) },
    });
    drive_cluster(&mut node, shrink_two);
    let after_shrink = node.log().last_index();
    let actions = node.step(Event::Propose {
        client_id: ClientId(1),
        seq: 1,
        payload: b"after".to_vec(),
    });
    drive_cluster(&mut node, actions);
    assert_eq!(
        node.commit_index(),
        after_shrink.next(),
        "R-06: the leader shrunk to one voter still commits"
    );
    assert_eq!(
        node.last_applied(),
        after_shrink.next(),
        "R-06: the leader shrunk to one voter still applies"
    );
}
