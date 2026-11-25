//! R-06: a cluster of one voter commits and applies its
//! leader's entries. The leader's own durable replica
//! completes its quorum of one, so commitment never waits
//! on a follower response that cannot exist. A cluster can
//! reach one voter by configuration as well as by a
//! single-server membership change, so the lone leader's
//! progress is a liveness requirement, not an edge case.

use simraft_core::{
    Action, ClientId, ClusterConfig, Config, Event, LogIndex, Node, NodeId, PersistKind, Role,
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
                let last = LogIndex(from_index.0 + u64::try_from(entries.len() - 1).unwrap_or(0));
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
