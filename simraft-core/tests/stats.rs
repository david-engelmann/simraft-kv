//! Stats snapshot contract (post-M4 Lane 4 Stage A): a driver
//! can read the node's whole operating state as data through
//! `Node::stats`, without stepping the node or inferring state
//! from the actions it emits.
//!
//! The snapshot is the node's own accessor values at one instant,
//! gathered into one struct: role, current term, the acknowledged
//! leader, the commit and applied indexes, the last log index,
//! the snapshot base, and, on a leader, every peer's replication
//! progress. These tests are the designed red of Lane 4 Stage A:
//! `NodeStats` does not exist yet, so this file does not compile
//! against the unmodified core.

use std::collections::BTreeMap;

use simraft_core::{
    Action, ClientId, ClusterConfig, Config, Event, LogIndex, Message, Node, NodeId, NodeStats,
    PeerStats, PersistKind, Role, Term,
};

fn cluster() -> BTreeMap<NodeId, Node> {
    let cfg = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("config validates");
    let mut nodes = BTreeMap::new();
    for id in [1_u64, 2, 3] {
        nodes.insert(
            NodeId(id),
            Node::new(NodeId(id), Config::default(), cfg.clone(), 7).expect("node builds"),
        );
    }
    nodes
}

/// Elect node 1 and release its leader `NoOp`, so proposals flow.
fn elect(nodes: &mut BTreeMap<NodeId, Node>) {
    let node = nodes.get_mut(&NodeId(1)).expect("node exists");
    node.step(Event::Tick { now: 1_000_000 });
    for peer in [NodeId(2), NodeId(3)] {
        let term = node.current_term();
        node.step(Event::Message {
            from: peer,
            msg: Message::RequestPreVoteResponse {
                term,
                vote_granted: true,
            },
        });
        if node.role() == Role::Candidate {
            break;
        }
    }
    node.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    for peer in [NodeId(2), NodeId(3)] {
        let term = node.current_term();
        node.step(Event::Message {
            from: peer,
            msg: Message::RequestVoteResponse {
                term,
                vote_granted: true,
            },
        });
        if node.role() == Role::Leader {
            break;
        }
    }
    assert_eq!(node.role(), Role::Leader, "the cluster elects");
    let last = node.log().last_index();
    node.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: last,
    });
}

/// Route actions between the nodes, acknowledging persists
/// immediately, until the cluster is quiescent.
fn route(nodes: &mut BTreeMap<NodeId, Node>, actions: Vec<Action>) {
    let mut queue: Vec<(NodeId, Event)> = Vec::new();
    let collect = |queue: &mut Vec<(NodeId, Event)>, source: NodeId, actions: Vec<Action>| {
        for action in actions {
            match action {
                Action::Send { to, msg } => {
                    queue.push((to, Event::Message { from: source, msg }));
                }
                Action::PersistHardState { .. } => queue.push((
                    source,
                    Event::Persisted {
                        kind: PersistKind::HardState,
                        up_to_index: LogIndex::ZERO,
                    },
                )),
                Action::PersistEntries { .. } => queue.push((
                    source,
                    Event::Persisted {
                        kind: PersistKind::Entries,
                        up_to_index: LogIndex::ZERO,
                    },
                )),
                _ => {}
            }
        }
    };
    collect(&mut queue, NodeId(1), actions);
    while let Some((to, event)) = queue.pop() {
        let event = match event {
            Event::Persisted {
                kind: PersistKind::Entries,
                ..
            } => Event::Persisted {
                kind: PersistKind::Entries,
                up_to_index: nodes.get(&to).expect("node").log().last_index(),
            },
            other => other,
        };
        let actions = nodes.get_mut(&to).expect("node exists").step(event);
        collect(&mut queue, to, actions);
    }
}

/// Propose one command on the leader, release it, and route the
/// cluster to quiescence.
fn commit_one_proposal(nodes: &mut BTreeMap<NodeId, Node>) {
    let mut actions = Vec::new();
    actions.extend(
        nodes
            .get_mut(&NodeId(1))
            .expect("leader exists")
            .step(Event::Propose {
                client_id: ClientId(1),
                seq: 1,
                payload: vec![7_u8; 16],
            }),
    );
    let last = nodes
        .get(&NodeId(1))
        .expect("leader exists")
        .log()
        .last_index();
    actions.extend(nodes.get_mut(&NodeId(1)).expect("leader exists").step(
        Event::Persisted {
            kind: PersistKind::Entries,
            up_to_index: last,
        },
    ));
    route(nodes, actions);
    // One heartbeat round so the followers learn the commit index
    // the leader reached on their acknowledgements.
    let heartbeat = nodes
        .get_mut(&NodeId(1))
        .expect("leader exists")
        .step(Event::Tick { now: 1_000_100 });
    route(nodes, heartbeat);
}

#[test]
fn a_fresh_node_reports_a_zeroed_follower_snapshot() {
    let nodes = cluster();
    let stats = nodes.get(&NodeId(1)).expect("node exists").stats();
    assert_eq!(
        stats,
        NodeStats {
            role: Role::Follower,
            current_term: Term(0),
            leader: None,
            commit_index: LogIndex::ZERO,
            last_applied: LogIndex::ZERO,
            last_log_index: LogIndex::ZERO,
            snapshot_index: LogIndex::ZERO,
            peers: BTreeMap::new(),
        },
        "a node that has not stepped reports its construction state"
    );
}

#[test]
fn an_elected_leader_reports_its_term_and_peer_progress() {
    let mut nodes = cluster();
    elect(&mut nodes);
    let stats = nodes.get(&NodeId(1)).expect("node exists").stats();
    assert_eq!(stats.role, Role::Leader);
    assert_eq!(stats.current_term, Term(1));
    assert_eq!(
        stats.leader,
        Some(NodeId(1)),
        "a leader acknowledges itself as leader"
    );
    assert_eq!(stats.last_log_index, LogIndex(1), "the leader NoOp");
    assert_eq!(
        stats.commit_index,
        LogIndex::ZERO,
        "nothing commits before a follower acknowledges"
    );
    assert_eq!(stats.snapshot_index, LogIndex::ZERO);
    for peer in [NodeId(2), NodeId(3)] {
        assert_eq!(
            stats.peers.get(&peer),
            Some(&PeerStats {
                next_index: LogIndex(1),
                match_index: LogIndex::ZERO,
            }),
            "a new leader offers each follower its log from index 1"
        );
        assert_eq!(
            stats.peers.get(&peer).map(|p| (p.next_index, p.match_index)),
            nodes.get(&NodeId(1)).expect("node exists").progress(peer),
            "the snapshot agrees with the progress accessor for {peer}"
        );
    }
}

#[test]
fn commit_and_applied_advance_together_on_leader_and_follower() {
    let mut nodes = cluster();
    elect(&mut nodes);
    commit_one_proposal(&mut nodes);
    let leader = nodes.get(&NodeId(1)).expect("node exists").stats();
    assert_eq!(leader.commit_index, LogIndex(2));
    assert_eq!(leader.last_applied, LogIndex(2));
    assert_eq!(
        leader.peers.get(&NodeId(2)).map(|p| p.match_index),
        Some(LogIndex(2)),
        "the follower's acknowledgement is in the leader's snapshot"
    );
    let follower = nodes.get(&NodeId(2)).expect("node exists").stats();
    assert_eq!(follower.role, Role::Follower);
    assert_eq!(
        follower.leader,
        Some(NodeId(1)),
        "a follower acknowledges the leader it heard from"
    );
    assert_eq!(follower.commit_index, LogIndex(2));
    assert_eq!(follower.last_applied, LogIndex(2));
    assert_eq!(follower.last_log_index, LogIndex(2));
}

#[test]
fn a_follower_reports_no_peer_progress() {
    let mut nodes = cluster();
    elect(&mut nodes);
    commit_one_proposal(&mut nodes);
    for follower in [NodeId(2), NodeId(3)] {
        let stats = nodes.get(&follower).expect("node exists").stats();
        assert_eq!(
            stats.peers.len(),
            0,
            "peer progress is a leader-only view, empty on {follower}"
        );
    }
}
