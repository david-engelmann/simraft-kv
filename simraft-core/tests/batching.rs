//! Leader batching contract (invariant B-02, post-M4 Lane 3
//! Stage A): proposals the leader appends before the persist
//! acknowledgement that releases them are broadcast as ONE
//! `AppendEntries` round per follower, not one round per
//! proposal.
//!
//! The mechanism the contract pins: `Node::step` holds each
//! proposal's sends until the entries are durable (invariant
//! T-02), and a later proposal's send to a follower is built from
//! the same next index as the earlier one's, so it carries every
//! entry the earlier send carried and more. When one persist
//! acknowledgement releases several held sends to the same
//! follower, all but the last are redundant retransmissions. The
//! coalescing test below is the designed red of Lane 3 Stage A:
//! on the unmodified core it observes one released round per
//! proposal.

use std::collections::BTreeMap;

use simraft_core::{
    Action, ClientId, ClientResult, ClusterConfig, Config, Event, LogIndex, Message, Node, NodeId,
    PersistKind, Role,
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

/// The `AppendEntries` sends to `peer` inside `actions`, as
/// (prev log index, entry count) pairs.
fn append_rounds(actions: &[Action], peer: NodeId) -> Vec<(LogIndex, usize)> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send {
                to,
                msg:
                    Message::AppendEntries {
                        prev_log_index,
                        entries,
                        ..
                    },
            } if *to == peer => Some((*prev_log_index, entries.len())),
            _ => None,
        })
        .collect()
}

/// Step `count` proposals into the leader (distinct clients, one
/// sequence each) and return the actions of the single persist
/// acknowledgement that releases them all.
fn propose_batch(nodes: &mut BTreeMap<NodeId, Node>, count: u64) -> Vec<Action> {
    for i in 1..=count {
        let actions = nodes
            .get_mut(&NodeId(1))
            .expect("leader exists")
            .step(Event::Propose {
                client_id: ClientId(i),
                seq: 1,
                payload: vec![7_u8; 16],
            });
        // Nothing is sent before the persist acknowledgement:
        // every proposal contributes exactly its persist request.
        assert_eq!(
            actions
                .iter()
                .filter(|a| matches!(a, Action::Send { .. }))
                .count(),
            0,
            "proposal {i} sent before its persist acknowledgement"
        );
    }
    let last = nodes
        .get(&NodeId(1))
        .expect("leader exists")
        .log()
        .last_index();
    nodes
        .get_mut(&NodeId(1))
        .expect("leader exists")
        .step(Event::Persisted {
            kind: PersistKind::Entries,
            up_to_index: last,
        })
}

#[test]
fn proposals_released_by_one_persist_ack_coalesce_into_one_append_round() {
    let mut nodes = cluster();
    elect(&mut nodes);
    let released = propose_batch(&mut nodes, 8);
    for peer in [NodeId(2), NodeId(3)] {
        let rounds = append_rounds(&released, peer);
        assert_eq!(
            rounds.len(),
            1,
            "B-02: one persist release must broadcast one AppendEntries \
             round per follower, got {rounds:?} for {peer}"
        );
        assert_eq!(
            rounds[0],
            (LogIndex(0), 9),
            "B-02: the single round carries the leader NoOp and \
             all eight proposals"
        );
    }
}

#[test]
fn a_single_proposal_still_sends_one_round() {
    let mut nodes = cluster();
    elect(&mut nodes);
    let released = propose_batch(&mut nodes, 1);
    for peer in [NodeId(2), NodeId(3)] {
        assert_eq!(
            append_rounds(&released, peer),
            vec![(LogIndex(0), 2)],
            "a lone proposal sends exactly one round, carrying \
             the leader NoOp and the proposal"
        );
    }
}

/// Route actions between the nodes, acknowledging persists
/// immediately, until the cluster is quiescent; returns the
/// (client, sequence) pair of every `Committed` client response
/// any node emitted on the way. Every node that applies a
/// command entry emits that entry's response, so the same pair
/// arrives once per applying node and drivers deduplicate by
/// client and sequence.
fn route(nodes: &mut BTreeMap<NodeId, Node>, actions: Vec<Action>) -> Vec<(ClientId, u64)> {
    let mut responses = Vec::new();
    let mut queue: Vec<(NodeId, Event)> = Vec::new();
    let collect = |queue: &mut Vec<(NodeId, Event)>,
                   responses: &mut Vec<(ClientId, u64)>,
                   source: NodeId,
                   actions: Vec<Action>| {
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
                Action::ClientResponse {
                    client_id,
                    seq,
                    result: ClientResult::Committed,
                } => responses.push((client_id, seq)),
                Action::ClientResponse { .. } => {}
                _ => {}
            }
        }
    };
    collect(&mut queue, &mut responses, NodeId(1), actions);
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
        collect(&mut queue, &mut responses, to, actions);
    }
    responses
}

#[test]
fn a_coalesced_batch_still_commits_every_proposal() {
    let mut nodes = cluster();
    elect(&mut nodes);
    // Drive the batch the way a driver would: propose, persist,
    // then route everything the leader emits until quiescence.
    let mut actions = Vec::new();
    for i in 1..=8_u64 {
        actions.extend(
            nodes
                .get_mut(&NodeId(1))
                .expect("leader exists")
                .step(Event::Propose {
                    client_id: ClientId(i),
                    seq: 1,
                    payload: vec![7_u8; 16],
                }),
        );
    }
    let last = nodes
        .get(&NodeId(1))
        .expect("leader exists")
        .log()
        .last_index();
    actions.extend(
        nodes
            .get_mut(&NodeId(1))
            .expect("leader exists")
            .step(Event::Persisted {
                kind: PersistKind::Entries,
                up_to_index: last,
            }),
    );
    let responses = route(&mut nodes, actions);
    assert_eq!(
        nodes.get(&NodeId(1)).expect("leader exists").commit_index(),
        LogIndex(9),
        "the leader NoOp and all eight proposals commit"
    );
    for i in 1..=8_u64 {
        assert!(
            responses.contains(&(ClientId(i), 1)),
            "proposal {i} is answered Committed"
        );
    }
    let mut distinct: Vec<(ClientId, u64)> = Vec::new();
    for pair in &responses {
        if !distinct.contains(pair) {
            distinct.push(*pair);
        }
    }
    assert_eq!(
        distinct.len(),
        8,
        "exactly the eight proposals are answered Committed"
    );
}
