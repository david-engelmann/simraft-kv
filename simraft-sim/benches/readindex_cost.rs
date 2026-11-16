#![allow(
    missing_docs,
    reason = "the criterion_group macro generates the bench entry point, which cannot carry a doc comment"
)]

//! `ReadIndex` cost bound (invariant L-01): a `ReadIndex` read costs
//! at most one heartbeat round of messages (one `AppendEntries` and
//! one response per config member) and zero log appends and zero
//! persists on the leader. Measured, not assumed: this bench
//! drives a three-node cluster through one read and counts the
//! leader's sends, appends, and persists during the round; a
//! count above the bound fails the bench (and CI's bench smoke
//! run, which executes the same assertions once).

use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use simraft_core::{
    Action, ClientId, ClusterConfig, Config, Event, LogIndex, Message, Node, NodeId, PersistKind,
    Role,
};
use simraft_kv::{KvOp, encode_command};

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

fn elect(nodes: &mut BTreeMap<NodeId, Node>, id: NodeId) {
    let node = nodes.get_mut(&id).expect("node exists");
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
}

/// Route actions until quiescent, counting (sends, persists) that
/// the node `count_from` emits along the way.
fn route_counting(
    nodes: &mut BTreeMap<NodeId, Node>,
    from: NodeId,
    actions: Vec<Action>,
    count_from: NodeId,
) -> (usize, usize) {
    let mut sends = 0_usize;
    let mut persists = 0_usize;
    let mut queue: Vec<(NodeId, Event)> = Vec::new();
    let mut collect = |queue: &mut Vec<(NodeId, Event)>, source: NodeId, actions: Vec<Action>| {
        for action in actions {
            match action {
                Action::Send { to, msg } => {
                    if source == count_from {
                        sends += 1;
                    }
                    queue.push((to, Event::Message { from: source, msg }));
                }
                Action::PersistHardState { .. } => {
                    if source == count_from {
                        persists += 1;
                    }
                    queue.push((
                        source,
                        Event::Persisted {
                            kind: PersistKind::HardState,
                            up_to_index: LogIndex::ZERO,
                        },
                    ));
                }
                Action::PersistEntries { .. } => {
                    if source == count_from {
                        persists += 1;
                    }
                    queue.push((
                        source,
                        Event::Persisted {
                            kind: PersistKind::Entries,
                            up_to_index: LogIndex::ZERO,
                        },
                    ));
                }
                _ => {}
            }
        }
    };
    collect(&mut queue, from, actions);
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
    (sends, persists)
}

fn bench_readindex_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("readindex_cost");
    group.bench_function("one_read_round", |b| {
        b.iter(|| {
            let mut nodes = cluster();
            elect(&mut nodes, NodeId(1));
            // Commit one write so the leader's first-term gate is
            // satisfied and reads can be served.
            let payload = encode_command(
                ClientId(1),
                1,
                &KvOp::Put {
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                },
            );
            let actions = nodes
                .get_mut(&NodeId(1))
                .expect("leader")
                .step(Event::Propose {
                    client_id: ClientId(1),
                    seq: 1,
                    payload,
                });
            route_counting(&mut nodes, NodeId(1), actions, NodeId(99));
            // The read round itself.
            let actions = nodes
                .get_mut(&NodeId(1))
                .expect("leader")
                .step(Event::ReadIndex {
                    request_id: 1,
                    key: b"k".to_vec(),
                });
            let (sends, persists) = route_counting(&mut nodes, NodeId(1), actions, NodeId(1));
            // One AppendEntries per config member is the bound
            // (the leader sends to the two followers; responses are
            // the followers' sends, not the leader's).
            assert!(
                sends <= 2,
                "L-01: the leader sent {sends} messages for one read, bound is one per member"
            );
            assert_eq!(
                persists, 0,
                "L-01: a read must cost zero appends and persists on the leader"
            );
            black_box((sends, persists))
        });
    });
    group.finish();
}

criterion_group!(benches, bench_readindex_cost);
criterion_main!(benches);
