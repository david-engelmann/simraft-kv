#![allow(
    missing_docs,
    reason = "the criterion_group macro generates the bench entry point, which cannot carry a doc comment"
)]

//! Replication throughput baseline (invariant B-01): `AppendEntries`
//! replication throughput in entries per second and step latency
//! per event class, measured with criterion over a three-node
//! cluster driven directly (no network model). Inputs and outputs
//! are wrapped in `black_box` and throughput is configured per
//! group. Numbers land in the README only from a recorded run of
//! this benchmark, never estimated.
//!
//! Post-M4 Lane 3 adds two measurements: `propose_commit_batch_32`,
//! the before and after yardstick for leader batching (invariant
//! B-02), and `catch_up_ten_thousand_entries`, the cost of
//! bringing an empty follower up to a 10,000-entry log.

use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use simraft_core::{
    Action, ClientId, ClusterConfig, Config, Event, LogIndex, Message, Node, NodeId, PersistKind,
    Role,
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

/// Elect `id` leader with direct response steps (the same shape
/// the core test harness uses).
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
    assert_eq!(node.role(), Role::Leader, "the bench cluster elects");
    // Release the leader's NoOp persist so proposals flow.
    let last = node.log().last_index();
    node.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: last,
    });
}

/// Route one node's actions to their targets, acknowledging
/// persists immediately, until the cluster is quiescent.
fn route(nodes: &mut BTreeMap<NodeId, Node>, from: NodeId, actions: Vec<Action>) {
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
    collect(&mut queue, from, actions);
    while let Some((to, event)) = queue.pop() {
        // Entries persists release by the node's current tail.
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

fn bench_replication(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("replication");
        group.throughput(Throughput::Elements(1));
        group.bench_function("propose_commit_round", |b| {
            let mut nodes = cluster();
            elect(&mut nodes, NodeId(1));
            let mut seq = 0_u64;
            b.iter(|| {
                seq += 1;
                let actions =
                    nodes
                        .get_mut(&NodeId(1))
                        .expect("leader exists")
                        .step(Event::Propose {
                            client_id: ClientId(1),
                            seq,
                            payload: black_box(vec![1_u8; 32]),
                        });
                route(&mut nodes, NodeId(1), actions);
                black_box(nodes.get(&NodeId(1)).expect("leader").commit_index())
            });
        });
        group.finish();
    }

    let mut group = c.benchmark_group("step_latency");
    group.bench_function("follower_append_entries", |b| {
        let mut nodes = cluster();
        elect(&mut nodes, NodeId(1));
        // The proposal's sends are released only once the entry
        // is persisted (persist-before-response), so acknowledge
        // the persist first and take the AppendEntries from the
        // released actions.
        nodes
            .get_mut(&NodeId(1))
            .expect("leader exists")
            .step(Event::Propose {
                client_id: ClientId(1),
                seq: 1,
                payload: vec![9_u8; 32],
            });
        let last = nodes
            .get(&NodeId(1))
            .expect("leader exists")
            .log()
            .last_index();
        let actions = nodes
            .get_mut(&NodeId(1))
            .expect("leader exists")
            .step(Event::Persisted {
                kind: PersistKind::Entries,
                up_to_index: last,
            });
        let message = actions
            .iter()
            .find_map(|a| match a {
                Action::Send {
                    to,
                    msg: m @ Message::AppendEntries { .. },
                } if *to == NodeId(2) => Some(m.clone()),
                _ => None,
            })
            .expect("the leader sends AppendEntries");
        b.iter(|| {
            let out = nodes
                .get_mut(&NodeId(2))
                .expect("follower")
                .step(Event::Message {
                    from: NodeId(1),
                    msg: black_box(message.clone()),
                });
            black_box(out.len())
        });
    });
    {
        let mut group = c.benchmark_group("replication_batched");
        group.throughput(Throughput::Elements(32));
        group.bench_function("propose_commit_batch_32", |b| {
            let mut nodes = cluster();
            elect(&mut nodes, NodeId(1));
            let mut seq = 0_u64;
            b.iter(|| {
                // Thirty-two proposals stepped back to back, then
                // routed once: the persist release and the rounds
                // it produces are the B-02 measurement.
                let mut actions = Vec::new();
                for _ in 0..32 {
                    seq += 1;
                    actions.extend(nodes.get_mut(&NodeId(1)).expect("leader exists").step(
                        Event::Propose {
                            client_id: ClientId(1),
                            seq,
                            payload: black_box(vec![1_u8; 32]),
                        },
                    ));
                }
                route(&mut nodes, NodeId(1), actions);
                black_box(nodes.get(&NodeId(1)).expect("leader").commit_index())
            });
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("replication_catch_up");
        group.throughput(Throughput::Elements(10_000));
        group.sample_size(10);
        group.bench_function("catch_up_ten_thousand_entries", |b| {
            b.iter_batched(
                || {
                    let mut nodes = cluster();
                    elect(&mut nodes, NodeId(1));
                    // Fill the leader's log to 10,000 entries past
                    // its NoOp, acknowledging each persist and
                    // discarding the released sends: the followers
                    // never see them and stay empty.
                    for seq in 1..=10_000_u64 {
                        let leader = nodes.get_mut(&NodeId(1)).expect("leader exists");
                        leader.step(Event::Propose {
                            client_id: ClientId(1),
                            seq,
                            payload: vec![1_u8; 32],
                        });
                        let last = leader.log().last_index();
                        leader.step(Event::Persisted {
                            kind: PersistKind::Entries,
                            up_to_index: last,
                        });
                    }
                    nodes
                },
                |mut nodes| {
                    // Drive leader heartbeats until node 3 holds
                    // the whole log.
                    let target = nodes
                        .get(&NodeId(1))
                        .expect("leader exists")
                        .log()
                        .last_index();
                    let mut now = 1_000_000_u64;
                    for _ in 0..10_000 {
                        now += 50;
                        let actions = nodes
                            .get_mut(&NodeId(1))
                            .expect("leader exists")
                            .step(Event::Tick { now });
                        route(&mut nodes, NodeId(1), actions);
                        if nodes.get(&NodeId(3)).expect("follower").log().last_index() == target {
                            break;
                        }
                    }
                    black_box(nodes.get(&NodeId(3)).expect("follower").log().last_index())
                },
                BatchSize::SmallInput,
            );
        });
        group.finish();
    }

    group.finish();
}

criterion_group!(benches, bench_replication);
criterion_main!(benches);
