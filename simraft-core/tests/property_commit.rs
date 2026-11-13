//! Commitment properties (invariants S-05 and S-06): the Figure 8
//! rule and same-index same-entry apply consistency.

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use simraft_core::{
    Action, ClientId, EntryKind, Event, HardState, LogEntry, LogIndex, Message, Node, NodeId,
    PersistKind, Term,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// A cluster driver with a two-cell partition filter: sends are
/// delivered only within a cell, modelling a network partition.
struct Filtered {
    nodes: BTreeMap<NodeId, Node>,
    applies: BTreeMap<NodeId, Vec<(LogIndex, LogEntry)>>,
}

impl Filtered {
    fn new(ids: &[u64], seed: u64) -> Self {
        Self {
            applies: ids.iter().map(|&i| (NodeId(i), Vec::new())).collect(),
            nodes: common::build_nodes(ids, seed),
        }
    }

    fn step(&mut self, id: NodeId, event: Event, partition: Option<(&[u64], &[u64])>) {
        // A send is delivered only when sender and recipient sit in
        // the same partition cell. None connects everyone.
        let cells: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)> = partition.map(|(a, b)| {
            (
                a.iter().map(|&i| NodeId(i)).collect(),
                b.iter().map(|&i| NodeId(i)).collect(),
            )
        });
        let mut queue: VecDeque<(NodeId, Event)> = VecDeque::new();
        queue.push_back((id, event));
        while let Some((node_id, ev)) = queue.pop_front() {
            let actions = self.nodes.get_mut(&node_id).expect("cluster node").step(ev);
            for action in actions {
                match action {
                    Action::Send { to, msg } => {
                        let connected = cells.as_ref().is_none_or(|(a, b)| {
                            (a.contains(&node_id) && a.contains(&to))
                                || (b.contains(&node_id) && b.contains(&to))
                        });
                        if connected {
                            queue.push_back((to, Event::Message { from: node_id, msg }));
                        }
                    }
                    Action::PersistHardState { .. } => queue.push_back((
                        node_id,
                        Event::Persisted {
                            kind: PersistKind::HardState,
                            up_to_index: LogIndex::ZERO,
                        },
                    )),
                    Action::PersistEntries {
                        from_index,
                        entries,
                    } => {
                        let up_to = if entries.is_empty() {
                            LogIndex(from_index.0.saturating_sub(1))
                        } else {
                            LogIndex(from_index.0 + entries.len() as u64 - 1)
                        };
                        queue.push_back((
                            node_id,
                            Event::Persisted {
                                kind: PersistKind::Entries,
                                up_to_index: up_to,
                            },
                        ));
                    }
                    Action::Apply { index, entry } => {
                        self.applies
                            .get_mut(&node_id)
                            .expect("cluster node")
                            .push((index, entry));
                    }
                    Action::ClientResponse { .. }
                    | Action::StepDown { .. }
                    | Action::ReadReady { .. }
                    | Action::ReadRejected { .. } => {}
                }
            }
        }
    }
}

#[test]
fn figure8_old_term_entry_not_committed_by_counting() {
    let cluster = common::all_voters(&[1, 2, 3, 4, 5]);
    let mut leader = Node::new(NodeId(1), simraft_core::Config::default(), cluster, 31).unwrap();
    let _ = leader.step(Event::Restored {
        hard_state: HardState {
            current_term: Term(1),
            voted_for: None,
        },
        entries: vec![LogEntry {
            term: Term(1),
            kind: EntryKind::Command {
                client_id: ClientId(1),
                seq: 1,
                payload: b"old".to_vec(),
            },
        }],
    });
    let _ = common::elect(&mut leader, &[NodeId(2), NodeId(3), NodeId(4)]);
    assert_eq!(leader.current_term(), Term(2));
    assert_eq!(leader.log().last_index(), LogIndex(2));

    for peer in [2_u64, 3] {
        let _ = leader.step(Event::Message {
            from: NodeId(peer),
            msg: Message::AppendEntriesResponse {
                term: Term(2),
                success: true,
                match_index: LogIndex(1),
                conflict_index: LogIndex(0),
                conflict_term: Term(0),
                read_round: 0,
            },
        });
    }
    assert_eq!(
        leader.commit_index(),
        LogIndex(0),
        "a majority holding only a previous-term entry must not commit it (Figure 8)"
    );

    for peer in [2_u64, 3] {
        let _ = leader.step(Event::Message {
            from: NodeId(peer),
            msg: Message::AppendEntriesResponse {
                term: Term(2),
                success: true,
                match_index: LogIndex(2),
                conflict_index: LogIndex(0),
                conflict_term: Term(0),
                read_round: 0,
            },
        });
    }
    assert_eq!(leader.commit_index(), LogIndex(2));
}

#[test]
fn current_term_entry_commits_by_counting() {
    let mut harness = common::Harness::new(&[1, 2, 3], 77);
    harness.run_until(3_000, 25);
    let leader_id = harness.leaders()[0].0;
    harness.step_node(
        leader_id,
        Event::Propose {
            client_id: ClientId(3),
            seq: 1,
            payload: b"v".to_vec(),
        },
    );
    harness.run_until(400, 25);
    for node in harness.nodes.values() {
        assert_eq!(
            node.commit_index(),
            LogIndex(2),
            "every node commits the index-2 entry once a majority holds it"
        );
    }
}

#[test]
fn figure8_overwritten_entry_never_applies() {
    // The full Figure 8 staging on five nodes. Roles are picked from
    // whoever wins each election, so the test does not depend on which
    // node a seed happens to elect first.
    let all = [1_u64, 2, 3, 4, 5];
    let mut fc = Filtered::new(&all, 55);
    let tick_all = |fc: &mut Filtered, now: u64, partition: Option<(&[u64], &[u64])>| {
        for &id in &all {
            fc.step(NodeId(id), Event::Tick { now }, partition);
        }
    };
    // Phase 1: full delivery until some node leads term 1.
    let mut now = 0_u64;
    let mut leader_a = None;
    while now <= 6_000 {
        tick_all(&mut fc, now, None);
        let leaders: Vec<NodeId> = fc
            .nodes
            .values()
            .filter(|n| n.role() == simraft_core::Role::Leader)
            .map(simraft_core::Node::id)
            .collect();
        if leaders.len() == 1 {
            leader_a = Some(leaders[0]);
            break;
        }
        now += 25;
    }
    let leader_a = leader_a.expect("a term-1 leader emerges");
    let others: Vec<u64> = all
        .iter()
        .copied()
        .filter(|&i| NodeId(i) != leader_a)
        .collect();
    let f1 = others[0];
    // Phase 2: the leader proposes X at index 2; only one follower
    // receives it. Two of five replicas: X stays uncommitted.
    let side_a: Vec<u64> = vec![leader_a.0, f1];
    let side_b: Vec<u64> = others[1..].to_vec();
    fc.step(
        leader_a,
        Event::Propose {
            client_id: ClientId(1),
            seq: 1,
            payload: b"x".to_vec(),
        },
        Some((&side_a, &side_b)),
    );
    assert!(
        fc.nodes[&leader_a].commit_index() < LogIndex(2),
        "X at index 2 has two of five replicas and must not commit"
    );
    // Phase 3: the other three nodes are isolated with the first two
    // and elect their own leader in term 2, committing its term-2 NoOp
    // at index 2 by counting.
    let group: Vec<u64> = others[1..].to_vec();
    assert_eq!(group.len(), 3);
    let mut leader_b = None;
    while now <= 12_000 {
        tick_all(&mut fc, now, Some((&side_a, &side_b)));
        for &g in &group {
            if fc.nodes[&NodeId(g)].role() == simraft_core::Role::Leader {
                leader_b = Some(NodeId(g));
            }
        }
        if leader_b.is_some() {
            break;
        }
        now += 25;
    }
    let leader_b = leader_b.expect("the isolated group elects a term-2 leader");
    assert_eq!(fc.nodes[&leader_b].current_term(), Term(2));
    assert_eq!(
        fc.nodes[&leader_b].commit_index(),
        LogIndex(2),
        "the term-2 entry at index 2 commits on its own replicas"
    );
    // Phase 4: heal. The old leader is overwritten at index 2.
    for _ in 0..20 {
        now += 25;
        tick_all(&mut fc, now, None);
    }
    let overwritten = &fc.nodes[&leader_a].log().entries()[1];
    assert_eq!(
        overwritten.term,
        Term(2),
        "index 2 on the old leader must be the term-2 entry"
    );
    assert_eq!(overwritten.kind, EntryKind::NoOp);
    for (id, applies) in &fc.applies {
        for (_, entry) in applies {
            if let EntryKind::Command { payload, .. } = &entry.kind {
                assert_ne!(payload, b"x", "node {id} applied the overwritten entry");
            }
        }
    }
}

#[test]
fn same_index_applies_same_entry_on_all_nodes() {
    let mut harness = common::Harness::new(&[1, 2, 3], 78);
    harness.run_until(3_000, 25);
    let leader_id = harness.leaders()[0].0;
    for seq in 1_u64..=4 {
        harness.step_node(
            leader_id,
            Event::Propose {
                client_id: ClientId(3),
                seq,
                payload: vec![u8::try_from(seq).expect("seq fits")],
            },
        );
        harness.run_until(300, 25);
    }
    let reference = &harness.applies[&leader_id];
    assert!(reference.len() >= 5, "leader applied its entries");
    for (id, applies) in &harness.applies {
        assert_eq!(
            applies, reference,
            "node {id} applied a different history than the leader"
        );
    }
}
