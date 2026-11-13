//! Shared test drivers: a small routed cluster with automatic persist
//! acknowledgment, plus a manual election helper for single-node tests.
//!
//! These drivers are deliberately simple versions of what simraft-sim
//! generalizes at M4: messages route immediately, persists acknowledge
//! immediately, and time only moves when a test ticks the cluster.

use simraft_core::{
    Action, ClusterConfig, Config, Event, LogEntry, LogIndex, Message, Node, NodeId, PersistKind,
    Role, Term,
};
use std::collections::{BTreeMap, VecDeque};

/// Cluster configuration where every listed node is a voter.
pub fn all_voters(ids: &[u64]) -> ClusterConfig {
    ClusterConfig::new(ids.iter().map(|&i| NodeId(i)), std::iter::empty())
        .expect("test cluster has voters")
}

/// Build one node per id with default timing configuration. Each node
/// gets a distinct deterministic seed derived from `seed`.
pub fn build_nodes(ids: &[u64], seed: u64) -> BTreeMap<NodeId, Node> {
    let cluster = all_voters(ids);
    ids.iter()
        .map(|&i| {
            (
                NodeId(i),
                Node::new(
                    NodeId(i),
                    Config::default(),
                    cluster.clone(),
                    seed ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                )
                .expect("default config validates"),
            )
        })
        .collect()
}

/// A routed test cluster with automatic persist acknowledgment.
pub struct Harness {
    /// The nodes, by id.
    pub nodes: BTreeMap<NodeId, Node>,
    /// Applied entries per node, in apply order.
    pub applies: BTreeMap<NodeId, Vec<(LogIndex, LogEntry)>>,
    /// Client responses observed, as (answering node, action) pairs.
    pub client_responses: Vec<(NodeId, Action)>,
}

impl Harness {
    /// A new cluster over `ids`.
    pub fn new(ids: &[u64], seed: u64) -> Self {
        Self {
            applies: ids.iter().map(|&i| (NodeId(i), Vec::new())).collect(),
            nodes: build_nodes(ids, seed),
            client_responses: Vec::new(),
        }
    }

    /// Feed one event to one node and route everything it produces
    /// until the cluster is quiet.
    pub fn step_node(&mut self, id: NodeId, event: Event) {
        let mut queue: VecDeque<(NodeId, Event)> = VecDeque::new();
        queue.push_back((id, event));
        while let Some((node_id, ev)) = queue.pop_front() {
            let actions = self
                .nodes
                .get_mut(&node_id)
                .expect("event for a cluster node")
                .step(ev);
            for action in actions {
                self.route(node_id, action, &mut queue);
            }
        }
    }

    fn route(
        &mut self,
        source: NodeId,
        action: Action,
        queue: &mut VecDeque<(NodeId, Event)>,
    ) {
        match action {
            Action::Send { to, msg } => queue.push_back((
                to,
                Event::Message {
                    from: source,
                    msg,
                },
            )),
            Action::PersistHardState { .. } => queue.push_back((
                source,
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
                    source,
                    Event::Persisted {
                        kind: PersistKind::Entries,
                        up_to_index: up_to,
                    },
                ));
            }
            Action::Apply { index, entry } => {
                self.applies
                    .get_mut(&source)
                    .expect("apply for a cluster node")
                    .push((index, entry));
            }
            Action::ClientResponse { .. } => {
                self.client_responses.push((source, action));
            }
            Action::StepDown { .. } => {}
        }
    }

    /// Tick every node at `now`, in ascending id order.
    pub fn tick(&mut self, now: u64) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            self.step_node(id, Event::Tick { now });
        }
    }

    /// Advance time in `step` increments up to `until`, ticking the
    /// whole cluster at each step.
    pub fn run_until(&mut self, until: u64, step: u64) {
        let mut now = 0;
        while now <= until {
            self.tick(now);
            now += step;
        }
    }

    /// Current leaders as (node, term) pairs.
    pub fn leaders(&self) -> Vec<(NodeId, Term)> {
        self.nodes
            .values()
            .filter(|n| n.role() == Role::Leader)
            .map(|n| (n.id(), n.current_term()))
            .collect()
    }
}

/// Drive one node through a complete election by hand: tick it past its
/// deadline, answer its pre-votes and votes with grants from `peers`
/// (the caller picks enough for a quorum), and acknowledge its persists.
/// Returns the final actions produced when it becomes leader (expected
/// to include the persist for its election NoOp entry).
pub fn elect(node: &mut Node, peers: &[NodeId]) -> Vec<Action> {
    let tick_actions = node.step(Event::Tick { now: 1_000_000 });
    assert!(
        tick_actions.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestPreVote { .. },
                ..
            }
        )),
        "expected pre-vote requests after the election deadline, got {tick_actions:?}"
    );
    let mut out = Vec::new();
    for &peer in peers {
        out = node.step(Event::Message {
            from: peer,
            msg: Message::RequestPreVoteResponse {
                term: node.current_term(),
                vote_granted: true,
            },
        });
        if node.role() == Role::Candidate {
            break;
        }
    }
    assert_eq!(
        node.role(),
        Role::Candidate,
        "pre-vote majority must promote the node to candidate"
    );
    // The candidate's self-vote persist must be acknowledged before its
    // RequestVote broadcast is released.
    let released = node.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    assert!(
        released.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestVote { .. },
                ..
            }
        )),
        "expected RequestVote broadcast after the hard state persist, got {released:?}"
    );
    let vote_term = node.current_term();
    for &peer in peers {
        out = node.step(Event::Message {
            from: peer,
            msg: Message::RequestVoteResponse {
                term: vote_term,
                vote_granted: true,
            },
        });
        if node.role() == Role::Leader {
            break;
        }
    }
    assert_eq!(
        node.role(),
        Role::Leader,
        "vote majority must promote the candidate to leader"
    );
    out
}
