//! A worked embedding of `simraft-core` in one file.
//!
//! Three nodes run in one process. A queue between them is the
//! transport, persist actions are acknowledged in line, and a
//! miniature state machine per node, a map from key to value that
//! interprets command payloads, shows where a driver's own code
//! plugs in. The example elects a leader, writes one key through
//! it, reads the value back from the applied state on every node,
//! and prints the leader's stats snapshot at the end.
//!
//! Run it with `cargo run -p simraft-core --example embed`. It
//! performs no I/O beyond printing and exits nonzero if any of
//! its checks fail.

use std::collections::{BTreeMap, VecDeque};

use simraft_core::{
    Action, ClientId, ClientResult, ClusterConfig, Config, EntryKind, Event, LogIndex, Node,
    NodeId, PersistKind, Role,
};

/// One cluster in one process: the nodes, the transport queue,
/// one applied key-value store per node, and whether the client
/// write has been answered committed.
struct Driver {
    nodes: BTreeMap<NodeId, Node>,
    queue: VecDeque<(NodeId, Event)>,
    stores: BTreeMap<NodeId, BTreeMap<String, String>>,
    committed: bool,
}

impl Driver {
    fn new() -> Self {
        let cfg = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("config validates");
        let mut nodes = BTreeMap::new();
        let mut stores = BTreeMap::new();
        for id in [1_u64, 2, 3] {
            nodes.insert(
                NodeId(id),
                Node::new(NodeId(id), Config::default(), cfg.clone(), id * 11)
                    .expect("node builds"),
            );
            stores.insert(NodeId(id), BTreeMap::new());
        }
        Self {
            nodes,
            queue: VecDeque::new(),
            stores,
            committed: false,
        }
    }

    /// Execute one node's actions: sends join the transport
    /// queue, persists are acknowledged back to their source at
    /// once, applies feed that node's store, and a committed
    /// client response is remembered. This workload produces no
    /// snapshots, reads, or config changes, so no other action
    /// kinds arise.
    fn handle(&mut self, source: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, msg } => {
                    self.queue.push_back((to, Event::Message { from: source, msg }));
                }
                Action::PersistHardState { .. } => {
                    self.queue.push_back((
                        source,
                        Event::Persisted {
                            kind: PersistKind::HardState,
                            up_to_index: LogIndex::ZERO,
                        },
                    ));
                }
                Action::PersistEntries { .. } => {
                    let up_to = self
                        .nodes
                        .get(&source)
                        .expect("source exists")
                        .log()
                        .last_index();
                    self.queue.push_back((
                        source,
                        Event::Persisted {
                            kind: PersistKind::Entries,
                            up_to_index: up_to,
                        },
                    ));
                }
                Action::Apply { entry, .. } => {
                    if let EntryKind::Command { payload, .. } = entry.kind {
                        let text = std::str::from_utf8(&payload).expect("payload is text");
                        let (key, value) = text.split_once('=').expect("payload is key=value");
                        self.stores
                            .get_mut(&source)
                            .expect("store exists")
                            .insert(key.to_owned(), value.to_owned());
                    }
                }
                Action::ClientResponse {
                    result: ClientResult::Committed,
                    ..
                } => {
                    self.committed = true;
                }
                _ => {}
            }
        }
    }

    /// Step one event into one node and execute its actions.
    fn step(&mut self, node: NodeId, event: Event) {
        let actions = self.nodes.get_mut(&node).expect("node exists").step(event);
        self.handle(node, actions);
    }

    /// Deliver the transport queue until it is empty.
    fn drain(&mut self) {
        while let Some((to, event)) = self.queue.pop_front() {
            self.step(to, event);
        }
    }

    /// The current leader, read from the stats snapshots.
    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .values()
            .find(|node| node.stats().role == Role::Leader)
            .and_then(|node| node.stats().leader)
    }
}

fn main() {
    let mut driver = Driver::new();

    // Tick every node in rounds until one of them leads. The
    // seeded election timers decide who wins; the transport is
    // drained after every step, so votes and heartbeats land.
    let mut now = 0_u64;
    let leader = loop {
        now += 100;
        for id in [NodeId(1), NodeId(2), NodeId(3)] {
            driver.step(id, Event::Tick { now });
            driver.drain();
        }
        if let Some(leader) = driver.leader() {
            break leader;
        }
        assert!(now < 20_000, "the cluster elects a leader");
    };
    let term = driver
        .nodes
        .get(&leader)
        .expect("leader exists")
        .stats()
        .current_term;
    println!("embed: {leader} leads at {term}");

    // Write one key through the leader, then tick it once more so
    // the followers learn the commit and apply the entry too.
    driver.step(
        leader,
        Event::Propose {
            client_id: ClientId(1),
            seq: 1,
            payload: b"color=blue".to_vec(),
        },
    );
    driver.drain();
    now += 100;
    driver.step(leader, Event::Tick { now });
    driver.drain();
    assert!(driver.committed, "the write commits");

    // Read the value back from every node's applied store.
    for id in [NodeId(1), NodeId(2), NodeId(3)] {
        let value = driver
            .stores
            .get(&id)
            .expect("store exists")
            .get("color")
            .expect("the key is applied");
        assert_eq!(value, "blue", "every node applies the write");
    }
    println!("embed: wrote color=blue and read it back on all three nodes");

    let stats = driver.nodes.get(&leader).expect("leader exists").stats();
    println!(
        "embed: leader stats role={:?} term={} leader={} commit={} applied={} \
         last_log={} snapshot_base={} peers={}",
        stats.role,
        stats.current_term.0,
        stats.leader.expect("a leader acknowledges itself"),
        stats.commit_index.0,
        stats.last_applied.0,
        stats.last_log_index.0,
        stats.snapshot_index.0,
        stats.peers.len()
    );
    assert_eq!(stats.commit_index, LogIndex(2), "the NoOp and the write");
    assert_eq!(stats.last_applied, LogIndex(2));
}
