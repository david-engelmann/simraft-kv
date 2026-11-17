//! The multi-node driver loop.
//!
//! Docs/ARCHITECTURE.md sections 12.1 to 12.5: many simraft-core
//! nodes, each with its simraft-kv state machine and its
//! simulated disk, driven over a virtual clock and a
//! fault-injecting packet network, with the safety oracles
//! checked after every event and the client history checked for
//! linearizability at the end of the run.
//!
//! A run has two phases: chaos (faults on, crashes and clock jumps
//! allowed) and bounded healing (no faults, latency bounded below
//! `min_election_timeout` / 4), after which the harness asserts one
//! stable leader, identical logs and state digests on every live
//! node, and a linearizable history (invariant H-02).

use std::collections::BTreeMap;

use simraft_core::{
    Action, ClientId, ClusterConfig, Config, ConfigChange, EntryKind, Event, HardState, LogIndex,
    Message, Node, NodeId, PersistKind, Role, SnapshotMeta, Term,
};
use simraft_kv::{
    ApplyOutcome, CommandResult, KvOp, KvStateMachine, encode_command, encode_snapshot,
};

use crate::clock::EventQueue;
use crate::disk::SimDisk;
use crate::faults::{FaultConfig, ScriptedOp};
use crate::history::{History, Op, OpResult};
use crate::liz::{Checker, Verdict};
use crate::oracle::{NodeView, OracleState, OracleViolation};
use crate::rng::Rng;
use crate::trace::{Trace, digest_hex};
use crate::wire::{decode_frame, encode_frame};

/// Everything a run is configured with. The seed alone decides the
/// run: the same seed always produces the same trace, the same
/// history, and the same final digest (invariant D-01).
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// The run's seed.
    pub seed: u64,
    /// The number of nodes (all voters at genesis).
    pub node_count: usize,
    /// Extra nodes that join the genesis configuration as
    /// learners (post-M4 Lane 2): they run, replicate, and apply
    /// from tick zero but never vote or campaign until a config
    /// change promotes them.
    pub learners: Vec<NodeId>,
    /// The core protocol configuration for every node.
    pub core: Config,
    /// The length of the chaos phase, in ticks.
    pub chaos_ticks: u64,
    /// The length of the healing phase, in ticks.
    pub heal_ticks: u64,
    /// The fault knobs.
    pub faults: FaultConfig,
    /// The number of simulated clients issuing operations.
    pub clients: usize,
    /// The size of the workload's key space.
    pub keys: usize,
    /// Operations scheduled at exact ticks by the scenario.
    pub scripted: Vec<(u64, ScriptedOp)>,
    /// The linearizability checker's per-key step budget.
    pub checker_budget: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            node_count: 3,
            learners: Vec::new(),
            core: Config::default(),
            chaos_ticks: 4_000,
            heal_ticks: 4_000,
            faults: FaultConfig::default(),
            clients: 2,
            keys: 4,
            scripted: Vec::new(),
            checker_budget: 1_000_000,
        }
    }
}

/// The outcome of one run.
#[derive(Debug)]
pub struct RunReport {
    /// The seed that produced this report.
    pub seed: u64,
    /// Whether the run reached the end of its healing phase (false
    /// when an oracle stopped it early).
    pub completed: bool,
    /// The virtual ticks the run advanced through.
    pub ticks_run: u64,
    /// The leader at the end of the run, when one held.
    pub final_leader: Option<NodeId>,
    /// The final leader's term.
    pub final_leader_term: Option<Term>,
    /// Every leadership change observed, as (tick, leader, term).
    pub leader_history: Vec<(u64, NodeId, Term)>,
    /// Whether every live node ended with identical log digests
    /// and identical state digests.
    pub converged: bool,
    /// The SHA-256 over all nodes' canonical final state bytes, in
    /// node order (invariant D-01's comparison value).
    pub state_digest: String,
    /// The linearizability verdict over the recorded history.
    pub verdict: Verdict,
    /// The oracle failure that stopped the run, when one fired.
    pub oracle_failure: Option<OracleViolation>,
    /// The recorded client history.
    pub history: History,
    /// Every node's last applied index at the end of the run, in
    /// node order (learners included): the catch-up evidence a
    /// scenario asserts against the final commit index.
    pub final_applied: Vec<(NodeId, LogIndex)>,
    /// The largest Propose-to-Apply latency over completed client
    /// writes, in virtual ticks (invariant L-02's measurement).
    pub max_commit_latency_ticks: u64,
    /// The run's trace.
    pub trace: Trace,
}

impl RunReport {
    /// Whether the run is a full pass: completed, converged, no
    /// oracle failure, and a linearizable history.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.completed
            && self.converged
            && self.oracle_failure.is_none()
            && self.verdict == Verdict::Linearizable
    }
}

/// The simulator driver. Construct with a configuration, then run.
#[derive(Debug)]
pub struct Harness {
    config: SimConfig,
}

impl Harness {
    /// A harness for the given configuration.
    #[must_use]
    pub const fn new(config: SimConfig) -> Self {
        Self { config }
    }

    /// The configuration this harness runs.
    #[must_use]
    pub const fn config(&self) -> &SimConfig {
        &self.config
    }

    /// Run the chaos and healing phases and produce the report.
    pub fn run(&mut self) -> RunReport {
        let mut driver = Driver::new(&self.config);
        driver.run()
    }
}

/// How often each node is polled with a Tick, in virtual ticks.
const POLL_INTERVAL: u64 = 10;
/// How often the client workload driver fires, in virtual ticks.
const CLIENT_INTERVAL: u64 = 25;
/// Ticks without a client response before the client abandons the
/// operation (the record stays pending in the history).
const CLIENT_TIMEOUT: u64 = 600;
/// An fsync completes this many ticks after its write.
const FSYNC_DELAY: u64 = 1;
/// A crashed node restarts this many ticks later (plus a seeded
/// jitter of up to the same again).
const RESTART_DELAY: u64 = 100;
/// A node compacts its log into a snapshot once this many entries
/// sit above its base.
const COMPACT_THRESHOLD: u64 = 16;
/// Client operations stop being issued this many ticks before the
/// end, so replication settles before the convergence check.
const QUIESCE_TICKS: u64 = 300;
/// The per-machine client session capacity (the dedup table bound).
const SESSION_CAPACITY: usize = 1_024;

#[derive(Debug, Clone, Copy)]
enum FsyncKind {
    Hard,
    Entries,
    Snapshot,
}

#[derive(Debug)]
enum SimEvent {
    Deliver {
        to: NodeId,
        frame: Vec<u8>,
    },
    Poll {
        node: NodeId,
    },
    Fsync {
        node: NodeId,
        epoch: u64,
        kind: FsyncKind,
    },
    Restart {
        node: NodeId,
    },
    DriveClients,
}

#[derive(Debug)]
struct NodeSim {
    node: Node,
    machine: KvStateMachine,
    disk: SimDisk,
    alive: bool,
    epoch: u64,
    applied: Vec<(LogIndex, simraft_core::LogEntry)>,
    deferred_reads: Vec<(u64, Vec<u8>, LogIndex)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingKind {
    Write,
    Read,
}

#[derive(Debug, Clone)]
struct OpenOp {
    record: usize,
    client: usize,
    started: u64,
}

#[derive(Debug)]
struct ClientSim {
    id: ClientId,
    seq: u64,
    /// The client's in-flight operation: (sequence or request id,
    /// kind, start tick).
    open: Option<(u64, PendingKind, u64)>,
    counter: u64,
}

struct Driver<'a> {
    config: &'a SimConfig,
    rng: Rng,
    now: u64,
    queue: EventQueue<SimEvent>,
    nodes: BTreeMap<NodeId, NodeSim>,
    genesis: ClusterConfig,
    oracle: OracleState,
    history: History,
    trace: Trace,
    leader_history: Vec<(u64, NodeId, Term)>,
    current_leader: Option<(NodeId, Term)>,
    clients: Vec<ClientSim>,
    open_writes: BTreeMap<(ClientId, u64), OpenOp>,
    open_reads: BTreeMap<u64, OpenOp>,
    outcomes: BTreeMap<(ClientId, u64), OpResult>,
    next_request: u64,
    scripted: Vec<(u64, ScriptedOp)>,
    max_latency: u64,
    last_fault_tick: u64,
    chaos_end: u64,
    run_end: u64,
    /// Bounds recursive re-steps inside one dispatch (leader-hint
    /// retargets and snapshot serves), so two nodes holding stale
    /// hints at each other cannot spin the driver forever.
    chain_budget: u32,
    /// Whether the healing phase has begun (its end is budgeted
    /// from that moment).
    heal_started: bool,
}

impl<'a> Driver<'a> {
    fn new(config: &'a SimConfig) -> Self {
        let ids: Vec<NodeId> = (1..=config.node_count as u64).map(NodeId).collect();
        let genesis = ClusterConfig::new(ids.clone(), config.learners.clone())
            .expect("the genesis config of a run always validates");
        let mut nodes = BTreeMap::new();
        for id in ids.iter().copied().chain(config.learners.iter().copied()) {
            let node = Node::new(id, config.core.clone(), genesis.clone(), config.seed ^ id.0)
                .expect("the run's core config always validates");
            nodes.insert(
                id,
                NodeSim {
                    node,
                    machine: KvStateMachine::new(SESSION_CAPACITY),
                    disk: SimDisk::new(),
                    alive: true,
                    epoch: 0,
                    applied: Vec::new(),
                    deferred_reads: Vec::new(),
                },
            );
        }
        let clients = (0..config.clients)
            .map(|i| ClientSim {
                id: ClientId(100 + i as u64),
                seq: 0,
                open: None,
                counter: 0,
            })
            .collect();
        let mut scripted = config.scripted.clone();
        scripted.sort_by_key(|(tick, _)| *tick);
        Self {
            config,
            rng: Rng::new(config.seed),
            now: 0,
            queue: EventQueue::new(),
            nodes,
            genesis,
            oracle: OracleState::new(),
            history: History::new(),
            trace: Trace::new(),
            leader_history: Vec::new(),
            current_leader: None,
            clients,
            open_writes: BTreeMap::new(),
            open_reads: BTreeMap::new(),
            outcomes: BTreeMap::new(),
            next_request: 1,
            scripted,
            max_latency: 0,
            last_fault_tick: 0,
            chaos_end: config.chaos_ticks,
            run_end: config.chaos_ticks + config.heal_ticks,
            chain_budget: 0,
            heal_started: false,
        }
    }

    fn run(&mut self) -> RunReport {
        for id in self.node_ids() {
            self.queue.push(1, SimEvent::Poll { node: id });
        }
        self.queue.push(CLIENT_INTERVAL, SimEvent::DriveClients);
        let mut oracle_failure: Option<OracleViolation> = None;
        let mut completed = false;
        while let Some((tick, event)) = self.queue.pop() {
            if tick > self.now {
                self.advance_clock(tick);
            }
            if !self.heal_started && !self.in_chaos() {
                self.heal_started = true;
                self.run_end = self.now + self.config.heal_ticks;
                self.trace.push(self.now, "healing begins");
            }
            if self.now > self.run_end && tick > self.run_end {
                completed = true;
                break;
            }
            self.fire_scripted();
            self.dispatch(event);
            if let Err(violation) = self.check_oracles() {
                self.trace
                    .push(self.now, &format!("oracle_failure {}", violation.oracle));
                oracle_failure = Some(violation);
                break;
            }
            self.track_leadership();
            if self.now >= self.run_end && self.queue.peek_tick().is_none_or(|t| t > self.run_end) {
                completed = true;
                break;
            }
        }
        self.finish(oracle_failure, completed)
    }

    fn node_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    const fn in_chaos(&self) -> bool {
        self.now < self.chaos_end
    }

    /// Advance the virtual clock to `tick`, processing the
    /// per-tick chaos faults (crashes and clock jumps) for every
    /// tick crossed. A clock jump consumes the phase budget like
    /// any other passage of time (invariant R-04's shape: the jump
    /// changes what is pending, never what answers are legal); the
    /// healing phase is budgeted from the moment it begins, so it
    /// always gets its full length.
    fn advance_clock(&mut self, tick: u64) {
        let target = tick.max(self.now);
        if self.in_chaos() {
            let mut t = self.last_fault_tick.max(self.now);
            while t < target {
                t += 1;
                self.last_fault_tick = t;
                if self.config.faults.clock_jump_per_mille > 0
                    && self.rng.per_mille(self.config.faults.clock_jump_per_mille)
                {
                    let jump = self
                        .rng
                        .range_inclusive(1, self.config.faults.max_clock_jump.max(1));
                    self.trace.push(t, &format!("clock_jump {jump}"));
                    self.now = t + jump;
                    self.last_fault_tick = self.now;
                    return;
                }
                if self.config.faults.crash_per_mille > 0 {
                    let ids = self.node_ids();
                    for id in ids {
                        if self.nodes.get(&id).is_some_and(|n| n.alive)
                            && self.rng.per_mille(self.config.faults.crash_per_mille)
                        {
                            self.crash_node(id);
                        }
                    }
                }
            }
        }
        self.now = target;
        self.last_fault_tick = target;
    }

    fn fire_scripted(&mut self) {
        while self
            .scripted
            .first()
            .is_some_and(|(tick, _)| *tick <= self.now)
        {
            let (_, op) = self.scripted.remove(0);
            match op {
                ScriptedOp::Crash(id) => self.crash_node(id),
                ScriptedOp::JumpClock(jump) => {
                    self.trace.push(self.now, &format!("clock_jump {jump}"));
                    self.now += jump;
                    self.last_fault_tick = self.now;
                }
                ScriptedOp::ProposeConfig(change) => {
                    if let Some((leader, _)) = self.current_leader {
                        self.step_node(leader, Event::ProposeConfig { change });
                    }
                }
                ScriptedOp::CorruptLog { node, index } => {
                    let flipped = self
                        .nodes
                        .get_mut(&node)
                        .is_some_and(|sim| sim.disk.corrupt_log_record(index));
                    self.trace.push(
                        self.now,
                        &format!("corrupt_log {node} {index} flipped={flipped}"),
                    );
                }
                ScriptedOp::CorruptSnapshot { node } => {
                    let flipped = self
                        .nodes
                        .get_mut(&node)
                        .is_some_and(|sim| sim.disk.corrupt_snapshot());
                    self.trace.push(
                        self.now,
                        &format!("corrupt_snapshot {node} flipped={flipped}"),
                    );
                }
                ScriptedOp::RemoveLeader => {
                    if let Some((leader, _)) = self.current_leader {
                        self.step_node(
                            leader,
                            Event::ProposeConfig {
                                change: ConfigChange::RemoveVoter { node: leader },
                            },
                        );
                    }
                }
            }
        }
    }

    fn crash_node(&mut self, id: NodeId) {
        let Some(sim) = self.nodes.get_mut(&id) else {
            return;
        };
        if !sim.alive {
            return;
        }
        sim.alive = false;
        sim.epoch += 1;
        sim.disk.crash();
        sim.deferred_reads.clear();
        self.oracle.forget_node(id);
        self.trace.push(self.now, &format!("crash {id}"));
        let delay = RESTART_DELAY + self.rng.next_below(RESTART_DELAY);
        self.queue
            .push(self.now + delay, SimEvent::Restart { node: id });
    }

    fn restart_node(&mut self, id: NodeId) {
        let (hard, restore, snapshot) = {
            let sim = self.nodes.get(&id).expect("restart targets a node");
            (
                sim.disk.durable_hard().unwrap_or(HardState {
                    current_term: Term::ZERO,
                    voted_for: None,
                }),
                sim.disk.restore_log(),
                sim.disk.durable_snapshot().cloned(),
            )
        };
        if let Some(torn) = &restore.torn {
            self.trace.push(
                self.now,
                &format!(
                    "log_truncated {id} index {} expected {:08x} actual {:08x}",
                    torn.index.0, torn.expected_crc, torn.actual_crc
                ),
            );
        }
        let entries = restore.entries;
        let mut node = Node::new(
            id,
            self.config.core.clone(),
            self.genesis.clone(),
            self.config.seed ^ id.0,
        )
        .expect("the run's core config always validates");
        let mut machine = KvStateMachine::new(SESSION_CAPACITY);
        // A durable snapshot whose payload verifies installs
        // into the machine here, and its boundary installs into
        // the core through the Restored event itself (invariant
        // N-07). A payload that fails its checksum is rejected
        // whole (post-M4 Lane 2, invariant H-06): neither install
        // happens, so the log replayed below stands and the
        // node's base never advances over state its machine
        // does not hold.
        let mut boot_snapshot: Option<(SnapshotMeta, Vec<u8>)> = None;
        if let Some((meta, data)) = snapshot {
            if let Ok(parts) = simraft_kv::decode_snapshot(&data) {
                machine.install_snapshot(parts);
                boot_snapshot = Some((meta, data));
            } else {
                self.trace
                    .push(self.now, &format!("snapshot_rejected {id}"));
            }
        }
        let mut actions = node.step(Event::Restored {
            hard_state: hard,
            entries,
            snapshot: boot_snapshot.as_ref().map(|(meta, _)| meta.clone()),
        });
        if let Some((meta, data)) = boot_snapshot {
            // Keep the verified bytes loaded so a node that goes
            // on to lead can relay them without a store round
            // trip.
            let mut more = node.step(Event::SnapshotLoaded { meta, data });
            actions.append(&mut more);
        }
        {
            let sim = self.nodes.get_mut(&id).expect("restart targets a node");
            sim.node = node;
            sim.machine = machine;
            sim.alive = true;
            sim.applied.clear();
            sim.deferred_reads.clear();
        }
        self.trace.push(self.now, &format!("restart {id}"));
        self.handle_actions(id, actions);
        self.queue
            .push(self.now + POLL_INTERVAL, SimEvent::Poll { node: id });
    }

    fn dispatch(&mut self, event: SimEvent) {
        self.chain_budget = 8;
        match event {
            SimEvent::Poll { node } => {
                if self.nodes.get(&node).is_some_and(|n| n.alive) {
                    self.step_node(node, Event::Tick { now: self.now });
                    self.queue
                        .push(self.now + POLL_INTERVAL, SimEvent::Poll { node });
                }
            }
            SimEvent::Deliver { to, frame } => {
                if self.nodes.get(&to).is_some_and(|n| n.alive) {
                    if let Ok((from, _, msg)) = decode_frame(&frame) {
                        self.step_node(to, Event::Message { from, msg });
                    }
                }
            }
            SimEvent::Fsync { node, epoch, kind } => {
                let live_epoch = self
                    .nodes
                    .get(&node)
                    .is_some_and(|n| n.alive && n.epoch == epoch);
                if live_epoch {
                    self.run_fsync(node, kind);
                }
            }
            SimEvent::Restart { node } => {
                if self.nodes.get(&node).is_some_and(|n| !n.alive) {
                    self.restart_node(node);
                }
            }
            SimEvent::DriveClients => {
                self.drive_clients();
                if self.now + CLIENT_INTERVAL <= self.run_end {
                    self.queue
                        .push(self.now + CLIENT_INTERVAL, SimEvent::DriveClients);
                }
            }
        }
    }

    fn run_fsync(&mut self, id: NodeId, kind: FsyncKind) {
        match kind {
            FsyncKind::Hard => {
                if let Some(sim) = self.nodes.get_mut(&id) {
                    sim.disk.fsync_hard();
                }
                self.step_node(
                    id,
                    Event::Persisted {
                        kind: PersistKind::HardState,
                        up_to_index: LogIndex::ZERO,
                    },
                );
            }
            FsyncKind::Entries => {
                let up_to = {
                    let sim = self.nodes.get_mut(&id).expect("fsync targets a node");
                    sim.disk.fsync_entries();
                    LogIndex(sim.disk.durable_entries().len() as u64)
                };
                self.step_node(
                    id,
                    Event::Persisted {
                        kind: PersistKind::Entries,
                        up_to_index: up_to,
                    },
                );
            }
            FsyncKind::Snapshot => {
                let snapshot = {
                    let sim = self.nodes.get_mut(&id).expect("fsync targets a node");
                    sim.disk.fsync_snapshot();
                    sim.disk.durable_snapshot().cloned()
                };
                if let Some((meta, data)) = snapshot {
                    // This fsync completes a snapshot persist:
                    // a received install (the core's
                    // PersistSnapshot action) or the node's own
                    // compaction (queued by maybe_compact). The
                    // payload enters the state machine here, paired
                    // with the cursor jump the core makes on
                    // SnapshotPersisted: for a received snapshot
                    // the install is what the machine holds at
                    // all, and without it the machine would
                    // silently miss the snapshotted state while
                    // the core applies past it; for the node's own
                    // snapshot the install restores exactly the
                    // state the machine already holds (post-M4
                    // Lane 2, invariant H-08).
                    if let Ok(parts) = simraft_kv::decode_snapshot(&data) {
                        if let Some(sim) = self.nodes.get_mut(&id) {
                            sim.machine.install_snapshot(parts);
                        }
                    }
                    self.step_node(id, Event::SnapshotPersisted { meta });
                }
            }
        }
    }

    /// Step one node and process every action it emits.
    fn step_node(&mut self, id: NodeId, event: Event) {
        let actions = {
            let Some(sim) = self.nodes.get_mut(&id) else {
                return;
            };
            if !sim.alive {
                return;
            }
            sim.node.step(event)
        };
        self.handle_actions(id, actions);
    }

    fn handle_actions(&mut self, id: NodeId, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Send { to, msg } => self.network_send(id, to, &msg),
                Action::PersistHardState {
                    current_term,
                    voted_for,
                } => {
                    let epoch = {
                        let sim = self.nodes.get_mut(&id).expect("action source exists");
                        sim.disk.write_hard(HardState {
                            current_term,
                            voted_for,
                        });
                        sim.epoch
                    };
                    self.queue.push(
                        self.now + FSYNC_DELAY,
                        SimEvent::Fsync {
                            node: id,
                            epoch,
                            kind: FsyncKind::Hard,
                        },
                    );
                }
                Action::PersistEntries {
                    from_index,
                    entries,
                } => {
                    let epoch = {
                        let sim = self.nodes.get_mut(&id).expect("action source exists");
                        sim.disk.write_entries(from_index, &entries);
                        sim.epoch
                    };
                    self.queue.push(
                        self.now + FSYNC_DELAY,
                        SimEvent::Fsync {
                            node: id,
                            epoch,
                            kind: FsyncKind::Entries,
                        },
                    );
                }
                Action::Apply { index, entry } => self.handle_apply(id, index, &entry),
                Action::ReadReady {
                    request_id,
                    key,
                    read_index,
                } => self.handle_read_ready(id, request_id, &key, read_index),
                Action::ReadRejected { request_id, leader } => {
                    self.retarget_read(request_id, leader);
                }
                Action::ClientResponse {
                    client_id,
                    seq,
                    result,
                } => self.handle_client_response(client_id, seq, &result),
                Action::PersistSnapshot { meta, data } => {
                    // A persist whose boundary lies past the end
                    // of the node's own log is a received snapshot:
                    // the node could not have compacted entries it
                    // never held. Traced as the scenarios' H-08
                    // evidence that a transfer really happened.
                    if self
                        .nodes
                        .get(&id)
                        .is_some_and(|sim| sim.node.log().last_index() < meta.last_included_index)
                    {
                        self.trace.push(
                            self.now,
                            &format!("snapshot_received {id} upto {}", meta.last_included_index.0),
                        );
                    }
                    let epoch = {
                        let sim = self.nodes.get_mut(&id).expect("action source exists");
                        sim.disk.write_snapshot(meta, data);
                        sim.epoch
                    };
                    self.queue.push(
                        self.now + FSYNC_DELAY,
                        SimEvent::Fsync {
                            node: id,
                            epoch,
                            kind: FsyncKind::Snapshot,
                        },
                    );
                }
                Action::LoadSnapshot { meta } => self.serve_load_snapshot(id, meta),
                Action::ConfigResponse { change, result } => {
                    self.trace
                        .push(self.now, &format!("config_response {change:?} {result:?}"));
                }
                Action::StepDown { term } => {
                    self.trace.push(self.now, &format!("step_down {id} {term}"));
                }
            }
        }
    }

    fn network_send(&mut self, from: NodeId, to: NodeId, msg: &Message) {
        if from == to {
            return;
        }
        let now = self.now;
        if self
            .config
            .faults
            .partitions
            .iter()
            .any(|cut| cut.denies(from, to, now))
        {
            return;
        }
        let chaos = self.in_chaos();
        if chaos && self.rng.per_mille(self.config.faults.drop_per_mille) {
            return;
        }
        let latency = if chaos {
            self.rng.range_inclusive(
                self.config.faults.latency_min,
                self.config
                    .faults
                    .latency_max
                    .max(self.config.faults.latency_min),
            )
        } else {
            1
        };
        let mut frame = encode_frame(from, to, msg);
        // Frame corruption (post-M4 Lane 2, invariant H-04): one
        // seeded bit flips in the encoded frame. The draw sits in
        // a fixed position in the send path's call order (after
        // the latency draw, before the duplication draw) so a
        // seed replays identically. The receiver's frame checks
        // reject the result exactly as they reject a truncated or
        // foreign frame, so corruption is indistinguishable from
        // loss at the protocol level.
        if chaos
            && self.config.faults.corrupt_per_mille > 0
            && self.rng.per_mille(self.config.faults.corrupt_per_mille)
        {
            let byte = usize::try_from(
                self.rng
                    .next_below(u64::try_from(frame.len()).unwrap_or(u64::MAX)),
            )
            .unwrap_or(0);
            let bit = u8::try_from(self.rng.next_below(8)).unwrap_or(0);
            frame[byte] ^= 1u8 << bit;
            self.trace
                .push(now, &format!("frame_corrupted {from}->{to}"));
        }
        self.queue.push(
            now + latency,
            SimEvent::Deliver {
                to,
                frame: frame.clone(),
            },
        );
        if chaos && self.rng.per_mille(self.config.faults.duplicate_per_mille) {
            let extra = 1 + self.rng.next_below(3);
            self.queue
                .push(now + latency + extra, SimEvent::Deliver { to, frame });
        }
    }

    fn handle_apply(&mut self, id: NodeId, index: LogIndex, entry: &simraft_core::LogEntry) {
        let outcome = {
            let sim = self.nodes.get_mut(&id).expect("apply targets a node");
            sim.applied.push((index, entry.clone()));
            sim.machine.apply(index, entry)
        };
        if let Ok(ApplyOutcome::Applied { result } | ApplyOutcome::Cached { result }) = outcome {
            if let EntryKind::Command { client_id, seq, .. } = &entry.kind {
                let mapped = match result {
                    CommandResult::Ack => OpResult::Ack,
                    CommandResult::Value(v) => OpResult::Value(Some(v)),
                    CommandResult::Error { message, .. } => OpResult::Error(message),
                };
                self.outcomes.insert((*client_id, *seq), mapped);
            }
        }
        // Reads deferred until this machine reached their index
        // can now be served.
        let ready: Vec<(u64, Vec<u8>, LogIndex)> = {
            let sim = self.nodes.get_mut(&id).expect("apply targets a node");
            let applied = sim.node.last_applied();
            let mut ready = Vec::new();
            sim.deferred_reads.retain(|(request, key, read_index)| {
                if *read_index <= applied {
                    ready.push((*request, key.clone(), *read_index));
                    false
                } else {
                    true
                }
            });
            ready
        };
        for (request_id, key, read_index) in ready {
            self.handle_read_ready(id, request_id, &key, read_index);
        }
        self.maybe_compact(id);
    }

    fn handle_read_ready(&mut self, id: NodeId, request_id: u64, key: &[u8], read_index: LogIndex) {
        let served = {
            let sim = self.nodes.get(&id).expect("read targets a node");
            sim.machine
                .read(read_index, key)
                .map(|v| v.map(<[u8]>::to_vec))
        };
        match served {
            Some(value) => {
                if let Some(open) = self.open_reads.remove(&request_id) {
                    self.history
                        .complete(open.record, self.now, OpResult::Value(value));
                    self.client_free(open.client);
                }
            }
            None => {
                if let Some(sim) = self.nodes.get_mut(&id) {
                    sim.deferred_reads
                        .push((request_id, key.to_vec(), read_index));
                }
            }
        }
    }

    fn retarget_read(&mut self, request_id: u64, leader: Option<NodeId>) {
        if self.chain_budget == 0 {
            return;
        }
        self.chain_budget -= 1;
        let Some(open) = self.open_reads.get(&request_id).cloned() else {
            return;
        };
        let Some(target) = leader.filter(|t| self.nodes.get(t).is_some_and(|n| n.alive)) else {
            return;
        };
        let key = match &self.history.records()[open.record].op {
            Op::Get { key } => key.clone(),
            _ => return,
        };
        self.step_node(target, Event::ReadIndex { request_id, key });
    }

    fn handle_client_response(
        &mut self,
        client_id: ClientId,
        seq: u64,
        result: &simraft_core::ClientResult,
    ) {
        match result {
            simraft_core::ClientResult::Committed => {
                if let Some(open) = self.open_writes.remove(&(client_id, seq)) {
                    let outcome = self
                        .outcomes
                        .remove(&(client_id, seq))
                        .unwrap_or(OpResult::Ack);
                    self.history.complete(open.record, self.now, outcome);
                    self.max_latency = self.max_latency.max(self.now - open.started);
                    self.client_free(open.client);
                }
            }
            simraft_core::ClientResult::NotLeader { leader } => {
                // The proposal was never appended; re-propose the
                // same (client, seq) to the hinted leader.
                if self.chain_budget == 0 {
                    return;
                }
                self.chain_budget -= 1;
                let Some(open) = self.open_writes.get(&(client_id, seq)).cloned() else {
                    return;
                };
                let Some(target) = leader.filter(|t| self.nodes.get(t).is_some_and(|n| n.alive))
                else {
                    return;
                };
                let payload = match &self.history.records()[open.record].op {
                    Op::Put { key, value } => encode_command(
                        client_id,
                        seq,
                        &KvOp::Put {
                            key: key.clone(),
                            value: value.clone(),
                        },
                    ),
                    Op::Delete { key } => {
                        encode_command(client_id, seq, &KvOp::Delete { key: key.clone() })
                    }
                    Op::Get { .. } => return,
                };
                self.step_node(
                    target,
                    Event::Propose {
                        client_id,
                        seq,
                        payload,
                    },
                );
            }
        }
    }

    fn client_free(&mut self, client: usize) {
        if let Some(sim) = self.clients.get_mut(client) {
            sim.open = None;
        }
    }

    fn serve_load_snapshot(&mut self, id: NodeId, meta: SnapshotMeta) {
        if self.chain_budget == 0 {
            return;
        }
        self.chain_budget -= 1;
        let bytes = {
            let sim = self.nodes.get(&id).expect("load targets a node");
            match sim.disk.durable_snapshot() {
                Some((stored, data)) if *stored == meta => Some(data.clone()),
                _ => {
                    // Rebuild from the machine only when the
                    // machine stands exactly at the requested
                    // boundary: a snapshot parts call labels the
                    // machine's current state with the requested
                    // meta, so rebuilding from a machine that has
                    // applied past the boundary would hand the
                    // receiver state from its future under an
                    // older index. In that case the load is
                    // unmet and the leader retries.
                    let term = sim.node.log().term_at(meta.last_included_index);
                    term.filter(|_| sim.node.last_applied() == meta.last_included_index)
                        .map(|_| {
                            let parts = sim.machine.snapshot_parts(
                                meta.last_included_index,
                                meta.last_included_term,
                                sim.node.cluster(),
                            );
                            encode_snapshot(&parts)
                        })
                }
            }
        };
        if let Some(data) = bytes {
            self.step_node(id, Event::SnapshotLoaded { meta, data });
        } else {
            self.trace
                .push(self.now, &format!("load_snapshot_unmet {id}"));
        }
    }

    fn maybe_compact(&mut self, id: NodeId) {
        let plan = {
            let sim = self.nodes.get(&id).expect("compact targets a node");
            let log = sim.node.log();
            let commit = sim.node.commit_index();
            if commit > log.base_index()
                && commit.0 - log.base_index().0 >= COMPACT_THRESHOLD
                && sim.node.last_applied() >= commit
            {
                let term = log.term_at(commit).unwrap_or_else(|| log.base_term());
                let parts = sim.machine.snapshot_parts(commit, term, sim.node.cluster());
                Some((
                    SnapshotMeta {
                        last_included_index: commit,
                        last_included_term: term,
                        config: sim.node.cluster().clone(),
                    },
                    encode_snapshot(&parts),
                ))
            } else {
                None
            }
        };
        if let Some((meta, data)) = plan {
            // The core advances the base on SnapshotPersisted,
            // and it emits no PersistSnapshot action for a node's
            // own snapshot (that action is reserved for received
            // installs), so the driver completes the persist
            // cycle itself: load the bytes into the core, write
            // them to the disk's volatile snapshot, and queue the
            // fsync that steps SnapshotPersisted. Without this
            // the base never advances and no follower ever falls
            // behind it, which silently disarms the whole
            // InstallSnapshot relay in the simulator (post-M4
            // Lane 2, invariant H-08).
            let epoch = {
                let sim = self.nodes.get_mut(&id).expect("compact targets a node");
                sim.disk.write_snapshot(meta.clone(), data.clone());
                sim.epoch
            };
            self.step_node(id, Event::SnapshotLoaded { meta, data });
            self.queue.push(
                self.now + FSYNC_DELAY,
                SimEvent::Fsync {
                    node: id,
                    epoch,
                    kind: FsyncKind::Snapshot,
                },
            );
        }
    }

    fn drive_clients(&mut self) {
        if self.now > self.run_end.saturating_sub(QUIESCE_TICKS) {
            return;
        }
        for ci in 0..self.clients.len() {
            if let Some((_, _, started)) = self.clients[ci].open {
                if self.now - started > CLIENT_TIMEOUT {
                    // Abandon the operation; its history record
                    // stays pending, which the checker handles.
                    self.clients[ci].open = None;
                } else {
                    continue;
                }
            }
            self.issue_op(ci);
        }
    }

    fn issue_op(&mut self, ci: usize) {
        let target = self.pick_target();
        let Some(target) = target else { return };
        let client_id = self.clients[ci].id;
        self.clients[ci].seq += 1;
        self.clients[ci].counter += 1;
        let seq = self.clients[ci].seq;
        let counter = self.clients[ci].counter;
        let key = format!("k{}", self.rng.next_below(self.config.keys.max(1) as u64)).into_bytes();
        let roll = self.rng.next_below(10);
        if roll < 2 {
            // ReadIndex read.
            let record = self
                .history
                .invoke(client_id.0, self.now, Op::Get { key: key.clone() });
            let request_id = self.next_request;
            self.next_request += 1;
            self.open_reads.insert(
                request_id,
                OpenOp {
                    record,
                    client: ci,
                    started: self.now,
                },
            );
            self.clients[ci].open = Some((request_id, PendingKind::Read, self.now));
            self.step_node(target, Event::ReadIndex { request_id, key });
        } else {
            let (op, kv) = if roll < 8 {
                let value = format!("v{counter}").into_bytes();
                (
                    Op::Put {
                        key: key.clone(),
                        value: value.clone(),
                    },
                    KvOp::Put { key, value },
                )
            } else {
                (Op::Delete { key: key.clone() }, KvOp::Delete { key })
            };
            let record = self.history.invoke(client_id.0, self.now, op);
            let payload = encode_command(client_id, seq, &kv);
            self.open_writes.insert(
                (client_id, seq),
                OpenOp {
                    record,
                    client: ci,
                    started: self.now,
                },
            );
            self.clients[ci].open = Some((seq, PendingKind::Write, self.now));
            self.step_node(
                target,
                Event::Propose {
                    client_id,
                    seq,
                    payload,
                },
            );
        }
    }

    fn pick_target(&mut self) -> Option<NodeId> {
        if let Some((leader, _)) = self.current_leader {
            if self.nodes.get(&leader).is_some_and(|n| n.alive) {
                return Some(leader);
            }
        }
        let alive: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.alive)
            .map(|(id, _)| *id)
            .collect();
        if alive.is_empty() {
            None
        } else {
            let bound = u64::try_from(alive.len()).unwrap_or(u64::MAX);
            let pick = usize::try_from(self.rng.next_below(bound)).unwrap_or(0);
            Some(alive[pick])
        }
    }

    fn check_oracles(&mut self) -> Result<(), OracleViolation> {
        let matches: Vec<Vec<(NodeId, LogIndex)>> = self
            .nodes
            .values()
            .filter(|sim| sim.alive)
            .map(|sim| {
                sim.node
                    .cluster()
                    .members()
                    .filter_map(|peer| sim.node.progress(peer).map(|(_, m)| (peer, m)))
                    .collect()
            })
            .collect();
        let views: Vec<NodeView<'_>> = self
            .nodes
            .iter()
            .filter(|(_, sim)| sim.alive)
            .zip(matches.iter())
            .map(|((id, sim), matches)| NodeView {
                id: *id,
                role: sim.node.role(),
                current_term: sim.node.current_term(),
                base_index: sim.node.log().base_index(),
                base_term: sim.node.log().base_term(),
                entries: sim.node.log().entries(),
                commit_index: sim.node.commit_index(),
                last_applied: sim.node.last_applied(),
                applied: &sim.applied,
                match_indexes: matches,
            })
            .collect();
        self.oracle.check_all(&views)
    }

    fn track_leadership(&mut self) {
        let leaders: Vec<(NodeId, Term)> = self
            .nodes
            .iter()
            .filter(|(_, sim)| sim.alive && sim.node.role() == Role::Leader)
            .map(|(id, sim)| (*id, sim.node.current_term()))
            .collect();
        if leaders.len() == 1 {
            let current = leaders[0];
            if self.current_leader != Some(current) {
                self.current_leader = Some(current);
                self.leader_history.push((self.now, current.0, current.1));
                self.trace
                    .push(self.now, &format!("leader {} {}", current.0, current.1));
            }
        }
    }

    fn log_digest(sim: &NodeSim) -> String {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&sim.node.log().base_index().0.to_le_bytes());
        bytes.extend_from_slice(&sim.node.log().base_term().0.to_le_bytes());
        for entry in sim.node.log().entries() {
            bytes.extend_from_slice(&entry.term.0.to_le_bytes());
            match &entry.kind {
                EntryKind::NoOp => bytes.push(0),
                EntryKind::Command {
                    client_id,
                    seq,
                    payload,
                } => {
                    bytes.push(1);
                    bytes.extend_from_slice(&client_id.0.to_le_bytes());
                    bytes.extend_from_slice(&seq.to_le_bytes());
                    bytes.extend_from_slice(payload);
                }
                EntryKind::Config { config } => {
                    bytes.push(2);
                    for voter in &config.voters {
                        bytes.extend_from_slice(&voter.0.to_le_bytes());
                    }
                    bytes.push(0xFF);
                    for learner in &config.learners {
                        bytes.extend_from_slice(&learner.0.to_le_bytes());
                    }
                }
            }
        }
        digest_hex(&bytes)
    }

    /// Whether two nodes hold the same logical log, allowing
    /// different snapshot bases: compaction timing is node local,
    /// so at the end of a run one node may hold entries another
    /// node has already folded into its snapshot. The logs agree
    /// when the last indexes match, the term at the higher base
    /// matches on both sides, and every entry above the higher
    /// base is identical.
    fn same_log(a: &NodeSim, b: &NodeSim) -> bool {
        let (la, lb) = (a.node.log(), b.node.log());
        if la.last_index() != lb.last_index() {
            return false;
        }
        let shared = la.base_index().max(lb.base_index());
        if la.term_at(shared) != lb.term_at(shared) {
            return false;
        }
        let ea = la
            .entries()
            .iter()
            .enumerate()
            .filter(|(i, _)| la.base_index().0 + 1 + *i as u64 > shared.0)
            .map(|(_, e)| e);
        let eb = lb
            .entries()
            .iter()
            .enumerate()
            .filter(|(i, _)| lb.base_index().0 + 1 + *i as u64 > shared.0)
            .map(|(_, e)| e);
        ea.eq(eb)
    }

    fn state_digest_node(sim: &NodeSim) -> String {
        let applied = sim.node.last_applied();
        let term = sim
            .node
            .log()
            .term_at(applied)
            .unwrap_or_else(|| sim.node.log().base_term());
        let parts = sim
            .machine
            .snapshot_parts(applied, term, sim.node.cluster());
        digest_hex(&encode_snapshot(&parts))
    }

    fn finish(&mut self, oracle_failure: Option<OracleViolation>, completed: bool) -> RunReport {
        // Convergence is a property of the cluster: the members
        // of the final configuration must share one log and one
        // state. The member set comes from the most current alive
        // view (highest commit, then highest term). A node removed
        // from the configuration is no longer part of the cluster;
        // its frozen log and state are expected to differ, and it
        // is excluded.
        let alive: Vec<(NodeId, &NodeSim)> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.alive)
            .map(|(id, n)| (*id, n))
            .collect();
        let converged = oracle_failure.is_none()
            && alive
                .iter()
                .max_by_key(|(_, n)| (n.node.commit_index(), n.node.current_term()))
                .is_some_and(|(_, view)| {
                    let config = view.node.cluster();
                    let members: Vec<&NodeSim> = alive
                        .iter()
                        .filter(|(id, _)| config.is_voter(*id) || config.learners.contains(id))
                        .map(|(_, n)| *n)
                        .collect();
                    !members.is_empty()
                        && members.iter().all(|n| {
                            Self::same_log(n, members[0])
                                && Self::state_digest_node(n) == Self::state_digest_node(members[0])
                        })
                });
        let mut digest_bytes = Vec::new();
        for (id, sim) in &self.nodes {
            digest_bytes.extend_from_slice(&id.0.to_le_bytes());
            digest_bytes.extend_from_slice(Self::log_digest(sim).as_bytes());
            digest_bytes.extend_from_slice(Self::state_digest_node(sim).as_bytes());
        }
        let verdict =
            Checker::with_budget(self.config.checker_budget).check(self.history.records());
        let (final_leader, final_leader_term) = match self.current_leader {
            Some((id, term))
                if self
                    .nodes
                    .get(&id)
                    .is_some_and(|n| n.alive && n.node.role() == Role::Leader) =>
            {
                (Some(id), Some(term))
            }
            _ => (None, None),
        };
        RunReport {
            seed: self.config.seed,
            completed: completed && oracle_failure.is_none(),
            ticks_run: self.now,
            final_leader,
            final_leader_term,
            leader_history: std::mem::take(&mut self.leader_history),
            converged,
            state_digest: digest_hex(&digest_bytes),
            verdict,
            oracle_failure,
            history: std::mem::take(&mut self.history),
            final_applied: self
                .nodes
                .iter()
                .map(|(id, sim)| (*id, sim.node.last_applied()))
                .collect(),
            max_commit_latency_ticks: self.max_latency,
            trace: std::mem::take(&mut self.trace),
        }
    }
}
