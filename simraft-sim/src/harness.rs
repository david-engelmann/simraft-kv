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

use simraft_core::{Config, NodeId, Term};

use crate::faults::{FaultConfig, ScriptedOp};
use crate::history::History;
use crate::liz::Verdict;
use crate::oracle::OracleViolation;
use crate::trace::Trace;

/// Everything a run is configured with. The seed alone decides the
/// run: the same seed always produces the same trace, the same
/// history, and the same final digest (invariant D-01).
#[derive(Debug, Clone)]
pub struct SimConfig {
    /// The run's seed.
    pub seed: u64,
    /// The number of nodes (all voters at genesis).
    pub node_count: usize,
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
        // Stage A shell: the driver loop lands at Stage B. The
        // report is honestly incomplete, which is what makes the
        // scenario suites red at this stage.
        RunReport {
            seed: self.config.seed,
            completed: false,
            ticks_run: 0,
            final_leader: None,
            final_leader_term: None,
            leader_history: Vec::new(),
            converged: false,
            state_digest: String::new(),
            verdict: Verdict::Unknown,
            oracle_failure: None,
            history: History::new(),
            max_commit_latency_ticks: 0,
            trace: Trace::new(),
        }
    }
}
