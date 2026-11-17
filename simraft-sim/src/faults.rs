//! The seeded fault schedule.
//!
//! Docs/ARCHITECTURE.md sections 12.2 and 12.4: packet drop,
//! duplication and latency faults, directed partitions with tick
//! spans, crash and clock-jump rates for the chaos phase, and
//! scripted operations a scenario can schedule at exact ticks.

use simraft_core::{ConfigChange, LogIndex, NodeId};

/// A directed partition: delivery from `from` to `to` is denied
/// while the tick is in `[start, end)`. The reverse direction is
/// unaffected unless it has its own cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirectedCut {
    /// The sender whose packets are denied.
    pub from: NodeId,
    /// The receiver they never reach.
    pub to: NodeId,
    /// The first denied tick.
    pub start: u64,
    /// The first tick delivered again.
    pub end: u64,
}

impl DirectedCut {
    /// Whether this cut denies delivery at `tick`.
    #[must_use]
    pub fn denies(&self, from: NodeId, to: NodeId, tick: u64) -> bool {
        self.from == from && self.to == to && tick >= self.start && tick < self.end
    }
}

/// An operation a scenario schedules at an exact tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedOp {
    /// Ask the current leader to propose this membership change.
    ProposeConfig(ConfigChange),
    /// Ask the current leader to propose its own removal (the
    /// self-removal scenario, invariant M-03).
    RemoveLeader,
    /// Crash this node now (it restarts after the harness's restart
    /// delay with only its durable state).
    Crash(NodeId),
    /// Flip one byte in this node's durable log record for
    /// `index` (post-M4 Lane 2). The record's checksum fails at
    /// the next restore, which truncates the log there
    /// (invariant H-05). No-op when the record is absent.
    CorruptLog {
        /// The node whose durable log is corrupted.
        node: NodeId,
        /// The log index whose record is corrupted.
        index: LogIndex,
    },
    /// Flip one byte in this node's durable snapshot payload
    /// (post-M4 Lane 2). The payload's checksum fails at the
    /// next restore, which rejects the snapshot and replays
    /// the log instead (invariant H-06). No-op when the node
    /// holds no durable snapshot.
    CorruptSnapshot {
        /// The node whose durable snapshot is corrupted.
        node: NodeId,
    },
    /// Jump the virtual clock forward by this many ticks at once.
    JumpClock(u64),
}

/// The fault knobs of a run. All rates are per mille.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultConfig {
    /// Per-packet drop probability during the chaos phase.
    pub drop_per_mille: u64,
    /// Per-packet duplication probability during the chaos phase.
    pub duplicate_per_mille: u64,
    /// Per-packet bit-flip probability during the chaos phase
    /// (post-M4 Lane 2): one seeded bit of the encoded frame is
    /// flipped before delivery, so the receiver's frame checks
    /// (magic, length, CRC-32) reject it (invariant H-04).
    pub corrupt_per_mille: u64,
    /// The latency range a packet's delivery delay is drawn from,
    /// in ticks, during the chaos phase.
    pub latency_min: u64,
    /// The top of the chaos latency range.
    pub latency_max: u64,
    /// Per-node, per-tick crash probability during the chaos phase.
    pub crash_per_mille: u64,
    /// Per-tick clock jump probability during the chaos phase.
    pub clock_jump_per_mille: u64,
    /// The largest single clock jump, in ticks.
    pub max_clock_jump: u64,
    /// The directed partitions in force (their spans decide when).
    pub partitions: Vec<DirectedCut>,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            drop_per_mille: 20,
            duplicate_per_mille: 10,
            corrupt_per_mille: 0,
            latency_min: 1,
            latency_max: 10,
            crash_per_mille: 1,
            clock_jump_per_mille: 2,
            max_clock_jump: 5_000,
            partitions: Vec::new(),
        }
    }
}

impl FaultConfig {
    /// A fault-free configuration: no drops, no duplicates, fixed
    /// minimal latency, no crashes, no jumps, no partitions.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            drop_per_mille: 0,
            duplicate_per_mille: 0,
            corrupt_per_mille: 0,
            latency_min: 1,
            latency_max: 1,
            crash_per_mille: 0,
            clock_jump_per_mille: 0,
            max_clock_jump: 0,
            partitions: Vec::new(),
        }
    }
}
