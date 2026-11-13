//! Inputs to the sans-I/O core.
//!
//! A driver (the simulator, or a future I/O adapter) feeds `Node` one
//! `Event` at a time and executes the `Action` values that come back.
//! Event kinds are added per milestone: `ReadIndex` and configuration
//! proposals land at M2 and M3.

use crate::ids::{ClientId, LogIndex, NodeId, Term, Tick};
use crate::log::LogEntry;
use crate::message::Message;

/// Which kind of durable write a `Persisted` event acknowledges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistKind {
    /// A hard state write (current term and voted-for) completed.
    HardState,
    /// A log entries write completed through `up_to_index`.
    Entries,
}

/// The persistent state a node restarts with: its hard state and its
/// durable log entries, exactly as the storage layer recovered them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    /// The latest term the node had seen before the crash.
    pub current_term: Term,
    /// The candidate the node voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
}

/// One input to `Node::step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The driver's clock reached `now`. The core compares this against
    /// its own deadlines; it never reads any other clock.
    Tick {
        /// The current virtual time.
        now: Tick,
    },
    /// A client asks the leader for a linearizable read of `key`.
    ///
    /// The core runs the ReadIndex protocol (docs/ARCHITECTURE.md
    /// section 8) and answers with `Action::ReadReady` once the read is
    /// safe to serve, or `Action::ReadRejected` when this node cannot
    /// serve it. The key is carried opaquely; the value lives in the
    /// state machine, not the core.
    ReadIndex {
        /// The client-chosen correlation id for this read.
        request_id: u64,
        /// The key to read.
        key: Vec<u8>,
    },
    /// A client proposes a command to this node.
    Propose {
        /// The proposing client.
        client_id: ClientId,
        /// The per-client sequence number.
        seq: u64,
        /// The canonical command payload (simraft-kv encoding).
        payload: Vec<u8>,
    },
    /// An RPC message arrived from another node.
    Message {
        /// The sender.
        from: NodeId,
        /// The message.
        msg: Message,
    },
    /// A durable write the driver was asked to perform has completed.
    /// Dependent responses are released only when this arrives
    /// (invariant T-02).
    Persisted {
        /// Which write completed.
        kind: PersistKind,
        /// For `PersistKind::Entries`, the highest index now durable.
        /// Ignored for `PersistKind::HardState`.
        up_to_index: LogIndex,
    },
    /// The node restarted; this is its recovered durable state. Sent by
    /// the driver as the first event after construction when restarting
    /// a crashed node.
    Restored {
        /// The recovered hard state.
        hard_state: HardState,
        /// The recovered durable log entries, in index order.
        entries: Vec<LogEntry>,
    },
}
