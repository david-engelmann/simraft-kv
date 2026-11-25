//! Outputs of the sans-I/O core.
//!
//! `Node::step` returns actions in order. The driver executes them:
//! sends go to the network, persists go to storage, applies go to the
//! state machine. The core itself performs no I/O.

use crate::error::ProtocolError;
use crate::ids::{ClientId, LogIndex, NodeId, Term};
use crate::log::LogEntry;
use crate::membership::ConfigChange;
use crate::message::Message;
use crate::snapshot::SnapshotMeta;

/// The outcome of a client proposal, delivered when the entry that
/// carries it commits and applies (or immediately, when this node cannot
/// accept it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientResult {
    /// The proposal committed and applied exactly once.
    Committed,
    /// This node is not the leader. `leader` is its current hint for the
    /// leader's identity, when it has one.
    NotLeader {
        /// The known leader, if any.
        leader: Option<NodeId>,
    },
    /// The proposal was rejected by a protocol rule (such as
    /// `Config::max_uncommitted_entries`) without appending to the log.
    Rejected {
        /// The protocol error naming the deciding values.
        error: ProtocolError,
    },
}

/// The outcome of a membership change proposal, delivered
/// immediately when the change is rejected or this node is not the
/// leader, and when the config entry commits when it is accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigResult {
    /// The config entry committed; the new configuration is in force.
    Committed,
    /// This node is not the leader. `leader` is its current hint for
    /// the leader's identity, when it has one.
    NotLeader {
        /// The known leader, if any.
        leader: Option<NodeId>,
    },
    /// The change broke a membership rule; the error names the values
    /// that decided the rejection.
    Rejected {
        /// The rule the change broke.
        error: ProtocolError,
    },
}

/// One output of `Node::step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Send an RPC message to a peer.
    Send {
        /// The recipient.
        to: NodeId,
        /// The message.
        msg: Message,
    },
    /// Persist the hard state (current term and voted-for) before any
    /// response that depends on it is released.
    PersistHardState {
        /// The current term to store.
        current_term: Term,
        /// The vote to store.
        voted_for: Option<NodeId>,
    },
    /// Persist log entries. Storage semantics: truncate the durable log
    /// to `from_index - 1`, then store `entries` at consecutive indexes
    /// starting at `from_index`.
    PersistEntries {
        /// The index of the first entry in `entries`.
        from_index: LogIndex,
        /// The entries to store, in index order.
        entries: Vec<LogEntry>,
    },
    /// Apply the committed entry at `index` to the state machine.
    /// Applies are emitted in index order, exactly once per index per
    /// node lifetime.
    Apply {
        /// The entry's log index.
        index: LogIndex,
        /// The entry to apply.
        entry: LogEntry,
    },
    /// A `ReadIndex` read is confirmed linearizable. The driver serves
    /// the value of `key` from its state machine once its applied index
    /// reaches `read_index`, and must not serve a newer-or-older value.
    ReadReady {
        /// The correlation id from `Event::ReadIndex`.
        request_id: u64,
        /// The key that was read.
        key: Vec<u8>,
        /// The commit index the read is pinned to.
        read_index: LogIndex,
    },
    /// A `ReadIndex` read cannot be served by this node (it is not the
    /// leader, or it stopped being the leader mid-round).
    ReadRejected {
        /// The correlation id from `Event::ReadIndex`.
        request_id: u64,
        /// The known leader, if any.
        leader: Option<NodeId>,
    },
    /// Answer a client proposal.
    ClientResponse {
        /// The proposing client.
        client_id: ClientId,
        /// The proposal's sequence number.
        seq: u64,
        /// The outcome.
        result: ClientResult,
    },
    /// Persist a fully assembled snapshot payload (received over the
    /// wire, or this node's own once loaded) before the core installs
    /// it. The matching `Event::SnapshotPersisted` releases the
    /// install.
    PersistSnapshot {
        /// The snapshot's identity.
        meta: SnapshotMeta,
        /// The complete snapshot payload bytes.
        data: Vec<u8>,
    },
    /// The core needs this node's snapshot payload (its log base has
    /// advanced past entries a follower still needs, or the driver
    /// asked for compaction headroom). The driver answers with
    /// `Event::SnapshotLoaded`.
    LoadSnapshot {
        /// The identity of the snapshot to load.
        meta: SnapshotMeta,
    },
    /// Answer a membership change proposal.
    ConfigResponse {
        /// The change that was proposed.
        change: ConfigChange,
        /// The outcome.
        result: ConfigResult,
    },
    /// This node was a leader and stepped down on discovering a higher
    /// term. Drivers use it to fail in-flight leader work promptly.
    StepDown {
        /// The term the node stepped down into.
        term: Term,
    },
}
