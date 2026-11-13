//! Outputs of the sans-I/O core.
//!
//! `Node::step` returns actions in order. The driver executes them:
//! sends go to the network, persists go to storage, applies go to the
//! state machine. The core itself performs no I/O.

use crate::ids::{ClientId, LogIndex, NodeId, Term};
use crate::log::LogEntry;
use crate::message::Message;

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
    /// A ReadIndex read is confirmed linearizable. The driver serves
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
    /// A ReadIndex read cannot be served by this node (it is not the
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
    /// This node was a leader and stepped down on discovering a higher
    /// term. Drivers use it to fail in-flight leader work promptly.
    StepDown {
        /// The term the node stepped down into.
        term: Term,
    },
}
