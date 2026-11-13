//! RPC messages between nodes, as structured values.
//!
//! The core never serializes. The canonical byte layout for these
//! messages is specified in docs/ARCHITECTURE.md section 7.1 and is
//! implemented by the simulator's packet layer and by future adapters.
//! Message kinds are added per milestone: voting and AppendEntries at
//! M1, InstallSnapshot at M3.

use crate::ids::{LogIndex, Term};
use crate::log::LogEntry;

/// One RPC message. The sender and recipient travel in the envelope
/// (`Event::Message::from` and `Action::Send::to`); every variant carries
/// the term it was created under so stale senders are detectable from the
/// message alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// A candidate asks for a vote in a real election.
    RequestVote {
        /// The candidate's term.
        term: Term,
        /// The candidate's last log index.
        last_log_index: LogIndex,
        /// The term of the candidate's last log entry.
        last_log_term: Term,
    },
    /// The voter's answer to RequestVote.
    RequestVoteResponse {
        /// The voter's current term when it answered.
        term: Term,
        /// Whether the vote was granted.
        vote_granted: bool,
    },
    /// A pre-candidate asks whether it would win an election for the
    /// hypothetical term `term` (the sender's current term + 1), without
    /// the sender changing any persistent state.
    RequestPreVote {
        /// The hypothetical future term.
        term: Term,
        /// The pre-candidate's last log index.
        last_log_index: LogIndex,
        /// The term of the pre-candidate's last log entry.
        last_log_term: Term,
    },
    /// The voter's answer to RequestPreVote. Granting it changed no
    /// persistent state on the voter and did not reset its timer.
    RequestPreVoteResponse {
        /// The voter's current term when it answered.
        term: Term,
        /// Whether the pre-vote was granted.
        vote_granted: bool,
    },
    /// The leader replicates entries (or, with no entries, heartbeats)
    /// to a follower.
    AppendEntries {
        /// The leader's term.
        term: Term,
        /// The index immediately before the first entry in this batch.
        prev_log_index: LogIndex,
        /// The term of the entry at `prev_log_index`.
        prev_log_term: Term,
        /// The entries to append, in index order.
        entries: Vec<LogEntry>,
        /// The leader's commit index.
        leader_commit: LogIndex,
        /// The leader's current ReadIndex round token (0 means none;
        /// ReadIndex activates at M2).
        read_round: u64,
    },
    /// The follower's answer to AppendEntries.
    AppendEntriesResponse {
        /// The follower's current term when it answered.
        term: Term,
        /// Whether the batch was accepted.
        success: bool,
        /// On success, the highest index the follower now holds
        /// contiguously from this leader. Zero on failure.
        match_index: LogIndex,
        /// On failure, the first index of the conflicting term in the
        /// follower's log, or its last index + 1 when its log is too
        /// short. Zero when not applicable.
        conflict_index: LogIndex,
        /// On failure, the term at `conflict_index` in the follower's
        /// log, or term 0 when the log was simply too short.
        conflict_term: Term,
        /// The highest read round echoed by the follower (M2).
        read_round: u64,
    },
}
