//! The deterministic key-value state machine.
//!
//! Applies committed log entries in index order, executes each client
//! command exactly once per (client, sequence) pair, and serves
//! `ReadIndex` reads pinned to an applied index. Deduplication semantics
//! (docs/ARCHITECTURE.md section 10): the session table remembers each
//! client's last sequence and its cached response; a retry of the last
//! sequence returns the cached response without re-executing; a
//! sequence gap is rejected with an error result and never applied;
//! the table is bounded by `max_sessions`, evicting the least recently
//! applied committed session, after which a retry of that client's
//! sequence may re-execute (the documented eviction contract).

use crate::command::{CommandResult, KvOp, decode_command};
use crate::error::KvError;
use simraft_core::{ClientId, EntryKind, LogEntry, LogIndex};
use std::collections::BTreeMap;

/// `CommandResult::Error` code for a rejected sequence gap.
pub const ERR_SESSION_GAP: u32 = 1;
/// `CommandResult::Error` code for an undecodable committed payload.
pub const ERR_DECODE: u32 = 2;

/// What applying one entry did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The entry was not a command (`NoOp` or configuration).
    NotCommand,
    /// The command executed; `result` is its outcome.
    Applied {
        /// The execution outcome.
        result: CommandResult,
    },
    /// The command was a retry of the client's last sequence; the
    /// cached response was returned and nothing re-executed.
    Cached {
        /// The originally recorded response.
        result: CommandResult,
    },
    /// The command repeated a sequence older than the client's last;
    /// its response is no longer retained. Nothing re-executed.
    DuplicateStale,
    /// The entry could not be executed (undecodable payload). The
    /// apply cursor still advanced past it: a committed entry must
    /// never wedge the state machine.
    Failed {
        /// The error that prevented execution.
        error: KvError,
    },
}

/// One client's session record.
#[derive(Debug, Clone)]
struct SessionEntry {
    last_seq: u64,
    response: CommandResult,
    applied_index: LogIndex,
}

/// The KV state machine: store, sessions, and the apply cursor.
#[derive(Debug)]
pub struct KvStateMachine {
    store: BTreeMap<Vec<u8>, Vec<u8>>,
    sessions: BTreeMap<ClientId, SessionEntry>,
    max_sessions: usize,
    last_applied: LogIndex,
}

impl KvStateMachine {
    /// A fresh state machine retaining at most `max_sessions` client
    /// sessions.
    #[must_use]
    pub const fn new(max_sessions: usize) -> Self {
        Self {
            store: BTreeMap::new(),
            sessions: BTreeMap::new(),
            max_sessions,
            last_applied: LogIndex::ZERO,
        }
    }

    /// The highest log index applied so far.
    #[must_use]
    pub const fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// How many client sessions are currently retained.
    #[must_use]
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// The current value of `key` in the store, ignoring apply
    /// ordering. Drivers serving reads use `read` instead.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.store.get(key).map(Vec::as_slice)
    }

    /// Serve a `ReadIndex` read: `None` until this machine has applied
    /// through `read_index`; then the value of `key` at that point
    /// (`Some(None)` when the key is absent). Because applies are
    /// sequential and this machine holds no newer unapplied state, a
    /// read gated this way observes exactly the writes committed at or
    /// before `read_index`.
    #[must_use]
    pub fn read(&self, read_index: LogIndex, key: &[u8]) -> Option<Option<&[u8]>> {
        if self.last_applied < read_index {
            return None;
        }
        Some(self.get(key))
    }

    /// Apply the committed entry at `index`.
    ///
    /// # Errors
    ///
    /// Returns `KvError::ApplyGap` when `index` is not the next index:
    /// applies are strictly sequential, and a gap means the driver lost
    /// an entry. Re-applying an index at or below the cursor is a
    /// no-op returning `ApplyOutcome::NotCommand`-equivalent stale
    /// handling: it returns the `Failed` variant with an `ApplyGap`
    /// error only when the index skips ahead; an index at or below the
    /// cursor is reported as `ApplyOutcome::DuplicateStale`.
    pub fn apply(&mut self, index: LogIndex, entry: &LogEntry) -> Result<ApplyOutcome, KvError> {
        if index <= self.last_applied {
            return Ok(ApplyOutcome::DuplicateStale);
        }
        if index != self.last_applied.next() {
            return Err(KvError::ApplyGap {
                expected: self.last_applied.next(),
                received: index,
            });
        }
        self.last_applied = index;
        let outcome = match &entry.kind {
            EntryKind::NoOp | EntryKind::Config { .. } => ApplyOutcome::NotCommand,
            EntryKind::Command { payload, .. } => self.apply_command(index, payload),
        };
        Ok(outcome)
    }

    /// Execute one command payload under the session rules of
    /// docs/ARCHITECTURE.md section 10.
    fn apply_command(&mut self, index: LogIndex, payload: &[u8]) -> ApplyOutcome {
        let (client, seq, op) = match decode_command(payload) {
            Ok(decoded) => decoded,
            Err(error) => return ApplyOutcome::Failed { error },
        };
        if let Some(session) = self.sessions.get(&client) {
            match seq.cmp(&session.last_seq) {
                std::cmp::Ordering::Equal => {
                    return ApplyOutcome::Cached {
                        result: session.response.clone(),
                    };
                }
                std::cmp::Ordering::Less => return ApplyOutcome::DuplicateStale,
                std::cmp::Ordering::Greater => {
                    if seq != session.last_seq + 1 {
                        return ApplyOutcome::Applied {
                            result: Self::gap_error(client, session.last_seq + 1, seq),
                        };
                    }
                }
            }
        } else if seq != 1 {
            return ApplyOutcome::Applied {
                result: Self::gap_error(client, 1, seq),
            };
        }
        let result = match &op {
            KvOp::Put { key, value } => {
                self.store.insert(key.clone(), value.clone());
                CommandResult::Ack
            }
            KvOp::Delete { key } => {
                self.store.remove(key);
                CommandResult::Ack
            }
        };
        self.sessions.insert(
            client,
            SessionEntry {
                last_seq: seq,
                response: result.clone(),
                applied_index: index,
            },
        );
        self.evict_sessions();
        ApplyOutcome::Applied { result }
    }

    /// The error result for a sequence gap: the command did not
    /// execute and the session did not advance (C-04).
    fn gap_error(client: ClientId, expected: u64, received: u64) -> CommandResult {
        CommandResult::Error {
            code: ERR_SESSION_GAP,
            message: KvError::SessionGap {
                client_id: client,
                expected,
                received,
            }
            .to_string(),
        }
    }

    /// Evict least-recently-applied sessions until the table is within
    /// its bound (C-03). The just-applied session carries the highest
    /// applied index, so it is never the victim unless it is the only
    /// session and the bound is zero.
    fn evict_sessions(&mut self) {
        while self.sessions.len() > self.max_sessions {
            let victim = self
                .sessions
                .iter()
                .min_by_key(|(_, session)| session.applied_index)
                .map(|(client, _)| *client);
            match victim {
                Some(client) => {
                    self.sessions.remove(&client);
                }
                None => break,
            }
        }
    }
}
