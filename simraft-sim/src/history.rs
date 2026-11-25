//! Client operation history recording.
//!
//! Docs/ARCHITECTURE.md section 12.6: every client operation is
//! an invoke and, when it completes, a return, stamped with
//! virtual ticks. Operations that never return stay pending and
//! are handed to the checker as incomplete, which is the standard
//! treatment.

/// One client operation against the key-value store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// Store `value` under `key`.
    Put {
        /// The key bytes.
        key: Vec<u8>,
        /// The value bytes.
        value: Vec<u8>,
    },
    /// Remove `key`.
    Delete {
        /// The key bytes.
        key: Vec<u8>,
    },
    /// Read `key` (served by `ReadIndex`, never appended to the log).
    Get {
        /// The key bytes.
        key: Vec<u8>,
    },
}

impl Op {
    /// The key this operation touches.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } | Self::Get { key } => key,
        }
    }
}

/// The observed outcome of a completed operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    /// A Put or Delete applied.
    Ack,
    /// A Get outcome: the value seen, or None when the key was
    /// absent at the read's linearization point.
    Value(Option<Vec<u8>>),
    /// The operation was rejected without applying (its only
    /// constraint on a linearization is real-time order).
    Error(String),
}

/// One recorded operation: invoke and return ticks, the operation,
/// and its result when it completed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpRecord {
    /// The record's position in the history (its invoke order).
    pub id: usize,
    /// The issuing client.
    pub client: u64,
    /// The virtual tick at which the client invoked the operation.
    pub invoke_tick: u64,
    /// The virtual tick at which the result returned, or None when
    /// the operation never completed (pending).
    pub return_tick: Option<u64>,
    /// The operation itself.
    pub op: Op,
    /// The observed result for a completed operation.
    pub result: Option<OpResult>,
}

/// An append-only record of client operations.
#[derive(Debug, Clone, Default)]
pub struct History {
    records: Vec<OpRecord>,
}

impl History {
    /// An empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an invocation at `tick`; returns the record id to
    /// complete later.
    pub fn invoke(&mut self, client: u64, tick: u64, op: Op) -> usize {
        let id = self.records.len();
        self.records.push(OpRecord {
            id,
            client,
            invoke_tick: tick,
            return_tick: None,
            op,
            result: None,
        });
        id
    }

    /// Record the return of the operation with the given id.
    pub fn complete(&mut self, id: usize, tick: u64, result: OpResult) {
        if let Some(record) = self.records.get_mut(id) {
            record.return_tick = Some(tick);
            record.result = Some(result);
        }
    }

    /// All records, in invoke order.
    #[must_use]
    pub fn records(&self) -> &[OpRecord] {
        &self.records
    }

    /// The number of recorded operations.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether no operations were recorded.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
