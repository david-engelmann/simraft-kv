//! Identifier and index newtypes shared by every simraft-core module.
//!
//! All identifiers are plain `u64` wrappers. They carry no behavior beyond
//! ordering, formatting, and the small amount of arithmetic the protocol
//! performs on terms and log indexes, so that raw integers never mix
//! domains (a term is never silently used as an index).

use core::fmt;

/// A virtual time value in ticks, supplied by the driver.
///
/// The core never reads a wall clock. One tick is one virtual millisecond
/// by convention; the protocol only ever compares ticks against deadlines
/// it computed itself.
pub type Tick = u64;

/// Identifies one node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeId(pub u64);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Identifies one client session. Chosen by the client at session start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ClientId(pub u64);

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "c{}", self.0)
    }
}

/// A Raft term: a monotonically increasing logical clock value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

impl Term {
    /// The term before any election has happened.
    pub const ZERO: Self = Self(0);

    /// The next term.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "term {}", self.0)
    }
}

/// A position in the replicated log. Index 0 is the sentinel empty entry;
/// the first real entry is index 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// The sentinel index before the first real entry.
    pub const ZERO: Self = Self(0);

    /// The next index.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "index {}", self.0)
    }
}
