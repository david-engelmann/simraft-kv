//! simraft-core: a pure sans-I/O Raft consensus state machine.
//!
//! A [`Node`] consumes [`Event`] values and returns [`Action`] values.
//! It reads no clock, opens no socket, touches no disk, spawns no
//! thread, and draws no randomness from the operating system. Drivers
//! (the deterministic simulator in simraft-sim, or a future I/O adapter)
//! supply time, transport, storage, and seeds, which is what makes the
//! whole cluster reproducible from a single seed.
//!
//! Implemented at M1: leader election with Pre-Vote, log replication
//! with conflict-term fast backup, monotonic match tracking, and
//! persist-before-response ordering. Commitment rules and ReadIndex
//! follow at M2; snapshots and membership changes at M3.
//!
//! This crate is strictly std-only (DECISIONS Q5) and forbids unsafe
//! code.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod action;
pub mod config;
pub mod error;
pub mod event;
pub mod ids;
pub mod log;
pub mod membership;
pub mod message;
pub mod node;

pub use action::{Action, ClientResult};
pub use config::Config;
pub use error::ConfigError;
pub use event::{Event, HardState, PersistKind};
pub use ids::{ClientId, LogIndex, NodeId, Term, Tick};
pub use log::{EntryKind, Log, LogEntry};
pub use membership::ClusterConfig;
pub use message::Message;
pub use node::{Node, Role};
