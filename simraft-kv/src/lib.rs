//! simraft-kv: the key-value state machine for the simraft Raft core.
//!
//! This crate holds the deterministic KV state machine, the canonical
//! command and snapshot payload encodings (docs/ARCHITECTURE.md section
//! 7), and the client session deduplication table (section 10). The
//! core in simraft-core is deliberately KV-agnostic and treats command
//! payloads as opaque bytes; this crate gives them meaning.
//!
//! This crate is strictly std-only (DECISIONS Q5) and forbids unsafe
//! code.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod command;
pub mod error;
pub mod machine;

/// The version of the canonical command and snapshot payload schema
/// implemented by this crate (docs/ARCHITECTURE.md section 7).
pub const SCHEMA_VERSION: u16 = 1;

pub use command::{
    CommandResult, KvOp, decode_command, decode_result, encode_command, encode_result,
};
pub use error::KvError;
pub use machine::{ApplyOutcome, KvStateMachine};
