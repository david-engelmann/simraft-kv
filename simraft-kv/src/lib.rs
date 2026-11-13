//! simraft-kv: the key-value state machine for the simraft Raft core.
//!
//! This crate holds the deterministic KV state machine, the canonical
//! command and snapshot payload encodings (docs/ARCHITECTURE.md section
//! 7), and the client session deduplication table. Those land at M2.
//! At M1 the crate is the workspace placeholder the architecture
//! reserves: the core in simraft-core is deliberately KV-agnostic and
//! treats command payloads as opaque bytes until this crate gives them
//! meaning.
//!
//! This crate is strictly std-only (DECISIONS Q5) and forbids unsafe
//! code.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// The version of the canonical command and snapshot payload schema
/// implemented by this crate (docs/ARCHITECTURE.md section 7).
pub const SCHEMA_VERSION: u16 = 1;
