//! simraft-sim: the deterministic simulation harness for simraft-kv.
//!
//! A single-threaded, single-seed simulator drives many simraft-core
//! nodes over a virtual clock, a fault-injecting packet network, and
//! durable-versus-volatile simulated disks, checking the safety
//! oracles from docs/INVARIANTS.md after every tick and verifying
//! recorded client histories with a per-key Wing and Gong
//! linearizability checker (docs/ARCHITECTURE.md section 12).
//!
//! This crate forbids unsafe code.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod clock;
pub mod completions;
pub mod disk;
pub mod faults;
pub mod harness;
pub mod history;
pub mod liz;
pub mod oracle;
pub mod rng;
pub mod trace;

/// The section 7.1 frame codec lives in simraft-wire, shared
/// with the simraft-node adapter; it is re-exported here under
/// its original module path.
pub use simraft_wire as wire;

/// The version of the simulator trace format emitted by this crate.
pub const TRACE_FORMAT_VERSION: u16 = 1;

pub use completions::{Shell, completions_script};
pub use faults::{DirectedCut, FaultConfig, ScriptedOp};
pub use harness::{Harness, RunReport, SimConfig};
pub use history::{History, Op, OpRecord, OpResult};
pub use liz::{Checker, Verdict};
pub use oracle::{NodeView, OracleState, OracleViolation};
pub use rng::Rng;
pub use trace::{LOG_DIGEST_DOMAIN, STATE_DIGEST_DOMAIN, Trace, digest_hex, domain_digest_hex};
pub use wire::{WireError, decode_frame, encode_frame};
