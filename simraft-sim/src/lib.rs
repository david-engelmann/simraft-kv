//! simraft-sim: the deterministic simulation harness for simraft-kv.
//!
//! A single-threaded, single-seed simulator drives many simraft-core
//! nodes over a virtual clock, a fault-injecting packet network, and
//! durable-versus-volatile simulated disks, checking the safety oracles
//! from docs/INVARIANTS.md after every tick and verifying recorded
//! client histories with a per-key linearizability checker.
//!
//! The harness lands at M4. At M1 this crate is the workspace
//! placeholder the architecture reserves; the protocol nodes it will
//! drive live in simraft-core and are tested there directly until the
//! harness generalizes those drivers.
//!
//! This crate forbids unsafe code.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// The version of the simulator trace format emitted by this crate.
pub const TRACE_FORMAT_VERSION: u16 = 1;
