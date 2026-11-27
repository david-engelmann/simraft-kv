//! simraft-node: the real I/O adapter for the simraft Raft core.
//!
//! This crate owns what the core deliberately does not: TCP
//! sockets, files, and a wall clock. It runs one `simraft_core`
//! node as a real process, carrying the section 7.1 frame format
//! over TCP, persisting to the durable record layout of section
//! 7.3 with the fsync discipline the core's
//! persist-before-response contract assumes, and serving client
//! Put, Get, and Delete operations against the KV state machine.
//!
//! The runtime is three modules behind this contract: `store`
//! (the section 7.3 durable records over files), `runtime`
//! (threads, TCP transport in the section 7.1 frame format, and
//! the driver loop), and `client` (the SRCL client protocol and
//! the redirect-following, retrying client implementation).
//!
//! Lane 1's recorded design question is settled: the section
//! 7.1 frame codec lives in simraft-wire, shared by the
//! simulator and this adapter, leaving the core
//! serialization-free as docs/ARCHITECTURE.md section 3
//! requires.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod completions;
pub mod runtime;
pub mod store;

pub use client::Client;
pub use completions::{Shell, completions_script};

use std::collections::BTreeSet;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

use simraft_core::{ClusterConfig, NodeId};

/// One cluster member: a node id and the address its peers and
/// clients reach it at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberConfig {
    /// The member's node id.
    pub id: NodeId,
    /// The member's peer and client address.
    pub addr: SocketAddr,
}

impl MemberConfig {
    /// Parse a member specification of the form `id@host:port`.
    ///
    /// # Errors
    /// Returns `NodeError::BadMemberSpec` naming the specification
    /// and the part that failed to parse.
    pub fn parse(spec: &str) -> Result<Self, NodeError> {
        let bad = |reason: &str| NodeError::BadMemberSpec {
            spec: spec.to_string(),
            reason: reason.to_string(),
        };
        let (id_part, addr_part) = spec.split_once('@').ok_or_else(|| bad("missing @"))?;
        let id: u64 = id_part
            .parse()
            .map_err(|_| bad("the id before @ is not an unsigned integer"))?;
        let addr: SocketAddr = addr_part
            .parse()
            .map_err(|_| bad("the address after @ is not a socket address"))?;
        Ok(Self {
            id: NodeId(id),
            addr,
        })
    }
}

/// The full configuration of one node process.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node's id. It must appear in `members`.
    pub id: NodeId,
    /// Every cluster member, including this node, as voters.
    pub members: Vec<MemberConfig>,
    /// The directory holding this node's durable records.
    pub data_dir: PathBuf,
    /// Wall-clock milliseconds between core `Tick` events.
    pub tick_ms: u64,
}

/// The smallest accepted tick interval, in milliseconds.
pub const MIN_TICK_MS: u64 = 1;
/// The largest accepted tick interval, in milliseconds.
pub const MAX_TICK_MS: u64 = 10_000;

impl NodeConfig {
    /// Validate the configuration against the contract the
    /// runtime relies on.
    ///
    /// # Errors
    /// Returns the first violation found, with the deciding
    /// values named: an empty member set, a duplicated member id,
    /// this node's id missing from the member set, or a tick
    /// interval outside the accepted bounds.
    pub fn validate(&self) -> Result<(), NodeError> {
        if self.members.is_empty() {
            return Err(NodeError::EmptyCluster);
        }
        let mut seen: BTreeSet<NodeId> = BTreeSet::new();
        for member in &self.members {
            if !seen.insert(member.id) {
                return Err(NodeError::DuplicateNodeId { id: member.id });
            }
        }
        if !seen.contains(&self.id) {
            return Err(NodeError::SelfNotInCluster { id: self.id });
        }
        if !(MIN_TICK_MS..=MAX_TICK_MS).contains(&self.tick_ms) {
            return Err(NodeError::TickOutOfRange {
                tick_ms: self.tick_ms,
            });
        }
        Ok(())
    }

    /// The core cluster configuration this node runs with: every
    /// member a voter, no learners.
    ///
    /// # Errors
    /// Returns the validation error when the configuration is
    /// invalid, and wraps the core's configuration error when
    /// the member set itself is rejected.
    pub fn cluster_config(&self) -> Result<ClusterConfig, NodeError> {
        self.validate()?;
        ClusterConfig::new(self.members.iter().map(|m| m.id), Vec::new()).map_err(|e| {
            NodeError::CoreConfig {
                reason: e.to_string(),
            }
        })
    }
}

/// A client operation against the cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOp {
    /// Store `value` under `key`.
    Put {
        /// The key to store under.
        key: Vec<u8>,
        /// The value to store.
        value: Vec<u8>,
    },
    /// Fetch the value under `key`, if any.
    Get {
        /// The key to fetch.
        key: Vec<u8>,
    },
    /// Remove `key` and its value, if present.
    Delete {
        /// The key to remove.
        key: Vec<u8>,
    },
}

/// The adapter's error type: every variant names the values that
/// decided it.
#[derive(Debug)]
pub enum NodeError {
    /// The member set is empty.
    EmptyCluster,
    /// A member id appears more than once.
    DuplicateNodeId {
        /// The duplicated id.
        id: NodeId,
    },
    /// This node's id is not in the member set.
    SelfNotInCluster {
        /// The id that was not found.
        id: NodeId,
    },
    /// The tick interval is outside the accepted bounds.
    TickOutOfRange {
        /// The rejected interval, in milliseconds.
        tick_ms: u64,
    },
    /// A member specification string failed to parse.
    BadMemberSpec {
        /// The specification as given.
        spec: String,
        /// What failed about it.
        reason: String,
    },
    /// The core rejected the derived cluster configuration.
    CoreConfig {
        /// The core's rejection, as text.
        reason: String,
    },
    /// A filesystem operation failed.
    Io {
        /// The path the operation targeted.
        path: PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A durable record is corrupt in a way recovery cannot
    /// repair (a torn log tail is repaired and reported, not
    /// an error).
    CorruptStore {
        /// What is corrupt, with the deciding values.
        detail: String,
    },
    /// A client-side operation could not complete: a transport
    /// failure, a timeout, or a server report, with context.
    Unavailable {
        /// The failure, with the deciding values.
        detail: String,
    },
}

impl fmt::Display for NodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCluster => write!(f, "the member set is empty"),
            Self::DuplicateNodeId { id } => {
                write!(f, "member id {id} appears more than once")
            }
            Self::SelfNotInCluster { id } => {
                write!(f, "this node's id {id} is not in the member set")
            }
            Self::TickOutOfRange { tick_ms } => write!(
                f,
                "tick interval {tick_ms} ms is outside {MIN_TICK_MS}..={MAX_TICK_MS} ms"
            ),
            Self::BadMemberSpec { spec, reason } => {
                write!(f, "bad member specification {spec:?}: {reason}")
            }
            Self::CoreConfig { reason } => {
                write!(f, "the core rejected the cluster configuration: {reason}")
            }
            Self::Io { path, source } => {
                write!(f, "I/O failed at {}: {source}", path.display())
            }
            Self::CorruptStore { detail } => {
                write!(f, "the durable store is corrupt: {detail}")
            }
            Self::Unavailable { detail } => {
                write!(f, "the operation is unavailable: {detail}")
            }
        }
    }
}

impl std::error::Error for NodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Run one node process until it is stopped.
///
/// The implementation is `runtime::run_node`: boot restores
/// the durable state, then the driver loop serves until the
/// process is stopped or a storage failure ends the run.
///
/// # Errors
/// Returns the first validation, configuration, or storage
/// failure, with context.
pub fn run_node(config: &NodeConfig) -> Result<(), NodeError> {
    runtime::run_node(config)
}

/// Issue one client operation against the member at `server`.
///
/// The implementation is `client::run_client`: it follows
/// leader redirects between members and retries transient
/// failures within a budget, always under one (client id,
/// sequence) identity, and returns the value for a Get and
/// `None` for writes.
///
/// # Errors
/// Returns `NodeError::Unavailable` with the last failure's
/// context when the operation cannot complete.
pub fn run_client(server: SocketAddr, op: &ClientOp) -> Result<Option<Vec<u8>>, NodeError> {
    client::run_client(server, op)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(id: u64, port: u16) -> MemberConfig {
        MemberConfig {
            id: NodeId(id),
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    fn config() -> NodeConfig {
        NodeConfig {
            id: NodeId(1),
            members: vec![member(1, 7001), member(2, 7002), member(3, 7003)],
            data_dir: PathBuf::from("node-data"),
            tick_ms: 50,
        }
    }

    #[test]
    fn valid_config_validates_and_derives_cluster() {
        let cfg = config();
        cfg.validate().expect("the fixture config is valid");
        let cluster = cfg.cluster_config().expect("the cluster derives");
        assert_eq!(cluster.voters.len(), 3);
        assert!(cluster.learners.is_empty());
    }

    #[test]
    fn duplicate_member_id_is_named() {
        let mut cfg = config();
        cfg.members.push(member(2, 7009));
        let err = cfg.validate().expect_err("a duplicate must fail");
        assert!(
            matches!(err, NodeError::DuplicateNodeId { id: NodeId(2) }),
            "the duplicate is named: {err}"
        );
    }

    #[test]
    fn missing_self_is_named() {
        let mut cfg = config();
        cfg.id = NodeId(9);
        let err = cfg.validate().expect_err("a missing self must fail");
        assert!(
            matches!(err, NodeError::SelfNotInCluster { id: NodeId(9) }),
            "the missing id is named: {err}"
        );
    }

    #[test]
    fn empty_cluster_and_tick_bounds_are_rejected() {
        let mut cfg = config();
        cfg.members.clear();
        assert!(matches!(cfg.validate(), Err(NodeError::EmptyCluster)));
        let mut cfg = config();
        cfg.tick_ms = 0;
        assert!(matches!(
            cfg.validate(),
            Err(NodeError::TickOutOfRange { tick_ms: 0 })
        ));
    }

    #[test]
    fn member_spec_parses_and_names_the_bad_part() {
        let parsed = MemberConfig::parse("2@127.0.0.1:7002").expect("a good spec parses");
        assert_eq!(parsed, member(2, 7002));
        let err = MemberConfig::parse("2-127.0.0.1:7002").expect_err("a missing @ must fail");
        assert!(
            err.to_string().contains("missing @"),
            "the error names the failure: {err}"
        );
        let err = MemberConfig::parse("x@127.0.0.1:7002").expect_err("a bad id must fail");
        assert!(
            err.to_string().contains("not an unsigned integer"),
            "the error names the failure: {err}"
        );
    }
}
