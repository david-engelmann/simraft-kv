//! Static configuration for a Raft node and its validation.
//!
//! Timing validation (invariant T-01) enforces the ratios the liveness
//! argument depends on: `min_election_timeout >= 10 * heartbeat_interval`
//! and `max_election_timeout >= 2 * min_election_timeout`, with
//! `max_election_timeout > min_election_timeout` so the randomized
//! election window is never empty.

use crate::error::ConfigError;

/// Static per-node configuration. All durations are in ticks (virtual
/// milliseconds, supplied by the driver).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// How often a leader sends heartbeats, in ticks.
    pub heartbeat_interval: u64,
    /// The shortest election timeout a follower or candidate will use.
    pub min_election_timeout: u64,
    /// The longest election timeout a follower or candidate will use.
    pub max_election_timeout: u64,
    /// The most entries one `AppendEntries` message may carry.
    pub max_entries_per_append: usize,
    /// The approximate byte budget for one `AppendEntries` message.
    pub max_append_bytes: usize,
    /// The chunk size for `InstallSnapshot` transfers (used from M3).
    pub snapshot_chunk_bytes: usize,
    /// The most client sessions the KV state machine retains (used from M2).
    pub max_sessions: usize,
    /// The longest uncommitted log tail the in-flight dedup scan covers
    /// (used from M2).
    pub max_uncommitted_entries: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: 50,
            min_election_timeout: 500,
            max_election_timeout: 1_000,
            max_entries_per_append: 64,
            max_append_bytes: 1_048_576,
            snapshot_chunk_bytes: 65_536,
            max_sessions: 10_000,
            max_uncommitted_entries: 10_000,
        }
    }
}

impl Config {
    /// Validate this configuration.
    ///
    /// # Errors
    ///
    /// Returns a `ConfigError` naming both offending values when a timing
    /// ratio is violated, a limit is zero, or a duration is zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.heartbeat_interval == 0 {
            return Err(ConfigError::ZeroValue {
                field: "heartbeat_interval",
            });
        }
        if self.min_election_timeout == 0 {
            return Err(ConfigError::ZeroValue {
                field: "min_election_timeout",
            });
        }
        let required_min = self.heartbeat_interval.saturating_mul(10);
        if self.min_election_timeout < required_min {
            return Err(ConfigError::ElectionTimeoutRatio {
                min_election_timeout: self.min_election_timeout,
                heartbeat_interval: self.heartbeat_interval,
                required_min,
            });
        }
        if self.max_election_timeout <= self.min_election_timeout {
            return Err(ConfigError::ElectionRangeOrder {
                min_election_timeout: self.min_election_timeout,
                max_election_timeout: self.max_election_timeout,
            });
        }
        let required_min_max = self.min_election_timeout.saturating_mul(2);
        if self.max_election_timeout < required_min_max {
            return Err(ConfigError::ElectionRangeRatio {
                min_election_timeout: self.min_election_timeout,
                max_election_timeout: self.max_election_timeout,
                required_min_max,
            });
        }
        for (field, value) in [
            ("max_entries_per_append", self.max_entries_per_append),
            ("max_append_bytes", self.max_append_bytes),
            ("snapshot_chunk_bytes", self.snapshot_chunk_bytes),
            ("max_sessions", self.max_sessions),
            ("max_uncommitted_entries", self.max_uncommitted_entries),
        ] {
            if value == 0 {
                return Err(ConfigError::ZeroValue { field });
            }
        }
        Ok(())
    }
}
