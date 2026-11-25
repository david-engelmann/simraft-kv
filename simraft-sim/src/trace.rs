//! Canonical trace lines and domain-separated state and log digests.
//!
//! Docs/ARCHITECTURE.md sections 3.3 and 7.5. Two runs of the same
//! seed in separate processes must produce byte-identical traces and
//! digests (invariant D-01), so trace lines are plain text in a
//! fixed format and every digest hashes an explicit domain prefix
//! before its canonical payload bytes.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

/// Domain prefix hashed before final simulator state digest inputs
/// (docs/ARCHITECTURE.md section 7.5).
pub const STATE_DIGEST_DOMAIN: &str = "simraft-kv/state-digest/v1";

/// Domain prefix hashed before per-node log digest inputs
/// (docs/ARCHITECTURE.md section 7.5).
pub const LOG_DIGEST_DOMAIN: &str = "simraft-kv/log-digest/v1";

/// An ordered record of trace lines for one run.
#[derive(Debug, Clone, Default)]
pub struct Trace {
    lines: Vec<String>,
}

impl Trace {
    /// An empty trace.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one line stamped with its tick.
    pub fn push(&mut self, tick: u64, text: &str) {
        self.lines.push(format!("tick={tick} {text}"));
    }

    /// All lines, in order.
    #[must_use]
    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    /// The trace rendered as one string, lines joined by newlines
    /// with a trailing newline.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        for line in &self.lines {
            out.push_str(line);
            out.push('\n');
        }
        out
    }
}

/// The lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn digest_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The lowercase hex SHA-256 of `domain` followed by `bytes`
/// (docs/ARCHITECTURE.md section 7.5).
#[must_use]
pub fn domain_digest_hex(domain: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for byte in digest {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_the_empty_sha256_and_domains_separate() {
        assert_eq!(
            digest_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_ne!(
            domain_digest_hex(STATE_DIGEST_DOMAIN, b"same"),
            domain_digest_hex(LOG_DIGEST_DOMAIN, b"same"),
            "distinct domain tags must produce distinct digests on identical bytes"
        );
    }
}
