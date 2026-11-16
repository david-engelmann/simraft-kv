//! Canonical trace lines and the state digest.
//!
//! Docs/ARCHITECTURE.md section 3.3. Two runs of the same seed in
//! separate processes must produce byte-identical traces and
//! digests (invariant D-01), so trace lines are plain text in a
//! fixed format and the digest is SHA-256 over canonical state
//! bytes the harness assembles in node order.

use std::fmt::Write as _;

use sha2::{Digest, Sha256};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_matches_the_empty_sha256() {
        assert_eq!(
            digest_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
