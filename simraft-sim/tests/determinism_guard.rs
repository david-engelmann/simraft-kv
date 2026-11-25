//! Determinism source guard and gate-blindness check (invariant D-02).
//!
//! Scans all Rust source files across the four deterministic crates
//! (`simraft-core`, `simraft-kv`, `simraft-wire`, and `simraft-sim`)
//! for forbidden nondeterministic identifiers outside line comments,
//! including braced `std::collections` imports, and verifies gate
//! non-blindness by planting synthetic violations in scratch inputs.

use std::path::{Path, PathBuf};

const fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn contains_word(haystack: &str, word: &str) -> bool {
    let bytes = haystack.as_bytes();
    let wbytes = word.as_bytes();
    let mut start = 0;
    while let Some(pos) = haystack[start..].find(word) {
        let idx = start + pos;
        let before_ok = idx == 0 || !is_word_byte(bytes[idx - 1]);
        let after_idx = idx + wbytes.len();
        let after_ok = after_idx == bytes.len() || !is_word_byte(bytes[after_idx]);
        if before_ok && after_ok {
            return true;
        }
        start = idx + 1;
    }
    false
}

fn scan_rust_source(path_label: &str, source: &str) -> Vec<String> {
    let mut hits = Vec::new();
    let words = [
        concat!("Hash", "Map"),
        concat!("Hash", "Set"),
        concat!("System", "Time"),
        concat!("thread_", "rng"),
        concat!("Os", "Rng"),
    ];
    let instant_now = concat!("Instant", "::now");
    for (lineno, raw_line) in source.lines().enumerate() {
        let trimmed = raw_line.trim_start();
        if trimmed.starts_with("//") {
            continue;
        }
        let code = raw_line.split("//").next().unwrap_or(raw_line);
        let forbidden = words.iter().any(|w| contains_word(code, w)) || code.contains(instant_now);
        if forbidden {
            hits.push(format!("{path_label}:{}: {}", lineno + 1, raw_line.trim()));
        }
    }
    hits
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir).expect("read crate directory");
    let mut paths: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn deterministic_crates_contain_no_forbidden_nondeterminism_identifiers() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let root = manifest_dir.parent().expect("workspace root");
    let crates = ["simraft-core", "simraft-kv", "simraft-wire", "simraft-sim"];

    let mut violations = Vec::new();
    for krate in crates {
        let mut files = Vec::new();
        collect_rs_files(&root.join(krate), &mut files);
        assert!(!files.is_empty(), "must scan source files in {krate}");
        for file in files {
            let text = std::fs::read_to_string(&file).expect("read source file");
            let rel = file
                .strip_prefix(root)
                .unwrap_or(&file)
                .display()
                .to_string();
            violations.extend(scan_rust_source(&rel, &text));
        }
    }
    assert!(
        violations.is_empty(),
        "D-02: forbidden nondeterminism identifiers found:\n{}",
        violations.join("\n")
    );
}

#[test]
fn determinism_guard_catches_planted_violations_including_braced_imports() {
    let planted = [
        (
            "scratch/braced_map.rs",
            concat!("use std::collections::{BTreeMap, ", "Hash", "Map};\n"),
        ),
        (
            "scratch/braced_set.rs",
            concat!("use std::collections::{", "Hash", "Set, BTreeSet};\n"),
        ),
        (
            "scratch/sys_time.rs",
            concat!("let t = std::time::", "System", "Time::now();\n"),
        ),
        (
            "scratch/instant.rs",
            concat!("let start = ", "Instant", "::now();\n"),
        ),
        (
            "scratch/rng.rs",
            concat!("let mut r = rand::", "thread_", "rng();\n"),
        ),
        ("scratch/os_rng.rs", concat!("let r = ", "Os", "Rng;\n")),
    ];
    for (label, snippet) in planted {
        let hits = scan_rust_source(label, snippet);
        assert_eq!(
            hits.len(),
            1,
            "D-02 gate-blindness check failed to catch planted violation in {label}: {snippet}"
        );
    }

    let allowed_comment = concat!(
        "// ",
        "Hash",
        "Map and ",
        "Instant",
        "::now are forbidden in code, allowed in comments.\nlet map = std::collections::BTreeMap::<u64, u64>::new();\n"
    );
    assert!(
        scan_rust_source("scratch/comment.rs", allowed_comment).is_empty(),
        "comment lines must not trigger false positives"
    );
}
