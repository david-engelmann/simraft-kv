//! The CLI contract (invariant D-03): every run prints its active
//! seed on its first output line, and a seed replays byte for byte
//! so a failing CI seed reproduces locally with that single value.

use std::process::Command;

fn run_json(seed: u64) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
        .args(["--seed", &seed.to_string(), "--seeds", "1", "--json"])
        .env("SIM_SEED", seed.to_string())
        .output()
        .expect("the sim binary runs");
    assert!(
        output.status.success(),
        "the run must pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is UTF-8")
}

#[test]
fn seed_is_printed_and_replays_failure_free() {
    let first = run_json(42);
    let first_line = first.lines().next().unwrap_or("");
    assert!(
        first_line.contains("\"seed\":42"),
        "D-03: the first line names the seed, got: {first_line}"
    );
    assert!(
        first_line.contains("\"passed\":true"),
        "the run must pass, got: {first_line}"
    );
    let replay = run_json(42);
    assert_eq!(first, replay, "D-03: a seed replays byte for byte");
}
