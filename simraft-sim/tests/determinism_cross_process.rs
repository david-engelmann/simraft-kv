//! Cross-process seed determinism (invariant D-01): two runs of
//! the simulator binary in separate OS processes with the same
//! seed produce byte-identical output, digests included.

use std::process::Command;

fn run_binary(seed: u64) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
        .args(["--seed", &seed.to_string(), "--seeds", "2"])
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
fn same_seed_same_bytes_in_separate_processes() {
    let first = run_binary(7);
    let second = run_binary(7);
    assert_eq!(first, second, "D-01: identical seeds, identical bytes");
    assert!(
        first.contains("seed=7"),
        "D-03: the seed is printed on the first output line"
    );
}
