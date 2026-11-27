//! The CLI contract (invariant D-03): every run prints its active
//! seed on its first output line, and a seed replays byte for byte
//! so any run (including under `--chaos` or a failure) reproduces
//! locally with that single value.

use std::process::Command;

fn run_json(seed: u64, chaos: bool) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_simraft-sim"));
    cmd.args(["--seed", &seed.to_string(), "--seeds", "1", "--json"]);
    if chaos {
        cmd.arg("--chaos");
    }
    cmd.env("SIM_SEED", seed.to_string());
    let output = cmd.output().expect("the sim binary runs");
    assert!(
        output.status.success(),
        "the run must pass: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout is UTF-8")
}

#[test]
fn seed_is_printed_and_replays_failure_free() {
    let first = run_json(42, false);
    let first_line = first.lines().next().unwrap_or("");
    assert!(
        first_line.contains("\"seed\":42"),
        "D-03: the first line names the seed, got: {first_line}"
    );
    assert!(
        first_line.contains("\"passed\":true"),
        "the run must pass, got: {first_line}"
    );
    let replay = run_json(42, false);
    assert_eq!(first, replay, "D-03: a seed replays byte for byte");
}

#[test]
fn chaos_flag_runs_and_replays_deterministically() {
    let first = run_json(42, true);
    let replay = run_json(42, true);
    assert_eq!(first, replay, "D-01/D-03: --chaos replays byte identically");
    let clean = run_json(42, false);
    assert_ne!(
        first, clean,
        "--chaos must exercise faults distinct from the fault-free default"
    );

    // Explicit `--seed` overrides `SIM_SEED`, while `SIM_SEED` sets the
    // base seed when `--seed` is omitted.
    let explicit = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
        .args(["--seed", "42", "--seeds", "1", "--ticks", "200", "--json"])
        .env("SIM_SEED", "99")
        .output()
        .expect("sim runs");
    assert!(explicit.status.success());
    assert!(
        String::from_utf8_lossy(&explicit.stdout).contains("\"seed\":42"),
        "explicit --seed must override SIM_SEED"
    );

    let from_env = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
        .args(["--seeds", "1", "--ticks", "200", "--json"])
        .env("SIM_SEED", "99")
        .output()
        .expect("sim runs");
    assert!(from_env.status.success());
    assert!(
        String::from_utf8_lossy(&from_env.stdout).contains("\"seed\":99"),
        "SIM_SEED must apply when --seed is omitted"
    );
}

#[test]
fn completions_flag_prints_all_shells_and_exits_without_running_simulation() {
    use simraft_sim::{Shell, completions_script};

    let cases = [
        (Shell::Bash, "bash", "complete -F _simraft_sim simraft-sim"),
        (Shell::Zsh, "zsh", "#compdef simraft-sim"),
        (Shell::Fish, "fish", "complete -c simraft-sim"),
        (
            Shell::PowerShell,
            "powershell",
            "Register-ArgumentCompleter -Native -CommandName 'simraft-sim'",
        ),
    ];

    for (variant, name, marker) in cases {
        assert_eq!(variant.as_str(), name);
        assert_eq!(variant.to_string(), name);
        assert_eq!(name.parse::<Shell>().expect("shell parses"), variant);

        let first = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
            .args(["--completions", name])
            .output()
            .expect("simraft-sim --completions runs");
        assert_eq!(
            first.status.code(),
            Some(0),
            "--completions {name} must exit 0: {}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert!(
            first.stderr.is_empty(),
            "--completions {name} must write nothing to stderr"
        );

        let second = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
            .args(["--completions", name])
            .output()
            .expect("simraft-sim --completions replays");
        assert_eq!(
            first.stdout, second.stdout,
            "--completions {name} must be byte-for-byte deterministic"
        );

        let script = String::from_utf8(first.stdout).expect("UTF-8 completion script");
        assert_eq!(
            script,
            completions_script(variant),
            "CLI stdout must match simraft_sim::completions_script({variant:?})"
        );
        assert!(script.contains(marker), "{name} script missing {marker:?}");
        assert!(
            !script.contains("\"seed\":"),
            "--completions {name} must exit immediately without running simulation"
        );

        for flag in [
            "--seed",
            "--seeds",
            "--json",
            "--trace",
            "--chaos",
            "--nodes",
            "--ticks",
            "--completions",
            "bash",
            "zsh",
            "fish",
            "powershell",
        ] {
            assert!(
                script.contains(flag),
                "{name} completion script missing {flag:?}"
            );
        }
    }

    assert!("nushell".parse::<Shell>().is_err());
    let bad = Command::new(env!("CARGO_BIN_EXE_simraft-sim"))
        .args(["--completions", "nushell"])
        .output()
        .expect("run simraft-sim with invalid shell");
    assert_eq!(
        bad.status.code(),
        Some(2),
        "invalid --completions shell must exit 2"
    );
    assert!(
        bad.stdout.is_empty(),
        "invalid shell must write nothing to stdout"
    );
}
