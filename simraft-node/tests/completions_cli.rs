//! CLI and library contracts for `simraft-node completions <SHELL>`.

use std::process::Command;

use simraft_node::{Shell, completions_script};

const BIN: &str = env!("CARGO_BIN_EXE_simraft-node");

#[test]
fn completions_cover_all_shells_and_subcommands_deterministically() {
    let cases = [
        (
            Shell::Bash,
            "bash",
            "complete -F _simraft_node simraft-node",
        ),
        (Shell::Zsh, "zsh", "#compdef simraft-node"),
        (Shell::Fish, "fish", "complete -c simraft-node"),
        (
            Shell::PowerShell,
            "powershell",
            "Register-ArgumentCompleter -Native -CommandName 'simraft-node'",
        ),
    ];

    for (variant, name, shell_marker) in cases {
        assert_eq!(variant.as_str(), name);
        assert_eq!(variant.to_string(), name);
        assert_eq!(name.parse::<Shell>().expect("shell parses"), variant);

        let first = Command::new(BIN)
            .args(["completions", name])
            .output()
            .expect("run simraft-node completions");
        assert_eq!(
            first.status.code(),
            Some(0),
            "completions {name} must exit 0: {}",
            String::from_utf8_lossy(&first.stderr)
        );
        assert!(
            first.stderr.is_empty(),
            "completions {name} must write nothing to stderr"
        );

        let second = Command::new(BIN)
            .args(["completions", name])
            .output()
            .expect("replay simraft-node completions");
        assert_eq!(
            first.stdout, second.stdout,
            "completions {name} must be byte-for-byte deterministic"
        );

        let script = String::from_utf8(first.stdout).expect("UTF-8 completion script");
        assert_eq!(
            script,
            completions_script(variant),
            "CLI stdout must match simraft_node::completions_script({variant:?})"
        );
        assert!(
            script.contains(shell_marker),
            "{name} script missing shell marker {shell_marker:?}"
        );

        for required in [
            "serve",
            "--id",
            "--member",
            "--data-dir",
            "--tick-ms",
            "client",
            "--server",
            "put",
            "get",
            "delete",
            "completions",
            "bash",
            "zsh",
            "fish",
            "powershell",
        ] {
            assert!(
                script.contains(required),
                "{name} completion script missing {required:?}"
            );
        }
    }
}

#[test]
fn completions_rejects_invalid_shell_with_exit_code_two() {
    assert!("tcsh".parse::<Shell>().is_err());

    let bad = Command::new(BIN)
        .args(["completions", "tcsh"])
        .output()
        .expect("run simraft-node completions with invalid shell");
    assert_eq!(
        bad.status.code(),
        Some(2),
        "invalid shell must exit with code 2"
    );
    assert!(
        bad.stdout.is_empty(),
        "invalid shell must write nothing to stdout"
    );
    assert!(
        !bad.stderr.is_empty(),
        "invalid shell must explain the error on stderr"
    );
}
