//! Static, zero-dependency shell completion scripts for `simraft-sim`.

use std::fmt;
use std::str::FromStr;

use clap::ValueEnum;

/// Supported interactive shells for static completion script generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, ValueEnum)]
#[value(rename_all = "lowercase")]
pub enum Shell {
    /// GNU Bourne-Again Shell (`bash`).
    Bash,
    /// Z shell (`zsh`).
    Zsh,
    /// Friendly Interactive Shell (`fish`).
    Fish,
    /// PowerShell (`powershell`).
    PowerShell,
}

impl Shell {
    /// Return the canonical lowercase CLI name of this shell.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Bash => "bash",
            Self::Zsh => "zsh",
            Self::Fish => "fish",
            Self::PowerShell => "powershell",
        }
    }
}

impl fmt::Display for Shell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Shell {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "bash" => Ok(Self::Bash),
            "zsh" => Ok(Self::Zsh),
            "fish" => Ok(Self::Fish),
            "powershell" => Ok(Self::PowerShell),
            _ => Err(format!(
                "unsupported shell {s:?}; expected bash, zsh, fish, or powershell"
            )),
        }
    }
}

const BASH_COMPLETIONS: &str = r#"_simraft_sim() {
    local cur prev
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    case "$prev" in
        --completions)
            COMPREPLY=( $(compgen -W "bash zsh fish powershell" -- "$cur") )
            return 0
            ;;
        --seed|--seeds|--nodes|--ticks)
            COMPREPLY=()
            return 0
            ;;
    esac

    COMPREPLY=( $(compgen -W "--seed --seeds --json --trace --chaos --nodes --ticks --completions -h --help -V --version" -- "$cur") )
    return 0
}

complete -F _simraft_sim simraft-sim
"#;

const ZSH_COMPLETIONS: &str = r#"#compdef simraft-sim

_simraft_sim() {
    _arguments \
        '--seed=[First seed to run]:seed:' \
        '--seeds=[Number of consecutive seeds to run]:seeds:' \
        '--json[Emit one JSON object per run]' \
        '--trace[Print full trace after each summary line]' \
        '--chaos[Enable seeded multi-fault chaos profile]' \
        '--nodes=[Cluster size for every run]:nodes:' \
        '--ticks=[Total tick budget per run]:ticks:' \
        '--completions=[Print static shell completion script]:shell:(bash zsh fish powershell)' \
        '(-h --help)'{-h,--help}'[Print help]' \
        '(-V --version)'{-V,--version}'[Print version]'
}

if [ "$funcstack[1]" = "_simraft_sim" ]; then
    _simraft_sim "$@"
else
    compdef _simraft_sim simraft-sim
fi
"#;

const FISH_COMPLETIONS: &str = r#"complete -c simraft-sim -f
complete -c simraft-sim -l seed -r -d "First seed to run (--seed)"
complete -c simraft-sim -l seeds -r -d "Number of consecutive seeds to run (--seeds)"
complete -c simraft-sim -l json -d "Emit one JSON object per run (--json)"
complete -c simraft-sim -l trace -d "Print full trace after each summary line (--trace)"
complete -c simraft-sim -l chaos -d "Enable seeded multi-fault chaos profile (--chaos)"
complete -c simraft-sim -l nodes -r -d "Cluster size for every run (--nodes)"
complete -c simraft-sim -l ticks -r -d "Total tick budget per run (--ticks)"
complete -c simraft-sim -l completions -r -a "bash zsh fish powershell" -d "Print static shell completion script (--completions)"
complete -c simraft-sim -s h -l help -d "Print help"
complete -c simraft-sim -s V -l version -d "Print version"
"#;

const POWERSHELL_COMPLETIONS: &str = r#"using namespace System.Management.Automation
using namespace System.Management.Automation.Language

Register-ArgumentCompleter -Native -CommandName 'simraft-sim' -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)

    $elements = $commandAst.CommandElements
    $prev = $null
    if ($elements.Count -ge 2) {
        $lastElem = $elements[$elements.Count - 1].Extent.Text
        if ($lastElem -eq $wordToComplete -and $elements.Count -ge 3) {
            $prev = $elements[$elements.Count - 2].Extent.Text
        } elseif ($lastElem -ne $wordToComplete) {
            $prev = $lastElem
        }
    }

    if ($prev -eq '--completions') {
        $candidates = @('bash', 'zsh', 'fish', 'powershell')
    } elseif ($prev -in @('--seed', '--seeds', '--nodes', '--ticks')) {
        $candidates = @()
    } else {
        $candidates = @(
            '--seed',
            '--seeds',
            '--json',
            '--trace',
            '--chaos',
            '--nodes',
            '--ticks',
            '--completions',
            '-h',
            '--help',
            '-V',
            '--version'
        )
    }

    $candidates | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
        [CompletionResult]::new($_, $_, 'ParameterValue', $_)
    }
}
"#;

/// Return the deterministic static completion script for `simraft-sim` in `shell`.
#[must_use]
pub const fn completions_script(shell: Shell) -> &'static str {
    match shell {
        Shell::Bash => BASH_COMPLETIONS,
        Shell::Zsh => ZSH_COMPLETIONS,
        Shell::Fish => FISH_COMPLETIONS,
        Shell::PowerShell => POWERSHELL_COMPLETIONS,
    }
}
