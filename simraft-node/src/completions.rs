//! Static, zero-dependency shell completion scripts for `simraft-node`.

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

const BASH_COMPLETIONS: &str = r#"_simraft_node() {
    local cur prev subcmd client_op word idx
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    subcmd=""
    client_op=""

    idx=1
    while [ "$idx" -lt "$COMP_CWORD" ]; do
        word="${COMP_WORDS[$idx]}"
        if [ -z "$subcmd" ]; then
            case "$word" in
                serve|client|completions|help)
                    subcmd="$word"
                    ;;
            esac
        elif [ "$subcmd" = "client" ] && [ -z "$client_op" ]; then
            case "$word" in
                --server)
                    idx=$((idx + 1))
                    ;;
                put|get|delete|help)
                    client_op="$word"
                    ;;
            esac
        fi
        idx=$((idx + 1))
    done

    case "$subcmd" in
        "")
            COMPREPLY=( $(compgen -W "serve client completions help -h --help -V --version" -- "$cur") )
            return 0
            ;;
        serve)
            case "$prev" in
                --data-dir)
                    COMPREPLY=( $(compgen -d -- "$cur") )
                    return 0
                    ;;
                --id|--member|--tick-ms)
                    COMPREPLY=()
                    return 0
                    ;;
            esac
            COMPREPLY=( $(compgen -W "--id --member --data-dir --tick-ms -h --help" -- "$cur") )
            return 0
            ;;
        client)
            if [ "$prev" = "--server" ]; then
                COMPREPLY=()
                return 0
            fi
            if [ -n "$client_op" ]; then
                COMPREPLY=( $(compgen -W "-h --help" -- "$cur") )
                return 0
            fi
            case "$cur" in
                -*)
                    COMPREPLY=( $(compgen -W "--server -h --help" -- "$cur") )
                    ;;
                *)
                    COMPREPLY=( $(compgen -W "put get delete help --server -h --help" -- "$cur") )
                    ;;
            esac
            return 0
            ;;
        completions)
            COMPREPLY=( $(compgen -W "bash zsh fish powershell -h --help" -- "$cur") )
            return 0
            ;;
        help)
            COMPREPLY=( $(compgen -W "serve client completions help" -- "$cur") )
            return 0
            ;;
    esac
}

complete -F _simraft_node simraft-node
"#;

const ZSH_COMPLETIONS: &str = r#"#compdef simraft-node

_simraft_node_client() {
    local -a ops
    ops=(
        'put:Store a value under a key'
        'get:Fetch the value under a key'
        'delete:Remove a key'
        'help:Print help for a client subcommand'
    )
    _arguments -C \
        '--server=[Cluster member socket address]:server:' \
        '(-h --help)'{-h,--help}'[Print help]' \
        '1:operation:->op' \
        '*::arg:->args'

    case "$state" in
        op)
            _describe -t operations 'simraft-node client operation' ops
            ;;
        args)
            _arguments '(-h --help)'{-h,--help}'[Print help]'
            ;;
    esac
}

_simraft_node() {
    local -a commands
    commands=(
        'serve:Run one cluster node in the foreground'
        'client:Issue one linearizable client operation against a member'
        'completions:Print static shell completion script'
        'help:Print help for subcommands'
    )

    _arguments -C \
        '(-h --help)'{-h,--help}'[Print help]' \
        '(-V --version)'{-V,--version}'[Print version]' \
        '1:command:->cmd' \
        '*::arg:->args'

    case "$state" in
        cmd)
            _describe -t commands 'simraft-node command' commands
            ;;
        args)
            case "${words[1]}" in
                serve)
                    _arguments \
                        '--id=[Numeric node identifier]:id:' \
                        '*--member=[Cluster member as id@host:port]:member:' \
                        '--data-dir=[Directory holding durable records]:data_dir:_files -/' \
                        '--tick-ms=[Milliseconds between core ticks]:tick_ms:' \
                        '(-h --help)'{-h,--help}'[Print help]'
                    ;;
                client)
                    _simraft_node_client
                    ;;
                completions)
                    _arguments \
                        '(-h --help)'{-h,--help}'[Print help]' \
                        '1:shell:(bash zsh fish powershell)'
                    ;;
                help)
                    _arguments '1:subcommand:(serve client completions help)'
                    ;;
            esac
            ;;
    esac
}

if [ "$funcstack[1]" = "_simraft_node" ]; then
    _simraft_node "$@"
else
    compdef _simraft_node simraft-node
fi
"#;

const FISH_COMPLETIONS: &str = r#"complete -c simraft-node -f
complete -c simraft-node -n "__fish_use_subcommand" -s h -l help -d "Print help"
complete -c simraft-node -n "__fish_use_subcommand" -s V -l version -d "Print version"
complete -c simraft-node -n "__fish_use_subcommand" -a "serve" -d "Run one cluster node in the foreground"
complete -c simraft-node -n "__fish_use_subcommand" -a "client" -d "Issue one linearizable client operation against a member"
complete -c simraft-node -n "__fish_use_subcommand" -a "completions" -d "Print static shell completion script"
complete -c simraft-node -n "__fish_use_subcommand" -a "help" -d "Print help for subcommands"

complete -c simraft-node -n "__fish_seen_subcommand_from serve" -l id -r -d "Numeric node identifier (--id)"
complete -c simraft-node -n "__fish_seen_subcommand_from serve" -l member -r -d "Cluster member as id@host:port (--member)"
complete -c simraft-node -n "__fish_seen_subcommand_from serve" -l data-dir -r -a "(__fish_complete_directories)" -d "Directory holding durable records (--data-dir)"
complete -c simraft-node -n "__fish_seen_subcommand_from serve" -l tick-ms -r -d "Milliseconds between core ticks (--tick-ms)"
complete -c simraft-node -n "__fish_seen_subcommand_from serve" -s h -l help -d "Print help"

complete -c simraft-node -n "__fish_seen_subcommand_from client" -l server -r -d "Cluster member socket address (--server)"
complete -c simraft-node -n "__fish_seen_subcommand_from client" -s h -l help -d "Print help"
complete -c simraft-node -n "__fish_seen_subcommand_from client; and not __fish_seen_subcommand_from put get delete help" -a "put" -d "Store a value under a key"
complete -c simraft-node -n "__fish_seen_subcommand_from client; and not __fish_seen_subcommand_from put get delete help" -a "get" -d "Fetch the value under a key"
complete -c simraft-node -n "__fish_seen_subcommand_from client; and not __fish_seen_subcommand_from put get delete help" -a "delete" -d "Remove a key"
complete -c simraft-node -n "__fish_seen_subcommand_from client; and not __fish_seen_subcommand_from put get delete help" -a "help" -d "Print help"

complete -c simraft-node -n "__fish_seen_subcommand_from completions" -a "bash zsh fish powershell" -d "Target shell"
complete -c simraft-node -n "__fish_seen_subcommand_from completions" -s h -l help -d "Print help"
"#;

const POWERSHELL_COMPLETIONS: &str = r#"using namespace System.Management.Automation
using namespace System.Management.Automation.Language

Register-ArgumentCompleter -Native -CommandName 'simraft-node' -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)

    $elements = $commandAst.CommandElements
    $tokens = @()
    for ($i = 1; $i -lt $elements.Count; $i++) {
        $text = $elements[$i].Extent.Text
        if ($text -ne $wordToComplete) {
            $tokens += $text
        }
    }

    $subcmd = $null
    foreach ($t in $tokens) {
        if ($t -in @('serve', 'client', 'completions', 'help')) {
            $subcmd = $t
            break
        }
    }

    $candidates = switch ($subcmd) {
        'serve' {
            @('--id', '--member', '--data-dir', '--tick-ms', '-h', '--help')
        }
        'client' {
            $hasOp = $false
            foreach ($t in $tokens) {
                if ($t -in @('put', 'get', 'delete', 'help')) {
                    $hasOp = $true
                    break
                }
            }
            if ($hasOp) {
                @('-h', '--help')
            } else {
                @('--server', 'put', 'get', 'delete', 'help', '-h', '--help')
            }
        }
        'completions' {
            @('bash', 'zsh', 'fish', 'powershell', '-h', '--help')
        }
        'help' {
            @('serve', 'client', 'completions', 'help')
        }
        default {
            @('serve', 'client', 'completions', 'help', '-h', '--help', '-V', '--version')
        }
    }

    $candidates | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
        [CompletionResult]::new($_, $_, 'ParameterValue', $_)
    }
}
"#;

/// Return the deterministic static completion script for `simraft-node` in `shell`.
#[must_use]
pub const fn completions_script(shell: Shell) -> &'static str {
    match shell {
        Shell::Bash => BASH_COMPLETIONS,
        Shell::Zsh => ZSH_COMPLETIONS,
        Shell::Fish => FISH_COMPLETIONS,
        Shell::PowerShell => POWERSHELL_COMPLETIONS,
    }
}
