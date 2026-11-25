//! The `simraft-node` binary: `serve` runs one Raft cluster node in
//! the foreground, and `client` issues one linearizable key-value
//! operation against a cluster member.
//!
//! Exit codes:
//! - `0`: the node exited cleanly, a write (`put` or `delete`)
//!   committed and printed `ok`, or `get` found the key and printed
//!   its value.
//! - `1`: `client get <key>` confirmed linearizably that `<key>` is
//!   absent from the store (prints `simraft-node: key not found` to
//!   stderr and nothing to stdout).
//! - `2`: invalid arguments, storage failure, or transport/cluster
//!   unavailability.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use simraft_core::NodeId;
use simraft_node::{ClientOp, MemberConfig, NodeConfig, run_client, run_node};

#[derive(Parser)]
#[command(
    name = "simraft-node",
    version,
    about = "Run or query a simraft-kv Raft cluster node"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one cluster node in the foreground.
    Serve(ServeArgs),
    /// Issue one linearizable client operation against a member.
    Client(ClientArgs),
}

#[derive(Args)]
struct ServeArgs {
    /// This node's numeric id.
    #[arg(long)]
    id: u64,
    /// A cluster member as id@host:port, repeated for every
    /// member including this node.
    #[arg(long = "member", required = true)]
    members: Vec<String>,
    /// The directory holding this node's durable records.
    #[arg(long)]
    data_dir: PathBuf,
    /// Wall-clock milliseconds between core ticks.
    #[arg(long, default_value_t = 50)]
    tick_ms: u64,
}

#[derive(Args)]
struct ClientArgs {
    /// The member to talk to, as a socket address (follows leader redirects).
    #[arg(long)]
    server: SocketAddr,
    #[command(subcommand)]
    op: ClientCommand,
}

#[derive(Subcommand)]
enum ClientCommand {
    /// Store a value under a key (prints `ok` and exits 0 on commit).
    Put {
        /// The key.
        key: String,
        /// The value.
        value: String,
    },
    /// Fetch the value under a key (prints the value and exits 0 if present, or exits 1 if absent).
    Get {
        /// The key.
        key: String,
    },
    /// Remove a key (prints `ok` and exits 0 on commit).
    Delete {
        /// The key.
        key: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => match serve(args) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("simraft-node: {error}");
                ExitCode::from(2)
            }
        },
        Command::Client(args) => client(args),
    }
}

fn serve(args: ServeArgs) -> Result<(), simraft_node::NodeError> {
    let mut members = Vec::with_capacity(args.members.len());
    for spec in &args.members {
        members.push(MemberConfig::parse(spec)?);
    }
    let config = NodeConfig {
        id: NodeId(args.id),
        members,
        data_dir: args.data_dir,
        tick_ms: args.tick_ms,
    };
    run_node(&config)
}

fn client(args: ClientArgs) -> ExitCode {
    let is_get = matches!(args.op, ClientCommand::Get { .. });
    let op = match args.op {
        ClientCommand::Put { key, value } => ClientOp::Put {
            key: key.into_bytes(),
            value: value.into_bytes(),
        },
        ClientCommand::Get { key } => ClientOp::Get {
            key: key.into_bytes(),
        },
        ClientCommand::Delete { key } => ClientOp::Delete {
            key: key.into_bytes(),
        },
    };
    match run_client(args.server, &op) {
        Ok(Some(value)) => {
            println!("{}", String::from_utf8_lossy(&value));
            ExitCode::SUCCESS
        }
        Ok(None) if is_get => {
            eprintln!("simraft-node: key not found");
            ExitCode::from(1)
        }
        Ok(None) => {
            println!("ok");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("simraft-node: {error}");
            ExitCode::from(2)
        }
    }
}
