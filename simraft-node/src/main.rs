//! The simraft-node binary: `serve` runs one node, `client`
//! issues one operation against a member.
//!
//! Stage A state: both modes parse and validate their arguments
//! through the simraft-node library contract and then report
//! that the runtime lands at Lane 1 Stage B, exiting with code
//! 2. Stage B replaces the report with the serving loop and the
//! client transport.

#![forbid(unsafe_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use simraft_core::NodeId;
use simraft_node::{ClientOp, MemberConfig, NodeConfig, run_client, run_node};

#[derive(Parser)]
#[command(name = "simraft-node", about = "Run or query a simraft-kv node")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one cluster node in the foreground.
    Serve(ServeArgs),
    /// Issue one client operation against a member.
    Client(ClientArgs),
}

#[derive(Args)]
struct ServeArgs {
    /// This node's id.
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
    /// The member to talk to, as a socket address.
    #[arg(long)]
    server: SocketAddr,
    #[command(subcommand)]
    op: ClientCommand,
}

#[derive(Subcommand)]
enum ClientCommand {
    /// Store a value under a key.
    Put {
        /// The key.
        key: String,
        /// The value.
        value: String,
    },
    /// Fetch the value under a key.
    Get {
        /// The key.
        key: String,
    },
    /// Remove a key.
    Delete {
        /// The key.
        key: String,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let result = match cli.command {
        Command::Serve(args) => serve(args),
        Command::Client(args) => client(args),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("simraft-node: {error}");
            ExitCode::from(2)
        }
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

fn client(args: ClientArgs) -> Result<(), simraft_node::NodeError> {
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
    match run_client(args.server, &op)? {
        Some(value) => println!("{}", String::from_utf8_lossy(&value)),
        None => println!("ok"),
    }
    Ok(())
}
