//! The simraft-sim command line (docs/ARCHITECTURE.md section
//! 3.3): run one seed or a batch of consecutive seeds, print each
//! run's seed on its first output line (invariant D-03), and exit
//! nonzero when any run fails, so the CI seed sweep fails with it.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use clap::Parser;
use simraft_sim::{FaultConfig, Harness, SimConfig};

/// Deterministic simulation runs for simraft-kv.
#[derive(Debug, Parser)]
#[command(name = "simraft-sim", version, about)]
struct Args {
    /// The first seed to run (defaults to `SIM_SEED` when set, else `0`).
    #[arg(long)]
    seed: Option<u64>,
    /// How many consecutive seeds to run.
    #[arg(long, default_value_t = 1)]
    seeds: u64,
    /// Emit one JSON object per run instead of text lines.
    #[arg(long)]
    json: bool,
    /// Print each run's full trace after its summary line.
    #[arg(long)]
    trace: bool,
    /// Enable the seeded multi-fault chaos profile (intermittent
    /// asymmetric partitions, crashes, clock jumps, and frame
    /// bit-flip corruption).
    #[arg(long)]
    chaos: bool,
    /// The cluster size for every run.
    #[arg(long, default_value_t = 3)]
    nodes: usize,
    /// The total tick budget per run, split evenly between the
    /// chaos and healing phases.
    #[arg(long, default_value_t = 8_000)]
    ticks: u64,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let base_seed = args.seed.unwrap_or_else(|| {
        std::env::var("SIM_SEED")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    });
    let mut all_passed = true;
    for offset in 0..args.seeds {
        let seed = base_seed.wrapping_add(offset);
        let chaos_ticks = args.ticks / 2;
        let heal_ticks = args.ticks - chaos_ticks;
        let faults = if args.chaos {
            FaultConfig::chaos(seed, args.nodes, chaos_ticks)
        } else {
            FaultConfig::default()
        };
        let config = SimConfig {
            seed,
            node_count: args.nodes,
            chaos_ticks,
            heal_ticks,
            faults,
            ..SimConfig::default()
        };
        let mut harness = Harness::new(config);
        let report = harness.run();
        if args.json {
            let verdict = match &report.verdict {
                simraft_sim::Verdict::Linearizable => "linearizable",
                simraft_sim::Verdict::NotLinearizable { .. } => "not_linearizable",
                simraft_sim::Verdict::Unknown => "unknown",
            };
            println!(
                "{{\"seed\":{},\"completed\":{},\"converged\":{},\"leader_converged\":{},\"ticks\":{},\"verdict\":\"{}\",\"digest\":\"{}\",\"passed\":{}}}",
                report.seed,
                report.completed,
                report.converged,
                report.leader_converged,
                report.ticks_run,
                verdict,
                report.state_digest,
                report.passed()
            );
        } else {
            println!(
                "seed={} completed={} converged={} leader_converged={} ticks={} verdict={:?} digest={} passed={}",
                report.seed,
                report.completed,
                report.converged,
                report.leader_converged,
                report.ticks_run,
                report.verdict,
                report.state_digest,
                report.passed()
            );
        }
        if args.trace {
            print!("{}", report.trace.render());
        }
        all_passed = all_passed && report.passed();
    }
    if all_passed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
