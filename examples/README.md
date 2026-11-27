# Runnable Examples

Self-contained examples demonstrating deterministic chaos sweeps, byte-identical trace replay, `"SRCL"` v1 client framing, shell completions, and a live 3-node localhost TCP cluster. Every script here is executed in CI on pull requests and `main`.

## Prerequisites

Build `simraft-sim` and `simraft-node` from the workspace root (or install prebuilt binaries via `install.sh`):

```sh
cargo build --locked -p simraft-sim --bin simraft-sim -p simraft-node --bin simraft-node
```

## Available examples

| Example | Language / Runtime | Requires loopback TCP | What it demonstrates |
| --- | --- | --- | --- |
| [`chaos_sweep_and_replay.py`](chaos_sweep_and_replay.py) | Python 3 (`>= 3.10`, stdlib only) | No (100% offline) | Sweeps 10 consecutive `--chaos` seeds via `simraft_kv.simulate`, replays seed `912` twice via `simraft_kv.replay_trace` asserting byte-identical SHA-256 `digest` and event traces, and round-trips `"SRCL"` v1 request/response frames with `encode_client_request` and `decode_client_response`. |
| [`cluster_quickstart.sh`](cluster_quickstart.sh) | POSIX `sh` | Optional (`--live-tcp`) | Runs deterministic `simraft-sim --seed 912 --seeds 3 --chaos --json` and `simraft-node completions bash`. With `--live-tcp`, boots a 3-node `simraft-node serve` cluster in a `mktemp -d` workspace, executes linearizable `put`, `get`, and `delete` operations across distinct members, verifies exit code `1` after deletion, and tears down cleanly via `trap`. |
| [`../simraft-core/examples/embed.rs`](../simraft-core/examples/embed.rs) | Rust (`cargo run -p simraft-core --example embed --locked`) | No (in-memory) | Embeds three `simraft_core::Node` instances in one process with a custom `VecDeque` transport and miniature key-value map using only `Node::step` and `Node::stats`. |

## Quick run

```sh
# Offline Python chaos sweep, deterministic trace replay, and SRCL wire check
python3 examples/chaos_sweep_and_replay.py

# Deterministic CLI sweep + shell completions smoke check (no sockets needed)
sh examples/cluster_quickstart.sh --sim-only

# Full 3-node localhost TCP cluster walkthrough with automatic trap cleanup
sh examples/cluster_quickstart.sh --live-tcp

# Single-file Rust sans-I/O embedding example
cargo run -p simraft-core --example embed --locked
```
