# Quickstart

Get `simraft-sim` and `simraft-node` running in under two minutes using prebuilt verified binaries, `cargo install`, or `pip install`.

## 1. Install

### Prebuilt Linux and macOS binaries (SHA-256 verified)

[`install.sh`](https://raw.githubusercontent.com/david-engelmann/simraft-kv/main/install.sh) downloads the release `SHA256SUMS` manifest from GitHub Releases and verifies every archive digest with `sha256sum` or `shasum -a 256` before extracting `simraft-node` and `simraft-sim` into `~/.local/bin` (override with `--install-dir`, `--version`, or `--bin`):

```sh
curl -fsSL https://raw.githubusercontent.com/david-engelmann/simraft-kv/main/install.sh | sh
```

### Build from crates.io

Requires Rust `1.88` or newer:

```sh
cargo install simraft-node simraft-sim --locked
```

### Python SDK and bundled simulator CLI from PyPI

```sh
pip install simraft-kv
```

### Shell completions

Both binaries emit deterministic static completion scripts for `bash`, `zsh`, `fish`, and `powershell`:

```sh
simraft-node completions zsh > ~/.zfunc/_simraft-node
simraft-sim --completions zsh > ~/.zfunc/_simraft-sim
```

## 2. Run a deterministic chaos sweep and replay one seed

Sweep 25 consecutive seeds under `--chaos` (asymmetric partitions, crashes wiping un-fsynced disk buffers, virtual clock jumps, and 15 per mille wire bit-flip corruption):

```sh
simraft-sim --seed 0 --seeds 25 --chaos
```

Replay seed `912` deterministically and inspect its machine-readable JSON verdict:

```sh
simraft-sim --seed 912 --seeds 1 --chaos --json
```

```json
{"seed":912,"completed":true,"converged":true,"leader_converged":true,"ticks":9292,"verdict":"linearizable","digest":"c8a282578b8f0c97569595cff000063cfa6dfd8f23fc41c6be10f4994bde7095","passed":true}
```

Re-running `--seed 912 --seeds 1 --chaos --json` on any supported OS produces the exact same `ticks`, `verdict`, and domain-separated SHA-256 state `digest` (`D-01`). Add `--trace` to stream every cluster event after the summary line.

## 3. Run a three-node localhost TCP cluster

Start three `simraft-node` processes in separate terminals:

```sh
simraft-node serve --id 1 --data-dir /tmp/srkv-1 \
  --member 1@127.0.0.1:7001 --member 2@127.0.0.1:7002 --member 3@127.0.0.1:7003

simraft-node serve --id 2 --data-dir /tmp/srkv-2 \
  --member 1@127.0.0.1:7001 --member 2@127.0.0.1:7002 --member 3@127.0.0.1:7003

simraft-node serve --id 3 --data-dir /tmp/srkv-3 \
  --member 1@127.0.0.1:7001 --member 2@127.0.0.1:7002 --member 3@127.0.0.1:7003
```

Issue linearizable `put`, `get`, and `delete` commands against any member. Follower nodes redirect the client to the active leader automatically, and reads execute via clock-free `ReadIndex`:

```sh
simraft-node client --server 127.0.0.1:7001 put greeting hello
simraft-node client --server 127.0.0.1:7002 get greeting
simraft-node client --server 127.0.0.1:7003 delete greeting
```

To run this entire workflow automatically in a temporary directory with clean process teardown, execute [`examples/cluster_quickstart.sh`](https://github.com/david-engelmann/simraft-kv/blob/main/examples/cluster_quickstart.sh) from a repository checkout:

```sh
sh examples/cluster_quickstart.sh --live-tcp
```

## 4. Drive sweeps and live clusters from Python

```python
import simraft_kv

# Run a 10-seed chaos sweep and assert all safety and linearizability checks pass
sweep = simraft_kv.simulate(seed=912, seeds=10, chaos=True)
assert sweep.all_passed

# Replay a single seed with full trace capture
report, trace = simraft_kv.replay_trace(seed=912, chaos=True)
assert report.verdict == "linearizable"

# Connect to a live simraft-node TCP cluster
client = simraft_kv.SimRaftClient(
    ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]
)
client.put("greeting", "hello")
assert client.get_text("greeting") == "hello"
client.delete("greeting")
```

See [`examples/chaos_sweep_and_replay.py`](https://github.com/david-engelmann/simraft-kv/blob/main/examples/chaos_sweep_and_replay.py) for an offline runnable walkthrough covering sweeps, byte-identical trace replay, and `"SRCL"` v1 wire framing.

## 5. Embed `simraft-core` in Rust

Add `simraft-core` to your `Cargo.toml`:

```toml
[dependencies]
simraft-core = "0.2.3"
```

Your runtime owns sockets, files, and timers while `Node::step` computes pure consensus transitions:

```rust
let actions = node.step(event);
for action in actions {
    // Dispatch outbound RPCs, fsync hard state and log entries, apply
    // committed entries, and complete client proposals or ReadIndex reads.
}
```

Run the self-contained single-file 3-node embedding example in [`simraft-core/examples/embed.rs`](https://github.com/david-engelmann/simraft-kv/blob/main/simraft-core/examples/embed.rs):

```sh
cargo run -p simraft-core --example embed --locked
```
