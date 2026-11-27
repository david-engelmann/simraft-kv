# CLI, TCP Cluster, and Python SDK

Reference for the `simraft-sim` simulator CLI, the `simraft-node` server and client CLI, and the `simraft_kv` Python SDK.

## `simraft-sim` CLI

`simraft-sim` runs one or more single-threaded deterministic cluster simulations and checks all five Raft safety invariants (`S-01..S-05`), post-heal leader and state digest convergence (`H-02`), and per-key Wing and Gong linearizability (`R-05`).

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--seed <U64>` | `0` (or `SIM_SEED`) | Starting 64-bit deterministic seed. |
| `--seeds <COUNT>` | `1` | Number of consecutive seeds (`seed..seed + seeds`) to execute. |
| `--chaos` | off | Enable `FaultConfig::chaos` (asymmetric cuts, drops, duplicates, wire bit-flips, crashes, clock jumps). |
| `--nodes <COUNT>` | `3` | Number of voters in the simulated cluster. |
| `--ticks <TICKS>` | `8000` | Virtual-clock tick budget before bounded healing verification. |
| `--json` | off | Emit one JSON `SeedReport` object per line on stdout. |
| `--trace` | off | Emit the deterministic per-event execution trace after the summary line (requires `--seeds 1`). |
| `--completions <SHELL>` | none | Print completion script for `bash`, `zsh`, `fish`, or `powershell` and exit `0`. |

### JSON `SeedReport` schema

Each line printed by `simraft-sim --json` contains:

| Field | Type | Meaning |
| --- | --- | --- |
| `seed` | `int` | 64-bit seed used for the run. |
| `completed` | `bool` | Simulation reached the end of its tick and healing schedule without invariant panics. |
| `converged` | `bool` | All active members converged on identical log and KV state digests after fault healing (`H-02`). |
| `leader_converged` | `bool` | A single stable leader emerged within `3 * max_election_timeout` ticks of fault healing (`H-02`). |
| `ticks` | `int` | Total virtual ticks elapsed including bounded healing. |
| `verdict` | `string` | Wing and Gong linearizability verdict (`"linearizable"` required for pass; `"unknown"` or `"nonlinearizable"` fails). |
| `digest` | `string` | 64-character lowercase hex SHA-256 state digest under domain `"simraft-kv/state-digest/v1"`. |
| `passed` | `bool` | `true` only when `completed`, `converged`, `leader_converged`, and `verdict == "linearizable"` all hold. |

### Exit codes

- `0`: every simulated seed passed all safety, convergence, and linearizability checks (or `--completions` succeeded).
- `1`: at least one seed failed a safety oracle, post-heal convergence check, or linearizability check.
- `2`: invalid CLI arguments.

## `simraft-node` CLI

`simraft-node` runs a durable file-backed Raft server over TCP (`serve`), issues linearizable client operations (`client`), or prints shell completions (`completions`).

### `simraft-node serve`

```sh
simraft-node serve \
  --id <NODE_ID> \
  --data-dir <PATH> \
  --tick-ms <MS> \
  --member <ID@HOST:PORT> [--member <ID@HOST:PORT> ...]
```

- `--id`: numeric `NodeId` of this process (must appear in `--member`).
- `--member`: repeatable `id@host:port` specification for every voter in the cluster. Peer `"SRKV"` frames and client `"SRCL"` frames multiplex on the same TCP listener by inspecting the leading 4-byte magic.
- `--data-dir`: directory storing `"SRHS"` hard state, `"SRLG"` CRC-32 log records, and atomic `"SRSS"` snapshot files.
- `--tick-ms`: wall-clock milliseconds between `Event::Tick` steps (`1..=10000`, default `50`).

### `simraft-node client`

```sh
simraft-node client --server <HOST:PORT> put <KEY> <VALUE>
simraft-node client --server <HOST:PORT> get <KEY>
simraft-node client --server <HOST:PORT> delete <KEY>
```

Each invocation allocates a collision-free 64-bit `ClientId`, follows follower redirect hints to the current leader, and retries transient failures for up to 20 seconds (`CLIENT_BUDGET`) under the same `(client_id, seq)` pair so committed retries never execute twice (`A-01`, `C-01`, `C-02`).

### Exit codes

| Exit code | Meaning |
| --- | --- |
| `0` | `serve` exited cleanly, `put` or `delete` committed and printed `ok`, `get` found `<KEY>` and printed its value to stdout, or `completions` printed a script. |
| `1` | `client get <KEY>` completed a linearizable `ReadIndex` read and confirmed `<KEY>` is absent (prints `simraft-node: key not found` to stderr and nothing to stdout). |
| `2` | Invalid arguments, local durable store failure, or transport/cluster unavailability. |

## `simraft_kv` Python SDK

Install via `pip install simraft-kv` (requires Python `>= 3.10`, zero third-party Python dependencies).

### Simulation and trace replay

- `simraft_kv.simulate(*, seed=0, seeds=1, chaos=False, nodes=3, ticks=8000, check=True, binary=None) -> SweepSummary`
  - Runs `simraft-sim --json` and returns `SweepSummary(runs: tuple[SeedReport, ...], exit_code: int)`.
  - Raises `SimRaftError` automatically if any seed fails when `check=True`.
- `simraft_kv.replay_trace(*, seed, chaos=False, nodes=3, ticks=8000, binary=None) -> tuple[SeedReport, str]`
  - Runs a single seed with `--json --trace` and returns both the parsed `SeedReport` and the full newline-terminated trace string.
- `simraft_kv.completions(shell="bash", *, tool="simraft-sim", binary=None) -> str`
  - Returns the static completion script for `simraft-sim` or `simraft-node`.
- `simraft_kv.version(*, binary=None) -> str`
  - Queries `simraft-sim --version`.

### Linearizable TCP client (`SimRaftClient`)

`SimRaftClient` speaks the binary `"SRCL"` v1 protocol directly over Python `socket` streams without spawning subprocesses:

```python
import simraft_kv

client = simraft_kv.SimRaftClient(
    ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"],
    timeout=10.0,
    connect_timeout=1.0,
)
client.put("session:1", b"\x01\x02\x03")
raw = client.get("session:1")          # b"\x01\x02\x03" or None
client.put("label", "quorum-ready")
text = client.get_text("label")        # "quorum-ready" or None
client.delete("session:1")
```

- Preserves one `client_id` per `SimRaftClient` instance, increments `seq` strictly on writes (`put` and `delete`), reuses `(client_id, seq)` across retries of the same operation, and promotes discovered leaders to `client.servers[0]`.
- Enforces the 16 MiB `MAX_PAYLOAD_BYTES` cap locally before advancing `seq` so rejected oversized writes never create sequence gaps (`C-04`).

### Low-level `"SRCL"` v1 wire helpers

For custom async transports, protocol testing, or offline frame inspection:

- `encode_client_request(client_id: int, seq: int, op: str, key: bytes | str, value: bytes | str | None = None) -> bytes`
  - Returns a length-prefixed `"SRCL"` v1 request frame (`4-byte u32 body_len` + `"SRCL"` body).
- `decode_client_response(body: bytes) -> ClientReply`
  - Decodes an unprefixed `"SRCL"` v1 response body into `ClientReply(status, value, leader_hint, error)` where `status` is `"ok"`, `"redirect"`, or `"error"`.
