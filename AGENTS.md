# Working with simraft-kv as an AI Agent

Guide for AI coding and operations agents embedding `simraft-core`, driving the `simraft-sim` simulator or `simraft-node` TCP cluster, calling the `simraft_kv` Python SDK, or editing this repository.

## 1. Driving CLIs and the Python SDK

### Deterministic simulation (`simraft-sim`)

Always pass `--json` when invoking `simraft-sim` programmatically so each seed emits a single machine-readable JSON line:

```sh
simraft-sim --seed 912 --seeds 5 --chaos --json
```

- Exit `0`: every seed in `--seeds` satisfied all safety oracles (`S-01..S-05`), post-heal leader and state digest convergence (`H-02`), and Wing and Gong linearizability (`verdict == "linearizable"`, `passed == true`).
- Exit `1`: at least one seed failed an invariant or convergence check. Replay that single seed with `simraft-sim --seed <SEED> --seeds 1 --chaos --json --trace` to inspect the full event trace.
- Exit `2`: invalid CLI flags.

### Live TCP cluster (`simraft-node`)

`simraft-node client` follows follower redirects automatically and retries transient failures for up to 20 seconds under one collision-free `(client_id, seq)` identity:

```sh
simraft-node client --server 127.0.0.1:7001 put config/epoch 7
simraft-node client --server 127.0.0.1:7002 get config/epoch
simraft-node client --server 127.0.0.1:7003 delete config/epoch
```

Check `simraft-node` exit codes explicitly rather than scraping stderr:

| Exit code | Meaning |
| --- | --- |
| `0` | `put` or `delete` committed (prints `ok`), `get` found the key (writes raw value bytes to stdout), `completions` succeeded, or `serve` exited cleanly. |
| `1` | `client get <key>` completed a linearizable `ReadIndex` read and confirmed `<key>` is absent (`simraft-node: key not found` on stderr, empty stdout). |
| `2` | Invalid arguments, local durable storage error, or cluster/transport unavailability. |

### Python SDK (`simraft_kv`)

Prefer the pure-stdlib `simraft_kv` package (`python/simraft_kv/__init__.py`) over ad-hoc subprocess parsing or raw socket framing:

```python
import simraft_kv

sweep = simraft_kv.simulate(seed=912, seeds=5, chaos=True)
assert sweep.all_passed

report, trace = simraft_kv.replay_trace(seed=912, chaos=True)

client = simraft_kv.SimRaftClient(
    ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"],
    timeout=10.0,
)
client.put("task/owner", "worker-1")
assert client.get_text("task/owner") == "worker-1"
client.delete("task/owner")
```

Reuse a single `SimRaftClient` instance across sequential operations: it caches the promoted leader at `client.servers[0]` and advances `seq` monotonically on writes while preserving `(client_id, seq)` across retries (`C-01`, `C-02`).

## 2. Embedding the sans-I/O core (`simraft-core`)

`simraft-core` depends only on `std` and performs zero I/O. Drivers feed `Event` values into `Node::step(&mut self, event: Event) -> Vec<Action>` and execute returned `Action` values in order:

1. **Durability watermarks are mandatory (`T-02`, `R-06`)**: `Node` withholds vote grants, append acknowledgments, held leader replication sends, single-voter commits, and `ReadIndex` releases until your driver persists `Action::PersistHardState` / `Action::PersistEntries` and steps back `Event::Persisted { kind, up_to_index }`.
2. **Reads are clock-free (`R-01..R-04`)**: issue `Event::ReadIndex { request_id, key }` and serve the read from your state machine only when `Action::ReadReady { request_id, key, read_index }` arrives and your applied index has reached `read_index`.
3. **Reference implementation**: read [`simraft-core/examples/embed.rs`](simraft-core/examples/embed.rs) (`cargo run -p simraft-core --example embed --locked`) before writing a custom driver.

## 3. Repository invariants for code and docs edits

Every pull request is gated by `.github/workflows/ci.yml`. Follow these rules strictly when modifying the codebase:

- **Determinism guard (`D-02`)**: `simraft-core`, `simraft-kv`, `simraft-wire`, and `simraft-sim` must never reference `HashMap`, `HashSet`, `SystemTime`, `Instant::now`, `thread_rng`, or `OsRng`. Use `BTreeMap`, `BTreeSet`, explicit `Tick` values, and `simraft-sim`'s internal SplitMix64/xoshiro256starstar PRNG.
- **No `unsafe` and strict lints**: all workspace crates carry `#![forbid(unsafe_code)]` and deny `clippy::all`, `clippy::pedantic`, `clippy::nursery`, `clippy::todo`, `clippy::unimplemented`, and `rustdoc` warnings.
- **Public copy rules**: CI rejects any `.md` file containing em dashes (`U+2014`) or en dashes (`U+2013`). Root `README.md` must use canonical absolute URLs (`https://...`) with zero relative links so PyPI renders README links and media cleanly.
- **Local verification**: run `make ci` (plus `make book` and `make fuzz-check` when touching `book/`, `docs/`, or codec surfaces) before finishing.

## 4. Navigating documentation efficiently

| Document | When to read |
| --- | --- |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Editing consensus roles, `"SRKV"` / `"SRSN"` / `"SRCL"` byte layouts, or domain-separated digests. |
| [`docs/INVARIANTS.md`](docs/INVARIANTS.md) | Adding or modifying protocol behavior; every behavioral guarantee maps to an invariant ID and enforcing test. |
| [`examples/`](examples/) | Copy-pasteable Python chaos sweep/replay and POSIX `sh` cluster scripts. |
| [`docs/COMPARISON.md`](docs/COMPARISON.md) · [`docs/FAQ.md`](docs/FAQ.md) | Architectural trade-offs, scope boundaries, and operational semantics. |

Skip `docs/ROADMAP.md` unless you are specifically auditing historical milestone delivery logs; it records past development phases rather than active API contracts.
