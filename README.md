# simraft-kv

`simraft-kv` is a deterministic Raft consensus implementation and
replicated key-value store in Rust. A cluster of nodes agrees on one
ordered history of commands even when individual servers crash, disk
tails tear, or network links drop, reorder, or partition packets.

The consensus engine makes protocol decisions as a pure function of
input events and performs zero input or output on its own. That
split lets a deterministic simulator drive an entire cluster on a
virtual clock from a single 64-bit seed while a production TCP and
file adapter runs the exact same core and wire format across real
processes. Every promised safety, durability, liveness, determinism,
and latency property maps to an executable test, oracle, or
benchmark in `docs/INVARIANTS.md`.

## Workspace crates

| Crate | Kind | External runtime deps | Role |
| --- | --- | --- | --- |
| `simraft-core` | library | none (`std` only) | Pure sans-I/O Raft state machine (`Node::step`), Pre-Vote elections, fast log backup, Figure 8 commitment, `ReadIndex`, chunked snapshots, and single-server membership changes. |
| `simraft-wire` | library | `crc32fast` | Canonical `"SRKV"` message frame, log entry, and cluster config codec with 16 MiB frame bounds and IEEE CRC-32 checksums. |
| `simraft-kv` | library | none (`std` only) | Deterministic `BTreeMap` key-value state machine, bounded client session deduplication, and SHA-256 verified snapshot payloads. |
| `simraft-node` | library and binary | `clap`, `crc32fast` | File-backed durable store, TCP peer transport, `serve` daemon, and linearizable `client` CLI (`put`, `get`, `delete`). |
| `simraft-sim` | library and binary | `clap`, `crc32fast`, `sha2` | Single-threaded deterministic simulator, seeded `--chaos` fault injector, per-event safety oracles, and Wing and Gong linearizability checker. |

## Quickstart with a three-node localhost cluster

Start three `simraft-node` servers in separate terminals from the
workspace root.

```sh
cargo run -p simraft-node --release -- serve \
  --id 1 --data-dir /tmp/srkv-1 \
  --member 1@127.0.0.1:7001 \
  --member 2@127.0.0.1:7002 \
  --member 3@127.0.0.1:7003

cargo run -p simraft-node --release -- serve \
  --id 2 --data-dir /tmp/srkv-2 \
  --member 1@127.0.0.1:7001 \
  --member 2@127.0.0.1:7002 \
  --member 3@127.0.0.1:7003

cargo run -p simraft-node --release -- serve \
  --id 3 --data-dir /tmp/srkv-3 \
  --member 1@127.0.0.1:7001 \
  --member 2@127.0.0.1:7002 \
  --member 3@127.0.0.1:7003
```

Write, read, and delete keys against any member. The client follows
leader hints automatically.

```sh
cargo run -p simraft-node --release -- client \
  --server 127.0.0.1:7001 \
  put greeting hello

cargo run -p simraft-node --release -- client \
  --server 127.0.0.1:7002 \
  get greeting

cargo run -p simraft-node --release -- client \
  --server 127.0.0.1:7003 \
  delete greeting
```

`put` and `delete` print `ok` to standard output and exit with code
`0`. `get` writes the raw value bytes to standard output and exits
with code `0` when the key exists, or writes
`simraft-node: key not found` to standard error and exits with code
`1` when the key is absent. Transport, timeout, or cluster errors
exit with code `2`.

## Embedding the sans-I/O core

Callers that embed `simraft-core` directly own the network, disk,
and clock while `Node::step` owns every consensus transition.

```rust
let actions = node.step(event);
for action in actions {
    // Send messages, persist durable state, apply committed entries,
    // or complete client proposals and reads in action order.
}
```

Run the self-contained single-file three-node embedding example with
an in-memory transport.

```sh
cargo run -p simraft-core --example embed
```

## Deterministic simulation and verification

Two binaries ship in the workspace (`simraft-node` for live clusters
and `simraft-sim` for deterministic fault simulation). Run a seeded
chaos sweep across asymmetric partitions, crashes, clock jumps, and
wire bit-flip corruption, then replay any single seed byte for byte.

```sh
cargo run -p simraft-sim --release -- --seed 0 --seeds 25 --chaos
cargo run -p simraft-sim --release -- --seed 912 --seeds 1 --chaos --trace
```

Run the full workspace verification gate on Rust `1.88` or newer
(CI pins Rust `1.100.0` in `rust-toolchain.toml` and checks MSRV
`1.88.0`).

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
PROPTEST_CASES=500 cargo test --workspace --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo bench --workspace --locked -- --test
```

Property test suites scale through `PROPTEST_CASES` (1,000 cases per
pull request and 10,000 on `main`), and chaos simulator sweeps run
at matching seed counts in CI plus 100,000 seeds every week.

## Benchmarks

Measured with the Criterion benchmarks in this repository from
recorded hosted runs on 2026-11-19. A full propose and commit round
on a three-node cluster driven directly, one 32-byte entry per
round, takes a median of 1.50 microseconds, about 668,000 rounds
per second. A batch of 32 proposals released by one persist
acknowledgment takes a median of 85.5 microseconds, about 374,000
entries per second, down from 122 microseconds before leader send
coalescing (`B-02`). A follower processing one `AppendEntries`
message takes a median of 76 nanoseconds, and `step_allocations`
verifies `Node::step` stays within 0 allocations on `Tick`, 2 on
`RequestVote`, and 6 on a single-entry `AppendEntries` (`APPEND_BUDGET = 7`).
A follower with an empty log catches up to a 10,000-entry leader log
in a median of 4.06 milliseconds. The linearizability checker
decides a generated history of 100,000 operations over 64 keys after
visiting 100,000 states. Reproduce locally with
`cargo bench --workspace`.

## Documentation

- `docs/ARCHITECTURE.md` specifies the protocol rules, byte schemas,
  domain-separated digests, simulator model, and rejected
  alternatives.
- `docs/INVARIANTS.md` maps every formal invariant to its enforcing
  property test, simulator oracle, scenario test, or benchmark.
- `DECISIONS.md` records the workspace architectural decisions.
- `docs/ROADMAP.md` records the completed milestone and post-M4
  delivery lanes plus parked future extensions.
- `SECURITY.md` documents the threat model, defensive bounds, and
  vulnerability reporting policy.

## License

Licensed under either of MIT or Apache-2.0 at your option.
