# Frequently Asked Questions

Common questions about the `simraft-kv` consensus engine, deterministic simulator, wire formats, and TCP runtime.

## Why is `simraft-core` sans-I/O instead of async?

Async runtimes interleave task wakeups, OS socket readiness, and wall-clock timers across worker threads. When a consensus bug surfaces once every ten thousand runs under asymmetric partitions and crash restarts, an async trace cannot be replayed deterministically from a single integer seed.

`simraft-core` expresses Raft as a pure state transition function, `Node::step(Event) -> Vec<Action>`, with zero external dependencies beyond `std`. Time, messages, client requests, and fsync completions enter as explicit `Event` values, and outbound messages, persistence requests, state-machine applies, and client replies exit as `Action` values. You can drive the exact same `Node` inside a single-threaded virtual-clock simulator (`simraft-sim`), an OS-thread TCP server (`simraft-node`), or an async Tokio/mio event loop in your own application.

## Does `simraft-sim` execute the same wire codec as `simraft-node`?

Yes. Both `simraft-sim` and `simraft-node` encode and decode every peer RPC through `simraft-wire` (`"SRKV"` v1 frames with 16 MiB bounds and domain-separated IEEE CRC-32 checksums under `"SRKV/frame/v1"`). When `--chaos` injects 15 per mille wire bit-flips in the simulator, it flips raw bytes inside real `"SRKV"` frames and verifies that corrupted packets are rejected before reaching `Node::step` (`H-04`).

## How do I reproduce and inspect a specific chaos seed?

Every simulator run is completely determined by its 64-bit seed, node count, tick budget, and fault profile (`D-01`, `D-03`). To replay seed `912` with per-event execution tracing:

```sh
simraft-sim --seed 912 --seeds 1 --chaos --json --trace > seed-912.trace
```

The first line of output is the JSON `SeedReport` containing the final state digest (`"simraft-kv/state-digest/v1"`) and linearizability verdict, followed by the ordered trace of cluster events. From Python, call `simraft_kv.replay_trace(seed=912, chaos=True)`.

## Why does `ReadIndex` avoid leader leases?

Clock-bound leader leases assume bounded clock drift across machines. Virtual machine pauses, GC stalls, NTP steps, or extreme clock skew can cause an old leader's lease to outlive a new leader's election and serve stale reads.

`simraft-kv` implements clock-free `ReadIndex` (`R-01` through `R-04`): the leader waits until at least one entry from its current term is committed, records `read_index = commit_index`, confirms leadership with a quorum round of heartbeat acknowledgments tagged with a monotonic `read_round` (or local durability on a single-voter cluster, `R-06`), and releases the read once `last_applied >= read_index`. Arbitrary virtual clock jumps in `scenario_clock_jump_read` never violate linearizability.

## What happens when a node crashes before fsync finishes or leaves a torn log tail?

`simraft-core` enforces persist-before-response (`T-02`): vote grants, append acknowledgments, single-voter commits, outbound leader replication batches, and `ReadIndex` releases are held until the driver delivers `Event::Persisted` for the required `HardState` and log watermarks. In `simraft-sim`, `SimDisk` separates volatile writes from fsynced durable state and wipes volatile buffers on crash (`H-03`).

On disk, `simraft-node` stores log entries as `"SRLG"` records protected by CRC-32 (`"SRKV/log/v1"`). If a power loss or crash leaves a partially written record at the end of the log file, recovery truncates cleanly at the first corrupt tail record, reports the diagnostic offset, and catches up from the cluster leader (`H-05`, `A-07`).

## How does client session deduplication prevent duplicate mutations on retry?

Clients retry writes when connections drop or leaders step down before a commit acknowledgment arrives. `simraft-kv` deduplicates retries at two layers:

1. **In-flight log deduplication (`C-02`)**: `simraft-core` checks uncommitted log entries for matching `(ClientId, seq)` pairs so duplicate retries attach to the pending proposal instead of appending a second log entry.
2. **Committed session table (`C-01`, `C-03`, `C-04`)**: `KvStateMachine` caches `(last_seq, CommandResult)` for up to `Config::max_sessions` clients, returning the cached outcome on duplicate retries, rejecting sequence gaps (`seq > last_seq + 1`), and preserving the session table across `"SRSN"` snapshots (`N-03`).

Both `simraft_node::Client` in Rust and `simraft_kv.SimRaftClient` in Python preserve `(client_id, seq)` across redirects and transient retries.

## Why does `simraft-node client get` return exit code `1` when a key is missing?

Shell scripts and automation agents need an unambiguous distinction between a key whose value is empty (`""`), a key that was linearizably confirmed absent, and a cluster or network failure:

- `0`: operation succeeded (`put`/`delete` committed and printed `ok`, or `get` found the key and wrote its value bytes to stdout).
- `1`: `get` completed a linearizable `ReadIndex` query and confirmed the key is not present (writes `simraft-node: key not found` to stderr and nothing to stdout).
- `2`: invalid CLI flags, local storage error, or cluster/transport unavailability within the retry budget.

## Can I embed `simraft-core` with a custom state machine instead of `simraft-kv`?

Yes. `simraft-core` treats command payloads and snapshot bytes as opaque `Vec<u8>` buffers and has zero dependency on `simraft-kv`. See [`simraft-core/examples/embed.rs`](../simraft-core/examples/embed.rs) (`cargo run -p simraft-core --example embed --locked`) for a self-contained three-node cluster embedding in ~200 lines of Rust.

## Does `simraft-node` include TLS or peer authentication?

No. As documented in [SECURITY.md](../SECURITY.md) and [ARCHITECTURE.md](ARCHITECTURE.md), `simraft-node` speaks unencrypted `"SRKV"` peer frames and `"SRCL"` client frames over TCP and is designed for localhost development, private VPCs, or deployment behind mutually authenticated TLS tunnels or service meshes (such as WireGuard, Envoy, or stunnel). All untrusted framing paths still enforce strict 16 MiB length caps, socket read/write timeouts, bounded channel queues, and CRC-32 / SHA-256 integrity checks.

## Where are the formal invariants and design decisions documented?

- [ARCHITECTURE.md](ARCHITECTURE.md) specifies roles, message schemas, byte layouts, domain-separated digests, and rejected alternatives.
- [INVARIANTS.md](INVARIANTS.md) maps every safety, durability, liveness, determinism, and adapter invariant (`S-01..S-10`, `T-01..T-06`, `R-01..R-06`, `C-01..C-04`, `M-01..M-05`, `N-01..N-08`, `H-01..H-09`, `D-01..D-03`, `L-01..L-04`, `A-01..A-09`) to the exact oracle, property test, or benchmark enforcing it.
- [DECISIONS.md](../DECISIONS.md) records workspace-level governance and dependency rules.
