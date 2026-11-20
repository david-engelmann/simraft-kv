# simraft-kv

simraft-kv is a Raft consensus implementation in Rust. Raft is how a
group of servers agrees on one shared history of commands even when
some of them crash or lose contact with each other. On top of that
agreement, simraft-kv runs a small key-value store, so the group acts
as one reliable store that keeps working as long as most of its
members are alive.

The project is built for proof, not just for use. The protocol core
makes decisions and performs no input or output of its own, which
means a test driver can feed it events in any order, drop or delay
messages, crash nodes, and replay the whole run from a seed. Every
behavior the design promises is written down as a numbered invariant
and tied to a test that would fail if the promise broke.

## What is in the workspace

- `simraft-core` holds the Raft protocol itself. It is a pure state
  machine. You hand it an event, such as a timer tick, an incoming
  message, or a client proposal, and it hands back a list of actions
  for you to carry out, such as sending a message, saving entries to
  disk, or applying a committed command. It depends on nothing outside
  the Rust standard library.
- `simraft-kv` holds the key-value state machine. It turns committed
  commands into reads and writes, remembers what each client already
  asked for so a retried request never runs twice, and serves
  linearizable reads, meaning a read is guaranteed to reflect every
  write that finished before it started.
- `simraft-sim` holds the deterministic simulator, the fault
  injector, and the linearizability checker.
- `simraft-wire` holds the message frame format that both the
  simulator and the real adapter speak, so the bytes tested in
  simulation are the bytes real nodes exchange.
- `simraft-node` is the real adapter. It runs one core node as a
  process, with files for storage and TCP for transport, and has
  a client mode for putting, getting, and deleting keys against
  a running cluster.

## What works today

- Leader election with Pre-Vote, so a server that was cut off from
  the group cannot force a needless new election when it comes back.
- Log replication with fast conflict backup, so a follower whose log
  disagrees with the leader catches up in a few round trips instead
  of one entry at a time.
- Commitment by replica counting that follows the Figure 8 rule from
  the Raft paper, so an entry from an older leader is never treated
  as final just because most servers hold a copy of it.
- Linearizable reads through the ReadIndex protocol, with client
  request deduplication on both sides of a commit, in flight and
  after the fact. This part landed as
  milestone M2.
- Snapshots with chunked transfer, so a server that falls too far
  behind receives the state it missed instead of replaying the
  whole log, and one-at-a-time membership changes, so servers join
  and leave without stopping the group. This part landed as
  milestone M3.
- A deterministic simulator that runs the whole system on a
  virtual clock with dropped, duplicated, and reordered messages,
  crashes, and clock jumps, checks five safety rules after every
  event, and verifies the recorded client history for
  linearizability at the end of every run. This part landed as
  milestone M4, which completes the planned build.

## How it is used

The protocol crates are libraries, and the core is driven like
this.

```rust
let actions = node.step(event);
for action in actions {
    // Send the message, persist the entries, apply the command,
    // or answer the client, as the action says.
}
```

Your code owns the network, the disk, and the clock. The core owns
the decisions. That split is the whole design, and it is what makes
the system replayable under test.

The simulator is the one binary in the workspace. It runs a
seeded cluster through chaos and healing phases and prints one
summary line per seed.

```sh
cargo run -p simraft-sim --release -- --seed 912 --seeds 1
```

The same seed in a separate process produces byte-identical
output, so any failure a seed sweep finds replays exactly.

## Running the tests

The toolchain is pinned in `rust-toolchain.toml`. With Rust
installed, the full check is

```sh
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The property suites scale with the `PROPTEST_CASES` environment
variable. Continuous integration runs 1,000 cases per pull request
and 10,000 on the main branch.

## Benchmarks

Measured with the criterion benches in this repository, recorded
from hosted runs on 2026-11-19. A full propose and commit round
on a three-node cluster driven directly, one 32-byte entry per
round, takes a median of 1.50 microseconds, about 668,000 rounds
per second. A batch of 32 proposals released by one persist
acknowledgement takes a median of 85.5 microseconds, about
374,000 entries per second, where the same batch took 122
microseconds before the release learned to coalesce its sends.
A follower processing one AppendEntries message takes a median
of 76 nanoseconds. A follower with an empty log catches up to a
10,000-entry leader log in a median of 4.06 milliseconds. The
linearizability checker decided a generated history of 100,000
operations over 64 keys after visiting 100,000 states, one per
operation, because the history is sequential per key. Reproduce
with `cargo bench --workspace`.

## Where to read more

- `docs/ARCHITECTURE.md` is the full design, written to be argued
  with. It covers the protocol choices, the byte formats, the
  simulator design, and the alternatives that were considered and
  rejected.
- `docs/INVARIANTS.md` lists every promised behavior and names the
  test, oracle, or benchmark that enforces it.
- `DECISIONS.md` records the founding choices for the project and
  where each one came from.
- `docs/ROADMAP.md` is the post-M4 plan: a real I/O adapter first,
  then a ruder simulator, measured performance work, and
  operability, in that order and for stated reasons.

## License

Licensed under either of MIT or Apache-2.0 at your option.
