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
- `simraft-sim` will hold the deterministic simulator, the fault
  injector, and the linearizability checker. It is a placeholder
  until milestone M4.

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
  after the fact. This part is finished and under review as
  milestone M2.

Snapshots and membership changes arrive at M3. The simulator, the
checker, and the command line tools arrive at M4.

## How it is used

There is no server binary yet. Today the crates are libraries, and
the core is driven like this.

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

## Where to read more

- `docs/ARCHITECTURE.md` is the full design, written to be argued
  with. It covers the protocol choices, the byte formats, the
  simulator design, and the alternatives that were considered and
  rejected.
- `docs/INVARIANTS.md` lists every promised behavior and names the
  test, oracle, or benchmark that enforces it.
- `DECISIONS.md` records the founding choices for the project and
  where each one came from.

## License

Licensed under either of MIT or Apache-2.0 at your option.
