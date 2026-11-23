# simraft-core

A sans-I/O Raft consensus core in Rust: leader election with
pre-vote, log replication, single-server membership changes,
chunked snapshot transfer, and linearizable reads through
ReadIndex. The core owns no threads, sockets, files, or
clocks. A driver feeds it events (ticks, messages, client
proposals, persistence acknowledgments) and executes the
actions it returns (sends, persists, applies), so the same
core runs under a deterministic simulator in tests and under
a real network adapter in production.

## Usage

Create one `Node` per member with a `Config` and the
cluster's `ClusterConfig`, then loop: translate the outside
world into `Event` values, call `Node::step`, and carry out
the returned `Action` values in order. Entries apply in log
order through `Action::Apply`. A snapshot the core asks the
driver to persist or load moves as bytes the core never
interprets.

The worked example in the repository's
`simraft-core/examples/embed.rs` shows a complete driver in
one file: three nodes, an in-memory transport, one write
read back on every node. Run it with
`cargo run -p simraft-core --example embed`.

## Documentation

The full design, the byte formats, and the invariant index
live in the repository, in `docs/ARCHITECTURE.md` and
`docs/INVARIANTS.md`. The crate is part of the simraft-kv
workspace, alongside the KV state machine (simraft-kv), the
wire codec (simraft-wire), the deterministic simulator
(simraft-sim), and the node adapter (simraft-node).

Licensed under either of MIT or Apache-2.0 at your option.
