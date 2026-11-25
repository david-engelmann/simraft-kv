# simraft-core

A sans-I/O Raft consensus core in Rust with leader election and
Pre-Vote, log replication with fast conflict backup, single-server
membership changes, chunked snapshot transfer, and clock-free
linearizable reads through ReadIndex. The core owns no threads,
sockets, files, or clocks. A driver feeds it events (ticks,
messages, client proposals, and persistence acknowledgments) and
executes the actions it returns (sends, persists, and applies), so
the same core runs inside a deterministic simulator in tests and
inside a real network adapter in production.

## Usage

Create one `Node` per member with a `Config` and the cluster
`ClusterConfig`, then loop by translating external inputs into
`Event` values, calling `Node::step`, and executing the returned
`Action` values in order. Committed entries apply in log order
through `Action::Apply`. Snapshots that the core asks the driver to
persist or load move as opaque byte buffers that the core never
interprets.

The worked example in `simraft-core/examples/embed.rs` shows a
complete three-node driver in one file with an in-memory transport
and one write read back on every node. Run it from the workspace
root with `cargo run -p simraft-core --example embed`.

## Documentation

- [Architecture specification](https://github.com/david-engelmann/simraft-kv/blob/main/docs/ARCHITECTURE.md)
- [Invariants and traceability matrix](https://github.com/david-engelmann/simraft-kv/blob/main/docs/INVARIANTS.md)
- [API reference on docs.rs](https://docs.rs/simraft-core)

Licensed under either of [MIT](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-APACHE) at your option.
