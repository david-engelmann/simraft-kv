# simraft-kv

`simraft-kv` provides the deterministic replicated key-value state
machine, bounded client session deduplication table, and canonical
command and snapshot codecs built on `simraft-core`. It depends on
nothing outside `simraft-core` and the Rust standard library.

## Capabilities

- `KvStateMachine` stores ordered key-value pairs in a `BTreeMap`
  and applies committed log entries in index order.
- Per-client session tracking caches the latest sequence number and
  response up to a configurable session bound
  (`KvStateMachine::with_max_sessions`), preventing retried writes
  from executing twice and rejecting sequence gaps with full
  diagnostic context.
- Canonical little-endian codecs (`encode_command`, `decode_command`,
  `encode_result`, `decode_result`, `encode_snapshot`, and
  `decode_snapshot`) enforce a 16 MiB payload ceiling, reject
  trailing bytes, and guard snapshots with a domain-separated
  SHA-256 digest (`"simraft-kv/snapshot/v1"`).

## Documentation

- [Architecture specification](https://github.com/david-engelmann/simraft-kv/blob/main/docs/ARCHITECTURE.md)
- [Invariants and traceability matrix](https://github.com/david-engelmann/simraft-kv/blob/main/docs/INVARIANTS.md)
- [API reference on docs.rs](https://docs.rs/simraft-kv)

Licensed under either of [MIT](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-APACHE) at your option.
