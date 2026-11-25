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

The full design and invariant traceability matrix live in
`docs/ARCHITECTURE.md` and `docs/INVARIANTS.md` in the workspace
repository.

Licensed under either of MIT or Apache-2.0 at your option.
