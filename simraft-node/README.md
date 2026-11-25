# simraft-node

`simraft-node` is the TCP and file-backed runtime adapter and CLI
client for `simraft-kv`. It drives one `simraft-core` Raft state
machine and one `simraft-kv` store inside an operating system
process using durable CRC-checked disk records and `simraft-wire`
peer frames.

## Running a local three-node cluster

Start three nodes in separate terminals from the workspace root.

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

Issue linearizable writes and reads against any node address. The
client follows leader hints automatically.

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

## Exit codes and library client

`put` and `delete` print `ok` and exit with status `0`. `get`
writes the raw value bytes to standard output and exits with status
`0` when the key exists, or writes `simraft-node: key not found` to
standard error and exits with status `1` when the key is absent.
Transport, timeout, or cluster errors exit with status `2`.

Rust callers issuing multiple operations from one process can use
`simraft_node::Client` to keep a persistent session and sequence
counter across calls, or `simraft_node::run_client` for single
operations with unique per-call session identifiers.

Licensed under either of MIT or Apache-2.0 at your option.
