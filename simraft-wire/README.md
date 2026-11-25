# simraft-wire

`simraft-wire` is the canonical message frame codec for the
`simraft-kv` workspace. Both the deterministic simulator
(`simraft-sim`) and the TCP node adapter (`simraft-node`) encode and
decode Raft peer messages through this crate so the exact wire bytes
tested under simulated packet loss and bit-flip corruption run on
real sockets.

## Frame format

Every frame begins with the 4-byte ASCII magic `"SRKV"`, format
version `1`, message kind and flags bytes, little-endian sender
term, source and destination node identifiers, and a 32-bit body
length capped at 16 MiB (`MAX_FRAME_BODY_LEN`). Every frame ends
with an IEEE CRC-32 checksum computed over the domain prefix
`"SRKV/frame/v1"` followed by the header and body bytes.

Helper functions also encode and decode log entries
(`encode_entry_bytes`, `decode_entry_bytes`) and cluster
configurations (`encode_config_bytes`, `decode_config_bytes`), while
`inspect_frame_header` validates a 36-byte header on streaming
sockets before allocating buffer space for the body.

## Documentation

- [Architecture specification and byte tables](https://github.com/david-engelmann/simraft-kv/blob/main/docs/ARCHITECTURE.md)
- [Invariants and traceability matrix](https://github.com/david-engelmann/simraft-kv/blob/main/docs/INVARIANTS.md)
- [API reference on docs.rs](https://docs.rs/simraft-wire)

Licensed under either of [MIT](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-APACHE) at your option.
