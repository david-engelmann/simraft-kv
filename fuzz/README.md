# simraft-kv Fuzz Targets

Out-of-workspace `cargo-fuzz` (`libFuzzer`) harnesses targeting every untrusted wire, snapshot, command, and client protocol decoder in `simraft-kv`.

`fuzz/` lives in its own Cargo workspace (`[workspace] members = ["."]`) so `libfuzzer-sys` stays isolated from the zero-dependency consensus crates while CI compiles all fuzz targets on stable Rust (`cargo check --manifest-path fuzz/Cargo.toml --locked --bins`) on every pull request.

## Targets

| Target | Entry point | Invariants exercised |
| --- | --- | --- |
| `wire_frame` | `fuzz_targets/wire_frame.rs` | Feeds arbitrary bytes into `simraft_wire::inspect_frame_header`, `frame_len`, `decode_frame`, `decode_entry_bytes`, `decode_config_bytes`, and `simraft_node::runtime::FrameBuffer` across arbitrary chunk splits (`H-04`, `A-04`, `E-01`), asserting canonical round-trip encoding on valid inputs and bounded rejection on hostile headers. |
| `kv_codec` | `fuzz_targets/kv_codec.rs` | Feeds arbitrary bytes into `simraft_kv::decode_command`, `decode_result`, and `decode_snapshot` (`N-03`, `E-01`), asserting byte-for-byte canonical round-trip stability and clean `KvStateMachine` snapshot installation on valid inputs. |
| `client_protocol` | `fuzz_targets/client_protocol.rs` | Feeds arbitrary bytes and length-prefixed `Cursor<&[u8]>` streams into `simraft_node::client::decode_request` and `decode_response`, asserting 16 MiB bound enforcement (`MAX_CLIENT_BODY_BYTES`) and round-trip fidelity without panics. |

## Seed corpora

Initial valid binary protocol seeds live under `fuzz/seeds/<target>/` so fuzzing immediately explores deep structural mutations past magic and checksum checks (`"SRKV"`, `"SRSN"`, and `"SRCL"`).

## Running locally

Compile-check all targets on stable Rust (also wired to `make fuzz-check`):

```sh
cargo check --manifest-path fuzz/Cargo.toml --locked --bins
```

Run a fuzz campaign with `cargo-fuzz` and a nightly toolchain:

```sh
cargo install cargo-fuzz
cargo +nightly fuzz run wire_frame fuzz/seeds/wire_frame -- -max_total_time=60
cargo +nightly fuzz run kv_codec fuzz/seeds/kv_codec -- -max_total_time=60
cargo +nightly fuzz run client_protocol fuzz/seeds/client_protocol -- -max_total_time=60
```
