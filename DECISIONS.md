# Architectural Decisions

Founding governance and engineering decisions for the `simraft-kv`
workspace.

- **Repository owner and visibility** The repository lives publicly
  under `david-engelmann/simraft-kv` from creation.
- **Licensing** Every crate and source file is dual licensed under
  `MIT OR Apache-2.0` at the user's option.
- **Rust edition, MSRV, and pinned CI toolchain** The workspace uses
  Rust 2024 edition with a verified minimum supported Rust version of
  `1.88`, commits `Cargo.lock` for deterministic builds, and pins the
  CI verification toolchain in `rust-toolchain.toml`.
- **Zero-dependency consensus core** `simraft-core` depends only on
  the Rust standard library, and `simraft-kv` depends only on
  `simraft-core` and `std`. External crates (`crc32fast`, `sha2`,
  `clap`, `criterion`, and `proptest`) are restricted to wire
  framing, I/O adapters, simulation CLIs, benchmarks, and test
  suites.
- **Continuous seed and property stress volume** Pull requests run
  1,000 property cases and 1,000 chaos simulator seeds, pushes to
  `main` run 10,000 cases and 10,000 chaos seeds, and the scheduled
  weekly sweep explores 100,000 chaos seeds across ten shards.
- **Formal invariant traceability** `docs/INVARIANTS.md` maps every
  consensus safety, durability, liveness, determinism, resource, and
  diagnostic invariant directly to the property test, simulator
  oracle, scenario test, or benchmark that enforces it.
