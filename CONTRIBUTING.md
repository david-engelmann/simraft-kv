# Contributing to simraft-kv

## Toolchain and MSRV

The CI verification toolchain is pinned in `rust-toolchain.toml` and
the workspace supports Rust `1.88` and newer (`rust-version = "1.88"`
in `Cargo.toml`). Run every command below from the workspace root.

## Verification gates

Run the full local gate before opening a pull request.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
PROPTEST_CASES=500 cargo test --workspace --all-features --locked
cargo bench --workspace --locked -- --test
cargo run -p simraft-core --example embed --locked
cargo run -p simraft-sim --release --locked -- --seed 0 --seeds 25 --chaos
```

Hosted CI runs these gates on Linux and macOS, verifies MSRV `1.88`,
checks public documentation rules and determinism guards, and runs
property and chaos seed sweeps at 1,000 cases and seeds per pull
request and 10,000 on `main`.

## Replaying a failing seed

Every simulator run prints its active seed on its first output line
(`D-03`). Replay any seed locally in release mode with `--chaos` and
`--trace` when inspecting a chaos sweep failure.

```sh
cargo run -p simraft-sim --release --locked -- --seed 4213 --seeds 1 --chaos --trace
```

Running the same seed in a separate operating system process
produces byte-identical traces and digests (`D-01`). Property test
suites scale through `PROPTEST_CASES` and print failing seed values
for direct deterministic replay.

## How changes land

Every promised behavior in `docs/ARCHITECTURE.md` has a row in
`docs/INVARIANTS.md` naming the exact test, simulator oracle, or
benchmark that enforces it in the repository tree. When adding or
changing protocol, codec, or adapter behavior, add or update the
corresponding invariant row and keep `docs/ARCHITECTURE.md`
synchronized with the code as built.

Commits carry a `Signed-off-by` trailer with the author's identity.

## Copy rules for public text

READMEs and documentation state clearly what the software is, what
it does, and how to operate and verify it. Never place em dashes or
en dashes anywhere in Markdown files (CI scans every `.md` file by
codepoint). Keep prose sentences plain and direct without colons or
semicolons outside code blocks, URLs, CLI commands, and Markdown
tables. Benchmark figures in `README.md` come exclusively from
recorded Criterion runs of the benchmarks in this repository.
