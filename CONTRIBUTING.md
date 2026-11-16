# Contributing to simraft-kv

## Toolchain

The Rust toolchain is pinned in `rust-toolchain.toml` (see
DECISIONS.md Q4). Everything below runs from the repository root.

## The gates

Run the full local gate before pushing:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --workspace
cargo bench --workspace -- --test
```

Hosted CI runs the same gates on the pinned toolchain plus two
guards: a docs guard (the public copy rules below) and a seed
sweep at the DECISIONS Q6 volumes (1,000 seeds per pull request,
10,000 on main).

## Replaying a failing seed

Every simulator run prints its seed on its first output line
(invariant D-03). A failing seed from CI replays locally with
that single value:

```sh
cargo run -p simraft-sim --release -- --seed 4213 --seeds 1 --trace
```

The same seed in a separate process produces byte-identical
output (invariant D-01), so a replay that does not reproduce is
itself a bug worth reporting. Property tests scale through the
`PROPTEST_CASES` environment variable; a failing proptest case
prints its own seed the same way.

## How changes land

Work happens in milestones (docs/ARCHITECTURE.md section 16),
each in two stages: Stage A lands the public contract and tests
that fail against it; Stage B implements until they pass and
syncs the docs to the code as built. Every invariant in
docs/INVARIANTS.md names its enforcing test, oracle, or bench;
when you add behavior, add or update the row in the same change,
naming the test by the name it will actually have in the tree.

Commits carry a Signed-off-by trailer with the repository
owner's identity.

## Copy rules for public text

READMEs and crate docs state what the software is, what it does,
and how it is used. No em dashes or en dashes anywhere in public
copy (the docs guard scans for them by codepoint). Numbers in the
README come only from recorded benchmark runs of the benches in
this repository, never from estimates.
