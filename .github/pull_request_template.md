## Summary

Describe the defect fixed or capability added and name any invariant row (`docs/INVARIANTS.md`), architecture section (`docs/ARCHITECTURE.md`), or decision record (`DECISIONS.md`) affected.

## Verification checklist

- [ ] `cargo fmt --all -- --check` passes
- [ ] `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` passes
- [ ] `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked` passes
- [ ] `cargo test --workspace --all-features --locked` passes
- [ ] `cargo bench --workspace --locked -- --test` passes
- [ ] `cargo deny check` passes
- [ ] `python3 -m unittest discover -s python/tests -v` passes
- [ ] `simraft-core` and `simraft-kv` keep zero external runtime dependencies and all deterministic crates pass the `D-02` nondeterminism guard
- [ ] Public Markdown files contain zero em dashes or en dashes
