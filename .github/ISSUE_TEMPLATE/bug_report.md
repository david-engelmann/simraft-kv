---
name: Bug report
about: Report a consensus safety violation, determinism drift, wire codec defect, or node runtime failure
title: ""
labels: bug
assignees: ""
---

## Environment

State the `simraft-sim --version` and `simraft-node --version` output, operating system, and `rustc --version`.

## Deterministic reproduction

Provide the exact `simraft-sim` command and 64-bit `--seed` value (with `--chaos` or `--trace` if used), `simraft-node` invocation, or minimal Rust or Python snippet that reproduces the issue.

## Expected versus actual behavior

State the invariant ID from `docs/INVARIANTS.md` when applicable alongside the expected outcome and the actual output or failure trace observed.
