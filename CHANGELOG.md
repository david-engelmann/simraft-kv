# Changelog

All notable changes to the `simraft-kv` workspace are documented in
this file. The format follows Keep a Changelog and this project
adheres to Semantic Versioning.

## [0.2.3] - 2026-11-27

### Added

- Published `mdBook` documentation site (`book/`) with custom deep-teal
  `brand.css` theme, Mermaid architecture diagram support, `404.html`,
  OpenGraph and Twitter social metadata, automated `book/sync-docs.sh`
  staging with canonical GitHub link rewriting, `mdbook-linkcheck`
  dead-link enforcement, and GitHub Pages deployment in
  `.github/workflows/docs.yml`.
- Runnable top-level `examples/` directory (`chaos_sweep_and_replay.py`
  and `cluster_quickstart.sh`) exercising 10-seed `--chaos` sweeps,
  byte-identical trace replay, offline `"SRCL"` v1 framing, shell
  completions, and 3-node localhost TCP `put`/`get`/`delete` workflows
  in CI.
- Out-of-workspace `cargo-fuzz` harness (`fuzz/`) with seed corpora and
  three fuzz targets (`wire_frame`, `kv_codec`, and `client_protocol`)
  covering `"SRKV"` frame reassembly, `"SRSN"` snapshot and command
  codecs, and `"SRCL"` client framing, compiled on stable Rust in CI.
- Root `AGENTS.md` guide for AI coding agents, `docs/FAQ.md`, and
  `docs/COMPARISON.md` comparing `simraft-kv` against `tikv/raft-rs`,
  `openraft`, `etcd/raft`, and deterministic simulation harnesses.
- Clean-machine release installer workflow
  (`.github/workflows/clean-install.yml`) verifying `install.sh` on
  fresh Ubuntu and macOS runners without a source checkout or Rust
  toolchain, plus SLSA build provenance attestations
  (`actions/attest-build-provenance@v2`) on GitHub Release archives in
  `.github/workflows/release.yml`.

### Changed

- Updated root `README.md` navigation links and documentation matrix,
  expanded `.github/ISSUE_TEMPLATE/config.yml` with Published Docs and
  GitHub Discussions links, added `book`, `examples`, and `fuzz-check`
  targets to `Makefile`, and bumped all five workspace crates and the
  `simraft-kv` Python package to `0.2.3`.

## [0.2.2] - 2026-11-27

### Added

- Deterministic zero-extra-dependency shell completion scripts (`Shell`
  and `completions_script`) for `bash`, `zsh`, `fish`, and `powershell`
  across `simraft-node completions <SHELL>`,
  `simraft-sim --completions <SHELL>`, and `simraft_kv.completions` in
  the Python SDK, with CLI and SDK integration tests covering all four
  shells across both binaries.
- POSIX `install.sh` release installer for Linux and macOS (`x86_64` and
  `aarch64`) that downloads `SHA256SUMS` and verifies every archive
  digest via `sha256sum` or `shasum -a 256` prior to extraction, failing
  closed on checksum mismatch and supporting `--version`,
  `--install-dir`, `--bin`, and `--base-url`.
- Supply-chain, citation, build, and community governance standards
  including `deny.toml` (`cargo-deny` v2 license, duplicate-crate,
  advisory, and registry checks), `CITATION.cff`, `CODE_OF_CONDUCT.md`
  (Contributor Covenant v2.1), `.editorconfig`, `Makefile`,
  `.github/pull_request_template.md`, and `.github/ISSUE_TEMPLATE`
  templates.

### Changed

- Added a `cargo-deny` supply-chain audit job (`deny`) and `shellcheck`
  validation for `install.sh` to `.github/workflows/ci.yml`, updated
  `.github/workflows/release.yml` fallback tags and
  `.github/workflows/verify-published.yml` registry smoke checks to
  `0.2.2`, and renamed the scenario sweep job in
  `.github/workflows/weekly-sweep.yml` to
  `Adversarial fault scenarios, wide seed batches`.
- Bumped all five Rust crates (`simraft-core`, `simraft-wire`,
  `simraft-kv`, `simraft-node`, and `simraft-sim`) and the `simraft-kv`
  Python package to `0.2.2`, and documented `install.sh` and shell
  completions in `README.md`.

## [0.2.1] - 2026-11-26

### Added

- Pure-Python `"SRCL"` v1 linearizable TCP Raft client (`SimRaftClient`,
  `ClientReply`, `encode_client_request`, `decode_client_response`,
  `find_node_binary`, `MAX_PAYLOAD_BYTES`, and `MAX_CLIENT_BODY_BYTES`)
  in `simraft_kv` with automatic leader redirect following, candidate
  leader promotion, 16 MiB payload enforcement, and `(client_id, seq)`
  session-deduplicated retries across `put`, `get`, `get_text`, and
  `delete`.
- Unit tests for `"SRCL"` request and response codecs, mock TCP
  redirect and session-deduplication retry tests, and a live 3-node
  `simraft-node serve` cluster failover integration test in
  `python/tests/test_sdk.py`.
- Post-publish registry verification workflow
  (`.github/workflows/verify-published.yml`) testing published `0.2.1`
  crates.io libraries (`simraft-core`, `simraft-kv`, `simraft-wire`),
  `cargo install` CLI binaries (`simraft-sim`, `simraft-node`), and the
  `simraft-kv==0.2.1` PyPI package on clean runners.

### Changed

- Updated workspace dependencies to `sha2 0.11`, `criterion 0.8`, and
  `clap 4.6.10`, and bumped all five Rust crates and the `simraft-kv`
  Python package to `0.2.1`.
- Pinned `actions/checkout` to `v7.0.1` across GitHub Actions workflows
  (including SHA-pinned `weekly-sweep.yml`) and updated `ci.yml` to
  build both `simraft-sim` and `simraft-node` prior to running the
  Python SDK test suite.
- Updated `docs/ROADMAP.md` with the `v0.2.0` and `v0.2.1` delivery
  history and expanded the Python Quickstart in `README.md` with
  `SimRaftClient`.

### Fixed

- Converted relative media (`docs/media/...`) and repository document
  links in the root `README.md` to canonical GitHub URLs so images and
  documentation links render properly on PyPI and external package
  registries.

## [0.2.0] - 2026-11-25

Full workspace release of all five crates at version `0.2.0`
(`simraft-core`, `simraft-wire`, `simraft-kv`, `simraft-node`, and
`simraft-sim`), advancing past the initial `simraft-core` `0.1.0`
registry publication so the entire workspace ships under a unified
version.

### Added

- `simraft-core` zero-dependency sans-I/O Raft consensus state
  machine supporting Pre-Vote leader election, leader recency checks,
  fast conflict log backup, Figure 8 commitment, single-voter
  durability-gated commitment (`R-06`), leader send coalescing
  (`B-02`), single-server membership changes (`AddLearner`,
  `PromoteVoter`, `RemoveVoter`), chunked `InstallSnapshot` transfer,
  clock-free linearizable reads via `ReadIndex`, uncommitted log tail
  backpressure (`Config::max_uncommitted_entries`), disjoint
  `ClusterConfig` validation, and `Node::stats` inspection (`O-01`).
- `simraft-wire` canonical message frame, log entry, and cluster
  configuration codec with 16 MiB frame bounds, streaming header
  validation (`inspect_frame_header`), and domain-separated IEEE
  CRC-32 integrity checks (`"SRKV/frame/v1"`).
- `simraft-kv` deterministic `BTreeMap` replicated key-value state
  machine with configurable bounded client session deduplication
  (`KvStateMachine::with_max_sessions`), 16 MiB command and snapshot
  payload bounds, strict trailing-byte rejection, and domain-separated
  FIPS 180-4 SHA-256 snapshot verification (`"simraft-kv/snapshot/v1"`).
- `simraft-node` TCP and file-backed Raft server runtime and CLI
  client (`serve` and `client put/get/delete`), stateful
  `simraft_node::Client` session API, collision-free per-call session
  identifiers in `run_client`, bounded peer and inbox channels, socket
  read and write timeouts, snapshot persistence fencing (`A-05` and
  `A-06`), and torn log tail truncation, mid-burst leader kill
  durability, and wiped-member rejoin via snapshot (`A-07` through
  `A-09`).
- `simraft-sim` single-threaded deterministic simulation harness,
  seeded chaos profile (`--chaos` and `FaultConfig::chaos`), virtual
  clock and priority event queue, simulated volatile and durable disk
  model, five per-event safety oracles, bounded healing and leader
  convergence checks (`H-02`), domain-separated state and log digests
  (`"simraft-kv/state-digest/v1"` and `"simraft-kv/log-digest/v1"`),
  and per-key Wing and Gong linearizability verification.
- Public documentation index (`docs/README.md`), per-crate `README.md`
  files, light and dark SVG lockups and architecture and baseline
  diagrams, a `1200x630` social card, a real-output
  `docs/media/money-shot.gif` walkthrough, a typed Python SDK and
  precompiled `simraft-kv` PyPI wheels (`pyproject.toml`,
  `python/simraft_kv`), and GitHub Actions OIDC release automation for
  crates.io, PyPI, and five-platform GitHub Releases.

## [0.1.0] - 2026-11-17

Initial crates.io publication of `simraft-core` `0.1.0`.
