# simraft-sim

`simraft-sim` is the single-threaded deterministic simulation
harness, fault injector, safety oracle runner, and Wing and Gong
linearizability checker for `simraft-kv`.

## Capabilities

- Virtual clock and deterministic event queue driven by an internal
  SplitMix64 and xoshiro256starstar PRNG stream so every run
  replays byte for byte from a single 64-bit seed across processes.
- Network and storage fault injection (`FaultConfig` and
  `FaultConfig::chaos`) covering packet drops, duplicates, variable
  latency, asymmetric and chain partitions, frame bit-flip
  corruption, crash and volatile disk loss, durable log and snapshot
  corruption, and virtual clock jumps.
- Tick-by-tick verification of Election Safety, Leader Append-Only,
  Log Matching, Leader Completeness, and State Machine Safety,
  bounded healing and leader convergence checks, domain-separated
  state and log digests, and per-key linearizability verification.

## CLI usage

Run a fault-free or chaos-enabled seed sweep from the workspace
root.

```sh
cargo run -p simraft-sim --release -- --seed 0 --seeds 25 --chaos
cargo run -p simraft-sim --release -- --seed 912 --seeds 1 --chaos --trace
cargo run -p simraft-sim --release -- --seed 0 --seeds 100 --chaos --json
```

`SIM_SEED` sets the starting seed when `--seed` is omitted. Every
run prints its seed on the first output line and exits nonzero if
any safety oracle, healing convergence check, or linearizability
verdict fails.

## Documentation

- [Architecture specification](https://github.com/david-engelmann/simraft-kv/blob/main/docs/ARCHITECTURE.md)
- [Invariants and traceability matrix](https://github.com/david-engelmann/simraft-kv/blob/main/docs/INVARIANTS.md)
- [API reference on docs.rs](https://docs.rs/simraft-sim)

Licensed under either of [MIT](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-MIT) or [Apache-2.0](https://github.com/david-engelmann/simraft-kv/blob/main/LICENSE-APACHE) at your option.
