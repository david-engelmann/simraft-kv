# simraft-kv documentation

Reference documentation for the `simraft-kv` sans-I/O Raft workspace.

## Start here

| If you want to... | Read |
| --- | --- |
| Run a three-node localhost cluster or a seeded chaos sweep | [Root README](../README.md) |
| Embed the zero-dependency consensus state machine | [simraft-core README](../simraft-core/README.md) |
| Encode and decode `"SRKV"` wire frames and log records | [simraft-wire README](../simraft-wire/README.md) |
| Use the deterministic key-value state machine and session table | [simraft-kv crate README](../simraft-kv/README.md) |
| Operate the TCP daemon and linearizable client CLI | [simraft-node README](../simraft-node/README.md) |
| Drive fault campaigns and Wing and Gong linearizability checks | [simraft-sim README](../simraft-sim/README.md) |

## Protocol specification and verification

| Document | What it covers |
| --- | --- |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Roles and transitions, Pre-Vote, fast conflict backup, Figure 8 commitment, `ReadIndex`, snapshots, membership changes, exact `"SRKV"` byte layouts, domain-separated digests, and simulator fault model. |
| [INVARIANTS.md](INVARIANTS.md) | Complete traceability matrix (`S-01..S-10`, `T-01..T-06`, `R-01..R-06`, `C-01..C-04`, `M-01..M-05`, `N-01..N-08`, `H-01..H-09`, `D-01..D-03`, `L-01..L-04`, `A-01..A-09`) naming every enforcing oracle, property test, and benchmark. |
| [DECISIONS.md](../DECISIONS.md) | Workspace architectural decisions and trade-off rationales. |
| [SECURITY.md](../SECURITY.md) | Defensive frame and session caps, unauthenticated peer transport scope, durability assumptions, and vulnerability reporting. |
| [ROADMAP.md](ROADMAP.md) · [CHANGELOG.md](../CHANGELOG.md) | Completed milestones, post-M4 hardening lanes, parked extensions, and release notes. |
