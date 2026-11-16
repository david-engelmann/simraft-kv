# simraft-kv post-M4 roadmap

The four-milestone build is complete and gated: the sans-I/O Raft
core, the KV state machine with client sessions, snapshots and
single-server membership, the deterministic simulator with its
oracles and linearizability checker, the CLI, and the CI seed
sweeps. This roadmap is the work that follows. It is ordered by
what each lane makes possible for the next one, not by what is
easiest.

Every lane follows the pattern the milestones used. Stage A lands
the public contract and the tests that fail against it. Stage B
implements until they pass and syncs the documents to the code as
built. Every new promised behavior gets a row in
docs/INVARIANTS.md in the same change that introduces it, naming
the test, oracle, or bench that enforces it by the name it
actually has in the tree. A lane lands through a pull request
with hosted CI green on the exact head and a gate review, the
same bar the milestones cleared.

## Lane 1: simraft-node, a real I/O adapter and runnable node

The core is sans-I/O so that something else owns the sockets, the
files, and the clock. Nothing does yet. This lane builds that
something: a fourth crate, simraft-node, that runs one core node
as a real process.

- Transport over TCP using the section 7.1 frame format, so the
  bytes on a real wire are the bytes the simulator already
  produces and checks. The frame codec currently lives in
  simraft-sim. The lane's first design question is its permanent
  home, and the recommendation is a small simraft-wire crate that
  both the simulator and the adapter use, leaving the core
  serialization-free as section 3 requires.
- File storage implementing the durable record layout of section
  7.3, with the fsync discipline the core's persist-before-response
  contract assumes: an action is acknowledged only after the
  record it names is durable.
- A node binary configured by flags (node id, peer addresses,
  data directory, client address) and a client mode that issues
  Put, Get, and Delete against any member and follows leader
  hints.
- Integration tests that run a three-node cluster on localhost:
  writes survive killing and restarting the leader, a restarted
  node rejoins and catches up, and reads through ReadIndex stay
  linearizable across a failover.

Exit criteria: the integration suite green in CI, new INVARIANTS
rows for adapter durability, frame compatibility, and restart
recovery, and a README section that runs the cluster.

## Lane 2: adversarial depth for the simulator

The simulator is the project's assurance engine, and its fault
model is still polite. This lane makes it ruder, in the open,
one fault at a time.

- Bit-flip corruption of frames in flight, exercising the CRC
  rejection path end to end, and corruption of the durable disk
  region, exercising checksum verification on restore.
- New scenarios: a rolling restart of every node, a partition
  during snapshot transfer, and a learner catching up under load.
- A scheduled weekly sweep at 100,000 seeds, so the long tail of
  the seed space is explored continuously rather than once.

Exit criteria: each fault and scenario lands with the invariant
row it enforces, and the weekly sweep runs clean for its first
month.

## Lane 3: performance and scale

The M4 baselines are recorded in the README: a propose and commit
round at a median of 16.4 microseconds on a directly driven
three-node cluster, and a follower AppendEntries step at 1.46
microseconds. This lane improves them where measurement says it
matters.

- Leader-side batching: coalesce proposals that arrive in the
  same step window into one AppendEntries round, measured against
  the B-01 baseline before and after.
- A replication bench at 10,000-entry logs, so catch-up cost is
  measured at a realistic scale rather than inferred.
- Checker scale: the L-04 history grows from 10,000 to 100,000
  operations, with the visited-state count still recorded.
- Allocation budgets from L-03 tightened to the measured values
  once they have been stable for a full release cycle.

Exit criteria: every change reports its before and after numbers
from the benches in this repository, and no number in the README
moves without a recorded run behind it.

## Lane 4: operability and examples

Small, continuous, and never allowed to block the lanes above.

- A worked example that embeds the core in a single file with an
  in-memory transport, so a reader can run the whole pattern in
  one sitting.
- A stats snapshot on the core (term, role, commit and applied
  indexes, peer progress) exposed as data the caller renders, so
  adapters can offer metrics without the core learning what a
  metric is.
- Documentation passes that keep ARCHITECTURE and INVARIANTS
  conformed to the code as built, of the kind the M3 and M4
  closures required.

## Parked: membership beyond single-server changes

Joint consensus was rejected for the build (ARCHITECTURE A2) in
favor of the simpler and well-understood single-server rule. A
future lane may prototype joint consensus behind a feature flag,
but this roadmap does not authorize it. It starts only on a fresh
decision recorded in DECISIONS.md, with its own design pass
against the dissertation's chapter 4 and a simulator-first test
plan.

## Order and rationale

Lane 1 goes first because every later lane is worth more once
the system runs for real: the adapter exercises the section 7.3
storage contract the simulator can only model, surfaces the API
friction a library only learns from a caller, and gives Lane 3
measurements that mean something outside a bench harness. Lane 2
is second because it is cheap and its yield compounds: every
fault it adds guards all the code that already exists. Lane 3
follows, aimed by Lane 1's real numbers. Lane 4 runs alongside
in small pieces. The parked item stays parked until someone with
the authority to unpark it does so in writing.
