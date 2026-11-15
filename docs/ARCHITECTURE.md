# simraft-kv Architecture

Status: Milestone 0 design. This document is the contract that Milestones 1 to 4
implement. Any deviation from this document requires an explicit decision update in
DECISIONS.md first. The companion document docs/INVARIANTS.md maps every invariant
stated here to the exact test, oracle, or benchmark that enforces it.

## 1. Purpose and scope

simraft-kv is a replicated key-value store built on a pure sans-I/O Raft core.
Two properties drive every design choice:

1. The consensus logic is a pure function of its inputs. A Raft node reads no
   clock, opens no socket, touches no disk, spawns no thread, and draws no
   operating system randomness. Time, transport, storage, and randomness are
   supplied by the caller as events and executed by the caller as actions.
2. Because the core is pure, the whole cluster can run inside a single-threaded
   deterministic simulation. One seed selects one complete execution: message
   order, timer order, fault schedule, and crash schedule. Any failure replays
   byte for byte from that seed in a separate process.

In scope: leader election with Pre-Vote, log replication with fast conflict
backtracking, the Figure 8 commitment rule, clock-free linearizable reads via
ReadIndex, idempotent client sessions, snapshotting with chunked InstallSnapshot
transfer, single-server membership changes, a fault-injecting simulation harness,
per-tick safety oracles, and a per-key linearizability checker.

Out of scope: Byzantine faults, multi-threaded consensus, joint consensus,
leader leases, stale or bounded-staleness reads, encryption, authentication,
and a production network server. Adapters that bind the core to real sockets
and real files may be built against the event and action protocol defined
here, but they are not part of Milestones 1 to 4.

## 2. System overview

```text
                Propose / ReadIndex / membership request
                                |
                                v
  +-------------------------------------------------------------+
  | Driver (simraft-sim harness, or a future I/O adapter)       |
  | owns: virtual clock, packet queue, simulated disks, RNG     |
  +-------------------------------------------------------------+
        | Event                                ^ Action
        v                                      |
  +-------------------------------------------------------------+
  | simraft-core  Node::step(Event) -> Vec<Action>              |
  | roles, log, commit rule, ReadIndex rounds, membership       |
  +-------------------------------------------------------------+
        | Apply (index, command)               ^ snapshot bytes
        v                                      |
  +-------------------------------------------------------------+
  | simraft-kv  KvStateMachine                                  |
  | BTreeMap store, client sessions, snapshot payload codec     |
  +-------------------------------------------------------------+
```

The driver is the only component that knows whether it is running a
simulation or real I/O. In Milestones 1 to 4 the only driver is the simulator.
The core cannot tell the difference, and that is the point: the code the
simulator tests is the code an adapter would run.

## 3. Crate and module boundaries

The workspace has exactly three crates in Milestones 1 to 4. Workspace
layout: the crates live at the repository root as simraft-core/,
simraft-kv/, and simraft-sim/, next to a root Cargo.toml workspace
manifest (which lands at M1 Stage A); there is no crates/ directory.

| Crate | Kind | External dependencies | Responsibility |
| --- | --- | --- | --- |
| simraft-core | library | none, std only | The Raft protocol as a sans-I/O state machine. |
| simraft-kv | library | none, std only | The KV state machine, client session table, canonical command and snapshot payload encoding. |
| simraft-sim | library plus binary | clap, serde_json, sha2, crc32fast (CLI and I/O only) | The deterministic simulation harness, fault injection, safety oracles, linearizability checker, trace output, and the simraft-sim CLI. |

Dependency direction is one way: simraft-sim depends on simraft-kv and
simraft-core; simraft-kv depends on simraft-core; simraft-core depends on
nothing outside std. proptest and criterion appear only as dev-dependencies
(property tests and benchmarks). crc32fast and sha2 appear only in simraft-sim
and in future storage or transport adapters, never in the core. The core does
not serialize at all (section 7), so it never needs a checksum or hash crate.

Every production library and binary target carries `#![forbid(unsafe_code)]`
at the crate root. Workspace lints deny clippy::all, clippy::pedantic,
clippy::nursery, clippy::todo, and clippy::unimplemented, warn on missing
docs, and build documentation with RUSTDOCFLAGS set to deny warnings.

### 3.1 simraft-core modules

| Module | Contents | Must not contain |
| --- | --- | --- |
| config | Config struct and validation (timing ratios, limits) | any runtime state |
| ids | NodeId, ClientId, Term, LogIndex, ReadRound newtypes | formatting beyond Display and Debug |
| error | ConfigError, ProtocolError, ApplyError with diagnostic context | string-only opaque errors |
| message | RPC request and response structs (structured values, no bytes) | serialization |
| event | The Event enum (section 4.1) | I/O |
| action | The Action enum (section 4.2) | I/O |
| log | Log struct, entry storage offset by the snapshot base index | persistence |
| snapshot | SnapshotMeta and chunk transfer state | payload interpretation |
| membership | ConfigState, single-server change rules | joint consensus |
| readindex | Pending read rounds and quorum accounting | clocks or leases |
| node | Node struct, role state machines, Node::step | threads, sockets, time |

### 3.2 simraft-kv modules

| Module | Contents |
| --- | --- |
| command | Command enum (Put, Delete, NoOp payloads are core-internal), canonical payload encoding and decoding with offset-carrying errors |
| kvsm | KvStateMachine: BTreeMap<Vec<u8>, Vec<u8>> store, apply in log order, state digest input |
| session | Session table: bounded per-client (last_seq, cached_response) deduplication state |
| snapshot_kv | Snapshot payload layout for the store plus session table (section 7.4) |

### 3.3 simraft-sim modules

| Module | Contents |
| --- | --- |
| rng | SplitMix64 seeding plus xoshiro256starstar stream; the only randomness source |
| clock | Virtual clock and the deterministic event priority queue |
| network | Packet queue, directed partition matrix, drop, duplicate, reorder faults |
| disk | Simulated disk with separate durable and volatile buffers |
| faults | Seeded fault schedule: partitions, crashes, clock jumps, healing phase |
| harness | Multi-node driver loop, tick-by-tick oracle invocation |
| oracle | The five core safety oracles plus commit and dedup checkers |
| history | Client operation history recording for linearizability |
| liz | Per-key Wing and Gong linearizability checker with visited-state memoization |
| trace | Canonical trace lines and the final state digest (sha2) |
| main (bin) | simraft-sim CLI: --seed, --json, --trace, --nodes, --ticks flags (clap) |

## 4. The sans-I/O protocol

### 4.1 Events (inputs to the core)

```text
Event::Tick { now: Tick }
Event::Propose { client_id: ClientId, seq: u64, payload: Vec<u8> }
Event::ProposeConfig { change: ConfigChange }
Event::ReadIndex { request_id: u64, key: Vec<u8> }
Event::Message { from: NodeId, msg: Message }
Event::Persisted { kind: PersistKind, up_to_index: LogIndex }
Event::SnapshotPersisted { meta: SnapshotMeta }
Event::Restored { hard_state: HardState, entries: Vec<LogEntry> }
```

As built at M1: the event and action enums carry the variants M1
implements (Tick, Propose, Message, Persisted, Restored). ProposeConfig,
ReadIndex, and SnapshotPersisted arrive at their milestones (M3, M2, M3).
Propose carries the canonical command payload bytes produced by
simraft-kv rather than a KV operation type: the core is KV-agnostic, and
the client identity travels alongside the payload for deduplication.

Tick is a u64 count of virtual milliseconds supplied by the driver. The core
compares Tick values only against deadlines it computed from its own config
and its deterministic per-node timer offsets. It never converts Tick to a
wall clock and never reads a wall clock itself.

Persisted is the acknowledgment that a prior Action::Persist reached the
simulated (or real) durable store. The ordering contract is strict: a node
emits no vote response, append response, or read response whose correctness
depends on state that is not yet acknowledged durable (INVARIANT T-02).

### 4.2 Actions (outputs from the core)

```text
Action::Send { to: NodeId, msg: Message }
Action::PersistHardState { current_term: Term, voted_for: Option<NodeId> }
Action::PersistEntries { entries: Vec<LogEntry> }
Action::PersistSnapshot { meta: SnapshotMeta, data: Vec<u8> }
Action::Apply { index: LogIndex, outcome: ApplyOutcome }
Action::ClientResponse { client_id: ClientId, seq: u64, result: ClientResult }
Action::ReadResponse { request_id: u64, result: ReadResult }
Action::StepDown { term: Term }
```

Node::step takes one event and returns a Vec<Action>. The driver executes
actions in order. Sends go to the packet queue (simulation) or the wire
(adapter). Persists go to the disk model or the file layer. Applies feed the
KV state machine in the driver or the node host, in index order, exactly once
per index per node lifetime, except that after a snapshot install the apply
cursor jumps to the snapshot last included index.

Node also exposes `poll_timeout() -> Option<Tick>` (the next deadline the
driver must wake it at) so the simulator can jump the virtual clock directly
to the next timer or packet event instead of stepping millisecond by
millisecond.

### 4.3 Determinism rules inside the core

- No iteration over HashMap or HashSet anywhere in simraft-core, simraft-kv,
  or the simulator's deterministic paths. All maps are BTreeMap and all sets
  are BTreeSet, so iteration order is sorted key order on every platform.
- No SystemTime, Instant, thread_rng, or OS entropy in any deterministic
  crate. Election timer randomization comes from a deterministic per-node
  offset derived from (seed, node_id) inside the simulator and passed in
  through Config; the core itself holds only the resulting deadline values.
- No floating point in protocol decisions.
- step() is total: every event in every role has a defined outcome, including
  stale, duplicated, reordered, and malformed inputs. Invalid inputs produce
  a counted, logged rejection, never a panic, in release code paths.

## 5. Core data structures

### 5.1 Node state

Persistent state (survives crash, written before dependent responses):
current_term (u64), voted_for (Option<NodeId>), and the log entries.

Volatile state: role, commit_index, last_applied, leader_id hint, the
election deadline, the heartbeat deadline, and pre-candidate vote tallies.

Leader-only volatile state, rebuilt on election: a BTreeMap<NodeId, Progress>
where Progress holds next_index, match_index, snapshot transfer state, and
the highest read round acknowledged by that follower.

Follower and candidate state: votes received as a BTreeSet<NodeId>, and for
pre-candidates a separate pre-vote tally, so a failed pre-vote leaves no
residue in the real vote state.

### 5.2 The log

Log stores entries in a Vec<LogEntry> preceded by a snapshot base:
base_index and base_term describe the last snapshotted entry, and the entry
at log index i lives at vec position i - base_index - 1. Index 0 is the
sentinel empty entry with term 0; the first real entry is index 1.
LogEntry is { term: Term, kind: EntryKind } where EntryKind is one of
Command { client_id, seq, op }, Config { config: ClusterConfig }, or NoOp.
Lookups of term by index, first index of a term, and last index of a term
are the only search operations, and they are binary searches over the Vec.
Keeping entries in one contiguous Vec is deliberate: replication scans are
sequential, snapshots copy a prefix once, and there is no per-entry node
allocation to fragment under churn (see rejected alternative A4).

### 5.3 Cluster configuration

ClusterConfig is { voters: BTreeSet<NodeId>, learners: BTreeSet<NodeId> }.
A node is in exactly one of the two sets. ConfigState holds the latest
config (from the newest config entry in the log, committed or not), the
committed config, and last_config_index, the log index of the newest config
entry. Quorums are always computed against one config at a time: the latest
config for elections and replication counting once its entry exists, which
is safe precisely because at most one config change is ever uncommitted
(section 9).


## 6. Roles and state transitions

Roles are Follower, PreCandidate, Candidate, and Leader. Learner is a
membership status, not a role: a learner runs the Follower transition set
with voting disabled and never starts an election.

| From | Trigger | To | Side effects |
| --- | --- | --- | --- |
| Follower | election deadline passes, no recent leader contact | PreCandidate | emit RequestPreVote for current_term + 1 |
| PreCandidate | pre-vote majority for term + 1 | Candidate | persist current_term + 1 and voted_for = self, then emit RequestVote |
| PreCandidate | pre-vote rejection, higher term seen, or valid AppendEntries | Follower | no persistent state was changed, nothing to undo |
| Candidate | vote majority in its term | Leader | append current-term NoOp entry, initialize Progress for every peer |
| Candidate | valid AppendEntries at its term, or higher term seen | Follower | clear vote tally |
| Candidate | election deadline passes again | PreCandidate | restart through Pre-Vote, never by raw term increment |
| Leader | higher term seen in any response or request | Follower | clear leader state and pending reads with errors |
| Leader | self-removal config entry commits (section 9) | Follower | step down only after the commit, never on append |
| Any | Restore after crash | Follower | rebuild volatile state from durable hard state, log, snapshot |

An election is never started by incrementing the term directly. Every
election deadline leads to PreCandidate first. The only bypass is a node
that has just been promoted from learner, which still runs a pre-vote; there
is no TimeoutNow leadership transfer in Milestones 1 to 4.

### 6.1 Elections and Pre-Vote

On receiving RequestPreVote { term: current_term + 1, last_log_index,
last_log_term } a voter grants the pre-vote only if all of these hold:

- The candidate's log is at least as up to date as the voter's log, using
  the standard comparison: higher last log term wins, then higher last log
  index.
- The voter has not heard from a live leader within its minimum election
  timeout (leader recency check). A voter that recently accepted
  AppendEntries from a leader in the current term denies the pre-vote.
- The hypothetical term is exactly the voter's current_term + 1 or higher
  than it; a pre-vote for a stale term is denied.

Granting or denying a pre-vote changes no persistent state: current_term is
not incremented, voted_for is not written, and the voter's election timer
is not reset (INVARIANT T-03). RequestVote handling follows the Raft paper:
grant only if the candidate's term is current, the voter has not voted for
another candidate in this term, and the candidate's log is up to date.
Granting a real vote persists hard state first and resets the election
timer.

### 6.2 Replication and fast log backup

The leader sends AppendEntries with prev_log_index, prev_log_term, a bounded
batch of entries (Config::max_entries_per_append, default 64, and
Config::max_append_bytes, default 1 MiB), leader_commit, and the current
read_round (section 8). A follower accepts only if its log matches at
prev_log_index and prev_log_term; conflicting entries and everything after
them are deleted, new entries are appended, the new entries are persisted,
and only then is the success response sent.

On rejection the follower returns conflict_index and conflict_term, the
standard fast-backup hint: conflict_index is the first index of the
conflicting term in the follower's log (or the follower's last index + 1
when its log is too short), and conflict_term is the term at that index
when one exists. The leader then sets that follower's next_index to the
last index of conflict_term in its own log when it has that term, else to
conflict_index. (As built at M1: the leader's resulting next_index is one
past its last index of conflict_term, the standard form; the follower's
contiguous verified prefix is what the next probe must match.) This turns
worst-case backup from one index per round trip into one term per round
trip.

match_index updates are monotonic. An AppendEntriesResponse carries the
follower's match_index as of that append; the leader applies
match_index = max(match_index, response.match_index) and ignores a stale
reordered response that would move it backwards (INVARIANT S-07).
next_index never exceeds match_index + 1 plus in-flight batching, and never
drops below the snapshot base index + 1 outside the InstallSnapshot path.

### 6.3 Commitment, including the Figure 8 rule

The leader advances commit_index to the highest N such that a quorum of
match_index values is at least N, N is greater than the current
commit_index, and the log entry at N is from the leader's current term.
Entries from earlier terms become committed only indirectly, as a prefix of
a current-term entry that commits (INVARIANT S-06). This is the Figure 8
rule from the Raft paper: counting replicas for an earlier-term entry can
commit an entry that a future leader is still able to overwrite.

On election every leader appends a NoOp entry in its own term before it
accepts config proposals or serves ReadIndex. The NoOp is the earliest
entry the new leader may count toward commitment, and committing it commits
the entire prefix, which is also what makes the leader's knowledge of the
committed prefix complete.

## 7. Byte-level schemas

simraft-core works with structured values only. The layouts below are the
canonical encodings used by simraft-kv payloads, by the simulator's disk and
trace layers, and by any future adapter. All multi-byte integers are
little-endian. All lengths are u32 unless stated. There is no padding and
no optional field without an explicit tag byte. Encoders reject payloads
over 16 MiB with an error that names the offending length and the limit.

### 7.1 Message frame (wire and simulated packet payload)

```text
offset  size  field
0       4     magic: ASCII "SRKV" (0x53 0x52 0x4B 0x56)
4       2     version: u16, value 1
6       1     kind: u8 (table below)
7       1     flags: u8, must be 0 in version 1, nonzero is a reject
8       8     term: u64 (sender current_term; for pre-vote, the hypothetical term)
16      8     from: u64 NodeId
24      8     to: u64 NodeId
32      4     body_len: u32
36      ..    body
end-4   4     crc32: u32, CRC-32 IEEE over every preceding byte, domain
              prefix "SRKV/frame/v1" prepended to the CRC input only
```

Message kinds: 1 RequestVote, 2 RequestVoteResponse, 3 RequestPreVote,
4 RequestPreVoteResponse, 5 AppendEntries, 6 AppendEntriesResponse,
7 InstallSnapshot, 8 InstallSnapshotResponse.

Bodies (in field order, no tags except where noted):

- RequestVote and RequestPreVote: last_log_index u64, last_log_term u64.
- RequestVoteResponse and RequestPreVoteResponse: vote_granted u8 (0 or 1).
- AppendEntries: prev_log_index u64, prev_log_term u64, leader_commit u64,
  read_round u64, entry_count u32, then that many encoded log entries
  (section 7.2).
- AppendEntriesResponse: success u8, match_index u64, conflict_index u64,
  conflict_term u64 (0 when the follower log was simply too short),
  read_round u64 (echo of the highest read round the follower has seen
  from this leader in this term).
- InstallSnapshot: snapshot body (section 7.4) chunk fields:
  last_included_index u64, last_included_term u64, config (section 7.2),
  offset u64, chunk_len u32, chunk bytes, done u8.
- InstallSnapshotResponse: match_index u64 (the snapshot last included
  index once installed, or the follower's current contiguous offset base
  while chunks are in flight).

### 7.2 Log entry and config encoding

```text
term: u64
kind: u8   0 = NoOp, 1 = Command, 2 = Config
Command body:  client_id u64, seq u64, op_kind u8 (0 = Put, 1 = Delete),
               key_len u32, key bytes, value_len u32, value bytes
               (value_len present only for Put)
Config body:   voter_count u32, voter NodeIds u64 each in ascending order,
               learner_count u32, learner NodeIds u64 each ascending
```

Ascending order is part of the encoding contract: two encodings of the same
config compare byte equal, which the state digest and the trace rely on.

Command result encoding (the cached response stored per session, used in
the snapshot format in section 7.4 and wherever a command outcome is
encoded):

```text
tag: u8   0 = Ack, 1 = Value, 2 = Error
Ack:      no payload. The Put or Delete applied successfully.
Value:    value_len u32, value bytes. A read-back value carried as a
          command outcome (reserved for read-modify commands; plain Put
          and Delete always produce Ack).
Error:    error_code u32, message_len u32, message bytes (UTF-8). The
          command was rejected without applying; error_code values are
          assigned in simraft-kv::command and never reused.
```

Any other tag value is a decode error naming the tag and its byte offset.

### 7.3 Durable storage records (simulator disk and future file adapter)

Hard state record: magic "SRHS" (4 bytes), version u16 = 1, record_len u32,
current_term u64, voted_for tag u8 (0 = none, 1 = some) plus NodeId u64 when
some, crc32 u32 over all preceding bytes with domain prefix "SRKV/hard/v1".

Log record: magic "SRLG", version u16 = 1, record_len u32, index u64, then
the section 7.2 entry encoding, then crc32 u32 with domain prefix
"SRKV/log/v1". A record whose CRC fails, whose magic is unknown, or whose
length runs past the end of the durable region is a torn write: recovery
truncates the log at the first bad record and reports the byte offset, the
index, and the expected versus actual CRC in the recovery error.

### 7.4 Snapshot format

Snapshot metadata travels in the core as SnapshotMeta { last_included_index,
last_included_term, config }. The snapshot byte payload layout is:

```text
0     4     magic: ASCII "SRSN"
4     2     version: u16 = 1
6     8     last_included_index u64
14    8     last_included_term u64
22    ..    config (section 7.2 Config body)
..    8     store_count u64, then per entry: key_len u32, key, value_len u32, value
..    8     session_count u64, then per session: client_id u64, last_seq u64,
            response (section 7.2 Command result encoding: tag u8, where
            0 = Ack with no payload, 1 = Value with value_len u32 plus
            value bytes, 2 = Error with error_code u32, message_len u32,
            plus message bytes)
end-32 32   SHA-256 digest over all preceding bytes with domain prefix
            "simraft-kv/snapshot/v1" prepended to the hash input
```

Store keys and session client ids are written in ascending order. A digest
mismatch rejects the whole snapshot with an error carrying the expected and
actual digests; a partial snapshot is never installed.

Chunk transfer: the leader splits the payload into chunks of
Config::snapshot_chunk_bytes (default 65,536) sent in strictly increasing
offset order. The follower accepts a chunk only when its offset equals the
number of bytes it has already buffered for that (last_included_index,
last_included_term) pair; any other chunk is rejected and the expected
offset is reported back. A chunk for a different snapshot identity discards
the previous partial buffer and starts the new one, so a leader change mid
transfer cannot livelock: the newest snapshot identity always wins and the
old buffer is dropped exactly once.

When a snapshot is fully received and its digest verifies, the follower
retains the matching log prefix if its log contains an entry at
last_included_index with last_included_term, and discards its entire log
otherwise, then sets its base to the snapshot meta. The leader switches a
follower to InstallSnapshot automatically whenever next_index is at or
below the leader's snapshot base index (INVARIANT N-01).

### 7.5 Digests and domain separation

Every digest in the system names its domain with a prefix string that is
hashed first: "simraft-kv/state-digest/v1" for the final simulator state
digest, "simraft-kv/snapshot/v1" for snapshots, "SRKV/frame/v1",
"SRKV/hard/v1", and "SRKV/log/v1" for the CRC domains above. No digest is
ever computed over bare payload bytes, so a collision between domains
cannot be replayed as validity in another domain.


## 8. Linearizable reads: clock-free ReadIndex

Leader leases are rejected (alternative A3): a lease is a bet on clock
behavior, and this system must stay linearizable under the simulator's
extreme clock jumps. ReadIndex here uses no clock at all.

Protocol:

1. A ReadIndex request reaches the leader. If the leader has not yet
   committed an entry in its current term (in practice, its election NoOp),
   the request is queued, never served. Serving earlier would let a leader
   that does not know the full committed prefix answer from stale state.
2. The leader records read_index = commit_index and allocates the next
   read_round, a u64 that is strictly monotonic within the leader's term
   and restarts at 1 on each new term. It stores a pending read
   { round, read_index, request_id, acks: BTreeSet<NodeId> including self }.
3. The next AppendEntries to each follower (heartbeat if there are no
   entries to send) carries that read_round. A follower echoes the highest
   read_round it has seen from the current leader in its
   AppendEntriesResponse, and only for the leader and term it currently
   accepts.
4. When acks for a round cover a quorum of the current config, every pending
   read at or below that round is confirmed: the leader waits until its own
   last_applied is at least read_index, then answers the read from the KV
   state machine.
5. If the leader steps down, all pending reads fail with a NotLeader error
   that names the term in which they were issued. Rounds from a previous
   term never confirm reads in a later term: the round token is scoped by
   (leader_id, term, round) and responses whose term differs are ignored.

Correlation is not optional decoration. Without the round token, a delayed
heartbeat ack from before a partition could confirm a read issued after
the partition formed, which is exactly the stale-leader read ReadIndex
exists to prevent.

## 9. Membership: single-server changes only

One change at a time, enforced structurally: a leader rejects a new
ProposeConfig while last_config_index is greater than commit_index, that
is, while any earlier config entry is still uncommitted (INVARIANT M-01).
A newly elected leader also refuses config proposals until its current-term
NoOp has committed, so a config change is never the first entry a leader
counts toward commitment.

Supported changes, each a single Config entry:

- AddLearner { node }: the node joins learners, receives replication and
  snapshots, never votes, and does not count toward quorum.
- PromoteVoter { node }: moves a learner that is caught up (its
  match_index is at least the leader's commit_index at proposal time) into
  voters. Promoting a lagging learner is rejected with the learner's
  match_index and the required index in the error.
- RemoveVoter { node }: removes a voter (or a learner via the same path).
  If the leader removes itself, it continues to lead only until that entry
  commits, replicates the commit to the remaining config, then steps down
  and triggers a fresh election in the new config (commit-then-step-down,
  INVARIANT M-03). It never steps down on append: an uncommitted removal
  must leave the cluster led.

Because only one config is ever in flight, every quorum computation uses a
single config: the newest one whose entry exists in the log. Overlapping
majorities from two configs are never required, which is the entire safety
argument for single-server changes (Raft dissertation, chapter 4).

Cold start: a one-node config bootstraps by committing its initial config
entry at index 1 as term 1 leader after a self-election with no peers.
Config validation also rejects any proposed config with zero voters: a
cluster may shrink to one voter, never to none.

## 10. Client sessions and deduplication

Clients are identified by a u64 client_id chosen by the client at session
start, and every proposal carries a per-client seq starting at 1 and
increasing by 1 per new operation. Retries reuse the same (client_id, seq).

Deduplication is two-layered, because a retry can arrive while the original
is still uncommitted:

1. Committed layer. The KV state machine keeps a session table:
   BTreeMap<ClientId, (last_seq, cached_response)>. Only the latest
   sequence per client is stored, so per-client memory is O(1) plus one
   response payload. On apply, an entry whose seq is at or below last_seq
   returns the cached response without re-executing the operation; an entry
   with seq exactly last_seq + 1 executes and replaces the cache entry; a
   gap (seq greater than last_seq + 1) is a protocol error that names the
   client, the expected seq, and the received seq.
2. In-flight layer. Before appending, the leader scans its own log
   backwards from the tail down to commit_index for an uncommitted Command
   entry with the same (client_id, seq). A hit means the retry attaches to
   the existing entry: no second entry is appended, and the retrying
   requester is registered to receive the response when that entry commits
   and applies. The scan is bounded by the uncommitted tail, which the
   config caps (max_uncommitted_entries, default 10,000).

Bounded memory: the session table itself is capped by
Config::max_sessions (default 10,000). Eviction removes the session with
the smallest last-applied activity index, and only sessions whose latest
entry is committed may be evicted. The contract is stated honestly: a
client that retries an evicted (client_id, seq) after eviction may
re-execute. Clients must retry within the retention window; the simulator
tests exercise eviction explicitly (INVARIANT C-03).

Session state is part of the KV snapshot payload (section 7.4), so a node
restored from a snapshot deduplicates exactly as the snapshotted node did.

## 11. Configuration and timing validation

Config::validate rejects, with a ConfigError naming both offending values:

- heartbeat_interval == 0, or min_election_timeout == 0.
- min_election_timeout < 10 * heartbeat_interval (INVARIANT T-01).
- max_election_timeout < 2 * min_election_timeout.
- max_election_timeout <= min_election_timeout.
- Any zero limit: max_entries_per_append, max_append_bytes,
  snapshot_chunk_bytes, max_sessions, max_uncommitted_entries.

Defaults: heartbeat_interval 50 ticks, min_election_timeout 500 ticks,
max_election_timeout 1,000 ticks. Election deadlines are drawn by the
driver-visible formula deadline = last_leader_contact + min_timeout +
offset, where offset is deterministic per (seed, node_id, election_epoch)
and bounded by (max - min). The healing phase of the simulator guarantees
network latency below min_election_timeout / 4, so a healthy leader's
heartbeats always land well inside every follower's window.

Safety does not depend on these ratios; liveness does. The ratios exist so
that a correct cluster is also a stable one under the fault matrix.

## 12. The simulator (simraft-sim)

### 12.1 Virtual clock and event queue

The simulator is single-threaded. Its event queue is a priority queue
ordered by (delivery_tick, insertion_seq), where insertion_seq is a u64
counter incremented on every enqueue. Ties are impossible, so execution
order is a pure function of the seed. When no packet is due, the clock
jumps directly to the earliest of: the next packet delivery, the next node
poll_timeout deadline, or the next scheduled fault. There is no
millisecond stepping and no wall clock anywhere in the loop.

One seeded PRNG (section 3.3, rng module) drives timer offsets, packet
latency selection, fault decisions, and workload generation, in a fixed
call order. The PRNG algorithm is implemented in the crate (SplitMix64
expansion of the seed into an xoshiro256starstar state) rather than taken
from an external rand crate, because external crates do not promise cross
version stream stability and a changed stream would silently change every
seed's meaning.

### 12.2 Network model

Packets carry (from, to, deliver_tick, frame bytes) and are encoded and
decoded through the section 7.1 frame format, so the simulator tests the
real wire codec, not structured shortcuts. Faults, all seeded:

- Drop with configured probability, per packet.
- Duplicate with configured probability (the copy gets a later delivery).
- Reorder, emergent from per-packet latency drawn from a range, plus an
  explicit hold-and-release fault that delays one selected packet past its
  successors (this is what exercises stale match_index handling).
- Directed partitions: a partition matrix over ordered pairs denies
  delivery A to B independently of B to A. The chaos matrix exercises
  one-way cuts (A->B only), chains (A->B, B->C, A cut from C both ways),
  and full isolation of one node, then heals all of them.

### 12.3 Disk model

Each node has a SimDisk with two regions: a volatile buffer (writes land
here first) and a durable region (writes move here only on an explicit
fsync action issued by the driver when it executes Action::Persist kinds
through to completion and returns Event::Persisted). A crash fault drops
the volatile buffer, kills the node, and restarts it later through
Event::Restored carrying only durable state. This models the real failure
that matters: a node that answered as if it had persisted, then lost the
write. The core's persist-before-response rule (INVARIANT T-02) is the
defense, and the crash-during-append seeds are its test.

### 12.4 Phases: chaos, then bounded healing

Every long run has a chaos phase (faults on, crashes allowed, clock jumps
allowed, including single jumps larger than max_election_timeout) followed
by a healing phase of a fixed tick budget in which: all partitions are
removed, packet latency is bounded below min_election_timeout / 4, no
crashes occur, and no packets are dropped. Healing assertions: exactly one
stable leader emerges and stays leader for the remainder of the phase, all
live nodes converge to identical logs and identical state digests, and the
recorded client history passes the linearizability check (INVARIANT H-02).

### 12.5 Oracles: checks on every tick

After every event delivery and before the next one, the harness runs the
oracle set over all nodes: Election Safety, Leader Append-Only, Log
Matching, Leader Completeness, and State Machine Safety (INVARIANTS S-01
to S-05), plus structural assertions: commit_index never decreases,
last_applied never exceeds commit_index, last_applied never decreases, and
match_index never decreases on a leader. An oracle failure stops the run
and prints the seed, the tick, the event index, and the two conflicting
states, which is enough to replay the failure deterministically.

### 12.6 Linearizability checker (liz)

Client operations are recorded as a history of invoke and return events
with virtual tick timestamps, client id, op, and result. Operations that
never return are recorded as pending.

The checker implements the Wing and Gong backtracking search, partitioned
by key: a history over many keys is linearizable if and only if every
per-key sub-history is, and each per-key model state is a single
Option<Value>, which keeps the model small and exact. Within a partition
the search maintains the set of linearized operations as a sorted bitset
and memoizes visited (linearized_set, model_state) pairs in a BTreeMap;
a revisited pair is pruned immediately. Pending operations are tried both
ways, linearized at a legal point or dropped, matching the standard
treatment of incomplete histories. Results are three-valued: Linearizable,
NotLinearizable (with the key and a minimal offending prefix), or Unknown
only when a configurable step budget is exhausted, and a CI run treats
Unknown as a failure, never as a pass.

## 13. Error model

All domain errors implement std::error::Error and Display, and all carry
concrete diagnostic context. Examples of the contract:

- ConfigError names the field, the received value, the required bound, and
  the conflicting field value (for example, both timeout values).
- DecodeError names the byte offset, the field being decoded, the expected
  length, and the actual remaining length, or the expected and actual CRC
  or digest values.
- ProtocolError for session gaps names the client_id, the expected seq,
  and the received seq. For membership it names the uncommitted
  last_config_index and the current commit_index.
- SnapshotError names the snapshot identity (last included index and term),
  the expected chunk offset, and the received chunk offset.

No error is a bare variant name. A test asserts the diagnostic fields of
each error type (INVARIANT E-01 in docs/INVARIANTS.md).

## 14. Rejected alternatives

### A1. An async, thread-per-node core (tokio or std threads inside the core)

Rejected. Threads and async scheduling reintroduce exactly the
nondeterminism this project exists to remove: interleavings depend on OS
scheduling, timers depend on the wall clock, and a failing execution cannot
be replayed from a seed. It also splits testing into two worlds, mock-based
unit tests of pieces and unrepeatable integration tests of the whole.
Failure mode consequence: the rare bug, the one that needs a specific
partition plus a specific reorder plus a crash, is unreachable in testing.
The sans-I/O step function makes that execution an ordinary input sequence.

### A2. Joint consensus for membership changes

Rejected in favor of single-server changes. Joint consensus keeps two
configs live at once, requires quorums of both, doubles the config state
every other rule must consult (elections, commit counting, ReadIndex
quorums, snapshot metadata), and has a long tail of edge cases around
overlapping changes. Single-server changes reduce every quorum computation
to one config and make the rule "one uncommitted change at a time"
checkable with a single integer comparison. The cost is real but small:
membership changes serialize, and changing more than the quorum-safe single
step takes several committed entries. For a system whose membership changes
are rare administrative acts, simplicity and auditability win.

### A3. Leader lease reads

Rejected. A lease read is correct only if the leader's clock and every
follower's clock stay within stated drift bounds. The simulator's fault
model includes clock jumps larger than any lease, because real machines
pause for GC, suspend, and step their clocks. Under a jump, a deposed
leader with an unexpired local lease serves stale reads and linearizability
is gone with no error anywhere. ReadIndex confirms leadership with a quorum
round and a monotonic round token, so its correctness depends on message
accounting only, never on clock behavior. The cost is one heartbeat round
per read batch, which the benchmarks measure (docs/INVARIANTS.md, L-01).

### A4. Log storage as a persistent tree or per-entry heap nodes (BTreeMap index to entry)

Rejected. Raft log access is overwhelmingly sequential: append at the tail,
scan batches for replication, truncate a suffix, copy a prefix into a
snapshot. A Vec with a snapshot base offset does all of these with
contiguous memory, one allocation amortized over many entries, and binary
search for term boundaries. A BTreeMap per-entry representation pays a heap
allocation and pointer chasing per entry, fragments cache lines across the
replication hot path, and buys a capability the protocol never uses: there
is no random insert into the middle of a Raft log, ever. Failure mode
consequence of the tree design: snapshot and truncation become per-entry
operations with per-entry failure surfaces instead of single range moves.

### A5. Adopting an existing Raft crate (raft-rs, openraft) as the core

Rejected for this repository's purpose. The deliverable is a consensus core
whose every rule can be audited against docs/INVARIANTS.md and simulated
deterministically. Existing crates bring their own storage, timer, and
executor assumptions, several are async-first (see A1), and their internal
state (Ready patterns, raw node wrappers) does not match the Event and
Action protocol this design is built around. Wrapping one would make the
simulator test the wrapper, not the protocol, and would import behavior
(for example, lease reads or joint consensus defaults) that this design
has explicitly decided against. The algorithms here follow the Raft paper
and dissertation directly, and every rule cites its invariant.

### A6. Generic serialization (serde plus bincode or JSON) for wire and disk formats

Rejected for the canonical formats. A derived serializer's byte layout is an
implementation detail of a crate version: field ordering, integer encoding,
and enum tags can change between versions, which would silently change the
meaning of existing durable bytes and break cross-process digest equality.
The hand-specified layouts in section 7 fix endianness, field widths, tag
values, and sort order as protocol constants, versioned by the format
version field. serde_json is still used where it belongs: the simulator
CLI's human-facing JSON output, where layout is presentation, not protocol.

## 15. Verification strategy (summary)

docs/INVARIANTS.md is the normative matrix. In outline:

- Property tests (proptest, PROPTEST_CASES scaled by CI: 1,000 per PR,
  10,000 on main) over core transitions: vote granting, log matching under
  appends and truncations, commit advancement, session dedup, config rules.
- Fault oracles in the simulator, asserted every tick (section 12.5).
- Seed sweeps: the simraft-sim CLI run over the CI seed ranges, checking
  exit status and the final digest line.
- Tamper tests: byte flips in frames, log records, and snapshots must be
  caught by CRC or digest at the specified offsets, never silently accepted.
- Determinism tests: the same seed run in two OS processes must produce
  byte-identical trace files and state digests (INVARIANT D-01).
- Allocation guard: benchmarks run under a counting allocator in the sim
  crate; Node::step has a measured allocation budget per event class, and a
  regression past the budget fails the benchmark gate (INVARIANT L-03).
- Benchmarks (criterion, inputs and outputs wrapped in black_box, throughput
  configured per group): step latency by event class, replication throughput
  in entries per second, snapshot install time by payload size, and checker
  time by history length.

## 16. Milestone map

| Milestone | Delivers | Architecture sections |
| --- | --- | --- |
| M1 | Roles, Pre-Vote, elections, AppendEntries, conflict fast backup, monotonic match_index, config validation | 4, 5, 6.1, 6.2, 11 |
| M2 | Figure 8 commit, ReadIndex, KV state machine, session deduplication | 6.3, 8, 10 |
| M3 | Snapshots, chunked InstallSnapshot, single-server membership | 7.4, 9, plus log truncation in 5.2 |
| M4 | Simulator, oracles, linearizability checker, CLI, seed sweeps in CI, README and CONTRIBUTING | 12, 13, 15 |

Status at the M1 gate: M1 is implemented as described (simraft-core
protocol plus config validation; simraft-kv and simraft-sim are the
documented placeholders their milestones fill). The Figure 8
current-term-only commitment rule is implemented in the core from M1
because replication correctness depends on it; its dedicated property
suite and the ReadIndex machinery that builds on it land at M2 as
scheduled.

Status at the M2 gate: M2 is implemented as described. The ReadIndex
machinery of section 8 runs as specified, with two as-built notes: a
follower remembers the highest round token it has seen from its
current leader and echoes that value on every successful response (a
response to any append can therefore confirm a round), and a leader
that steps down mid-round rejects the batched reads rather than
letting them hang. The in-flight half of section 10 dedup is
implemented with an index from (client, seq) to log position covering
exactly the uncommitted tail, which is the efficient equivalent of
the uncommitted-tail scan the section describes; the committed half
lives in simraft-kv's session table as specified, including the
eviction contract. The Figure 8 suite of section 6.3 now enforces
the rule that landed at M1.

Each implementation milestone runs in two stages. Stage A lands public
interfaces, doc comments, structural assertions, and property or fault tests
that fail because the logic is unimplemented (todo and unimplemented are
denied by workspace lints outside the test scaffolding allowance, so Stage A
uses explicit failing assertions, not stub macros, in shipped code paths).
Stage B implements the minimal design that passes those tests, then syncs
this document and docs/INVARIANTS.md to the implementation as built.

## 17. References

- Ongaro and Ousterhout, In Search of an Understandable Consensus Algorithm,
  USENIX ATC 2014 (the Raft paper; Figure 8 and sections 5.4, 6.4).
- Ongaro, Consensus: Bridging Theory and Practice, Stanford dissertation
  2014 (chapter 3 leader election, chapter 4 membership changes by single
  server change, chapter 6 ReadIndex, section 9.6 Pre-Vote).
- The sans-I/O protocol pattern: protocol implementations as pure state
  machines over byte or message events, with all effects owned by the
  caller (sans-io community documentation; the pattern as used by quinn,
  h2, and Raft implementations in the etcd and tikv ecosystems).
- Will Wilson and the FoundationDB simulation testing approach: deterministic
  single-threaded simulation of a distributed system with seeded faults
  (FoundationDB testing documentation; the buggify fault-injection idea).
- TigerBeetle VOPR: deterministic simulation with explicit chaos and healing
  style phases and seed-only reproduction.
- Wing and Gong, linearizability checking by backtracking search; Porcupine
  (Anish Athalye) for per-key partitioning and visited-state memoization
  practice.
- Jepsen and Knossos: history-based verification of distributed key-value
  stores under partitions and crashes.
