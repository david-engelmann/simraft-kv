//! Snapshot property tests (docs/ARCHITECTURE.md section 7.4,
//! invariants N-01, N-02, N-04, N-05 in docs/INVARIANTS.md).
//!
//! Chunked `InstallSnapshot`: automatic fallback at the base
//! boundary, strictly sequential chunk offsets, snapshot identity
//! replacement, the prefix retention rule on install, and compaction
//! that never crosses the commit boundary.

mod common;

use common::{all_voters, elect};
use simraft_core::{
    Action, ClientId, ClusterConfig, Config, EntryKind, Event, HardState, LogEntry, LogIndex,
    Message, Node, NodeId, PersistKind, SnapshotMeta, Term,
};

fn node(id: u64, cluster: &ClusterConfig, seed: u64) -> Node {
    Node::new(NodeId(id), Config::default(), cluster.clone(), seed ^ id).expect("config validates")
}

fn meta(index: u64, term: u64, config: &ClusterConfig) -> SnapshotMeta {
    SnapshotMeta {
        last_included_index: LogIndex(index),
        last_included_term: Term(term),
        config: config.clone(),
    }
}

fn chunk(meta: &SnapshotMeta, offset: u64, data: &[u8], done: bool) -> Event {
    Event::Message {
        from: NodeId(1),
        msg: Message::InstallSnapshot {
            term: meta.last_included_term,
            last_included_index: meta.last_included_index,
            last_included_term: meta.last_included_term,
            config: meta.config.clone(),
            offset,
            data: data.to_vec(),
            done,
        },
    }
}

fn persist_snapshots(actions: &[Action]) -> Vec<&Action> {
    actions
        .iter()
        .filter(|a| matches!(a, Action::PersistSnapshot { .. }))
        .collect()
}

fn append_response(from: u64, term: Term, match_index: u64) -> Event {
    Event::Message {
        from: NodeId(from),
        msg: Message::AppendEntriesResponse {
            term,
            success: true,
            match_index: LogIndex(match_index),
            conflict_index: LogIndex::ZERO,
            conflict_term: Term::ZERO,
            read_round: 0,
        },
    }
}

#[test]
fn leader_switches_to_snapshot_at_base_boundary() {
    let cfg = all_voters(&[1, 2, 3]);
    let mut leader = node(1, &cfg, 7);
    elect(&mut leader, &[NodeId(2), NodeId(3)]);
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let term = leader.current_term();
    // Commit entries through index 4 on the leader and node 2; node 3
    // never answers, so its progress stays at the election default.
    for seq in 1..=3_u64 {
        leader.step(Event::Propose {
            client_id: ClientId(1),
            seq,
            payload: vec![u8::try_from(seq).unwrap_or(0)],
        });
        leader.step(Event::Persisted {
            kind: PersistKind::Entries,
            up_to_index: leader.log().last_index(),
        });
    }
    leader.step(append_response(2, term, 4));
    assert_eq!(leader.commit_index(), LogIndex(4));

    // The leader's own snapshot lands: its base advances to 4.
    let snap = meta(4, term.0, &cfg);
    leader.step(Event::SnapshotLoaded {
        meta: snap.clone(),
        data: vec![9; 32],
    });
    leader.step(Event::SnapshotPersisted { meta: snap });
    assert_eq!(leader.log().base_index(), LogIndex(4));

    // The next sends to node 3, whose next index (2) is at or below
    // the base, must be InstallSnapshot chunks, not AppendEntries.
    let actions = leader.step(Event::Tick { now: 10_000 });
    let to_three: Vec<&Message> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { to, msg } if *to == NodeId(3) => Some(msg),
            _ => None,
        })
        .collect();
    assert!(
        to_three
            .iter()
            .any(|m| matches!(m, Message::InstallSnapshot { .. })),
        "N-01: a follower at the base boundary gets InstallSnapshot, got {to_three:?}"
    );
    assert!(
        !to_three
            .iter()
            .any(|m| matches!(m, Message::AppendEntries { .. })),
        "N-01: no AppendEntries may be sent below the base, got {to_three:?}"
    );
}

#[test]
fn chunks_require_exact_sequential_offsets() {
    let cfg = all_voters(&[1, 2, 3]);
    let mut follower = node(2, &cfg, 7);
    let snap = meta(3, 1, &cfg);

    follower.step(chunk(&snap, 0, &[1, 2, 3], false));
    // A gapped chunk is rejected and changes nothing.
    follower.step(chunk(&snap, 99, &[9], false));
    let actions = follower.step(chunk(&snap, 3, &[4, 5], true));
    let persists = persist_snapshots(&actions);
    assert_eq!(
        persists.len(),
        1,
        "exactly one snapshot is assembled, got {actions:?}"
    );
    match persists[0] {
        Action::PersistSnapshot { meta: m, data } => {
            assert_eq!(*m, snap);
            assert_eq!(
                data,
                &[1, 2, 3, 4, 5],
                "the gapped chunk contributed nothing"
            );
        }
        _ => unreachable!(),
    }

    // Only the persist acknowledgment installs it.
    assert_eq!(follower.log().base_index(), LogIndex::ZERO);
    follower.step(Event::SnapshotPersisted { meta: snap });
    assert_eq!(follower.log().base_index(), LogIndex(3));
    assert_eq!(follower.commit_index(), LogIndex(3));
    assert_eq!(follower.last_applied(), LogIndex(3));
}

#[test]
fn new_snapshot_identity_discards_stale_buffer() {
    let cfg = all_voters(&[1, 2, 3]);
    let mut follower = node(2, &cfg, 7);
    let stale = meta(5, 1, &cfg);
    let fresh = meta(6, 1, &cfg);

    follower.step(chunk(&stale, 0, &[1, 1], false));
    follower.step(chunk(&fresh, 0, &[2, 2], false));
    let actions = follower.step(chunk(&fresh, 2, &[3], true));
    let persists = persist_snapshots(&actions);
    assert_eq!(
        persists.len(),
        1,
        "the fresh snapshot assembles, got {actions:?}"
    );
    match persists[0] {
        Action::PersistSnapshot { meta: m, data } => {
            assert_eq!(*m, fresh);
            assert_eq!(data, &[2, 2, 3], "no bytes from the stale buffer survive");
        }
        _ => unreachable!(),
    }
}

#[test]
fn matching_prefix_retained_divergent_log_discarded() {
    let cfg = all_voters(&[1, 2, 3]);
    let entries = |terms: &[u64]| -> Vec<LogEntry> {
        terms
            .iter()
            .map(|&t| LogEntry {
                term: Term(t),
                kind: EntryKind::NoOp,
            })
            .collect()
    };
    let restored = |terms: &[u64]| -> Node {
        let mut n = node(2, &cfg, 7);
        n.step(Event::Restored {
            hard_state: HardState {
                current_term: Term(2),
                voted_for: None,
            },
            entries: entries(terms),
        });
        n
    };
    let snap = meta(3, 2, &cfg);

    // Entry at index 3 carries term 2, matching the snapshot: the
    // suffix after index 3 is retained.
    let mut matching = restored(&[1, 1, 2, 2, 2]);
    matching.step(chunk(&snap, 0, &[7; 8], true));
    matching.step(Event::SnapshotPersisted { meta: snap.clone() });
    assert_eq!(matching.log().base_index(), LogIndex(3));
    assert_eq!(
        matching.log().last_index(),
        LogIndex(5),
        "N-04: the matching suffix is retained"
    );
    assert!(matching.log().entry_at(LogIndex(4)).is_some());

    // Entry at index 3 carries term 1: the whole log is discarded.
    let mut divergent = restored(&[1, 1, 1, 2, 2]);
    divergent.step(chunk(&snap, 0, &[7; 8], true));
    divergent.step(Event::SnapshotPersisted { meta: snap });
    assert_eq!(divergent.log().base_index(), LogIndex(3));
    assert_eq!(
        divergent.log().last_index(),
        LogIndex(3),
        "N-04: a divergent log is discarded whole"
    );
    assert!(divergent.log().entries().is_empty());
}

#[test]
fn compaction_never_touches_committed_or_uncommitted_boundary() {
    let cfg = all_voters(&[1, 2, 3]);
    let mut leader = node(1, &cfg, 7);
    elect(&mut leader, &[NodeId(2), NodeId(3)]);
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let term = leader.current_term();
    leader.step(append_response(2, term, 1));
    leader.step(append_response(3, term, 1));
    for seq in 1..=2_u64 {
        leader.step(Event::Propose {
            client_id: ClientId(1),
            seq,
            payload: vec![u8::try_from(seq).unwrap_or(0)],
        });
        leader.step(Event::Persisted {
            kind: PersistKind::Entries,
            up_to_index: leader.log().last_index(),
        });
    }
    leader.step(append_response(2, term, 3));
    leader.step(append_response(3, term, 3));
    assert_eq!(leader.commit_index(), LogIndex(3));
    // Two more entries sit uncommitted at indexes 4 and 5.
    for seq in 3..=4_u64 {
        leader.step(Event::Propose {
            client_id: ClientId(1),
            seq,
            payload: vec![u8::try_from(seq).unwrap_or(0)],
        });
        leader.step(Event::Persisted {
            kind: PersistKind::Entries,
            up_to_index: leader.log().last_index(),
        });
    }
    assert_eq!(leader.log().last_index(), LogIndex(5));

    // A snapshot claiming index 5 covers uncommitted entries: the
    // base must not advance past the commit index (N-05).
    let too_far = meta(5, term.0, &cfg);
    leader.step(Event::SnapshotLoaded {
        meta: too_far.clone(),
        data: vec![1; 16],
    });
    leader.step(Event::SnapshotPersisted { meta: too_far });
    assert!(
        leader.log().base_index() <= LogIndex(3),
        "N-05: compaction never crosses the commit boundary, base is {}",
        leader.log().base_index()
    );

    // A snapshot at the commit index compacts exactly the committed
    // prefix and keeps the uncommitted tail.
    let at_commit = meta(3, term.0, &cfg);
    leader.step(Event::SnapshotLoaded {
        meta: at_commit.clone(),
        data: vec![2; 16],
    });
    leader.step(Event::SnapshotPersisted { meta: at_commit });
    assert_eq!(leader.log().base_index(), LogIndex(3));
    assert_eq!(
        leader.log().last_index(),
        LogIndex(5),
        "the uncommitted tail survives compaction"
    );
}
