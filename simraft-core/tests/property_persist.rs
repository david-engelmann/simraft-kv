//! Persist-before-response ordering (invariant T-02): no vote, append,
//! or replication response is emitted before the durable write it
//! depends on has been acknowledged.

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use simraft_core::{
    Action, ClientId, ClusterConfig, EntryKind, Event, LogEntry, LogIndex, Message, Node, NodeId,
    PersistKind, SnapshotMeta, Term,
};

fn sends(actions: &[Action]) -> Vec<&Action> {
    actions
        .iter()
        .filter(|a| matches!(a, Action::Send { .. }))
        .collect()
}

#[test]
fn no_response_before_persist_ack() {
    vote_response_waits_for_persist();
    append_response_waits_for_persist();
}

fn vote_response_waits_for_persist() {
    let mut voter = Node::new(
        NodeId(2),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        17,
    )
    .unwrap();
    let actions = voter.step(Event::Message {
        from: NodeId(1),
        msg: Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        },
    });
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::PersistHardState { .. })),
        "granting a vote must persist hard state, got {actions:?}"
    );
    assert!(
        sends(&actions).is_empty(),
        "no vote response may be sent before the persist ack, got {actions:?}"
    );
    let released = voter.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex(0),
    });
    assert!(
        released.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestVoteResponse {
                    vote_granted: true,
                    ..
                },
                ..
            }
        )),
        "the persist ack must release the vote response, got {released:?}"
    );
}

fn append_response_waits_for_persist() {
    let mut follower = Node::new(
        NodeId(3),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        19,
    )
    .unwrap();
    let entry = LogEntry {
        term: Term(1),
        kind: EntryKind::Command {
            client_id: ClientId(1),
            seq: 1,
            payload: b"set".to_vec(),
        },
    };
    let actions = follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(1),
            prev_log_index: LogIndex(0),
            prev_log_term: Term(0),
            entries: vec![entry],
            leader_commit: LogIndex(0),
            read_round: 0,
        },
    });
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::PersistEntries { .. } | Action::PersistHardState { .. }
        )),
        "accepting entries must persist first, got {actions:?}"
    );
    assert!(
        sends(&actions).is_empty(),
        "no append response may be sent before the entries are durable, got {actions:?}"
    );
    // Acknowledge hard state, then the entries; the response follows the
    // entries ack (acknowledging hard state alone is not enough).
    let _ = follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex(0),
    });
    let released = follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    assert!(
        released.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::AppendEntriesResponse {
                    success: true,
                    match_index: LogIndex(1),
                    ..
                },
                ..
            }
        )),
        "the entries ack must release the append response, got {released:?}"
    );
}

#[test]
fn leader_does_not_advertise_unpersisted_entries() {
    let mut leader = Node::new(
        NodeId(1),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        23,
    )
    .unwrap();
    let _ = common::elect(&mut leader, &[NodeId(2), NodeId(3)]);
    // Acknowledge the election NoOp persist; only then may appends flow.
    let _ = leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let actions = leader.step(Event::Propose {
        client_id: ClientId(4),
        seq: 1,
        payload: b"v".to_vec(),
    });
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::PersistEntries { .. })),
        "a proposal must persist the new entry, got {actions:?}"
    );
    assert!(
        sends(&actions).is_empty(),
        "the leader must not send AppendEntries for an entry that is not yet durable, got {actions:?}"
    );
}

fn cmd_entry(term: u64, seq: u64) -> LogEntry {
    LogEntry {
        term: Term(term),
        kind: EntryKind::Command {
            client_id: ClientId(9),
            seq,
            payload: vec![b'x'],
        },
    }
}

#[test]
fn truncation_and_snapshot_clamp_durable_watermark_and_ignore_stale_acks() {
    // Subcase A: Term-1 entries 1..=6 are already persisted, then Term-2
    // truncates 2..=6 and appends entry 2 in Term 2. AppendEntriesResponse
    // must NOT fire until the new PersistEntries ack arrives.
    let mut follower = Node::new(
        NodeId(3),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        29,
    )
    .unwrap();
    let t1_entries: Vec<LogEntry> = (1..=6).map(|s| cmd_entry(1, s)).collect();
    let _ = follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::ZERO,
            entries: t1_entries.clone(),
            leader_commit: LogIndex::ZERO,
            read_round: 0,
        },
    });
    let _ = follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    let _ = follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(6),
    });

    let t2_actions = follower.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntries {
            term: Term(2),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            entries: vec![cmd_entry(2, 7)],
            leader_commit: LogIndex::ZERO,
            read_round: 0,
        },
    });
    assert_eq!(sends(&t2_actions).len(), 0);
    let after_hard = follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    assert!(
        sends(&after_hard).is_empty(),
        "T-02: truncating 2..=6 must clamp entries_durable_up_to so HardState ack alone cannot release AppendEntriesResponse: {after_hard:?}"
    );
    let after_entries = follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });
    assert!(
        after_entries.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::AppendEntriesResponse {
                    term: Term(2),
                    success: true,
                    match_index: LogIndex(2),
                    ..
                },
                ..
            }
        )),
        "T-02: post-truncation PersistEntries ack must release AppendEntriesResponse: {after_entries:?}"
    );

    assert_stale_pre_truncation_ack_ignored(t1_entries);
}

fn assert_stale_pre_truncation_ack_ignored(t1_entries: Vec<LogEntry>) {
    // Subcase B: Term-1 PersistEntries(1..=6) is still in flight when Term-2
    // truncates 2..=6; the stale pre-truncation PersistEntries(6) ack must not
    // prematurely release Term-2 AppendEntriesResponse.
    let mut async_follower = Node::new(
        NodeId(3),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        31,
    )
    .unwrap();
    let _ = async_follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::ZERO,
            entries: t1_entries,
            leader_commit: LogIndex::ZERO,
            read_round: 0,
        },
    });
    let _ = async_follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    let _ = async_follower.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntries {
            term: Term(2),
            prev_log_index: LogIndex(1),
            prev_log_term: Term(1),
            entries: vec![cmd_entry(2, 8)],
            leader_commit: LogIndex::ZERO,
            read_round: 0,
        },
    });
    let _ = async_follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    let stale_ack = async_follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(6),
    });
    assert!(
        sends(&stale_ack).is_empty(),
        "T-02: pre-truncation PersistEntries(6) ack must not release Term-2 AppendEntriesResponse: {stale_ack:?}"
    );
    let fresh_ack = async_follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });
    assert_eq!(sends(&fresh_ack).len(), 1);
}

#[test]
fn divergent_snapshot_install_clamps_durable_watermark() {
    let t1_entries: Vec<LogEntry> = (1..=6).map(|s| cmd_entry(1, s)).collect();
    let mut snap_follower = Node::new(
        NodeId(3),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        37,
    )
    .unwrap();
    let _ = snap_follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::ZERO,
            entries: t1_entries,
            leader_commit: LogIndex::ZERO,
            read_round: 0,
        },
    });
    let _ = snap_follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    let _ = snap_follower.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(6),
    });
    let meta = SnapshotMeta {
        last_included_index: LogIndex(3),
        last_included_term: Term(2),
        config: common::all_voters(&[1, 2, 3]),
    };
    let _ = snap_follower.step(Event::Message {
        from: NodeId(2),
        msg: Message::InstallSnapshot {
            term: Term(2),
            last_included_index: meta.last_included_index,
            last_included_term: meta.last_included_term,
            config: meta.config.clone(),
            offset: 0,
            data: b"snap".to_vec(),
            done: true,
        },
    });
    let _ = snap_follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    let _ = snap_follower.step(Event::SnapshotPersisted { meta });
    let post_snap = snap_follower.step(Event::Message {
        from: NodeId(2),
        msg: Message::AppendEntries {
            term: Term(2),
            prev_log_index: LogIndex(3),
            prev_log_term: Term(2),
            entries: vec![cmd_entry(2, 9)],
            leader_commit: LogIndex(3),
            read_round: 0,
        },
    });
    assert!(
        sends(&post_snap).is_empty(),
        "T-02: post-snapshot entry 4 must wait for PersistEntries rather than reusing pre-snapshot watermark 6: {post_snap:?}"
    );
}

#[test]
fn non_voter_and_snapshot_higher_term_persist_before_response() {
    let cfg = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], [NodeId(4)]).unwrap();
    let mut learner = Node::new(NodeId(4), simraft_core::Config::default(), cfg, 41).unwrap();
    let vote_actions = learner.step(Event::Message {
        from: NodeId(1),
        msg: Message::RequestVote {
            term: Term(3),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        },
    });
    assert!(
        vote_actions.iter().any(|a| matches!(
            a,
            Action::PersistHardState {
                current_term: Term(3),
                voted_for: None,
            }
        )),
        "T-02: learner advancing term on RequestVote must emit PersistHardState: {vote_actions:?}"
    );
    assert!(
        sends(&vote_actions).is_empty(),
        "T-02: learner must hold RequestVoteResponse until PersistHardState ack: {vote_actions:?}"
    );
    let vote_released = learner.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    assert!(vote_released.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: Message::RequestVoteResponse {
                term: Term(3),
                vote_granted: false,
            },
            ..
        }
    )));

    let mut follower = Node::new(
        NodeId(2),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        43,
    )
    .unwrap();
    let meta = SnapshotMeta {
        last_included_index: LogIndex(5),
        last_included_term: Term(4),
        config: common::all_voters(&[1, 2, 3]),
    };
    let snap_actions = follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::InstallSnapshot {
            term: Term(4),
            last_included_index: meta.last_included_index,
            last_included_term: meta.last_included_term,
            config: meta.config,
            offset: 0,
            data: b"part".to_vec(),
            done: false,
        },
    });
    assert!(
        snap_actions.iter().any(|a| matches!(
            a,
            Action::PersistHardState {
                current_term: Term(4),
                ..
            }
        )),
        "T-02: InstallSnapshot at higher term must persist hard state: {snap_actions:?}"
    );
    assert!(
        sends(&snap_actions).is_empty(),
        "T-02: InstallSnapshotResponse must wait for PersistHardState ack: {snap_actions:?}"
    );
    let snap_released = follower.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    assert!(snap_released.iter().any(|a| matches!(
        a,
        Action::Send {
            msg: Message::InstallSnapshotResponse { term: Term(4), .. },
            ..
        }
    )));
}
