//! Membership property tests (docs/ARCHITECTURE.md section 9,
//! invariants M-01 to M-05 in docs/INVARIANTS.md).
//!
//! Single-server changes only: one uncommitted change at a time, the
//! leader's `NoOp` commits before any change, learners never vote or
//! count, promotion requires a caught-up learner, and every quorum is
//! computed against exactly one configuration, the newest in the log.

#![allow(
    dead_code,
    reason = "the shared test harness is included whole; this suite uses only its free helpers"
)]

mod common;

use common::elect;
use simraft_core::{
    Action, ClientId, ClusterConfig, Config, ConfigChange, ConfigResult, Event, LogIndex, Message,
    Node, NodeId, PersistKind, ProtocolError, Role, Term,
};

fn cluster(voters: &[u64], learners: &[u64]) -> ClusterConfig {
    ClusterConfig::new(
        voters.iter().map(|&i| NodeId(i)),
        learners.iter().map(|&i| NodeId(i)),
    )
    .expect("test cluster is valid")
}

fn node(id: u64, cluster: &ClusterConfig, seed: u64) -> Node {
    Node::new(NodeId(id), Config::default(), cluster.clone(), seed ^ id).expect("config validates")
}

const fn append_response(from: u64, term: Term, match_index: u64) -> Event {
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

fn propose_command(leader: &mut Node, seq: u64) {
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

fn config_responses(actions: &[Action]) -> Vec<ConfigResult> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::ConfigResponse { result, .. } => Some(result.clone()),
            _ => None,
        })
        .collect()
}

/// Elect node 1 in a 3-voter cluster and commit its `NoOp` (index 1).
fn leader_with_committed_noop(cluster: &ClusterConfig) -> Node {
    let mut leader = node(1, cluster, 42);
    elect(&mut leader, &[NodeId(2), NodeId(3)]);
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let term = leader.current_term();
    leader.step(append_response(2, term, 1));
    leader.step(append_response(3, term, 1));
    assert_eq!(leader.commit_index(), LogIndex(1), "NoOp must commit");
    leader
}

#[test]
fn second_config_change_rejected_while_first_uncommitted() {
    let cfg = cluster(&[1, 2, 3, 4, 5], &[]);
    let mut leader = node(1, &cfg, 7);
    elect(&mut leader, &[NodeId(2), NodeId(3)]);
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(1),
    });
    let term = leader.current_term();
    leader.step(append_response(2, term, 1));
    leader.step(append_response(3, term, 1));
    leader.step(append_response(4, term, 1));
    assert_eq!(leader.commit_index(), LogIndex(1));

    // First change: appended at index 2, held uncommitted by giving
    // the leader only one matching follower (quorum of 5 is 3).
    let actions = leader.step(Event::ProposeConfig {
        change: ConfigChange::AddLearner { node: NodeId(6) },
    });
    assert!(
        config_responses(&actions).is_empty(),
        "an accepted change answers only when it commits, got {actions:?}"
    );
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });
    leader.step(append_response(2, term, 2));
    assert_eq!(
        leader.commit_index(),
        LogIndex(1),
        "change must be uncommitted"
    );

    // Second change while the first is uncommitted: rejected, with
    // both indexes named (M-01).
    let actions = leader.step(Event::ProposeConfig {
        change: ConfigChange::AddLearner { node: NodeId(7) },
    });
    assert_eq!(
        config_responses(&actions),
        vec![ConfigResult::Rejected {
            error: ProtocolError::ConfigChangePending {
                last_config_index: LogIndex(2),
                commit_index: LogIndex(1),
            },
        }],
    );
}

#[test]
fn config_rejected_before_leader_noop_commits() {
    let cfg = cluster(&[1, 2, 3], &[]);
    let mut leader = node(1, &cfg, 7);
    elect(&mut leader, &[NodeId(2), NodeId(3)]);
    assert_eq!(leader.commit_index(), LogIndex::ZERO, "NoOp uncommitted");
    let actions = leader.step(Event::ProposeConfig {
        change: ConfigChange::AddLearner { node: NodeId(4) },
    });
    assert_eq!(
        config_responses(&actions),
        vec![ConfigResult::Rejected {
            error: ProtocolError::LeaderNoopUncommitted {
                noop_index: LogIndex(1),
                commit_index: LogIndex::ZERO,
            },
        }],
        "M-02: no config proposal before the election NoOp commits"
    );
}

#[test]
fn learners_never_vote_or_count() {
    let cfg = cluster(&[1, 2, 3], &[4]);

    // A learner never starts an election, however long it is cut off.
    let mut learner = node(4, &cfg, 7);
    for now in [1_000_000, 2_000_000, 10_000_000] {
        let actions = learner.step(Event::Tick { now });
        assert!(
            !actions.iter().any(|a| matches!(a, Action::Send { .. })),
            "a learner must never send election traffic, got {actions:?}"
        );
        assert_eq!(learner.role(), Role::Follower);
    }

    // A learner never grants a vote.
    let mut learner = node(4, &cfg, 7);
    let actions = learner.step(Event::Message {
        from: NodeId(1),
        msg: Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::ZERO,
        },
    });
    let mut released = actions;
    released.extend(learner.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    }));
    assert!(
        !released.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestVoteResponse {
                    vote_granted: true,
                    ..
                },
                ..
            }
        )),
        "a learner must never grant a vote, got {released:?}"
    );

    // A learner's grant never counts toward an election quorum.
    let mut candidate = node(1, &cfg, 7);
    candidate.step(Event::Tick { now: 1_000_000 });
    candidate.step(Event::Message {
        from: NodeId(2),
        msg: Message::RequestPreVoteResponse {
            term: Term::ZERO,
            vote_granted: true,
        },
    });
    candidate.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    assert_eq!(candidate.role(), Role::Candidate);
    candidate.step(Event::Message {
        from: NodeId(4),
        msg: Message::RequestVoteResponse {
            term: candidate.current_term(),
            vote_granted: true,
        },
    });
    assert_eq!(
        candidate.role(),
        Role::Candidate,
        "self vote plus a learner grant is not a quorum of 3 voters"
    );
    candidate.step(Event::Message {
        from: NodeId(2),
        msg: Message::RequestVoteResponse {
            term: candidate.current_term(),
            vote_granted: true,
        },
    });
    assert_eq!(candidate.role(), Role::Leader, "a second voter grant wins");
}

#[test]
fn promotion_requires_caught_up_learner() {
    let cfg = cluster(&[1, 2, 3], &[4]);
    let mut leader = leader_with_committed_noop(&cfg);
    propose_command(&mut leader, 1);
    propose_command(&mut leader, 2);
    let term = leader.current_term();
    leader.step(append_response(2, term, 3));
    leader.step(append_response(3, term, 3));
    assert_eq!(leader.commit_index(), LogIndex(3));

    // The learner has replicated nothing (match index 0).
    let actions = leader.step(Event::ProposeConfig {
        change: ConfigChange::PromoteVoter { node: NodeId(4) },
    });
    assert_eq!(
        config_responses(&actions),
        vec![ConfigResult::Rejected {
            error: ProtocolError::LearnerNotCaughtUp {
                node: NodeId(4),
                match_index: LogIndex::ZERO,
                required: LogIndex(3),
            },
        }],
        "M-04: promotion names the learner's match and the required index"
    );
}

#[test]
fn quorum_uses_latest_single_config() {
    let cfg = cluster(&[1, 2, 3], &[4]);
    let mut leader = leader_with_committed_noop(&cfg);
    let term = leader.current_term();

    // Bring the learner to the commit index first, so the promotion
    // gate (M-04) passes: its match reaches 1, the NoOp's index.
    leader.step(append_response(4, term, 1));

    // Promote the learner: the config entry at index 2 installs a
    // 4-voter configuration (quorum 3) as the latest config.
    leader.step(Event::ProposeConfig {
        change: ConfigChange::PromoteVoter { node: NodeId(4) },
    });
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });
    leader.step(append_response(4, term, 2));
    leader.step(append_response(2, term, 2));
    assert_eq!(leader.commit_index(), LogIndex(2), "config entry commits");

    // A command at index 3 matched by only one other voter of the new
    // config must not commit: the newest config's quorum is 3, even
    // though the old 3-voter quorum was 2.
    propose_command(&mut leader, 1);
    leader.step(append_response(2, term, 3));
    assert_eq!(
        leader.commit_index(),
        LogIndex(2),
        "M-05: commit counting uses the newest config alone"
    );
    leader.step(append_response(4, term, 3));
    assert_eq!(leader.commit_index(), LogIndex(3), "a third match commits");
}

#[test]
fn leader_steps_down_only_after_self_removal_commits() {
    let cfg = cluster(&[1, 2, 3], &[]);
    let mut leader = leader_with_committed_noop(&cfg);
    let term = leader.current_term();

    let actions = leader.step(Event::ProposeConfig {
        change: ConfigChange::RemoveVoter { node: NodeId(1) },
    });
    assert!(
        config_responses(&actions).is_empty(),
        "removal answers at commit, not at append, got {actions:?}"
    );
    assert_eq!(leader.role(), Role::Leader, "M-03: no step-down on append");
    leader.step(Event::Persisted {
        kind: PersistKind::Entries,
        up_to_index: LogIndex(2),
    });
    assert_eq!(
        leader.role(),
        Role::Leader,
        "still leader while uncommitted"
    );

    let mut final_actions = leader.step(append_response(2, term, 2));
    final_actions.extend(leader.step(append_response(3, term, 2)));
    assert_eq!(leader.commit_index(), LogIndex(2));
    assert_eq!(
        leader.role(),
        Role::Follower,
        "M-03: the leader steps down once its removal commits"
    );
    assert!(
        config_responses(&final_actions).contains(&ConfigResult::Committed),
        "the removal proposal answers Committed, got {final_actions:?}"
    );
}
