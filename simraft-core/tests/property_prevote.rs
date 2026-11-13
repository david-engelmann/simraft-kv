//! Property tests for Pre-Vote purity and the hypothetical term
//! (invariants T-03, T-04, and the recency half of T-05).

#[allow(dead_code)]
mod common;

use simraft_core::{Action, Event, Message, Node, NodeId, Role, Term};

fn single_node(id: u64, cluster_ids: &[u64]) -> Node {
    Node::new(
        NodeId(id),
        simraft_core::Config::default(),
        common::all_voters(cluster_ids),
        7,
    )
    .unwrap()
}

#[test]
fn prevote_leaves_hard_state_and_timer_untouched() {
    let mut voter = single_node(2, &[1, 2, 3]);
    // Establish a known timer state first.
    let _ = voter.step(Event::Tick { now: 100 });
    let term_before = voter.current_term();
    let vote_before = voter.voted_for();
    let deadline_before = voter.poll_timeout();

    let actions = voter.step(Event::Message {
        from: NodeId(1),
        msg: Message::RequestPreVote {
            term: Term(1),
            last_log_index: simraft_core::LogIndex(0),
            last_log_term: Term(0),
        },
    });

    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestPreVoteResponse {
                    vote_granted: true,
                    ..
                },
                ..
            }
        )),
        "an up-to-date pre-candidate with no live leader must be granted, got {actions:?}"
    );
    assert_eq!(voter.current_term(), term_before, "pre-vote changed the term");
    assert_eq!(voter.voted_for(), vote_before, "pre-vote recorded a vote");
    assert_eq!(
        voter.poll_timeout(),
        deadline_before,
        "granting a pre-vote reset the election timer"
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::PersistHardState { .. })),
        "pre-vote handling must not persist anything, got {actions:?}"
    );
}

#[test]
fn candidate_term_increments_only_after_prevote_majority() {
    let mut node = single_node(1, &[1, 2, 3]);
    let actions = node.step(Event::Tick { now: 1_000_000 });
    assert_eq!(node.role(), Role::PreCandidate);
    assert_eq!(
        node.current_term(),
        Term(0),
        "the election timer firing must not increment the persistent term"
    );
    assert!(
        actions.iter().all(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestPreVote { term: Term(1), .. },
                ..
            }
        ) && matches!(a, Action::Send { .. })),
        "a pre-candidate sends only RequestPreVote for term + 1, got {actions:?}"
    );

    // One grant of two needed (quorum of 3 is 2, counting itself) is
    // already a majority together with its own implicit pre-vote.
    let _ = node.step(Event::Message {
        from: NodeId(2),
        msg: Message::RequestPreVoteResponse {
            term: Term(0),
            vote_granted: true,
        },
    });
    assert_eq!(node.role(), Role::Candidate);
    assert_eq!(
        node.current_term(),
        Term(1),
        "the term increments exactly when the pre-vote majority lands"
    );
}

#[test]
fn prevote_denied_when_leader_recently_heard_from() {
    let mut voter = single_node(2, &[1, 2, 3]);
    // Hear from a leader in term 1 at tick 1_000 (heartbeat, no entries).
    let _ = voter.step(Event::Tick { now: 1_000 });
    let actions = voter.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(1),
            prev_log_index: simraft_core::LogIndex(0),
            prev_log_term: Term(0),
            entries: vec![],
            leader_commit: simraft_core::LogIndex(0),
            read_round: 0,
        },
    });
    // Acknowledge the term-adoption persist so the heartbeat completes.
    let _ = voter.step(Event::Persisted {
        kind: simraft_core::PersistKind::HardState,
        up_to_index: simraft_core::LogIndex(0),
    });
    let _ = actions;
    // A pre-vote request in the same window must be denied: the voter
    // has a live leader.
    let actions = voter.step(Event::Message {
        from: NodeId(3),
        msg: Message::RequestPreVote {
            term: Term(2),
            last_log_index: simraft_core::LogIndex(0),
            last_log_term: Term(0),
        },
    });
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestPreVoteResponse {
                    vote_granted: false,
                    ..
                },
                ..
            }
        )),
        "pre-vote must be denied while a leader was recently heard from, got {actions:?}"
    );
}
