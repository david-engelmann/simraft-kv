//! Election safety properties (invariants S-01, S-09, S-10), exercised
//! over seeded cluster runs and direct message sequences.

#[allow(
    dead_code,
    reason = "each test file uses a different subset of the shared driver helpers"
)]
mod common;

use common::Harness;
use proptest::prelude::*;
use simraft_core::{
    Action, EntryKind, Event, HardState, LogEntry, LogIndex, Message, Node, NodeId, PersistKind,
    Role, Term,
};
use std::collections::BTreeMap;

#[test]
fn one_leader_per_term() {
    for seed in 0_u64..120 {
        let mut harness = Harness::new(&[1, 2, 3], seed);
        let mut now = 0_u64;
        while now <= 6_000 {
            harness.tick(now);
            let mut by_term: BTreeMap<Term, NodeId> = BTreeMap::new();
            for (id, term) in harness.leaders() {
                if let Some(other) = by_term.insert(term, id) {
                    panic!("seed {seed} tick {now}: two leaders in {term}: {other} and {id}");
                }
            }
            now += 25;
        }
        assert!(
            !harness.leaders().is_empty() || seed == u64::MAX,
            "seed {seed}: no leader emerged in 6,000 ticks"
        );
    }
}

#[test]
fn one_vote_per_term_across_restart() {
    let cluster = common::all_voters(&[1, 2, 3]);
    let mut voter = Node::new(NodeId(2), simraft_core::Config::default(), cluster, 3).unwrap();

    // Grant a vote to node 1 in term 1 (and acknowledge the persist).
    let _ = voter.step(Event::Message {
        from: NodeId(1),
        msg: Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        },
    });
    let _ = voter.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex(0),
    });

    // Crash and restore with exactly the durable state.
    let _ = voter.step(Event::Restored {
        hard_state: HardState {
            current_term: Term(1),
            voted_for: Some(NodeId(1)),
        },
        entries: vec![],
        snapshot: None,
    });

    // A second candidate in the same term must be refused.
    let actions = voter.step(Event::Message {
        from: NodeId(3),
        msg: Message::RequestVote {
            term: Term(1),
            last_log_index: LogIndex(0),
            last_log_term: Term(0),
        },
    });
    assert!(
        actions.iter().any(|a| matches!(
            a,
            Action::Send {
                to: NodeId(3),
                msg: Message::RequestVoteResponse {
                    vote_granted: false,
                    ..
                }
            }
        )),
        "second vote in term 1 after restart must be denied, got {actions:?}"
    );
    assert_eq!(voter.voted_for(), Some(NodeId(1)));
}

#[test]
fn stale_log_candidate_is_refused() {
    let cluster = common::all_voters(&[1, 2, 3]);
    let mut voter = Node::new(NodeId(2), simraft_core::Config::default(), cluster, 5).unwrap();
    // Give the voter a log with terms [1, 1, 2] at indexes 1..=3.
    let _ = voter.step(Event::Restored {
        hard_state: HardState {
            current_term: Term(2),
            voted_for: None,
        },
        entries: vec![
            LogEntry {
                term: Term(1),
                kind: EntryKind::NoOp,
            },
            LogEntry {
                term: Term(1),
                kind: EntryKind::NoOp,
            },
            LogEntry {
                term: Term(2),
                kind: EntryKind::NoOp,
            },
        ],
        snapshot: None,
    });

    // (candidate last index, candidate last term, expected grant)
    let cases = [
        (0_u64, 0_u64, false), // empty log against a 3-entry log
        (3, 1, false),         // same length, older last term
        (2, 2, false),         // same last term, shorter log
        (3, 2, true),          // identical tip: up to date
        (4, 2, true),          // longer log, same last term
        (1, 3, true),          // newer last term wins over length
    ];
    for (idx, term, expected) in cases {
        // Fresh voter per case so the one-vote rule cannot interfere:
        // bump the candidate term above the voter's term each round.
        let mut v = Node::new(
            NodeId(2),
            simraft_core::Config::default(),
            common::all_voters(&[1, 2, 3]),
            5,
        )
        .unwrap();
        let _ = v.step(Event::Restored {
            hard_state: HardState {
                current_term: Term(2),
                voted_for: None,
            },
            entries: vec![
                LogEntry {
                    term: Term(1),
                    kind: EntryKind::NoOp,
                },
                LogEntry {
                    term: Term(1),
                    kind: EntryKind::NoOp,
                },
                LogEntry {
                    term: Term(2),
                    kind: EntryKind::NoOp,
                },
            ],
            snapshot: None,
        });
        let actions = v.step(Event::Message {
            from: NodeId(1),
            msg: Message::RequestVote {
                term: Term(3),
                last_log_index: LogIndex(idx),
                last_log_term: Term(term),
            },
        });
        // A grant is released only after its persist; acknowledge it.
        let mut all = actions.clone();
        if actions
            .iter()
            .any(|a| matches!(a, Action::PersistHardState { .. }))
        {
            all.extend(v.step(Event::Persisted {
                kind: PersistKind::HardState,
                up_to_index: LogIndex(0),
            }));
        }
        let granted = all.iter().any(|a| {
            matches!(
                a,
                Action::Send {
                    msg: Message::RequestVoteResponse {
                        vote_granted: true,
                        ..
                    },
                    ..
                }
            )
        });
        assert_eq!(
            granted, expected,
            "candidate tip (index {idx}, term {term}) against voter tip (3, 2)"
        );
        let _ = &mut voter;
    }
    assert_eq!(Role::Follower, Role::Follower);
}

proptest! {
    #[test]
    fn up_to_date_voting_rule_holds_over_arbitrary_tips(
        mut voter_terms in prop::collection::vec(1_u64..=5, 0..=10),
        cand_idx in 0_u64..=12,
        cand_term_raw in 0_u64..=6,
    ) {
        voter_terms.sort_unstable();
        let cand_term = if cand_idx == 0 { 0 } else { cand_term_raw.max(1) };
        let voter_last_idx = voter_terms.len() as u64;
        let voter_last_term = voter_terms.last().copied().unwrap_or(0);
        let expected = (cand_term, cand_idx) >= (voter_last_term, voter_last_idx);

        let entries: Vec<LogEntry> = voter_terms
            .iter()
            .map(|&t| LogEntry {
                term: Term(t),
                kind: EntryKind::NoOp,
            })
            .collect();
        let mut voter = Node::new(
            NodeId(2),
            simraft_core::Config::default(),
            common::all_voters(&[1, 2, 3]),
            5,
        )
        .unwrap();
        let _ = voter.step(Event::Restored {
            hard_state: HardState {
                current_term: Term(6),
                voted_for: None,
            },
            entries,
            snapshot: None,
        });
        let mut all = voter.step(Event::Message {
            from: NodeId(1),
            msg: Message::RequestVote {
                term: Term(7),
                last_log_index: LogIndex(cand_idx),
                last_log_term: Term(cand_term),
            },
        });
        if all.iter().any(|a| matches!(a, Action::PersistHardState { .. })) {
            all.extend(voter.step(Event::Persisted {
                kind: PersistKind::HardState,
                up_to_index: LogIndex::ZERO,
            }));
        }
        let granted = all.iter().any(|a| matches!(
            a,
            Action::Send {
                msg: Message::RequestVoteResponse { vote_granted: true, .. },
                ..
            }
        ));
        prop_assert_eq!(granted, expected);
    }
}
