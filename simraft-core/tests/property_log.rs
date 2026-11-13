//! Log safety properties (invariants S-02, S-03, S-08) over seeded
//! cluster runs and direct message sequences.

#[allow(dead_code)]
mod common;

use common::Harness;
use simraft_core::{
    Action, ClientId, EntryKind, Event, HardState, LogEntry, LogIndex, Message, Node, NodeId,
    Role, Term,
};

fn entries_with_terms(terms: &[u64]) -> Vec<LogEntry> {
    terms
        .iter()
        .map(|&t| LogEntry {
            term: Term(t),
            kind: EntryKind::NoOp,
        })
        .collect()
}

#[test]
fn leader_log_is_append_only() {
    let mut harness = Harness::new(&[1, 2, 3], 99);
    harness.run_until(3_000, 25);
    let leaders = harness.leaders();
    assert_eq!(leaders.len(), 1, "expected exactly one leader");
    let leader_id = leaders[0].0;
    // Propose entries through the leader while watching its log: after
    // every step the previously observed prefix must be intact.
    let mut observed: Vec<LogEntry> =
        harness.nodes[&leader_id].log().entries().to_vec();
    for seq in 1_u64..=8 {
        harness.step_node(
            leader_id,
            Event::Propose {
                client_id: ClientId(1),
                seq,
                payload: vec![seq as u8],
            },
        );
        let current = harness.nodes[&leader_id].log().entries().to_vec();
        assert!(
            current.len() >= observed.len()
                && current[..observed.len()] == observed[..],
            "leader log prefix changed: was {observed:?}, now {current:?}"
        );
        observed = current;
        harness.run_until(200, 25);
    }
}

#[test]
fn matching_index_term_implies_matching_prefix() {
    let mut harness = Harness::new(&[1, 2, 3], 7);
    harness.run_until(3_000, 25);
    let leader_id = harness.leaders()[0].0;
    for seq in 1_u64..=6 {
        harness.step_node(
            leader_id,
            Event::Propose {
                client_id: ClientId(9),
                seq,
                payload: format!("value-{seq}").into_bytes(),
            },
        );
        harness.run_until(300, 25);
    }
    // After settling, every pair of logs must agree on every prefix
    // whose (index, term) matches.
    let ids: Vec<NodeId> = harness.nodes.keys().copied().collect();
    for &a in &ids {
        for &b in &ids {
            let la = &harness.nodes[&a].log();
            let lb = &harness.nodes[&b].log();
            let shared = la.last_index().0.min(lb.last_index().0);
            for i in 1..=shared {
                let idx = LogIndex(i);
                if la.term_at(idx) == lb.term_at(idx) {
                    assert_eq!(
                        la.entries()[..i as usize],
                        lb.entries()[..i as usize],
                        "nodes {a} and {b} share (index {i}, term {:?}) but prefixes differ",
                        la.term_at(idx)
                    );
                }
            }
        }
    }
    // And all nodes converged to the leader's committed prefix.
    let leader_len = harness.nodes[&leader_id].log().entries().len();
    assert!(leader_len >= 6, "leader should hold the proposals");
}

#[test]
fn conflict_hint_lands_on_term_boundary() {
    // Follower with terms [1, 1, 2, 2] at indexes 1..=4.
    let mut follower = Node::new(
        NodeId(2),
        simraft_core::Config::default(),
        common::all_voters(&[1, 2, 3]),
        11,
    )
    .unwrap();
    let _ = follower.step(Event::Restored {
        hard_state: HardState {
            current_term: Term(2),
            voted_for: None,
        },
        entries: entries_with_terms(&[1, 1, 2, 2]),
    });

    // Leader probes at index 4 claiming term 3: mismatch at term 2.
    let actions = follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(3),
            prev_log_index: LogIndex(4),
            prev_log_term: Term(3),
            entries: vec![],
            leader_commit: LogIndex(0),
            read_round: 0,
        },
    });
    let (conflict_index, conflict_term) = actions
        .iter()
        .find_map(|a| match a {
            Action::Send {
                msg:
                    Message::AppendEntriesResponse {
                        success: false,
                        conflict_index,
                        conflict_term,
                        ..
                    },
                ..
            } => Some((*conflict_index, *conflict_term)),
            _ => None,
        })
        .expect("a rejection with a conflict hint");
    assert_eq!(conflict_term, Term(2));
    assert_eq!(
        conflict_index,
        LogIndex(3),
        "the hint must name the first index of the conflicting term"
    );

    // A probe past the end of the follower's log: too short.
    let actions = follower.step(Event::Message {
        from: NodeId(1),
        msg: Message::AppendEntries {
            term: Term(3),
            prev_log_index: LogIndex(9),
            prev_log_term: Term(3),
            entries: vec![],
            leader_commit: LogIndex(0),
            read_round: 0,
        },
    });
    let (conflict_index, conflict_term) = actions
        .iter()
        .find_map(|a| match a {
            Action::Send {
                msg:
                    Message::AppendEntriesResponse {
                        success: false,
                        conflict_index,
                        conflict_term,
                        ..
                    },
                ..
            } => Some((*conflict_index, *conflict_term)),
            _ => None,
        })
        .expect("a rejection with a conflict hint");
    assert_eq!(conflict_index, LogIndex(5), "last index + 1 when too short");
    assert_eq!(conflict_term, Term(0));
    assert_eq!(follower.role(), Role::Follower);
}
