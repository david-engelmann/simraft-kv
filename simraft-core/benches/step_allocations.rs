#![allow(
    missing_docs,
    reason = "the criterion_group macro generates the bench entry point, which cannot carry a doc comment"
)]

//! Step allocation budget (invariant L-03): `Node::step`
//! allocates within a measured per-event-class budget, verified
//! with a counting allocator in this bench harness. Tick, vote,
//! and response handling allocate at most a small constant number
//! of allocations; `AppendEntries` handling may allocate in
//! proportion to the batch it accepts. The budgets below are the
//! recorded bounds; a regression fails CI's bench smoke run
//! (`cargo bench --workspace -- --test` executes the same
//! assertions once per bench).

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};

use criterion::{Criterion, criterion_group, criterion_main};
use simraft_core::{
    ClusterConfig, Config, Event, LogIndex, Message, Node, NodeId, PersistKind, Role, Term,
};

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        // SAFETY: forwards to the system allocator with the same
        // layout the caller provided.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwards the pointer and layout unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

fn allocations_around(f: impl FnOnce()) -> usize {
    ALLOCATIONS.store(0, Ordering::Relaxed);
    f();
    ALLOCATIONS.load(Ordering::Relaxed)
}

/// The recorded per-class budgets (allocations per step).
const TICK_BUDGET: usize = 16;
const VOTE_BUDGET: usize = 16;
const APPEND_BUDGET: usize = 32;

fn node(id: u64) -> Node {
    let cfg = ClusterConfig::new([NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("config validates");
    Node::new(NodeId(id), Config::default(), cfg, 7).expect("node builds")
}

fn leader() -> Node {
    let mut node = node(1);
    node.step(Event::Tick { now: 1_000_000 });
    for peer in [NodeId(2), NodeId(3)] {
        let term = node.current_term();
        node.step(Event::Message {
            from: peer,
            msg: Message::RequestPreVoteResponse {
                term,
                vote_granted: true,
            },
        });
        if node.role() == Role::Candidate {
            break;
        }
    }
    node.step(Event::Persisted {
        kind: PersistKind::HardState,
        up_to_index: LogIndex::ZERO,
    });
    for peer in [NodeId(2), NodeId(3)] {
        let term = node.current_term();
        node.step(Event::Message {
            from: peer,
            msg: Message::RequestVoteResponse {
                term,
                vote_granted: true,
            },
        });
        if node.role() == Role::Leader {
            break;
        }
    }
    node
}

fn bench_step_allocations(c: &mut Criterion) {
    let mut group = c.benchmark_group("step_allocations");
    group.bench_function("tick", |b| {
        let mut node = node(2);
        let mut now = 0_u64;
        b.iter(|| {
            now += 10;
            let count = allocations_around(|| {
                black_box(node.step(Event::Tick { now }));
            });
            assert!(
                count <= TICK_BUDGET,
                "L-03: a Tick step allocated {count} times, budget {TICK_BUDGET}"
            );
        });
    });
    group.bench_function("request_vote", |b| {
        b.iter(|| {
            let mut node = node(2);
            let count = allocations_around(|| {
                black_box(node.step(Event::Message {
                    from: NodeId(1),
                    msg: Message::RequestVote {
                        term: Term(1),
                        last_log_index: LogIndex::ZERO,
                        last_log_term: Term::ZERO,
                    },
                }));
            });
            assert!(
                count <= VOTE_BUDGET,
                "L-03: a RequestVote step allocated {count} times, budget {VOTE_BUDGET}"
            );
        });
    });
    group.bench_function("append_entries_one", |b| {
        let mut leader = leader();
        let actions = leader.step(Event::Propose {
            client_id: simraft_core::ClientId(1),
            seq: 1,
            payload: vec![3_u8; 16],
        });
        let message = actions
            .iter()
            .find_map(|a| match a {
                simraft_core::Action::Send {
                    to,
                    msg: m @ Message::AppendEntries { .. },
                } if *to == NodeId(2) => Some(m.clone()),
                _ => None,
            })
            .expect("the leader sends AppendEntries");
        b.iter(|| {
            let mut follower = node(2);
            // Bring the follower to the leader's term first (the
            // vote above is not part of the measurement).
            follower.step(Event::Tick { now: 500 });
            let count = allocations_around(|| {
                black_box(follower.step(Event::Message {
                    from: NodeId(1),
                    msg: message.clone(),
                }));
            });
            assert!(
                count <= APPEND_BUDGET,
                "L-03: an AppendEntries step allocated {count} times, budget {APPEND_BUDGET}"
            );
        });
    });
    group.finish();
}

criterion_group!(benches, bench_step_allocations);
criterion_main!(benches);
