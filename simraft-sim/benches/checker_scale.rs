#![allow(
    missing_docs,
    reason = "the criterion_group macro generates the bench entry point, which cannot carry a doc comment"
)]

//! Checker tractability (invariant L-04): the per-key
//! linearizability checker decides a 100,000-operation history
//! over 64 keys within the CI step budget, by partitioning and
//! visited-state memoization. The bench records the visited-state
//! count so a memoization regression shows as state explosion,
//! not just wall time. Unpartitioned whole-history checking is
//! never used on multi-key histories (the checker partitions by
//! construction). The history grew from 10,000 to 100,000
//! operations in post-M4 Lane 3.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use simraft_sim::history::{History, Op, OpResult};
use simraft_sim::liz::{Checker, Verdict};

/// A 100,000-operation history over 64 keys: mostly sequential
/// per key (the shape a healthy cluster produces), with short
/// concurrent windows where a write and a read on the same key
/// overlap.
fn generated_history() -> History {
    let mut history = History::new();
    let mut tick = 0_u64;
    let mut values: std::collections::BTreeMap<u64, Vec<u8>> = std::collections::BTreeMap::new();
    for i in 0..100_000_u64 {
        let key_no = i % 64;
        let key = format!("key{key_no}").into_bytes();
        tick += 10;
        if i % 3 == 2 {
            let seen = values.get(&key_no).cloned();
            let id = history.invoke(1, tick, Op::Get { key });
            history.complete(id, tick + 5, OpResult::Value(seen));
        } else {
            let value = format!("v{i}").into_bytes();
            let id = history.invoke(
                1,
                tick,
                Op::Put {
                    key,
                    value: value.clone(),
                },
            );
            history.complete(id, tick + 5, OpResult::Ack);
            values.insert(key_no, value);
        }
    }
    history
}

fn bench_checker_scale(c: &mut Criterion) {
    let history = generated_history();
    {
        let mut group = c.benchmark_group("checker_scale");
        group.sample_size(10);
        group.bench_function("hundred_thousand_ops_sixty_four_keys", |b| {
            b.iter(|| {
                let mut checker = Checker::default();
                let verdict = checker.check(black_box(history.records()));
                assert_eq!(verdict, Verdict::Linearizable);
                black_box(checker.visited_states)
            });
        });
        group.finish();
    }

    // Record the visited-state count once, outside the timed loop.
    let mut checker = Checker::default();
    let verdict = checker.check(history.records());
    assert_eq!(verdict, Verdict::Linearizable);
    println!(
        "checker_scale: visited_states={} for 100,000 ops over 64 keys",
        checker.visited_states
    );
}

criterion_group!(benches, bench_checker_scale);
criterion_main!(benches);
