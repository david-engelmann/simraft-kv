//! Checker self-tests (invariant R-05): known linearizable and
//! non-linearizable histories are pinned to their verdicts —
//! stale read, phantom value, and lost update must all be caught.

use simraft_sim::history::{History, Op, OpResult};
use simraft_sim::liz::{Checker, Verdict};

fn put(key: &[u8], value: &[u8]) -> Op {
    Op::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn get(key: &[u8]) -> Op {
    Op::Get { key: key.to_vec() }
}

#[test]
fn sequential_put_then_get_is_linearizable() {
    let mut h = History::new();
    let a = h.invoke(1, 10, put(b"k", b"1"));
    h.complete(a, 20, OpResult::Ack);
    let b = h.invoke(1, 30, get(b"k"));
    h.complete(b, 40, OpResult::Value(Some(b"1".to_vec())));
    assert_eq!(Checker::default().check(h.records()), Verdict::Linearizable);
}

#[test]
fn stale_read_is_not_linearizable() {
    let mut h = History::new();
    let a = h.invoke(1, 10, put(b"k", b"1"));
    h.complete(a, 20, OpResult::Ack);
    let b = h.invoke(1, 30, put(b"k", b"2"));
    h.complete(b, 40, OpResult::Ack);
    // This read starts after both writes returned, so it must see 2.
    let c = h.invoke(1, 50, get(b"k"));
    h.complete(c, 60, OpResult::Value(Some(b"1".to_vec())));
    assert!(matches!(
        Checker::default().check(h.records()),
        Verdict::NotLinearizable { .. }
    ));
}

#[test]
fn phantom_value_is_not_linearizable() {
    let mut h = History::new();
    let a = h.invoke(1, 10, get(b"k"));
    h.complete(a, 20, OpResult::Value(Some(b"ghost".to_vec())));
    assert!(matches!(
        Checker::default().check(h.records()),
        Verdict::NotLinearizable { .. }
    ));
}

#[test]
fn lost_update_is_not_linearizable() {
    let mut h = History::new();
    let a = h.invoke(1, 10, put(b"k", b"1"));
    let b = h.invoke(2, 10, put(b"k", b"2"));
    h.complete(a, 30, OpResult::Ack);
    h.complete(b, 30, OpResult::Ack);
    // A read after both writes must see one of them, never a third.
    let c = h.invoke(1, 40, get(b"k"));
    h.complete(c, 50, OpResult::Value(Some(b"3".to_vec())));
    assert!(matches!(
        Checker::default().check(h.records()),
        Verdict::NotLinearizable { .. }
    ));
}

#[test]
fn pending_write_may_take_effect_for_a_later_read() {
    let mut h = History::new();
    // The write never returns, but it may have linearized, so the
    // read that observes it keeps the history linearizable.
    let _pending = h.invoke(1, 10, put(b"k", b"1"));
    let b = h.invoke(2, 20, get(b"k"));
    h.complete(b, 30, OpResult::Value(Some(b"1".to_vec())));
    assert_eq!(Checker::default().check(h.records()), Verdict::Linearizable);
}

#[test]
fn concurrent_read_may_see_the_older_value() {
    let mut h = History::new();
    let a = h.invoke(1, 10, put(b"k", b"1"));
    h.complete(a, 20, OpResult::Ack);
    // The second write and the read overlap, so the read may
    // linearize before the write and see 1.
    let b = h.invoke(1, 30, put(b"k", b"2"));
    let c = h.invoke(2, 35, get(b"k"));
    h.complete(c, 45, OpResult::Value(Some(b"1".to_vec())));
    h.complete(b, 50, OpResult::Ack);
    assert_eq!(Checker::default().check(h.records()), Verdict::Linearizable);
}
