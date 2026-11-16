//! The virtual clock and the deterministic event priority queue.
//!
//! Docs/ARCHITECTURE.md section 12.1. Events are ordered by
//! (delivery tick, insertion sequence); ties are impossible, so
//! execution order is a pure function of the seed.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// One scheduled item: delivery tick, insertion sequence, payload.
#[derive(Debug)]
struct Scheduled<T> {
    tick: u64,
    seq: u64,
    item: T,
}

impl<T> PartialEq for Scheduled<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.tick, self.seq) == (other.tick, other.seq)
    }
}

impl<T> Eq for Scheduled<T> {}

impl<T> PartialOrd for Scheduled<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Scheduled<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.tick, self.seq).cmp(&(other.tick, other.seq))
    }
}

/// A min-first queue of scheduled items keyed by delivery tick.
#[derive(Debug)]
pub struct EventQueue<T> {
    heap: BinaryHeap<Reverse<Scheduled<T>>>,
    next_seq: u64,
}

impl<T> Default for EventQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EventQueue<T> {
    /// An empty queue.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            next_seq: 0,
        }
    }

    /// Schedule `item` for delivery at `tick`.
    pub fn push(&mut self, tick: u64, item: T) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(Reverse(Scheduled { tick, seq, item }));
    }

    /// Remove and return the earliest item and its delivery tick.
    pub fn pop(&mut self) -> Option<(u64, T)> {
        self.heap.pop().map(|Reverse(s)| (s.tick, s.item))
    }

    /// The earliest delivery tick, if any item is scheduled.
    #[must_use]
    pub fn peek_tick(&self) -> Option<u64> {
        self.heap.peek().map(|Reverse(s)| s.tick)
    }

    /// Whether the queue holds no items.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// The number of scheduled items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delivery_order_is_tick_then_insertion() {
        let mut q = EventQueue::new();
        q.push(10, "c");
        q.push(5, "a");
        q.push(5, "b");
        q.push(10, "d");
        assert_eq!(q.pop(), Some((5, "a")));
        assert_eq!(q.pop(), Some((5, "b")));
        assert_eq!(q.pop(), Some((10, "c")));
        assert_eq!(q.pop(), Some((10, "d")));
        assert_eq!(q.pop(), None);
    }
}
