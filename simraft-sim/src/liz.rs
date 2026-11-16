//! The per-key Wing and Gong linearizability checker.
//!
//! Docs/ARCHITECTURE.md section 12.6. A history over many keys is
//! linearizable exactly when every per-key sub-history is, and
//! each per-key model is a single optional value, which keeps the
//! model small and exact. The backtracking search memoizes
//! visited (linearized set, dropped set, model state) triples; a
//! revisited triple is pruned immediately. Verdicts are
//! three-valued, and a caller in CI treats Unknown as a failure,
//! never as a pass.

use std::collections::{BTreeMap, BTreeSet};

use crate::history::{Op, OpRecord, OpResult};

/// The checker's verdict for a whole history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Every per-key sub-history has a legal linearization.
    Linearizable,
    /// Some key's sub-history has none; `prefix_len` is the length
    /// of the shortest prefix (in return order) of that key's
    /// completed operations that is already non-linearizable.
    NotLinearizable {
        /// The offending key.
        key: Vec<u8>,
        /// The minimal offending prefix length for that key.
        prefix_len: usize,
    },
    /// The step budget was exhausted before a decision.
    Unknown,
}

/// The Wing and Gong checker with a configurable step budget.
#[derive(Debug, Clone)]
pub struct Checker {
    /// The maximum number of search states to visit per key.
    pub step_budget: u64,
    /// The total visited-state count of the most recent check,
    /// recorded so a memoization regression shows as state
    /// explosion rather than only as wall time (invariant L-04).
    pub visited_states: usize,
}

impl Default for Checker {
    fn default() -> Self {
        Self {
            step_budget: 1_000_000,
            visited_states: 0,
        }
    }
}

impl Checker {
    /// A checker with the given per-key step budget.
    #[must_use]
    pub const fn with_budget(step_budget: u64) -> Self {
        Self {
            step_budget,
            visited_states: 0,
        }
    }

    /// Check a whole history, partitioned by key.
    pub fn check(&mut self, history: &[OpRecord]) -> Verdict {
        self.visited_states = 0;
        let mut by_key: BTreeMap<&[u8], Vec<&OpRecord>> = BTreeMap::new();
        for record in history {
            by_key.entry(record.op.key()).or_default().push(record);
        }
        for (key, records) in by_key {
            match self.check_key(&records) {
                KeyVerdict::Linearizable => {}
                KeyVerdict::Unknown => return Verdict::Unknown,
                KeyVerdict::NotLinearizable => {
                    let prefix_len = self.minimal_prefix(&records);
                    return Verdict::NotLinearizable {
                        key: key.to_vec(),
                        prefix_len,
                    };
                }
            }
        }
        Verdict::Linearizable
    }

    /// The shortest prefix of the key's completed operations (in
    /// return-tick order, pending operations with an invoke inside
    /// the prefix included) that is already non-linearizable.
    fn minimal_prefix(&mut self, records: &[&OpRecord]) -> usize {
        let mut completed: Vec<&OpRecord> = records
            .iter()
            .copied()
            .filter(|r| r.return_tick.is_some())
            .collect();
        completed.sort_by_key(|r| (r.return_tick, r.id));
        for len in 1..=completed.len() {
            let cutoff = completed[len - 1].return_tick.unwrap_or(u64::MAX);
            let subset: Vec<&OpRecord> = records
                .iter()
                .copied()
                .filter(|r| {
                    r.return_tick
                        .map_or(r.invoke_tick <= cutoff, |t| t <= cutoff)
                })
                .collect();
            if self.check_key(&subset) == KeyVerdict::NotLinearizable {
                return len;
            }
        }
        completed.len()
    }

    /// Decide one key's sub-history.
    fn check_key(&mut self, records: &[&OpRecord]) -> KeyVerdict {
        let n = records.len();
        // Precedence: i must precede j when i returned before j was
        // invoked. Pending operations precede nothing.
        let mut predecessors: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
        for i in 0..n {
            for j in 0..n {
                if i != j
                    && records[i]
                        .return_tick
                        .is_some_and(|returned| returned < records[j].invoke_tick)
                {
                    predecessors[j].insert(i);
                }
            }
        }
        let mut search = Search {
            records,
            predecessors: &predecessors,
            visited: BTreeSet::new(),
            budget: self.step_budget,
            spent: 0,
        };
        let outcome = search.run();
        self.visited_states += search.spent;
        outcome
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyVerdict {
    Linearizable,
    NotLinearizable,
    Unknown,
}

/// The memoized search states: (linearized set, dropped set,
/// model value) triples already proven to fail from here.
type Visited = BTreeSet<(BTreeSet<usize>, BTreeSet<usize>, Option<Vec<u8>>)>;

struct Search<'a> {
    records: &'a [&'a OpRecord],
    predecessors: &'a [BTreeSet<usize>],
    visited: Visited,
    budget: u64,
    spent: usize,
}

impl Search<'_> {
    fn run(&mut self) -> KeyVerdict {
        self.step(&BTreeSet::new(), &BTreeSet::new(), None)
    }

    /// Depth-first Wing and Gong search: at each state, any
    /// unlinearized operation whose predecessors are all linearized
    /// may go next; pending operations may also be dropped.
    fn step(
        &mut self,
        linearized: &BTreeSet<usize>,
        dropped: &BTreeSet<usize>,
        model: Option<&Vec<u8>>,
    ) -> KeyVerdict {
        if linearized.len() + dropped.len() == self.records.len() {
            return KeyVerdict::Linearizable;
        }
        if self.spent >= usize::try_from(self.budget).unwrap_or(usize::MAX) {
            return KeyVerdict::Unknown;
        }
        let state = (linearized.clone(), dropped.clone(), model.cloned());
        if self.visited.contains(&state) {
            return KeyVerdict::NotLinearizable;
        }
        self.visited.insert(state);
        self.spent += 1;
        let mut saw_unknown = false;
        for i in 0..self.records.len() {
            if linearized.contains(&i) || dropped.contains(&i) {
                continue;
            }
            if !self.predecessors[i].iter().all(|p| linearized.contains(p)) {
                continue;
            }
            let record = self.records[i];
            // Try to linearize operation i at this point.
            if let Consistency::Consistent(next_model) = apply(record, model) {
                let mut next = linearized.clone();
                next.insert(i);
                match self.step(&next, dropped, next_model.as_ref()) {
                    KeyVerdict::Linearizable => return KeyVerdict::Linearizable,
                    KeyVerdict::Unknown => saw_unknown = true,
                    KeyVerdict::NotLinearizable => {}
                }
            }
            // A pending operation may instead never take effect.
            if record.return_tick.is_none() {
                let mut next_dropped = dropped.clone();
                next_dropped.insert(i);
                match self.step(linearized, &next_dropped, model) {
                    KeyVerdict::Linearizable => return KeyVerdict::Linearizable,
                    KeyVerdict::Unknown => saw_unknown = true,
                    KeyVerdict::NotLinearizable => {}
                }
            }
        }
        if saw_unknown {
            KeyVerdict::Unknown
        } else {
            KeyVerdict::NotLinearizable
        }
    }
}

/// Whether an operation's recorded result is consistent with
/// linearizing at a point where the model holds a given value,
/// and the model value afterwards when it is.
enum Consistency {
    /// Consistent; the contained value is the next model state.
    Consistent(Option<Vec<u8>>),
    /// The recorded result cannot occur at this point.
    Inconsistent,
}

/// Apply one operation to the per-key model.
fn apply(record: &OpRecord, model: Option<&Vec<u8>>) -> Consistency {
    let current = model.cloned();
    match &record.result {
        // A rejected operation had no effect; it is consistent at
        // any legal point.
        Some(OpResult::Error(_)) => Consistency::Consistent(current),
        _ => match &record.op {
            Op::Put { value, .. } => match &record.result {
                Some(OpResult::Ack) | None => Consistency::Consistent(Some(value.clone())),
                _ => Consistency::Inconsistent,
            },
            Op::Delete { .. } => match &record.result {
                Some(OpResult::Ack) | None => Consistency::Consistent(None),
                _ => Consistency::Inconsistent,
            },
            Op::Get { .. } => match &record.result {
                Some(OpResult::Value(seen)) => {
                    if seen.as_ref() == model {
                        Consistency::Consistent(current)
                    } else {
                        Consistency::Inconsistent
                    }
                }
                // A pending Get has an unobserved result, so it is
                // consistent wherever it may legally sit.
                None => Consistency::Consistent(current),
                _ => Consistency::Inconsistent,
            },
        },
    }
}
