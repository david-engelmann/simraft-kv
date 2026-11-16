//! The simulator's only randomness source.
//!
//! Docs/ARCHITECTURE.md sections 3.3 and 12.1: a `SplitMix64`
//! expansion of the seed into an `xoshiro256starstar` state. The
//! algorithm is implemented here rather than taken from an
//! external crate because external crates do not promise
//! cross-version stream stability, and a changed stream would
//! silently change every seed's meaning.

/// A deterministic `xoshiro256starstar` stream.
#[derive(Debug, Clone)]
pub struct Rng {
    state: [u64; 4],
}

/// The `SplitMix64` finalizer (also used for the seed expansion).
const fn splitmix64(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

impl Rng {
    /// Expand `seed` into the generator state through `SplitMix64`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        let mut x = seed;
        let mut state = [0_u64; 4];
        for slot in &mut state {
            x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            *slot = splitmix64(x);
        }
        Self { state }
    }

    /// The next u64 in the stream.
    pub const fn next_u64(&mut self) -> u64 {
        let result = (self.state[1].wrapping_mul(5).rotate_left(7)).wrapping_mul(9);
        let t = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    /// A value in `0..bound` by modulo reduction. Modulo bias is
    /// accepted: the stream's contract is determinism, and every
    /// consumer draws in a fixed call order from the same stream.
    ///
    /// # Panics
    /// Panics when `bound` is zero, which is a caller bug.
    pub fn next_below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "next_below requires a positive bound");
        self.next_u64() % bound
    }

    /// A value in `lo..=hi` (both bounds inclusive).
    ///
    /// # Panics
    /// Panics when `hi < lo`, which is a caller bug.
    pub fn range_inclusive(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi >= lo, "range_inclusive requires hi >= lo");
        lo + self.next_below(hi - lo + 1)
    }

    /// True with probability `numerator / denominator`.
    ///
    /// # Panics
    /// Panics when `denominator` is zero or `numerator` exceeds it.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        assert!(
            denominator > 0 && numerator <= denominator,
            "chance requires 0 <= numerator <= denominator and denominator > 0"
        );
        self.next_below(denominator) < numerator
    }

    /// A per-mille probability test (0..=1000).
    pub fn per_mille(&mut self, rate: u64) -> bool {
        self.chance(rate.min(1000), 1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_matches_the_published_first_output() {
        // The first SplitMix64 output for seed 0 is a published
        // constant of the algorithm: the state advances by the
        // golden constant, then the finalizer runs. It anchors the
        // seed expansion.
        assert_eq!(splitmix64(0x9E37_79B9_7F4A_7C15), 0xE220_A839_7B1D_CDAF);
    }

    #[test]
    fn stream_is_deterministic_per_seed() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..128 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        let mut c = Rng::new(43);
        let differs = (0..128).any(|_| a.next_u64() != c.next_u64());
        assert!(differs, "different seeds must produce different streams");
    }

    #[test]
    fn ranges_stay_in_bounds() {
        let mut rng = Rng::new(7);
        for _ in 0..1_000 {
            assert!(rng.next_below(9) < 9);
            let v = rng.range_inclusive(5, 8);
            assert!((5..=8).contains(&v));
        }
    }
}
