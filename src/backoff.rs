//! retry pacing, shared by op retries and the http source's request loop:
//! capped exponential growth, plus full jitter so ops that fail together
//! don't retry together.

use std::time::Duration;

/// `base * 2^attempt`, never above `max`. `attempt` counts from 0 for the
/// first retry, so attempt 0 waits `base`.
pub(crate) fn capped_exponential(base: Duration, attempt: u32, max: Duration) -> Duration {
    base.saturating_mul(2u32.saturating_pow(attempt)).min(max)
}

/// full jitter: uniform in `[0, d]`. spreading the whole window (rather than
/// jittering around it) is what breaks a lockstep herd apart fastest.
pub(crate) fn full_jitter(d: Duration) -> Duration {
    d.mul_f64(unit_random())
}

/// [`capped_exponential`] with [`full_jitter`] applied.
pub(crate) fn jittered_exponential(base: Duration, attempt: u32, max: Duration) -> Duration {
    full_jitter(capped_exponential(base, attempt, max))
}

// a uniform-ish f64 in [0, 1] without a rand dependency: RandomState seeds
// itself from the os once per thread and bumps a counter per instance, so
// hashing nothing still gives a fresh, well-spread u64 every call. this
// paces retries, it does not protect anything.
fn unit_random() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let n = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    n as f64 / u64::MAX as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_doubles_then_caps() {
        let base = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        assert_eq!(capped_exponential(base, 0, max), Duration::from_millis(100));
        assert_eq!(capped_exponential(base, 1, max), Duration::from_millis(200));
        assert_eq!(capped_exponential(base, 2, max), Duration::from_millis(400));
        assert_eq!(capped_exponential(base, 3, max), Duration::from_millis(800));
        // 1600ms would overshoot
        assert_eq!(capped_exponential(base, 4, max), max);
        // and a huge attempt count neither panics nor overflows
        assert_eq!(capped_exponential(base, u32::MAX, max), max);
    }

    #[test]
    fn jitter_stays_in_range_and_varies() {
        let d = Duration::from_secs(4);
        let samples: Vec<Duration> = (0..64).map(|_| full_jitter(d)).collect();
        assert!(
            samples.iter().all(|s| *s <= d),
            "jitter exceeded its window"
        );
        let distinct = samples.iter().collect::<std::collections::HashSet<_>>();
        assert!(distinct.len() > 32, "jitter barely moved: {distinct:?}");
        assert_eq!(full_jitter(Duration::ZERO), Duration::ZERO);
    }
}
