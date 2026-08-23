//! the numbers a deployment pages on, and the text a scrape reads them off.
//!
//! two kinds of number live here and they do not mean the same thing.
//!
//! **gauges are read off the store when the scrape arrives**, so each one
//! describes the whole deployment, and every process serving this endpoint
//! answers with the same figure. aggregate them with `max`, never with `sum`:
//! three workers reporting a queue of four are not a queue of twelve.
//!
//! **counters and histograms belong to the process that is scraped.** each one
//! moves at the call that has just written the same fact to the run log, so it
//! counts what landed and nothing else, and each reads **zero after a
//! restart**. that is not a shortcut around the store: every table hestan keeps
//! is prunable by [retention](crate::Retention), so a `_total` read off a
//! `COUNT(*)` would fall the first time a prune ran, and prometheus reads a
//! counter that fell as a process that restarted and invents the rate to match.
//! a counter that resets at a restart is a shape prometheus already knows how
//! to read; one that drops halfway through a Tuesday is not.
//!
//! **what may be a label.** a label value here is a `&'static str`, and that is
//! the whole of the cardinality rule *and* the whole of its enforcement: a job
//! name, an asset name, a partition key or a run id is read out of a database
//! row and borrows from it, so it does not typecheck as one. what is left is
//! the words hestan spells in its own source, which is why every label below
//! comes from a **closed** enum: [`RunStatus`], [`Reclaim`] and [`TickOutcome`]
//! each have an `as_str` returning `&'static str`, while an open enum's borrows
//! from `self` and cannot be written here at all. the one label that is not a
//! word is a histogram's `le`, and it is a number off a const list. so the set
//! of series this file can emit is fixed.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::model::{Reclaim, RunStatus, TickOutcome};

/// the three terminal [statuses](RunStatus), in the order [`Meters::runs`]
/// holds them.
const RUN_OUTCOMES: [RunStatus; 3] = [RunStatus::Success, RunStatus::Failed, RunStatus::Canceled];

/// what became of an occurrence, in the order [`Meters::fires`] holds them.
///
/// a caught-up fire is [`TickOutcome::Fired`] with the catch-up flag set, and
/// it is worth its own value: a schedule that only ever fires late is a
/// deployment missing its window, and one that fires on time and catches up
/// after a deploy is a deployment that restarted.
const FIRE_OUTCOMES: [&str; 5] = ["fired", "caught_up", "skipped", "deferred", "error"];

/// what a reclaimed run became, in the order [`Meters::reclaims`] holds them.
const RECLAIM_OUTCOMES: [&str; 2] = ["failed", "requeued"];

/// how long a run waited between being written down and being claimed.
///
/// the first bound is the dispatcher's own poll interval, which is the delay a
/// queue with a free slot already has; the sixth is the lease, past which a run
/// has waited longer than a claim is believed for; the last is half an hour, by
/// which point nobody is arguing about whether the queue is stuck.
const CLAIM_BOUNDS: &[f64] = &[0.5, 1.0, 2.0, 5.0, 15.0, 60.0, 300.0, 1800.0];

/// how late a schedule fired: the gap between the occurrence and the fire.
///
/// a minute is the finest a cron expression can ask for, so the interesting
/// range starts below one and ends at an hour, past which the catch-up rule
/// rather than the scheduler's pace is what is being measured.
const LATE_BOUNDS: &[f64] = &[1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 3600.0];

/// what one process has counted since it started.
///
/// one per [`Store`](crate::Store), shared by every clone of it, which is one
/// per process in every deployment that opens its run log once. each is moved
/// **after** the write recording the same fact has committed, so a transaction
/// that rolled back and was tried again moves it once rather than twice, and a
/// write that never landed does not move it at all.
pub(crate) struct Meters {
    /// runs this process took to a terminal status, by which one.
    runs: [AtomicU64; RUN_OUTCOMES.len()],
    /// runs this process claimed off the queue.
    claims: AtomicU64,
    /// claims this process took back from a holder that stopped renewing.
    reclaims: [AtomicU64; RECLAIM_OUTCOMES.len()],
    /// op attempts that failed with another attempt still to come.
    op_retries: AtomicU64,
    /// occurrences this process accounted for, by what it did about them.
    fires: [AtomicU64; FIRE_OUTCOMES.len()],
    /// queued to claimed.
    claim_delay: Histogram,
    /// due to fired.
    lateness: Histogram,
}

impl Default for Meters {
    fn default() -> Meters {
        Meters {
            runs: Default::default(),
            claims: AtomicU64::new(0),
            reclaims: Default::default(),
            op_retries: AtomicU64::new(0),
            fires: Default::default(),
            claim_delay: Histogram::new(CLAIM_BOUNDS),
            lateness: Histogram::new(LATE_BOUNDS),
        }
    }
}

impl Meters {
    /// `times` runs reached `status`.
    ///
    /// anything that is not terminal is ignored: `queued` and `running` are not
    /// outcomes. a sixth status would be a compile error here rather than a
    /// silent gap, since [`RunStatus`] is a closed set.
    pub(crate) fn run_finished(&self, status: RunStatus, times: u64) {
        let slot = match status {
            RunStatus::Success => 0,
            RunStatus::Failed => 1,
            RunStatus::Canceled => 2,
            RunStatus::Queued | RunStatus::Running => return,
        };
        self.runs[slot].fetch_add(times, Ordering::Relaxed);
    }

    /// a run was claimed off the queue, having waited `waited` since it was
    /// written down.
    pub(crate) fn claimed(&self, waited: chrono::Duration) {
        self.claims.fetch_add(1, Ordering::Relaxed);
        self.claim_delay.observe(waited);
    }

    /// `times` claims were taken back from holders that went quiet, and what
    /// the [policy](Reclaim) did with the runs.
    pub(crate) fn reclaimed(&self, policy: Reclaim, times: u64) {
        let slot = match policy {
            Reclaim::Fail => 0,
            Reclaim::Requeue => 1,
        };
        self.reclaims[slot].fetch_add(times, Ordering::Relaxed);
    }

    /// an attempt failed and another is coming.
    pub(crate) fn op_retried(&self) {
        self.op_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// an occurrence was accounted for, `late` after it came due.
    pub(crate) fn tick(&self, outcome: TickOutcome, caught_up: bool, late: chrono::Duration) {
        let slot = match outcome {
            TickOutcome::Fired if caught_up => 1,
            TickOutcome::Fired => 0,
            TickOutcome::Skipped => 2,
            TickOutcome::Deferred => 3,
            TickOutcome::Error => 4,
        };
        self.fires[slot].fetch_add(1, Ordering::Relaxed);
        // only a fire has a lateness. an occurrence that was skipped or held
        // was not late, it was decided about
        if matches!(outcome, TickOutcome::Fired) {
            self.lateness.observe(late);
        }
    }

    /// every counter and every histogram total, by name, for a case asserting
    /// that one of them moved.
    ///
    /// a map rather than accessors because what a case wants to say is that
    /// *this* number moved and the rest did not, and a list of accessors is a
    /// list somebody forgets to extend.
    #[cfg(test)]
    pub(crate) fn counted(&self) -> std::collections::BTreeMap<&'static str, u64> {
        let mut out = std::collections::BTreeMap::new();
        for (slot, status) in RUN_OUTCOMES.iter().enumerate() {
            let key = match status {
                RunStatus::Success => "runs:success",
                RunStatus::Failed => "runs:failed",
                RunStatus::Canceled => "runs:canceled",
                RunStatus::Queued | RunStatus::Running => unreachable!("not an outcome"),
            };
            out.insert(key, self.runs[slot].load(Ordering::Relaxed));
        }
        out.insert("claims", self.claims.load(Ordering::Relaxed));
        for (slot, outcome) in ["reclaims:failed", "reclaims:requeued"].iter().enumerate() {
            out.insert(outcome, self.reclaims[slot].load(Ordering::Relaxed));
        }
        out.insert("op_retries", self.op_retries.load(Ordering::Relaxed));
        for (slot, outcome) in [
            "fires:fired",
            "fires:caught_up",
            "fires:skipped",
            "fires:deferred",
            "fires:error",
        ]
        .iter()
        .enumerate()
        {
            out.insert(outcome, self.fires[slot].load(Ordering::Relaxed));
        }
        out.insert("claim_delay:count", self.claim_delay.observations());
        out.insert("claim_delay:millis", self.claim_delay.total_millis());
        out.insert("lateness:count", self.lateness.observations());
        out.insert("lateness:millis", self.lateness.total_millis());
        out
    }
}

/// observations in fixed buckets, which is the only shape of latency a scrape
/// can aggregate across processes.
///
/// a quantile computed per process cannot be averaged, added or merged with
/// another process's, so three workers each reporting a p99 say nothing about
/// the deployment. buckets add.
struct Histogram {
    /// upper bounds in seconds, ascending. `+Inf` is the slot past the end and
    /// is not spelled here.
    bounds: &'static [f64],
    /// one per bound plus one for `+Inf`, each holding the observations that
    /// fell in that slot rather than the running total the exposition wants.
    slots: Vec<AtomicU64>,
    /// every observation added up, in microseconds, so it is one atomic
    /// integer: there is no atomic f64, and a lock here would sit on the path
    /// of every claim a deployment makes.
    micros: AtomicU64,
}

impl Histogram {
    fn new(bounds: &'static [f64]) -> Histogram {
        Histogram {
            bounds,
            slots: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            micros: AtomicU64::new(0),
        }
    }

    /// how many spans this has seen, which is what a case asserts against.
    #[cfg(test)]
    pub(crate) fn observations(&self) -> u64 {
        self.slots.iter().map(|s| s.load(Ordering::Relaxed)).sum()
    }

    /// every span added up, rounded to milliseconds so a case can name a
    /// figure without naming a microsecond.
    #[cfg(test)]
    pub(crate) fn total_millis(&self) -> u64 {
        self.micros.load(Ordering::Relaxed) / 1_000
    }

    /// record one span.
    ///
    /// a negative span is recorded as zero rather than dropped. it means two
    /// hosts disagree about the time, the work still happened, and a histogram
    /// that quietly lost those observations would disagree with the counter
    /// beside it about how many there were.
    fn observe(&self, span: chrono::Duration) {
        let micros = span.num_microseconds().unwrap_or(i64::MAX).max(0) as u64;
        let seconds = micros as f64 / 1e6;
        let slot = self
            .bounds
            .iter()
            .position(|bound| seconds <= *bound)
            .unwrap_or(self.bounds.len());
        self.slots[slot].fetch_add(1, Ordering::Relaxed);
        self.micros.fetch_add(micros, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_observation_lands_in_the_first_bucket_it_is_under() {
        // exactly on a bound belongs to that bound: prometheus buckets are
        // `le`, less than or equal
        for (span, slot) in [(0, 0), (999, 0), (1_000, 0), (1_001, 1), (10_001, 2)] {
            let hist = Histogram::new(&[1.0, 10.0]);
            hist.observe(chrono::Duration::milliseconds(span));
            assert_eq!(
                hist.slots[slot].load(Ordering::Relaxed),
                1,
                "{span}ms did not land in slot {slot}"
            );
        }
        // and a clock that ran backwards is zero rather than a lost
        // observation
        let hist = Histogram::new(&[1.0, 10.0]);
        hist.observe(chrono::Duration::seconds(-5));
        assert_eq!(hist.slots[0].load(Ordering::Relaxed), 1);
        assert_eq!(hist.micros.load(Ordering::Relaxed), 0);
    }

    // the cardinality rule is that a label is a `&'static str` off a closed
    // enum or a const list, so this list is the whole of what can appear. an
    // enum that grew a variant is a compile error in `Meters` above
    #[test]
    fn every_label_value_is_a_word_hestan_spells() {
        let spelled: Vec<&str> = RUN_OUTCOMES
            .iter()
            .map(RunStatus::as_str)
            .chain(FIRE_OUTCOMES)
            .chain(RECLAIM_OUTCOMES)
            .collect();
        assert_eq!(
            spelled,
            [
                "success",
                "failed",
                "canceled",
                "fired",
                "caught_up",
                "skipped",
                "deferred",
                "error",
                "failed",
                "requeued",
            ]
        );
    }
}
