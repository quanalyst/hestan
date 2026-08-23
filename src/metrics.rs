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
//! of series this file can emit is fixed, and a case in `server.rs` counts it
//! against a store stuffed with job names and partition keys.

use std::sync::atomic::{AtomicU64, Ordering};

use std::fmt::Display;
use std::fmt::Write as _;

use chrono::Utc;

use crate::executor::Runner;
use crate::model::{Reclaim, RunStatus, TickOutcome};

/// one label on one sample: a name hestan spells and a value hestan spells.
///
/// see the module docs for why both halves are `&'static str`.
type Label = (&'static str, &'static str);

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

/// the text of one exposition, built line by line.
struct Text(String);

impl Text {
    /// the two comment lines every family opens with. one family per name and
    /// each name once: a scrape that meets the same name twice discards it.
    fn family(&mut self, name: &str, kind: &str, help: &str) {
        let _ = writeln!(self.0, "# HELP {name} {help}");
        let _ = writeln!(self.0, "# TYPE {name} {kind}");
    }

    /// one sample line.
    fn sample(&mut self, name: &str, labels: &[Label], value: impl Display) {
        self.0.push_str(name);
        if let Some(((first, head), rest)) = labels.split_first() {
            let _ = write!(self.0, "{{{first}=\"{head}\"");
            for (label, value) in rest {
                let _ = write!(self.0, ",{label}=\"{value}\"");
            }
            self.0.push('}');
        }
        let _ = writeln!(self.0, " {value}");
    }

    /// a family of exactly one unlabelled sample, which most gauges are.
    fn one(&mut self, name: &str, kind: &str, help: &str, value: impl Display) {
        self.family(name, kind, help);
        self.sample(name, &[], value);
    }

    /// the `_bucket`, `_sum` and `_count` lines one histogram is made of.
    ///
    /// `le` is written here rather than through [`sample`](Self::sample)
    /// because it is a number off a const list rather than a word, which is
    /// also why it cannot be a way round the rule that keeps rows out of
    /// labels.
    fn histogram(&mut self, name: &str, help: &str, hist: &Histogram) {
        self.family(name, "histogram", help);
        let mut running = 0;
        for (slot, bound) in hist.bounds.iter().enumerate() {
            running += hist.slots[slot].load(Ordering::Relaxed);
            let _ = writeln!(self.0, "{name}_bucket{{le=\"{bound}\"}} {running}");
        }
        running += hist.slots[hist.bounds.len()].load(Ordering::Relaxed);
        // `+Inf` is required, and it is also where a scrape finds the count
        // when every observation ran off the end of the bounds
        let _ = writeln!(self.0, "{name}_bucket{{le=\"+Inf\"}} {running}");
        let micros = hist.micros.load(Ordering::Relaxed);
        let _ = writeln!(self.0, "{name}_sum {}", micros as f64 / 1e6);
        let _ = writeln!(self.0, "{name}_count {running}");
    }
}

/// every metric this process can answer for, in prometheus text exposition
/// format.
///
/// nothing is cached: the gauges are one pass over the run log taken when the
/// scrape arrives, and the counters are atomic loads. a scrape that cannot read
/// the store still renders, with `hestan_store_up` at 0 and the gauges it would
/// have carried left out: failing the request outright would take the metric
/// that says why down with it.
pub(crate) fn render(runner: &Runner) -> String {
    let mut text = Text(String::with_capacity(4096));
    let now = Utc::now();
    let store = runner.store();
    let read = store.counts(now);

    // what this process knows about itself, which it knows whether or not the
    // database is answering
    text.one(
        "hestan_store_up",
        "gauge",
        "1 while this process can read the run log. the deployment-wide gauges \
         are missing while it is 0",
        u8::from(read.is_ok()),
    );
    text.one(
        "hestan_store_writing",
        "gauge",
        "1 while the last write this process attempted landed. a 0 is a process \
         that has stopped claiming runs",
        u8::from(!store.health().failing()),
    );
    text.one(
        "hestan_runs_given_up",
        "gauge",
        "runs this process claimed and stopped executing without recording an \
         outcome, waiting on a reclaimer",
        runner.given_up().len(),
    );

    // and what the run log says, which is the same for every process serving
    // this endpoint: take the max across targets, never the sum
    if let Ok(counts) = &read {
        text.one(
            "hestan_queue_depth",
            "gauge",
            "runs written down and claimed by nobody",
            counts.queued,
        );
        text.one(
            "hestan_queue_oldest_seconds",
            "gauge",
            "how long the oldest unclaimed run has waited; 0 when the queue is \
             empty",
            counts.oldest_queued,
        );
        text.one(
            "hestan_runs_active",
            "gauge",
            "runs claimed by some process and not yet terminal",
            counts.active,
        );
        text.one(
            "hestan_runs_stalled",
            "gauge",
            "claimed runs past the lease they were claimed under: work nothing \
             has reclaimed",
            counts.stalled,
        );
        text.one(
            "hestan_schedules_paused",
            "gauge",
            "declared schedules that are paused, so their occurrences are \
             stepped over",
            counts.schedules_paused,
        );
        text.one(
            "hestan_sensors_paused",
            "gauge",
            "declared sensors that are paused, so they are not evaluated",
            counts.sensors_paused,
        );
        // read off the store rather than off this process's belief, for the
        // reason `/api/health` reads it there: a process that has stopped being
        // the decider is exactly the one whose belief about it is wrong
        text.one(
            "hestan_decider_held",
            "gauge",
            "1 while this process holds the decision lease: the one that runs \
             the schedules, the sensors and the policies",
            u8::from(counts.decider.held_by(runner.instance(), now)),
        );
        text.one(
            "hestan_decider_lease_seconds",
            "gauge",
            "how long the decision lease has left, whoever holds it; 0 when \
             nobody does",
            lease_left(&counts.decider, now),
        );
    }

    let meters = store.meters();
    text.family(
        "hestan_runs_total",
        "counter",
        "runs this process took to a terminal status, by which one",
    );
    for (slot, status) in RUN_OUTCOMES.iter().enumerate() {
        let count = meters.runs[slot].load(Ordering::Relaxed);
        text.sample("hestan_runs_total", &[("status", status.as_str())], count);
    }
    text.one(
        "hestan_run_claims_total",
        "counter",
        "runs this process claimed off the queue",
        meters.claims.load(Ordering::Relaxed),
    );
    text.family(
        "hestan_run_reclaims_total",
        "counter",
        "claims this process took back from a holder that stopped renewing its \
         lease, by what became of the run",
    );
    for (slot, outcome) in RECLAIM_OUTCOMES.iter().enumerate() {
        let count = meters.reclaims[slot].load(Ordering::Relaxed);
        text.sample("hestan_run_reclaims_total", &[("outcome", outcome)], count);
    }
    text.one(
        "hestan_op_retries_total",
        "counter",
        "op attempts that failed with another attempt still to come",
        meters.op_retries.load(Ordering::Relaxed),
    );
    text.family(
        "hestan_schedule_fires_total",
        "counter",
        "occurrences this process accounted for, by what it did about each one",
    );
    for (slot, outcome) in FIRE_OUTCOMES.iter().enumerate() {
        let count = meters.fires[slot].load(Ordering::Relaxed);
        text.sample(
            "hestan_schedule_fires_total",
            &[("outcome", outcome)],
            count,
        );
    }

    let health = store.health();
    text.one(
        "hestan_store_write_retries_total",
        "counter",
        "writes the store refused that hestan tried again. rising while the two \
         below stay flat is a database stumbling and recovering",
        health.write_retries(),
    );
    text.one(
        "hestan_store_unrecorded_writes_total",
        "counter",
        "writes recording what a run did that never landed. each one is a run \
         page missing what happened",
        health.unrecorded_writes(),
    );
    text.one(
        "hestan_store_dropped_writes_total",
        "counter",
        "best-effort writes let go: an event, a captured line, a pid",
        health.dropped_writes(),
    );

    text.histogram(
        "hestan_run_claim_delay_seconds",
        "how long the runs this process claimed had waited on the queue. \
         measured from when a run was written down, so a requeued run reports \
         its whole wait rather than the wait since it was requeued",
        &meters.claim_delay,
    );
    text.histogram(
        "hestan_schedule_lateness_seconds",
        "the gap between an occurrence coming due and this process firing it. a \
         catch-up fire after downtime is legitimately hours late and is in here \
         too",
        &meters.lateness,
    );
    text.0
}

/// seconds left on the decision lease, never negative: an expired lease is
/// nobody's, and reporting it as -4 would be reporting a lease.
fn lease_left(decider: &crate::model::Decider, now: chrono::DateTime<Utc>) -> f64 {
    decider.lease_until.map_or(0.0, |until| {
        (until - now).num_milliseconds().max(0) as f64 / 1000.0
    })
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

    #[test]
    fn the_buckets_a_scrape_reads_are_cumulative_and_end_at_inf() {
        let hist = Histogram::new(&[1.0, 10.0]);
        for span in [500, 5_000, 50_000] {
            hist.observe(chrono::Duration::milliseconds(span));
        }
        let mut text = Text(String::new());
        text.histogram("t_seconds", "help", &hist);
        let lines: Vec<&str> = text.0.lines().filter(|l| !l.starts_with('#')).collect();
        assert_eq!(
            lines,
            [
                "t_seconds_bucket{le=\"1\"} 1",
                "t_seconds_bucket{le=\"10\"} 2",
                "t_seconds_bucket{le=\"+Inf\"} 3",
                "t_seconds_sum 55.5",
                "t_seconds_count 3",
            ]
        );
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
