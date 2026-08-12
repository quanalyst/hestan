use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde_json::{Value, json};

use crate::error::Error;
use crate::executor::Runner;
use crate::model::{Catchup, Overlap, ScheduleRow, TickOutcome, Trigger};
use crate::store::Store;

/// how far back a catch-up scan will walk to enumerate missed occurrences. a
/// seconds-resolution schedule down for a month is millions of them, and
/// nothing downstream wants that list; past this the drop is reported as "at
/// least" rather than counted exactly.
const MAX_MISSED_SCAN: usize = 10_000;

/// how long a pass waits before trying the paused flag again, after a read of
/// it failed. the pass fired nothing, so this is the whole cost of the retry.
const CONTROL_READ_RETRY: Duration = Duration::from_secs(1);

/// one schedule as the code declares it: which job, which cron expression, and
/// the three things that used to be positional arguments.
///
/// ```no_run
/// # use hestan::{Catchup, Hestan, Schedule};
/// # use serde_json::json;
/// Hestan::new().add_schedule(
///     Schedule::new("orders_etl", "0 * * * *")
///         .tz("Europe/London")
///         .params(json!({"region": "eu"}))
///         .catchup(Catchup::All { limit: 24 }),
/// );
/// ```
///
/// [`Hestan::schedule`](crate::Hestan::schedule) and friends are this with the
/// common defaults filled in — utc, `{}`, [`Catchup::Skip`].
#[derive(Debug, Clone)]
pub struct Schedule {
    pub(crate) job: String,
    pub(crate) expr: String,
    pub(crate) tz: String,
    pub(crate) params: Value,
    pub(crate) catchup: Catchup,
}

impl Schedule {
    /// `cron_expr` is a 5-field crontab expression, evaluated in utc until
    /// [`tz`](Self::tz) says otherwise.
    pub fn new(job: impl Into<String>, cron_expr: impl Into<String>) -> Schedule {
        Schedule {
            job: job.into(),
            expr: cron_expr.into(),
            tz: "UTC".into(),
            params: json!({}),
            catchup: Catchup::default(),
        }
    }

    /// evaluate the expression in a named iana timezone.
    pub fn tz(mut self, tz: impl Into<String>) -> Schedule {
        self.tz = tz.into();
        self
    }

    /// what every fire launches with. validated against the job's ops at
    /// startup, so a schedule that could never launch is an error from
    /// `serve`/`run_once` rather than a tick that fails at 3am.
    pub fn params(mut self, params: Value) -> Schedule {
        self.params = params;
        self
    }

    /// what to do with occurrences that came due while nothing was running to
    /// fire them. [`Catchup::Skip`] by default.
    pub fn catchup(mut self, catchup: Catchup) -> Schedule {
        self.catchup = catchup;
        self
    }
}

#[derive(Debug)]
pub(crate) struct ScheduleEntry {
    pub job: String,
    pub expr: String,
    pub tz: Tz,
    pub schedule: cron::Schedule,
    /// what every fire launches with; `{}` unless the declaration set it.
    pub params: Value,
    /// what downtime does to this schedule. it comes from the declaration and
    /// is stored for the api rather than read back from it — the row is synced
    /// from the same value at startup, so the two cannot disagree.
    pub catchup: Catchup,
}

impl ScheduleEntry {
    pub(crate) fn with_params(mut self, params: Value) -> ScheduleEntry {
        self.params = params;
        self
    }

    pub(crate) fn with_catchup(mut self, catchup: Catchup) -> ScheduleEntry {
        self.catchup = catchup;
        self
    }

    fn key(&self) -> (String, String) {
        (self.job.clone(), self.expr.clone())
    }
}

pub(crate) fn parse(job: &str, expr: &str, tz: &str) -> Result<ScheduleEntry, Error> {
    let tz: Tz = tz.parse().map_err(|_| Error::Timezone(tz.to_string()))?;
    // the cron crate wants a seconds field; accept plain 5-field crontab
    let fields: Vec<&str> = expr.split_whitespace().collect();
    let full = if fields.len() == 5 {
        let mut fields = fields;
        let dow = remap_dow(fields[4]);
        fields[4] = &dow;
        format!("0 {}", fields.join(" "))
    } else {
        expr.to_string()
    };
    let schedule = cron::Schedule::from_str(&full).map_err(|e| Error::Cron {
        expr: expr.to_string(),
        reason: e.to_string(),
    })?;
    Ok(ScheduleEntry {
        job: job.to_string(),
        expr: expr.to_string(),
        tz,
        schedule,
        params: json!({}),
        catchup: Catchup::default(),
    })
}

// posix crontab numbers sunday 0 (or 7); the cron crate numbers the week 1..7.
// a range ending in 7 ("5-7") inverts and fails to parse at startup, loudly.
fn remap_dow(field: &str) -> String {
    field
        .split(',')
        .map(|part| {
            let (body, step) = match part.split_once('/') {
                Some((b, s)) => (b, Some(s)),
                None => (part, None),
            };
            let body = body
                .split('-')
                .map(|tok| match tok.parse::<u32>() {
                    Ok(n) if n <= 7 => ((n % 7) + 1).to_string(),
                    _ => tok.to_string(),
                })
                .collect::<Vec<_>>()
                .join("-");
            match step {
                Some(s) => format!("{body}/{s}"),
                None => body,
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn upcoming_fires(
    entry: &ScheduleEntry,
    now: DateTime<Utc>,
    window_secs: i64,
) -> Vec<DateTime<Utc>> {
    let end = now + chrono::Duration::seconds(window_secs);
    entry
        .schedule
        .after(&now.with_timezone(&entry.tz))
        .map(|t| t.with_timezone(&Utc))
        .take_while(|t| *t <= end)
        .take(100)
        .collect()
}

/// the stored side of every schedule, keyed by `(job, expr)`: pause state and
/// cursor, both of which can change under a running process.
///
/// `None` when the read failed. it used to be an empty map, which read as "no
/// schedule is paused" — an administrative stop that fails open. the caller
/// stops the pass instead: a missed occurrence is recoverable, since catch-up
/// sees it on the next pass, and a launch nobody asked for is not.
fn rows(store: &Store) -> Option<HashMap<(String, String), ScheduleRow>> {
    match store.schedules() {
        Ok(rows) => Some(
            rows.into_iter()
                .map(|r| ((r.job.clone(), r.expr.clone()), r))
                .collect(),
        ),
        Err(e) => {
            tracing::warn!("schedule read failed, holding every schedule this pass: {e}");
            None
        }
    }
}

/// the most recent occurrence strictly before `now`; `None` when the schedule
/// has not come due yet at all.
fn last_before(entry: &ScheduleEntry, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    entry
        .schedule
        .after(&now.with_timezone(&entry.tz))
        .next_back()
        .map(|t| t.with_timezone(&Utc))
}

/// occurrences strictly after `cursor` and strictly before `now`, newest
/// first, at most `MAX_MISSED_SCAN` of them — the flag says the scan hit that
/// wall with more behind it. this is the missed set, and the cursor is the only
/// thing that makes it knowable: without one, "what should have fired while we
/// were down" has no answer at all.
pub(crate) fn missed_since(
    entry: &ScheduleEntry,
    cursor: DateTime<Utc>,
    now: DateTime<Utc>,
) -> (Vec<DateTime<Utc>>, bool) {
    let mut walk = entry.schedule.after(&now.with_timezone(&entry.tz));
    let mut out = Vec::new();
    while let Some(t) = walk.next_back() {
        let t = t.with_timezone(&Utc);
        if t <= cursor {
            return (out, false);
        }
        if out.len() == MAX_MISSED_SCAN {
            return (out, true);
        }
        out.push(t);
    }
    (out, false)
}

/// bring one schedule's cursor up to now, applying its catch-up policy to
/// whatever it swallowed. runs on every pass, not only at boot: in steady state
/// the loop advances the cursor as it fires, so the missed set is empty and
/// this costs one cron walk that stops immediately.
fn catch_up(entry: &ScheduleEntry, runner: &Runner, row: Option<&ScheduleRow>, now: DateTime<Utc>) {
    let Some(cursor) = row.and_then(|r| r.cursor) else {
        // first sight of this schedule: there is no downtime to reconstruct,
        // and treating its whole cron past as missed would fire the epoch
        advance(runner, entry, now);
        return;
    };
    let Some(last) = last_before(entry, now).filter(|t| *t > cursor) else {
        return;
    };
    // pause means stop, including the catch-up: a schedule paused for a week
    // must not fire a week of backlog the moment it is resumed
    if row.is_some_and(|r| r.paused) {
        tracing::info!(job = %entry.job, expr = %entry.expr, "paused: cursor advanced over the gap");
        advance(runner, entry, last);
        return;
    }
    match entry.catchup {
        Catchup::Skip => {
            tracing::info!(job = %entry.job, expr = %entry.expr, "missed fires skipped to {last}");
        }
        Catchup::One => {
            tracing::info!(job = %entry.job, expr = %entry.expr, "catching up the latest missed fire");
            queue_fire(runner, entry, last);
        }
        Catchup::All { limit } => {
            let (newest_first, truncated) = missed_since(entry, cursor, now);
            let (fire, dropped) = newest_first.split_at(newest_first.len().min(limit.max(1)));
            // the cap drops the oldest, and says which: a backlog quietly
            // losing its head is the failure mode this policy exists to avoid
            if let Some(oldest) = dropped.last() {
                let count = match truncated {
                    true => format!("at least {}", dropped.len()),
                    false => dropped.len().to_string(),
                };
                let msg = format!(
                    "catch-up cap {limit}: dropped {count} missed occurrences up to {}",
                    dropped[0]
                );
                tracing::warn!(job = %entry.job, expr = %entry.expr, "{msg}");
                // one skipped tick carrying the reason, at the oldest dropped
                // occurrence: a tick per drop would bury the log it belongs in
                if let Err(e) = runner.store().record_tick(
                    &entry.job,
                    &entry.expr,
                    *oldest,
                    TickOutcome::Skipped,
                    false,
                    None,
                    Some(&msg),
                ) {
                    tracing::warn!(job = %entry.job, "tick write failed: {e}");
                }
            }
            for t in fire.iter().rev() {
                queue_fire(runner, entry, *t);
            }
        }
    }
    advance(runner, entry, last);
}

/// move the cursor to `at`. never backwards: a queued fire draining long after
/// its occurrence must not un-account for everything since.
fn advance(runner: &Runner, entry: &ScheduleEntry, at: DateTime<Utc>) {
    if let Err(e) = runner
        .store()
        .set_schedule_cursor(&entry.job, &entry.expr, at)
    {
        tracing::warn!(job = %entry.job, "cursor write failed: {e}");
    }
}

/// launch a caught-up occurrence, or record it as held when the job is busy or
/// something older is already waiting. either way the occurrence is on disk
/// before this returns, which is what makes a backlog survive a second restart.
fn queue_fire(runner: &Runner, entry: &ScheduleEntry, at: DateTime<Utc>) {
    let free = matches!(runner.store().has_active_run(&entry.job), Ok(false))
        && !has_pending(runner, &entry.job);
    match free {
        true => note_tick(runner, &entry.job, &entry.expr, at, &entry.params, true),
        false => note_runless_tick(runner, &entry.job, &entry.expr, at, TickOutcome::Deferred),
    }
}

fn has_pending(runner: &Runner, job: &str) -> bool {
    match runner.store().pending_fires() {
        Ok(rows) => rows.iter().any(|(j, ..)| j == job),
        Err(e) => {
            tracing::warn!(job = %job, "pending-fire read failed: {e}");
            true
        }
    }
}

/// launch whatever is waiting, oldest first and one per job. the queue is the
/// tick log — a `deferred` tick with no later tick for the same occurrence —
/// so a fire held when the process died is still held when it comes back.
fn drain_pending(
    entries: &[ScheduleEntry],
    runner: &Runner,
    rows: &HashMap<(String, String), ScheduleRow>,
) {
    let pending = match runner.store().pending_fires() {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!("pending-fire read failed: {e}");
            return;
        }
    };
    let mut launched: HashSet<&str> = HashSet::new();
    for (job, expr, at) in &pending {
        let Some(entry) = entries.iter().find(|e| &e.job == job && &e.expr == expr) else {
            // the declaration is gone, so nothing can launch it with the right
            // params; close the tick out rather than leaving it pending forever
            tracing::info!(job = %job, expr = %expr, "held fire dropped: schedule no longer declared");
            note_runless_tick(runner, job, expr, *at, TickOutcome::Skipped);
            continue;
        };
        if rows.get(&entry.key()).is_some_and(|r| r.paused) {
            tracing::info!(job = %job, expr = %expr, "held fire dropped: schedule paused");
            note_runless_tick(runner, job, expr, *at, TickOutcome::Skipped);
            continue;
        }
        if launched.contains(job.as_str()) {
            continue;
        }
        match runner.store().has_active_run(job) {
            Ok(false) => {
                tracing::info!(job = %job, expr = %expr, "held fire launching");
                note_tick(runner, job, expr, *at, &entry.params, true);
                launched.insert(job.as_str());
            }
            Ok(true) => {
                launched.insert(job.as_str());
            }
            Err(e) => {
                tracing::warn!(job = %job, "active-run check failed: {e}");
                launched.insert(job.as_str());
            }
        }
    }
}

pub(crate) async fn run_scheduler(mut entries: Vec<ScheduleEntry>, runner: Runner) {
    if entries.is_empty() {
        return;
    }
    loop {
        let now = Utc::now();
        // fail closed: a pass that cannot read the paused flag fires nothing
        // and moves no cursor, rather than treating unknown as unpaused
        let Some(stored) = rows(runner.store()) else {
            tokio::time::sleep(CONTROL_READ_RETRY).await;
            continue;
        };
        // downtime first, then the backlog it may have just added to: both are
        // the same mechanism, and both are on disk before anything launches
        for entry in &entries {
            catch_up(entry, &runner, stored.get(&entry.key()), now);
        }
        drain_pending(&entries, &runner, &stored);
        let waiting = runner
            .store()
            .pending_fires()
            .map(|p| !p.is_empty())
            .unwrap_or(false);

        let mut fires: Vec<(DateTime<Utc>, String, String, Value)> = Vec::new();
        entries.retain(|e| match e.schedule.upcoming(e.tz).next() {
            Some(t) => {
                fires.push((
                    t.with_timezone(&Utc),
                    e.job.clone(),
                    e.expr.clone(),
                    e.params.clone(),
                ));
                true
            }
            None => {
                tracing::warn!(job = %e.job, expr = %e.expr, "schedule has no future fires, dropping it");
                false
            }
        });
        if fires.is_empty() && !waiting {
            tracing::info!("all schedules exhausted, scheduler exiting");
            return;
        }
        let until = fires
            .iter()
            .map(|(t, ..)| *t)
            .min()
            .map(|earliest| (earliest - Utc::now()).to_std().unwrap_or_default())
            .unwrap_or(Duration::from_secs(60));
        // cap sleeps so clock drift self-corrects; poll faster with a fire waiting
        let cap = if waiting {
            Duration::from_secs(2)
        } else {
            Duration::from_secs(60)
        };
        if !until.is_zero() {
            tokio::time::sleep(until.min(cap)).await;
            let now = Utc::now();
            if !fires.iter().any(|(t, ..)| *t <= now) {
                continue;
            }
        }
        let now = Utc::now();
        // same again for the fires this pass is about to make: an occurrence
        // left un-accounted for is one the next pass's catch-up will see, and
        // catch-up honours the paused flag it can read by then
        let Some(stored) = rows(runner.store()) else {
            tokio::time::sleep(CONTROL_READ_RETRY).await;
            continue;
        };
        // two schedules on the same job sharing a tick fire one run, not two
        let mut fired: HashSet<String> = HashSet::new();
        for (t, job, expr, params) in fires {
            if t > now {
                continue;
            }
            let entry = entries
                .iter()
                .find(|e| e.job == job && e.expr == expr)
                .expect("fires come from entries");
            // the occurrence is accounted for however this pass treats it, so
            // the next catch-up pass does not see it as swallowed by downtime
            if stored
                .get(&(job.clone(), expr.clone()))
                .is_some_and(|r| r.paused)
            {
                advance(&runner, entry, t);
                continue;
            }
            advance(&runner, entry, t);
            if !fired.insert(job.clone()) {
                // the runner-up's fire was real; leave a tick, not silence
                note_runless_tick(&runner, &job, &expr, t, TickOutcome::Skipped);
                continue;
            }
            let held = has_pending(&runner, &job);
            let active = held || matches!(runner.store().has_active_run(&job), Ok(true) | Err(_));
            let policy = runner
                .jobs()
                .get(&job)
                .map(|j| j.overlap())
                .unwrap_or_default();
            if !active || policy == Overlap::Allow {
                tracing::info!(job = %job, expr = %expr, "schedule fired");
                note_tick(&runner, &job, &expr, t, &params, false);
            } else if policy == Overlap::Queue && !held {
                tracing::info!(job = %job, expr = %expr, "fire deferred: run still active");
                note_runless_tick(&runner, &job, &expr, t, TickOutcome::Deferred);
            } else {
                tracing::info!(job = %job, expr = %expr, "fire skipped: run still active");
                note_runless_tick(&runner, &job, &expr, t, TickOutcome::Skipped);
            }
        }
    }
}

fn note_runless_tick(
    runner: &Runner,
    job: &str,
    expr: &str,
    due: DateTime<Utc>,
    outcome: TickOutcome,
) {
    if let Err(e) = runner
        .store()
        .record_tick(job, expr, due, outcome, false, None, None)
    {
        tracing::warn!(job = %job, "tick write failed: {e}");
    }
}

/// launch one occurrence and record what happened to it. `caught_up` says the
/// occurrence came due while nothing was running to fire it — the tick row
/// cannot tell the two apart, and the event kind does.
fn note_tick(
    runner: &Runner,
    job: &str,
    expr: &str,
    due: DateTime<Utc>,
    params: &Value,
    caught_up: bool,
) {
    let tick = match runner.launch_at(job, params.clone(), Trigger::Schedule, Some(due)) {
        Ok(run_id) => runner.store().record_tick(
            job,
            expr,
            due,
            TickOutcome::Fired,
            caught_up,
            Some(&run_id),
            None,
        ),
        Err(err) => {
            tracing::error!(job = %job, error = %err, "scheduled launch failed");
            runner.store().record_tick(
                job,
                expr,
                due,
                TickOutcome::Error,
                caught_up,
                None,
                Some(&err.to_string()),
            )
        }
    };
    if let Err(e) = tick {
        tracing::warn!(job = %job, "tick write failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use crate::model::Tick;
    use crate::op::Op;
    use crate::store::Store;
    use chrono::Timelike;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    fn stored(store: &Store) -> HashMap<(String, String), ScheduleRow> {
        rows(store).expect("the schedules table is readable")
    }

    fn echo_params_job(name: &str, seen: Arc<Mutex<Vec<serde_json::Value>>>) -> Job {
        Job::builder(name)
            .op(Op::new("echo", move |ctx: crate::op::OpCtx| {
                let seen = seen.clone();
                async move {
                    seen.lock().unwrap().push(ctx.params().clone());
                    Ok(json!(null))
                }
            }))
            .build()
            .unwrap()
    }

    fn nap_job(name: &str, ms: u64, overlap: Overlap) -> Job {
        Job::builder(name)
            .overlap(overlap)
            .op(Op::new("nap", move |_| async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(json!(null))
            }))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn skip_policy_holds_to_one_run_and_records_ticks() {
        let store = Store::open(":memory:").unwrap();
        let runner =
            crate::Runner::new([nap_job("slow", 2500, Overlap::Skip)], store.clone()).unwrap();
        let entry = parse("slow", "* * * * * *", "UTC").unwrap();
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(2600)).await;
        sched.abort();

        let runs = store.runs(None, None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        let ticks = store.ticks(Some("slow"), 10).unwrap();
        assert_eq!(
            ticks
                .iter()
                .filter(|t| t.outcome == TickOutcome::Fired)
                .count(),
            1
        );
        assert!(ticks.iter().any(|t| t.outcome == TickOutcome::Skipped));
    }

    #[tokio::test]
    async fn queue_policy_defers_then_catches_up() {
        let store = Store::open(":memory:").unwrap();
        let runner =
            crate::Runner::new([nap_job("q", 1500, Overlap::Queue)], store.clone()).unwrap();
        let entry = parse("q", "* * * * * *", "UTC").unwrap();
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(4500)).await;
        sched.abort();

        let mut runs = store.runs(None, None, None, None, None, 20).unwrap();
        assert!(
            runs.len() >= 2,
            "expected a deferred catch-up run, got {}",
            runs.len()
        );
        let ticks = store.ticks(Some("q"), 20).unwrap();
        let deferred_fired = ticks
            .iter()
            .filter(|t| t.outcome == TickOutcome::Fired)
            .any(|t| (t.fired_at - t.scheduled_for).num_milliseconds() >= 400);
        assert!(deferred_fired, "no fired tick shows a deferral gap");
        let (held, caught_up) = ticks
            .iter()
            .filter(|t| t.outcome == TickOutcome::Deferred)
            .find_map(|d| {
                ticks
                    .iter()
                    .find(|f| f.outcome == TickOutcome::Fired && f.scheduled_for == d.scheduled_for)
                    .map(|f| (d, f))
            })
            .expect("no deferred tick paired with a fired catch-up");
        assert_eq!(held.run_id, None);
        assert!(
            held.id < caught_up.id,
            "deferred tick must precede its fired twin"
        );
        runs.sort_by_key(|r| r.created_at);
        for pair in runs.windows(2) {
            let prev_end = pair[0].finished_at.expect("finished");
            assert!(
                pair[1].created_at >= prev_end,
                "runs overlapped under queue policy"
            );
        }
    }

    #[tokio::test]
    async fn same_instant_twin_schedules_fire_once_and_record_skip() {
        let store = Store::open(":memory:").unwrap();
        // Allow, so any skipped tick can only come from the same-pass dedupe
        let runner =
            crate::Runner::new([nap_job("twin", 10, Overlap::Allow)], store.clone()).unwrap();
        let e1 = parse("twin", "* * * * * *", "UTC").unwrap();
        let e2 = parse("twin", "*/1 * * * * *", "UTC").unwrap();
        let sched = tokio::spawn(run_scheduler(vec![e1, e2], runner));
        tokio::time::sleep(Duration::from_millis(1100)).await;
        sched.abort();

        let ticks = store.ticks(Some("twin"), 20).unwrap();
        let fired: Vec<_> = ticks
            .iter()
            .filter(|t| t.outcome == TickOutcome::Fired)
            .collect();
        assert!(!fired.is_empty(), "no fire in over a second");
        for f in &fired {
            assert!(
                ticks.iter().any(|s| s.outcome == TickOutcome::Skipped
                    && s.scheduled_for == f.scheduled_for
                    && s.expr != f.expr),
                "no skipped twin tick for {}",
                f.scheduled_for
            );
        }
        let runs = store.runs(None, None, None, None, None, 20).unwrap();
        assert_eq!(runs.len(), fired.len());
    }

    #[tokio::test]
    async fn a_fire_launches_with_the_schedules_params() {
        let store = Store::open(":memory:").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let runner =
            crate::Runner::new([echo_params_job("p", seen.clone())], store.clone()).unwrap();
        let entry = parse("p", "* * * * * *", "UTC")
            .unwrap()
            .with_params(json!({"region": "eu"}));
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(1300)).await;
        sched.abort();

        let runs = store.runs(None, None, None, None, None, 10).unwrap();
        assert!(!runs.is_empty(), "no fire in over a second");
        assert!(runs.iter().all(|r| r.params == json!({"region": "eu"})));
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "no op ran");
        assert!(seen.iter().all(|p| *p == json!({"region": "eu"})));
    }

    #[tokio::test]
    async fn a_deferred_fire_keeps_the_params_it_was_held_with() {
        let store = Store::open(":memory:").unwrap();
        let runner =
            crate::Runner::new([nap_job("q", 1500, Overlap::Queue)], store.clone()).unwrap();
        let entry = parse("q", "* * * * * *", "UTC")
            .unwrap()
            .with_params(json!({"batch": 7}));
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(4500)).await;
        sched.abort();

        let ticks = store.ticks(Some("q"), 20).unwrap();
        assert!(
            ticks.iter().any(|t| t.outcome == TickOutcome::Deferred),
            "no fire was ever deferred"
        );
        let runs = store.runs(None, None, None, None, None, 20).unwrap();
        assert!(
            runs.len() >= 2,
            "expected a deferred catch-up run, got {}",
            runs.len()
        );
        assert!(runs.iter().all(|r| r.params == json!({"batch": 7})));
    }

    // ---- the durable cursor -------------------------------------------

    const HOURLY: &str = "0 * * * *";

    fn hourly_entry(job: &str, catchup: Catchup) -> ScheduleEntry {
        parse(job, HOURLY, "UTC").unwrap().with_catchup(catchup)
    }

    /// a store with one hourly schedule whose cursor is planted in the past —
    /// which is exactly what a process that was down for `hours` leaves behind.
    fn down_for(store: &Store, hours: i64, catchup: Catchup) -> (DateTime<Utc>, DateTime<Utc>) {
        use chrono::TimeZone;
        let now = Utc.with_ymd_and_hms(2026, 3, 4, 10, 30, 0).unwrap();
        let cursor = now - chrono::Duration::hours(hours);
        store
            .sync_schedules(&[Schedule::new("etl", HOURLY).catchup(catchup)])
            .unwrap();
        store.set_schedule_cursor("etl", HOURLY, cursor).unwrap();
        (cursor, now)
    }

    fn cursor_of(store: &Store) -> Option<DateTime<Utc>> {
        store.schedules().unwrap()[0].cursor
    }

    fn ticks_oldest_first(store: &Store) -> Vec<Tick> {
        let mut ticks = store.ticks(Some("etl"), 100).unwrap();
        ticks.reverse();
        ticks
    }

    async fn wait_idle(store: &Store, job: &str) {
        for _ in 0..300 {
            if !store.has_active_run(job).unwrap() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{job} never went idle");
    }

    #[tokio::test]
    async fn downtime_with_skip_fires_nothing_and_advances_the_cursor() {
        let store = Store::open(":memory:").unwrap();
        let runner = crate::Runner::new([nap_job("etl", 5, Overlap::Skip)], store.clone()).unwrap();
        let (_, now) = down_for(&store, 3, Catchup::Skip);
        let entry = hourly_entry("etl", Catchup::Skip);

        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);

        assert!(
            store
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(store.ticks(Some("etl"), 10).unwrap().is_empty());
        // 07:30 to 10:30 swallowed 08:00, 09:00 and 10:00; the cursor now says
        // so, which is the whole point — without it the next boot would have
        // no idea any of them existed
        assert_eq!(cursor_of(&store), Some(now - chrono::Duration::minutes(30)));

        // and a second pass has nothing left to find
        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);
        assert!(
            store
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn downtime_with_one_fires_only_the_latest_missed() {
        let store = Store::open(":memory:").unwrap();
        let runner = crate::Runner::new([nap_job("etl", 5, Overlap::Skip)], store.clone()).unwrap();
        let (_, now) = down_for(&store, 3, Catchup::One);
        let entry = hourly_entry("etl", Catchup::One);

        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);

        let ten = now - chrono::Duration::minutes(30);
        let runs = store.runs(None, None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].scheduled_for, Some(ten));
        assert_eq!(runs[0].trigger, Trigger::Schedule);
        let ticks = ticks_oldest_first(&store);
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].outcome, TickOutcome::Fired);
        assert_eq!(ticks[0].scheduled_for, ten);
        assert_eq!(cursor_of(&store), Some(ten));
    }

    #[tokio::test]
    async fn downtime_with_all_fires_each_missed_occurrence_oldest_first() {
        let store = Store::open(":memory:").unwrap();
        let runner = crate::Runner::new([nap_job("etl", 5, Overlap::Skip)], store.clone()).unwrap();
        let (_, now) = down_for(&store, 3, Catchup::All { limit: 10 });
        let entry = hourly_entry("etl", Catchup::All { limit: 10 });
        let at = |h: i64| now - chrono::Duration::minutes(30) - chrono::Duration::hours(h);

        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);
        // one launches now, the rest are on disk as held fires: catch-up
        // queues a backlog, it never starts three runs of one job at once
        let ticks = ticks_oldest_first(&store);
        assert_eq!(ticks[0].outcome, TickOutcome::Fired);
        assert_eq!(ticks[0].scheduled_for, at(2));
        assert_eq!(
            ticks[1..]
                .iter()
                .map(|t| (t.outcome, t.scheduled_for))
                .collect::<Vec<_>>(),
            [
                (TickOutcome::Deferred, at(1)),
                (TickOutcome::Deferred, at(0))
            ]
        );

        for _ in 0..2 {
            wait_idle(&store, "etl").await;
            drain_pending(
                &[hourly_entry("etl", Catchup::All { limit: 10 })],
                &runner,
                &stored(&store),
            );
        }
        wait_idle(&store, "etl").await;

        let mut runs = store.runs(None, None, None, None, None, 10).unwrap();
        runs.sort_by_key(|r| r.created_at);
        assert_eq!(
            runs.iter().map(|r| r.scheduled_for).collect::<Vec<_>>(),
            [Some(at(2)), Some(at(1)), Some(at(0))],
            "a backlog runs oldest first, one at a time"
        );
        assert_eq!(cursor_of(&store), Some(at(0)));
    }

    #[tokio::test]
    async fn catchup_all_drops_the_oldest_past_the_cap_and_says_which() {
        let store = Store::open(":memory:").unwrap();
        let runner = crate::Runner::new([nap_job("etl", 5, Overlap::Skip)], store.clone()).unwrap();
        let (_, now) = down_for(&store, 6, Catchup::All { limit: 2 });
        let entry = hourly_entry("etl", Catchup::All { limit: 2 });
        let at = |h: i64| now - chrono::Duration::minutes(30) - chrono::Duration::hours(h);

        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);

        let ticks = ticks_oldest_first(&store);
        // six missed, two allowed: the four oldest are dropped, and the drop
        // leaves a record rather than a silence
        let dropped = &ticks[0];
        assert_eq!(dropped.outcome, TickOutcome::Skipped);
        assert_eq!(dropped.scheduled_for, at(5));
        let msg = dropped.error.as_deref().unwrap();
        assert!(msg.contains("catch-up cap 2"), "{msg}");
        assert!(msg.contains("dropped 4 missed occurrences"), "{msg}");
        assert_eq!(
            ticks[1..]
                .iter()
                .map(|t| (t.outcome, t.scheduled_for))
                .collect::<Vec<_>>(),
            [(TickOutcome::Fired, at(1)), (TickOutcome::Deferred, at(0))]
        );
        assert_eq!(cursor_of(&store), Some(at(0)));
    }

    #[tokio::test]
    async fn a_caught_up_run_knows_the_hour_it_is_for() {
        let store = Store::open(":memory:").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let job = Job::builder("etl")
            .op(Op::new("pull", move |ctx: crate::op::OpCtx| {
                let sink = sink.clone();
                async move {
                    sink.lock().unwrap().push(ctx.scheduled_for());
                    Ok(json!(null))
                }
            }))
            .build()
            .unwrap();
        let runner = crate::Runner::new([job], store.clone()).unwrap();
        let (_, now) = down_for(&store, 1, Catchup::One);
        let entry = hourly_entry("etl", Catchup::One);

        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);
        wait_idle(&store, "etl").await;

        // the run launched at 10:30 for the 10:00 hour, and the op is told so:
        // a catch-up that could not say which hour it was for would be no use
        // to anything that pulls data *for* an hour
        let ten = now - chrono::Duration::minutes(30);
        assert_eq!(*seen.lock().unwrap(), [Some(ten)]);
        let runs = store.runs(None, None, None, None, None, 10).unwrap();
        assert_eq!(runs[0].scheduled_for, Some(ten));
        assert_eq!(
            serde_json::to_value(&runs[0]).unwrap()["scheduled_for"],
            json!(ten)
        );

        // a manual launch stands for nothing but itself
        runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), [Some(ten), None]);
    }

    #[tokio::test]
    async fn the_cursor_and_a_held_fire_both_survive_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        use chrono::TimeZone;
        let now = Utc.with_ymd_and_hms(2026, 3, 4, 10, 30, 0).unwrap();
        let held = Utc.with_ymd_and_hms(2026, 3, 4, 9, 0, 0).unwrap();

        // process one: a cursor written by a catch-up, and a fire held because
        // the job was busy. both go to disk and the process then dies
        let store = Store::open(&path).unwrap();
        store
            .sync_schedules(&[Schedule::new("etl", HOURLY).catchup(Catchup::One)])
            .unwrap();
        store
            .set_schedule_cursor("etl", HOURLY, now - chrono::Duration::hours(3))
            .unwrap();
        let entry = hourly_entry("etl", Catchup::One);
        let runner =
            crate::Runner::new([nap_job("etl", 5, Overlap::Queue)], store.clone()).unwrap();
        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);
        // process one's own run finishes before it dies: the boot sweep in
        // process two respects a live claim now, and a run left mid-flight in
        // this same process is exactly what a live claim looks like
        wait_idle(&store, "etl").await;
        store
            .record_tick(
                "etl",
                HOURLY,
                held,
                TickOutcome::Deferred,
                false,
                None,
                None,
            )
            .unwrap();
        let cursor = cursor_of(&store);
        assert_eq!(cursor, Some(now - chrono::Duration::minutes(30)));
        assert_eq!(
            store.pending_fires().unwrap(),
            [("etl".to_string(), HOURLY.to_string(), held)]
        );
        drop(runner);
        drop(store);

        // process two: nothing in memory, everything read back off disk
        let store = Store::open(&path).unwrap();
        // the boot sweep a real restart runs, so the run process one left
        // mid-flight is not mistaken for one still going
        store.fail_interrupted().unwrap();
        let runner =
            crate::Runner::new([nap_job("etl", 5, Overlap::Queue)], store.clone()).unwrap();
        store
            .sync_schedules(&[Schedule::new("etl", HOURLY).catchup(Catchup::One)])
            .unwrap();
        assert_eq!(cursor_of(&store), cursor, "the sync must not reset it");

        let entry = hourly_entry("etl", Catchup::One);
        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);
        assert!(
            store
                .runs(Some("etl"), None, None, None, None, 10)
                .unwrap()
                .len()
                <= 1,
            "the cursor already accounted for those occurrences"
        );

        drain_pending(&[entry], &runner, &stored(&store));
        wait_idle(&store, "etl").await;
        let held_run = store
            .runs(None, None, None, None, None, 10)
            .unwrap()
            .into_iter()
            .find(|r| r.scheduled_for == Some(held));
        assert!(
            held_run.is_some(),
            "a queue-policy fire held before the restart was lost with the process"
        );
        assert!(store.pending_fires().unwrap().is_empty());
    }

    #[tokio::test]
    async fn pausing_advances_the_cursor_without_firing() {
        let store = Store::open(":memory:").unwrap();
        let runner = crate::Runner::new([nap_job("etl", 5, Overlap::Skip)], store.clone()).unwrap();
        let (_, now) = down_for(&store, 4, Catchup::All { limit: 10 });
        store
            .set_schedule_paused("etl", HOURLY, true, None)
            .unwrap();
        let entry = hourly_entry("etl", Catchup::All { limit: 10 });

        catch_up(&entry, &runner, stored(&store).get(&entry.key()), now);

        // pause means stop, including the catch-up: resuming must not fire a
        // week of backlog at whatever the schedule was paused for
        assert!(
            store
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(store.ticks(Some("etl"), 10).unwrap().is_empty());
        assert_eq!(cursor_of(&store), Some(now - chrono::Duration::minutes(30)));
    }

    #[test]
    fn five_field_expr_normalized() {
        let e = parse("etl", "*/2 * * * *", "UTC").unwrap();
        assert_eq!(e.expr, "*/2 * * * *");
        let next = e.schedule.upcoming(Utc).next().unwrap();
        assert_eq!(next.second(), 0);
    }

    #[test]
    fn posix_numeric_dow_remapped() {
        use chrono::{Datelike, Weekday};
        let e = parse("etl", "0 9 * * 1", "UTC").unwrap();
        assert_eq!(
            e.schedule.upcoming(Utc).next().unwrap().weekday(),
            Weekday::Mon
        );
        // 0 is valid crontab for sunday; the cron crate alone rejects it
        let e = parse("etl", "0 9 * * 0", "UTC").unwrap();
        assert_eq!(
            e.schedule.upcoming(Utc).next().unwrap().weekday(),
            Weekday::Sun
        );
    }

    #[test]
    fn dow_ranges_and_names_survive() {
        use chrono::{Datelike, Weekday};
        let e = parse("etl", "0 9 * * 1-5", "UTC").unwrap();
        let days: HashSet<Weekday> = e
            .schedule
            .upcoming(Utc)
            .take(10)
            .map(|t| t.weekday())
            .collect();
        assert!(days.contains(&Weekday::Mon) && days.contains(&Weekday::Fri));
        assert!(!days.contains(&Weekday::Sat) && !days.contains(&Weekday::Sun));
        let e = parse("etl", "0 9 * * MON", "UTC").unwrap();
        assert_eq!(
            e.schedule.upcoming(Utc).next().unwrap().weekday(),
            Weekday::Mon
        );
    }

    #[test]
    fn bad_expr_yields_cron_error() {
        let err = parse("etl", "not a cron", "UTC").unwrap_err();
        match err {
            Error::Cron { expr, .. } => assert_eq!(expr, "not a cron"),
            other => panic!("expected cron error, got {other:?}"),
        }
    }

    #[test]
    fn timezone_applied_to_next_fire() {
        let e = parse("etl", "0 9 * * *", "America/New_York").unwrap();
        assert_eq!(e.tz, chrono_tz::America::New_York);
        let next = e.schedule.upcoming(e.tz).next().unwrap();
        assert_eq!(next.hour(), 9);
        assert_ne!(next.with_timezone(&Utc).hour(), 9);
    }

    #[test]
    fn upcoming_fires_windowed_and_capped() {
        use chrono::TimeZone;
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 30, 0).unwrap();

        let hourly = parse("etl", "0 * * * *", "UTC").unwrap();
        let fires = upcoming_fires(&hourly, now, 3600);
        assert_eq!(fires, [Utc.with_ymd_and_hms(2026, 1, 1, 1, 0, 0).unwrap()]);
        assert!(upcoming_fires(&hourly, now, 60).is_empty());

        let minutely = parse("etl", "* * * * *", "UTC").unwrap();
        assert_eq!(upcoming_fires(&minutely, now, 86400).len(), 100);
    }

    #[test]
    fn bad_timezone_yields_timezone_error() {
        let err = parse("etl", "0 9 * * *", "Mars/Olympus").unwrap_err();
        assert_eq!(err.to_string(), "unknown timezone: Mars/Olympus");
        match err {
            Error::Timezone(tz) => assert_eq!(tz, "Mars/Olympus"),
            other => panic!("expected timezone error, got {other:?}"),
        }
    }

    #[test]
    fn fall_back_hour_still_fires() {
        use chrono::TimeZone;
        // 2026-11-01 america/new_york repeats 01:00-01:59. cron 0.12 dropped the
        // ambiguous hour entirely; 0.17 fires it at both offsets.
        let e = parse("etl", "0 * * * *", "America/New_York").unwrap();
        let start = Utc.with_ymd_and_hms(2026, 11, 1, 3, 30, 0).unwrap(); // 23:30 edt
        let fires: Vec<_> = e
            .schedule
            .after(&start.with_timezone(&e.tz))
            .take(4)
            .map(|t| t.with_timezone(&Utc))
            .collect();
        assert_eq!(
            fires,
            [
                Utc.with_ymd_and_hms(2026, 11, 1, 4, 0, 0).unwrap(), // 00:00 edt
                Utc.with_ymd_and_hms(2026, 11, 1, 5, 0, 0).unwrap(), // 01:00 edt
                Utc.with_ymd_and_hms(2026, 11, 1, 6, 0, 0).unwrap(), // 01:00 est
                Utc.with_ymd_and_hms(2026, 11, 1, 7, 0, 0).unwrap(), // 02:00 est
            ]
        );

        let e = parse("etl", "30 1 * * *", "America/New_York").unwrap();
        let first = e
            .schedule
            .after(&start.with_timezone(&e.tz))
            .next()
            .map(|t| t.with_timezone(&Utc));
        // fires on the transition day, not 2026-11-02
        assert_eq!(
            first,
            Some(Utc.with_ymd_and_hms(2026, 11, 1, 5, 30, 0).unwrap())
        );
    }

    #[tokio::test]
    async fn an_unreadable_paused_flag_holds_every_schedule() {
        let store = Store::open(":memory:").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let runner =
            crate::Runner::new([echo_params_job("etl", seen.clone())], store.clone()).unwrap();
        let entry = parse("etl", "* * * * * *", "UTC").unwrap();
        // the paused flag lives in this table, so nothing can be read about it
        store.drop_table("schedules").unwrap();

        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(1800)).await;
        sched.abort();

        assert!(
            store
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty(),
            "a paused flag nobody could read let a schedule fire"
        );
        assert!(seen.lock().unwrap().is_empty());
        assert!(store.ticks(Some("etl"), 10).unwrap().is_empty());
    }
}
