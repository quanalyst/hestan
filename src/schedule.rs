use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use serde_json::{Value, json};

use crate::asset::AssetRegistry;
use crate::error::Error;
use crate::executor::{Launched, Runner};
use crate::model::{Catchup, Overlap, ScheduleRow, TickOutcome};
use crate::store::{Fire, Store};

/// how far back a catch-up scan will walk to enumerate missed occurrences. a
/// seconds-resolution schedule down for a month is millions of them, and
/// nothing downstream wants that list; past this the drop is reported as "at
/// least" rather than counted exactly.
const MAX_MISSED_SCAN: usize = 10_000;

/// how long a pass waits before trying the paused flag again, after a read of
/// it failed. the pass fired nothing, so this is the whole cost of the retry.
const CONTROL_READ_RETRY: Duration = Duration::from_secs(1);

/// the prefix an [asset schedule](Schedule::asset) is stored under.
///
/// a schedule is keyed on `(what it fires, expr)`: that pair is the row's
/// primary key, what a pause names, and the unique index that makes one
/// occurrence fire once. an asset schedule fires a run of the internal
/// `assets` job, so keying it on the job it launches would make every asset
/// scheduled at 06:00 one schedule. it is keyed on the asset instead, in the
/// same column, behind this prefix so that the two kinds cannot collide.
pub(crate) const ASSET_PREFIX: &str = "asset:";

/// what a [`Schedule`] fires when its expression comes round.
///
/// **one type in one list.** both kinds are the same `ScheduleEntry` in the
/// same `schedules` table, ticked by the same loop, caught up by the same
/// cursor and the same [`Catchup`] rules. what differs is one call at the
/// moment of firing, which is what keeps "when does this happen" a single
/// mechanism to reason about and a single place a fire can be lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// a run of this job, with the schedule's params.
    Job(String),
    /// a build of this asset, planned the way every other build of it is.
    Asset(String),
}

impl Target {
    /// the string this schedule is stored and keyed under.
    pub(crate) fn id(&self) -> String {
        match self {
            Target::Job(job) => job.clone(),
            Target::Asset(asset) => format!("{ASSET_PREFIX}{asset}"),
        }
    }

    /// the target a stored id names, which is how a row read back becomes a
    /// declaration again.
    pub(crate) fn from_id(id: &str) -> Target {
        match id.strip_prefix(ASSET_PREFIX) {
            Some(asset) => Target::Asset(asset.to_string()),
            None => Target::Job(id.to_string()),
        }
    }

    /// `job orders_etl` / `asset vendor_prices`: what this schedule fires, in
    /// a sentence a person is reading.
    pub(crate) fn label(&self) -> String {
        match self {
            Target::Job(job) => format!("job {job}"),
            Target::Asset(asset) => format!("asset {asset}"),
        }
    }

    /// the asset this fires a build of, or `None` for a job schedule.
    pub(crate) fn asset(&self) -> Option<&str> {
        match self {
            Target::Asset(asset) => Some(asset),
            Target::Job(_) => None,
        }
    }
}

/// one schedule as the code declares it: what it fires, which cron expression,
/// and the three things that used to be positional arguments.
///
/// ```no_run
/// # use hestan::{Catchup, Hestan, Schedule};
/// # use serde_json::json;
/// Hestan::new()
///     .add_schedule(
///         Schedule::new("orders_etl", "0 * * * *")
///             .tz("Europe/London")
///             .params(json!({"region": "eu"}))
///             .catchup(Catchup::All { limit: 24 }),
///     )
///     .add_schedule(Schedule::asset("vendor_prices", "0 6 * * *"));
/// ```
///
/// [`Hestan::schedule`](crate::Hestan::schedule) and friends are this with the
/// common defaults filled in: utc, `{}`, [`Catchup::Skip`].
#[derive(Debug, Clone)]
pub struct Schedule {
    pub(crate) target: Target,
    pub(crate) expr: String,
    pub(crate) tz: String,
    pub(crate) params: Value,
    pub(crate) catchup: Catchup,
}

impl Schedule {
    /// `cron_expr` is a 5-field crontab expression, evaluated in utc until
    /// [`tz`](Self::tz) says otherwise.
    pub fn new(job: impl Into<String>, cron_expr: impl Into<String>) -> Schedule {
        Schedule::on(Target::Job(job.into()), cron_expr)
    }

    /// a cron that **builds an asset**: it, plus whatever upstream of it is
    /// stale, exactly as `hestan build <asset>` and
    /// `POST /api/assets/{name}/build` plan it.
    ///
    /// this is the clock an [automation policy](crate::AutoPolicy) is not. a
    /// policy reacts to staleness, which answers "rebuild when what it is made
    /// of moved". a schedule answers "build at 06:00, because that is when the
    /// vendor publishes", which is a fact about the world rather than about
    /// the graph. [`AutoPolicy::after_cron`](crate::AutoPolicy::after_cron) is
    /// the two together and is still the right answer when you want the clock
    /// *and* the staleness check; this one builds when the hour comes round.
    ///
    /// the run is [`Trigger::Schedule`](crate::Trigger::Schedule), carries the
    /// occurrence it fired for on
    /// [`scheduled_for`](crate::OpCtx::scheduled_for), and is tagged with the
    /// asset, so the asset page lists it beside a build somebody asked for.
    ///
    /// **it takes no params**: a build's params are `{}`, so
    /// [`params`](Self::params) on one of these is refused at startup rather
    /// than dropped on the floor.
    ///
    /// **an unknown asset and a source asset are refused at startup**, from
    /// `serve` and from `run_once`, the way a job schedule's params are
    /// validated against the job's ops. a schedule that could never fire is a
    /// build error, not a 3am tick.
    ///
    /// **[`Catchup::All`](crate::Catchup::All) is refused**, also at startup.
    /// catching up fires each missed occurrence for its own logical time, and
    /// a build has no logical time to be for: the first one makes the asset
    /// fresh, so the other two would plan nothing. [`Catchup::One`] is the one
    /// that means anything here ("we were down, build now"), and
    /// [`Catchup::Skip`] is the default.
    ///
    /// **a partitioned asset builds its default target set**, which is the
    /// stale and missing keys, newest first, capped by the set's
    /// [build limit](crate::Partitions::build_limit). not "the latest key" and
    /// not "every stale key at once": it is the same set a build that names no
    /// keys already takes, because it is planned by the same function.
    pub fn asset(asset: impl Into<String>, cron_expr: impl Into<String>) -> Schedule {
        Schedule::on(Target::Asset(asset.into()), cron_expr)
    }

    fn on(target: Target, cron_expr: impl Into<String>) -> Schedule {
        Schedule {
            target,
            expr: cron_expr.into(),
            tz: "UTC".into(),
            params: json!({}),
            catchup: Catchup::default(),
        }
    }

    /// the string this schedule is stored and keyed under: the job's name, or
    /// the asset's behind [`ASSET_PREFIX`].
    pub(crate) fn id(&self) -> String {
        self.target.id()
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
    /// what this schedule is stored and keyed under: the job's name, or the
    /// asset's behind [`ASSET_PREFIX`]. every identity site (the row, the
    /// cursor, the pause flag, the tick log, the one-fire-per-occurrence
    /// index) uses this and nothing else.
    pub id: String,
    /// what firing it does, which is the only place the two kinds differ.
    pub target: Target,
    pub expr: String,
    pub tz: Tz,
    pub schedule: cron::Schedule,
    /// what every fire launches with; `{}` unless the declaration set it.
    pub params: Value,
    /// what downtime does to this schedule. it comes from the declaration and
    /// is stored for the api rather than read back from it: the row is synced
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
        (self.id.clone(), self.expr.clone())
    }
}

/// a cron expression and the timezone it is read in, parsed once.
///
/// a [`Schedule`] is one of these on a job and an
/// [automation policy](crate::AutoPolicy) is one on an asset. both want the
/// same crontab dialect and the same "when did this last come round" answer,
/// so both get them here rather than from two parsers that could drift.
#[derive(Debug, Clone)]
pub(crate) struct Cron {
    pub schedule: cron::Schedule,
    pub tz: Tz,
}

impl Cron {
    pub(crate) fn parse(expr: &str, tz: &str) -> Result<Cron, Error> {
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
        Ok(Cron { schedule, tz })
    }

    /// the most recent occurrence strictly before `now`; `None` when it has not
    /// come due yet at all.
    pub(crate) fn last_before(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        last_before(&self.schedule, self.tz, now)
    }
}

/// one entry from a stored row: `id` is the column, and the target it names is
/// read back out of it rather than stored twice.
pub(crate) fn parse(id: &str, expr: &str, tz: &str) -> Result<ScheduleEntry, Error> {
    let cron = Cron::parse(expr, tz)?;
    Ok(ScheduleEntry {
        id: id.to_string(),
        target: Target::from_id(id),
        expr: expr.to_string(),
        tz: cron.tz,
        schedule: cron.schedule,
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
/// schedule is paused", an administrative stop that fails open. the caller
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
fn last_before(schedule: &cron::Schedule, tz: Tz, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    schedule
        .after(&now.with_timezone(&tz))
        .next_back()
        .map(|t| t.with_timezone(&Utc))
}

/// occurrences strictly after `cursor` and strictly before `now`, newest
/// first, at most `MAX_MISSED_SCAN` of them; the flag says the scan hit that
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
fn catch_up(
    entry: &ScheduleEntry,
    runner: &Runner,
    registry: Option<&AssetRegistry>,
    row: Option<&ScheduleRow>,
    now: DateTime<Utc>,
) {
    let Some(cursor) = row.and_then(|r| r.cursor) else {
        // first sight of this schedule: there is no downtime to reconstruct,
        // and treating its whole cron past as missed would fire the epoch
        advance(runner, entry, now);
        return;
    };
    let Some(last) = last_before(&entry.schedule, entry.tz, now).filter(|t| *t > cursor) else {
        return;
    };
    // pause means stop, including the catch-up: a schedule paused for a week
    // must not fire a week of backlog the moment it is resumed
    if row.is_some_and(|r| r.paused) {
        tracing::info!(job = %entry.id, expr = %entry.expr, "paused: cursor advanced over the gap");
        advance(runner, entry, last);
        return;
    }
    match entry.catchup {
        Catchup::Skip => {
            tracing::info!(job = %entry.id, expr = %entry.expr, "missed fires skipped to {last}");
        }
        Catchup::One => {
            tracing::info!(job = %entry.id, expr = %entry.expr, "catching up the latest missed fire");
            queue_fire(runner, registry, entry, last);
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
                tracing::warn!(job = %entry.id, expr = %entry.expr, "{msg}");
                // one skipped tick carrying the reason, at the oldest dropped
                // occurrence: a tick per drop would bury the log it belongs in
                if let Err(e) = runner.store().record_tick(
                    &entry.id,
                    &entry.expr,
                    *oldest,
                    TickOutcome::Skipped,
                    false,
                    None,
                    Some(&msg),
                ) {
                    tracing::warn!(job = %entry.id, "tick write failed: {e}");
                }
            }
            for t in fire.iter().rev() {
                queue_fire(runner, registry, entry, *t);
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
        .set_schedule_cursor(&entry.id, &entry.expr, at)
    {
        tracing::warn!(job = %entry.id, "cursor write failed: {e}");
    }
}

/// whether what this schedule fires is already going.
///
/// a job schedule asks the run log, which is what [`Overlap`] is about. an
/// asset schedule does not ask anything: a build is refused by the claim it
/// takes in the same transaction as its run row, so a read here would be a
/// second opinion that could disagree with the only one that decides. a fire
/// refused that way lands as a skipped tick carrying the reason, which is
/// where a job schedule's refusal lands too.
fn busy(runner: &Runner, entry: &ScheduleEntry) -> bool {
    match &entry.target {
        Target::Job(job) => matches!(runner.store().has_active_run(job), Ok(true) | Err(_)),
        Target::Asset(_) => false,
    }
}

/// launch a caught-up occurrence, or record it as held when what it fires is
/// busy or something older is already waiting. either way the occurrence is on
/// disk before this returns, which is what makes a backlog survive a second
/// restart.
fn queue_fire(
    runner: &Runner,
    registry: Option<&AssetRegistry>,
    entry: &ScheduleEntry,
    at: DateTime<Utc>,
) {
    let free = !busy(runner, entry) && !has_pending(runner, &entry.id);
    match free {
        true => note_tick(runner, registry, entry, at, true),
        false => note_runless_tick(runner, &entry.id, &entry.expr, at, TickOutcome::Deferred),
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

/// launch whatever is waiting, oldest first and one per schedule target. the
/// queue is the tick log (a `deferred` tick with no later tick for the same
/// occurrence), so a fire held when the process died is still held when it
/// comes back.
fn drain_pending(
    entries: &[ScheduleEntry],
    runner: &Runner,
    registry: Option<&AssetRegistry>,
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
        let Some(entry) = entries.iter().find(|e| &e.id == job && &e.expr == expr) else {
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
        if busy(runner, entry) {
            launched.insert(job.as_str());
            continue;
        }
        tracing::info!(job = %job, expr = %expr, "held fire launching");
        note_tick(runner, registry, entry, *at, true);
        launched.insert(job.as_str());
    }
}

/// the loop `serve` runs: catch up, drain, then fire whatever comes due.
///
/// `registry` is what an [asset schedule](Schedule::asset) plans its build
/// from, and is `None` in a deployment that declares no assets. it is handed
/// in rather than read off the runner for the same reason the policy pass and
/// the backfill chunker take it: the runner holds jobs, and the asset graph
/// the `assets` job was lowered from is not one.
pub(crate) async fn run_scheduler(
    mut entries: Vec<ScheduleEntry>,
    runner: Runner,
    registry: Option<Arc<AssetRegistry>>,
) {
    if entries.is_empty() {
        return;
    }
    let registry = registry.as_deref();
    loop {
        // nothing below this line happens on a process that is not the
        // decider: no cursor moves, no tick is written and no run is launched.
        // it waits rather than polls, so a handover starts the next pass at
        // the instant the lease is taken
        runner.deciding().wait().await;
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
            catch_up(entry, &runner, registry, stored.get(&entry.key()), now);
        }
        drain_pending(&entries, &runner, registry, &stored);
        let waiting = runner
            .store()
            .pending_fires()
            .map(|p| !p.is_empty())
            .unwrap_or(false);

        let mut fires: Vec<(DateTime<Utc>, String, String)> = Vec::new();
        entries.retain(|e| match e.schedule.upcoming(e.tz).next() {
            Some(t) => {
                fires.push((t.with_timezone(&Utc), e.id.clone(), e.expr.clone()));
                true
            }
            None => {
                tracing::warn!(job = %e.id, expr = %e.expr, "schedule has no future fires, dropping it");
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
        // asked again after the sleep, because a lease can lapse inside one:
        // the fires below are the writes, and a pass that decided it was the
        // decider a minute ago is not evidence about now
        if !runner.may_decide() {
            continue;
        }
        let now = Utc::now();
        // same again for the fires this pass is about to make: an occurrence
        // left un-accounted for is one the next pass's catch-up will see, and
        // catch-up honours the paused flag it can read by then
        let Some(stored) = rows(runner.store()) else {
            tokio::time::sleep(CONTROL_READ_RETRY).await;
            continue;
        };
        // two schedules on one target sharing a tick fire one run, not two.
        // keyed on what the schedule fires, so two assets due at 06:00 are two
        // builds and two schedules on one asset are one
        let mut fired: HashSet<String> = HashSet::new();
        for (t, job, expr) in fires {
            if t > now {
                continue;
            }
            let entry = entries
                .iter()
                .find(|e| e.id == job && e.expr == expr)
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
            let active = held || busy(&runner, entry);
            let policy = runner
                .jobs()
                .get(&job)
                .map(|j| j.overlap())
                .unwrap_or_default();
            if !active || policy == Overlap::Allow {
                tracing::info!(job = %job, expr = %expr, "schedule fired");
                note_tick(&runner, registry, entry, t, false);
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

/// what a refused fire leaves in the tick log, so the refusal is visible where
/// somebody is already looking rather than only in this process's stderr.
const ALREADY_FIRED: &str = "another process had already fired this occurrence";

/// and what a fire refused by the [term fence](crate::Store::decider) is. it
/// leaves no tick: nothing fired, and the occurrence is downtime as far as the
/// next decider's catch-up is concerned, which is the honest thing for it to
/// be.
const NOT_DECIDING: &str = "the deciding lease moved on before the fire landed";

/// and what an [asset schedule](Schedule::asset) whose asset is already fresh
/// leaves. nothing was owed, so nothing was built, and that is an outcome
/// rather than a failure.
const NOTHING_STALE: &str = "nothing to build: the asset and its upstream are fresh";

/// launch one occurrence and record what happened to it. `caught_up` says the
/// occurrence came due while nothing was running to fire it: the tick row
/// cannot tell the two apart, and the event kind does.
///
/// the tick and the run are one transaction, so this either launches a run and
/// records the fire or does neither. a refusal is the store saying the
/// occurrence already has a fire against it, which on a single scheduler cannot
/// happen and beside a second one is exactly what stops the second run.
///
/// **both kinds of schedule fire through the one function below.** a job
/// schedule launches the job with its params; an asset schedule plans the
/// build the api and the command line plan and launches that. what they share
/// is everything else: the claim on the occurrence, the tick, the refusals and
/// what each refusal is recorded as.
fn note_tick(
    runner: &Runner,
    registry: Option<&AssetRegistry>,
    entry: &ScheduleEntry,
    due: DateTime<Utc>,
    caught_up: bool,
) {
    let (job, expr) = (entry.id.as_str(), entry.expr.as_str());
    let fire = Fire {
        job,
        expr,
        scheduled_for: due,
        caught_up,
    };
    let launched = match (&entry.target, registry) {
        (Target::Job(name), _) => runner.fire_scheduled(name, fire, entry.params.clone()),
        (Target::Asset(asset), Some(reg)) => crate::asset::fire_build(runner, reg, asset, fire),
        // unreachable: an asset schedule is refused at startup unless the
        // asset is registered, and a registered asset means a registry. said
        // rather than unwrapped, because a fire that vanished silently is the
        // thing the tick log exists to make impossible
        (Target::Asset(asset), None) => Err(Error::UnknownAsset(asset.clone())),
    };
    let tick = match launched {
        // an asset schedule whose asset is already fresh: there was nothing to
        // build, so no run exists and the occurrence is recorded as skipped
        // saying so. not an error, and not silence
        Ok(None) => {
            tracing::info!(job = %job, expr = %expr, "fire built nothing: {NOTHING_STALE}");
            runner.store().record_tick(
                job,
                expr,
                due,
                TickOutcome::Skipped,
                caught_up,
                None,
                Some(NOTHING_STALE),
            )
        }
        Ok(Some(launched)) => return note_launched(runner, job, expr, due, caught_up, launched),
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

/// what the launch came to, recorded.
fn note_launched(
    runner: &Runner,
    job: &str,
    expr: &str,
    due: DateTime<Utc>,
    caught_up: bool,
    launched: Launched,
) {
    let tick = match launched {
        // the fire and its tick landed together; nothing more to record. a
        // fire carries a cron occurrence and never a launch key, so the keyed
        // answer is grouped here rather than panicked over: a run exists
        // either way and there is nothing more for a tick to say
        Launched::Queued(_) | Launched::Repeat(_) => Ok(()),
        Launched::Taken => {
            tracing::info!(job = %job, expr = %expr, "fire refused: {ALREADY_FIRED}");
            runner.store().record_tick(
                job,
                expr,
                due,
                TickOutcome::Skipped,
                caught_up,
                None,
                Some(ALREADY_FIRED),
            )
        }
        // this process stopped being the decider mid-pass. nothing was
        // written, including no tick: the occurrence is un-accounted for, and
        // the new decider's catch-up is what will account for it
        Launched::Stale => {
            tracing::warn!(job = %job, expr = %expr, "fire refused: {NOT_DECIDING}");
            Ok(())
        }
        // an asset schedule whose build meets one already running. the
        // occurrence is accounted for, saying which asset and which run, and
        // the asset is still stale for the next pass to pick up
        Launched::Overlaps { what, run: held } => {
            let msg = format!("{what} is already being built by run {held}");
            tracing::info!(job = %job, expr = %expr, "fire refused: {msg}");
            runner.store().record_tick(
                job,
                expr,
                due,
                TickOutcome::Skipped,
                caught_up,
                None,
                Some(&msg),
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
    use crate::model::{Tick, Trigger};
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
        let sched = tokio::spawn(run_scheduler(vec![entry], runner, None));
        tokio::time::sleep(Duration::from_millis(2600)).await;
        sched.abort();

        let runs = store.runs(None, None, None, None, None, None, 10).unwrap();
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

    // ---------------------------------------------------------- asset
    // schedules

    /// two assets over one source, so a plan has more than one op in it and
    /// "the same ops" is worth asserting.
    fn priced() -> Arc<crate::asset::AssetRegistry> {
        use crate::asset::Asset;
        let vendor = Asset::source("vendor");
        let prices = Asset::new("prices", |_| async { Ok(json!({"rows": 3})) }).from(&vendor);
        let report = Asset::new("report", |ctx: crate::OpCtx| async move {
            let rows = ctx.input("prices").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"lines": rows}))
        })
        .from(&prices);
        Arc::new(
            crate::asset::AssetRegistry::new(vec![vendor, prices, report], Vec::new(), Vec::new())
                .unwrap(),
        )
    }

    /// the probe row a source would have written; the write api is
    /// crate-internal, and a source that has never been seen makes everything
    /// under it unbuildable rather than stale
    fn seen(store: &Store, asset: &str, fingerprint: &str) {
        store
            .record_materialization(asset, None, fingerprint, &json!({}), None, None, None)
            .unwrap();
    }

    // the point of the feature: a cron on an asset builds it, as a run of the
    // assets job triggered by the schedule and tagged with the asset, and the
    // run carries the occurrence it stands for
    #[tokio::test]
    async fn an_asset_schedule_fires_a_build_of_that_asset() {
        let store = Store::open(":memory:").unwrap();
        let reg = priced();
        let runner = crate::Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        seen(&store, "vendor", "v1");

        let entry = parse("asset:report", "0 6 * * *", "UTC").unwrap();
        let at = "2026-03-04T06:00:00Z".parse::<DateTime<Utc>>().unwrap();
        note_tick(&runner, Some(&reg), &entry, at, false);

        let runs = store.runs(None, None, None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        let run = &runs[0];
        assert_eq!(run.job, crate::asset::ASSETS_JOB);
        // what asked was a cron, and that is what the trigger is for
        assert_eq!(run.trigger, crate::Trigger::Schedule);
        assert_eq!(run.scheduled_for, Some(at));
        // tagged like every other build, so the asset page finds it without
        // being taught about the combination
        assert_eq!(run.tags["asset"], "report");

        let ticks = store.ticks(Some("asset:report"), 10).unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].outcome, TickOutcome::Fired);
        assert_eq!(ticks[0].run_id.as_deref(), Some(run.id.as_str()));
    }

    // and it is the *same* plan: a third caller that planned a build its own
    // way would make "build this asset" mean two things
    #[tokio::test]
    async fn an_asset_schedule_plans_exactly_what_build_one_plans() {
        let reg = priced();

        let fired = Store::open(":memory:").unwrap();
        let by_cron = crate::Runner::new([reg.lower_job().unwrap()], fired.clone()).unwrap();
        seen(&fired, "vendor", "v1");
        let entry = parse("asset:report", "0 6 * * *", "UTC").unwrap();
        let at = "2026-03-04T06:00:00Z".parse::<DateTime<Utc>>().unwrap();
        note_tick(&by_cron, Some(&reg), &entry, at, false);

        let asked = Store::open(":memory:").unwrap();
        let by_hand = crate::Runner::new([reg.lower_job().unwrap()], asked.clone()).unwrap();
        seen(&asked, "vendor", "v1");
        let id = crate::asset::build_one(&by_hand, &reg, "report", &[])
            .unwrap()
            .unwrap();

        let ops = |store: &Store, run: &str| {
            let mut names: Vec<String> = store
                .op_runs(run)
                .unwrap()
                .into_iter()
                .map(|o| o.op)
                .collect();
            names.sort();
            names
        };
        let cron_runs = fired.runs(None, None, None, None, None, None, 10).unwrap();
        assert_eq!(
            cron_runs.len(),
            1,
            "the cron launched no run; ticks: {:?}",
            fired
                .ticks(Some("asset:report"), 10)
                .unwrap()
                .iter()
                .map(|t| (t.outcome, t.error.clone()))
                .collect::<Vec<_>>()
        );
        let cron_run = cron_runs[0].id.clone();
        assert_eq!(ops(&fired, &cron_run), ops(&asked, &id));
        assert_eq!(ops(&fired, &cron_run), ["prices", "report"]);
    }

    // nothing owed is not a failure and not silence: the occurrence is
    // accounted for, saying why no run came of it
    #[tokio::test]
    async fn an_asset_schedule_whose_asset_is_fresh_records_why_it_built_nothing() {
        let store = Store::open(":memory:").unwrap();
        let reg = priced();
        let runner = crate::Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        seen(&store, "vendor", "v1");
        // build it once, by hand, so the cron finds nothing left to do
        let id = crate::asset::build_one(&runner, &reg, "report", &[])
            .unwrap()
            .unwrap();
        wait_idle(&store, crate::asset::ASSETS_JOB).await;
        assert_eq!(
            store.run(&id).unwrap().unwrap().status,
            crate::RunStatus::Success
        );

        let entry = parse("asset:report", "0 6 * * *", "UTC").unwrap();
        let at = "2026-03-04T06:00:00Z".parse::<DateTime<Utc>>().unwrap();
        note_tick(&runner, Some(&reg), &entry, at, false);

        assert_eq!(
            store
                .runs(None, None, None, None, None, None, 10)
                .unwrap()
                .len(),
            1,
            "the cron launched a second run of an asset that was fresh"
        );
        let ticks = store.ticks(Some("asset:report"), 10).unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].outcome, TickOutcome::Skipped);
        assert_eq!(ticks[0].error.as_deref(), Some(NOTHING_STALE));
    }

    /// `priced()` with the build of `prices` slow enough to still be going
    /// when something fires beside it, plus an asset over a source of its own
    /// that nothing in that chain touches, so a plan for one and a plan for
    /// the other have nothing in common.
    fn priced_and_beside() -> Arc<crate::asset::AssetRegistry> {
        use crate::asset::Asset;
        let vendor = Asset::source("vendor");
        let prices = Asset::new("prices", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!({"rows": 3}))
        })
        .from(&vendor);
        let report = Asset::new("report", |_| async { Ok(json!({"lines": 3})) }).from(&prices);
        let feed = Asset::source("feed");
        let stock = Asset::new("stock", |_| async { Ok(json!({"rows": 1})) }).from(&feed);
        Arc::new(
            crate::asset::AssetRegistry::new(
                vec![vendor, prices, report, feed, stock],
                Vec::new(),
                Vec::new(),
            )
            .unwrap(),
        )
    }

    // a cron on an asset writes a run row saying what it will materialize, so
    // it has to be refused by what is already materializing it: two runs
    // building one asset from two plans is the whole thing the claim exists to
    // stop. the occurrence is accounted for, saying which asset and which run
    #[tokio::test]
    async fn an_asset_schedule_firing_into_an_intersecting_build_is_refused() {
        let store = Store::open(":memory:").unwrap();
        let reg = priced_and_beside();
        let runner = crate::Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        seen(&store, "vendor", "v1");
        seen(&store, "feed", "f1");

        // a build somebody asked for, still going: `prices` does not finish
        let held = crate::asset::build_one(&runner, &reg, "report", &[])
            .unwrap()
            .unwrap();

        let entry = parse("asset:report", "0 6 * * *", "UTC").unwrap();
        let at = "2026-03-04T06:00:00Z".parse::<DateTime<Utc>>().unwrap();
        note_tick(&runner, Some(&reg), &entry, at, false);

        let runs = store.runs(None, None, None, None, None, None, 10).unwrap();
        assert_eq!(
            runs.len(),
            1,
            "the cron launched a second run over a build already under way"
        );
        assert_eq!(runs[0].id, held);

        let ticks = store.ticks(Some("asset:report"), 10).unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].outcome, TickOutcome::Skipped);
        assert_eq!(ticks[0].run_id, None);
        let why = ticks[0].error.clone().unwrap();
        assert!(
            why.contains(&format!("is already being built by run {held}")),
            "the refusal does not name the run holding the claim: {why}"
        );
        assert!(
            why.starts_with("prices ") || why.starts_with("report "),
            "the refusal does not name an asset both plans build: {why}"
        );
    }

    // and it is an intersection rather than a lock on the assets job: a cron
    // building something the run under way does not touch fires as usual
    #[tokio::test]
    async fn an_asset_schedule_fires_beside_a_build_it_shares_nothing_with() {
        let store = Store::open(":memory:").unwrap();
        let reg = priced_and_beside();
        let runner = crate::Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        seen(&store, "vendor", "v1");
        seen(&store, "feed", "f1");

        let held = crate::asset::build_one(&runner, &reg, "report", &[])
            .unwrap()
            .unwrap();

        let entry = parse("asset:stock", "0 6 * * *", "UTC").unwrap();
        let at = "2026-03-04T06:00:00Z".parse::<DateTime<Utc>>().unwrap();
        note_tick(&runner, Some(&reg), &entry, at, false);

        let ticks = store.ticks(Some("asset:stock"), 10).unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].outcome, TickOutcome::Fired);
        let fired = ticks[0].run_id.clone().expect("a fired tick names its run");
        assert_ne!(fired, held);
        assert_eq!(
            store.run(&fired).unwrap().unwrap().tags["asset"],
            "stock",
            "the cron built something other than what it was declared on"
        );
    }

    // one list, one loop, one table: the two kinds are the same entry, and a
    // deployment with both ticks both without either knowing about the other
    #[tokio::test]
    async fn a_job_schedule_and_an_asset_schedule_both_tick_in_one_deployment() {
        let store = Store::open(":memory:").unwrap();
        let reg = priced();
        let runner = crate::Runner::new(
            [
                reg.lower_job().unwrap(),
                echo_params_job("etl", Arc::new(Mutex::new(Vec::new()))),
            ],
            store.clone(),
        )
        .unwrap();
        seen(&store, "vendor", "v1");

        let entries = vec![
            parse("etl", "* * * * * *", "UTC").unwrap(),
            parse("asset:prices", "* * * * * *", "UTC").unwrap(),
        ];
        let sched = tokio::spawn(run_scheduler(entries, runner, Some(reg)));
        tokio::time::sleep(Duration::from_millis(2600)).await;
        sched.abort();

        let fired = |id: &str| {
            store
                .ticks(Some(id), 20)
                .unwrap()
                .into_iter()
                .filter(|t| t.outcome == TickOutcome::Fired)
                .count()
        };
        assert!(fired("etl") >= 1, "the job schedule never fired");
        assert!(fired("asset:prices") >= 1, "the asset schedule never fired");
        // and the two are two schedules, not one: they share an expression and
        // the id is what keeps their occurrences apart
        let runs = store.runs(None, None, None, None, None, None, 20).unwrap();
        assert!(runs.iter().any(|r| r.job == "etl"));
        assert!(runs.iter().any(|r| r.job == crate::asset::ASSETS_JOB));
    }

    #[tokio::test]
    async fn queue_policy_defers_then_catches_up() {
        let store = Store::open(":memory:").unwrap();
        let runner =
            crate::Runner::new([nap_job("q", 1500, Overlap::Queue)], store.clone()).unwrap();
        let entry = parse("q", "* * * * * *", "UTC").unwrap();
        let sched = tokio::spawn(run_scheduler(vec![entry], runner, None));
        tokio::time::sleep(Duration::from_millis(4500)).await;
        sched.abort();

        let mut runs = store.runs(None, None, None, None, None, None, 20).unwrap();
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
        let sched = tokio::spawn(run_scheduler(vec![e1, e2], runner, None));
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
        let runs = store.runs(None, None, None, None, None, None, 20).unwrap();
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
        let sched = tokio::spawn(run_scheduler(vec![entry], runner, None));
        tokio::time::sleep(Duration::from_millis(1300)).await;
        sched.abort();

        let runs = store.runs(None, None, None, None, None, None, 10).unwrap();
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
        let sched = tokio::spawn(run_scheduler(vec![entry], runner, None));
        tokio::time::sleep(Duration::from_millis(4500)).await;
        sched.abort();

        let ticks = store.ticks(Some("q"), 20).unwrap();
        assert!(
            ticks.iter().any(|t| t.outcome == TickOutcome::Deferred),
            "no fire was ever deferred"
        );
        let runs = store.runs(None, None, None, None, None, None, 20).unwrap();
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

    /// a store with one hourly schedule whose cursor is planted in the past,
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

        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);

        assert!(
            store
                .runs(None, None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
        assert!(store.ticks(Some("etl"), 10).unwrap().is_empty());
        // 07:30 to 10:30 swallowed 08:00, 09:00 and 10:00; the cursor now says
        // so, which is the whole point: without it the next boot would have
        // no idea any of them existed
        assert_eq!(cursor_of(&store), Some(now - chrono::Duration::minutes(30)));

        // and a second pass has nothing left to find
        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);
        assert!(
            store
                .runs(None, None, None, None, None, None, 10)
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

        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);

        let ten = now - chrono::Duration::minutes(30);
        let runs = store.runs(None, None, None, None, None, None, 10).unwrap();
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

        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);
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
                None,
                &stored(&store),
            );
        }
        wait_idle(&store, "etl").await;

        let mut runs = store.runs(None, None, None, None, None, None, 10).unwrap();
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

        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);

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

        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);
        wait_idle(&store, "etl").await;

        // the run launched at 10:30 for the 10:00 hour, and the op is told so:
        // a catch-up that could not say which hour it was for would be no use
        // to anything that pulls data *for* an hour
        let ten = now - chrono::Duration::minutes(30);
        assert_eq!(*seen.lock().unwrap(), [Some(ten)]);
        let runs = store.runs(None, None, None, None, None, None, 10).unwrap();
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
        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);
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
        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);
        assert!(
            store
                .runs(Some("etl"), None, None, None, None, None, 10)
                .unwrap()
                .len()
                <= 1,
            "the cursor already accounted for those occurrences"
        );

        drain_pending(&[entry], &runner, None, &stored(&store));
        wait_idle(&store, "etl").await;
        let held_run = store
            .runs(None, None, None, None, None, None, 10)
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

        catch_up(&entry, &runner, None, stored(&store).get(&entry.key()), now);

        // pause means stop, including the catch-up: resuming must not fire a
        // week of backlog at whatever the schedule was paused for
        assert!(
            store
                .runs(None, None, None, None, None, None, 10)
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

        let sched = tokio::spawn(run_scheduler(vec![entry], runner, None));
        tokio::time::sleep(Duration::from_millis(1800)).await;
        sched.abort();

        assert!(
            store
                .runs(None, None, None, None, None, None, 10)
                .unwrap()
                .is_empty(),
            "a paused flag nobody could read let a schedule fire"
        );
        assert!(seen.lock().unwrap().is_empty());
        assert!(store.ticks(Some("etl"), 10).unwrap().is_empty());
    }
}
