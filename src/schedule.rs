use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use chrono::Utc;
use chrono_tz::Tz;
use serde_json::{Value, json};

use crate::error::Error;
use crate::executor::Runner;
use crate::model::{Overlap, TickOutcome, Trigger};
use crate::store::Store;

#[derive(Debug)]
pub(crate) struct ScheduleEntry {
    pub job: String,
    pub expr: String,
    pub tz: Tz,
    pub schedule: cron::Schedule,
    /// what every fire launches with; `{}` unless the declaration set it.
    pub params: Value,
}

impl ScheduleEntry {
    pub(crate) fn with_params(mut self, params: Value) -> ScheduleEntry {
        self.params = params;
        self
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
    now: chrono::DateTime<Utc>,
    window_secs: i64,
) -> Vec<chrono::DateTime<Utc>> {
    let end = now + chrono::Duration::seconds(window_secs);
    entry
        .schedule
        .after(&now.with_timezone(&entry.tz))
        .map(|t| t.with_timezone(&Utc))
        .take_while(|t| *t <= end)
        .take(100)
        .collect()
}

fn paused_set(store: &Store) -> HashSet<(String, String)> {
    match store.schedules() {
        Ok(rows) => rows
            .into_iter()
            .filter(|r| r.paused)
            .map(|r| (r.job, r.expr))
            .collect(),
        Err(e) => {
            tracing::warn!("schedule read failed: {e}");
            HashSet::new()
        }
    }
}

pub(crate) async fn run_scheduler(mut entries: Vec<ScheduleEntry>, runner: Runner) {
    if entries.is_empty() {
        return;
    }
    // queue-policy fires wait here until their job frees up, one per job. the
    // params are the ones captured when the fire was held, not the ones the
    // declaration carries by the time it launches
    let mut deferred: HashMap<String, (String, chrono::DateTime<Utc>, Value)> = HashMap::new();
    loop {
        let paused_now = if deferred.is_empty() {
            HashSet::new()
        } else {
            paused_set(runner.store())
        };
        deferred.retain(|job, (expr, due, params)| {
            if paused_now.contains(&(job.clone(), expr.clone())) {
                tracing::info!(job = %job, expr = %expr, "deferred fire dropped: schedule paused");
                note_runless_tick(&runner, job, expr, *due, TickOutcome::Skipped);
                return false;
            }
            match runner.store().has_active_run(job) {
                Ok(true) => true,
                Ok(false) => {
                    tracing::info!(job = %job, expr = %expr, "deferred fire launching");
                    note_tick(&runner, job, expr, *due, params);
                    false
                }
                Err(e) => {
                    tracing::warn!(job = %job, "active-run check failed: {e}");
                    true
                }
            }
        });

        let mut fires: Vec<(chrono::DateTime<Utc>, String, String, Value)> = Vec::new();
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
        if fires.is_empty() && deferred.is_empty() {
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
        let cap = if deferred.is_empty() {
            Duration::from_secs(60)
        } else {
            Duration::from_secs(2)
        };
        if !until.is_zero() {
            tokio::time::sleep(until.min(cap)).await;
            let now = Utc::now();
            if !fires.iter().any(|(t, ..)| *t <= now) {
                continue;
            }
        }
        let now = Utc::now();
        let paused = paused_set(runner.store());
        // two schedules on the same job sharing a tick fire one run, not two
        let mut fired: HashSet<String> = HashSet::new();
        for (t, job, expr, params) in fires {
            if t > now || paused.contains(&(job.clone(), expr.clone())) {
                continue;
            }
            if !fired.insert(job.clone()) {
                // the runner-up's fire was real; leave a tick, not silence
                note_runless_tick(&runner, &job, &expr, t, TickOutcome::Skipped);
                continue;
            }
            let active = deferred.contains_key(&job)
                || matches!(runner.store().has_active_run(&job), Ok(true) | Err(_));
            let policy = runner
                .jobs()
                .get(&job)
                .map(|j| j.overlap())
                .unwrap_or_default();
            if !active || policy == Overlap::Allow {
                tracing::info!(job = %job, expr = %expr, "schedule fired");
                note_tick(&runner, &job, &expr, t, &params);
            } else if policy == Overlap::Queue && !deferred.contains_key(&job) {
                tracing::info!(job = %job, expr = %expr, "fire deferred: run still active");
                // the wait itself lives in memory; this tick is its durable trace
                note_runless_tick(&runner, &job, &expr, t, TickOutcome::Deferred);
                deferred.insert(job.clone(), (expr.clone(), t, params));
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
    due: chrono::DateTime<Utc>,
    outcome: TickOutcome,
) {
    if let Err(e) = runner
        .store()
        .record_tick(job, expr, due, outcome, None, None)
    {
        tracing::warn!(job = %job, "tick write failed: {e}");
    }
}

fn note_tick(runner: &Runner, job: &str, expr: &str, due: chrono::DateTime<Utc>, params: &Value) {
    let tick = match runner.launch(job, params.clone(), Trigger::Schedule) {
        Ok(run_id) => {
            runner
                .store()
                .record_tick(job, expr, due, TickOutcome::Fired, Some(&run_id), None)
        }
        Err(err) => {
            tracing::error!(job = %job, error = %err, "scheduled launch failed");
            runner.store().record_tick(
                job,
                expr,
                due,
                TickOutcome::Error,
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
    use crate::op::Op;
    use crate::store::Store;
    use chrono::Timelike;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

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
        let runner = crate::Runner::new([nap_job("slow", 2500, Overlap::Skip)], store.clone());
        let entry = parse("slow", "* * * * * *", "UTC").unwrap();
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(2600)).await;
        sched.abort();

        let runs = store.runs(None, None, None, None, 10).unwrap();
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
        let runner = crate::Runner::new([nap_job("q", 1500, Overlap::Queue)], store.clone());
        let entry = parse("q", "* * * * * *", "UTC").unwrap();
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(4500)).await;
        sched.abort();

        let mut runs = store.runs(None, None, None, None, 20).unwrap();
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
        let runner = crate::Runner::new([nap_job("twin", 10, Overlap::Allow)], store.clone());
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
        let runs = store.runs(None, None, None, None, 20).unwrap();
        assert_eq!(runs.len(), fired.len());
    }

    #[tokio::test]
    async fn a_fire_launches_with_the_schedules_params() {
        let store = Store::open(":memory:").unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let runner = crate::Runner::new([echo_params_job("p", seen.clone())], store.clone());
        let entry = parse("p", "* * * * * *", "UTC")
            .unwrap()
            .with_params(json!({"region": "eu"}));
        let sched = tokio::spawn(run_scheduler(vec![entry], runner));
        tokio::time::sleep(Duration::from_millis(1300)).await;
        sched.abort();

        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert!(!runs.is_empty(), "no fire in over a second");
        assert!(runs.iter().all(|r| r.params == json!({"region": "eu"})));
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "no op ran");
        assert!(seen.iter().all(|p| *p == json!({"region": "eu"})));
    }

    #[tokio::test]
    async fn a_deferred_fire_keeps_the_params_it_was_held_with() {
        let store = Store::open(":memory:").unwrap();
        let runner = crate::Runner::new([nap_job("q", 1500, Overlap::Queue)], store.clone());
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
        let runs = store.runs(None, None, None, None, 20).unwrap();
        assert!(
            runs.len() >= 2,
            "expected a deferred catch-up run, got {}",
            runs.len()
        );
        assert!(runs.iter().all(|r| r.params == json!({"batch": 7})));
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
}
