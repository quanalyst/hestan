use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::asset::{
    ASSETS_JOB, AssetRegistry, ProbeFn, launch_plan, mats_map, plan_targets, staleness,
};
use crate::executor::Runner;
use crate::model::{Run, RunCursor, RunStatus, SensorOutcome, Trigger};
use crate::op::InputError;

/// what a sensor evaluation asks for: launch `job` with `params`.
pub struct RunRequest {
    pub job: String,
    pub params: Value,
}

type SensorFn = dyn Fn(
        SensorCtx,
    ) -> BoxFuture<'static, Result<Vec<RunRequest>, Box<dyn std::error::Error + Send + Sync>>>
    + Send
    + Sync;

/// a polling closure evaluated on an interval: it inspects the world (a
/// directory, a queue, an api) and returns the runs to launch — usually none.
/// register with `Hestan::sensor`; `serve` runs the loop.
pub struct Sensor {
    name: String,
    every: Duration,
    f: Arc<SensorFn>,
}

impl Sensor {
    pub fn new<F, Fut>(name: impl Into<String>, every: Duration, f: F) -> Sensor
    where
        F: Fn(SensorCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<RunRequest>, Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        Sensor {
            name: name.into(),
            every,
            f: Arc::new(move |ctx| Box::pin(f(ctx))),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// how many terminal runs one [`RunStatusSensor`] evaluation reads. a sensor
/// that was paused for a week has a backlog, and draining it a page at a time
/// keeps one tick from launching thousands of runs at once.
const RUN_SENSOR_PAGE: u32 = 200;

/// what a [`RunStatusSensor`] closure is handed about a run that just
/// finished. deliberately a small public struct and not the internal `Run`:
/// what a sensor needs to decide is the identity, the job, how it went and
/// when — not the params blob or the resume chain.
#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub id: String,
    pub job: String,
    pub status: RunStatus,
    pub trigger: Trigger,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// the first op that terminally failed, named; `None` unless it failed.
    pub error: Option<String>,
}

impl From<&Run> for RunSummary {
    fn from(run: &Run) -> RunSummary {
        RunSummary {
            id: run.id.clone(),
            job: run.job.clone(),
            status: run.status,
            trigger: run.trigger,
            started_at: run.started_at,
            finished_at: run.finished_at,
            error: run.error.clone(),
        }
    }
}

type RunSensorFn = dyn Fn(
        SensorCtx,
        RunSummary,
    ) -> BoxFuture<'static, Result<Vec<RunRequest>, Box<dyn std::error::Error + Send + Sync>>>
    + Send
    + Sync;

/// "when job A succeeds, run job B". a sensor whose world is the run log: each
/// evaluation reads the terminal runs it has not seen, calls the closure once
/// per run, and launches whatever comes back.
///
/// ```no_run
/// # use hestan::{Hestan, RunRequest, RunStatus, RunStatusSensor, RunSummary};
/// # use serde_json::json;
/// # use std::time::Duration;
/// Hestan::new().run_sensor(
///     RunStatusSensor::new("chain", |_ctx, run: RunSummary| async move {
///         Ok(vec![RunRequest {
///             job: "publish".into(),
///             params: json!({ "from": run.id }),
///         }])
///     })
///     .on([RunStatus::Success])
///     .for_job("orders_etl")
///     .every(Duration::from_secs(15)),
/// );
/// ```
///
/// it registers as `run:{name}` in the sensors table, so it shares pausing,
/// tick history and cursor storage with every other sensor — there is one
/// sensor loop, and this is a third kind of source on it rather than a fourth
/// loop.
///
/// **a chain can feed itself.** a closure that launches the job whose run
/// triggered it is legal and will run forever: the launched run finishes, the
/// sensor sees it, and round it goes. narrow it with
/// [`for_job`](Self::for_job), a status filter, or a check inside the closure.
pub struct RunStatusSensor {
    name: String,
    every: Duration,
    statuses: Vec<RunStatus>,
    job: Option<String>,
    f: Arc<RunSensorFn>,
}

impl RunStatusSensor {
    /// `f` is called once per matching run, with the same [`SensorCtx`] a user
    /// sensor gets — though the cursor is the loop's here, and
    /// [`set_cursor`](SensorCtx::set_cursor) has no effect on a run sensor.
    pub fn new<F, Fut>(name: impl Into<String>, f: F) -> RunStatusSensor
    where
        F: Fn(SensorCtx, RunSummary) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<RunRequest>, Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        RunStatusSensor {
            name: name.into(),
            every: Duration::from_secs(15),
            statuses: vec![RunStatus::Success],
            job: None,
            f: Arc::new(move |ctx, run| Box::pin(f(ctx, run))),
        }
    }

    /// which terminal statuses fire it; success by default. an empty list
    /// means success, not nothing — a sensor that can never fire is a typo.
    pub fn on(mut self, statuses: impl IntoIterator<Item = RunStatus>) -> RunStatusSensor {
        let statuses: Vec<RunStatus> = statuses.into_iter().collect();
        if !statuses.is_empty() {
            self.statuses = statuses;
        }
        self
    }

    /// watch one job only; without it, every job in the process.
    pub fn for_job(mut self, job: impl Into<String>) -> RunStatusSensor {
        self.job = Some(job.into());
        self
    }

    /// how often it reads the run log (default 15s).
    pub fn every(mut self, every: Duration) -> RunStatusSensor {
        self.every = every;
        self
    }

    /// the name it is registered under, `run:{name}`.
    pub fn sensor_name(&self) -> String {
        format!("run:{}", self.name)
    }
}

/// handed to each sensor evaluation: the committed cursor, and `set_cursor` to
/// stage a new one. a staged cursor is persisted only if the whole evaluation
/// succeeds, so a failed one re-reads the old cursor next time.
pub struct SensorCtx {
    cursor: Option<Value>,
    new_cursor: Arc<Mutex<Option<Value>>>,
}

impl SensorCtx {
    /// the cursor the last fully-successful evaluation committed.
    pub fn cursor(&self) -> Option<&Value> {
        self.cursor.as_ref()
    }

    /// [`cursor`](Self::cursor) deserialized into `T`; `Ok(None)` when no
    /// cursor has ever been committed.
    pub fn cursor_as<T: DeserializeOwned>(&self) -> Result<Option<T>, InputError> {
        match &self.cursor {
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| InputError::Mismatch(e.to_string())),
            None => Ok(None),
        }
    }

    /// stage a cursor to commit if this evaluation fully succeeds. the last
    /// call wins.
    pub fn set_cursor(&self, v: Value) {
        *self.new_cursor.lock().unwrap() = Some(v);
    }
}

// probes are sensors, and so are run-status chains: a probed source becomes an
// entry named `probe:<asset>` and a chain becomes `run:<name>`. three sources,
// one loop, one set of ticks, one pause switch
pub(crate) enum SensorEval {
    User(Arc<SensorFn>),
    Probe {
        asset: String,
        probe: Arc<ProbeFn>,
    },
    Runs {
        statuses: Vec<RunStatus>,
        job: Option<String>,
        f: Arc<RunSensorFn>,
    },
}

pub(crate) struct SensorEntry {
    pub name: String,
    pub every: Duration,
    pub eval: SensorEval,
}

impl SensorEntry {
    pub(crate) fn user(sensor: Sensor) -> SensorEntry {
        SensorEntry {
            name: sensor.name,
            every: sensor.every,
            eval: SensorEval::User(sensor.f),
        }
    }

    pub(crate) fn runs(sensor: RunStatusSensor) -> SensorEntry {
        SensorEntry {
            name: sensor.sensor_name(),
            every: sensor.every,
            eval: SensorEval::Runs {
                statuses: sensor.statuses,
                job: sensor.job,
                f: sensor.f,
            },
        }
    }

    /// what this entry watches, for the api to show: `None` for a user sensor
    /// and a probe, which watch whatever their closure looks at.
    pub(crate) fn filter(&self) -> Option<serde_json::Value> {
        match &self.eval {
            SensorEval::Runs { statuses, job, .. } => Some(json!({
                "job": job,
                "statuses": statuses,
            })),
            _ => None,
        }
    }
}

/// the sensor loop: every entry evaluates at startup, then on its own interval.
/// paused entries are skipped without a tick and keep their schedule.
pub(crate) async fn run_sensors(
    entries: Vec<SensorEntry>,
    runner: Runner,
    registry: Arc<AssetRegistry>,
) {
    if entries.is_empty() {
        return;
    }
    let mut due: Vec<Instant> = vec![Instant::now(); entries.len()];
    loop {
        let (i, &at) = due
            .iter()
            .enumerate()
            .min_by_key(|(_, t)| **t)
            .expect("entries is non-empty");
        tokio::time::sleep_until(at).await;
        let entry = &entries[i];
        if !sensor_paused(&runner, &entry.name) {
            evaluate(entry, &runner, &registry).await;
        }
        // next due counts from evaluation end, so a slow closure never stacks
        due[i] = Instant::now() + entry.every;
    }
}

fn sensor_paused(runner: &Runner, name: &str) -> bool {
    match runner.store().sensors() {
        Ok(rows) => rows.into_iter().any(|r| r.name == name && r.paused),
        Err(e) => {
            tracing::warn!(sensor = %name, "sensor read failed: {e}");
            false
        }
    }
}

async fn evaluate(entry: &SensorEntry, runner: &Runner, registry: &AssetRegistry) {
    let (outcome, launched, error) = match &entry.eval {
        SensorEval::User(f) => evaluate_user(&entry.name, f, runner).await,
        SensorEval::Probe { asset, probe } => evaluate_probe(asset, probe, runner, registry).await,
        SensorEval::Runs { statuses, job, f } => {
            evaluate_runs(&entry.name, statuses, job.as_deref(), f, runner).await
        }
    };
    if let Err(e) =
        runner
            .store()
            .record_sensor_tick(&entry.name, outcome, launched, error.as_deref())
    {
        tracing::warn!(sensor = %entry.name, "tick write failed: {e}");
    }
}

async fn evaluate_user(
    name: &str,
    f: &Arc<SensorFn>,
    runner: &Runner,
) -> (SensorOutcome, u32, Option<String>) {
    let cursor = match runner.store().sensors() {
        Ok(rows) => rows
            .into_iter()
            .find(|r| r.name == name)
            .and_then(|r| r.cursor),
        Err(e) => {
            // a lost cursor read degrades to "no cursor": at-least-once, not a stall
            tracing::warn!(sensor = %name, "cursor read failed: {e}");
            None
        }
    };
    let new_cursor = Arc::new(Mutex::new(None));
    let ctx = SensorCtx {
        cursor,
        new_cursor: new_cursor.clone(),
    };
    // a panicking closure is an evaluation error, not a dead sensor loop
    let result = match AssertUnwindSafe(async { f(ctx).await })
        .catch_unwind()
        .await
    {
        Ok(Ok(requests)) => Ok(requests),
        Ok(Err(e)) => Err(e.to_string()),
        Err(panic) => Err(match panic_payload(panic.as_ref()) {
            Some(s) => format!("sensor panicked: {s}"),
            None => "sensor panicked".to_string(),
        }),
    };
    let requests = match result {
        Ok(r) => r,
        Err(msg) => {
            tracing::warn!(sensor = %name, "evaluation failed: {msg}");
            return (SensorOutcome::Error, 0, Some(msg));
        }
    };
    let mut launched = 0u32;
    for req in requests {
        match runner.launch(&req.job, req.params, Trigger::Sensor) {
            Ok(run_id) => {
                tracing::info!(sensor = %name, job = %req.job, run = %run_id, "sensor fired");
                launched += 1;
            }
            // a launch failure fails the evaluation: the cursor stays put
            Err(e) => {
                let msg = format!("launch of job {:?} failed: {e}", req.job);
                tracing::warn!(sensor = %name, "{msg}");
                return (SensorOutcome::Error, launched, Some(msg));
            }
        }
    }
    let staged = new_cursor.lock().unwrap().take();
    if let Some(c) = staged
        && let Err(e) = runner.store().set_sensor_cursor(name, &c)
    {
        tracing::warn!(sensor = %name, "cursor write failed: {e}");
        return (
            SensorOutcome::Error,
            launched,
            Some(format!("cursor write failed: {e}")),
        );
    }
    (SensorOutcome::Fired, launched, None)
}

/// one run-status evaluation: read the terminal runs past the cursor, hand each
/// matching one to the closure, launch what comes back, and only then commit
/// the cursor.
///
/// the cursor covers every run *read*, not every run matched, so a filtered-out
/// failure is consumed rather than re-read forever. and it is committed once at
/// the end: a launch that fails halfway leaves the cursor where it was, so the
/// runs already handled are handed over again next tick. that is the same
/// at-least-once contract a user sensor has, for the same reason — a sensor
/// that could lose an event would be worse than one that repeats it.
async fn evaluate_runs(
    name: &str,
    statuses: &[RunStatus],
    job: Option<&str>,
    f: &Arc<RunSensorFn>,
    runner: &Runner,
) -> (SensorOutcome, u32, Option<String>) {
    let stored = match sensor_cursor(runner, name) {
        Ok(c) => c,
        Err(msg) => return (SensorOutcome::Error, 0, Some(msg)),
    };
    let cursor: Option<RunCursor> = match &stored {
        Some(v) => match serde_json::from_value(v.clone()) {
            Ok(c) => Some(c),
            // an unreadable cursor is a sensor that would re-chain its whole
            // history; reseed from now and say so rather than do that
            Err(e) => {
                tracing::warn!(sensor = %name, "cursor unreadable, reseeding: {e}");
                None
            }
        },
        None => None,
    };
    let Some(cursor) = cursor else {
        // a new run sensor starts from now: it chains what happens next, not
        // the run log it was added to
        return match seed_cursor(runner, name, job) {
            Ok(()) => (SensorOutcome::Fired, 0, None),
            Err(msg) => (SensorOutcome::Error, 0, Some(msg)),
        };
    };
    let runs = match runner
        .store()
        .terminal_runs_after(job, Some(&cursor), RUN_SENSOR_PAGE)
    {
        Ok(r) => r,
        Err(e) => return (SensorOutcome::Error, 0, Some(e.to_string())),
    };
    let Some(last) = runs.last() else {
        return (SensorOutcome::Fired, 0, None);
    };
    let seen = RunCursor {
        finished_at: last.finished_at.expect("terminal runs carry a finish time"),
        id: last.id.clone(),
    };
    let mut launched = 0u32;
    for run in runs.iter().filter(|r| statuses.contains(&r.status)) {
        let ctx = SensorCtx {
            cursor: stored.clone(),
            new_cursor: Arc::new(Mutex::new(None)),
        };
        let summary = RunSummary::from(run);
        let result = match AssertUnwindSafe(async { f(ctx, summary).await })
            .catch_unwind()
            .await
        {
            Ok(Ok(requests)) => Ok(requests),
            Ok(Err(e)) => Err(e.to_string()),
            Err(panic) => Err(match panic_payload(panic.as_ref()) {
                Some(s) => format!("sensor panicked: {s}"),
                None => "sensor panicked".to_string(),
            }),
        };
        let requests = match result {
            Ok(r) => r,
            Err(msg) => {
                tracing::warn!(sensor = %name, run = %run.id, "evaluation failed: {msg}");
                return (SensorOutcome::Error, launched, Some(msg));
            }
        };
        for req in requests {
            match runner.launch(&req.job, req.params, Trigger::Sensor) {
                Ok(run_id) => {
                    tracing::info!(sensor = %name, job = %req.job, run = %run_id, "sensor fired");
                    launched += 1;
                }
                Err(e) => {
                    let msg = format!("launch of job {:?} failed: {e}", req.job);
                    tracing::warn!(sensor = %name, "{msg}");
                    return (SensorOutcome::Error, launched, Some(msg));
                }
            }
        }
    }
    let seen = serde_json::to_value(&seen).expect("RunCursor is json");
    if let Err(e) = runner.store().set_sensor_cursor(name, &seen) {
        tracing::warn!(sensor = %name, "cursor write failed: {e}");
        return (
            SensorOutcome::Error,
            launched,
            Some(format!("cursor write failed: {e}")),
        );
    }
    (SensorOutcome::Fired, launched, None)
}

fn sensor_cursor(runner: &Runner, name: &str) -> Result<Option<Value>, String> {
    runner
        .store()
        .sensors()
        .map(|rows| {
            rows.into_iter()
                .find(|r| r.name == name)
                .and_then(|r| r.cursor)
        })
        .map_err(|e| format!("cursor read failed: {e}"))
}

fn seed_cursor(runner: &Runner, name: &str, job: Option<&str>) -> Result<(), String> {
    let seed = runner
        .store()
        .latest_terminal_run(job)
        .map_err(|e| e.to_string())?
        .unwrap_or_else(|| RunCursor {
            // nothing has finished yet, so anything that finishes from here is
            // new. the epoch is the honest floor, not a run id
            finished_at: DateTime::<Utc>::MIN_UTC,
            id: String::new(),
        });
    let seed = serde_json::to_value(&seed).expect("RunCursor is json");
    runner
        .store()
        .set_sensor_cursor(name, &seed)
        .map_err(|e| format!("cursor write failed: {e}"))
}

async fn evaluate_probe(
    asset: &str,
    probe: &Arc<ProbeFn>,
    runner: &Runner,
    registry: &AssetRegistry,
) -> (SensorOutcome, u32, Option<String>) {
    let fingerprint = match AssertUnwindSafe(async { probe().await })
        .catch_unwind()
        .await
    {
        Ok(Ok(fp)) => fp,
        Ok(Err(e)) => return (SensorOutcome::Error, 0, Some(e.to_string())),
        Err(panic) => {
            let msg = match panic_payload(panic.as_ref()) {
                Some(s) => format!("probe panicked: {s}"),
                None => "probe panicked".to_string(),
            };
            return (SensorOutcome::Error, 0, Some(msg));
        }
    };
    let current = match runner.store().materialization(asset, None) {
        Ok(m) => m.map(|m| m.fingerprint),
        Err(e) => return (SensorOutcome::Error, 0, Some(e.to_string())),
    };
    if current.as_deref() != Some(fingerprint.as_str()) {
        tracing::info!(asset = %asset, "probe saw a new fingerprint");
        if let Err(e) = runner.store().record_materialization(
            asset,
            None,
            &fingerprint,
            &json!({}),
            None,
            None,
            None,
        ) {
            return (SensorOutcome::Error, 0, Some(e.to_string()));
        }
    }
    // changed or not: the fingerprint commits before any launch, so re-deriving
    // every tick is what heals a launch that failed after the commit
    match launch_stale_auto(asset, runner, registry) {
        Ok(launched) => (SensorOutcome::Fired, launched, None),
        Err(msg) => (SensorOutcome::Error, 0, Some(msg)),
    }
}

// one combined plan, never one per target: overlapping plans would share stale
// ancestors and race each other's lineage writes (assets.md)
fn launch_stale_auto(
    asset: &str,
    runner: &Runner,
    registry: &AssetRegistry,
) -> Result<u32, String> {
    let mats = mats_map(runner.store()).map_err(|e| e.to_string())?;
    let stale = staleness(registry, &mats);
    let downstream = registry.downstream(asset);
    let targets: Vec<String> = registry
        .topo()
        .filter(|m| !m.source && m.auto && downstream.contains(&m.name))
        .filter(|m| stale.get(&m.name).is_some_and(|s| s.stale))
        .map(|m| m.name.clone())
        .collect();
    if targets.is_empty() {
        return Ok(0);
    }
    if runner
        .store()
        .has_active_run(ASSETS_JOB)
        .map_err(|e| e.to_string())?
    {
        tracing::info!(asset = %asset, "auto build skipped: asset build already running");
        return Ok(0);
    }
    let plan = plan_targets(registry, &mats, &targets).map_err(|e| e.to_string())?;
    let run_id = launch_plan(runner, plan, Trigger::Build)
        .map_err(|e| format!("auto build of {} failed: {e}", targets.join(", ")))?;
    tracing::info!(assets = %targets.join(", "), run = %run_id, "auto build launched");
    Ok(1)
}

fn panic_payload(panic: &(dyn std::any::Any + Send)) -> Option<&str> {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::job::Job;
    use crate::model::{RunStatus, Trigger};
    use crate::op::Op;
    use crate::store::Store;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn echo_runner(store: Store) -> Runner {
        let job = Job::builder("etl")
            .op(Op::new(
                "echo",
                |ctx| async move { Ok(ctx.params().clone()) },
            ))
            .build()
            .unwrap();
        Runner::new([job], store)
    }

    async fn wait_terminal(runner: &Runner, id: &str) -> RunStatus {
        for _ in 0..300 {
            let run = runner.store().run(id).unwrap().unwrap();
            if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
                return run.status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("run {id} never reached a terminal status");
    }

    fn cursor_of(store: &Store, name: &str) -> Option<Value> {
        store
            .sensors()
            .unwrap()
            .into_iter()
            .find(|r| r.name == name)
            .unwrap()
            .cursor
    }

    // a sensor names its own params per request; nothing about the schedule
    // path applies here, and the launch validates them like any other
    #[tokio::test]
    async fn a_sensor_launches_each_request_with_its_own_params() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let entry = SensorEntry::user(Sensor::new(
            "watch",
            Duration::from_secs(3600),
            |_ctx: SensorCtx| async move {
                Ok(vec![
                    RunRequest {
                        job: "etl".into(),
                        params: json!({"shard": 1}),
                    },
                    RunRequest {
                        job: "etl".into(),
                        params: json!({"shard": 2}),
                    },
                ])
            },
        ));
        evaluate(&entry, &runner, &AssetRegistry::empty()).await;

        let ticks = store.sensor_ticks(Some("watch"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 2);
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().all(|r| r.trigger == Trigger::Sensor));
        for run in &runs {
            assert_eq!(wait_terminal(&runner, &run.id).await, RunStatus::Success);
            // the echo op returns its params, so the output proves they arrived
            let out = store.op_runs(&run.id).unwrap()[0].output.clone();
            assert_eq!(out, Some(run.params.clone()));
        }
        let mut shards: Vec<i64> = runs
            .iter()
            .map(|r| r.params["shard"].as_i64().unwrap())
            .collect();
        shards.sort();
        assert_eq!(shards, [1, 2]);
    }

    #[tokio::test]
    async fn cursor_commits_on_success_and_rolls_back_on_error() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let registry = AssetRegistry::empty();
        let calls = Arc::new(AtomicU32::new(0));
        let counter = calls.clone();
        let entry = SensorEntry::user(Sensor::new(
            "watch",
            Duration::from_secs(3600),
            move |ctx: SensorCtx| {
                let calls = counter.clone();
                async move {
                    match calls.fetch_add(1, Ordering::SeqCst) {
                        0 => {
                            ctx.set_cursor(json!(1));
                            Err("flaky".into())
                        }
                        1 => {
                            ctx.set_cursor(json!(2));
                            Ok(vec![])
                        }
                        _ => {
                            ctx.set_cursor(json!(3));
                            Ok(vec![RunRequest {
                                job: "ghost".into(),
                                params: json!({}),
                            }])
                        }
                    }
                }
            },
        ));

        evaluate(&entry, &runner, &registry).await;
        assert_eq!(cursor_of(&store, "watch"), None);
        evaluate(&entry, &runner, &registry).await;
        assert_eq!(cursor_of(&store, "watch"), Some(json!(2)));
        evaluate(&entry, &runner, &registry).await;
        assert_eq!(cursor_of(&store, "watch"), Some(json!(2)));

        let ticks = store.sensor_ticks(Some("watch"), 10).unwrap();
        assert_eq!(ticks.len(), 3);
        // newest first: error (unknown job), fired, error (closure)
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert!(ticks[0].error.as_deref().unwrap().contains("ghost"));
        assert_eq!(ticks[1].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[1].launched, 0);
        assert_eq!(ticks[2].outcome, SensorOutcome::Error);
        assert_eq!(ticks[2].error.as_deref(), Some("flaky"));
        assert!(store.runs(None, None, None, None, 10).unwrap().is_empty());
    }

    #[tokio::test]
    async fn sensor_launches_requested_runs_with_sensor_trigger() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let entry = SensorEntry::user(Sensor::new(
            "watch",
            Duration::from_secs(3600),
            |ctx: SensorCtx| async move {
                ctx.set_cursor(json!("seen"));
                Ok(vec![RunRequest {
                    job: "etl".into(),
                    params: json!({"n": 4}),
                }])
            },
        ));
        evaluate(&entry, &runner, &AssetRegistry::empty()).await;

        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].trigger, Trigger::Sensor);
        assert_eq!(runs[0].params, json!({"n": 4}));
        assert_eq!(
            wait_terminal(&runner, &runs[0].id).await,
            RunStatus::Success
        );

        let ticks = store.sensor_ticks(Some("watch"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);
        assert_eq!(cursor_of(&store, "watch"), Some(json!("seen")));
    }

    #[tokio::test]
    async fn panicking_sensor_is_an_error_tick() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["jumpy".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let entry = SensorEntry::user(Sensor::new(
            "jumpy",
            Duration::from_secs(3600),
            |_ctx: SensorCtx| async move {
                panic!("sensor bug");
                #[allow(unreachable_code)]
                Ok(vec![])
            },
        ));
        evaluate(&entry, &runner, &AssetRegistry::empty()).await;
        let ticks = store.sensor_ticks(Some("jumpy"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert!(ticks[0].error.as_deref().unwrap().contains("sensor bug"));
    }

    fn probe_registry(fp: Arc<Mutex<String>>) -> Arc<AssetRegistry> {
        let source = Asset::source("docs").probe(move || {
            let fp = fp.clone();
            async move { Ok(fp.lock().unwrap().clone()) }
        });
        let stats = Asset::new("stats", |ctx| async move {
            assert_eq!(ctx.input("docs"), Some(&Value::Null));
            Ok(json!({"n": 1}))
        })
        .from(&source)
        .auto();
        Arc::new(AssetRegistry::new(vec![source, stats], Vec::new(), Vec::new()).unwrap())
    }

    fn probe_entry(reg: &AssetRegistry, asset: &str) -> SensorEntry {
        let probe = reg.get(asset).unwrap().probe.clone().unwrap();
        SensorEntry {
            name: format!("probe:{asset}"),
            every: Duration::from_secs(3600),
            eval: SensorEval::Probe {
                asset: asset.into(),
                probe,
            },
        }
    }

    #[tokio::test]
    async fn probe_change_materializes_source_and_auto_builds() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["probe:docs".into()]).unwrap();
        let fp = Arc::new(Mutex::new("one".to_string()));
        let reg = probe_registry(fp.clone());
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let entry = probe_entry(&reg, "docs");

        evaluate(&entry, &runner, &reg).await;
        let docs = store.materialization("docs", None).unwrap().unwrap();
        assert_eq!(docs.fingerprint, "one");
        assert_eq!(docs.value, None);
        assert_eq!(docs.run_id, None);
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].trigger, Trigger::Build);
        assert_eq!(
            wait_terminal(&runner, &runs[0].id).await,
            RunStatus::Success
        );
        let stats = store.materialization("stats", None).unwrap().unwrap();
        assert_eq!(stats.inputs, json!({"docs": "one"}));
        let first_built = stats.built_at;
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);

        evaluate(&entry, &runner, &reg).await;
        assert_eq!(store.runs(None, None, None, None, 10).unwrap().len(), 1);
        let docs_again = store.materialization("docs", None).unwrap().unwrap();
        assert_eq!(docs_again.built_at, docs.built_at);
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 0);

        *fp.lock().unwrap() = "two".to_string();
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(
            store
                .materialization("docs", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "two"
        );
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(
            wait_terminal(&runner, &runs[0].id).await,
            RunStatus::Success
        );
        let stats = store.materialization("stats", None).unwrap().unwrap();
        assert_eq!(stats.inputs, json!({"docs": "two"}));
        assert!(stats.built_at >= first_built);
    }

    #[tokio::test]
    async fn failing_probe_is_an_error_tick_without_writes() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["probe:docs".into()]).unwrap();
        let source =
            Asset::source("docs").probe(|| async { Err("disk on fire".to_string().into()) });
        let reg = Arc::new(AssetRegistry::new(vec![source], Vec::new(), Vec::new()).unwrap());
        let runner = echo_runner(store.clone());
        let entry = probe_entry(&reg, "docs");
        evaluate(&entry, &runner, &reg).await;
        assert!(store.materialization("docs", None).unwrap().is_none());
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert_eq!(ticks[0].error.as_deref(), Some("disk on fire"));
    }

    #[tokio::test]
    async fn probe_unions_stale_auto_targets_into_one_run() {
        // s -> a(auto) -> b(auto): per-target plans would overlap on a
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["probe:s".into()]).unwrap();
        let s = Asset::source("s").probe(|| async { Ok("one".to_string()) });
        let a = Asset::new("a", |_| async { Ok(json!(1)) }).from(&s).auto();
        let b = Asset::new("b", |ctx| async move {
            Ok(json!(ctx.input("a").unwrap().as_i64().unwrap() + 1))
        })
        .from(&a)
        .auto();
        let reg = Arc::new(AssetRegistry::new(vec![s, a, b], Vec::new(), Vec::new()).unwrap());
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let entry = probe_entry(&reg, "s");

        evaluate(&entry, &runner, &reg).await;
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        let ops = store.op_runs(&runs[0].id).unwrap();
        let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        let ticks = store.sensor_ticks(Some("probe:s"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);
        assert_eq!(
            wait_terminal(&runner, &runs[0].id).await,
            RunStatus::Success
        );
        let ma = store.materialization("a", None).unwrap().unwrap();
        let mb = store.materialization("b", None).unwrap().unwrap();
        assert_eq!(mb.inputs, json!({"a": ma.fingerprint}));
    }

    #[tokio::test]
    async fn probe_skips_auto_build_while_assets_run_active() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["probe:docs".into()]).unwrap();
        let fp = Arc::new(Mutex::new("one".to_string()));
        let reg = probe_registry(fp.clone());
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let entry = probe_entry(&reg, "docs");

        // an assets run planted as live, without an executor behind it
        let active = crate::model::Run {
            id: "active".into(),
            job: "assets".into(),
            status: RunStatus::Running,
            trigger: Trigger::Manual,
            params: json!({}),
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: None,
            scheduled_for: None,
        };
        store.create_run(&active, &[]).unwrap();
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(
            store
                .materialization("docs", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "one"
        );
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 0);
        assert_eq!(store.runs(None, None, None, None, 10).unwrap().len(), 1);

        // the next tick sees an unchanged fingerprint and still catches up
        store
            .run_finished("active", RunStatus::Success, None)
            .unwrap();
        evaluate(&entry, &runner, &reg).await;
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 2);
        let build = runs.iter().find(|r| r.id != "active").unwrap();
        assert_eq!(wait_terminal(&runner, &build.id).await, RunStatus::Success);
        assert_eq!(
            store
                .materialization("stats", None)
                .unwrap()
                .unwrap()
                .inputs,
            json!({"docs": "one"})
        );
    }

    #[tokio::test]
    async fn probe_self_heals_stale_auto_after_failed_launch() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["probe:docs".into()]).unwrap();
        let fp = Arc::new(Mutex::new("one".to_string()));
        let reg = probe_registry(fp.clone());
        let entry = probe_entry(&reg, "docs");

        // a runner without the assets job: the launch fails after the commit
        let broken = echo_runner(store.clone());
        evaluate(&entry, &broken, &reg).await;
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert!(ticks[0].error.as_deref().unwrap().contains("stats"));
        assert_eq!(
            store
                .materialization("docs", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "one"
        );
        assert!(store.runs(None, None, None, None, 10).unwrap().is_empty());

        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        evaluate(&entry, &runner, &reg).await;
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(
            wait_terminal(&runner, &runs[0].id).await,
            RunStatus::Success
        );
        assert_eq!(
            store
                .materialization("stats", None)
                .unwrap()
                .unwrap()
                .inputs,
            json!({"docs": "one"})
        );
    }

    #[tokio::test]
    async fn loop_skips_paused_sensors_and_resumes() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["counter".into()]).unwrap();
        store.set_sensor_paused("counter", true).unwrap();
        let runner = echo_runner(store.clone());
        let calls = Arc::new(AtomicU32::new(0));
        let counter = calls.clone();
        let entry = SensorEntry::user(Sensor::new(
            "counter",
            Duration::from_millis(20),
            move |_ctx: SensorCtx| {
                let calls = counter.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![])
                }
            },
        ));
        let handle = tokio::spawn(run_sensors(
            vec![entry],
            runner,
            Arc::new(AssetRegistry::empty()),
        ));

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(store.sensor_ticks(Some("counter"), 10).unwrap().is_empty());

        store.set_sensor_paused("counter", false).unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        handle.abort();
        assert!(calls.load(Ordering::SeqCst) > 0);
        let ticks = store.sensor_ticks(Some("counter"), 10).unwrap();
        assert!(!ticks.is_empty());
        assert!(ticks.iter().all(|t| t.outcome == SensorOutcome::Fired));
    }

    // ---- run-status sensors -------------------------------------------

    /// two jobs: `etl`, which succeeds or fails on demand, and `publish`,
    /// which is what a chain launches.
    fn chain_runner(store: Store) -> Runner {
        let etl = Job::builder("etl")
            .op(Op::new("work", |ctx: crate::op::OpCtx| async move {
                match ctx.params()["fail"].as_bool().unwrap_or(false) {
                    true => Err("etl broke".into()),
                    false => Ok(json!({"rows": 1})),
                }
            }))
            .build()
            .unwrap();
        let publish = Job::builder("publish")
            .op(Op::new("push", |ctx: crate::op::OpCtx| async move {
                Ok(ctx.params().clone())
            }))
            .build()
            .unwrap();
        Runner::new([etl, publish], store)
    }

    fn chain_entry(name: &str, sensor: RunStatusSensor) -> SensorEntry {
        let entry = SensorEntry::runs(sensor);
        assert_eq!(entry.name, format!("run:{name}"));
        entry
    }

    /// the plain "when etl succeeds, publish" chain, tagging each request with
    /// the run that triggered it so a test can prove which one it saw. the
    /// `for_job` is not decoration: without it the chain would watch the
    /// `publish` runs it launches and feed itself.
    fn publish_chain(name: &str) -> RunStatusSensor {
        RunStatusSensor::new(name, |_ctx, run: RunSummary| async move {
            Ok(vec![RunRequest {
                job: "publish".into(),
                params: json!({"from": run.id, "job": run.job, "status": run.status}),
            }])
        })
        .for_job("etl")
    }

    async fn finish(runner: &Runner, job: &str, params: Value) -> String {
        let run = runner.run(job, params, Trigger::Manual).await.unwrap();
        run.id
    }

    /// what the chain launched, oldest first — sensor-triggered only, so a
    /// manual `publish` planted by a test is not mistaken for a chained one.
    fn published(store: &Store) -> Vec<Value> {
        let mut runs = store.runs(Some("publish"), None, None, None, 20).unwrap();
        runs.retain(|r| r.trigger == Trigger::Sensor);
        runs.sort_by_key(|r| r.created_at);
        runs.into_iter().map(|r| r.params).collect()
    }

    #[tokio::test]
    async fn a_success_chains_the_downstream_job_and_a_failure_does_not() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:chain".into()]).unwrap();
        let runner = chain_runner(store.clone());
        let entry = chain_entry("chain", publish_chain("chain"));
        let reg = AssetRegistry::empty();

        // a fresh sensor seeds its cursor and chains nothing: it is here for
        // what happens next, not for the run log it was added to
        finish(&runner, "etl", json!({})).await;
        evaluate(&entry, &runner, &reg).await;
        assert!(published(&store).is_empty());
        assert!(cursor_of(&store, "run:chain").is_some());

        let ok = finish(&runner, "etl", json!({})).await;
        finish(&runner, "etl", json!({"fail": true})).await;
        evaluate(&entry, &runner, &reg).await;

        // the default filter is success only, so the failed run is read past
        // rather than chained
        let params = published(&store);
        assert_eq!(params.len(), 1);
        assert_eq!(params[0]["from"], json!(ok));
        assert_eq!(params[0]["status"], json!("success"));
        let ticks = store.sensor_ticks(Some("run:chain"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);
    }

    #[tokio::test]
    async fn the_cursor_stops_a_run_firing_twice() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:chain".into()]).unwrap();
        let runner = chain_runner(store.clone());
        let entry = chain_entry("chain", publish_chain("chain"));
        let reg = AssetRegistry::empty();

        evaluate(&entry, &runner, &reg).await;
        let first = finish(&runner, "etl", json!({})).await;
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(published(&store).len(), 1);
        let cursor = cursor_of(&store, "run:chain").unwrap();
        assert_eq!(cursor["id"], json!(first));

        // nothing new: the same run must not chain again on the next tick
        evaluate(&entry, &runner, &reg).await;
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(published(&store).len(), 1);
        assert_eq!(cursor_of(&store, "run:chain"), Some(cursor));

        let second = finish(&runner, "etl", json!({})).await;
        evaluate(&entry, &runner, &reg).await;
        let params = published(&store);
        assert_eq!(params.len(), 2);
        assert_eq!(params[1]["from"], json!(second));
    }

    #[tokio::test]
    async fn on_widens_the_filter_and_for_job_narrows_it() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:watch".into()]).unwrap();
        let runner = chain_runner(store.clone());
        let entry = chain_entry(
            "watch",
            publish_chain("watch").on([RunStatus::Success, RunStatus::Failed]),
        );
        let reg = AssetRegistry::empty();
        evaluate(&entry, &runner, &reg).await;

        let ok = finish(&runner, "etl", json!({})).await;
        let bad = finish(&runner, "etl", json!({"fail": true})).await;
        // a publish run of its own: for_job("etl") must not see it, or the
        // chain would feed itself
        finish(&runner, "publish", json!({"manual": true})).await;
        evaluate(&entry, &runner, &reg).await;

        let params = published(&store);
        let from: Vec<&Value> = params.iter().map(|p| &p["from"]).collect();
        assert_eq!(from, [&json!(ok), &json!(bad)]);
        assert_eq!(params[1]["status"], json!("failed"));
        assert!(
            params.iter().all(|p| p["job"] == json!("etl")),
            "for_job let another job through"
        );
        assert_eq!(
            entry.filter(),
            Some(json!({"job": "etl", "statuses": ["success", "failed"]}))
        );
    }

    // a chain that launches the job that triggered it is legal and loops
    // forever; the filter is what stops it, and this is what that looks like
    #[tokio::test]
    async fn a_self_chaining_sensor_is_stopped_by_its_filter() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:loop".into()]).unwrap();
        let runner = chain_runner(store.clone());
        // "when publish succeeds, publish again" — but only for the manual
        // one, so the run it launches cannot re-trigger it
        let entry = chain_entry(
            "loop",
            RunStatusSensor::new("loop", |_ctx, run: RunSummary| async move {
                if run.trigger != Trigger::Manual {
                    return Ok(vec![]);
                }
                Ok(vec![RunRequest {
                    job: "publish".into(),
                    params: json!({"echo": run.id}),
                }])
            })
            .for_job("publish"),
        );
        let reg = AssetRegistry::empty();
        evaluate(&entry, &runner, &reg).await;

        let seed = finish(&runner, "publish", json!({"manual": true})).await;
        for _ in 0..4 {
            evaluate(&entry, &runner, &reg).await;
            for run in store.runs(Some("publish"), None, None, None, 20).unwrap() {
                wait_terminal(&runner, &run.id).await;
            }
        }
        let chained: Vec<Value> = published(&store)
            .into_iter()
            .filter(|p| p["echo"] != Value::Null)
            .collect();
        assert_eq!(chained.len(), 1, "the chain fed itself");
        assert_eq!(chained[0]["echo"], json!(seed));
    }

    #[tokio::test]
    async fn a_partial_launch_failure_replays_rather_than_skips() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:chain".into()]).unwrap();
        let runner = chain_runner(store.clone());
        let reg = AssetRegistry::empty();
        // the second run's request names a job nobody registered, so its
        // launch fails after the first one already went out
        let calls = Arc::new(AtomicU32::new(0));
        let counter = calls.clone();
        let entry = chain_entry(
            "chain",
            RunStatusSensor::new("chain", move |_ctx, run: RunSummary| {
                let calls = counter.clone();
                async move {
                    let job = match calls.fetch_add(1, Ordering::SeqCst) {
                        1 => "ghost",
                        _ => "publish",
                    };
                    Ok(vec![RunRequest {
                        job: job.into(),
                        params: json!({"from": run.id}),
                    }])
                }
            })
            .for_job("etl"),
        );
        evaluate(&entry, &runner, &reg).await;
        let first = finish(&runner, "etl", json!({})).await;
        let second = finish(&runner, "etl", json!({})).await;
        let before = cursor_of(&store, "run:chain");

        evaluate(&entry, &runner, &reg).await;
        let ticks = store.sensor_ticks(Some("run:chain"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert_eq!(ticks[0].launched, 1);
        assert!(ticks[0].error.as_deref().unwrap().contains("ghost"));
        assert_eq!(
            cursor_of(&store, "run:chain"),
            before,
            "a failed evaluation must leave the cursor where it was"
        );

        // at-least-once: the first run is handed over a second time, which is
        // the contract — a sensor that could lose an event would be worse
        evaluate(&entry, &runner, &reg).await;
        let params = published(&store);
        assert_eq!(
            params.iter().map(|p| p["from"].clone()).collect::<Vec<_>>(),
            [json!(first), json!(first), json!(second)]
        );
        assert_eq!(cursor_of(&store, "run:chain").unwrap()["id"], json!(second));
    }

    #[tokio::test]
    async fn pausing_stops_a_run_sensor_like_any_other() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:chain".into()]).unwrap();
        let runner = chain_runner(store.clone());
        let entry = chain_entry(
            "chain",
            publish_chain("chain").every(Duration::from_millis(20)),
        );
        evaluate(&entry, &runner, &AssetRegistry::empty()).await;
        store.set_sensor_paused("run:chain", true).unwrap();
        finish(&runner, "etl", json!({})).await;

        let handle = tokio::spawn(run_sensors(
            vec![entry],
            runner.clone(),
            Arc::new(AssetRegistry::empty()),
        ));
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(published(&store).is_empty());
        let paused_ticks = store.sensor_ticks(Some("run:chain"), 10).unwrap().len();

        store.set_sensor_paused("run:chain", false).unwrap();
        for _ in 0..100 {
            if !published(&store).is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        assert_eq!(published(&store).len(), 1, "resuming did not chain the run");
        assert!(store.sensor_ticks(Some("run:chain"), 20).unwrap().len() > paused_ticks);
    }
}
