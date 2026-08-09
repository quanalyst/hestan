use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::asset::{
    ASSETS_JOB, AssetRegistry, ProbeFn, launch_plan, mats_map, plan_targets, staleness,
};
use crate::backoff::{capped_exponential, full_jitter};
use crate::error::Error;
use crate::executor::Runner;
use crate::model::{Run, RunCursor, RunStatus, SensorOutcome, Trigger};
use crate::op::InputError;
use crate::store::RunKey;

/// what a sensor evaluation asks for: launch `job` with `params`, at most once
/// per [`key`](Self::key) if it names one.
///
/// ```no_run
/// # use hestan::RunRequest;
/// # use serde_json::json;
/// RunRequest::new("publish")
///     .params(json!({ "day": "2026-08-09" }))
///     .key("2026-08-09");
/// ```
pub struct RunRequest {
    pub job: String,
    pub params: Value,
    /// the [run key](Self::key) this request launches under; `None` is the
    /// at-least-once default.
    pub key: Option<String>,
}

impl RunRequest {
    /// a request to launch `job` with params `{}` and no run key.
    pub fn new(job: impl Into<String>) -> RunRequest {
        RunRequest {
            job: job.into(),
            params: json!({}),
            key: None,
        }
    }

    /// what the run launches with; `{}` unless this is called.
    pub fn params(mut self, params: Value) -> RunRequest {
        self.params = params;
        self
    }

    /// launch this at most once per key, ever, for this sensor.
    ///
    /// sensors are at-least-once by design — a partial launch failure replays
    /// the whole batch, and so does a cursor write that failed — so a key is
    /// what turns that into **effectively-once per sensor**: the key is
    /// claimed in the same transaction that creates the run, and a request
    /// naming a claimed key is skipped rather than launched. keys are scoped
    /// to the sensor that used them, so two sensors may use the same string
    /// and mean different things.
    ///
    /// keys are never collected on their own. a sensor keyed by the day keeps
    /// a row per day for as long as the file exists unless
    /// [`retention_days`](crate::Hestan::retention_days) is set, which prunes
    /// them on the same cutoff as runs.
    pub fn key(mut self, key: impl Into<String>) -> RunRequest {
        self.key = Some(key.into());
        self
    }
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
    timeout: Duration,
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
            timeout: DEFAULT_SENSOR_TIMEOUT,
            f: Arc::new(move |ctx| Box::pin(f(ctx))),
        }
    }

    /// how long one evaluation may take before the loop gives up on it
    /// (default 60s). on expiry the evaluation is abandoned, the tick records
    /// the timeout as an error, and the staged cursor is not committed.
    ///
    /// abandoning is not stopping, and this is the same limit ops have. an
    /// `.await` inside the closure is where an abandoned evaluation actually
    /// goes away; a closure doing blocking work between await points cannot be
    /// dropped at all, so it keeps its thread until that work returns — and if
    /// it does return, late, what it returns still counts. nothing else can
    /// have run in the meantime: the loop never evaluates a sensor whose
    /// previous evaluation is still going. [`SensorCtx::is_cancelled`] is the
    /// cooperative half.
    pub fn timeout(mut self, timeout: Duration) -> Sensor {
        self.timeout = timeout;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// how many terminal runs one [`RunStatusSensor`] evaluation reads. a sensor
/// that was paused for a week has a backlog, and draining it a page at a time
/// keeps one tick from launching thousands of runs at once.
const RUN_SENSOR_PAGE: u32 = 200;

/// how long one evaluation may take before the loop abandons it, unless the
/// sensor asked for something else. probes get the same one and have no way to
/// change it: a fingerprint that takes a minute is a broken probe.
pub(crate) const DEFAULT_SENSOR_TIMEOUT: Duration = Duration::from_secs(60);

/// how many sensors evaluate at once. the loop evaluated in sequence before
/// this, so one slow closure delayed every sensor and every probe behind it;
/// the bound is here because the alternative — every due sensor at once — is
/// how a hundred entries become a hundred concurrent api calls.
const MAX_CONCURRENT_EVALS: usize = 8;

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
///         Ok(vec![
///             RunRequest::new("publish").params(json!({ "from": run.id })),
///         ])
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
    timeout: Duration,
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
            timeout: DEFAULT_SENSOR_TIMEOUT,
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

    /// how long one evaluation may take before the loop gives up on it
    /// (default 60s), with the same meaning and the same limits as
    /// [`Sensor::timeout`]. one evaluation here is a whole page of runs, not
    /// one call of the closure.
    pub fn timeout(mut self, timeout: Duration) -> RunStatusSensor {
        self.timeout = timeout;
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
    deadline: Instant,
}

impl SensorCtx {
    fn new(
        cursor: Option<Value>,
        new_cursor: Arc<Mutex<Option<Value>>>,
        deadline: Instant,
    ) -> SensorCtx {
        SensorCtx {
            cursor,
            new_cursor,
            deadline,
        }
    }

    /// the cursor the last fully-successful evaluation committed.
    pub fn cursor(&self) -> Option<&Value> {
        self.cursor.as_ref()
    }

    /// true once this evaluation's [timeout](Sensor::timeout) has passed. cheap
    /// enough to call in a loop — it reads the clock and allocates nothing.
    ///
    /// an async closure does not need this: it is dropped at its next await
    /// point. blocking work cannot be dropped at all, so polling this is the
    /// only way it ever stops early, exactly as with
    /// [`OpCtx::is_cancelled`](crate::OpCtx::is_cancelled):
    ///
    /// ```no_run
    /// # use hestan::{RunRequest, Sensor, SensorCtx};
    /// # use std::time::Duration;
    /// Sensor::new("crunch", Duration::from_secs(60), |ctx: SensorCtx| async move {
    ///     for chunk in 0..1_000 {
    ///         if ctx.is_cancelled() {
    ///             return Err("evaluation timed out".into());
    ///         }
    ///         # let _ = chunk;
    ///     }
    ///     Ok(Vec::<RunRequest>::new())
    /// });
    /// ```
    pub fn is_cancelled(&self) -> bool {
        Instant::now() >= self.deadline
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
    pub timeout: Duration,
    pub eval: SensorEval,
    /// when it is next due and whether an evaluation of it is still going.
    /// shared, because evaluations run on tasks of their own now.
    pub state: Arc<SensorState>,
}

impl SensorEntry {
    pub(crate) fn user(sensor: Sensor) -> SensorEntry {
        SensorEntry {
            name: sensor.name,
            every: sensor.every,
            timeout: sensor.timeout,
            eval: SensorEval::User(sensor.f),
            state: SensorState::new(),
        }
    }

    pub(crate) fn runs(sensor: RunStatusSensor) -> SensorEntry {
        SensorEntry {
            name: sensor.sensor_name(),
            every: sensor.every,
            timeout: sensor.timeout,
            eval: SensorEval::Runs {
                statuses: sensor.statuses,
                job: sensor.job,
                f: sensor.f,
            },
            state: SensorState::new(),
        }
    }

    /// the entry a probed source asset becomes. probes carry the default
    /// timeout: there is no declaration to hang another one on.
    pub(crate) fn probe(asset: &str, probe: Arc<ProbeFn>, every: Duration) -> SensorEntry {
        SensorEntry {
            name: format!("probe:{asset}"),
            every,
            timeout: DEFAULT_SENSOR_TIMEOUT,
            eval: SensorEval::Probe {
                asset: asset.to_string(),
                probe,
            },
            state: SensorState::new(),
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

/// how far apart a repeatedly failing sensor's evaluations grow. a sensor
/// erroring every tick at full rate is how one broken endpoint becomes a log
/// flood and a rate-limit ban.
const BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);

/// how long to wait before the next evaluation, after `failures` consecutive
/// failed ones. no failures is the sensor's own interval exactly; each failure
/// doubles it to [`BACKOFF_MAX`], jittered over the top half of the window so
/// a fleet of sensors watching the same broken endpoint does not come back in
/// lockstep.
///
/// the floor doubles as well as the ceiling, which is what makes the gap
/// actually lengthen rather than merely lengthen on average — and it is never
/// shorter than the interval that was asked for, including for a sensor whose
/// interval is already past the cap and so has nothing to back off to.
fn next_gap(every: Duration, failures: u32) -> Duration {
    let capped = capped_exponential(every, failures, BACKOFF_MAX);
    let floor = (capped / 2).max(every);
    floor + full_jitter(capped.saturating_sub(floor))
}

/// one sensor's place in the loop, shared with the task that evaluates it and
/// with the api, which reports the next evaluation and the failure streak so a
/// degraded sensor reads as degraded rather than as slow.
pub(crate) struct SensorState(Mutex<StateInner>);

struct StateInner {
    due: Instant,
    /// the same instant as `due` on the wall clock, for the api. kept beside
    /// it rather than derived: a monotonic instant has no calendar time to
    /// convert to
    next_eval: DateTime<Utc>,
    /// an evaluation of this sensor is under way — waiting for a permit
    /// counts, because it is going to run
    running: bool,
    /// a skip has already been recorded for this stall, so the ones after it
    /// are log lines. a sensor wedged for an hour must not bury every other
    /// sensor's tick history under its own
    stalled: bool,
    /// evaluations that have failed in a row, reset by the first success
    failures: u32,
}

impl StateInner {
    fn wait(&mut self, gap: Duration) {
        self.due = Instant::now() + gap;
        self.next_eval =
            Utc::now() + chrono::Duration::from_std(gap).unwrap_or(chrono::TimeDelta::MAX);
    }
}

/// what the loop decided to do with a sensor that came due.
enum Claim {
    Go,
    /// the previous evaluation is still going. skip this turn rather than
    /// queue it: a queued second evaluation could commit a cursor over a newer
    /// one, and a backlog of them is not what "every 5 seconds" asked for.
    Stalled {
        first: bool,
    },
}

impl SensorState {
    pub(crate) fn new() -> Arc<SensorState> {
        Arc::new(SensorState(Mutex::new(StateInner {
            due: Instant::now(),
            next_eval: Utc::now(),
            running: false,
            stalled: false,
            failures: 0,
        })))
    }

    pub(crate) fn due(&self) -> Instant {
        self.0.lock().unwrap().due
    }

    /// when the sensor is next due, and how many evaluations have failed in a
    /// row — what `GET /api/sensors` reports.
    pub(crate) fn snapshot(&self) -> (DateTime<Utc>, u32) {
        let inner = self.0.lock().unwrap();
        (inner.next_eval, inner.failures)
    }

    /// push the next evaluation out by `every` without evaluating: what a
    /// paused sensor does, so its schedule keeps ticking over rather than
    /// coming due the instant it resumes.
    fn defer(&self, every: Duration) {
        self.0.lock().unwrap().wait(every);
    }

    fn claim(&self, every: Duration) -> Claim {
        let mut inner = self.0.lock().unwrap();
        // tentative: the evaluation resets it when it ends, so in the ordinary
        // case the gap still counts from the end of the last evaluation
        inner.wait(every);
        if inner.running {
            let first = !inner.stalled;
            inner.stalled = true;
            return Claim::Stalled { first };
        }
        inner.running = true;
        Claim::Go
    }

    /// hand the sensor back once its evaluation has ended. a failure lengthens
    /// the next gap and the first success collapses it back to `every`.
    fn release(&self, every: Duration, failed: bool) {
        let mut inner = self.0.lock().unwrap();
        inner.running = false;
        inner.stalled = false;
        inner.failures = match failed {
            true => inner.failures.saturating_add(1),
            false => 0,
        };
        let gap = next_gap(every, inner.failures);
        if inner.failures > 0 {
            tracing::debug!(failures = inner.failures, "sensor backing off for {gap:?}");
        }
        inner.wait(gap);
    }
}

/// the sensor loop: every entry evaluates at startup, then on its own interval.
/// paused entries are skipped without a tick and keep their schedule.
///
/// due entries evaluate on tasks of their own, at most [`MAX_CONCURRENT_EVALS`]
/// at a time, so one slow closure no longer delays every sensor behind it. two
/// evaluations of the *same* sensor never overlap: the loop skips a sensor
/// whose previous evaluation is still going rather than queueing another,
/// which is what keeps a slow evaluation from committing its cursor over a
/// newer one.
pub(crate) async fn run_sensors(
    entries: Vec<SensorEntry>,
    runner: Runner,
    registry: Arc<AssetRegistry>,
) {
    if entries.is_empty() {
        return;
    }
    let entries: Vec<Arc<SensorEntry>> = entries.into_iter().map(Arc::new).collect();
    let limit = Arc::new(Semaphore::new(MAX_CONCURRENT_EVALS));
    loop {
        let (i, at) = entries
            .iter()
            .enumerate()
            .map(|(i, e)| (i, e.state.due()))
            .min_by_key(|(_, t)| *t)
            .expect("entries is non-empty");
        tokio::time::sleep_until(at).await;
        let entry = entries[i].clone();
        // an evaluation that finished while the loop slept may have moved this
        // one out from under it
        if entry.state.due() > Instant::now() {
            continue;
        }
        if sensor_paused(&runner, &entry.name) {
            entry.state.defer(entry.every);
            continue;
        }
        if let Claim::Stalled { first } = entry.state.claim(entry.every) {
            tracing::warn!(sensor = %entry.name, "still evaluating: this turn skipped");
            if first {
                note_skipped_tick(&runner, &entry.name);
            }
            continue;
        }
        let (runner, registry, limit) = (runner.clone(), registry.clone(), limit.clone());
        tokio::spawn(async move {
            // the bound is on evaluating, not on dispatching: a task waiting
            // here already holds its sensor, which is what stops a second
            // evaluation of it starting behind this one
            let _permit = limit
                .acquire()
                .await
                .expect("the semaphore is never closed");
            let outcome = evaluate(&entry, &runner, &registry).await;
            entry
                .state
                .release(entry.every, outcome == SensorOutcome::Error);
        });
    }
}

/// record the turn a sensor was too busy to take. one per stall, not one per
/// turn: the sensor's own tick is still coming, and burying it under skips
/// would be worse than saying nothing.
fn note_skipped_tick(runner: &Runner, name: &str) {
    if let Err(e) = runner.store().record_sensor_tick(
        name,
        SensorOutcome::Skipped,
        0,
        0,
        Some("previous evaluation still running"),
    ) {
        tracing::warn!(sensor = %name, "tick write failed: {e}");
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

/// what one evaluation did, beside how it ended: runs it launched, and keyed
/// requests it skipped because the key was already claimed.
///
/// shared with the evaluation rather than returned from it, so a tick can
/// still say what an evaluation managed to launch before it was abandoned at
/// its timeout.
#[derive(Default)]
struct Counts {
    launched: AtomicU32,
    skipped: AtomicU32,
}

impl Counts {
    fn record(&self, fired: Fired) {
        let counter = match fired {
            Fired::Launched => &self.launched,
            Fired::Skipped => &self.skipped,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// evaluate one sensor and record its tick; the outcome is what the loop
/// paces the next evaluation from.
async fn evaluate(entry: &SensorEntry, runner: &Runner, registry: &AssetRegistry) -> SensorOutcome {
    let counts = Counts::default();
    let deadline = Instant::now() + entry.timeout;
    let eval = async {
        match &entry.eval {
            SensorEval::User(f) => evaluate_user(&entry.name, f, runner, &counts, deadline).await,
            SensorEval::Probe { asset, probe } => {
                evaluate_probe(asset, probe, runner, registry, &counts).await
            }
            SensorEval::Runs { statuses, job, f } => {
                evaluate_runs(
                    &entry.name,
                    statuses,
                    job.as_deref(),
                    f,
                    runner,
                    &counts,
                    deadline,
                )
                .await
            }
        }
    };
    // abandoning is not stopping: the evaluation goes away at its next await
    // point, and a closure blocking between them keeps its thread until it
    // returns. what is guaranteed is that nothing it does after this counts —
    // the cursor is not committed and the tick is already written
    let (outcome, error) = match tokio::time::timeout_at(deadline, eval).await {
        Ok(done) => done,
        Err(_) => {
            let msg = format!("evaluation timed out after {:?}", entry.timeout);
            tracing::warn!(sensor = %entry.name, "{msg}");
            (SensorOutcome::Error, Some(msg))
        }
    };
    if let Err(e) = runner.store().record_sensor_tick(
        &entry.name,
        outcome,
        counts.launched.load(Ordering::Relaxed),
        counts.skipped.load(Ordering::Relaxed),
        error.as_deref(),
    ) {
        tracing::warn!(sensor = %entry.name, "tick write failed: {e}");
    }
    outcome
}

/// what launching one request did.
enum Fired {
    Launched,
    /// its run key was already claimed, so it is not launched again.
    Skipped,
}

/// launch one request under its run key, if it has one. the error is the
/// message that fails the whole evaluation.
///
/// the key is read first and claimed inside the run's own transaction, so a
/// launch that fails leaves no key behind and a key that exists always names a
/// run that does. a read that fails skips nothing and launches nothing: not
/// being able to tell whether a key is claimed is not a licence to duplicate.
fn launch_request(sensor: &str, req: RunRequest, runner: &Runner) -> Result<Fired, String> {
    let RunRequest { job, params, key } = req;
    let fail = |e: Error| format!("launch of job {job:?} failed: {e}");
    let Some(key) = key.as_deref() else {
        return match runner.launch(&job, params, Trigger::Sensor) {
            Ok(run_id) => {
                tracing::info!(sensor = %sensor, job = %job, run = %run_id, "sensor fired");
                Ok(Fired::Launched)
            }
            Err(e) => Err(fail(e)),
        };
    };
    match runner.store().run_key_claimed(sensor, key) {
        Ok(true) => {
            tracing::info!(sensor = %sensor, job = %job, key = %key, "run key already launched");
            return Ok(Fired::Skipped);
        }
        Ok(false) => {}
        Err(e) => return Err(format!("run key read failed: {e}")),
    }
    match runner.launch_keyed(&job, params, Trigger::Sensor, RunKey { sensor, key }) {
        Ok(Some(run_id)) => {
            tracing::info!(sensor = %sensor, job = %job, key = %key, run = %run_id, "sensor fired");
            Ok(Fired::Launched)
        }
        // claimed between the read and the insert; one launch either way
        Ok(None) => Ok(Fired::Skipped),
        Err(e) => Err(fail(e)),
    }
}

async fn evaluate_user(
    name: &str,
    f: &Arc<SensorFn>,
    runner: &Runner,
    counts: &Counts,
    deadline: Instant,
) -> (SensorOutcome, Option<String>) {
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
    let ctx = SensorCtx::new(cursor, new_cursor.clone(), deadline);
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
            return (SensorOutcome::Error, Some(msg));
        }
    };
    for req in requests {
        match launch_request(name, req, runner) {
            Ok(fired) => counts.record(fired),
            // a launch failure fails the evaluation: the cursor stays put
            Err(msg) => {
                tracing::warn!(sensor = %name, "{msg}");
                return (SensorOutcome::Error, Some(msg));
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
            Some(format!("cursor write failed: {e}")),
        );
    }
    (SensorOutcome::Fired, None)
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
/// that could lose an event would be worse than one that repeats it. a request
/// carrying a [run key](RunRequest::key) is the way out of the replay: the
/// second sight of it is skipped rather than launched.
#[allow(clippy::too_many_arguments)]
async fn evaluate_runs(
    name: &str,
    statuses: &[RunStatus],
    job: Option<&str>,
    f: &Arc<RunSensorFn>,
    runner: &Runner,
    counts: &Counts,
    deadline: Instant,
) -> (SensorOutcome, Option<String>) {
    let stored = match sensor_cursor(runner, name) {
        Ok(c) => c,
        Err(msg) => return (SensorOutcome::Error, Some(msg)),
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
            Ok(()) => (SensorOutcome::Fired, None),
            Err(msg) => (SensorOutcome::Error, Some(msg)),
        };
    };
    let runs = match runner
        .store()
        .terminal_runs_after(job, Some(&cursor), RUN_SENSOR_PAGE)
    {
        Ok(r) => r,
        Err(e) => return (SensorOutcome::Error, Some(e.to_string())),
    };
    let Some(last) = runs.last() else {
        return (SensorOutcome::Fired, None);
    };
    let seen = RunCursor {
        finished_at: last.finished_at.expect("terminal runs carry a finish time"),
        id: last.id.clone(),
    };
    for run in runs.iter().filter(|r| statuses.contains(&r.status)) {
        let ctx = SensorCtx::new(stored.clone(), Arc::new(Mutex::new(None)), deadline);
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
                return (SensorOutcome::Error, Some(msg));
            }
        };
        for req in requests {
            match launch_request(name, req, runner) {
                Ok(fired) => counts.record(fired),
                Err(msg) => {
                    tracing::warn!(sensor = %name, "{msg}");
                    return (SensorOutcome::Error, Some(msg));
                }
            }
        }
    }
    let seen = serde_json::to_value(&seen).expect("RunCursor is json");
    if let Err(e) = runner.store().set_sensor_cursor(name, &seen) {
        tracing::warn!(sensor = %name, "cursor write failed: {e}");
        return (
            SensorOutcome::Error,
            Some(format!("cursor write failed: {e}")),
        );
    }
    (SensorOutcome::Fired, None)
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
    counts: &Counts,
) -> (SensorOutcome, Option<String>) {
    let fingerprint = match AssertUnwindSafe(async { probe().await })
        .catch_unwind()
        .await
    {
        Ok(Ok(fp)) => fp,
        Ok(Err(e)) => return (SensorOutcome::Error, Some(e.to_string())),
        Err(panic) => {
            let msg = match panic_payload(panic.as_ref()) {
                Some(s) => format!("probe panicked: {s}"),
                None => "probe panicked".to_string(),
            };
            return (SensorOutcome::Error, Some(msg));
        }
    };
    let current = match runner.store().materialization(asset, None) {
        Ok(m) => m.map(|m| m.fingerprint),
        Err(e) => return (SensorOutcome::Error, Some(e.to_string())),
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
            return (SensorOutcome::Error, Some(e.to_string()));
        }
    }
    // changed or not: the fingerprint commits before any launch, so re-deriving
    // every tick is what heals a launch that failed after the commit
    match launch_stale_auto(asset, runner, registry) {
        Ok(launched) => {
            counts.launched.fetch_add(launched, Ordering::Relaxed);
            (SensorOutcome::Fired, None)
        }
        Err(msg) => (SensorOutcome::Error, Some(msg)),
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
                    RunRequest::new("etl").params(json!({"shard": 1})),
                    RunRequest::new("etl").params(json!({"shard": 2})),
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
                            Ok(vec![RunRequest::new("ghost").params(json!({}))])
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
                Ok(vec![RunRequest::new("etl").params(json!({"n": 4}))])
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
        SensorEntry::probe(asset, probe, Duration::from_secs(3600))
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
            Ok(vec![RunRequest::new("publish").params(
                json!({"from": run.id, "job": run.job, "status": run.status}),
            )])
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
                Ok(vec![
                    RunRequest::new("publish").params(json!({"echo": run.id})),
                ])
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
                    Ok(vec![RunRequest::new(job).params(json!({"from": run.id}))])
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

    // ---- run keys ------------------------------------------------------

    /// a sensor that asks for the same two runs every evaluation: one under a
    /// run key and one without, so a single test watches both contracts.
    fn keyed_entry(name: &str, key: &str) -> SensorEntry {
        let key = key.to_string();
        SensorEntry::user(Sensor::new(
            name,
            Duration::from_secs(3600),
            move |_ctx: SensorCtx| {
                let key = key.clone();
                async move {
                    Ok(vec![
                        RunRequest::new("etl")
                            .params(json!({"kind": "keyed"}))
                            .key(key),
                        RunRequest::new("etl").params(json!({"kind": "keyless"})),
                    ])
                }
            },
        ))
    }

    fn launched_kinds(store: &Store) -> Vec<String> {
        let mut runs = store.runs(None, None, None, None, 20).unwrap();
        runs.sort_by_key(|r| r.created_at);
        runs.iter()
            .map(|r| r.params["kind"].as_str().unwrap_or("?").to_string())
            .collect()
    }

    #[tokio::test]
    async fn a_run_key_launches_once_while_a_keyless_request_replays() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let entry = keyed_entry("watch", "2026-08-09");
        let reg = AssetRegistry::empty();

        evaluate(&entry, &runner, &reg).await;
        evaluate(&entry, &runner, &reg).await;

        assert_eq!(launched_kinds(&store), ["keyed", "keyless", "keyless"]);
        let ticks = store.sensor_ticks(Some("watch"), 10).unwrap();
        assert_eq!((ticks[1].launched, ticks[1].skipped), (2, 0));
        assert_eq!(
            (ticks[0].launched, ticks[0].skipped),
            (1, 1),
            "the second evaluation should report the keyed request as skipped"
        );
        assert!(ticks.iter().all(|t| t.outcome == SensorOutcome::Fired));

        // the key belongs to the sensor that used it, not to the process
        assert!(store.run_key_claimed("watch", "2026-08-09").unwrap());
        assert!(!store.run_key_claimed("other", "2026-08-09").unwrap());
    }

    #[tokio::test]
    async fn a_failed_launch_leaves_no_run_key_behind() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        let reg = AssetRegistry::empty();
        let entry = SensorEntry::user(Sensor::new(
            "watch",
            Duration::from_secs(3600),
            |_ctx: SensorCtx| async move {
                Ok(vec![
                    RunRequest::new("etl")
                        .params(json!({"kind": "keyed"}))
                        .key("once"),
                ])
            },
        ));

        // a runner that has never heard of the job: the launch fails, and a key
        // recorded here would drop this work forever
        let broken = Runner::new(Vec::<Job>::new(), store.clone());
        evaluate(&entry, &broken, &reg).await;
        let ticks = store.sensor_ticks(Some("watch"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert_eq!((ticks[0].launched, ticks[0].skipped), (0, 0));
        assert!(store.runs(None, None, None, None, 10).unwrap().is_empty());
        assert!(
            !store.run_key_claimed("watch", "once").unwrap(),
            "a key outlived the launch it was supposed to record"
        );

        // so the work is still launchable, which is the whole point
        let runner = echo_runner(store.clone());
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(launched_kinds(&store), ["keyed"]);
        assert!(store.run_key_claimed("watch", "once").unwrap());
    }

    #[tokio::test]
    async fn a_replayed_batch_does_not_launch_a_keyed_request_twice() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["run:chain".into()]).unwrap();
        let runner = chain_runner(store.clone());
        let reg = AssetRegistry::empty();
        // the same partial failure as the at-least-once test above — the second
        // request names a job nobody registered — with each request keyed by the
        // run that triggered it
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
                    Ok(vec![
                        RunRequest::new(job)
                            .params(json!({"from": run.id}))
                            .key(run.id),
                    ])
                }
            })
            .for_job("etl"),
        );
        evaluate(&entry, &runner, &reg).await;
        let first = finish(&runner, "etl", json!({})).await;
        let second = finish(&runner, "etl", json!({})).await;

        evaluate(&entry, &runner, &reg).await;
        assert_eq!(
            store.sensor_ticks(Some("run:chain"), 10).unwrap()[0].outcome,
            SensorOutcome::Error
        );
        evaluate(&entry, &runner, &reg).await;

        // the replay hands the first run over again, and its key stops the
        // second publish that at-least-once would otherwise have launched
        assert_eq!(
            published(&store)
                .iter()
                .map(|p| p["from"].clone())
                .collect::<Vec<_>>(),
            [json!(first), json!(second)]
        );
        let ticks = store.sensor_ticks(Some("run:chain"), 10).unwrap();
        assert_eq!((ticks[0].launched, ticks[0].skipped), (1, 1));
        assert_eq!(cursor_of(&store, "run:chain").unwrap()["id"], json!(second));
    }

    // ---- timeouts and concurrency --------------------------------------

    /// a sensor that counts its calls, sleeps `takes`, and launches nothing.
    fn slow_entry(name: &str, every: Duration, takes: Duration, calls: Arc<AtomicU32>) -> Sensor {
        Sensor::new(name, every, move |_ctx: SensorCtx| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(takes).await;
                Ok(vec![])
            }
        })
    }

    #[tokio::test]
    async fn a_slow_sensor_does_not_delay_a_fast_one() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["slow".into(), "fast".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let (slow_calls, fast_calls) = (Arc::new(AtomicU32::new(0)), Arc::new(AtomicU32::new(0)));
        let entries = vec![
            SensorEntry::user(slow_entry(
                "slow",
                Duration::from_secs(3600),
                Duration::from_millis(400),
                slow_calls.clone(),
            )),
            SensorEntry::user(slow_entry(
                "fast",
                Duration::from_millis(20),
                Duration::ZERO,
                fast_calls.clone(),
            )),
        ];
        let handle = tokio::spawn(run_sensors(
            entries,
            runner,
            Arc::new(AssetRegistry::empty()),
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;
        handle.abort();
        assert_eq!(slow_calls.load(Ordering::SeqCst), 1);
        // in sequence the fast sensor would have got exactly zero turns while
        // the slow one held the loop
        assert!(
            fast_calls.load(Ordering::SeqCst) >= 4,
            "the fast sensor ran {} times behind a slow one",
            fast_calls.load(Ordering::SeqCst)
        );
        assert!(
            store.sensor_ticks(Some("slow"), 10).unwrap().is_empty(),
            "the slow evaluation ticked before it finished"
        );
    }

    #[tokio::test]
    async fn a_sensor_still_evaluating_is_skipped_rather_than_evaluated_twice() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["slow".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let calls = Arc::new(AtomicU32::new(0));
        let entry = SensorEntry::user(slow_entry(
            "slow",
            Duration::from_millis(20),
            Duration::from_millis(300),
            calls.clone(),
        ));
        let handle = tokio::spawn(run_sensors(
            vec![entry],
            runner,
            Arc::new(AssetRegistry::empty()),
        ));

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a second evaluation started while the first was still going"
        );
        // one skip per stall, not one per turn: seven turns came due here
        let ticks = store.sensor_ticks(Some("slow"), 10).unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].outcome, SensorOutcome::Skipped);
        assert_eq!(
            ticks[0].error.as_deref(),
            Some("previous evaluation still running")
        );

        // and once it finishes, the sensor evaluates again normally
        tokio::time::sleep(Duration::from_millis(250)).await;
        handle.abort();
        let ticks = store.sensor_ticks(Some("slow"), 10).unwrap();
        assert!(calls.load(Ordering::SeqCst) >= 2);
        assert!(ticks.iter().any(|t| t.outcome == SensorOutcome::Fired));
    }

    #[tokio::test]
    async fn no_more_than_the_bound_evaluate_at_once() {
        let store = Store::open(":memory:").unwrap();
        let names: Vec<String> = (0..20).map(|i| format!("s{i}")).collect();
        store.sync_sensors(&names).unwrap();
        let runner = echo_runner(store.clone());
        let live = Arc::new(AtomicU32::new(0));
        let peak = Arc::new(AtomicU32::new(0));
        let entries: Vec<SensorEntry> = names
            .iter()
            .map(|name| {
                let (live, peak) = (live.clone(), peak.clone());
                SensorEntry::user(Sensor::new(
                    name,
                    Duration::from_secs(3600),
                    move |_ctx: SensorCtx| {
                        let (live, peak) = (live.clone(), peak.clone());
                        async move {
                            let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(now, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(60)).await;
                            live.fetch_sub(1, Ordering::SeqCst);
                            Ok(vec![])
                        }
                    },
                ))
            })
            .collect();
        let handle = tokio::spawn(run_sensors(
            entries,
            runner,
            Arc::new(AssetRegistry::empty()),
        ));

        tokio::time::sleep(Duration::from_millis(400)).await;
        handle.abort();
        let peak = peak.load(Ordering::SeqCst) as usize;
        assert_eq!(peak, MAX_CONCURRENT_EVALS, "the bound did not hold");
        // and all twenty still got their turn, a permit at a time
        assert_eq!(store.sensor_ticks(None, 50).unwrap().len(), 20);
    }

    #[tokio::test]
    async fn a_timed_out_evaluation_errors_and_leaves_the_cursor_alone() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["stuck".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let entry = SensorEntry::user(
            Sensor::new(
                "stuck",
                Duration::from_secs(3600),
                |ctx: SensorCtx| async move {
                    ctx.set_cursor(json!("moved"));
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok(vec![RunRequest::new("etl")])
                },
            )
            .timeout(Duration::from_millis(50)),
        );
        evaluate(&entry, &runner, &AssetRegistry::empty()).await;

        let ticks = store.sensor_ticks(Some("stuck"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert!(
            ticks[0].error.as_deref().unwrap().contains("timed out"),
            "{:?}",
            ticks[0].error
        );
        assert_eq!(cursor_of(&store, "stuck"), None);
        assert!(store.runs(None, None, None, None, 10).unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_cooperative_sensor_can_see_its_deadline_pass() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["chunks".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let chunks = Arc::new(AtomicU32::new(0));
        let counter = chunks.clone();
        // the shape a blocking closure has: work in chunks, checking between
        // them. blocking work cannot be abandoned, so this is the only way it
        // ever stops — and stopping is the closure's own decision to make
        let entry = SensorEntry::user(
            Sensor::new(
                "chunks",
                Duration::from_secs(3600),
                move |ctx: SensorCtx| {
                    let chunks = counter.clone();
                    async move {
                        ctx.set_cursor(json!("done"));
                        for _ in 0..1000 {
                            if ctx.is_cancelled() {
                                return Err("evaluation timed out".into());
                            }
                            chunks.fetch_add(1, Ordering::SeqCst);
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Ok(vec![])
                    }
                },
            )
            .timeout(Duration::from_millis(60)),
        );
        evaluate(&entry, &runner, &AssetRegistry::empty()).await;

        assert!(
            chunks.load(Ordering::SeqCst) < 1000,
            "the closure never saw its deadline"
        );
        let ticks = store.sensor_ticks(Some("chunks"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Error);
        assert_eq!(
            cursor_of(&store, "chunks"),
            None,
            "a cursor staged past the deadline was committed"
        );
    }

    // ---- failure backoff -----------------------------------------------

    #[test]
    fn consecutive_failures_lengthen_the_gap_and_cap_it() {
        let every = Duration::from_secs(10);
        // no failures is the interval exactly: no jitter on the happy path
        assert_eq!(next_gap(every, 0), every);
        // the floor doubles with the ceiling, so the gap really does lengthen
        for (failures, lo, hi) in [(1, 10, 20), (2, 20, 40), (3, 40, 80), (4, 80, 160)] {
            let (lo, hi) = (Duration::from_secs(lo), Duration::from_secs(hi));
            for _ in 0..32 {
                let gap = next_gap(every, failures);
                assert!(gap >= lo && gap <= hi, "{failures} failures gave {gap:?}");
            }
        }
        // and it stops growing there rather than climbing to a day
        for failures in [12, 40, u32::MAX] {
            let gap = next_gap(every, failures);
            assert!(gap >= BACKOFF_MAX / 2 && gap <= BACKOFF_MAX, "{gap:?}");
        }
        // a sensor whose interval already exceeds the cap has nothing to back
        // off to, and must not be sped up instead
        let hourly = Duration::from_secs(3600);
        assert_eq!(next_gap(hourly, 5), hourly);
    }

    #[test]
    fn one_success_resets_the_backoff() {
        let state = SensorState::new();
        let every = Duration::from_secs(10);
        for expected in 1..=3 {
            state.claim(every);
            state.release(every, true);
            assert_eq!(state.snapshot().1, expected);
        }
        let (before, _) = state.snapshot();
        assert!(before >= Utc::now() + chrono::Duration::seconds(39));

        state.claim(every);
        state.release(every, false);
        let (after, failures) = state.snapshot();
        assert_eq!(failures, 0);
        assert!(
            after <= Utc::now() + chrono::Duration::seconds(11),
            "one success did not collapse the gap back to the interval"
        );
    }

    #[tokio::test]
    async fn a_failing_sensor_is_evaluated_less_and_less_often() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["broken".into()]).unwrap();
        let runner = echo_runner(store.clone());
        let calls = Arc::new(AtomicU32::new(0));
        let counter = calls.clone();
        // every 20ms and always failing, so the gaps floor at 20, 40, 80, 160
        // and 320ms: six evaluations at the outside in 500ms, where the
        // interval alone would allow twenty-five
        let entry = SensorEntry::user(Sensor::new(
            "broken",
            Duration::from_millis(20),
            move |_ctx: SensorCtx| {
                let calls = counter.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err("endpoint is down".into())
                }
            },
        ));
        let handle = tokio::spawn(run_sensors(
            vec![entry],
            runner,
            Arc::new(AssetRegistry::empty()),
        ));
        tokio::time::sleep(Duration::from_millis(500)).await;
        handle.abort();

        let calls = calls.load(Ordering::SeqCst);
        assert!((2..=6).contains(&calls), "{calls} evaluations in 500ms");
        let ticks = store.sensor_ticks(Some("broken"), 20).unwrap();
        assert_eq!(ticks.len() as u32, calls);
        assert!(ticks.iter().all(|t| t.outcome == SensorOutcome::Error));
    }
}
