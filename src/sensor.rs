use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::asset::{
    ASSETS_JOB, AssetRegistry, ProbeFn, launch_plan, mats_map, plan_targets, staleness,
};
use crate::executor::Runner;
use crate::model::{SensorOutcome, Trigger};
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

// probes are sensors: a probed source becomes an entry named `probe:<asset>`
pub(crate) enum SensorEval {
    User(Arc<SensorFn>),
    Probe { asset: String, probe: Arc<ProbeFn> },
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
    let current = match runner.store().materialization(asset) {
        Ok(m) => m.map(|m| m.fingerprint),
        Err(e) => return (SensorOutcome::Error, 0, Some(e.to_string())),
    };
    if current.as_deref() != Some(fingerprint.as_str()) {
        tracing::info!(asset = %asset, "probe saw a new fingerprint");
        if let Err(e) =
            runner
                .store()
                .upsert_materialization(asset, &fingerprint, &json!({}), None, None)
        {
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
        Arc::new(AssetRegistry::new(vec![source, stats]).unwrap())
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
        let docs = store.materialization("docs").unwrap().unwrap();
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
        let stats = store.materialization("stats").unwrap().unwrap();
        assert_eq!(stats.inputs, json!({"docs": "one"}));
        let first_built = stats.built_at;
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);

        evaluate(&entry, &runner, &reg).await;
        assert_eq!(store.runs(None, None, None, None, 10).unwrap().len(), 1);
        let docs_again = store.materialization("docs").unwrap().unwrap();
        assert_eq!(docs_again.built_at, docs.built_at);
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks.len(), 2);
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 0);

        *fp.lock().unwrap() = "two".to_string();
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(
            store.materialization("docs").unwrap().unwrap().fingerprint,
            "two"
        );
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(
            wait_terminal(&runner, &runs[0].id).await,
            RunStatus::Success
        );
        let stats = store.materialization("stats").unwrap().unwrap();
        assert_eq!(stats.inputs, json!({"docs": "two"}));
        assert!(stats.built_at >= first_built);
    }

    #[tokio::test]
    async fn failing_probe_is_an_error_tick_without_writes() {
        let store = Store::open(":memory:").unwrap();
        store.sync_sensors(&["probe:docs".into()]).unwrap();
        let source =
            Asset::source("docs").probe(|| async { Err("disk on fire".to_string().into()) });
        let reg = Arc::new(AssetRegistry::new(vec![source]).unwrap());
        let runner = echo_runner(store.clone());
        let entry = probe_entry(&reg, "docs");
        evaluate(&entry, &runner, &reg).await;
        assert!(store.materialization("docs").unwrap().is_none());
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
        let reg = Arc::new(AssetRegistry::new(vec![s, a, b]).unwrap());
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
        let ma = store.materialization("a").unwrap().unwrap();
        let mb = store.materialization("b").unwrap().unwrap();
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
        };
        store.create_run(&active, &[]).unwrap();
        evaluate(&entry, &runner, &reg).await;
        assert_eq!(
            store.materialization("docs").unwrap().unwrap().fingerprint,
            "one"
        );
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 0);
        assert_eq!(store.runs(None, None, None, None, 10).unwrap().len(), 1);

        // the next tick sees an unchanged fingerprint and still catches up
        store.run_finished("active", RunStatus::Success).unwrap();
        evaluate(&entry, &runner, &reg).await;
        let ticks = store.sensor_ticks(Some("probe:docs"), 10).unwrap();
        assert_eq!(ticks[0].outcome, SensorOutcome::Fired);
        assert_eq!(ticks[0].launched, 1);
        let runs = store.runs(None, None, None, None, 10).unwrap();
        assert_eq!(runs.len(), 2);
        let build = runs.iter().find(|r| r.id != "active").unwrap();
        assert_eq!(wait_terminal(&runner, &build.id).await, RunStatus::Success);
        assert_eq!(
            store.materialization("stats").unwrap().unwrap().inputs,
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
            store.materialization("docs").unwrap().unwrap().fingerprint,
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
            store.materialization("stats").unwrap().unwrap().inputs,
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
}
