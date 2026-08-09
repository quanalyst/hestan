use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;

use crate::asset::{Asset, AssetCheck, AssetRegistry, mats_map, plan_target};
use crate::error::Error;
use crate::executor::{FailureHook, RunFailure, Runner};
use crate::io::{Io, IoManager};
use crate::job::Job;
use crate::model::{Run, ScheduleDef, Trigger};
use crate::resource::{self, Resource, ResourceCtx, ResourceFn};
use crate::schedule::{self, ScheduleEntry};
use crate::sensor::{Sensor, SensorEntry, SensorEval, run_sensors};
use crate::server::{AppState, SensorInfo, router};
use crate::store::Store;

/// how many materializations each asset keeps unless
/// [`asset_history`](Hestan::asset_history) says otherwise.
const DEFAULT_ASSET_HISTORY: usize = 200;

/// entry point: collect jobs, assets, sensors and schedules, then `serve` the ui
/// or `run_once` headless.
pub struct Hestan {
    jobs: Vec<Job>,
    schedules: Vec<ScheduleDef>,
    assets: Vec<Asset>,
    checks: Vec<AssetCheck>,
    sensors: Vec<Sensor>,
    pools: Vec<(String, usize)>,
    resources: Vec<(String, ResourceFn)>,
    io_default: Option<Arc<dyn IoManager>>,
    io_named: HashMap<String, Arc<dyn IoManager>>,
    db_path: String,
    hooks: Vec<FailureHook>,
    retention_days: Option<u32>,
    asset_history: usize,
    #[cfg(feature = "http")]
    sources: Vec<crate::http::HttpSource>,
}

impl Default for Hestan {
    fn default() -> Self {
        Hestan {
            jobs: Vec::new(),
            schedules: Vec::new(),
            assets: Vec::new(),
            checks: Vec::new(),
            sensors: Vec::new(),
            pools: Vec::new(),
            resources: Vec::new(),
            io_default: None,
            io_named: HashMap::new(),
            db_path: "hestan.db".into(),
            hooks: Vec::new(),
            retention_days: None,
            asset_history: DEFAULT_ASSET_HISTORY,
            #[cfg(feature = "http")]
            sources: Vec::new(),
        }
    }
}

impl Hestan {
    /// an empty builder; nothing is opened or validated until `serve`,
    /// `run_once`, or `build_asset`.
    pub fn new() -> Hestan {
        Hestan::default()
    }

    /// register a job; stackable. duplicate names collide at build.
    pub fn job(mut self, job: Job) -> Self {
        self.jobs.push(job);
        self
    }

    pub fn jobs(mut self, jobs: impl IntoIterator<Item = Job>) -> Self {
        self.jobs.extend(jobs);
        self
    }

    /// register assets; stackable. when any exist, build registers the
    /// internal job `"assets"` that materializes them — a user job with that
    /// name then collides ([`Error::DuplicateJob`]).
    pub fn assets(mut self, assets: impl IntoIterator<Item = Asset>) -> Self {
        self.assets.extend(assets);
        self
    }

    /// register an [asset check](AssetCheck); stackable. it lowers into an op
    /// of the internal `assets` job named `check:{asset}:{check}`, so it runs
    /// as part of the build that materialized the asset. naming an unknown
    /// asset or a source, or declaring the same check name twice on one
    /// asset, is [`Error::Graph`] at build.
    pub fn check(mut self, check: AssetCheck) -> Self {
        self.checks.push(check);
        self
    }

    /// register a sensor; stackable. `serve` runs one loop evaluating every
    /// sensor (and every source probe) on its interval.
    pub fn sensor(mut self, sensor: Sensor) -> Self {
        self.sensors.push(sensor);
        self
    }

    /// declare a named concurrency pool: at most `limit` ops that name it via
    /// [`Op::pool`](crate::Op::pool) run at once, across every job in this
    /// process. that is the shape most external limits have — "at most 3
    /// requests to this api, ever" — which per-job `max_parallel` cannot
    /// express once two jobs can overlap. stackable; declaring the same name
    /// twice, or naming an undeclared pool from an op, fails the build with
    /// [`Error::Graph`]. a limit below 1 means 1.
    pub fn pool(mut self, name: impl Into<String>, limit: usize) -> Self {
        self.pools.push((name.into(), limit));
        self
    }

    /// register a process-wide resource: a value built once at startup and
    /// shared by every op that asks for it, which is what replaces capturing
    /// a client in a closure.
    ///
    /// ```no_run
    /// # use hestan::{Hestan, Op, OpCtx};
    /// # use serde_json::json;
    /// # #[derive(Clone)] struct ApiClient;
    /// # impl ApiClient { fn new() -> Result<ApiClient, std::io::Error> { Ok(ApiClient) } }
    /// Hestan::new()
    ///     .resource("api", |_| async { Ok(ApiClient::new()?) })
    ///     .job(todo!());
    /// ```
    ///
    /// the constructor is async and fallible, and runs during startup before
    /// the store is opened, so a resource that cannot be built aborts the
    /// process with [`Error::Resource`] rather than leaving a half-live
    /// server. resources are built in declaration order and each is handed a
    /// [`ResourceCtx`] holding the ones before it, so a client can lean on
    /// the config it reads.
    ///
    /// resources live for the process: there is no per-run scoping and no
    /// teardown hook in this phase. anything needing either should own it
    /// inside the op.
    ///
    /// ops read one with [`OpCtx::resource`](crate::OpCtx::resource), and
    /// declaring it with [`Op::requires`](crate::Op::requires) turns a
    /// missing name into a build error. declaring the same name twice is
    /// [`Error::Resource`].
    pub fn resource<T, F, Fut>(mut self, name: impl Into<String>, f: F) -> Self
    where
        T: Any + Send + Sync,
        F: FnOnce(ResourceCtx) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let ctor: ResourceFn = Box::new(move |ctx: ResourceCtx| {
            Box::pin(async move {
                let value = f(ctx).await?;
                Ok(Resource {
                    type_name: std::any::type_name::<T>(),
                    value: Arc::new(value),
                })
            })
        });
        self.resources.push((name.into(), ctor));
        self
    }

    /// where op outputs are persisted, for every op that does not select a
    /// named manager. the default is [`Inline`](crate::Inline) — outputs are
    /// their own handles and land in the run log as json, which is what
    /// hestan has always done and is wrong for anything bulky.
    ///
    /// ```no_run
    /// # use hestan::{FileIo, Hestan};
    /// Hestan::new().io(FileIo::new("/var/lib/hestan/io"));
    /// ```
    pub fn io(mut self, manager: impl IoManager) -> Self {
        self.io_default = Some(Arc::new(manager));
        self
    }

    /// register an io manager under `name`, for ops that select it with
    /// [`Op::io`](crate::Op::io). naming one that was never registered here
    /// fails the build; registering the same name twice keeps the last.
    pub fn io_named(mut self, name: impl Into<String>, manager: impl IoManager) -> Self {
        self.io_named.insert(name.into(), Arc::new(manager));
        self
    }

    /// attach a 5-field cron expression to a job, evaluated in utc; fires
    /// launch with params `{}`. validated in serve/run_once.
    pub fn schedule(self, job: impl Into<String>, cron_expr: impl Into<String>) -> Self {
        self.schedule_with(job, cron_expr, serde_json::json!({}))
    }

    /// like [`Hestan::schedule`] but evaluated in a named iana timezone.
    pub fn schedule_tz(
        self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        tz: impl Into<String>,
    ) -> Self {
        self.schedule_tz_with(job, cron_expr, tz, serde_json::json!({}))
    }

    /// like [`Hestan::schedule`] with the params every fire launches with.
    /// they go through the job's op validators at build, so a schedule that
    /// could never launch is [`Error::InvalidParams`] at startup rather than a
    /// tick that fails forever at 3am.
    pub fn schedule_with(
        mut self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        params: Value,
    ) -> Self {
        self.schedules
            .push((job.into(), cron_expr.into(), "UTC".into(), params));
        self
    }

    /// [`Hestan::schedule_with`] in a named iana timezone.
    pub fn schedule_tz_with(
        mut self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        tz: impl Into<String>,
        params: Value,
    ) -> Self {
        self.schedules
            .push((job.into(), cron_expr.into(), tz.into(), params));
        self
    }

    /// register an http source: build lowers it into a job named after the
    /// source, plus a schedule if `cron` was set.
    #[cfg(feature = "http")]
    pub fn source(mut self, src: crate::http::HttpSource) -> Self {
        self.sources.push(src);
        self
    }

    /// where the run log lives; defaults to `hestan.db`. `":memory:"` works
    /// for tests and keeps nothing.
    pub fn db(mut self, path: impl Into<String>) -> Self {
        self.db_path = path.into();
        self
    }

    /// at startup, delete terminal runs older than `days` days with their op runs
    /// and events. active runs and op state survive; the default keeps everything.
    pub fn retention_days(mut self, days: u32) -> Self {
        self.retention_days = Some(days);
        self
    }

    /// keep at most `n` materializations per asset and `n` results per check,
    /// trimmed at startup (default 200). both are append-only and grow with
    /// every build, so unlike [`retention_days`](Self::retention_days) this
    /// cap applies whether you ask for it or not.
    ///
    /// the newest entry is never trimmed, whatever `n` says: an asset's
    /// newest materialization is its current state — what staleness compares
    /// against and what a memoized build seeds — and a check's newest result
    /// is what the asset summary counts.
    pub fn asset_history(mut self, n: usize) -> Self {
        self.asset_history = n;
        self
    }

    /// call `hook` whenever a run finishes failed — never on success, on cancel,
    /// or for runs the startup sweep marks failed. callable multiple times.
    pub fn on_failure(mut self, hook: impl Fn(RunFailure) + Send + Sync + 'static) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    pub async fn run_once(self, job: &str, params: Value) -> Result<Run, Error> {
        let built = self.build().await?;
        built.runner.run(job, params, Trigger::Manual).await
    }

    /// materialize `name` headless, like [`run_once`](Self::run_once): one run of
    /// its stale ancestors plus the target, which always rebuilds. check
    /// `GET /api/assets` first if you only want to build when stale.
    pub async fn build_asset(self, name: &str) -> Result<Run, Error> {
        let built = self.build().await?;
        let mats = mats_map(built.runner.store())?;
        let plan = plan_target(&built.registry, &mats, name)?;
        built
            .runner
            .run_subset(
                crate::asset::ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                serde_json::json!({}),
                Trigger::Build,
            )
            .await
    }

    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        let addr = addr.into();
        let built = self.build().await?;
        // bind before spawning the loops: a bind failure must not leave detached
        // tasks firing jobs into a server that never started
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let scheduler = tokio::spawn(schedule::run_scheduler(built.entries, built.runner.clone()));
        let sensor_infos: Vec<SensorInfo> = built
            .sensor_entries
            .iter()
            .map(|e| SensorInfo {
                name: e.name.clone(),
                every: e.every,
            })
            .collect();
        let sensors = tokio::spawn(run_sensors(
            built.sensor_entries,
            built.runner.clone(),
            built.registry.clone(),
        ));
        let state = AppState {
            jobs: Arc::new(built.runner.jobs().clone()),
            runner: built.runner,
            assets: built.registry,
            sensors: Arc::new(sensor_infos),
        };
        tracing::info!("hestan ui on http://{addr}");
        let served = axum::serve(listener, router(state)).await;
        scheduler.abort();
        sensors.abort();
        served?;
        Ok(())
    }

    async fn build(self) -> Result<Built, Error> {
        #[cfg(feature = "http")]
        let (mut jobs, schedules) = {
            let (mut jobs, mut schedules) = (self.jobs, self.schedules);
            for src in &self.sources {
                let name = src
                    .name
                    .clone()
                    .ok_or_else(|| Error::Graph("http source needs a name".into()))?;
                // build_job, not into_job: the cron is consumed below, so the
                // dropped-cron warning would be a lie here
                jobs.push(src.build_job(&name)?);
                if let Some((expr, tz)) = &src.cron {
                    schedules.push((name, expr.clone(), tz.clone(), serde_json::json!({})));
                }
            }
            (jobs, schedules)
        };
        #[cfg(not(feature = "http"))]
        let (mut jobs, schedules) = (self.jobs, self.schedules);

        // lowered only when assets exist, so the name stays free otherwise, and
        // before the duplicate check, so a user job named "assets" collides below
        let registry = if self.assets.is_empty() {
            // a check with no assets at all can only be naming one that does
            // not exist, and saying so beats dropping it silently
            if let Some(check) = self.checks.first() {
                return Err(Error::Graph(format!(
                    "check {}: no assets are registered",
                    check.name()
                )));
            }
            Arc::new(AssetRegistry::empty())
        } else {
            let registry = Arc::new(AssetRegistry::new(self.assets, self.checks)?);
            jobs.push(registry.lower_job()?);
            registry
        };

        let mut names = HashSet::new();
        for job in &jobs {
            if !names.insert(job.name().to_string()) {
                return Err(Error::DuplicateJob(job.name().to_string()));
            }
        }
        let mut entries = Vec::new();
        for (job, expr, tz, params) in &schedules {
            let Some(defined) = jobs.iter().find(|j| j.name() == job) else {
                return Err(Error::UnknownJob(job.clone()));
            };
            // the same validators a launch runs, at startup: a schedule whose
            // params no op accepts is a build error, not a 3am tick that fails
            if let Some((op, reason)) = defined.params_error(params) {
                return Err(Error::InvalidParams {
                    op,
                    reason: format!("schedule {expr} on job {job}: {reason}"),
                });
            }
            entries.push(schedule::parse(job, expr, tz)?.with_params(params.clone()));
        }

        let mut sensor_entries: Vec<SensorEntry> =
            self.sensors.into_iter().map(SensorEntry::user).collect();
        for meta in registry.topo() {
            if let Some(probe) = &meta.probe {
                sensor_entries.push(SensorEntry {
                    name: format!("probe:{}", meta.name),
                    every: meta.probe_every,
                    eval: SensorEval::Probe {
                        asset: meta.name.clone(),
                        probe: probe.clone(),
                    },
                });
            }
        }
        let mut sensor_names = HashSet::new();
        for e in &sensor_entries {
            if !sensor_names.insert(e.name.clone()) {
                return Err(Error::Graph(format!("duplicate sensor {}", e.name)));
            }
        }

        // before the store opens: a process whose api client could not be
        // built has nothing useful to serve, and should not leave a database
        // behind saying otherwise
        let resources = resource::build(self.resources).await?;

        let store = Store::open(&self.db_path)?;
        store.fail_interrupted()?;
        store.sync_schedules(&schedules)?;
        store.prune_ticks(5000)?;
        let sensor_names: Vec<String> = sensor_entries.iter().map(|e| e.name.clone()).collect();
        store.sync_sensors(&sensor_names)?;
        store.prune_sensor_ticks(5000)?;
        let trimmed = store.prune_materializations(self.asset_history)?;
        let trimmed = trimmed + store.prune_asset_checks(self.asset_history)?;
        if trimmed > 0 {
            tracing::info!("trimmed {trimmed} asset history rows past the cap");
        }
        if let Some(days) = self.retention_days {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
            let removed = store.prune_runs(cutoff)?;
            if removed > 0 {
                tracing::info!("retention: removed {removed} runs older than {days} days");
            }
        }
        let io = Io::new(self.io_default, self.io_named);
        let runner = Runner::with_resources(jobs, store, self.hooks, self.pools, resources, io)?;
        Ok(Built {
            runner,
            entries,
            registry,
            sensor_entries,
        })
    }
}

struct Built {
    runner: Runner,
    entries: Vec<ScheduleEntry>,
    registry: Arc<AssetRegistry>,
    sensor_entries: Vec<SensorEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use crate::model::RunStatus;
    use crate::op::Op;
    use serde_json::json;

    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Window {
        days: u32,
    }

    fn windowed(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new("render", |_| async { Ok(json!(null)) }).params::<Window>())
            .build()
            .unwrap()
    }

    fn pooled(job: &str) -> Job {
        Job::builder(job)
            .op(Op::new("call", |_| async { Ok(serde_json::json!(null)) }).pool("api"))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn a_pool_must_be_declared_before_an_op_takes_from_it() {
        let err = Hestan::new()
            .job(pooled("pull"))
            .db(":memory:")
            .run_once("pull", json!({}))
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::Graph(_)), "{err}");
        assert!(err.to_string().contains("not declared"), "{err}");

        let err = Hestan::new()
            .job(pooled("pull"))
            .pool("api", 2)
            .pool("api", 3)
            .db(":memory:")
            .run_once("pull", json!({}))
            .await
            .err()
            .unwrap();
        assert!(err.to_string().contains("declared twice"), "{err}");

        let run = Hestan::new()
            .job(pooled("pull"))
            .pool("api", 2)
            .db(":memory:")
            .run_once("pull", json!({}))
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
    }

    // a schedule that could never launch is a startup error, not a tick that
    // fails forever at 3am
    #[tokio::test]
    async fn schedule_params_are_validated_at_build() {
        let good = json!({"days": 7});
        let err = Hestan::new()
            .job(windowed("report"))
            .schedule_with("report", "0 9 * * *", json!({"days": "many"}))
            .db(":memory:")
            .run_once("report", good.clone())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, Error::InvalidParams { ref op, .. } if op == "render"),
            "{err}"
        );
        assert!(
            err.to_string().contains("schedule 0 9 * * * on job report"),
            "{err}"
        );

        // the plain form fires with {}, which this job's op also refuses
        let err = Hestan::new()
            .job(windowed("report"))
            .schedule("report", "0 9 * * *")
            .db(":memory:")
            .run_once("report", good.clone())
            .await
            .err()
            .unwrap();
        assert!(matches!(err, Error::InvalidParams { .. }), "{err}");

        let run = Hestan::new()
            .job(windowed("report"))
            .schedule_with("report", "0 9 * * *", good.clone())
            .db(":memory:")
            .run_once("report", good)
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
    }

    #[tokio::test]
    async fn schedule_params_survive_a_pause_and_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        let boot = || {
            Hestan::new()
                .job(windowed("report"))
                .schedule_with("report", "0 9 * * *", json!({"days": 7}))
                .db(path.clone())
        };

        boot().run_once("report", json!({"days": 1})).await.unwrap();
        let store = Store::open(&path).unwrap();
        assert!(
            store
                .set_schedule_paused("report", "0 9 * * *", true)
                .unwrap()
        );
        drop(store);

        boot().run_once("report", json!({"days": 1})).await.unwrap();
        let store = Store::open(&path).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].paused, "the pause did not survive the restart");
        assert_eq!(rows[0].params, json!({"days": 7}));
    }
}
