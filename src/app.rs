use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;

use crate::asset::{
    Asset, AssetCheck, AssetRegistry, MultiAsset, asset_tag, mats_map, plan_target,
};
use crate::error::Error;
use crate::executor::{FailureHook, RunFailure, Runner};
use crate::freshness::{self, LateEvent, LateHook};
use crate::io::{Io, IoManager};
use crate::job::Job;
use crate::model::{Run, RunTags, Trigger};
use crate::resource::{self, Resource, ResourceCtx, ResourceFn};
use crate::schedule::{self, Schedule, ScheduleEntry};
use crate::sensor::{RunStatusSensor, Sensor, SensorEntry, run_sensors};
use crate::server::{AppState, SensorInfo, router};
use crate::store::Store;

/// how many materializations each asset keeps unless
/// [`asset_history`](Hestan::asset_history) says otherwise.
const DEFAULT_ASSET_HISTORY: usize = 200;

/// entry point: collect jobs, assets, sensors and schedules, then `serve` the ui
/// or `run_once` headless.
pub struct Hestan {
    jobs: Vec<Job>,
    schedules: Vec<Schedule>,
    presets: Vec<(String, String, Value)>,
    run_tags: RunTags,
    assets: Vec<Asset>,
    multis: Vec<MultiAsset>,
    checks: Vec<AssetCheck>,
    sensors: Vec<Sensor>,
    run_sensors: Vec<RunStatusSensor>,
    pools: Vec<(String, usize)>,
    resources: Vec<(String, ResourceFn)>,
    io_default: Option<Arc<dyn IoManager>>,
    io_named: HashMap<String, Arc<dyn IoManager>>,
    db_path: String,
    hooks: Vec<FailureHook>,
    late_hooks: Vec<LateHook>,
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
            presets: Vec::new(),
            run_tags: RunTags::new(),
            assets: Vec::new(),
            multis: Vec::new(),
            checks: Vec::new(),
            sensors: Vec::new(),
            run_sensors: Vec::new(),
            pools: Vec::new(),
            resources: Vec::new(),
            io_default: None,
            io_named: HashMap::new(),
            db_path: "hestan.db".into(),
            hooks: Vec::new(),
            late_hooks: Vec::new(),
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

    /// register [multi-assets](MultiAsset); stackable. each lowers to one op
    /// of the same internal `assets` job and materializes every asset it
    /// produces, which then behave exactly like assets registered with
    /// [`assets`](Self::assets) — deps, checks, staleness, builds and all.
    pub fn multi_assets(mut self, multis: impl IntoIterator<Item = MultiAsset>) -> Self {
        self.multis.extend(multis);
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

    /// register a [run-status sensor](RunStatusSensor): "when job A succeeds,
    /// run job B". stackable, and registered as `run:{name}` alongside every
    /// other sensor — it runs on the same loop, on its own interval, and
    /// pauses the same way.
    pub fn run_sensor(mut self, sensor: RunStatusSensor) -> Self {
        self.run_sensors.push(sensor);
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

    /// register a [`Schedule`]: job, cron expression, and any of timezone,
    /// params and [catch-up policy](crate::Catchup) that differ from the
    /// defaults.
    ///
    /// ```no_run
    /// # use hestan::{Catchup, Hestan, Schedule};
    /// # use serde_json::json;
    /// Hestan::new().add_schedule(
    ///     Schedule::new("orders_etl", "0 * * * *")
    ///         .params(json!({"region": "eu"}))
    ///         .catchup(Catchup::All { limit: 24 }),
    /// );
    /// ```
    ///
    /// [`schedule`](Self::schedule), [`schedule_tz`](Self::schedule_tz),
    /// [`schedule_with`](Self::schedule_with) and
    /// [`schedule_tz_with`](Self::schedule_tz_with) are this with the defaults
    /// filled in, and stay the short way to say the common thing.
    pub fn add_schedule(mut self, schedule: Schedule) -> Self {
        self.schedules.push(schedule);
        self
    }

    /// attach a 5-field cron expression to a job, evaluated in utc; fires
    /// launch with params `{}`. validated in serve/run_once.
    pub fn schedule(self, job: impl Into<String>, cron_expr: impl Into<String>) -> Self {
        self.add_schedule(Schedule::new(job, cron_expr))
    }

    /// like [`Hestan::schedule`] but evaluated in a named iana timezone.
    pub fn schedule_tz(
        self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        tz: impl Into<String>,
    ) -> Self {
        self.add_schedule(Schedule::new(job, cron_expr).tz(tz))
    }

    /// like [`Hestan::schedule`] with the params every fire launches with.
    /// they go through the job's op validators at build, so a schedule that
    /// could never launch is [`Error::InvalidParams`] at startup rather than a
    /// tick that fails forever at 3am.
    pub fn schedule_with(
        self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        params: Value,
    ) -> Self {
        self.add_schedule(Schedule::new(job, cron_expr).params(params))
    }

    /// [`Hestan::schedule_with`] in a named iana timezone.
    pub fn schedule_tz_with(
        self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        tz: impl Into<String>,
        params: Value,
    ) -> Self {
        self.add_schedule(Schedule::new(job, cron_expr).tz(tz).params(params))
    }

    /// declare a named parameter set for `job`, seeded into the store at
    /// build. stackable.
    ///
    /// ```no_run
    /// # use hestan::Hestan;
    /// # use serde_json::json;
    /// Hestan::new().preset("orders_etl", "nightly", json!({"region": "eu", "days": 1}));
    /// ```
    ///
    /// presets are runtime data — the launchpad saves and deletes them too —
    /// so a declared one is an **upsert**: it refreshes on every start, and a
    /// preset made in the ui under another name is left alone. dropping the
    /// declaration therefore leaves the stored preset behind; delete it from
    /// the ui or with [`Store::delete_preset`](crate::Store::delete_preset).
    ///
    /// the params go through the job's op validators at build, exactly as a
    /// [schedule's](Self::schedule_with) do, so a preset that could never
    /// launch is [`Error::InvalidParams`] at startup rather than a 400 at 2am.
    pub fn preset(
        mut self,
        job: impl Into<String>,
        name: impl Into<String>,
        params: Value,
    ) -> Self {
        self.presets.push((job.into(), name.into(), params));
        self
    }

    /// tag every run this process launches with `tags` — the deployment,
    /// the region, whatever a run's provenance needs to say. stackable, and a
    /// repeated key keeps the last.
    ///
    /// ```no_run
    /// # use hestan::Hestan;
    /// Hestan::new().run_tags([("env", "prod"), ("cluster", "eu-1")]);
    /// ```
    ///
    /// these are **defaults**: a launch that names the same key wins, since a
    /// default is a fact about the deployment and the launch is closer to the
    /// truth. automatic tags on machine-made runs win the same way.
    pub fn run_tags<I, K, V>(mut self, tags: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.run_tags
            .extend(tags.into_iter().map(|(k, v)| (k.into(), v.into())));
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
    /// and events, plus [sensor run keys](crate::RunRequest::key) claimed before
    /// the same cutoff. active runs and op state survive; the default keeps
    /// everything, run keys included.
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

    /// call `hook` whenever a job or asset with a declared freshness policy
    /// crosses from fresh to late — [`JobBuilder::fresh_within`](crate::JobBuilder::fresh_within)
    /// and [`Asset::fresh_within`](crate::Asset::fresh_within). callable
    /// multiple times, dispatched exactly like [`on_failure`](Self::on_failure)
    /// so a hook may block.
    ///
    /// ```no_run
    /// # use hestan::{Hestan, LateEvent};
    /// Hestan::new().on_late(|e: LateEvent| {
    ///     eprintln!("{} {} is {:?} late", e.kind.as_str(), e.name, e.late_by)
    /// });
    /// ```
    ///
    /// once per crossing, not once per poll: the last-notified state lives in
    /// the database, so something late for a week alerts once and survives a
    /// restart without re-announcing itself. a recovery is not an alert, but it
    /// re-arms the next one. `serve` runs the checker; `run_once` does not.
    pub fn on_late(mut self, hook: impl Fn(LateEvent) + Send + Sync + 'static) -> Self {
        self.late_hooks.push(Arc::new(hook));
        self
    }

    pub async fn run_once(self, job: &str, params: Value) -> Result<Run, Error> {
        // the worker guard, first: see `serve`
        #[cfg(unix)]
        if let Some(req) = crate::isolate::requested() {
            self.work(req).await
        }
        let built = self.build().await?;
        built.runner.run(job, params, Trigger::Manual).await
    }

    /// materialize `name` headless, like [`run_once`](Self::run_once): one run of
    /// its stale ancestors plus the target, which always rebuilds. check
    /// `GET /api/assets` first if you only want to build when stale.
    pub async fn build_asset(self, name: &str) -> Result<Run, Error> {
        // the worker guard, first: see `serve`
        #[cfg(unix)]
        if let Some(req) = crate::isolate::requested() {
            self.work(req).await
        }
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
                asset_tag(name),
            )
            .await
    }

    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        // before the address, before the store, before anything: this process
        // may be a worker child of a hestan already running against this
        // database, and every line of boot behaviour below assumes it owns the
        // place. `fail_interrupted` alone would mark its own parent's in-flight
        // runs as interrupted, mid-run. `work` never returns.
        #[cfg(unix)]
        if let Some(req) = crate::isolate::requested() {
            self.work(req).await
        }
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
                filter: e.filter(),
                state: e.state.clone(),
            })
            .collect();
        let sensors = tokio::spawn(run_sensors(
            built.sensor_entries,
            built.runner.clone(),
            built.registry.clone(),
        ));
        // the chunker: it launches each backfill's next range as the last one
        // finishes, so a long backfill never fires every partition at once
        let backfills = tokio::spawn(crate::backfill::run_backfills(
            built.runner.clone(),
            built.registry.clone(),
        ));
        let checker = tokio::spawn(freshness::run_checker(
            built.runner.clone(),
            built.registry.clone(),
            Arc::new(built.late_hooks),
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
        backfills.abort();
        checker.abort();
        served?;
        Ok(())
    }

    /// the jobs this process runs, with http sources and assets lowered in.
    ///
    /// the server path and the [worker](Self::work) path both go through here,
    /// which is what makes "the child rebuilds the same registry" true rather
    /// than hopeful: there is one lowering, and both processes run it.
    fn lower(&mut self) -> Result<(Vec<Job>, Arc<AssetRegistry>), Error> {
        let mut jobs = std::mem::take(&mut self.jobs);
        #[cfg(feature = "http")]
        for src in &self.sources {
            let name = src
                .name
                .clone()
                .ok_or_else(|| Error::Graph("http source needs a name".into()))?;
            // build_job, not into_job: the cron is consumed below, so the
            // dropped-cron warning would be a lie here
            jobs.push(src.build_job(&name)?);
            if let Some((expr, tz)) = &src.cron {
                self.schedules
                    .push(Schedule::new(name, expr.clone()).tz(tz.clone()));
            }
        }

        // lowered only when assets exist, so the name stays free otherwise, and
        // before the duplicate check, so a user job named "assets" collides below
        let registry = if self.assets.is_empty() && self.multis.is_empty() {
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
            let registry = Arc::new(AssetRegistry::new(
                std::mem::take(&mut self.assets),
                std::mem::take(&mut self.multis),
                std::mem::take(&mut self.checks),
            )?);
            jobs.push(registry.lower_job()?);
            registry
        };

        let mut names = HashSet::new();
        for job in &jobs {
            if !names.insert(job.name().to_string()) {
                return Err(Error::DuplicateJob(job.name().to_string()));
            }
        }
        Ok((jobs, registry))
    }

    /// the worker path: build the same registry the server path would, run one
    /// op of one run, and exit — 0 for a success, 1 for anything else.
    ///
    /// what is *not* here is the point of it. no `fail_interrupted`, no
    /// schedule sync, no tick prune, no retention sweep, no scheduler, sensor,
    /// freshness or backfill loop, and no listener: every one of those assumes
    /// this process owns the database, and a worker child owns nothing. it
    /// opens the store, reads what its op was handed, runs it, and writes the
    /// result back.
    #[cfg(unix)]
    async fn work(mut self, req: crate::isolate::Request) -> ! {
        let code = match self.worked(req).await {
            Ok(crate::isolate::Worked::Success) => 0,
            Ok(crate::isolate::Worked::Failed) => 1,
            // printed rather than traced: a worker whose store or registry is
            // wrong cannot record anything anywhere, and its stderr is its
            // parent's stderr
            Err(e) => {
                eprintln!("hestan worker: {e}");
                1
            }
        };
        std::process::exit(code)
    }

    #[cfg(unix)]
    async fn worked(
        &mut self,
        req: crate::isolate::Request,
    ) -> Result<crate::isolate::Worked, Error> {
        let (jobs, _) = self.lower()?;
        let resources = resource::build(std::mem::take(&mut self.resources)).await?;
        let store = Store::open(&self.db_path)?;
        let io = Io::new(self.io_default.take(), std::mem::take(&mut self.io_named));
        crate::isolate::work(&req, &jobs, &store, &io, &resources).await
    }

    async fn build(mut self) -> Result<Built, Error> {
        let (jobs, registry) = self.lower()?;
        let schedules = std::mem::take(&mut self.schedules);
        let mut entries = Vec::new();
        for s in &schedules {
            let (job, expr) = (&s.job, &s.expr);
            let Some(defined) = jobs.iter().find(|j| j.name() == job) else {
                return Err(Error::UnknownJob(job.clone()));
            };
            // the same validators a launch runs, at startup: a schedule whose
            // params no op accepts is a build error, not a 3am tick that fails
            if let Some((op, reason)) = defined.params_error(&s.params) {
                return Err(Error::InvalidParams {
                    op,
                    reason: format!("schedule {expr} on job {job}: {reason}"),
                });
            }
            entries.push(
                schedule::parse(job, expr, &s.tz)?
                    .with_params(s.params.clone())
                    .with_catchup(s.catchup),
            );
        }
        // and the same validators over declared presets: a preset that cannot
        // launch is not worth storing, wherever it was declared
        for (job, name, params) in &self.presets {
            let Some(defined) = jobs.iter().find(|j| j.name() == job) else {
                return Err(Error::UnknownJob(job.clone()));
            };
            if let Some((op, reason)) = defined.params_error(params) {
                return Err(Error::InvalidParams {
                    op,
                    reason: format!("preset {name} on job {job}: {reason}"),
                });
            }
        }

        let mut sensor_entries: Vec<SensorEntry> =
            self.sensors.into_iter().map(SensorEntry::user).collect();
        sensor_entries.extend(self.run_sensors.into_iter().map(SensorEntry::runs));
        for meta in registry.topo() {
            if let Some(probe) = &meta.probe {
                sensor_entries.push(SensorEntry::probe(
                    &meta.name,
                    probe.clone(),
                    meta.probe_every,
                ));
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
        // seeded, not synced: the launchpad's presets share the table, so
        // there is nothing here to sweep away
        for (job, name, params) in &self.presets {
            store.put_preset(job, name, params)?;
        }
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
            // sensor run keys are never collected on their own: a sensor keyed
            // by the day would keep a row per day for as long as the file lives
            let keys = store.prune_sensor_run_keys(cutoff)?;
            if keys > 0 {
                tracing::info!("retention: removed {keys} sensor run keys older than {days} days");
            }
        }
        let io = Io::new(self.io_default, self.io_named);
        let runner = Runner::with_resources(jobs, store, self.hooks, self.pools, resources, io)?
            .with_run_tags(self.run_tags);
        Ok(Built {
            runner,
            entries,
            registry,
            sensor_entries,
            late_hooks: self.late_hooks,
        })
    }
}

struct Built {
    runner: Runner,
    entries: Vec<ScheduleEntry>,
    registry: Arc<AssetRegistry>,
    sensor_entries: Vec<SensorEntry>,
    late_hooks: Vec<LateHook>,
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

    // the same reasoning as a schedule's params, in the same place
    #[tokio::test]
    async fn declared_preset_params_are_validated_at_build() {
        let good = json!({"days": 7});
        let err = Hestan::new()
            .job(windowed("report"))
            .preset("report", "nightly", json!({"days": "many"}))
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
            err.to_string().contains("preset nightly on job report"),
            "{err}"
        );

        let err = Hestan::new()
            .job(windowed("report"))
            .preset("ghost", "nightly", good.clone())
            .db(":memory:")
            .run_once("report", good.clone())
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, Error::UnknownJob(ref j) if j == "ghost"),
            "{err}"
        );

        let run = Hestan::new()
            .job(windowed("report"))
            .preset("report", "nightly", good.clone())
            .db(":memory:")
            .run_once("report", good)
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
    }

    // a declared preset is seeded on every start; one made in the ui beside it
    // is nobody else's business
    #[tokio::test]
    async fn a_declared_preset_upserts_without_clobbering_a_ui_made_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        let boot = |days: u32| {
            Hestan::new()
                .job(windowed("report"))
                .preset("report", "nightly", json!({"days": days}))
                .db(path.clone())
        };

        boot(1)
            .run_once("report", json!({"days": 1}))
            .await
            .unwrap();
        let store = Store::open(&path).unwrap();
        store
            .put_preset("report", "by_hand", &json!({"days": 30}))
            .unwrap();
        drop(store);

        // the declaration moved; the hand-made one did not
        boot(7)
            .run_once("report", json!({"days": 1}))
            .await
            .unwrap();
        let store = Store::open(&path).unwrap();
        let presets = store.presets("report").unwrap();
        assert_eq!(
            presets.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
            ["by_hand", "nightly"]
        );
        assert_eq!(presets[0].params, json!({"days": 30}));
        assert_eq!(presets[1].params, json!({"days": 7}));
    }

    // a default says something about the deployment; the launch is closer to
    // the truth about the run
    #[tokio::test]
    async fn default_run_tags_merge_with_per_launch_tags() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        Hestan::new()
            .job(windowed("report"))
            .run_tags([("env", "prod"), ("cluster", "eu-1")])
            .run_tags([("cluster", "eu-2")])
            .db(path.clone())
            .run_once("report", json!({"days": 1}))
            .await
            .unwrap();

        let store = Store::open(&path).unwrap();
        let run = &store.runs(None, None, None, None, None, 10).unwrap()[0];
        // stackable, last wins within the defaults themselves
        assert_eq!(run.tags["env"], "prod");
        assert_eq!(run.tags["cluster"], "eu-2");
        drop(store);

        // and a launch that names a default's key overrides it for that run
        let store = Store::open(&path).unwrap();
        let runner = Runner::new(vec![windowed("report")], store.clone())
            .with_run_tags(RunTags::from([("env".to_string(), "prod".to_string())]));
        let id = runner
            .launch_tagged(
                "report",
                json!({"days": 1}),
                Trigger::Manual,
                RunTags::from([
                    ("env".to_string(), "staging".to_string()),
                    ("kind".to_string(), "smoke".to_string()),
                ]),
            )
            .unwrap();
        let tags = store.run(&id).unwrap().unwrap().tags;
        assert_eq!(tags["env"], "staging");
        assert_eq!(tags["kind"], "smoke");
    }

    // the whole point of the worker guard: a child process must not run one
    // line of boot behaviour. `fail_interrupted` assumes the last process died,
    // so a worker that reached it would mark its own parent's in-flight runs as
    // interrupted, mid-run.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_worker_start_does_not_touch_the_runs_it_finds() {
        use crate::model::{OpStatus, RunTags};
        use crate::op::Op;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        let job = Job::builder("guarded")
            .op(Op::new("quick", |_| async { Ok(json!({"ran": true})) }).isolated())
            .build()
            .unwrap();

        // a queued run for the worker, and one belonging to nobody it knows —
        // which is exactly the shape of a parent's in-flight run
        let store = Store::open(&path).unwrap();
        let planted = |id: &str| Run {
            id: id.to_string(),
            job: "guarded".into(),
            status: RunStatus::Queued,
            trigger: Trigger::Manual,
            params: json!({}),
            created_at: chrono::Utc::now(),
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: None,
            scheduled_for: None,
            tags: RunTags::new(),
        };
        store
            .create_run(&planted("mine"), &["quick".to_string()])
            .unwrap();
        store
            .create_run(&planted("someone-elses"), &["quick".to_string()])
            .unwrap();
        drop(store);

        let req = crate::isolate::Request {
            run_id: "mine".into(),
            op: "quick".into(),
        };
        let mut app = Hestan::new().job(job).db(path.clone());
        let worked = app.worked(req).await.unwrap();
        assert!(matches!(worked, crate::isolate::Worked::Success));

        let store = Store::open(&path).unwrap();
        // the op it was sent for ran and recorded itself, through the ordinary
        // paths and nothing else
        let row = store.op_run("mine", "quick").unwrap().unwrap();
        assert_eq!(row.status, OpStatus::Success);
        assert_eq!(row.output, Some(json!({"ran": true})));
        // and the run it was never asked about is untouched: not failed, not
        // errored, and with no interruption announced in its log
        let other = store.run("someone-elses").unwrap().unwrap();
        assert_eq!(other.status, RunStatus::Queued, "{:?}", other.error);
        assert_eq!(other.error, None);
        assert_eq!(
            store
                .op_run("someone-elses", "quick")
                .unwrap()
                .unwrap()
                .status,
            OpStatus::Pending
        );
        // including the run the worker did work for: its own status is its
        // parent's business, not a worker's
        assert_eq!(
            store.run("mine").unwrap().unwrap().status,
            RunStatus::Queued
        );
        for id in ["mine", "someone-elses"] {
            let events = store.events(id, 0).unwrap();
            assert!(
                !events.iter().any(|e| e.message.contains("interrupted")),
                "worker announced an interruption on {id}: {:?}",
                events.iter().map(|e| &e.message).collect::<Vec<_>>()
            );
        }
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
