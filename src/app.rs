use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::asset::{
    Asset, AssetCheck, AssetRegistry, MultiAsset, asset_tag, mats_map, plan_target,
};
use crate::auth::{self, Auth};
use crate::error::Error;
use crate::executor::{Limits, Runner};
use crate::freshness::{self, LateEvent, LateHook};
use crate::hooks::{self, FailureHook, OpEvent, OpHook, RunEvent, RunFailure, RunHook};
use crate::io::{Io, IoManager};
use crate::job::Job;
use crate::logs;
use crate::model::{Reclaim, Role, Run, RunTags, Trigger};
use crate::resource::{self, Resource, ResourceCtx, ResourceFn};
use crate::retention::{self, Retention};
use crate::schedule::{self, Schedule, ScheduleEntry};
use crate::sensor::{RunStatusSensor, Sensor, SensorEntry, run_sensors};
use crate::server::{AppState, SensorInfo, router};
use crate::store::Store;

/// how many materializations each asset keeps unless
/// [`asset_history`](Hestan::asset_history) says otherwise.
const DEFAULT_ASSET_HISTORY: usize = 200;

/// entry point: collect jobs, assets, sensors and schedules, then `serve` the ui
/// or `run_once` headless.
///
/// ```no_run
/// # use hestan::{Hestan, Job, Op, Retention};
/// # use serde_json::json;
/// # async fn f(nightly: Job) -> Result<(), hestan::Error> {
/// Hestan::new()
///     .job(nightly)
///     .schedule("nightly", "0 3 * * *")
///     .db("var/hestan.db")
///     .retention(Retention::days(30).keep_last(50))
///     .serve(([127, 0, 0, 1], 4000))
///     .await
/// # }
/// ```
///
/// nothing here opens a file, resolves a name or validates a dag: the whole
/// builder is inert until one of [`serve`](Hestan::serve),
/// [`run_once`](Hestan::run_once) or [`build_asset`](Hestan::build_asset) is
/// called, which is where a bad cron expression or a cycle is reported. so
/// a registry can be assembled in pieces, by several modules, and handed
/// around before anyone decides what to do with it.
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
    /// `None` is not "no authentication": it is nothing configured, which is
    /// what [`up`](Hestan::up) refuses to serve on a reachable address.
    auth: Option<Auth>,
    hooks: Vec<FailureHook>,
    run_hooks: Vec<RunHook>,
    op_hooks: Vec<OpHook>,
    late_hooks: Vec<LateHook>,
    limits: Limits,
    priority: i64,
    reclaim: Reclaim,
    role: Role,
    slots: usize,
    retention: Retention,
    retention_every: Duration,
    durable: bool,
    asset_history: usize,
    log_bytes: u64,
    log_line_cap: u64,
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
            auth: None,
            hooks: Vec::new(),
            run_hooks: Vec::new(),
            op_hooks: Vec::new(),
            late_hooks: Vec::new(),
            limits: Limits::new(),
            priority: 0,
            reclaim: Reclaim::default(),
            role: Role::default(),
            slots: usize::MAX,
            retention: Retention::default(),
            retention_every: retention::DEFAULT_INTERVAL,
            durable: false,
            asset_history: DEFAULT_ASSET_HISTORY,
            log_bytes: logs::DEFAULT_BYTES,
            log_line_cap: logs::DEFAULT_LINES,
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

    /// register several jobs at once, for a deployment that builds its
    /// registry somewhere else and hands it over.
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

    /// cap how many runs execute at once, across every job. the rest wait on
    /// the queue in priority order and start as earlier ones finish; a value
    /// below 1 means 1.
    ///
    /// this is a limit on *executing* runs. a run sitting on the queue costs
    /// nothing and counts as nothing — which is what makes it different from
    /// [`Overlap`](crate::Overlap), the policy that decides whether a scheduled
    /// fire should exist at all while its job has a run outstanding.
    ///
    /// [`JobBuilder::max_concurrent_runs`](crate::JobBuilder::max_concurrent_runs)
    /// is the same thing scoped to one job, and [`tag_limit`](Self::tag_limit)
    /// scoped to a [tag](crate::RunTags).
    pub fn max_concurrent_runs(mut self, n: usize) -> Self {
        self.limits = std::mem::take(&mut self.limits).global(n);
        self
    }

    /// at most `n` runs carrying the tag `key: value` executing at once —
    /// `tag_limit("env", "prod", 2)` whatever the jobs are. stackable; the same
    /// pair twice keeps the last. a value below 1 means 1.
    ///
    /// tags are how a run says what it is beyond its job — see
    /// [`run_tags`](Self::run_tags) — so this is the limit to reach for when
    /// what is scarce belongs to none of the jobs in particular: the production
    /// warehouse, the paid api, the one machine with the gpu.
    pub fn tag_limit(mut self, key: impl Into<String>, value: impl Into<String>, n: usize) -> Self {
        self.limits = std::mem::take(&mut self.limits).tag(key, value, n);
        self
    }

    /// the queue position runs launched by this process get unless the launch
    /// asks for another. higher goes first, ties by creation time; 0 is the
    /// default and negatives are legal, which is how a deployment says "these
    /// are the background ones".
    ///
    /// priority is a preference rather than an order: the dispatcher skips a
    /// run a limit would block and starts the next one that fits, because one
    /// blocked run at the head of the queue holding up everything unrelated
    /// behind it is worse than starting things slightly out of turn.
    pub fn priority(mut self, n: i64) -> Self {
        self.priority = n;
        self
    }

    /// what this process does about the queue: [`Role::All`] (the default,
    /// and one process doing everything), [`Role::Scheduler`] (fires schedules
    /// and sensors, executes nothing) or [`Role::Worker`] (executes, decides
    /// nothing).
    ///
    /// **exactly one process** in a deployment should be `All` or `Scheduler`.
    /// schedules, sensors, freshness checks and backfill chunking are
    /// decisions, and two processes making them independently is two of every
    /// scheduled run — the store has no lock that would stop it. any number of
    /// processes may be `Worker`; that is what the queue is for.
    ///
    /// [`work`](Self::work) is `role(Role::Worker)` with the address made
    /// optional, and is the shorter way to say the common thing.
    ///
    /// [`run_once`](Self::run_once) and [`build_asset`](Self::build_asset)
    /// ignore this: a headless one-shot has to execute its own run or it would
    /// return nothing.
    pub fn role(mut self, role: Role) -> Self {
        self.role = role;
        self
    }

    /// how many runs **this process** will execute at once; unlimited unless
    /// set, which is right for a single process and wrong for a worker beside
    /// another.
    ///
    /// [`max_concurrent_runs`](Self::max_concurrent_runs) says how much work
    /// the deployment does at once and lives in the store, shared. this says
    /// how much of it lands here, and lives in this process. a worker with four
    /// slots claims at most four runs however long the queue is, which is what
    /// leaves the rest for the worker beside it — and what bounds what one
    /// container has to hold.
    pub fn slots(mut self, n: usize) -> Self {
        self.slots = n.max(1);
        self
    }

    /// what happens to a run whose claimer stopped saying it was there.
    ///
    /// every process executing a run renews its claim on a heartbeat, and a
    /// claim nobody has renewed for a minute is taken back by whichever process
    /// notices. the default, [`Reclaim::Fail`], fails the run and says why on
    /// its ops; [`Reclaim::Requeue`] puts it back on the queue.
    ///
    /// fail is the default because a run that got halfway may have done half
    /// its side effects, and doing them again quietly is worse than a stall
    /// somebody has to look at. requeue is the right answer when the work is
    /// idempotent.
    pub fn reclaim(mut self, reclaim: Reclaim) -> Self {
        self.reclaim = reclaim;
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
    ///
    /// # A database, worked through
    ///
    /// hestan wraps no database client. the client is
    /// [`tokio_postgres`](https://docs.rs/tokio-postgres)'s, the query is
    /// yours, and what hestan owns is when it runs and what is recorded about
    /// it. `docs/connecting.md` is the long version, and holds this example
    /// to what compiles here.
    ///
    /// ```
    /// use hestan::prelude::*;
    /// use tokio_postgres::{Client, NoTls};
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// #   let Ok(test_url) = std::env::var("HESTAN_TEST_PG") else { return Ok(()) };
    /// #   let (setup, driver) = tokio_postgres::connect(&test_url, NoTls).await?;
    /// #   tokio::spawn(driver);
    /// #   setup.batch_execute("create table if not exists orders (id int)").await?;
    ///     let nightly = Job::builder("nightly")
    ///         .op(Op::new("count_orders", |ctx: OpCtx| async move {
    ///             let db = ctx.resource::<Client>("warehouse")?;
    ///             let row = db.query_one("select count(*) from orders", &[]).await?;
    ///             let rows: i64 = row.get(0);
    ///             ctx.meta("rows", Meta::count(rows as u64));
    ///             Ok(json!({ "rows": rows }))
    ///         })
    ///         .requires(["warehouse"])
    ///         .retries(3))
    ///         .build()?;
    ///
    ///     let run = Hestan::new()
    ///         // connected once, shared by every op, and never a param: params
    ///         // are stored on the run and served over the api
    ///         .resource("warehouse", |_| async {
    /// #           let url = std::env::var("HESTAN_TEST_PG")?;
    /// #           /*
    ///             let url = std::env::var("WAREHOUSE_URL")?;
    /// #           */
    ///             let (client, driver) = tokio_postgres::connect(&url, NoTls).await?;
    ///             // the driver owns the socket and has to be polled by
    ///             // somebody; the client is the handle the ops share
    ///             tokio::spawn(driver);
    ///             Ok(client)
    ///         })
    ///         .job(nightly)
    ///         .db(":memory:")
    ///         .run_once("nightly", json!({}))
    ///         .await?;
    ///
    ///     assert_eq!(run.status, RunStatus::Success);
    ///     Ok(())
    /// }
    /// ```
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
    #[cfg_attr(docsrs, doc(cfg(feature = "http")))]
    pub fn source(mut self, src: crate::http::HttpSource) -> Self {
        self.sources.push(src);
        self
    }

    /// where the run log lives; defaults to `hestan.db`. `":memory:"` works
    /// for tests and keeps nothing.
    ///
    /// a target beginning `postgres://` or `postgresql://` is a
    /// [postgres][pg] database and anything else is a sqlite path. this one
    /// string is what an [isolated op](crate::Op::isolated)'s child process
    /// and every queue worker is handed, so every process reaches the same
    /// database.
    ///
    #[cfg_attr(feature = "postgres", doc = "[pg]: crate::Store::connect")]
    #[cfg_attr(not(feature = "postgres"), doc = "[pg]: crate::Store")]
    pub fn db(mut self, target: impl Into<String>) -> Self {
        self.db_path = target.into();
        self
    }

    /// what checks who is asking, and what each of them may do.
    ///
    /// ```no_run
    /// # use hestan::{Auth, Hestan};
    /// # fn f(app: Hestan) -> Hestan {
    /// app.auth(Auth::bearer(std::env::var("HESTAN_TOKEN").expect("a token")))
    /// # }
    /// ```
    ///
    /// unset — the default — is **not** "no authentication". it is "no
    /// authenticator configured", and [`serve`](Self::serve) refuses to start
    /// on any address but loopback under it, because this api launches runs
    /// and cancels them and a warning about that is a warning somebody
    /// scrolls past. loopback is untouched: one process talking to itself
    /// configures nothing and behaves exactly as it always has.
    ///
    /// [`Auth::None`] is the deliberate way out, for a deployment fronted by
    /// something that authenticates for it. see [`the module`](crate::auth)
    /// for the two authenticators and the roles they hand out.
    pub fn auth(mut self, auth: Auth) -> Self {
        self.auth = Some(auth);
        self
    }

    /// how many bytes of [captured output](crate::Op::isolated) one *attempt*
    /// of one op may store before capture stops for it; default 1 MiB.
    ///
    /// a cap rather than a preference: an op in a `println!` loop would
    /// otherwise fill the disk the run log lives on, and a run log that ran
    /// out of room is a run log that records nothing at all. past the cap one
    /// line says what was dropped and why, and the attempt goes on running —
    /// capture stopping is not the op failing. per attempt because a retry
    /// starts from a full budget, the failed attempt's output being the part
    /// usually worth reading.
    ///
    /// the limit covers every capture in this process, the `capture` feature's
    /// [layer][cap] included — the host composes that with a store handle of
    /// its own, and a cap that only reached the writers hestan happened to
    /// build would be a cap that quietly does not hold.
    ///
    #[cfg_attr(feature = "capture", doc = "[cap]: crate::capture_layer")]
    #[cfg_attr(not(feature = "capture"), doc = "[cap]: crate")]
    pub fn log_limit(mut self, bytes: u64) -> Self {
        self.log_bytes = bytes;
        self
    }

    /// how many lines of captured output one attempt may store; default
    /// 10,000. the other half of [`log_limit`](Self::log_limit) — a million
    /// empty lines are under any byte cap worth setting and are still a
    /// million rows.
    pub fn log_lines(mut self, lines: u64) -> Self {
        self.log_line_cap = lines;
        self
    }

    /// how much history to keep, for every job that does not
    /// [say otherwise](crate::JobBuilder::retention). the default keeps
    /// everything.
    ///
    /// ```no_run
    /// # use hestan::{Hestan, Retention};
    /// Hestan::new().retention(Retention::days(30).keep_last(20).failed_days(90));
    /// ```
    ///
    /// a sweep runs at startup and every
    /// [`retention_interval`](Self::retention_interval) after it, in whichever
    /// process [decides](crate::Role) — a worker must never prune, since the
    /// history it would be deleting belongs to runs it does not own.
    pub fn retention(mut self, retention: Retention) -> Self {
        self.retention = retention;
        self
    }

    /// delete terminal runs older than `days` days with their op runs, events
    /// and captured output, plus [sensor run keys](crate::RunRequest::key)
    /// claimed before the same cutoff. active runs and op state survive; the
    /// default keeps everything, run keys included.
    ///
    /// [`retention(Retention::days(days))`](Self::retention) said the short
    /// way, and the short way is still right for the common case.
    pub fn retention_days(self, days: u32) -> Self {
        self.retention(Retention::days(days))
    }

    /// write every run's terminal event to the database inside the same
    /// transaction as the run's terminal row, and deliver it from a loop that
    /// retries and gives up loudly.
    ///
    /// **off by default, and meant to stay off for most people.** an embedder
    /// whose hook bumps a counter or writes a line wants a callback, not a
    /// table and a delivery loop; the ordinary dispatch is a `spawn_blocking`
    /// call and costs nothing. this is for the hook whose job is to tell a
    /// human, where losing one is the failure mode that matters — without it,
    /// a process that dies between a run failing and the hook running has
    /// nothing anywhere recording that an alert was owed.
    ///
    /// **delivery is at-least-once.** a crash between a hook returning and the
    /// row being marked delivered re-delivers on the next pass, so a hook must
    /// tolerate seeing the same event twice — key on `run_id` if that matters.
    /// exactly-once needs the receiver's cooperation and hestan will not
    /// pretend otherwise.
    ///
    /// a hook that panics is a failed delivery, retried on the same backoff an
    /// op's retries use and given up on after eight attempts, leaving the row
    /// visible as `failed` with its last error — on `GET /api/notifications`
    /// and on the runs page. the loop belongs to the process that
    /// [decides](crate::Role), so register the hooks there.
    ///
    /// covers run events. op hooks and [`on_late`](Self::on_late) stay
    /// in-process: they fire per attempt and per poll, and a table of them is
    /// a different bargain than the one this makes.
    pub fn durable_notifications(mut self) -> Self {
        self.durable = true;
        self
    }

    /// how often the retention sweep comes round; default one hour.
    ///
    /// it also runs at startup, which is all it ever used to do — and a server
    /// that stays up for three months is exactly the deployment a retention
    /// policy is for.
    pub fn retention_interval(mut self, every: Duration) -> Self {
        self.retention_every = every;
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
    ///
    /// [`on_run_finished`](Self::on_run_finished) is this without the filter,
    /// and is the one to reach for in new code: it fires for every terminal
    /// status and says which on the event. this is the same hook with
    /// `status == Failed` applied for you, and is not going anywhere.
    pub fn on_failure(mut self, hook: impl Fn(RunFailure) + Send + Sync + 'static) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    /// call `hook` whenever a run reaches a terminal status — succeeded,
    /// failed or canceled alike, with [`status`](RunEvent::status) saying
    /// which. callable multiple times, and dispatched exactly like
    /// [`on_failure`](Self::on_failure), so a hook may block.
    ///
    /// ```no_run
    /// # use hestan::{Hestan, RunEvent};
    /// Hestan::new().on_run_finished(|e: RunEvent| {
    ///     println!("{} {} in {:?}", e.job, e.status.as_str(), e.duration)
    /// });
    /// ```
    ///
    /// a run the boot sweep marked failed does not fire: nothing executed it,
    /// and a restart after a crash should not replay a morning of old failures
    /// into an alert channel. [`JobBuilder::on_run_finished`](crate::JobBuilder::on_run_finished)
    /// is the same hook scoped to one job.
    pub fn on_run_finished(mut self, hook: impl Fn(RunEvent) + Send + Sync + 'static) -> Self {
        self.run_hooks.push(Arc::new(hook));
        self
    }

    /// call `hook` whenever one **attempt** of one op ends, whatever the run
    /// it belongs to goes on to do. callable multiple times.
    ///
    /// per attempt rather than per op, because an op that failed twice and
    /// worked on the third try is three facts and only the hook knows which of
    /// them it wanted — [`attempt`](OpEvent::attempt) and
    /// [`status`](OpEvent::status) are how it says so. an op skipped by its
    /// [trigger rule](crate::When) produces nothing: there was no attempt.
    ///
    /// [`JobBuilder::on_op_finished`](crate::JobBuilder::on_op_finished) is the
    /// same hook scoped to one job.
    pub fn on_op_finished(mut self, hook: impl Fn(OpEvent) + Send + Sync + 'static) -> Self {
        self.op_hooks.push(Arc::new(hook));
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

    /// run one job to completion and return it, with no ui and no loops. the
    /// [role](Self::role) does not apply: a one-shot executes its own run.
    pub async fn run_once(self, job: &str, params: Value) -> Result<Run, Error> {
        // the op-subprocess guard, first: see `up`
        #[cfg(unix)]
        if let Some(req) = crate::isolate::requested() {
            self.run_op_subprocess(req).await
        }
        let built = self.role(Role::All).build().await?;
        let run = built.runner.run(job, params, Trigger::Manual).await;
        delivered(&built.runner).await;
        run
    }

    /// materialize `name` headless, like [`run_once`](Self::run_once): one run of
    /// its stale ancestors plus the target, which always rebuilds. check
    /// `GET /api/assets` first if you only want to build when stale.
    pub async fn build_asset(self, name: &str) -> Result<Run, Error> {
        // the op-subprocess guard, first: see `up`
        #[cfg(unix)]
        if let Some(req) = crate::isolate::requested() {
            self.run_op_subprocess(req).await
        }
        let built = self.role(Role::All).build().await?;
        let mats = mats_map(built.runner.store())?;
        let plan = plan_target(&built.registry, &mats, name)?;
        let run = built
            .runner
            .run_subset(
                crate::asset::ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                serde_json::json!({}),
                Trigger::Build,
                asset_tag(name),
            )
            .await;
        delivered(&built.runner).await;
        run
    }

    /// run the ui and whatever loops this process's [role](Self::role) owns.
    /// the default role is [`Role::All`] — one process doing everything, which
    /// is right until it is not.
    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        let addr = addr.into();
        self.up(Some(addr)).await
    }

    /// [`serve`](Self::serve) as a **queue worker**: it claims queued runs and
    /// executes them, and fires no schedule, evaluates no sensor, checks no
    /// freshness policy and chunks no backfill. exactly one process in a
    /// deployment should own those, and `Hestan::serve` under
    /// [`Role::Scheduler`] is that process.
    ///
    /// `addr` is optional because a worker has nothing to show: with `None` it
    /// binds no socket at all. give it one and you get the same ui, which is
    /// worth having for `/api/health` — that is where a worker says which runs
    /// it is holding.
    ///
    /// this is [`role(Role::Worker)`](Self::role) with the addresses made
    /// optional, exactly as [`schedule`](Self::schedule) is
    /// [`add_schedule`](Self::add_schedule) with the defaults filled in.
    ///
    /// **not** [`Op::isolated`](crate::Op::isolated), which also spawns
    /// processes: that spawns one op subprocess which runs a single op and
    /// exits. this is a long-lived process that claims whole runs — and it
    /// spawns op subprocesses itself, like any other hestan process.
    pub async fn work(self, addr: Option<SocketAddr>) -> Result<(), Error> {
        self.role(Role::Worker).up(addr).await
    }

    async fn up(self, addr: Option<SocketAddr>) -> Result<(), Error> {
        // before the address, before the store, before anything: this process
        // may be an op subprocess of a hestan already running against this
        // database, and every line of boot behaviour below assumes otherwise —
        // it would sweep, sync schedules, bind a listener and start claiming
        // runs, when it is here to run one op. `run_op_subprocess` never
        // returns.
        #[cfg(unix)]
        if let Some(req) = crate::isolate::requested() {
            self.run_op_subprocess(req).await
        }
        let role = self.role;
        let auth = self.auth.clone();
        // the socket first, before the store is opened and before a loop
        // exists to abort: a deployment that is going to be refused should be
        // refused having done nothing. a bind failure must not leave detached
        // tasks firing jobs into a server that never started either, and this
        // is that guarantee made earlier rather than given up
        let listener = match addr {
            Some(addr) => Some(tokio::net::TcpListener::bind(addr).await?),
            None => None,
        };
        // and the guard on the address the listener is *holding*, not the one
        // it was handed. today those are the same and this is the check that
        // decides whether the control plane is reachable by strangers, so it
        // goes on the one requests will arrive on
        if let Some(listener) = &listener
            && let Some(said) = auth::guard(listener.local_addr()?, auth.as_ref())?
        {
            tracing::warn!("{said}");
        }
        let built = self.build().await?;
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
        let mut loops = Vec::new();
        if role.decides() {
            loops.push(tokio::spawn(schedule::run_scheduler(
                built.entries,
                built.runner.clone(),
            )));
            loops.push(tokio::spawn(run_sensors(
                built.sensor_entries,
                built.runner.clone(),
                built.registry.clone(),
            )));
            // the chunker: it launches each backfill's next range as the last
            // one finishes, so a long backfill never fires every partition at
            // once
            loops.push(tokio::spawn(crate::backfill::run_backfills(
                built.runner.clone(),
                built.registry.clone(),
            )));
            loops.push(tokio::spawn(freshness::run_checker(
                built.runner.clone(),
                built.registry.clone(),
                Arc::new(built.late_hooks),
            )));
            // the sweeper: what a policy set at boot means three months later
            loops.push(tokio::spawn(retention::run_sweeper(
                built.runner.clone(),
                built.retention,
                built.retention_every,
            )));
            // and the deliverer, if anything is writing rows for it. one
            // process delivers, for the same reason one process decides: two
            // of them would send every alert twice
            if built.runner.durable() {
                loops.push(tokio::spawn(hooks::run_delivery(built.runner.clone())));
            }
        }
        if role.executes() {
            // the dispatcher: the queue's own loop. every launch pokes it and
            // every run that finishes pokes it, so what this covers is the two
            // things no local poke can — a run another process enqueued, and a
            // limit that changed under a queue nobody is touching
            loops.push(tokio::spawn(crate::executor::run_dispatcher(
                built.runner.clone(),
            )));
        }
        // the lease loop runs whatever the role is: a process holding nothing
        // still notices a claimer that went away, and a deployment where only
        // the dead process could have noticed would never notice
        loops.push(tokio::spawn(crate::executor::run_leases(
            built.runner.clone(),
        )));
        let instance = built.runner.instance().to_string();
        let state = AppState {
            jobs: Arc::new(built.runner.jobs().clone()),
            runner: built.runner,
            assets: built.registry,
            sensors: Arc::new(sensor_infos),
            auth,
        };
        let served = match listener {
            Some(listener) => {
                let addr = addr.expect("a listener came from an address");
                tracing::info!("hestan {role} {instance} on http://{addr}");
                axum::serve(listener, router(state)).await
            }
            // a worker with no socket: the loops are the process, and there is
            // nothing to serve them to
            None => {
                tracing::info!("hestan {role} {instance}, no listener");
                std::future::pending().await
            }
        };
        for handle in loops {
            handle.abort();
        }
        served?;
        Ok(())
    }

    /// the jobs this process runs, with http sources and assets lowered in.
    ///
    /// the server path and the [op-subprocess](Self::run_op_subprocess) path
    /// both go through here, which is what makes "the subprocess rebuilds the
    /// same registry" true rather than hopeful: there is one lowering, and both
    /// processes run it.
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

    /// the op-subprocess path: build the same registry the server path would,
    /// run one op of one run, and exit — 0 for a success, 1 for anything else.
    ///
    /// what is *not* here is the point of it. no `fail_interrupted`, no
    /// schedule sync, no tick prune, no retention sweep, no scheduler, sensor,
    /// freshness or backfill loop, no dispatcher and no listener: this process
    /// claims nothing and owns nothing. it opens the store, reads what its op
    /// was handed, runs it, and writes the result back.
    ///
    /// not to be confused with [`work`](Self::work), the queue worker, which
    /// is long-lived and claims whole runs.
    #[cfg(unix)]
    pub(crate) async fn run_op_subprocess(mut self, req: crate::isolate::Request) -> ! {
        let code = match self.ran_op_subprocess(req).await {
            Ok(crate::isolate::Worked::Success) => 0,
            Ok(crate::isolate::Worked::Failed) => 1,
            // the op ran; nothing recorded it. said here for the same reason
            // the arm below prints rather than traces — the parent captures
            // this stream as the attempt's output, and its own read of the op
            // row is about to find nothing there
            Ok(crate::isolate::Worked::Unrecorded) => {
                eprintln!("hestan op subprocess: the store would not record what this op did");
                1
            }
            // printed rather than traced: a worker whose store or registry is
            // wrong cannot record anything anywhere, and its stderr is piped
            // by its parent, which stores it as this attempt's output
            Err(e) => {
                eprintln!("hestan op subprocess: {e}");
                1
            }
        };
        std::process::exit(code)
    }

    #[cfg(unix)]
    async fn ran_op_subprocess(
        &mut self,
        req: crate::isolate::Request,
    ) -> Result<crate::isolate::Worked, Error> {
        let (jobs, _) = self.lower()?;
        let resources = resource::build(std::mem::take(&mut self.resources)).await?;
        let store = Store::at(&self.db_path)?;
        logs::set_caps(Some(self.log_bytes), Some(self.log_line_cap));
        let io = Io::new(self.io_default.take(), std::mem::take(&mut self.io_named));
        crate::isolate::run_one_op(&req, &jobs, &store, &io, &resources).await
    }

    /// the store this app is configured with, opened and migrated and
    /// otherwise left entirely alone.
    ///
    /// that is the whole difference from [`build`](Self::build), and the reason
    /// there are two ways in. a process starting up is entitled to tidy the
    /// database it is about to own — fail what a dead process left running,
    /// sync the schedules, sweep retention. a command line asking what ran last
    /// night is not, and a cron line running one every minute would be doing
    /// all of it sixty times an hour on behalf of a process that exits
    /// immediately.
    #[cfg(feature = "cli")]
    pub(crate) fn open(&self) -> Result<Store, Error> {
        Store::at(&self.db_path)
    }

    /// the registry beside the store, lowered and validated, and still with
    /// none of the boot behaviour — see [`open`](Self::open) for what is
    /// deliberately not happening.
    #[cfg(feature = "cli")]
    pub(crate) fn inspect(mut self) -> Result<Inspected, Error> {
        let (jobs, registry) = self.lower()?;
        let store = Store::at(&self.db_path)?;
        Ok(Inspected {
            jobs,
            registry,
            store,
            pools: self.pools,
            limits: self.limits,
            retention: self.retention,
            role: self.role,
            auth: self.auth,
            db: self.db_path,
        })
    }

    pub(crate) async fn build(mut self) -> Result<Built, Error> {
        let (jobs, registry) = self.lower()?;
        let schedules = std::mem::take(&mut self.schedules);
        let mut entries = Vec::new();
        let mut pairs: HashSet<(&str, &str)> = HashSet::new();
        for s in &schedules {
            let (job, expr) = (&s.job, &s.expr);
            let Some(defined) = jobs.iter().find(|j| j.name() == job) else {
                return Err(Error::UnknownJob(job.clone()));
            };
            // the store keys a schedule on the pair, so a second declaration of
            // one is not a second schedule: it is that row with whichever
            // timezone and params came last on it, and both entries firing
            if !pairs.insert((job.as_str(), expr.as_str())) {
                return Err(Error::Graph(format!(
                    "schedule {expr} on job {job} is declared twice"
                )));
            }
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

        let store = Store::at(&self.db_path)?;
        logs::set_caps(Some(self.log_bytes), Some(self.log_line_cap));
        store.fail_interrupted()?;
        store.sync_schedules(&schedules)?;
        // seeded, not synced: the launchpad's presets share the table, so
        // there is nothing here to sweep away
        for (job, name, params) in &self.presets {
            store.put_preset(job, name, params)?;
        }
        let sensor_names: Vec<String> = sensor_entries.iter().map(|e| e.name.clone()).collect();
        store.sync_sensors(&sensor_names)?;
        let trimmed = store.prune_materializations(self.asset_history)?;
        let trimmed = trimmed + store.prune_asset_checks(self.asset_history)?;
        if trimmed > 0 {
            tracing::info!("trimmed {trimmed} asset history rows past the cap");
        }
        let io = Io::new(self.io_default, self.io_named);
        let mut runner =
            Runner::with_resources(jobs, store, self.hooks, self.pools, resources, io)?
                .with_hooks(self.run_hooks, self.op_hooks)
                .with_run_tags(self.run_tags)
                .with_limits(self.limits, self.priority)
                .with_reclaim(self.reclaim)
                .with_role(self.role, self.slots);
        if self.durable {
            runner = runner.with_durable_notifications();
        }
        // before anything new launches, and before the loop that takes it from
        // here: a process that runs for an hour and exits should still tidy up
        retention::sweep(&runner, &self.retention, chrono::Utc::now());
        Ok(Built {
            runner,
            entries,
            registry,
            sensor_entries,
            late_hooks: self.late_hooks,
            retention: self.retention,
            retention_every: self.retention_every,
        })
    }
}

/// deliver what a headless one-shot just wrote down, since nothing else will:
/// there is no loop in this process and it is about to exit. a no-op unless
/// [`durable_notifications`](Hestan::durable_notifications) is on.
async fn delivered(runner: &Runner) {
    if runner.durable() {
        hooks::deliver_once(runner, chrono::Utc::now()).await;
    }
}

/// what an app is, to a reader: the jobs it defines, the assets it declares,
/// the store it would open, and the limits it would apply. no runner, because
/// nothing here executes anything.
#[cfg(feature = "cli")]
pub(crate) struct Inspected {
    pub(crate) jobs: Vec<Job>,
    pub(crate) registry: Arc<AssetRegistry>,
    pub(crate) store: Store,
    pub(crate) pools: Vec<(String, usize)>,
    pub(crate) limits: Limits,
    pub(crate) retention: Retention,
    pub(crate) role: Role,
    /// what would check who is asking, if this served. `None` is nothing
    /// configured — which is what `doctor` reports, since it is the difference
    /// between a deployment that can be moved off loopback and one that
    /// cannot.
    pub(crate) auth: Option<Auth>,
    /// the path or url the store was opened at.
    pub(crate) db: String,
}

pub(crate) struct Built {
    pub(crate) runner: Runner,
    entries: Vec<ScheduleEntry>,
    pub(crate) registry: Arc<AssetRegistry>,
    sensor_entries: Vec<SensorEntry>,
    late_hooks: Vec<LateHook>,
    retention: Retention,
    retention_every: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::job::Job;
    use crate::model::RunStatus;
    use crate::op::Op;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Window {
        days: u32,
    }

    /// the worked postgres example on [`Hestan::resource`], as a reader of the
    /// rendered docs sees it: the fenced block around the one query in it,
    /// with the doctest's hidden lines dropped.
    fn worked_example() -> String {
        let src = include_str!("app.rs");
        // the query is in the example and nowhere else, so the fence it sits
        // between is the example's
        let query = src
            .find("select count(*) from orders")
            .expect("the example");
        let fence = "    /// ```\n";
        let start = src[..query].rfind(fence).expect("an opening fence") + fence.len();
        let end = query + src[query..].find(fence).expect("a closing fence");
        src[start..end]
            .lines()
            .map(|line| {
                line.trim_start_matches("    ///")
                    .strip_prefix(' ')
                    .unwrap_or("")
            })
            .filter(|line| !line.trim_start().starts_with("# "))
            .collect::<Vec<&str>>()
            .join("\n")
    }

    // the page shows the example and the doctest runs it, so they have to be
    // the same text. a docs page carrying code nothing compiles is the one
    // that tells you to call a method that was renamed two releases ago —
    // which no test here would otherwise catch, since markdown compiles fine
    #[test]
    fn the_connecting_page_shows_exactly_the_example_the_doctest_runs() {
        let example = worked_example();
        assert!(
            example.contains("tokio_postgres::connect") && example.lines().count() > 20,
            "the example was not scraped: {example}"
        );
        // and nothing hidden leaked into what the page is held to
        assert!(!example.contains("HESTAN_TEST_PG"), "{example}");
        assert!(
            include_str!("../docs/connecting.md").contains(&example),
            "docs/connecting.md no longer holds this example:\n{example}"
        );
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

    // through build, which is the path serve takes, rather than only through
    // the runner underneath it
    #[tokio::test]
    async fn two_jobs_of_one_name_are_refused_at_build() {
        let err = Hestan::new()
            .job(windowed("report"))
            .job(windowed("report"))
            .db(":memory:")
            .run_once("report", json!({"days": 7}))
            .await
            .err()
            .unwrap();
        assert!(
            matches!(err, Error::DuplicateJob(ref name) if name == "report"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn one_schedule_declared_twice_is_refused_at_build() {
        let good = json!({"days": 7});
        let err = Hestan::new()
            .job(windowed("report"))
            .schedule_with("report", "0 9 * * *", good.clone())
            .schedule_tz_with("report", "0 9 * * *", "Europe/Lisbon", good.clone())
            .db(":memory:")
            .run_once("report", good.clone())
            .await
            .err()
            .unwrap();
        assert!(
            err.to_string()
                .contains("schedule 0 9 * * * on job report is declared twice"),
            "{err}"
        );

        // two expressions on one job are a different thing entirely, and stay
        // one job with two schedules
        let run = Hestan::new()
            .job(windowed("report"))
            .schedule_with("report", "0 9 * * *", good.clone())
            .schedule_with("report", "0 21 * * *", good.clone())
            .db(":memory:")
            .run_once("report", good)
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
            .unwrap()
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

    // the whole point of the op-subprocess guard: a subprocess must not run one
    // line of boot behaviour. it would sweep, sync schedules, and start
    // claiming runs off the queue, when it is here to run one op of one run.
    #[cfg(unix)]
    #[tokio::test]
    async fn an_op_subprocess_does_not_touch_the_runs_it_finds() {
        use crate::model::{OpStatus, RunTags};
        use crate::op::Op;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        let job = Job::builder("guarded")
            .op(Op::new("quick", |_| async { Ok(json!({"ran": true})) }).isolated())
            .build()
            .unwrap();

        // a queued run for the op subprocess, and one belonging to nobody it
        // knows — which is exactly the shape of a parent's in-flight run
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
            replay_of: None,
            scheduled_for: None,
            tags: RunTags::new(),
            priority: 0,
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
            actor: None,
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
        let worked = app.ran_op_subprocess(req).await.unwrap();
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
        // including the run it did the work for: that run's own status is its
        // claimer's business, not an op subprocess's
        assert_eq!(
            store.run("mine").unwrap().unwrap().status,
            RunStatus::Queued
        );
        for id in ["mine", "someone-elses"] {
            let events = store.events(id, 0).unwrap();
            assert!(
                !events.iter().any(|e| e.message.contains("interrupted")),
                "an op subprocess announced an interruption on {id}: {:?}",
                events.iter().map(|e| &e.message).collect::<Vec<_>>()
            );
        }
    }

    /// a port nothing is on, on `host`. the listener is dropped before the
    /// address is handed back — the same small race every test that needs a
    /// port it can name has, and the only way to know which port `serve`
    /// bound before it has bound it.
    fn free_port(host: &str) -> SocketAddr {
        let listener = std::net::TcpListener::bind((host, 0)).unwrap();
        listener.local_addr().unwrap()
    }

    /// serve in a task and read `/api/health` back, by hand: a default build
    /// has no http client in it, and what is being asserted here is only who
    /// got an answer.
    async fn health(app: Hestan, addr: SocketAddr, token: Option<&str>) -> String {
        let serving = tokio::spawn(app.serve(addr));
        let request = match token {
            Some(token) => format!(
                "GET /api/health HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\
                 Connection: close\r\n\r\n"
            ),
            None => {
                "GET /api/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".into()
            }
        };
        let mut answered = None;
        for _ in 0..100 {
            // whatever it bound to, the request comes from this machine
            let Ok(mut socket) = tokio::net::TcpStream::connect(("127.0.0.1", addr.port())).await
            else {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            socket.write_all(request.as_bytes()).await.unwrap();
            let mut said = String::new();
            socket.read_to_string(&mut said).await.unwrap();
            answered = Some(said);
            break;
        }
        serving.abort();
        answered.expect("the server answered")
    }

    // the refusal, and it is the point of the whole arrangement: a deployment
    // reachable by strangers with nothing checking who they are does not start
    #[tokio::test]
    async fn serve_refuses_an_address_anyone_can_reach_with_nothing_guarding_it() {
        for host in ["0.0.0.0", "::"] {
            let addr = free_port(host);
            let err = Hestan::new()
                .db(":memory:")
                .serve(addr)
                .await
                .expect_err("an unguarded address served");
            assert!(matches!(err, Error::Unguarded(_)), "{err}");
            let said = err.to_string();
            // the address it refused, and what to do instead
            assert!(said.contains(&addr.to_string()), "{said}");
            assert!(said.contains("Hestan::auth"), "{said}");
            // and nothing is holding the port: it refused rather than served
            assert!(
                std::net::TcpListener::bind(addr).is_ok(),
                "a refused serve left a listener on {addr}"
            );
        }
    }

    // and the case that must not have changed: one process, one machine,
    // nothing configured
    #[tokio::test]
    async fn loopback_serves_with_nothing_configured_at_all() {
        let said = health(Hestan::new().db(":memory:"), free_port("127.0.0.1"), None).await;
        assert!(said.contains("200 OK"), "{said}");
        assert!(said.contains("\"ok\":true"), "{said}");
    }

    #[tokio::test]
    async fn an_authenticator_is_what_makes_a_reachable_address_servable() {
        let served = |token| {
            health(
                Hestan::new().db(":memory:").auth(Auth::bearer("s3cret")),
                free_port("0.0.0.0"),
                token,
            )
        };
        let said = served(Some("s3cret")).await;
        assert!(said.contains("200 OK"), "{said}");
        // and the same address answers a stranger with a 401 rather than the
        // deployment
        let said = served(None).await;
        assert!(said.contains("401 Unauthorized"), "{said}");

        // the opt-out serves everyone, having said what it is leaning on — see
        // `auth::guard`, where that sentence is asserted
        let said = health(
            Hestan::new().db(":memory:").auth(Auth::None),
            free_port("0.0.0.0"),
            None,
        )
        .await;
        assert!(said.contains("200 OK"), "{said}");
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
                .set_schedule_paused("report", "0 9 * * *", true, None)
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
