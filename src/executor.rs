use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use serde_json::{Value, json};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::{Id, JoinSet};
use tracing::Instrument;

use crate::error::Error;
use crate::graph;
use crate::hooks::{FailureHook, Hooks, OpEvent, OpHook, RunEvent, RunHook, fire_hooks};
use crate::io::{Io, IoKey, IoManager};
use crate::job::Job;
use crate::model::{
    EventKind, EventLevel, OpRun, OpStatus, Reclaim, Role, Run, RunStatus, RunTags, Trigger, When,
    new_run_id,
};
use crate::op::{self, Cancel, MetaBuf, Op, OpCtx};
use crate::resource::{self, Resources};
use crate::store::{Built, RunKey, Store, note};

/// how far back a resume follows `resumed_from` links. resuming a resume is
/// normal; a chain this long is a bug, and the walk says so instead of looping.
const MAX_RESUME_CHAIN: usize = 256;

/// how long a canceled run waits for its aborted tasks to actually join before
/// recording them as never observed to stop. aborting an async op lands at its
/// next await point, which is usually immediate; blocking work cannot be
/// aborted at all, and waiting forever for it would hang the run.
///
/// an [isolated op](Op::isolated) gets the same window between its SIGTERM and
/// its SIGKILL, so "a few seconds to wind down" means one thing across hestan.
pub(crate) const CANCEL_GRACE: Duration = Duration::from_secs(3);

/// how long a claim is believed for, and how often its holder says so.
///
/// a claimer that stops renewing loses its runs one lease after it went quiet,
/// which is the whole of how a process that died is noticed. the gap between
/// the two is deliberate: four heartbeats have to be missed before anything is
/// taken, so a slow store or a paused process is not mistaken for a dead one.
pub(crate) const LEASE: Duration = Duration::from_secs(60);
pub(crate) const HEARTBEAT: Duration = Duration::from_secs(15);

/// how often the dispatcher looks at the queue on its own.
///
/// it is also poked whenever a run is enqueued and whenever one finishes, so
/// this is the backstop rather than the mechanism: what it covers is a run
/// another process enqueued, and a limit that changed under a queue nobody is
/// touching.
pub(crate) const DISPATCH_POLL: Duration = Duration::from_millis(500);

/// how deep into the queue one dispatch pass looks for something startable.
/// past this the head of the queue really is the queue.
pub(crate) const QUEUE_SCAN: u32 = 500;

/// what the dispatcher will not start past.
///
/// every limit here counts runs that are **executing** — claimed and not yet
/// finished. a queued run nobody has claimed costs nothing and counts as
/// nothing, which is the difference between this and
/// [`Overlap`](crate::Overlap): a limit is about machines, and overlap is about
/// whether a job should have two of itself outstanding at all.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Limits {
    pub(crate) global: Option<usize>,
    pub(crate) per_job: BTreeMap<String, usize>,
    pub(crate) tags: BTreeMap<(String, String), usize>,
}

impl Limits {
    /// no limits at all, which is also the default: build one up with
    /// [`global`](Limits::global), [`job`](Limits::job) and
    /// [`tag`](Limits::tag).
    pub fn new() -> Limits {
        Limits::default()
    }

    /// at most `n` runs executing anywhere in this deployment. below 1 means 1:
    /// a limit of zero is a queue that never drains, which is never what
    /// anybody meant.
    pub fn global(mut self, n: usize) -> Limits {
        self.global = Some(n.max(1));
        self
    }

    /// at most `n` runs of `job` executing at once.
    pub fn job(mut self, job: impl Into<String>, n: usize) -> Limits {
        self.per_job.insert(job.into(), n.max(1));
        self
    }

    /// at most `n` runs carrying the [tag](RunTags) `key: value` executing at
    /// once — `env: prod` at 2, whatever the jobs are.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>, n: usize) -> Limits {
        self.tags.insert((key.into(), value.into()), n.max(1));
        self
    }

    /// the global cap, if there is one.
    pub fn global_limit(&self) -> Option<usize> {
        self.global
    }

    /// every per-job cap, by job.
    pub fn jobs(&self) -> Vec<(&str, usize)> {
        self.per_job.iter().map(|(j, n)| (j.as_str(), *n)).collect()
    }

    /// every tag-scoped cap, as `(key, value, limit)`.
    pub fn tag_limits(&self) -> Vec<(&str, &str, usize)> {
        self.tags
            .iter()
            .map(|((k, v), n)| (k.as_str(), v.as_str(), *n))
            .collect()
    }

    /// whether anything here caps anything.
    ///
    /// which is what decides whether two dispatchers have to agree about
    /// capacity before either spends it. no limits is the default and the
    /// common case, and it is the case where they need not meet at all.
    pub(crate) fn binding(&self) -> bool {
        self.global.is_some() || !self.per_job.is_empty() || !self.tags.is_empty()
    }
}

/// why a queued run is not starting right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Blocked {
    /// the deployment-wide cap is full.
    Global(usize),
    /// this job's own cap is full.
    Job {
        /// the job that is at its limit — this run's job.
        job: String,
        /// what that limit is.
        limit: usize,
    },
    /// a cap on one tag value is full, and the run carries that tag.
    Tag {
        /// the tag key the limit is on.
        key: String,
        /// the value of it this run carries.
        value: String,
        /// what that limit is.
        limit: usize,
    },
    /// nothing here can run it: this process does not define the job. a run
    /// left over from a job that was deleted, or — once workers are split by
    /// what they can build — one waiting for a process that knows how.
    Undefined(String),
}

impl Blocked {
    /// which limit this is, for anything grouping by it.
    pub fn scope(&self) -> &'static str {
        match self {
            Blocked::Global(_) => "global",
            Blocked::Job { .. } => "job",
            Blocked::Tag { .. } => "tag",
            Blocked::Undefined(_) => "undefined",
        }
    }

    /// the sentence a queue view shows.
    pub fn reason(&self) -> String {
        match self {
            Blocked::Global(n) => format!("{n} runs are already executing, which is the limit"),
            Blocked::Job { job, limit } => {
                format!("{limit} runs of {job} are already executing, which is its limit")
            }
            Blocked::Tag { key, value, limit } => {
                format!(
                    "{limit} runs tagged {key}:{value} are already executing, which is the limit"
                )
            }
            Blocked::Undefined(job) => format!("no job named {job} is defined in this process"),
        }
    }
}

/// what is executing right now, counted only where a limit asks.
///
/// tag counts are kept for the pairs [`Limits`] names and no others: a run
/// tagged with a backfill id would otherwise put a row in this map per
/// backfill, forever, to answer a question nobody asked.
pub(crate) struct InFlight {
    total: usize,
    per_job: HashMap<String, usize>,
    tags: HashMap<(String, String), usize>,
}

impl InFlight {
    pub(crate) fn new() -> InFlight {
        InFlight {
            total: 0,
            per_job: HashMap::new(),
            tags: HashMap::new(),
        }
    }

    /// count one executing run against every limit it touches.
    pub(crate) fn take(&mut self, limits: &Limits, job: &str, tags: &RunTags) {
        self.total += 1;
        *self.per_job.entry(job.to_string()).or_default() += 1;
        for (key, value) in tags {
            let pair = (key.clone(), value.clone());
            if limits.tags.contains_key(&pair) {
                *self.tags.entry(pair).or_default() += 1;
            }
        }
    }

    /// the first limit starting this run would break, if any. the order is
    /// broadest first, so a run held back by two limits names the one an
    /// operator would raise.
    pub(crate) fn blocker(&self, limits: &Limits, job: &str, tags: &RunTags) -> Option<Blocked> {
        if let Some(n) = limits.global
            && self.total >= n
        {
            return Some(Blocked::Global(n));
        }
        if let Some(&n) = limits.per_job.get(job)
            && self.per_job.get(job).copied().unwrap_or(0) >= n
        {
            return Some(Blocked::Job {
                job: job.to_string(),
                limit: n,
            });
        }
        for (key, value) in tags {
            let pair = (key.clone(), value.clone());
            if let Some(&n) = limits.tags.get(&pair)
                && self.tags.get(&pair).copied().unwrap_or(0) >= n
            {
                return Some(Blocked::Tag {
                    key: key.clone(),
                    value: value.clone(),
                    limit: n,
                });
            }
        }
        None
    }
}

/// one queued run as the queue view reports it.
pub struct Queued {
    /// the run row itself, so a queue view needs no second read to show what
    /// is waiting.
    pub run: Run,
    /// 1 for the head of the queue.
    pub position: usize,
    /// why it is not executing; `None` on one the next dispatch pass starts.
    pub blocked: Option<Blocked>,
}

/// a named concurrency limit shared by every job in the process, declared with
/// `Hestan::pool` and taken by [`Op::pool`].
pub(crate) struct Pool {
    pub(crate) limit: usize,
    sem: Arc<Semaphore>,
}

pub(crate) type Pools = Arc<HashMap<String, Pool>>;

/// what [`Runner::cancel`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// the signal was sent; the run will finish canceled shortly.
    Requested,
    /// the run exists but is already terminal (or belongs to a dead process).
    AlreadyFinished,
    /// no run with that id.
    Unknown,
}

/// what a resume would do, from [`Runner::resume_plan`]. both lists are in the
/// job's topological order, and together they cover every op the job has
/// except ones that neither re-run nor have an output worth reusing.
#[derive(Debug, Clone)]
pub struct ResumePlan {
    /// the job the resumed run belongs to.
    pub job: String,
    /// the ops the resumed run executes.
    pub rerun: Vec<String>,
    /// the ops it seeds from a recorded output instead of executing.
    pub reuse: Vec<String>,
    params: Value,
    // `reuse`'s outputs plus the job's external names, which a launch seeds null
    seeded: HashMap<String, Value>,
    resumed_from: String,
}

/// how one attempt ended, before the retry policy has looked at it.
pub(crate) enum Ended {
    /// an in-process body returned this.
    Value(Value),
    /// an [isolated](Op::isolated) child recorded its own success; this is the
    /// handle it stored the output under.
    Handle(Value),
    /// the attempt failed, with the message the run records.
    Failed(String),
    /// an isolated child was stopped for real: signalled, then killed, and
    /// watched to die.
    Killed(String),
}

/// how one op invocation ended, as the run has to record it.
enum Outcome {
    /// a value the run persists and writes the terminal row for.
    Produced {
        output: Value,
        state: Option<Value>,
        meta: Option<Value>,
        /// what an asset op built, for the transaction that records this op
        /// finishing. empty for every other op.
        built: Vec<Built>,
    },
    /// an isolated op whose child persisted its own output and wrote its own
    /// terminal row: the run takes the handle and writes nothing.
    Recorded(Value),
    /// terminally failed, after any retries.
    Failed(String),
    /// an isolated op the run's cancellation killed. unlike the cooperative
    /// path this one gets a real finish time, because for once hestan watched
    /// the work stop.
    Killed(String),
    /// the store would not take the row that says this op started, so nothing
    /// this process could write about it afterwards would mean anything. the
    /// run stops here rather than recording an outcome over a row it never
    /// established.
    Unrecorded,
}

type OpOutcome = (String, Outcome);

/// one instance of a mapped op, keyed in the run by its `{op}[{label}]` name —
/// the element's index, or the element itself on an op that
/// [labels its instances](Op::labels_instances).
struct Instance {
    parent: String,
    index: usize,
    element: Value,
}

/// a mapped op mid-expansion: one slot per element, so the collected output
/// comes out in element order however the instances interleave. `names` is
/// what each slot's instance is called, which is the key its output was
/// persisted under.
struct Fanout {
    names: Vec<String>,
    slots: Vec<Option<Value>>,
    remaining: usize,
    failed: bool,
}

/// this process's identity: eight hex digits, made once, short enough to read
/// off a run row and unlikely enough to collide with the process next to it.
pub(crate) fn instance_id() -> &'static str {
    static ID: std::sync::LazyLock<String> =
        std::sync::LazyLock::new(|| uuid::Uuid::now_v7().simple().to_string()[24..].to_string());
    &ID
}

/// executes jobs against a store. cheap to clone.
#[derive(Clone)]
pub struct Runner {
    jobs: Arc<HashMap<String, Job>>,
    store: Store,
    // one watch sender per in-flight run; cancel() flips it to true
    active: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    // runs this process claimed and stopped executing without recording an
    // outcome, because the store would not take the write that says what
    // happened. their leases are deliberately left to lapse — see
    // `Runner::abandon`
    abandoned: Arc<Mutex<HashSet<String>>>,
    // what this process registered for every job, beside whatever each job
    // registered for itself
    hooks: Arc<Hooks>,
    pools: Pools,
    resources: Resources,
    io: Io,
    // tags every run this runner launches carries, under whatever the launch
    // itself said
    run_tags: Arc<RunTags>,
    // what this process claims runs as
    claimer: &'static str,
    // read at the top of every dispatch pass, so raising a limit takes effect
    // on the next one rather than on the next deploy
    limits: Arc<Mutex<Limits>>,
    // the queue position a launch that does not ask gets
    priority: i64,
    // what happens to a run whose claimer went quiet
    reclaim: Reclaim,
    // whether a run's terminal event is written down and delivered by a loop
    // rather than handed straight to the hooks
    durable: bool,
    // what this process does about the queue at all
    role: Role,
    // how many runs this process will execute at once, whatever the queue holds
    slots: usize,
    // one dispatch pass at a time in this process: two passes counting the same
    // free slot would both fill it
    dispatching: Arc<Mutex<()>>,
    // woken whenever a run reaches a terminal status, which is what `run` waits
    // on instead of polling
    settled: Arc<Notify>,
    // who this handle is acting for, where somebody is. set by `as_actor` and
    // nothing else, so a runner that was never told is one that records nobody
    actor: Option<Arc<str>>,
}

impl Runner {
    /// a runner over `jobs`, writing to `store`.
    ///
    /// this is the layer under [`Hestan`](crate::Hestan): no schedules, no
    /// sensors, no server, and nothing running in the background until
    /// something asks it to. reach for it when you want to execute a job from
    /// your own code and nothing else.
    ///
    /// no pools are declared, so an op that names one fails at run time —
    /// [`with_pools`](Runner::with_pools) is the constructor for that.
    ///
    /// two jobs under one name is [`Error::DuplicateJob`]: which of them you
    /// would have got depends on the order they were handed over, and a
    /// deployment running the other one is not something a warning fixes.
    pub fn new(jobs: impl IntoIterator<Item = Job>, store: Store) -> Result<Runner, Error> {
        Runner::with_failure_hooks(jobs, store, Vec::new())
    }

    /// like [`Runner::new`] with failure hooks attached: each is invoked on
    /// its own task whenever a run finishes failed. canceled runs don't fire.
    ///
    /// these are [run hooks](crate::RunHook) that filter on the status rather
    /// than a mechanism of their own — see
    /// [`with_hooks`](Runner::with_hooks), which is how the wider events get
    /// in.
    ///
    /// no pools are declared, so an op that names one fails at run time; use
    /// [`Runner::with_pools`] (or `Hestan::pool`) when any op does.
    ///
    /// two jobs under one name is [`Error::DuplicateJob`], exactly as it is
    /// for [`Runner::new`].
    pub fn with_failure_hooks(
        jobs: impl IntoIterator<Item = Job>,
        store: Store,
        hooks: Vec<FailureHook>,
    ) -> Result<Runner, Error> {
        let mut map = HashMap::new();
        for job in jobs {
            let name = job.name().to_string();
            if map.insert(name.clone(), job).is_some() {
                return Err(Error::DuplicateJob(name));
            }
        }
        Ok(Runner {
            jobs: Arc::new(map),
            store,
            active: Arc::new(Mutex::new(HashMap::new())),
            abandoned: Arc::new(Mutex::new(HashSet::new())),
            hooks: Arc::new(Hooks {
                run: hooks.into_iter().map(crate::hooks::as_run_hook).collect(),
                op: Vec::new(),
            }),
            pools: Arc::new(HashMap::new()),
            resources: resource::none(),
            io: Io::default(),
            run_tags: Arc::new(RunTags::new()),
            claimer: instance_id(),
            limits: Arc::new(Mutex::new(Limits::new())),
            priority: 0,
            reclaim: Reclaim::default(),
            durable: false,
            role: Role::default(),
            slots: usize::MAX,
            dispatching: Arc::new(Mutex::new(())),
            settled: Arc::new(Notify::new()),
            actor: None,
        })
    }

    /// the hooks every job's events reach, on top of any a
    /// [job registered for itself](crate::JobBuilder::on_run_finished).
    /// `Hestan::on_run_finished` and `Hestan::on_op_finished` are the way in.
    ///
    /// a hook registered here with
    /// [`with_failure_hooks`](Runner::with_failure_hooks) keeps whatever it
    /// had: the two go to the same place and this adds to it.
    pub fn with_hooks(self, run: Vec<RunHook>, op: Vec<OpHook>) -> Runner {
        let mut hooks = Hooks::clone(&self.hooks);
        hooks.run.extend(run);
        hooks.op.extend(op);
        Runner {
            hooks: Arc::new(hooks),
            ..self
        }
    }

    /// every run hook one run's terminal event goes to: this process's, then
    /// its job's own.
    pub(crate) fn run_hooks(&self, job: &str) -> Vec<RunHook> {
        let mut hooks = self.hooks.run.clone();
        if let Some(job) = self.jobs.get(job) {
            hooks.extend(job.hooks().run.iter().cloned());
        }
        hooks
    }

    /// every op hook one run's attempts go to, in the same order.
    fn op_hooks(&self, job: &Job) -> Vec<OpHook> {
        let mut hooks = self.hooks.op.clone();
        hooks.extend(job.hooks().op.iter().cloned());
        hooks
    }

    /// tag every run this runner launches with `tags`, under whatever the
    /// launch itself asked for. `Hestan::run_tags` is the way in.
    pub fn with_run_tags(self, tags: RunTags) -> Runner {
        Runner {
            run_tags: Arc::new(tags),
            ..self
        }
    }

    /// what the dispatcher will not start past, and the queue position a launch
    /// that does not ask for one gets. `Hestan::max_concurrent_runs`,
    /// `Hestan::tag_limit` and `Hestan::priority` are the way in.
    ///
    /// per-job limits declared with
    /// [`JobBuilder::max_concurrent_runs`](crate::JobBuilder::max_concurrent_runs)
    /// are folded in here too, so one place answers "what holds this run back".
    pub fn with_limits(self, limits: Limits, priority: i64) -> Runner {
        let mut limits = limits;
        for job in self.jobs.values() {
            if let Some(n) = job.max_concurrent_runs() {
                limits.per_job.entry(job.name().to_string()).or_insert(n);
            }
        }
        Runner {
            limits: Arc::new(Mutex::new(limits)),
            priority,
            ..self
        }
    }

    /// what this process does about the queue, and how many runs it will
    /// execute at once. `Hestan::role` and `Hestan::slots` are the way in.
    pub fn with_role(self, role: Role, slots: usize) -> Runner {
        Runner {
            role,
            slots: slots.max(1),
            ..self
        }
    }

    /// what this process does about the queue.
    pub fn role(&self) -> Role {
        self.role
    }

    /// what happens to a run this deployment loses track of.
    /// `Hestan::reclaim` is the way in.
    pub fn with_reclaim(self, reclaim: Reclaim) -> Runner {
        Runner { reclaim, ..self }
    }

    /// write each run's terminal event down instead of handing it straight to
    /// the hooks, for a [delivery loop](crate::Hestan::durable_notifications)
    /// to deliver. off by default and meant to stay that way for anything
    /// whose hook is a metric rather than a page.
    pub fn with_durable_notifications(self) -> Runner {
        Runner {
            durable: true,
            ..self
        }
    }

    /// whether this runner writes its terminal events down.
    pub(crate) fn durable(&self) -> bool {
        self.durable
    }

    /// what the dispatcher is enforcing right now.
    pub fn limits(&self) -> Limits {
        self.limits.lock().unwrap().clone()
    }

    /// change what the dispatcher enforces, live. the next pass reads it — and
    /// there is a pass whenever a run finishes, and one every half second
    /// besides — so raising a limit drains the queue it was holding back
    /// without a restart.
    pub fn set_limits(&self, limits: Limits) {
        *self.limits.lock().unwrap() = limits;
        self.dispatch();
    }

    /// the id this process claims runs under, as it appears on a run row.
    pub fn instance(&self) -> &str {
        self.claimer
    }

    /// like [`Runner::with_failure_hooks`] plus named concurrency pools, each
    /// a `(name, limit)` shared by every job this runner owns. an op naming a
    /// pool that isn't declared here is [`Error::Graph`], as is declaring the
    /// same pool twice; a limit below 1 means 1.
    pub fn with_pools(
        jobs: impl IntoIterator<Item = Job>,
        store: Store,
        hooks: Vec<FailureHook>,
        pools: impl IntoIterator<Item = (String, usize)>,
    ) -> Result<Runner, Error> {
        Runner::with_resources(jobs, store, hooks, pools, resource::none(), Io::default())
    }

    /// like [`Runner::with_pools`] with [io managers](crate::IoManager)
    /// attached: `default` is where every op's output is persisted, and
    /// `named` are the ones an op can select with [`Op::io`]. an op naming a
    /// manager that is not in `named` is [`Error::Graph`].
    ///
    /// `Arc::new(Inline)` as the default is exactly today's behaviour —
    /// outputs are their own handles and land in the run log as json.
    /// `Hestan::io` and `Hestan::io_named` are the way in from the builder.
    pub fn with_io(
        jobs: impl IntoIterator<Item = Job>,
        store: Store,
        hooks: Vec<FailureHook>,
        pools: impl IntoIterator<Item = (String, usize)>,
        default: Arc<dyn IoManager>,
        named: impl IntoIterator<Item = (String, Arc<dyn IoManager>)>,
    ) -> Result<Runner, Error> {
        let io = Io::new(Some(default), named.into_iter().collect());
        Runner::with_resources(jobs, store, hooks, pools, resource::none(), io)
    }

    /// like [`Runner::with_pools`] plus the process-wide resources every op
    /// shares, already built. an op that declared [`Op::requires`] for a
    /// resource that is not here is [`Error::Graph`], on the same grounds as
    /// an undeclared pool: a dependency you named and did not get is a build
    /// mistake, not a 3am one. `Hestan::resource` is the way in.
    pub(crate) fn with_resources(
        jobs: impl IntoIterator<Item = Job>,
        store: Store,
        hooks: Vec<FailureHook>,
        pools: impl IntoIterator<Item = (String, usize)>,
        resources: Resources,
        io: Io,
    ) -> Result<Runner, Error> {
        let mut declared: HashMap<String, Pool> = HashMap::new();
        for (name, limit) in pools {
            if declared.contains_key(&name) {
                return Err(Error::Graph(format!("pool {name} is declared twice")));
            }
            let limit = limit.max(1);
            let pool = Pool {
                limit,
                sem: Arc::new(Semaphore::new(limit)),
            };
            declared.insert(name, pool);
        }
        let runner = Runner::with_failure_hooks(jobs, store, hooks)?;
        // every op that names a pool must find it, or the limit it was written
        // to respect would silently not exist
        for job in runner.jobs.values() {
            for op in job.ops() {
                if let Some(pool) = op.pool_name()
                    && !declared.contains_key(pool)
                {
                    return Err(Error::Graph(format!(
                        "job {}: op {} takes from pool {pool}, which is not declared",
                        job.name(),
                        op.name()
                    )));
                }
            }
        }
        for job in runner.jobs.values() {
            for op in job.ops() {
                for name in op.required_resources() {
                    if !resources.contains_key(name) {
                        return Err(Error::Graph(format!(
                            "job {}: op {} requires resource {name}, which is not registered",
                            job.name(),
                            op.name()
                        )));
                    }
                }
            }
        }
        // an isolated op's child opens this database by path and reads the run
        // out of it. `:memory:` is private to a connection, so the child would
        // open an empty one and find no run at all — refused here rather than
        // discovered as a baffling failure at 3am
        if runner.store.is_private() {
            for job in runner.jobs.values() {
                for op in job.ops() {
                    if op.is_isolated() {
                        return Err(Error::Graph(format!(
                            "job {}: op {} is .isolated(), which needs a database a child \
                             process can open; \":memory:\" is private to this one",
                            job.name(),
                            op.name()
                        )));
                    }
                }
            }
        }
        // an op persisting through a manager nobody registered would quietly
        // fall back to the run log, which is the one place it said not to go
        for job in runner.jobs.values() {
            for op in job.ops() {
                if let Some(name) = op.io_name()
                    && !io.knows(name)
                {
                    return Err(Error::Graph(format!(
                        "job {}: op {} persists through io manager {name}, which is not registered",
                        job.name(),
                        op.name()
                    )));
                }
            }
        }
        Ok(Runner {
            pools: Arc::new(declared),
            resources,
            io,
            ..runner
        })
    }

    /// the resources this runner hands its ops: names and declared types,
    /// sorted, never values.
    pub fn resources(&self) -> Vec<(&str, &'static str)> {
        let mut all: Vec<(&str, &'static str)> = self
            .resources
            .iter()
            .map(|(name, res)| (name.as_str(), res.type_name))
            .collect();
        all.sort_by_key(|(name, _)| *name);
        all
    }

    /// the run log this runner writes to, for reading it back. cloning it is
    /// cheap and shares the same connection.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// what this process can execute, by name. a run of a job that is not in
    /// here can be read and cannot be started.
    pub fn jobs(&self) -> &HashMap<String, Job> {
        &self.jobs
    }

    /// the managers this runner persists op outputs through. retention reaches
    /// for them as it prunes: a run's rows are the only record of what it
    /// wrote.
    pub(crate) fn io(&self) -> &Io {
        &self.io
    }

    /// the limit declared for `name`, for reporting it back.
    pub fn pool_limit(&self, name: &str) -> Option<usize> {
        self.pools.get(name).map(|p| p.limit)
    }

    /// the same runner, launching and cancelling **as** somebody.
    ///
    /// what it changes is what gets written down: the run row's actor, the
    /// events for what this handle does, and nothing else — a role was already
    /// checked before anything got here, and this is the audit trail rather
    /// than a second gate.
    ///
    /// `None` gives a handle that records nobody, which is what an
    /// unauthenticated deployment and every internal loop use. an empty name
    /// is not "system": a run nobody can be named for says so by having no
    /// actor at all.
    ///
    /// cheap, like every other clone of a runner.
    pub fn as_actor(&self, name: Option<&str>) -> Runner {
        Runner {
            actor: name.map(Arc::from),
            ..self.clone()
        }
    }

    /// who this handle acts for.
    pub fn actor(&self) -> Option<&str> {
        self.actor.as_deref()
    }

    /// put a run of `job` on the queue.
    ///
    /// launching is a request, not a start: the run row exists when this
    /// returns and the dispatcher starts it as soon as no
    /// [limit](Limits) says otherwise, which with no limits declared is the
    /// same instant. the caller never blocks either way — what comes back is
    /// the run id, exactly as it always did.
    pub fn launch(&self, job: &str, params: Value, trigger: Trigger) -> Result<String, Error> {
        self.launch_at(job, params, trigger, None)
    }

    /// [`Runner::launch`] with [tags](RunTags) on the run: the launch's own,
    /// merged over whatever `with_run_tags` set, per-launch winning.
    pub fn launch_tagged(
        &self,
        job: &str,
        params: Value,
        trigger: Trigger,
        tags: RunTags,
    ) -> Result<String, Error> {
        self.launch_prioritized(job, params, trigger, tags, None)
    }

    /// [`Runner::launch_tagged`] at a chosen queue position: higher goes first,
    /// ties by creation time, `None` for whatever `Hestan::priority` set.
    ///
    /// priority is a preference, not an order. the dispatcher skips a run a
    /// limit would block and starts the next one that fits, because one
    /// `env:prod` run at the head of the queue holding up everything unrelated
    /// behind it is worse than starting things slightly out of order.
    pub fn launch_prioritized(
        &self,
        job: &str,
        params: Value,
        trigger: Trigger,
        tags: RunTags,
        priority: Option<i64>,
    ) -> Result<String, Error> {
        Ok(self
            .enqueue(job, None, params, trigger, None, None, None, tags, priority)?
            .expect("only a claimed run key skips a launch"))
    }

    /// [`Runner::launch`] for a run that stands for a logical time: the cron
    /// occurrence it fires for, which is not the wall clock it launched at
    /// once a schedule is catching up or a held fire drains. the ops read it
    /// back with [`OpCtx::scheduled_for`](crate::OpCtx::scheduled_for), and it
    /// lands on the run row. `None` is an ordinary launch.
    pub fn launch_at(
        &self,
        job: &str,
        params: Value,
        trigger: Trigger,
        scheduled_for: Option<DateTime<Utc>>,
    ) -> Result<String, Error> {
        Ok(self
            .enqueue(
                job,
                None,
                params,
                trigger,
                None,
                scheduled_for,
                None,
                RunTags::new(),
                None,
            )?
            .expect("only a claimed run key skips a launch"))
    }

    /// [`Runner::launch`] for a request carrying a [run
    /// key](crate::RunRequest::key): the key is claimed in the same
    /// transaction that creates the run, so it can never name a run that was
    /// not created. `Ok(None)` is a key already claimed — nothing launched,
    /// and nothing failed.
    pub(crate) fn launch_keyed(
        &self,
        job: &str,
        params: Value,
        trigger: Trigger,
        key: RunKey<'_>,
        tags: RunTags,
    ) -> Result<Option<String>, Error> {
        self.enqueue(
            job,
            None,
            params,
            trigger,
            None,
            None,
            Some(key),
            tags,
            None,
        )
    }

    /// like [`Runner::launch`] but awaits completion — including the time the
    /// run spends queued, if a limit is holding it back.
    pub async fn run(&self, job: &str, params: Value, trigger: Trigger) -> Result<Run, Error> {
        let id = self.launch(job, params, trigger)?;
        self.settle(&id).await
    }

    /// wait for a run to reach a terminal status, whoever ends up executing it.
    ///
    /// the wake-up is a notification rather than a poll, so the common case —
    /// this process started the run and this process finished it — costs one
    /// wake. the timeout beside it covers the two cases no local notification
    /// can: another process claimed the run, and a limit freed up somewhere
    /// nothing here was watching.
    async fn settle(&self, id: &str) -> Result<Run, Error> {
        loop {
            // registered before the status is read, so a run that finishes
            // between the two is not a wake-up missed
            let waiter = self.settled.notified();
            tokio::pin!(waiter);
            waiter.as_mut().enable();
            let run = self.store.run(id)?.expect("run row written at launch");
            if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
                return Ok(run);
            }
            self.dispatch();
            tokio::select! {
                () = waiter => {}
                () = tokio::time::sleep(DISPATCH_POLL) => {}
            }
        }
    }

    /// start whatever the queue will let this process start, now.
    ///
    /// called whenever a run is enqueued and whenever one finishes, and on a
    /// timer besides. each pass claims one run at a time and re-reads the queue
    /// after every claim, so a limit is counted against what is actually
    /// executing rather than against a snapshot taken before this pass started
    /// filling it.
    pub(crate) fn dispatch(&self) {
        // a process that does not execute has no business claiming: it enqueues
        // and leaves the queue for whoever does
        if !self.role.executes() {
            return;
        }
        // and neither has one whose store is refusing writes. claiming a run is
        // promising to record what it does, and a process that cannot keep that
        // promise draining the queue into itself is how a backlog becomes a
        // pile of runs nobody can account for. the lease loop's renewal is the
        // write that says the store is back
        if self.store.health().failing() {
            return;
        }
        // one pass at a time in this process: two passes counting the same free
        // slot would both fill it. across processes the store's claim does it,
        // which is the only place it can be done
        let _pass = self.dispatching.lock().unwrap();
        let limits = self.limits.lock().unwrap().clone();
        let defined: HashSet<String> = self.jobs.keys().cloned().collect();
        loop {
            // a worker takes what it can run, not what it can see. without this
            // the first process to look claims the whole queue and the worker
            // beside it has nothing to do
            if self.active.lock().unwrap().len() >= self.slots {
                return;
            }
            match self
                .store
                .claim_next(self.claimer, LEASE, &limits, &defined)
            {
                Ok(Some((run, plan))) => self.start(run, plan),
                Ok(None) => return,
                Err(e) => {
                    tracing::warn!("dispatch failed: {e}");
                    return;
                }
            }
        }
    }

    /// the queue as this runner sees it: its limits, its jobs. only the cases
    /// below ask — the ui and the command line both go through
    /// [`server::queue_json`](crate::server::queue_json), which takes the
    /// limits as an argument because a reader that does not own them has none
    /// to report.
    #[cfg(test)]
    pub(crate) fn queue(&self, limit: u32) -> Result<Vec<Queued>, Error> {
        let limits = self.limits.lock().unwrap().clone();
        let defined: HashSet<String> = self.jobs.keys().cloned().collect();
        self.store.queue(&limits, &defined, limit)
    }

    #[cfg(test)]
    pub(crate) fn queue_depth(&self) -> Result<usize, Error> {
        self.store.queue_depth()
    }

    /// execute a run this process has claimed.
    fn start(&self, run: Run, plan: Option<Value>) {
        let job = self
            .jobs
            .get(&run.job)
            .cloned()
            .expect("a claim only ever names a job this process defines");
        let (pending, seeded) = planned(&job, plan.as_ref());
        // registered before the task is spawned, so a cancel that can see the
        // claimed run always finds a live sender
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.active
            .lock()
            .unwrap()
            .insert(run.id.clone(), cancel_tx);
        tokio::spawn(execute(
            job,
            run.id,
            run.params,
            run.trigger,
            run.scheduled_for,
            self.clone(),
            cancel_rx,
            pending,
            seeded,
        ));
    }

    /// stop touching a run this process cannot record.
    ///
    /// it gets no terminal status and fires no hooks. **this process no longer
    /// knows what is true about it**: the work may have finished, and the row
    /// that would say so is the write that did not land. reporting success
    /// would be a lie and reporting failure would be a guess, so it reports
    /// nothing and lets go — the claim stops being renewed, the lease lapses,
    /// and [`Hestan::reclaim`](crate::Hestan::reclaim) decides what the run
    /// was: failed, saying a claimer went away, or back on the queue.
    ///
    /// what that leaves behind is a run sitting `running` with a dead lease
    /// until some process with a working store notices. that is worse than a
    /// clean failure and it is the trade being made on purpose, because the
    /// alternative is a run that says `success` over a store that never heard
    /// about it.
    fn abandon(&self, run_id: &str) {
        // the write that would not land named itself in the line above this
        // one, at the same level, from `Store::landed`
        tracing::error!(
            run = %run_id,
            "leaving this run for a reclaimer rather than reporting an outcome \
             the store did not take"
        );
        self.abandoned.lock().unwrap().insert(run_id.to_string());
        self.active.lock().unwrap().remove(run_id);
    }

    /// the runs this process is executing, by id.
    ///
    /// the claim it has given up on is not one of them: the row still names
    /// this process and it is not executing it, which is the difference a
    /// reader has to be able to see.
    pub(crate) fn holding(&self) -> Result<Vec<String>, Error> {
        let given_up = self.abandoned.lock().unwrap();
        Ok(self
            .store
            .held_by(self.claimer)?
            .into_iter()
            .filter(|id| !given_up.contains(id))
            .collect())
    }

    /// the runs this process claimed and could not record, waiting on a
    /// reclaimer. empty in every deployment whose store is working.
    pub(crate) fn given_up(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.abandoned.lock().unwrap().iter().cloned().collect();
        ids.sort();
        ids
    }

    /// one turn of the lease loop: say this process is still here, then take
    /// back whatever belonged to one that is not.
    ///
    /// both halves run everywhere, including in a process that claims nothing
    /// of its own — noticing a dead claimer is not the executor's job in
    /// particular, and a deployment where only the dead process could have
    /// noticed would never notice.
    pub(crate) fn heartbeat(&self) {
        let given_up: Vec<String> = self.abandoned.lock().unwrap().iter().cloned().collect();
        if let Err(e) = self.store.renew_leases(self.claimer, LEASE, &given_up) {
            tracing::warn!("lease renewal failed: {e}");
        }
        let durable = self.durable;
        let taken = match self.store.reclaim_expired(self.reclaim, |run| {
            durable.then(|| serde_json::to_value(reclaimed(run)).expect("a run event is json"))
        }) {
            Ok(taken) => taken,
            Err(e) => {
                tracing::warn!("reclaim failed: {e}");
                return;
            }
        };
        if taken.is_empty() {
            return;
        }
        for run in &taken {
            tracing::warn!(
                run = %run.id,
                "reclaimed from {}, which stopped renewing its lease: {}",
                run.claimed_by.as_deref().unwrap_or("an unknown claimer"),
                match self.reclaim {
                    Reclaim::Fail => "failed",
                    Reclaim::Requeue => "requeued",
                }
            );
            // a stall is exactly the thing an on-call hook exists to hear
            // about, and `Fail` is the default because surfacing one beats
            // repeating half its side effects in silence. durable, the row the
            // reclaim wrote is what carries it, and the delivery loop sends it
            if self.reclaim == Reclaim::Fail && !durable {
                fire_hooks(&self.run_hooks(&run.job), reclaimed(run), "run");
            }
        }
        self.settled.notify_waiters();
        self.dispatch();
    }

    /// move a queued run up or down the queue, and look at the queue again.
    pub(crate) fn set_priority(&self, run_id: &str, priority: i64) -> Result<bool, Error> {
        let moved = self.store.set_run_priority(run_id, priority)?;
        if moved {
            self.dispatch();
        }
        Ok(moved)
    }

    /// launch over a subset of the job's ops with upstream outputs pre-seeded.
    /// every subset member's dep must be in the subset or seeded, else
    /// [`Error::Graph`]. asset builds and resumes are the callers.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn launch_subset(
        &self,
        job: &str,
        ops: HashSet<String>,
        seeded: HashMap<String, Value>,
        params: Value,
        trigger: Trigger,
        resumed_from: Option<&str>,
        tags: RunTags,
        priority: Option<i64>,
    ) -> Result<String, Error> {
        Ok(self
            .enqueue(
                job,
                Some((ops, seeded)),
                params,
                trigger,
                resumed_from,
                None,
                None,
                tags,
                priority,
            )?
            .expect("only a claimed run key skips a launch"))
    }

    /// re-run a finished run from where it broke: every op that did not
    /// succeed runs again, with the outputs of the ones that did seeded in.
    /// returns the new run's id.
    pub fn resume(&self, run_id: &str) -> Result<String, Error> {
        self.resume_from(run_id, None)
    }

    /// [`Runner::resume`] with a chosen starting point. `from` re-runs exactly
    /// those ops and everything downstream of them whatever their last status
    /// was — "re-run from here" — and `None` means every op that did not
    /// succeed. the new run records `resumed_from`, carries the original run's
    /// params, and is triggered [`Trigger::Resume`].
    pub fn resume_from(&self, run_id: &str, from: Option<&[String]>) -> Result<String, Error> {
        let plan = self.resume_plan(run_id, from)?;
        self.launch_subset(
            &plan.job,
            plan.rerun.iter().cloned().collect(),
            plan.seeded,
            plan.params,
            Trigger::Resume,
            Some(&plan.resumed_from),
            RunTags::new(),
            None,
        )
    }

    /// what [`Runner::resume_from`] would launch, without launching it. every
    /// refusal it can raise is raised here too, so a preview and the launch
    /// that follows it agree.
    ///
    /// outputs come from the run and, following `resumed_from`, from the runs
    /// it continues: each op is seeded with the most recent successful output
    /// recorded anywhere in that chain. the ops recorded across the chain must
    /// still be exactly the job's ops, or the resume is refused — resuming
    /// into a changed graph would record lineage that never happened. that
    /// also rules out resuming a run that was itself launched as a subset
    /// without being a resume (an asset build records rows for its plan only).
    pub fn resume_plan(&self, run_id: &str, from: Option<&[String]>) -> Result<ResumePlan, Error> {
        // an empty selection is a caller saying nothing, not asking for nothing
        let from = from.filter(|names| !names.is_empty());
        let run = self
            .store
            .run(run_id)?
            .ok_or_else(|| Error::UnknownRun(run_id.to_string()))?;
        match run.status {
            RunStatus::Queued | RunStatus::Running => {
                return Err(Error::RunActive(run_id.to_string()));
            }
            // nothing to continue in a run that worked; re-running from a
            // chosen op is still meaningful, so only the plain resume refuses
            RunStatus::Success if from.is_none() => {
                return Err(Error::RunNotFailed(run_id.to_string()));
            }
            _ => {}
        }
        let job = self
            .jobs
            .get(&run.job)
            .ok_or_else(|| Error::UnknownJob(run.job.clone()))?;

        // newest run first, so the first row seen for an op is the one that counts
        let mut latest: HashMap<String, OpStatus> = HashMap::new();
        let mut reusable: HashMap<String, Value> = HashMap::new();
        for prev in self.resume_chain(&run)? {
            let rows = self.store.op_runs(&prev.id)?;
            for (op, status, output) in fold_instances(&self.io, job, &prev.id, rows) {
                latest.entry(op.clone()).or_insert(status);
                if let (OpStatus::Success, Some(output)) = (status, output) {
                    reusable.entry(op).or_insert(output);
                }
            }
        }

        let current: BTreeSet<&str> = job.ops().iter().map(|o| o.name()).collect();
        let recorded: BTreeSet<&str> = latest.keys().map(String::as_str).collect();
        // a mapped op records no row of its own — its instances are the
        // record, and an expansion over an empty array leaves none at all — so
        // it stays out of the shape check and simply re-expands below
        let mapped: BTreeSet<&str> = job
            .ops()
            .iter()
            .filter(|o| o.mapped_over().is_some())
            .map(|o| o.name())
            .collect();
        let want: BTreeSet<&str> = current.difference(&mapped).copied().collect();
        let have: BTreeSet<&str> = recorded.difference(&mapped).copied().collect();
        if want != have {
            let mut parts = Vec::new();
            let only_job: Vec<&str> = want.difference(&have).copied().collect();
            let only_run: Vec<&str> = have.difference(&want).copied().collect();
            if !only_job.is_empty() {
                parts.push(format!("only in the job: {}", only_job.join(", ")));
            }
            if !only_run.is_empty() {
                parts.push(format!("only in the run: {}", only_run.join(", ")));
            }
            return Err(Error::Graph(format!(
                "job {}: run {run_id} recorded a different set of ops ({})",
                job.name(),
                parts.join("; ")
            )));
        }

        let roots: Vec<&str> = match from {
            Some(names) => {
                let unknown: Vec<&str> = names
                    .iter()
                    .map(String::as_str)
                    .filter(|n| !current.contains(n))
                    .collect();
                if !unknown.is_empty() {
                    return Err(Error::Graph(format!(
                        "job {}: run {run_id} cannot re-run from ops the job does not have: {}",
                        job.name(),
                        unknown.join(", ")
                    )));
                }
                names.iter().map(String::as_str).collect()
            }
            // a success with no recorded output has to run again whatever its row says
            None => current
                .iter()
                .copied()
                .filter(|n| {
                    latest.get(*n) != Some(&OpStatus::Success) || !reusable.contains_key(*n)
                })
                .collect(),
        };
        let pairs = job.dep_pairs();
        let mut subset: HashSet<String> = roots.iter().map(|n| n.to_string()).collect();
        for root in &roots {
            subset.extend(graph::downstream(&pairs, root));
        }
        if subset.is_empty() {
            return Err(Error::NothingToResume(run_id.to_string()));
        }

        let rerun: Vec<String> = job
            .order()
            .iter()
            .filter(|n| subset.contains(*n))
            .cloned()
            .collect();
        let reuse: Vec<String> = job
            .order()
            .iter()
            .filter(|n| !subset.contains(*n) && reusable.contains_key(*n))
            .cloned()
            .collect();
        // every dep a re-run op reads has to come from somewhere: the subset
        // itself, an output recorded up the chain, or an external name
        for name in &rerun {
            let op = job.op(name).expect("subset op is an op of the job");
            for dep in op.deps() {
                if subset.contains(dep) || reusable.contains_key(dep) || job.is_external(dep) {
                    continue;
                }
                return Err(Error::Graph(format!(
                    "job {}: run {run_id} cannot re-run {name}: its dep {dep} \
                     has no recorded output to reuse",
                    job.name()
                )));
            }
        }

        let mut seeded: HashMap<String, Value> = reuse
            .iter()
            .map(|n| (n.clone(), reusable[n].clone()))
            .collect();
        // externals are seeded null by a full launch; a resume of one keeps that
        seeded.extend(job.external_seeds());
        Ok(ResumePlan {
            job: run.job,
            rerun,
            reuse,
            params: run.params,
            seeded,
            resumed_from: run.id,
        })
    }

    // the run, then every run it continues, newest first
    fn resume_chain(&self, run: &Run) -> Result<Vec<Run>, Error> {
        let mut chain = vec![run.clone()];
        let mut seen: HashSet<String> = HashSet::from([run.id.clone()]);
        while let Some(parent) = chain[chain.len() - 1].resumed_from.clone() {
            if chain.len() >= MAX_RESUME_CHAIN {
                return Err(Error::ResumeChain(format!(
                    "run {} continues more than {MAX_RESUME_CHAIN} runs",
                    run.id
                )));
            }
            // uuid v7 ids only ever point backwards in time, so this is a
            // corrupted chain rather than something a resume can produce
            if !seen.insert(parent.clone()) {
                return Err(Error::ResumeChain(format!(
                    "run {} loops back to {parent}",
                    run.id
                )));
            }
            let Some(prev) = self.store.run(&parent)? else {
                return Err(Error::ResumeChain(format!(
                    "run {} continues {parent}, which is no longer in the run history",
                    run.id
                )));
            };
            chain.push(prev);
        }
        Ok(chain)
    }

    /// like [`Runner::launch_subset`] but awaits completion.
    pub(crate) async fn run_subset(
        &self,
        job: &str,
        ops: HashSet<String>,
        seeded: HashMap<String, Value>,
        params: Value,
        trigger: Trigger,
        tags: RunTags,
    ) -> Result<Run, Error> {
        let id = self.launch_subset(job, ops, seeded, params, trigger, None, tags, None)?;
        self.settle(&id).await
    }

    /// ask a queued or running run to stop. in-flight ops are aborted, every
    /// op that isn't terminal yet is marked canceled, and the run finishes
    /// with status canceled.
    pub fn cancel(&self, run_id: &str) -> Result<CancelOutcome, Error> {
        let Some(run) = self.store.run(run_id)? else {
            return Ok(CancelOutcome::Unknown);
        };
        if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
            return Ok(CancelOutcome::AlreadyFinished);
        }
        // still on the queue with nobody executing it: take it off the queue,
        // which is the only way to stop a run that has not started. atomic
        // against a dispatcher reaching for the same row — one of the two wins,
        // and if the claim does, the sender below is there to signal
        if run.status == RunStatus::Queued
            && run.claimed_by.is_none()
            && self.store.cancel_queued(run_id, self.actor())?
        {
            self.active.lock().unwrap().remove(run_id);
            self.settled.notify_waiters();
            return Ok(CancelOutcome::Requested);
        }
        match self.active.lock().unwrap().get(run_id) {
            Some(tx) => {
                // the run's own terminal event is written by whatever is
                // executing it, and that is not this call and has no idea who
                // asked — so the asking is a line of its own, and it is the
                // line with the name on it
                note(self.store.cancel_requested(run_id, self.actor()));
                let _ = tx.send(true);
                Ok(CancelOutcome::Requested)
            }
            // active status but no live sender: a run from before a restart
            None => Ok(CancelOutcome::AlreadyFinished),
        }
    }

    /// write the run row and poke the dispatcher. `Ok(None)` only ever comes
    /// back for a `key` another run already claimed.
    #[allow(clippy::too_many_arguments)]
    fn enqueue(
        &self,
        job: &str,
        subset: Option<(HashSet<String>, HashMap<String, Value>)>,
        params: Value,
        trigger: Trigger,
        resumed_from: Option<&str>,
        scheduled_for: Option<DateTime<Utc>>,
        key: Option<RunKey<'_>>,
        tags: RunTags,
        priority: Option<i64>,
    ) -> Result<Option<String>, Error> {
        let job = self
            .jobs
            .get(job)
            .ok_or_else(|| Error::UnknownJob(job.to_string()))?
            .clone();
        let subset_plan = subset.is_some();
        let (pending, seeded) = match subset {
            None => (job.order().to_vec(), job.external_seeds()),
            Some((ops, seeded)) => {
                for name in &ops {
                    let Some(op) = job.op(name) else {
                        return Err(Error::Graph(format!(
                            "job {}: subset op {name} is not an op of the job",
                            job.name()
                        )));
                    };
                    if seeded.contains_key(name) {
                        return Err(Error::Graph(format!(
                            "job {}: op {name} is both in the subset and seeded",
                            job.name()
                        )));
                    }
                    for dep in op.deps() {
                        if !ops.contains(dep) && !seeded.contains_key(dep) {
                            return Err(Error::Graph(format!(
                                "job {}: subset op {name} depends on {dep}, \
                                 which is neither in the subset nor seeded",
                                job.name()
                            )));
                        }
                    }
                }
                // the job's topo order filtered down, so a subset runs its ops
                // in the order a full launch would
                let pending: Vec<String> = job
                    .order()
                    .iter()
                    .filter(|n| ops.contains(*n))
                    .cloned()
                    .collect();
                (pending, seeded)
            }
        };
        // validated before the run row exists, so a rejected launch leaves no trace
        for op in job.ops() {
            if !pending.iter().any(|n| n == op.name()) {
                continue;
            }
            if let Err(reason) = op.validate_params(&params) {
                return Err(Error::InvalidParams {
                    op: op.name().to_string(),
                    reason,
                });
            }
        }
        let run = Run {
            id: new_run_id(),
            job: job.name().to_string(),
            status: RunStatus::Queued,
            trigger,
            params,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: resumed_from.map(str::to_string),
            scheduled_for,
            // a default is a fact about the deployment and the launch is
            // closer to the truth, so the launch's own tags win
            tags: match self.run_tags.is_empty() {
                true => tags,
                false => {
                    let mut all = (*self.run_tags).clone();
                    all.extend(tags);
                    all
                }
            },
            priority: priority.unwrap_or(self.priority),
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
            actor: self.actor.as_deref().map(str::to_string),
        };
        // a mapped op is never a row of its own: its instances are the record,
        // and how many there are is not known until its dep has produced
        let rows: Vec<String> = pending
            .iter()
            .filter(|n| job.op(n).expect("op in topo order").mapped_over().is_none())
            .cloned()
            .collect();
        // a full launch reconstructs itself from the job, so only a subset has
        // anything to record — and it has to, because its seeds are outputs of
        // an earlier run that live in this process's memory and nowhere a
        // claimer in another process could look
        let plan = subset_plan.then(|| json!({ "ops": &pending, "seeds": &seeded }));
        if !self
            .store
            .create_run_keyed(&run, &rows, key, plan.as_ref())?
        {
            // the key was claimed between the caller's check and this insert:
            // no run row was written, and nothing launched
            return Ok(None);
        }
        // enqueued and on disk; whether it starts now is the dispatcher's
        // business, and with no limits declared "now" is this call
        self.dispatch();
        Ok(Some(run.id))
    }
}

/// what a claimed run is to execute: the ops, in the job's topological order,
/// and the outputs seeded in ahead of them.
///
/// `None` is a run of the whole job, which is every launch that is not a
/// resume, an asset build or a subset — it needs nothing recorded because the
/// job itself is the plan. anything else was written at
/// [enqueue](Runner::enqueue) time, because by the time it is read the process
/// that decided it may be gone.
fn planned(job: &Job, plan: Option<&Value>) -> (Vec<String>, HashMap<String, Value>) {
    let whole = || (job.order().to_vec(), job.external_seeds());
    let Some(plan) = plan else { return whole() };
    let (Some(ops), Some(seeds)) = (plan.get("ops").and_then(Value::as_array), plan.get("seeds"))
    else {
        // written by this crate and nothing else, so this is a plan from a
        // future version or a hand-edited row. running the whole job is the
        // one option that cannot silently run less than was asked for
        tracing::warn!(job = %job.name(), "run plan unreadable; running the whole job");
        return whole();
    };
    let ops = ops
        .iter()
        .filter_map(|v| v.as_str().map(str::to_string))
        .collect();
    let seeds = seeds
        .as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    (ops, seeds)
}

// "process[3]" -> ("process", "3"), but only when `process` is a mapped op of
// this job; any other bracketed name is just an op name
pub(crate) fn instance_of(job: &Job, name: &str) -> Option<(String, String)> {
    let (parent, label) = name.split_once('[')?;
    let label = label.strip_suffix(']')?;
    job.op(parent)?.mapped_over()?;
    Some((parent.to_string(), label.to_string()))
}

/// what one instance of a fan-out is called after the `[`: its index, or the
/// element itself on an op that names its instances by them — which is what
/// makes a partitioned asset's instances read as `daily_orders[2026-01-05]`.
fn instance_label(op: &Op, index: usize, element: &Value) -> String {
    match element.as_str().filter(|_| op.labels_instances()) {
        Some(key) => key.to_string(),
        None => index.to_string(),
    }
}

/// one mapped op's instance rows mid-fold, by index: the instance's name, what
/// it did, and what it recorded.
type InstanceRows = BTreeMap<usize, (String, OpStatus, Option<Value>)>;

/// collapse one run's fan-out instance rows into a single entry for the mapped
/// op they belong to. it counts as succeeded only when the instances cover
/// `0..n` and every one of them did: the array a mapped op expands over can
/// differ on a re-run, so anything less has to expand again from scratch. a
/// mapped op with no rows at all — never reached, or expanded over an empty
/// array — is absent here, which resume planning reads the same way.
///
/// the one manager call hestan makes on its caller's own thread, because
/// [`resume_plan`](Runner::resume_plan) is synchronous and reads its store the
/// same way. it is not a run's task: nothing is executing while a resume is
/// being planned.
fn fold_instances(
    io: &Io,
    job: &Job,
    run_id: &str,
    rows: Vec<OpRun>,
) -> Vec<(String, OpStatus, Option<Value>)> {
    let mut folded: Vec<(String, OpStatus, Option<Value>)> = Vec::with_capacity(rows.len());
    let mut groups: HashMap<String, InstanceRows> = HashMap::new();
    let mut labeled: HashSet<String> = HashSet::new();
    for row in rows {
        match instance_of(job, &row.op) {
            Some((parent, label)) => {
                // an op that names its instances by their element has no index
                // to order or count them by, so it never folds back into a
                // reusable array — it expands again from scratch
                match label.parse::<usize>() {
                    Ok(index) if !job.op(&parent).is_some_and(Op::labels_instances) => {
                        groups
                            .entry(parent)
                            .or_default()
                            .insert(index, (row.op, row.status, row.output));
                    }
                    _ => {
                        labeled.insert(parent);
                    }
                }
            }
            None => folded.push((row.op, row.status, row.output)),
        }
    }
    for parent in labeled {
        groups.remove(&parent);
        folded.push((parent, OpStatus::Failed, None));
    }
    for (parent, slots) in groups {
        let whole = slots.keys().copied().eq(0..slots.len())
            && slots
                .values()
                .all(|(_, status, output)| *status == OpStatus::Success && output.is_some());
        if !whole {
            folded.push((parent, OpStatus::Failed, None));
            continue;
        }
        // the instances' recorded outputs are handles; a mapped op's own
        // value is the array of what they resolve to, exactly as the run that
        // produced it assembled one. anything unreadable re-expands instead
        let manager = io.manager(job.op(&parent).and_then(Op::io_name));
        let mut collected: Vec<Value> = Vec::with_capacity(slots.len());
        let mut readable = true;
        for (_, (op, _, output)) in slots {
            let handle = output.expect("checked just above");
            let key = IoKey {
                run_id: run_id.to_string(),
                job: job.name().to_string(),
                op,
            };
            match manager.get(&key, &handle) {
                Ok(v) => collected.push(v),
                Err(e) => {
                    tracing::warn!(run = %run_id, op = %key.op, "instance output unreadable: {e}");
                    readable = false;
                    break;
                }
            }
        }
        if !readable {
            folded.push((parent, OpStatus::Failed, None));
            continue;
        }
        folded.push((parent, OpStatus::Success, Some(Value::Array(collected))));
    }
    folded
}

/// the terminal event of a run whose claimer went away, from the row the
/// reclaim left behind.
///
/// no `failed_op`: nothing this run did failed. the process holding it stopped
/// saying it was there, which is not any op's doing and should not be reported
/// as one.
fn reclaimed(run: &Run) -> RunEvent {
    let finished_at = run.finished_at.unwrap_or_else(Utc::now);
    RunEvent {
        run_id: run.id.clone(),
        job: run.job.clone(),
        trigger: run.trigger,
        status: RunStatus::Failed,
        failed_op: None,
        error: run.error.clone(),
        started_at: run.started_at,
        finished_at,
        duration: run.started_at.and_then(|s| (finished_at - s).to_std().ok()),
    }
}

/// the loop `serve` runs: [`Runner::heartbeat`] every [`HEARTBEAT`], forever.
pub(crate) async fn run_leases(runner: Runner) {
    let mut ticker = tokio::time::interval(HEARTBEAT);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        runner.heartbeat();
    }
}

/// the loop `serve` runs: [`Runner::dispatch`] every [`DISPATCH_POLL`], forever.
pub(crate) async fn run_dispatcher(runner: Runner) {
    let mut ticker = tokio::time::interval(DISPATCH_POLL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        runner.dispatch();
    }
}

/// one run, inside one span.
///
/// the span is the root of the tree `docs/events.md` calls a trace: every
/// attempt below opens a child of it. it costs nothing when nothing is
/// subscribed, and it is what the `otel` feature exports. it is passed *into*
/// the work as well as wrapped around it because `tokio::spawn` carries no
/// span into the task it starts, and every op runs in one.
#[allow(clippy::too_many_arguments)]
async fn execute(
    job: Job,
    run_id: String,
    params: Value,
    trigger: Trigger,
    scheduled_for: Option<DateTime<Utc>>,
    runner: Runner,
    cancel: watch::Receiver<bool>,
    pending: Vec<String>,
    seeded: HashMap<String, Value>,
) {
    let run_span = tracing::info_span!(
        "hestan.run",
        run_id = %run_id,
        job = %job.name(),
        trigger = %trigger
    );
    execute_in_span(
        job,
        run_id,
        params,
        trigger,
        scheduled_for,
        runner,
        cancel,
        pending,
        seeded,
        run_span.clone(),
    )
    .instrument(run_span)
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_in_span(
    job: Job,
    run_id: String,
    params: Value,
    trigger: Trigger,
    scheduled_for: Option<DateTime<Utc>>,
    runner: Runner,
    mut cancel: watch::Receiver<bool>,
    pending: Vec<String>,
    seeded: HashMap<String, Value>,
    run_span: tracing::Span,
) {
    let store = runner.store.clone();
    let started_at = Utc::now();
    // set the moment a write that records what this run did will not land.
    // from there the run stops touching the store and stops touching itself:
    // see `Runner::abandon` for what it leaves behind and why that is the
    // least dishonest thing available
    let mut unrecorded = !store
        .landed("run_started", || store.run_started(&run_id, started_at))
        .await;
    note(store.append_event(
        &run_id,
        None,
        EventLevel::Info,
        EventKind::RunStarted,
        "run started",
        Some(&json!({ "job": job.name(), "trigger": trigger })),
    ));
    // resolved once per run rather than per op: the set cannot change under a
    // run, and every attempt would otherwise rebuild it
    let op_hooks = Arc::new(runner.op_hooks(&job));

    let pairs = job.dep_pairs();
    let cap = job.max_parallel().unwrap_or(usize::MAX).max(1);
    let mut pending = pending;
    let mut outputs: HashMap<String, Value> = seeded;
    // what each settled unit did, which is what readiness and the trigger
    // rules are asked about. seeded names count as succeeded: a resume's
    // reused output, an asset build's memoized value and a source asset all
    // stand in for a run that worked.
    let mut statuses: HashMap<String, OpStatus> = outputs
        .keys()
        .map(|n| (n.clone(), OpStatus::Success))
        .collect();
    let mut failed = false;
    let mut canceled = false;
    let mut first_failure: Option<(String, String)> = None;
    let mut tasks: JoinSet<OpOutcome> = JoinSet::new();
    // JoinError only carries the task id, so remember which op each task is
    let mut names: HashMap<Id, String> = HashMap::new();
    // one handle per in-flight in-process op, and deliberately not one per
    // isolated op: a cancel aborts the first kind, because being dropped is the
    // only way they stop, and leaves the second kind alone, because an isolated
    // op stops by killing its own child and needs to be alive to do it
    let mut abortable: HashMap<Id, tokio::task::AbortHandle> = HashMap::new();
    // fan-out state: what each instance is for, and where its output belongs
    let mut instances: HashMap<String, Instance> = HashMap::new();
    let mut fanouts: HashMap<String, Fanout> = HashMap::new();

    'run: while !unrecorded {
        // settle every unit whose deps have all reached a terminal status: its
        // trigger rule either admits it or skips it here, and a skip is itself
        // terminal, so this repeats until a sweep settles nothing new.
        //
        // an admitted mapped op expands in the same sweep: it leaves `pending`
        // and its instances take its place, so everything below treats them as
        // ordinary ops. all of it runs on the run's own task, which is why the
        // rows an expansion inserts can never race the terminal status write.
        let mut ready: Vec<String> = Vec::new();
        loop {
            let mut settled = false;
            let mut i = 0;
            while i < pending.len() {
                let name = pending[i].clone();
                let op = unit_op(&job, &instances, &name);
                if !op.deps().iter().all(|d| statuses.contains_key(d)) {
                    i += 1;
                    continue;
                }
                settled = true;
                if let Err(reason) = admits(op, &statuses) {
                    pending.remove(i);
                    note(store.append_event(
                        &run_id,
                        Some(&name),
                        EventLevel::Warn,
                        EventKind::OpSkipped,
                        reason,
                        Some(&json!({ "reason": reason, "when": op.runs_when() })),
                    ));
                    if !store
                        .landed("op_finished", || {
                            store.op_finished(
                                &run_id,
                                &name,
                                OpStatus::Skipped,
                                None,
                                None,
                                None,
                                &[],
                            )
                        })
                        .await
                    {
                        unrecorded = true;
                        break 'run;
                    }
                    statuses.insert(name.clone(), OpStatus::Skipped);
                    let reason = format!("skipped: upstream {name} was skipped");
                    if !skip_downstream(
                        &job,
                        &pairs,
                        &name,
                        &reason,
                        &mut pending,
                        &mut statuses,
                        &run_id,
                        &store,
                    )
                    .await
                    {
                        unrecorded = true;
                        break 'run;
                    }
                    continue;
                }
                // an instance resolves to its parent's op, which is mapped
                // too; only the parent ever expands
                let expandable = !instances.contains_key(&name);
                let Some(over) = op.mapped_over().filter(|_| expandable).map(str::to_string) else {
                    ready.push(pending.remove(i));
                    continue;
                };
                pending.remove(i);
                let mut expanded_over: Option<&'static str> = None;
                let mut unreadable: Option<String> = None;
                // a rule can admit a mapped op whose array never arrived. there
                // is nothing to expand over, so it expands into nothing: the
                // same zero-instance fan-out an empty array gives, output `[]`.
                // the array itself is an op output like any other, so it is
                // fetched back through its manager before it can be counted.
                let elements = match outputs.get(&over).cloned() {
                    None => Ok(Some(Vec::new())),
                    Some(held) => resolve(&runner.io, &job, &run_id, &over, held)
                        .await
                        .map(|v| match v {
                            Value::Array(a) => Some(a),
                            other => {
                                expanded_over = Some(json_type(&other));
                                None
                            }
                        }),
                };
                let elements = match elements {
                    Ok(Some(elements)) => Some(elements),
                    Ok(None) => None,
                    Err(e) => {
                        expanded_over = Some("unreadable");
                        unreadable = Some(e);
                        None
                    }
                };
                // every instance is a row of its own, so two elements that
                // would be called the same thing are not an expansion at all
                let mut repeated: Option<String> = None;
                let elements = elements.filter(|elements| {
                    let mut labels: HashSet<String> = HashSet::new();
                    for (i, element) in elements.iter().enumerate() {
                        let label = instance_label(op, i, element);
                        if !labels.insert(label.clone()) {
                            repeated = Some(label);
                            return false;
                        }
                    }
                    true
                });
                let Some(elements) = elements else {
                    let msg = match (&unreadable, &repeated) {
                        (Some(e), _) => format!("could not read the output of {over}: {e}"),
                        (None, Some(label)) => {
                            format!("expanded over {over}, which named the key {label:?} twice")
                        }
                        (None, None) => format!(
                            "mapped over {over}, which produced {} rather than an array",
                            expanded_over.unwrap_or("something else")
                        ),
                    };
                    note(store.append_event(
                        &run_id,
                        Some(&name),
                        EventLevel::Error,
                        EventKind::OpFailed,
                        &msg,
                        Some(&json!({ "error": &msg })),
                    ));
                    if first_failure.is_none() {
                        first_failure = Some((name.clone(), msg));
                    }
                    failed = true;
                    statuses.insert(name.clone(), OpStatus::Failed);
                    let reason = format!("skipped: upstream {name} failed");
                    if !skip_downstream(
                        &job,
                        &pairs,
                        &name,
                        &reason,
                        &mut pending,
                        &mut statuses,
                        &run_id,
                        &store,
                    )
                    .await
                    {
                        unrecorded = true;
                        break 'run;
                    }
                    continue;
                };
                // rows first, so a cancel or a skip has something to write to,
                // exactly as a static op's row exists from the launch on
                let mut created: Vec<String> = Vec::with_capacity(elements.len());
                for (index, element) in elements.into_iter().enumerate() {
                    let label = instance_label(op, index, &element);
                    let instance = format!("{name}[{label}]");
                    if !store
                        .landed("create_op_run", || store.create_op_run(&run_id, &instance))
                        .await
                    {
                        unrecorded = true;
                        break 'run;
                    }
                    instances.insert(
                        instance.clone(),
                        Instance {
                            parent: name.clone(),
                            index,
                            element,
                        },
                    );
                    created.push(instance);
                }
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Info,
                    EventKind::OpExpanded,
                    &format!("expanded into {} instances over {over}", created.len()),
                    Some(&json!({ "instances": created.len(), "over": over })),
                ));
                if created.is_empty() {
                    // nothing to wait on: an empty array is a legal fan-out,
                    // and downstream runs normally on `[]`
                    outputs.insert(name.clone(), json!([]));
                    statuses.insert(name, OpStatus::Success);
                    continue;
                }
                let n = created.len();
                fanouts.insert(
                    name,
                    Fanout {
                        names: created.clone(),
                        slots: vec![None; n],
                        remaining: n,
                        failed: false,
                    },
                );
                // at `i`, so the instances settle in this same sweep
                pending.splice(i..i, created);
            }
            if !settled {
                break;
            }
        }

        let mut ready = ready.into_iter();
        let spawnable: Vec<String> = ready
            .by_ref()
            .take(cap.saturating_sub(tasks.len()))
            .collect();
        let leftover: Vec<String> = ready.collect();
        if !leftover.is_empty() {
            pending.splice(0..0, leftover);
        }
        for name in spawnable {
            let op = unit_op(&job, &instances, &name).clone();
            // a dep a rule let this op run past may have produced nothing, so
            // its entry is simply absent — `ctx.input` says None, and
            // `ctx.dep_status` says what it did instead
            // keyed by what the body calls each dep, which differs from the
            // job-level name only inside a flattened graph instance
            let mut inputs: HashMap<String, Value> = HashMap::new();
            let mut dep_statuses: HashMap<String, OpStatus> = HashMap::new();
            let mut unresolved: Option<(String, String)> = None;
            // an isolated op's inputs go to its child instead, as the handles
            // and statuses this run holds — it resolves them itself, in the
            // process that wants the bytes
            let mut held: serde_json::Map<String, Value> = serde_json::Map::new();
            let mut dep_json: serde_json::Map<String, Value> = serde_json::Map::new();
            for dep in op.deps() {
                let seen = op.dep_alias(dep).to_string();
                if let Some(handle) = outputs.get(dep).cloned() {
                    if op.is_isolated() {
                        held.insert(dep.clone(), handle);
                    } else {
                        // `outputs` carries handles, so this is where a dep's
                        // output is actually fetched back
                        match resolve(&runner.io, &job, &run_id, dep, handle).await {
                            Ok(v) => {
                                inputs.entry(seen.clone()).or_insert(v);
                            }
                            Err(e) if unresolved.is_none() => {
                                unresolved = Some((dep.clone(), e));
                            }
                            Err(_) => {}
                        }
                    }
                }
                if let Some(s) = statuses.get(dep) {
                    dep_json.insert(dep.clone(), json!(s.as_str()));
                    dep_statuses.entry(seen).or_insert(*s);
                }
            }
            let op_isolated = op.is_isolated();
            let invocation = op_isolated.then(|| invocation(held, dep_json));
            // an input hestan cannot fetch is this op's failure, recorded the
            // same way a failing body would be, rather than an op that runs
            // believing its dep produced nothing
            if let Some((dep, msg)) = unresolved {
                let msg = format!("could not read the output of {dep}: {msg}");
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Error,
                    EventKind::OpFailed,
                    &msg,
                    Some(&json!({ "error": &msg })),
                ));
                if !store
                    .landed("op_finished", || {
                        store.op_finished(
                            &run_id,
                            &name,
                            OpStatus::Failed,
                            None,
                            None,
                            Some(&msg),
                            &[],
                        )
                    })
                    .await
                {
                    unrecorded = true;
                    break 'run;
                }
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                if !give_up(
                    &name,
                    &instances,
                    &mut fanouts,
                    &job,
                    &pairs,
                    &mut pending,
                    &mut statuses,
                    &run_id,
                    &store,
                )
                .await
                {
                    unrecorded = true;
                    break 'run;
                }
                continue;
            }
            // instrumented as well as parented: what `run_op` itself writes to the
            // run log is hestan narrating the run, and belongs on the run's own
            // span. what the op body says belongs on the attempt's, and gets
            // there because the body is instrumented with that one
            let handle = tasks.spawn(
                run_op(
                    op,
                    name.clone(),
                    instances.get(&name).map(|i| i.element.clone()),
                    job.name().to_string(),
                    run_id.clone(),
                    params.clone(),
                    scheduled_for,
                    Arc::new(inputs),
                    Arc::new(dep_statuses),
                    invocation,
                    runner.resources.clone(),
                    store.clone(),
                    runner.pools.clone(),
                    op_hooks.clone(),
                    cancel.clone(),
                    run_span.clone(),
                )
                .instrument(run_span.clone()),
            );
            if !op_isolated {
                abortable.insert(handle.id(), handle.clone());
            }
            names.insert(handle.id(), name);
        }

        let joined = tokio::select! {
            // only true is ever sent, so any resolution here means cancel
            _ = cancel.changed() => {
                canceled = true;
                break;
            }
            joined = tasks.join_next_with_id() => match joined {
                Some(j) => j,
                None => break,
            },
        };
        match joined {
            Ok((id, (name, outcome))) => {
                names.remove(&id);
                abortable.remove(&id);
                // an isolated op's child already did all of this for itself,
                // which is why `Recorded` only has a handle to hand over
                let persisted = match outcome {
                    Outcome::Recorded(handle) => Ok(handle),
                    Outcome::Produced {
                        output,
                        state,
                        meta,
                        built,
                    } => {
                        // persisted before the success is recorded: a row
                        // saying success with an output that was never stored
                        // is a lie the next run would trip over
                        let unit = unit_op(&job, &instances, &name);
                        let key = io_key(&run_id, &job, &name);
                        match crate::io::put(&runner.io, unit.io_name(), key, output).await {
                            Ok(handle) => {
                                // what the manager knows about what it stored,
                                // beside what the op staged
                                let meta = crate::io::handle_meta(&handle, meta);
                                // and whatever it built, in that same write:
                                // the output is stored, so the row that says
                                // the asset is current is now true
                                if !store
                                    .landed("op_finished", || {
                                        store.op_finished(
                                            &run_id,
                                            &name,
                                            OpStatus::Success,
                                            Some(&handle),
                                            meta.as_ref(),
                                            None,
                                            &built,
                                        )
                                    })
                                    .await
                                {
                                    unrecorded = true;
                                    break 'run;
                                }
                                // state second: a crash between the writes
                                // re-runs the op, never skips it. a watermark
                                // that will not commit stops the run for the
                                // same reason a terminal row does: the run
                                // would otherwise finish `success` having
                                // promised to remember where it got to
                                if let Some(state) = state
                                    && !store
                                        .landed("set_op_state", || {
                                            store.set_op_state(job.name(), &name, &state)
                                        })
                                        .await
                                {
                                    unrecorded = true;
                                    break 'run;
                                }
                                Ok(handle)
                            }
                            Err(e) => {
                                let msg = format!("could not persist the output: {e}");
                                note(store.append_event(
                                    &run_id,
                                    Some(&name),
                                    EventLevel::Error,
                                    EventKind::OpFailed,
                                    &msg,
                                    Some(&json!({ "error": &msg })),
                                ));
                                // and `built` goes with the attempt: an asset
                                // whose value nothing stored was not built
                                if !store
                                    .landed("op_finished", || {
                                        store.op_finished(
                                            &run_id,
                                            &name,
                                            OpStatus::Failed,
                                            None,
                                            None,
                                            Some(&msg),
                                            &[],
                                        )
                                    })
                                    .await
                                {
                                    unrecorded = true;
                                    break 'run;
                                }
                                Err(msg)
                            }
                        }
                    }
                    Outcome::Failed(msg) => {
                        if !store
                            .landed("op_finished", || {
                                store.op_finished(
                                    &run_id,
                                    &name,
                                    OpStatus::Failed,
                                    None,
                                    None,
                                    Some(&msg),
                                    &[],
                                )
                            })
                            .await
                        {
                            unrecorded = true;
                            break 'run;
                        }
                        Err(msg)
                    }
                    // only a cancel produces this, so the run is stopping:
                    // record what was watched to happen and go drain the rest
                    Outcome::Killed(msg) => {
                        unrecorded = !op_killed(&store, &run_id, &name, &msg).await;
                        canceled = true;
                        break 'run;
                    }
                    // the op ran and its own start was never written down, so
                    // this process has no idea what state its row is in
                    Outcome::Unrecorded => {
                        unrecorded = true;
                        break 'run;
                    }
                };
                match persisted {
                    Ok(handle) => {
                        collect(
                            name,
                            handle,
                            &runner.io,
                            &job,
                            &run_id,
                            &instances,
                            &mut fanouts,
                            &mut outputs,
                            &mut statuses,
                        )
                        .await
                    }
                    Err(msg) => {
                        if first_failure.is_none() {
                            first_failure = Some((name.clone(), msg));
                        }
                        failed = true;
                        if !give_up(
                            &name,
                            &instances,
                            &mut fanouts,
                            &job,
                            &pairs,
                            &mut pending,
                            &mut statuses,
                            &run_id,
                            &store,
                        )
                        .await
                        {
                            unrecorded = true;
                            break 'run;
                        }
                    }
                }
            }
            Err(join_err) => {
                let name = names.remove(&join_err.id()).expect("spawned with id");
                abortable.remove(&join_err.id());
                let msg = format!("op panicked: {join_err}");
                // run_op never got to report, so emit the terminal event here
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Error,
                    EventKind::OpFailed,
                    &msg,
                    Some(&json!({ "error": msg })),
                ));
                if !store
                    .landed("op_finished", || {
                        store.op_finished(
                            &run_id,
                            &name,
                            OpStatus::Failed,
                            None,
                            None,
                            Some(&msg),
                            &[],
                        )
                    })
                    .await
                {
                    unrecorded = true;
                    break 'run;
                }
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                if !give_up(
                    &name,
                    &instances,
                    &mut fanouts,
                    &job,
                    &pairs,
                    &mut pending,
                    &mut statuses,
                    &run_id,
                    &store,
                )
                .await
                {
                    unrecorded = true;
                    break 'run;
                }
            }
        }
    }

    if canceled && !unrecorded {
        // abort lands at an op's next await point; an op that never awaits, and
        // blocking work an op spawned, never land at all.
        //
        // isolated ops are not aborted: each is watching this same signal and
        // stopping its own child — SIGTERM, a grace, then a kill — and a task
        // dropped mid-sequence would kill the process outright instead. so they
        // are given room to do it, and the wait below is doubled to cover the
        // grace they are spending inside it.
        let waiting_on_a_child = !names.is_empty() && names.len() > abortable.len();
        for handle in abortable.values() {
            handle.abort();
        }
        // a bounded grace period, so an op that really does stop is recorded as
        // whatever it really did rather than guessed at
        let grace = match waiting_on_a_child {
            true => CANCEL_GRACE * 2,
            false => CANCEL_GRACE,
        };
        let deadline = tokio::time::Instant::now() + grace;
        'drain: loop {
            let joined = match tokio::time::timeout_at(deadline, tasks.join_next_with_id()).await {
                Ok(Some(joined)) => joined,
                // every task landed, or the grace ran out with some still running
                Ok(None) | Err(_) => break,
            };
            match joined {
                // won the race against the abort: record what really happened
                Ok((id, (name, outcome))) => {
                    names.remove(&id);
                    match outcome {
                        Outcome::Produced {
                            output,
                            state,
                            meta,
                            built,
                        } => {
                            // won the race against the abort, so it is
                            // persisted like any other success — or recorded
                            // failed if it cannot be, and what it built goes
                            // or stays with that row either way
                            let unit = unit_op(&job, &instances, &name);
                            let key = io_key(&run_id, &job, &name);
                            match crate::io::put(&runner.io, unit.io_name(), key, output).await {
                                Ok(handle) => {
                                    let meta = crate::io::handle_meta(&handle, meta);
                                    if !store
                                        .landed("op_finished", || {
                                            store.op_finished(
                                                &run_id,
                                                &name,
                                                OpStatus::Success,
                                                Some(&handle),
                                                meta.as_ref(),
                                                None,
                                                &built,
                                            )
                                        })
                                        .await
                                    {
                                        unrecorded = true;
                                        break 'drain;
                                    }
                                    if let Some(state) = state
                                        && !store
                                            .landed("set_op_state", || {
                                                store.set_op_state(job.name(), &name, &state)
                                            })
                                            .await
                                    {
                                        unrecorded = true;
                                        break 'drain;
                                    }
                                }
                                Err(e) => {
                                    let msg = format!("could not persist the output: {e}");
                                    if !store
                                        .landed("op_finished", || {
                                            store.op_finished(
                                                &run_id,
                                                &name,
                                                OpStatus::Failed,
                                                None,
                                                None,
                                                Some(&msg),
                                                &[],
                                            )
                                        })
                                        .await
                                    {
                                        unrecorded = true;
                                        break 'drain;
                                    }
                                }
                            }
                        }
                        // the child wrote its own row before the cancel reached
                        // it; there is nothing left to record
                        Outcome::Recorded(_) => {}
                        Outcome::Failed(msg) => {
                            if !store
                                .landed("op_finished", || {
                                    store.op_finished(
                                        &run_id,
                                        &name,
                                        OpStatus::Failed,
                                        None,
                                        None,
                                        Some(&msg),
                                        &[],
                                    )
                                })
                                .await
                            {
                                unrecorded = true;
                                break 'drain;
                            }
                        }
                        Outcome::Killed(msg) => {
                            if !op_killed(&store, &run_id, &name, &msg).await {
                                unrecorded = true;
                                break 'drain;
                            }
                        }
                        // its start was never recorded either, so there is
                        // nothing here this process can put right
                        Outcome::Unrecorded => {
                            unrecorded = true;
                            break 'drain;
                        }
                    }
                }
                Err(join_err) if join_err.is_cancelled() => {
                    let name = names.remove(&join_err.id()).expect("spawned with id");
                    if !op_canceled(&store, &run_id, &name).await {
                        unrecorded = true;
                        break 'drain;
                    }
                }
                Err(join_err) => {
                    let name = names.remove(&join_err.id()).expect("spawned with id");
                    let msg = format!("op panicked: {join_err}");
                    note(store.append_event(
                        &run_id,
                        Some(&name),
                        EventLevel::Error,
                        EventKind::OpFailed,
                        &msg,
                        Some(&json!({ "error": msg })),
                    ));
                    if !store
                        .landed("op_finished", || {
                            store.op_finished(
                                &run_id,
                                &name,
                                OpStatus::Failed,
                                None,
                                None,
                                Some(&msg),
                                &[],
                            )
                        })
                        .await
                    {
                        unrecorded = true;
                        break 'drain;
                    }
                }
            }
        }
        // whatever is still in `names` never joined. aborting cannot stop
        // blocking work, so hestan does not know if it is over — and says so
        // instead of stamping a finish time it never observed.
        let mut unstopped: Vec<String> = names.drain().map(|(_, name)| name).collect();
        unstopped.sort();
        for name in unstopped {
            unrecorded |= !op_unstopped(&store, &run_id, &name, grace).await;
        }
        for name in pending.drain(..) {
            unrecorded |= !op_canceled(&store, &run_id, &name).await;
        }
    }

    // every op of this run that could be recorded has been; what cannot be is
    // the run itself, and this is where it stops. dropping the `JoinSet` on
    // the way out aborts whatever is still in flight — and kills an isolated
    // op's child with it — so nothing carries on working for a run nobody is
    // going to record
    if unrecorded {
        runner.abandon(&run_id);
        return;
    }

    debug_assert!(pending.is_empty(), "ops left unspawned at run end");
    let status = if canceled {
        RunStatus::Canceled
    } else if failed {
        RunStatus::Failed
    } else {
        RunStatus::Success
    };
    let (level, kind, msg) = match status {
        RunStatus::Failed => (EventLevel::Error, EventKind::RunFailed, "run failed"),
        RunStatus::Canceled => (EventLevel::Warn, EventKind::RunCanceled, "run canceled"),
        _ => (EventLevel::Info, EventKind::RunSuccess, "run succeeded"),
    };
    // the run's own error: the first op that terminally failed, named, so a
    // hook or an alert reading the run row sees what RunFailure carries
    let error = first_failure
        .as_ref()
        .map(|(op, msg)| format!("op {op} failed: {msg}"));
    // event first: anyone who reads a terminal status must also see this line
    let ended_at = Utc::now();
    note(store.append_event(
        &run_id,
        None,
        level,
        kind,
        msg,
        Some(&json!({
            "job": job.name(),
            "status": status,
            "error": error,
            "failed_op": first_failure.as_ref().map(|(op, _)| op),
            "duration_secs": (ended_at - started_at).to_std().ok().map(|d| d.as_secs_f64()),
        })),
    ));

    // every terminal status fires, and the status says which — a hook that
    // only wants failures is what `on_failure` still is. the boot sweep does
    // not come through here, so a restart after a crash replays nothing.
    // the same instant the event carried: a hook and the log disagreeing about
    // how long a run took by a millisecond is a question nobody wants to answer
    let finished_at = ended_at;
    let (failed_op, op_error) = match first_failure {
        Some((op, msg)) => (Some(op), Some(msg)),
        None => (None, None),
    };
    let event = RunEvent {
        run_id: run_id.clone(),
        job: job.name().to_string(),
        trigger,
        status,
        failed_op,
        error: op_error,
        started_at: Some(started_at),
        finished_at,
        duration: (finished_at - started_at).to_std().ok(),
    };
    // durable: the event goes into the terminal transaction and the delivery
    // loop takes it from there, so nothing fires twice
    let queued = runner
        .durable
        .then(|| serde_json::to_value(&event).expect("a run event is json"));
    if !store
        .landed("run_finished", || {
            store.run_finished(
                &run_id,
                status,
                error.as_deref(),
                finished_at,
                queued.as_ref(),
            )
        })
        .await
    {
        // every op of it is recorded and the run's own row is not, so the
        // status this process would report is the one thing nobody can read
        runner.abandon(&run_id);
        return;
    }
    runner.active.lock().unwrap().remove(&run_id);
    // this run's slot is free: wake anything waiting on it, then go and see
    // what the queue can start in its place
    runner.settled.notify_waiters();
    runner.dispatch();

    if queued.is_none() {
        fire_hooks(&runner.run_hooks(job.name()), event, "run");
    }
}

/// what the run records on the row of an op it is handing to a child: the
/// handle it holds for each dep, and what each dep did, under the job's names
/// for them.
///
/// handles rather than values, so an op reading a gigabyte through a
/// [file io manager](crate::FileIo) reads it once, in the process that wants
/// it. seeded deps — a resume's reused output, an asset build's memoized value
/// — are here too, and they are the reason this exists at all: everything else
/// an op invocation needs is already a row the child can find on its own.
/// `isolate::handed_over` is what reads it back.
fn invocation(held: serde_json::Map<String, Value>, deps: serde_json::Map<String, Value>) -> Value {
    json!({ "held": held, "deps": deps })
}

// an instance runs its parent's op under its own name; everything else is
// itself
fn unit_op<'a>(job: &'a Job, instances: &HashMap<String, Instance>, name: &str) -> &'a Op {
    let op = instances.get(name).map_or(name, |i| i.parent.as_str());
    job.op(op)
        .expect("every unit is an op of the job or an instance of one")
}

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// what an op's trigger rule says about the statuses its deps reached: `Ok`
/// to run it, `Err(reason)` to skip it with that reason recorded. the reason
/// names the rule, so a rule-declined skip never reads like the
/// upstream-failure skip that [`skip_downstream`] writes.
fn admits(op: &Op, statuses: &HashMap<String, OpStatus>) -> Result<(), &'static str> {
    let all_succeeded = op
        .deps()
        .iter()
        .all(|d| statuses.get(d) == Some(&OpStatus::Success));
    match op.runs_when() {
        When::AllSucceeded if !all_succeeded => {
            Err("skipped by rule all_succeeded: a dep did not succeed")
        }
        // an op with no deps has nothing that could have failed
        When::AnyFailed if all_succeeded => Err("skipped by rule any_failed: every dep succeeded"),
        _ => Ok(()),
    }
}

// an instance's output goes into its parent's slot; the mapped op's own
// output appears, in element order, once every instance has landed
#[allow(clippy::too_many_arguments)]
async fn collect(
    name: String,
    handle: Value,
    io: &Io,
    job: &Job,
    run_id: &str,
    instances: &HashMap<String, Instance>,
    fanouts: &mut HashMap<String, Fanout>,
    outputs: &mut HashMap<String, Value>,
    statuses: &mut HashMap<String, OpStatus>,
) {
    statuses.insert(name.clone(), OpStatus::Success);
    let Some(instance) = instances.get(&name) else {
        outputs.insert(name, handle);
        return;
    };
    let fan = fanouts
        .get_mut(&instance.parent)
        .expect("an instance belongs to a live fan-out");
    fan.slots[instance.index] = Some(handle);
    fan.remaining -= 1;
    if fan.remaining == 0 && !fan.failed {
        // the collected array is a value, not a handle: a mapped op has no
        // row and never put anything of its own, so the instances' handles
        // are resolved here rather than left for downstream to puzzle over
        let named = job.op(&instance.parent).and_then(Op::io_name);
        let mut collected: Vec<Value> = Vec::with_capacity(fan.slots.len());
        for (index, slot) in std::mem::take(&mut fan.slots).into_iter().enumerate() {
            let handle = slot.expect("every instance filled its slot");
            let op = fan.names[index].clone();
            let key = IoKey {
                run_id: run_id.to_string(),
                job: job.name().to_string(),
                op: op.clone(),
            };
            match crate::io::get(io, named, key, handle).await {
                Ok(v) => collected.push(v),
                Err(e) => {
                    // the whole fan-out is unusable, and saying so beats
                    // handing downstream an array with a hole in it
                    tracing::warn!(
                        run = %run_id, op = %op, "instance output unreadable: {e}"
                    );
                    fan.failed = true;
                    return;
                }
            }
        }
        outputs.insert(instance.parent.clone(), Value::Array(collected));
        statuses.insert(instance.parent.clone(), OpStatus::Success);
    }
}

fn io_key(run_id: &str, job: &Job, op: &str) -> IoKey {
    IoKey {
        run_id: run_id.to_string(),
        job: job.name().to_string(),
        op: op.to_string(),
    }
}

/// turn what the run holds for `op` back into a value. every input an op
/// receives comes through here, whether it was produced by this run, seeded
/// from a resume, or memoized by an asset build — which is why a manager's
/// `get` has to pass through anything it did not write.
async fn resolve(io: &Io, job: &Job, run_id: &str, op: &str, held: Value) -> Result<Value, String> {
    let name = job.op(op).and_then(Op::io_name);
    crate::io::get(io, name, io_key(run_id, job, op), held)
        .await
        .map_err(|e| e.to_string())
}

// a failed unit skips its downstream. one failing instance fails its whole
// mapped op — there is no partial array — and the siblings still in flight
// run to the end, exactly as an op's siblings do
#[allow(clippy::too_many_arguments)]
async fn give_up(
    name: &str,
    instances: &HashMap<String, Instance>,
    fanouts: &mut HashMap<String, Fanout>,
    job: &Job,
    pairs: &[(String, Vec<String>)],
    pending: &mut Vec<String>,
    statuses: &mut HashMap<String, OpStatus>,
    run_id: &str,
    store: &Store,
) -> bool {
    statuses.insert(name.to_string(), OpStatus::Failed);
    let Some(instance) = instances.get(name) else {
        let reason = format!("skipped: upstream {name} failed");
        return skip_downstream(job, pairs, name, &reason, pending, statuses, run_id, store).await;
    };
    let fan = fanouts
        .get_mut(&instance.parent)
        .expect("an instance belongs to a live fan-out");
    fan.remaining -= 1;
    // the first failure is what fails the mapped op and skips downstream;
    // later ones just close a slot
    if fan.failed {
        return true;
    }
    fan.failed = true;
    let parent = instance.parent.clone();
    statuses.insert(parent.clone(), OpStatus::Failed);
    let reason = format!("skipped: upstream {parent} failed");
    skip_downstream(
        job, pairs, &parent, &reason, pending, statuses, run_id, store,
    )
    .await
}

async fn op_canceled(store: &Store, run_id: &str, name: &str) -> bool {
    let landed = store
        .landed("op_finished", || {
            store.op_finished(
                run_id,
                name,
                OpStatus::Canceled,
                None,
                None,
                Some("canceled"),
                &[],
            )
        })
        .await;
    note(store.append_event(
        run_id,
        Some(name),
        EventLevel::Warn,
        EventKind::OpCanceled,
        "canceled",
        Some(&json!({ "reason": "canceled", "stopped": true })),
    ));
    landed
}

/// canceled, and stopped: an [isolated](Op::isolated) op's process was
/// signalled, killed and reaped, so this row gets a real finish time. that is
/// the difference the subprocess buys — everywhere else hestan can only record
/// what it asked for.
async fn op_killed(store: &Store, run_id: &str, name: &str, msg: &str) -> bool {
    note(store.append_event(
        run_id,
        Some(name),
        EventLevel::Warn,
        EventKind::OpCanceled,
        msg,
        // the process was signalled, killed and reaped, so this one is a fact
        // about the work having stopped rather than about having asked it to
        Some(&json!({ "reason": msg, "stopped": true })),
    ));
    store
        .landed("op_finished", || {
            store.op_finished(run_id, name, OpStatus::Canceled, None, None, Some(msg), &[])
        })
        .await
}

// canceled, but only the request is a fact: the op never joined, so it gets no
// finish time and an error that says exactly that
async fn op_unstopped(store: &Store, run_id: &str, name: &str, grace: Duration) -> bool {
    let msg = format!(
        "cancellation requested; this op was not observed to stop within {grace:?} \
         and may still be running (blocking work stops only if it polls ctx.is_cancelled())"
    );
    note(store.append_event(
        run_id,
        Some(name),
        EventLevel::Warn,
        EventKind::OpCanceled,
        &msg,
        // `stopped` is the whole difference between this and the two above:
        // the request is the fact, and whether the work stopped is not known
        Some(&json!({ "reason": &msg, "stopped": false })),
    ));
    store
        .landed("op_unstopped", || store.op_unstopped(run_id, name, &msg))
        .await
}

#[allow(clippy::too_many_arguments)]
async fn run_op(
    op: Op,
    // the op's own name, except on a fan-out instance, which runs its parent's
    // body under `{parent}[{i}]` and is recorded under that everywhere
    name: String,
    element: Option<Value>,
    job: String,
    run_id: String,
    params: Value,
    scheduled_for: Option<DateTime<Utc>>,
    inputs: Arc<HashMap<String, Value>>,
    dep_statuses: Arc<HashMap<String, OpStatus>>,
    // what an isolated op's child is to read instead of `inputs`; `None` for
    // every op that runs in this process
    invocation: Option<Value>,
    resources: Resources,
    store: Store,
    pools: Pools,
    hooks: Arc<Vec<OpHook>>,
    cancel: watch::Receiver<bool>,
    // the run this op belongs to, as a span. every attempt opens a child of it,
    // which is what makes a run a tree rather than a pile of unrelated spans —
    // `tokio::spawn` carries no span into the task it starts, so it is passed
    // rather than inherited
    run_span: tracing::Span,
) -> OpOutcome {
    // loaded once, before attempt 1: every retry sees the same starting state
    let state = Arc::new(match store.op_state(&job, &name) {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(job = %job, op = %name, "state read failed: {e}");
            None
        }
    });
    let mut attempt = 1;
    if !store
        .landed("op_started", || store.op_started(&run_id, &name, attempt))
        .await
    {
        return (name, Outcome::Unrecorded);
    }
    note(store.append_event(
        &run_id,
        Some(&name),
        EventLevel::Info,
        EventKind::OpStarted,
        "starting",
        Some(&json!({ "attempt": attempt })),
    ));
    // Runner::with_pools refuses this at build time; a Runner assembled without
    // pools can still reach it, and running unlimited would quietly break the
    // very promise the pool was declared to keep
    let pool = match op.pool_name() {
        None => None,
        Some(pool) => match pools.get(pool) {
            Some(p) => Some((pool.to_string(), p.sem.clone())),
            None => {
                let msg = format!("op takes from pool {pool}, which is not declared");
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Error,
                    EventKind::OpFailed,
                    &msg,
                    Some(&json!({ "error": &msg })),
                ));
                return (name, Outcome::Failed(msg));
            }
        },
    };
    loop {
        // one span per attempt, and a retry gets its own rather than a second
        // annotation on the first: two attempts of an op are two spans of
        // different lengths and that is what a waterfall has to show. it is
        // also the span `capture_layer` reads its three fields off, and — for
        // an isolated op — the span whose context the child is handed.
        //
        // costs nothing when nothing is subscribed, which is the ordinary case.
        let span = tracing::info_span!(
            parent: &run_span,
            "hestan.op",
            run_id = %run_id,
            op = %name,
            attempt
        );
        // fresh buffers per attempt: a failed attempt's staged state,
        // metadata and asset builds must not leak into the one that works
        let new_state = Arc::new(Mutex::new(None));
        let new_meta: MetaBuf = Arc::new(Mutex::new(BTreeMap::new()));
        let built: op::BuiltBuf = Arc::new(Mutex::new(Vec::new()));
        // one more stop signal per attempt, flipped by this attempt's timeout;
        // the run's own cancel channel is the other half
        let (expired, on_expiry) = watch::channel(false);
        // this attempt's own start, which on a retry is later than the one on
        // the op run row: that column keeps the first attempt's. before the
        // pool, so an attempt that queued for a slot reports how long it
        // really took to get anywhere
        let began = Utc::now();
        // admitted before anything of the attempt runs, and handed to the ctx
        // rather than kept on this stack: an abort takes the stack and leaves
        // blocking work the body started, so a slot released with the stack is
        // released while the thing it admitted is still calling the api the
        // pool was declared to protect. see `op::Slot`
        let slot: Option<op::Slot> = match &pool {
            None => None,
            Some((pool, sem)) => Some(Arc::new(match sem.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    // otherwise a queued op is just an op sitting in `running`
                    note(store.append_event(
                        &run_id,
                        Some(&name),
                        EventLevel::Info,
                        EventKind::Log,
                        &format!("waiting for a {pool} pool permit"),
                        None,
                    ));
                    sem.clone()
                        .acquire_owned()
                        .await
                        .expect("pool semaphores are never closed")
                }
            })),
        };
        let ctx = OpCtx {
            cancel: Cancel {
                run: cancel.clone(),
                attempt: on_expiry,
            },
            run_id: run_id.clone(),
            job: job.clone(),
            op: name.clone(),
            params: params.clone(),
            scheduled_for,
            element: element.clone(),
            partition: None,
            inputs: inputs.clone(),
            dep_statuses: dep_statuses.clone(),
            resources: resources.clone(),
            state: state.clone(),
            new_state: new_state.clone(),
            new_fingerprint: Arc::new(Mutex::new(None)),
            new_meta: new_meta.clone(),
            new_per_asset: Arc::new(Mutex::new(BTreeMap::new())),
            built: built.clone(),
            store: store.clone(),
            slot: slot.clone(),
        };
        let ended = match &invocation {
            // the body runs in a child, which owns the whole of what an
            // attempt is: its own timeout, its own kill
            Some(invocation) => {
                let ended = isolated(
                    &op, &run_id, &name, attempt, invocation, &store, &cancel, &span,
                )
                .await;
                // the child was the work and it has been watched to stop,
                // so this ctx went nowhere: dropping it here is what keeps
                // a retry backoff off the slot
                drop(ctx);
                ended
            }
            None => {
                // the call sits inside the async block, so a closure that panics
                // before returning its future is caught by the retry policy too.
                //
                // the span is entered across every await of the body, and
                // it is the whole of how a tracing event is attributed to
                // an op: `capture_layer` stores events whose span context
                // carries these three fields and ignores everything else,
                // which is how a library captures its ops' logging without
                // touching the host application's
                let call = AssertUnwindSafe(async { op.call(ctx).await })
                    .catch_unwind()
                    .instrument(span.clone());
                let caught = match op.timeout_after() {
                    None => Ok(call.await),
                    Some(limit) => match tokio::time::timeout(limit, call).await {
                        Ok(caught) => Ok(caught),
                        // dropping the future stops an async op here; a blocking
                        // one only stops if it polls, so flip the flag it polls
                        Err(_) => {
                            let _ = expired.send(true);
                            Err(limit)
                        }
                    },
                };
                match caught {
                    Ok(Ok(Ok(output))) => Ended::Value(output),
                    Ok(Ok(Err(e))) => Ended::Failed(e.to_string()),
                    // as_ref, not &: &Box<dyn Any> would downcast against the box
                    Ok(Err(panic)) => Ended::Failed(match panic_payload(panic.as_ref()) {
                        Some(s) => format!("op panicked: {s}"),
                        None => "op panicked".to_string(),
                    }),
                    Err(limit) => Ended::Failed(format!("timed out after {limit:?}")),
                }
            }
        };
        // this attempt is over as far as this task is concerned, so let go of
        // the slot: a retry never sits on the resource it backed off from. the
        // slot is only free once the body's ctx has gone too, which is what
        // holds it for blocking work that outlived the abort
        drop(slot);
        // one event per attempt however it went, and before the retry policy
        // has had its say: three attempts of an op that worked on the third is
        // three facts, and a hook that wants the last one filters on `status`
        let told = |status, error: Option<&str>| {
            fire_hooks(
                &hooks,
                OpEvent {
                    run_id: run_id.clone(),
                    job: job.clone(),
                    op: name.clone(),
                    attempt,
                    status,
                    error: error.map(str::to_string),
                    started_at: began,
                    finished_at: Utc::now(),
                    duration: (Utc::now() - began).to_std().unwrap_or_default(),
                },
                "op",
            );
        };
        let msg = match ended {
            Ended::Value(output) => {
                // the same tagged map the op run and any materialization of
                // this build carry, so a reader following the log alone sees
                // the rows and the bytes without going back for the op run
                let meta = op::staged_meta(&new_meta);
                let data = json!({
                    "attempt": attempt,
                    "output_type": op.output_type(),
                    "meta": meta,
                });
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Info,
                    EventKind::OpSuccess,
                    "finished",
                    Some(&data),
                ));
                told(OpStatus::Success, None);
                return (
                    name,
                    Outcome::Produced {
                        output,
                        state: new_state.lock().unwrap().take(),
                        meta,
                        // what the body says it built, which is not yet a fact
                        // about anything: the run writes it when the output is
                        // stored and the op run says success
                        built: op::staged_builds(&built),
                    },
                );
            }
            // the child recorded its own success, event and all: nothing this
            // process staged applies, because nothing here ran the body
            Ended::Handle(handle) => {
                told(OpStatus::Success, None);
                return (name, Outcome::Recorded(handle));
            }
            Ended::Killed(msg) => {
                told(OpStatus::Canceled, Some(&msg));
                return (name, Outcome::Killed(msg));
            }
            Ended::Failed(msg) => {
                told(OpStatus::Failed, Some(&msg));
                msg
            }
        };
        // retries are extra attempts after the first
        let retrying = attempt <= op.max_retries();
        let kind = if retrying {
            EventKind::OpRetry
        } else {
            EventKind::OpFailed
        };
        // both say which attempt and what went wrong: a retry that hid the
        // error left the run page with "attempt 1 failed" and nowhere to look
        let data = json!({ "attempt": attempt, "error": msg });
        note(store.append_event(
            &run_id,
            Some(&name),
            EventLevel::Error,
            kind,
            &format!("attempt {attempt} failed: {msg}"),
            Some(&data),
        ));
        if !retrying {
            return (name, Outcome::Failed(msg));
        }
        // an in-process op's retry sleep dies with the abort a cancel sends.
        // an isolated op is not aborted — it is trusted to stop its own child —
        // so it watches for the cancel itself rather than making a canceled run
        // wait out a backoff nobody wants the end of
        let waited = tokio::time::sleep(op.delay(attempt));
        if op.is_isolated() {
            tokio::pin!(waited);
            tokio::select! {
                () = &mut waited => {}
                () = op::flipped(cancel.clone()) => {
                    return (name, Outcome::Killed("canceled while waiting to retry".to_string()));
                }
            }
        } else {
            waited.await;
        }
        attempt += 1;
        // a fresh attempt of an isolated op is a fresh child process
        if !store
            .landed("op_started", || store.op_started(&run_id, &name, attempt))
            .await
        {
            return (name, Outcome::Unrecorded);
        }
    }
}

/// run one attempt in a child process.
///
/// off unix there is no such thing, and the job build says so long before a run
/// could reach this — see `validate_isolated`.
#[allow(clippy::too_many_arguments)]
async fn isolated(
    op: &Op,
    run_id: &str,
    name: &str,
    // which attempt this is, because what the child prints is stored under it
    attempt: u32,
    invocation: &Value,
    store: &Store,
    cancel: &watch::Receiver<bool>,
    // this attempt's span. the child is handed its trace context, which is the
    // only way a subprocess's spans can nest under the op that spawned it
    span: &tracing::Span,
) -> Ended {
    #[cfg(unix)]
    {
        crate::isolate::attempt(op, run_id, name, attempt, invocation, store, cancel, span).await
    }
    #[cfg(not(unix))]
    {
        let _ = (op, run_id, attempt, invocation, store, cancel, span);
        Ended::Failed(format!(
            "op {name} is isolated, which hestan supports on unix only"
        ))
    }
}

pub(crate) fn panic_payload(panic: &(dyn std::any::Any + Send)) -> Option<&str> {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
}

/// mark what `root` not succeeding cuts off, and say whether every row of it
/// landed.
/// propagation asks each candidate's
/// [trigger rule](When) instead of assuming: an op that would still run is not
/// skipped, and neither is anything hanging off it, which waits on what that
/// op does rather than on what happened above it. everything reached through
/// plain [`When::AllSucceeded`] ops is skipped as one, naming `root` — that is
/// one failure with one cause, not a chain of them.
#[allow(clippy::too_many_arguments)]
async fn skip_downstream(
    job: &Job,
    pairs: &[(String, Vec<String>)],
    root: &str,
    reason: &str,
    pending: &mut Vec<String>,
    statuses: &mut HashMap<String, OpStatus>,
    run_id: &str,
    store: &Store,
) -> bool {
    let down = graph::downstream_through(pairs, root, |n| {
        job.op(n)
            .is_none_or(|o| o.runs_when() == When::AllSucceeded)
    });
    let mut i = 0;
    while i < pending.len() {
        if down.contains(&pending[i]) {
            let name = pending.remove(i);
            // event first, like every other terminal transition
            note(store.append_event(
                run_id,
                Some(&name),
                EventLevel::Warn,
                EventKind::OpSkipped,
                reason,
                Some(&json!({ "reason": reason, "upstream": root })),
            ));
            if !store
                .landed("op_finished", || {
                    store.op_finished(run_id, &name, OpStatus::Skipped, None, None, None, &[])
                })
                .await
            {
                return false;
            }
            statuses.insert(name, OpStatus::Skipped);
        } else {
            i += 1;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::Op;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn sleepy_job(name: &str, ms: u64) -> Job {
        Job::builder(name)
            .op(Op::new("nap", move |_| async move {
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(json!(null))
            }))
            .build()
            .unwrap()
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

    // which of the two you got used to depend on the order they were handed
    // over, and an orchestration definition does not get to be ambiguous about
    // what a name means
    #[test]
    fn two_jobs_under_one_name_are_refused_rather_than_ranked() {
        let err = Runner::new(
            [sleepy_job("etl", 1), sleepy_job("etl", 2)],
            Store::open(":memory:").unwrap(),
        )
        .err()
        .unwrap();
        assert!(
            matches!(err, Error::DuplicateJob(ref name) if name == "etl"),
            "{err}"
        );

        // and every constructor above it answers the same way, because they
        // are all the same registration
        let err = Runner::with_pools(
            [sleepy_job("etl", 1), sleepy_job("etl", 2)],
            Store::open(":memory:").unwrap(),
            Vec::new(),
            [("api".to_string(), 2)],
        )
        .err()
        .unwrap();
        assert!(matches!(err, Error::DuplicateJob(_)), "{err}");

        // two names, no argument
        assert!(
            Runner::new(
                [sleepy_job("etl", 1), sleepy_job("report", 2)],
                Store::open(":memory:").unwrap(),
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn cancel_registry_registers_at_launch_and_drains() {
        let runner = Runner::new(
            [sleepy_job("slow", 30_000)],
            Store::open(":memory:").unwrap(),
        )
        .unwrap();
        let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
        assert_eq!(runner.active.lock().unwrap().len(), 1);
        assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
        assert_eq!(wait_terminal(&runner, &id).await, RunStatus::Canceled);
        assert!(runner.active.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cancel_mid_run_clears_registry() {
        let runner = Runner::new(
            [sleepy_job("slow", 30_000)],
            Store::open(":memory:").unwrap(),
        )
        .unwrap();
        let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
        for _ in 0..300 {
            if runner.store().run(&id).unwrap().unwrap().status == RunStatus::Running {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
        assert_eq!(wait_terminal(&runner, &id).await, RunStatus::Canceled);
        assert!(runner.active.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_validation_prepare_leaves_registry_empty() {
        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        struct Gate {
            threshold: u32,
        }
        let job = Job::builder("gated")
            .op(Op::new("check", |_| async { Ok(json!(null)) }).params::<Gate>())
            .build()
            .unwrap();
        let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
        let err = runner
            .launch("gated", json!({"threshold": "high"}), Trigger::Manual)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParams { .. }), "{err}");
        assert!(runner.active.lock().unwrap().is_empty());
        assert!(
            runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_create_run_cleans_registry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap();
        let runner = Runner::new([sleepy_job("slow", 1)], Store::open(path).unwrap()).unwrap();
        // sabotage the insert out from under the store
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("DROP TABLE runs").unwrap();
        assert!(runner.launch("slow", json!({}), Trigger::Manual).is_err());
        assert!(runner.active.lock().unwrap().is_empty());
    }

    fn abc_job() -> Job {
        Job::builder("abc")
            .op(Op::new("a", |_| async { Ok(json!(1)) }))
            .op(Op::new("b", |ctx| async move {
                Ok(json!(ctx.input("a").unwrap().as_i64().unwrap() * 10))
            })
            .after(["a"]))
            .op(Op::new("c", |ctx| async move {
                Ok(json!(ctx.input("b").unwrap().as_i64().unwrap() + 1))
            })
            .after(["b"]))
            .build()
            .unwrap()
    }

    // the child opens the database by path, so a database with no path is one
    // it would open empty — refused where every other undeliverable promise is
    #[tokio::test]
    async fn an_isolated_op_needs_a_database_a_child_could_open() {
        let job = Job::builder("iso")
            .op(Op::new("risky", |_| async { Ok(json!(null)) }).isolated())
            .build()
            .unwrap();
        let refused = Runner::with_pools(
            [job.clone()],
            Store::open(":memory:").unwrap(),
            Vec::new(),
            [],
        );
        let Err(err) = refused else {
            panic!("an isolated op was accepted against an in-memory database");
        };
        assert!(matches!(err, Error::Graph(_)), "{err}");
        assert!(err.to_string().contains("private to this one"), "{err}");

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let store = Store::open(path.to_str().unwrap()).unwrap();
        Runner::with_pools([job], store, Vec::new(), []).expect("a file-backed store is reachable");
    }

    #[tokio::test]
    async fn subset_rejects_unsatisfied_deps() {
        let runner = Runner::new([abc_job()], Store::open(":memory:").unwrap()).unwrap();
        let err = runner
            .launch_subset(
                "abc",
                HashSet::from(["b".into()]),
                HashMap::new(),
                json!({}),
                Trigger::Manual,
                None,
                RunTags::new(),
                None,
            )
            .unwrap_err();
        assert!(matches!(err, Error::Graph(_)), "{err}");
        assert!(err.to_string().contains("neither in the subset nor seeded"));

        let err = runner
            .launch_subset(
                "abc",
                HashSet::from(["ghost".into()]),
                HashMap::new(),
                json!({}),
                Trigger::Manual,
                None,
                RunTags::new(),
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("not an op of the job"), "{err}");

        let err = runner
            .launch_subset(
                "abc",
                HashSet::from(["a".into(), "b".into()]),
                HashMap::from([("a".into(), json!(1))]),
                json!({}),
                Trigger::Manual,
                None,
                RunTags::new(),
                None,
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("both in the subset and seeded"),
            "{err}"
        );

        assert!(
            runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn subset_runs_only_its_ops_with_seeded_inputs() {
        let runner = Runner::new([abc_job()], Store::open(":memory:").unwrap()).unwrap();
        let run = runner
            .run_subset(
                "abc",
                HashSet::from(["b".into(), "c".into()]),
                HashMap::from([("a".into(), json!(5))]),
                json!({}),
                Trigger::Manual,
                RunTags::new(),
            )
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        let ops = runner.store().op_runs(&run.id).unwrap();
        let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
        assert_eq!(names, ["b", "c"]);
        assert_eq!(ops[0].output, Some(json!(50)));
        assert_eq!(ops[1].output, Some(json!(51)));
    }

    // ------------------------------------------------------------- the queue

    /// a job whose op announces itself and then waits for `gate`, so a test can
    /// hold runs open and see exactly which ones the dispatcher started.
    fn gated(name: &str, gate: Arc<AtomicBool>, started: Arc<Mutex<Vec<String>>>) -> Job {
        Job::builder(name)
            .op(Op::new("work", move |ctx: crate::op::OpCtx| {
                let gate = gate.clone();
                let started = started.clone();
                let who = ctx
                    .params()
                    .get("who")
                    .and_then(Value::as_str)
                    .unwrap_or("?")
                    .to_string();
                async move {
                    started.lock().unwrap().push(who);
                    while !gate.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                    Ok(json!(null))
                }
            }))
            .build()
            .unwrap()
    }

    fn who(n: &str) -> Value {
        json!({ "who": n })
    }

    async fn until(what: &str, mut held: impl FnMut() -> bool) {
        for _ in 0..1_000 {
            if held() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("{what} never happened");
    }

    /// what the ops of these tests have announced, in order.
    fn order(started: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        started.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn a_global_limit_holds_and_the_queue_drains_as_runs_finish() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [gated("etl", gate.clone(), started.clone())],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new().global(1), 0);

        let first = runner.launch("etl", who("a"), Trigger::Manual).unwrap();
        let second = runner.launch("etl", who("b"), Trigger::Manual).unwrap();
        until("the first run started", || order(&started) == ["a"]).await;

        // the second is on the queue, unclaimed, and stays there: a limit is
        // about what is executing, and one thing is
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(order(&started), ["a"]);
        let waiting = runner.store().run(&second).unwrap().unwrap();
        assert_eq!(waiting.status, RunStatus::Queued);
        assert_eq!(waiting.claimed_by, None);
        assert_eq!(
            runner.store().run(&first).unwrap().unwrap().claimed_by,
            Some(runner.instance().to_string())
        );

        gate.store(true, Ordering::SeqCst);
        until("the queue drained", || order(&started) == ["a", "b"]).await;
        assert_eq!(wait_terminal(&runner, &second).await, RunStatus::Success);
        assert_eq!(runner.queue_depth().unwrap(), 0);
    }

    #[tokio::test]
    async fn a_per_job_limit_holds_that_job_and_nothing_else() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        // the limit declared on the job itself, which is where the readme
        // always said it would go
        let one_at_a_time = Job::builder("etl")
            .op(gated("etl", gate.clone(), started.clone()).ops()[0].clone())
            .max_concurrent_runs(1)
            .build()
            .unwrap();
        let runner = Runner::new(
            [
                one_at_a_time,
                gated("reports", gate.clone(), started.clone()),
            ],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new(), 0);

        runner.launch("etl", who("etl-1"), Trigger::Manual).unwrap();
        let held = runner.launch("etl", who("etl-2"), Trigger::Manual).unwrap();
        runner
            .launch("reports", who("reports-1"), Trigger::Manual)
            .unwrap();
        runner
            .launch("reports", who("reports-2"), Trigger::Manual)
            .unwrap();

        until("the unlimited job ran both", || {
            let seen = order(&started);
            seen.contains(&"reports-1".to_string()) && seen.contains(&"reports-2".to_string())
        })
        .await;
        let seen = order(&started);
        assert!(seen.contains(&"etl-1".to_string()));
        assert!(!seen.contains(&"etl-2".to_string()), "{seen:?}");
        let blocked = runner.queue(10).unwrap();
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].run.id, held);
        assert_eq!(blocked[0].position, 1);
        assert_eq!(blocked[0].blocked.as_ref().unwrap().scope(), "job");

        gate.store(true, Ordering::SeqCst);
        until("the held run started", || {
            order(&started).contains(&"etl-2".to_string())
        })
        .await;
    }

    #[tokio::test]
    async fn a_tag_limit_holds_across_jobs() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [
                gated("etl", gate.clone(), started.clone()),
                gated("reports", gate.clone(), started.clone()),
            ],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new().tag("env", "prod", 1), 0);

        let prod = || RunTags::from([("env".to_string(), "prod".to_string())]);
        runner
            .launch_tagged("etl", who("prod-etl"), Trigger::Manual, prod())
            .unwrap();
        let held = runner
            .launch_tagged("reports", who("prod-reports"), Trigger::Manual, prod())
            .unwrap();
        runner
            .launch_tagged(
                "reports",
                who("staging"),
                Trigger::Manual,
                RunTags::from([("env".to_string(), "staging".to_string())]),
            )
            .unwrap();

        // the tag is what is scarce, not the job: one prod run of either job
        until("the untagged pair ran", || {
            order(&started).contains(&"staging".to_string())
        })
        .await;
        let seen = order(&started);
        assert!(seen.contains(&"prod-etl".to_string()));
        assert!(!seen.contains(&"prod-reports".to_string()), "{seen:?}");
        let blocked = runner.queue(10).unwrap();
        assert_eq!(blocked[0].run.id, held);
        let why = blocked[0].blocked.as_ref().unwrap();
        assert_eq!(why.scope(), "tag");
        assert!(why.reason().contains("env:prod"), "{}", why.reason());
    }

    #[tokio::test]
    async fn priority_decides_which_queued_run_starts_next() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [gated("etl", gate.clone(), started.clone())],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new().global(1), 0);

        runner.launch("etl", who("first"), Trigger::Manual).unwrap();
        until("the first run started", || order(&started) == ["first"]).await;
        // enqueued oldest first, and deliberately not in priority order
        for (name, priority) in [("low", -1), ("high", 10), ("middle", 1)] {
            runner
                .launch_prioritized(
                    "etl",
                    who(name),
                    Trigger::Manual,
                    RunTags::new(),
                    Some(priority),
                )
                .unwrap();
        }
        assert_eq!(runner.queue_depth().unwrap(), 3);
        // and the queue is already in the order they will be taken
        let queue = runner.queue(10).unwrap();
        let waiting: Vec<&str> = queue
            .iter()
            .map(|q| q.run.params["who"].as_str().unwrap())
            .collect();
        assert_eq!(waiting, ["high", "middle", "low"]);

        gate.store(true, Ordering::SeqCst);
        until("the queue drained", || order(&started).len() == 4).await;
        assert_eq!(order(&started), ["first", "high", "middle", "low"]);
    }

    // head-of-line blocking would be worse, so the dispatcher skips a run a
    // limit holds back and starts the next one that fits — which is exactly why
    // priority is a preference and not an order
    #[tokio::test]
    async fn a_blocked_run_does_not_hold_up_a_lower_priority_one_behind_it() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [
                gated("etl", gate.clone(), started.clone()),
                gated("reports", gate.clone(), started.clone()),
            ],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new().job("etl", 1), 0);

        runner.launch("etl", who("etl-1"), Trigger::Manual).unwrap();
        until("the first run started", || order(&started) == ["etl-1"]).await;
        let blocked = runner
            .launch_prioritized(
                "etl",
                who("etl-2"),
                Trigger::Manual,
                RunTags::new(),
                Some(9),
            )
            .unwrap();
        runner
            .launch_prioritized(
                "reports",
                who("reports"),
                Trigger::Manual,
                RunTags::new(),
                Some(0),
            )
            .unwrap();

        until("the lower-priority run started", || {
            order(&started).contains(&"reports".to_string())
        })
        .await;
        // the higher-priority one is still waiting, and the queue says why
        let queue = runner.queue(10).unwrap();
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].run.id, blocked);
        assert_eq!(queue[0].blocked.as_ref().unwrap().scope(), "job");
        gate.store(true, Ordering::SeqCst);
        until("the blocked run started", || {
            order(&started).contains(&"etl-2".to_string())
        })
        .await;
    }

    // the limits are read at the top of every pass, so raising one drains the
    // queue it was holding back without a restart and without a run finishing
    #[tokio::test]
    async fn raising_a_limit_starts_the_queue_without_a_restart() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [gated("etl", gate.clone(), started.clone())],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new().global(1), 0);

        runner.launch("etl", who("a"), Trigger::Manual).unwrap();
        runner.launch("etl", who("b"), Trigger::Manual).unwrap();
        until("the first run started", || order(&started) == ["a"]).await;
        assert_eq!(runner.queue_depth().unwrap(), 1);
        assert_eq!(runner.limits().global_limit(), Some(1));

        runner.set_limits(Limits::new().global(2));
        until("the queued run started", || order(&started) == ["a", "b"]).await;
        // nothing finished to make room: the limit moved, and the queue noticed
        assert_eq!(
            runner
                .store()
                .run(
                    &runner
                        .store()
                        .runs(None, None, None, None, None, 10)
                        .unwrap()[1]
                        .id
                )
                .unwrap()
                .unwrap()
                .status,
            RunStatus::Running
        );
    }

    // a run bumped up the queue is taken next, and one nobody can bump any more
    // says so rather than answering as if it had moved
    #[tokio::test]
    async fn a_queued_runs_priority_can_be_changed_until_it_is_claimed() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [gated("etl", gate.clone(), started.clone())],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_limits(Limits::new().global(1), 0);

        let running = runner.launch("etl", who("a"), Trigger::Manual).unwrap();
        until("the first run started", || order(&started) == ["a"]).await;
        runner.launch("etl", who("b"), Trigger::Manual).unwrap();
        let last = runner.launch("etl", who("c"), Trigger::Manual).unwrap();

        assert!(runner.set_priority(&last, 5).unwrap());
        let queue = runner.queue(10).unwrap();
        assert_eq!(queue[0].run.id, last);

        // one already claimed has spent its priority, and one that never
        // existed is not the same mistake
        let err = runner.set_priority(&running, 5).unwrap_err();
        assert!(matches!(err, Error::RunActive(_)), "{err}");
        assert!(!runner.set_priority("nope", 5).unwrap());

        gate.store(true, Ordering::SeqCst);
        until("the queue drained", || order(&started).len() == 3).await;
        assert_eq!(order(&started), ["a", "c", "b"]);
    }

    // ---------------------------------------------------- claims and leases

    /// a queued run planted straight into a store, as a launch in some other
    /// process would have left it.
    fn plant_queued(store: &Store, id: &str, job: &str) {
        let run = Run {
            id: id.to_string(),
            job: job.to_string(),
            status: RunStatus::Queued,
            trigger: Trigger::Manual,
            params: json!({}),
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: None,
            scheduled_for: None,
            tags: RunTags::new(),
            priority: 0,
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
            actor: None,
        };
        store.create_run(&run, &["work".to_string()]).unwrap();
    }

    fn file_store(dir: &tempfile::TempDir) -> (String, Store) {
        let path = dir.path().join("hestan.db").display().to_string();
        let store = Store::open(&path).unwrap();
        (path, store)
    }

    // the claim is a compare-and-set, so two claimers reaching for one run is
    // not a race that has to be avoided — it is one that resolves
    #[tokio::test]
    async fn two_claimers_race_one_run_and_exactly_one_wins() {
        let dir = tempfile::tempdir().unwrap();
        let (path, store) = file_store(&dir);
        plant_queued(&store, "contested", "etl");
        let defined = HashSet::from(["etl".to_string()]);

        let both: Vec<_> = ["alpha", "beta"]
            .map(|claimer| {
                let path = path.clone();
                let defined = defined.clone();
                tokio::task::spawn_blocking(move || {
                    let store = Store::open(&path).unwrap();
                    store
                        .claim_next(claimer, LEASE, &Limits::new(), &defined)
                        .unwrap()
                        .map(|(run, _)| (claimer, run.id))
                })
            })
            .into_iter()
            .collect();
        let mut won = Vec::new();
        for handle in both {
            if let Some(claim) = handle.await.unwrap() {
                won.push(claim);
            }
        }

        assert_eq!(won.len(), 1, "the same run was claimed twice: {won:?}");
        assert_eq!(won[0].1, "contested");
        let row = store.run("contested").unwrap().unwrap();
        assert_eq!(row.claimed_by.as_deref(), Some(won[0].0));
        assert!(row.lease_until.is_some());
        // and it is off the queue for good
        assert_eq!(store.queue_depth().unwrap(), 0);
    }

    // the review finding, asserted as the thing it is: process B booting must
    // not touch process A's in-flight work
    #[tokio::test]
    async fn a_live_lease_survives_another_processs_boot_and_an_expired_one_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        plant_queued(&store, "live", "etl");
        plant_queued(&store, "stranded", "etl");
        // both are being executed by processes that are not this one; one is
        // still saying so, the other stopped a while ago
        store
            .plant_claim(
                "live",
                "other-process",
                Some(Utc::now() + chrono::Duration::seconds(45)),
            )
            .unwrap();
        store
            .plant_claim(
                "stranded",
                "dead-process",
                Some(Utc::now() - chrono::Duration::seconds(90)),
            )
            .unwrap();
        for id in ["live", "stranded"] {
            store.run_started(id, Utc::now()).unwrap();
            store.op_started(id, "work", 1).unwrap();
        }

        // this is what booting is: the sweep that used to assume it was alone
        store.fail_interrupted().unwrap();

        let live = store.run("live").unwrap().unwrap();
        assert_eq!(
            live.status,
            RunStatus::Running,
            "a boot elsewhere failed a live process's run: {:?}",
            live.error
        );
        assert_eq!(live.error, None);
        assert_eq!(live.claimed_by.as_deref(), Some("other-process"));
        assert_eq!(
            store.op_run("live", "work").unwrap().unwrap().status,
            OpStatus::Running,
            "a boot elsewhere marked a live process's op as interrupted"
        );
        assert!(
            !store
                .events("live", 0)
                .unwrap()
                .iter()
                .any(|e| e.message.contains("interrupted")),
            "a boot elsewhere announced an interruption on a live run"
        );

        // and the one whose claimer really is gone is swept, exactly as before
        let stranded = store.run("stranded").unwrap().unwrap();
        assert_eq!(stranded.status, RunStatus::Failed);
        assert!(stranded.error.unwrap().contains("interrupted"));
        assert_eq!(
            store.op_run("stranded", "work").unwrap().unwrap().status,
            OpStatus::Failed
        );
    }

    // a queued run nobody has claimed is the queue, not a casualty of whatever
    // restarted, and it has to still be there afterwards
    #[tokio::test]
    async fn a_boot_leaves_the_queue_where_it_found_it() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        plant_queued(&store, "waiting", "etl");

        store.fail_interrupted().unwrap();

        let waiting = store.run("waiting").unwrap().unwrap();
        assert_eq!(waiting.status, RunStatus::Queued, "{:?}", waiting.error);
        assert_eq!(store.queue_depth().unwrap(), 1);
        assert_eq!(
            store.op_run("waiting", "work").unwrap().unwrap().status,
            OpStatus::Pending
        );
    }

    #[tokio::test]
    async fn a_heartbeat_extends_the_lease_it_holds_and_nobody_elses() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        plant_queued(&store, "mine", "etl");
        plant_queued(&store, "theirs", "etl");
        let runner = Runner::new([sleepy_job("etl", 30_000)], store.clone()).unwrap();
        let stale = Utc::now() - chrono::Duration::seconds(5);
        store
            .plant_claim("mine", runner.instance(), Some(stale))
            .unwrap();
        store
            .plant_claim("theirs", "somebody-else", Some(stale))
            .unwrap();

        assert_eq!(
            store.renew_leases(runner.instance(), LEASE, &[]).unwrap(),
            1
        );
        let mine = store.run("mine").unwrap().unwrap();
        assert!(
            mine.lease_until.unwrap() > Utc::now(),
            "the heartbeat did not move the lease"
        );
        assert_eq!(
            store.run("theirs").unwrap().unwrap().lease_until.unwrap(),
            stale,
            "a heartbeat renewed a claim it does not hold"
        );
        assert_eq!(runner.holding().unwrap(), ["mine"]);
    }

    #[tokio::test]
    async fn an_expired_lease_is_failed_with_its_ops_saying_why() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        plant_queued(&store, "stalled", "etl");
        store
            .plant_claim(
                "stalled",
                "vanished",
                Some(Utc::now() - chrono::Duration::seconds(90)),
            )
            .unwrap();
        store.run_started("stalled", Utc::now()).unwrap();
        store.op_started("stalled", "work", 1).unwrap();
        let runner = Runner::new([sleepy_job("etl", 30_000)], store.clone()).unwrap();

        runner.heartbeat();

        let run = store.run("stalled").unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Failed);
        assert!(run.error.as_ref().unwrap().contains("claimer went away"));
        assert_eq!(run.lease_until, None);
        let op = store.op_run("stalled", "work").unwrap().unwrap();
        assert_eq!(op.status, OpStatus::Failed);
        let why = op.error.unwrap();
        assert!(why.contains("claimer went away"), "{why}");
        assert!(why.contains("vanished"), "{why}");
    }

    #[tokio::test]
    async fn an_expired_lease_goes_back_on_the_queue_under_requeue() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        plant_queued(&store, "stalled", "etl");
        store
            .plant_claim(
                "stalled",
                "vanished",
                Some(Utc::now() - chrono::Duration::seconds(90)),
            )
            .unwrap();
        store.run_started("stalled", Utc::now()).unwrap();
        let runner = Runner::new([sleepy_job("etl", 1)], store.clone())
            .unwrap()
            .with_reclaim(Reclaim::Requeue);

        // heartbeat reclaims and then dispatches, so this process picks the run
        // straight back up — which is what requeue is for
        runner.heartbeat();

        let run = store.run("stalled").unwrap().unwrap();
        assert_ne!(run.claimed_by.as_deref(), Some("vanished"));
        assert!(
            matches!(run.status, RunStatus::Queued | RunStatus::Running),
            "a requeued run went terminal: {:?}",
            run.status
        );
        assert!(
            store
                .events("stalled", 0)
                .unwrap()
                .iter()
                .any(|e| e.message.contains("requeued for another claimer"))
        );
        assert_eq!(wait_terminal(&runner, "stalled").await, RunStatus::Success);
    }

    // ------------------------------------------------------------- the roles

    // the split, in one process: what a scheduler leaves behind is a row, and
    // what a worker does with it is everything else
    #[tokio::test]
    async fn a_scheduler_enqueues_and_a_worker_executes() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        let scheduler = Runner::new([sleepy_job("etl", 1)], store.clone())
            .unwrap()
            .with_role(Role::Scheduler, 1);

        let id = scheduler.launch("etl", json!({}), Trigger::Manual).unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let queued = store.run(&id).unwrap().unwrap();
        assert_eq!(
            queued.status,
            RunStatus::Queued,
            "a scheduler executed a run"
        );
        assert_eq!(queued.claimed_by, None);
        assert_eq!(scheduler.queue_depth().unwrap(), 1);

        let worker = Runner::new([sleepy_job("etl", 1)], store.clone())
            .unwrap()
            .with_role(Role::Worker, 4);
        worker.dispatch();
        assert_eq!(wait_terminal(&worker, &id).await, RunStatus::Success);
        assert_eq!(
            store.run(&id).unwrap().unwrap().claimed_by.as_deref(),
            Some(worker.instance())
        );
    }

    // a worker takes what it can run rather than what it can see, which is the
    // whole of why two of them share a queue instead of the first one taking it
    #[tokio::test]
    async fn slots_cap_what_one_process_claims_however_long_the_queue_is() {
        let gate = Arc::new(AtomicBool::new(false));
        let started = Arc::new(Mutex::new(Vec::new()));
        let runner = Runner::new(
            [gated("etl", gate.clone(), started.clone())],
            Store::open(":memory:").unwrap(),
        )
        .unwrap()
        .with_role(Role::Worker, 2);

        for n in ["a", "b", "c", "d", "e"] {
            runner.launch("etl", who(n), Trigger::Manual).unwrap();
        }
        until("the slots filled", || order(&started).len() == 2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(order(&started).len(), 2, "a worker claimed past its slots");
        assert_eq!(runner.queue_depth().unwrap(), 3);

        gate.store(true, Ordering::SeqCst);
        until("the queue drained", || order(&started).len() == 5).await;
    }

    // a subset launch's seeds belong to an earlier run and live nowhere on this
    // one, so the plan is written down — which is what lets a claimer that was
    // not the launcher start it
    #[tokio::test]
    async fn a_subset_launch_records_the_plan_it_will_execute() {
        let runner = Runner::new([abc_job()], Store::open(":memory:").unwrap()).unwrap();
        let run = runner
            .run_subset(
                "abc",
                HashSet::from(["b".into(), "c".into()]),
                HashMap::from([("a".into(), json!(5))]),
                json!({}),
                Trigger::Manual,
                RunTags::new(),
            )
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);

        let job = runner.jobs()["abc"].clone();
        let plan = json!({ "ops": ["b", "c"], "seeds": { "a": 5 } });
        let (pending, seeded) = planned(&job, Some(&plan));
        assert_eq!(pending, ["b", "c"]);
        assert_eq!(seeded["a"], json!(5));
        // and a run of the whole job needs nothing recorded at all
        let (pending, seeded) = planned(&job, None);
        assert_eq!(pending, ["a", "b", "c"]);
        assert!(seeded.is_empty());
    }

    #[tokio::test]
    async fn cancel_registry_empty_after_normal_finish() {
        let runner =
            Runner::new([sleepy_job("quick", 1)], Store::open(":memory:").unwrap()).unwrap();
        let run = runner
            .run("quick", json!({}), Trigger::Manual)
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        assert!(runner.active.lock().unwrap().is_empty());
        assert_eq!(
            runner.cancel(&run.id).unwrap(),
            CancelOutcome::AlreadyFinished
        );
    }

    // a lock under a run is the ordinary case, and the run should not be able
    // to tell: every write it makes goes back for it, and what comes out the
    // other end is the run that happened
    #[tokio::test]
    async fn a_store_that_stumbles_twice_costs_the_run_nothing() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([abc_job()], store.clone()).unwrap();
        store.fail_writes(2);

        let run = runner.run("abc", json!({}), Trigger::Manual).await.unwrap();

        assert_eq!(run.status, RunStatus::Success);
        let ops = store.op_runs(&run.id).unwrap();
        assert_eq!(ops.len(), 3);
        assert!(ops.iter().all(|o| o.status == OpStatus::Success), "{ops:?}");
        // nothing was lost on the way there, which is the difference between
        // a write that was tried again and a write that was let go
        assert_eq!(store.health().unrecorded_writes(), 0);
        assert_eq!(store.health().dropped_writes(), 0);
    }

    /// a job whose op breaks one write on its way out: the body runs, and the
    /// row that would say so is the one thing the store will not take.
    ///
    /// that write and no other, so that a run which carried on regardless
    /// would record its outcome perfectly well — which is exactly what these
    /// cases have to be able to tell apart.
    fn breaks_the_store(store: &Store) -> Job {
        let store = store.clone();
        Job::builder("etl")
            .op(Op::new("work", move |_| {
                let store = store.clone();
                async move {
                    store.fail_writes_to("op_finished", u64::MAX);
                    Ok(json!("done"))
                }
            }))
            .build()
            .unwrap()
    }

    /// wait for this process to give up on the run it cannot record.
    async fn given_up(runner: &Runner, store: &Store) {
        for _ in 0..1_000 {
            if store.health().unrecorded_writes() > 0 && runner.active.lock().unwrap().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the run neither finished nor was given up on");
    }

    // the whole point of the phase: an op that worked, a store that will not
    // say so, and a run that reports nothing rather than reporting success
    #[tokio::test]
    async fn a_run_whose_outcome_cannot_be_written_does_not_claim_one() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        let runner = Runner::new([breaks_the_store(&store)], store.clone()).unwrap();

        let id = runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        given_up(&runner, &store).await;

        // not success, not failure: this process does not know which, and
        // says so by leaving the row where the last write it managed left it
        let run = store.run(&id).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Running);
        assert_eq!(run.finished_at, None);
        assert_eq!(run.error, None);
        let op = store.op_run(&id, "work").unwrap().unwrap();
        assert_eq!(op.status, OpStatus::Running);
        assert_eq!(op.finished_at, None);

        // the claim is still there, which is what a reclaimer needs, and the
        // lease is what will run out: a heartbeat renews everything this
        // process is executing and this is no longer one of them
        assert_eq!(run.claimed_by.as_deref(), Some(runner.instance()));
        let lease = run.lease_until.expect("a claim carries a lease");
        store.fail_writes(0);
        runner.heartbeat();
        assert_eq!(
            store.run(&id).unwrap().unwrap().lease_until,
            Some(lease),
            "an abandoned run's lease was renewed, so nothing could ever reclaim it"
        );
    }

    // and what it is left as is the reclaim policy's decision rather than this
    // process's: the same run, seen by something with a working store, after
    // the lease it stopped renewing has run out
    #[tokio::test]
    async fn a_run_left_for_a_reclaimer_is_settled_by_the_reclaim_policy() {
        for (policy, expected) in [
            (Reclaim::Fail, RunStatus::Failed),
            (Reclaim::Requeue, RunStatus::Queued),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (_, store) = file_store(&dir);
            let runner = Runner::new([breaks_the_store(&store)], store.clone())
                .unwrap()
                .with_reclaim(policy);

            let id = runner.launch("etl", json!({}), Trigger::Manual).unwrap();
            given_up(&runner, &store).await;
            store.fail_writes(0);

            // the lease this process stopped renewing, run out
            store
                .plant_claim(
                    &id,
                    runner.instance(),
                    Some(Utc::now() - chrono::Duration::seconds(90)),
                )
                .unwrap();
            runner.heartbeat();

            let run = store.run(&id).unwrap().unwrap();
            assert_eq!(run.status, expected, "under {policy:?}");
            if policy == Reclaim::Fail {
                let why = run.error.unwrap();
                assert!(why.contains("claimer went away"), "{why}");
                let op = store.op_run(&id, "work").unwrap().unwrap();
                assert_eq!(op.status, OpStatus::Failed);
            }
        }
    }

    // and the same from the other end of an op: a run that cannot record that
    // an op *started* has no row to record it finishing against either, so it
    // stops there rather than running work nothing is keeping the score of
    #[tokio::test]
    async fn an_op_whose_start_cannot_be_written_stops_the_run_before_it_runs() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        let ran = Arc::new(AtomicBool::new(false));
        let watched = ran.clone();
        let job = Job::builder("etl")
            .op(Op::new("work", move |_| {
                let ran = watched.clone();
                async move {
                    ran.store(true, Ordering::SeqCst);
                    Ok(json!(null))
                }
            }))
            .build()
            .unwrap();
        let runner = Runner::new([job], store.clone()).unwrap();
        store.fail_writes_to("op_started", u64::MAX);

        let id = runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        given_up(&runner, &store).await;

        assert!(!ran.load(Ordering::SeqCst), "the op body ran anyway");
        let run = store.run(&id).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Running);
        assert_eq!(run.finished_at, None);
    }

    // a process that cannot record a run has no business claiming another one:
    // a queue draining into something that will not write it down is the
    // failure this phase exists to end, and it is not an improvement on one
    // lost run
    #[tokio::test]
    async fn a_process_whose_store_is_failing_stops_claiming() {
        let dir = tempfile::tempdir().unwrap();
        let (_, store) = file_store(&dir);
        let runner = Runner::new([breaks_the_store(&store)], store.clone()).unwrap();

        let first = runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        given_up(&runner, &store).await;
        assert!(store.health().failing());

        // the queue still works and this process still would, but it does not
        let second = runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        store.fail_writes(0);
        runner.dispatch();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let queued = store.run(&second).unwrap().unwrap();
        assert_eq!(queued.status, RunStatus::Queued);
        assert_eq!(queued.claimed_by, None);
        assert_ne!(first, second);

        // the lease loop is the write that says the store is back, and the
        // queue moves again on the strength of it
        runner.heartbeat();
        assert!(!store.health().failing());
        runner.dispatch();
        for _ in 0..1_000 {
            if store.run(&second).unwrap().unwrap().claimed_by.is_some() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the queue never moved again");
    }
}
