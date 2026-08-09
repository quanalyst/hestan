use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::FutureExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{Semaphore, watch};
use tokio::task::{Id, JoinSet};

use crate::error::Error;
use crate::graph;
use crate::io::{Io, IoKey, IoManager};
use crate::job::Job;
use crate::model::{
    EventKind, EventLevel, OpRun, OpStatus, Run, RunStatus, Trigger, When, new_run_id,
};
use crate::op::{self, Cancel, MetaBuf, Op, OpCtx};
use crate::resource::{self, Resources};
use crate::store::Store;

/// how far back a resume follows `resumed_from` links. resuming a resume is
/// normal; a chain this long is a bug, and the walk says so instead of looping.
const MAX_RESUME_CHAIN: usize = 256;

/// how long a canceled run waits for its aborted tasks to actually join before
/// recording them as never observed to stop. aborting an async op lands at its
/// next await point, which is usually immediate; blocking work cannot be
/// aborted at all, and waiting forever for it would hang the run.
const CANCEL_GRACE: Duration = Duration::from_secs(3);

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

/// what a failure hook receives when a run finishes failed.
#[derive(Debug, Clone, Serialize)]
pub struct RunFailure {
    pub run_id: String,
    pub job: String,
    pub trigger: Trigger,
    /// the first op that terminally failed this run.
    pub failed_op: Option<String>,
    /// that op's error message.
    pub error: Option<String>,
    pub finished_at: DateTime<Utc>,
}

/// a callback invoked on its own task when a run finishes failed.
pub type FailureHook = Arc<dyn Fn(RunFailure) + Send + Sync>;

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

/// what one op invocation produced: its output, plus whatever the attempt
/// that worked staged for the terminal write to commit.
struct Produced {
    output: Value,
    state: Option<Value>,
    meta: Option<Value>,
}

type OpOutcome = (String, Result<Produced, String>);

/// one instance of a mapped op, keyed in the run by its `{op}[{i}]` name.
struct Instance {
    parent: String,
    index: usize,
    element: Value,
}

/// a mapped op mid-expansion: one slot per element, so the collected output
/// comes out in element order however the instances interleave.
struct Fanout {
    slots: Vec<Option<Value>>,
    remaining: usize,
    failed: bool,
}

/// executes jobs against a store. cheap to clone.
#[derive(Clone)]
pub struct Runner {
    jobs: Arc<HashMap<String, Job>>,
    store: Store,
    // one watch sender per in-flight run; cancel() flips it to true
    active: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    hooks: Arc<Vec<FailureHook>>,
    pools: Pools,
    resources: Resources,
    io: Io,
}

impl Runner {
    pub fn new(jobs: impl IntoIterator<Item = Job>, store: Store) -> Runner {
        Runner::with_failure_hooks(jobs, store, Vec::new())
    }

    /// like [`Runner::new`] with failure hooks attached: each is invoked on
    /// its own task whenever a run finishes failed. canceled runs don't fire.
    ///
    /// no pools are declared, so an op that names one fails at run time; use
    /// [`Runner::with_pools`] (or `Hestan::pool`) when any op does.
    pub fn with_failure_hooks(
        jobs: impl IntoIterator<Item = Job>,
        store: Store,
        hooks: Vec<FailureHook>,
    ) -> Runner {
        let mut map = HashMap::new();
        for job in jobs {
            let name = job.name().to_string();
            if map.insert(name.clone(), job).is_some() {
                tracing::warn!("duplicate job {name:?}: keeping the last one registered");
            }
        }
        Runner {
            jobs: Arc::new(map),
            store,
            active: Arc::new(Mutex::new(HashMap::new())),
            hooks: Arc::new(hooks),
            pools: Arc::new(HashMap::new()),
            resources: resource::none(),
            io: Io::default(),
        }
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
        let runner = Runner::with_failure_hooks(jobs, store, hooks);
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

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn jobs(&self) -> &HashMap<String, Job> {
        &self.jobs
    }

    /// the limit declared for `name`, for reporting it back.
    pub fn pool_limit(&self, name: &str) -> Option<usize> {
        self.pools.get(name).map(|p| p.limit)
    }

    /// create the run queued and execute it on a spawned task.
    pub fn launch(&self, job: &str, params: Value, trigger: Trigger) -> Result<String, Error> {
        let (id, fut) = self.prepare(job, None, params, trigger, None)?;
        tokio::spawn(fut);
        Ok(id)
    }

    /// like [`Runner::launch`] but awaits completion.
    pub async fn run(&self, job: &str, params: Value, trigger: Trigger) -> Result<Run, Error> {
        let (id, fut) = self.prepare(job, None, params, trigger, None)?;
        // spawned so that dropping this future (timeout, select) detaches the
        // run instead of aborting its ops mid-write
        let _ = tokio::spawn(fut).await;
        Ok(self.store.run(&id)?.expect("run row written at launch"))
    }

    /// launch over a subset of the job's ops with upstream outputs pre-seeded.
    /// every subset member's dep must be in the subset or seeded, else
    /// [`Error::Graph`]. asset builds and resumes are the callers.
    pub(crate) fn launch_subset(
        &self,
        job: &str,
        ops: HashSet<String>,
        seeded: HashMap<String, Value>,
        params: Value,
        trigger: Trigger,
        resumed_from: Option<&str>,
    ) -> Result<String, Error> {
        let (id, fut) = self.prepare(job, Some((ops, seeded)), params, trigger, resumed_from)?;
        tokio::spawn(fut);
        Ok(id)
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
                if subset.contains(dep)
                    || reusable.contains_key(dep)
                    || job.external().contains(dep)
                {
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
        seeded.extend(job.external().iter().map(|n| (n.clone(), Value::Null)));
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
    ) -> Result<Run, Error> {
        let (id, fut) = self.prepare(job, Some((ops, seeded)), params, trigger, None)?;
        let _ = tokio::spawn(fut).await;
        Ok(self.store.run(&id)?.expect("run row written at launch"))
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
        match self.active.lock().unwrap().get(run_id) {
            Some(tx) => {
                let _ = tx.send(true);
                Ok(CancelOutcome::Requested)
            }
            // active status but no live sender: a run from before a restart
            None => Ok(CancelOutcome::AlreadyFinished),
        }
    }

    fn prepare(
        &self,
        job: &str,
        subset: Option<(HashSet<String>, HashMap<String, Value>)>,
        params: Value,
        trigger: Trigger,
        resumed_from: Option<&str>,
    ) -> Result<(String, impl Future<Output = ()> + Send + 'static), Error> {
        let job = self
            .jobs
            .get(job)
            .ok_or_else(|| Error::UnknownJob(job.to_string()))?
            .clone();
        let (pending, seeded) = match subset {
            None => (
                job.order().to_vec(),
                job.external()
                    .iter()
                    .map(|n| (n.clone(), Value::Null))
                    .collect(),
            ),
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
        };
        // a mapped op is never a row of its own: its instances are the record,
        // and how many there are is not known until its dep has produced
        let rows: Vec<String> = pending
            .iter()
            .filter(|n| job.op(n).expect("op in topo order").mapped_over().is_none())
            .cloned()
            .collect();
        // registered before create_run so a cancel that can see the queued run
        // always finds a live sender
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.active
            .lock()
            .unwrap()
            .insert(run.id.clone(), cancel_tx);
        if let Err(e) = self.store.create_run(&run, &rows) {
            self.active.lock().unwrap().remove(&run.id);
            return Err(e);
        }
        let id = run.id.clone();
        let fut = execute(
            job,
            run.id,
            run.params,
            trigger,
            self.clone(),
            cancel_rx,
            pending,
            seeded,
        );
        Ok((id, fut))
    }
}

// "process[3]" -> ("process", 3), but only when `process` is a mapped op of
// this job; any other bracketed name is just an op name
fn instance_of(job: &Job, name: &str) -> Option<(String, usize)> {
    let (parent, index) = name.split_once('[')?;
    let index: usize = index.strip_suffix(']')?.parse().ok()?;
    job.op(parent)?.mapped_over()?;
    Some((parent.to_string(), index))
}

/// collapse one run's fan-out instance rows into a single entry for the mapped
/// op they belong to. it counts as succeeded only when the instances cover
/// `0..n` and every one of them did: the array a mapped op expands over can
/// differ on a re-run, so anything less has to expand again from scratch. a
/// mapped op with no rows at all — never reached, or expanded over an empty
/// array — is absent here, which resume planning reads the same way.
fn fold_instances(
    io: &Io,
    job: &Job,
    run_id: &str,
    rows: Vec<OpRun>,
) -> Vec<(String, OpStatus, Option<Value>)> {
    let mut folded: Vec<(String, OpStatus, Option<Value>)> = Vec::with_capacity(rows.len());
    let mut groups: HashMap<String, BTreeMap<usize, (OpStatus, Option<Value>)>> = HashMap::new();
    for row in rows {
        match instance_of(job, &row.op) {
            Some((parent, index)) => {
                groups
                    .entry(parent)
                    .or_default()
                    .insert(index, (row.status, row.output));
            }
            None => folded.push((row.op, row.status, row.output)),
        }
    }
    for (parent, slots) in groups {
        let whole = slots.keys().copied().eq(0..slots.len())
            && slots
                .values()
                .all(|(status, output)| *status == OpStatus::Success && output.is_some());
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
        for (index, (_, output)) in slots {
            let handle = output.expect("checked just above");
            let key = IoKey {
                run_id: run_id.to_string(),
                job: job.name().to_string(),
                op: format!("{parent}[{index}]"),
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

#[allow(clippy::too_many_arguments)]
async fn execute(
    job: Job,
    run_id: String,
    params: Value,
    trigger: Trigger,
    runner: Runner,
    mut cancel: watch::Receiver<bool>,
    pending: Vec<String>,
    seeded: HashMap<String, Value>,
) {
    let store = runner.store.clone();
    note(store.run_started(&run_id));
    note(store.append_event(
        &run_id,
        None,
        EventLevel::Info,
        EventKind::RunStarted,
        "run started",
        None,
    ));

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
    // fan-out state: what each instance is for, and where its output belongs
    let mut instances: HashMap<String, Instance> = HashMap::new();
    let mut fanouts: HashMap<String, Fanout> = HashMap::new();

    loop {
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
                        Some(&json!({ "when": op.runs_when() })),
                    ));
                    note(store.op_finished(&run_id, &name, OpStatus::Skipped, None, None, None));
                    statuses.insert(name.clone(), OpStatus::Skipped);
                    let reason = format!("skipped: upstream {name} was skipped");
                    skip_downstream(
                        &job,
                        &pairs,
                        &name,
                        &reason,
                        &mut pending,
                        &mut statuses,
                        &run_id,
                        &store,
                    );
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
                let elements = match outputs.get(&over) {
                    None => Ok(Some(Vec::new())),
                    Some(held) => {
                        resolve(&runner.io, &job, &run_id, &over, held).map(|v| match v {
                            Value::Array(a) => Some(a),
                            other => {
                                expanded_over = Some(json_type(&other));
                                None
                            }
                        })
                    }
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
                let Some(elements) = elements else {
                    let msg = match &unreadable {
                        Some(e) => format!("could not read the output of {over}: {e}"),
                        None => format!(
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
                    skip_downstream(
                        &job,
                        &pairs,
                        &name,
                        &reason,
                        &mut pending,
                        &mut statuses,
                        &run_id,
                        &store,
                    );
                    continue;
                };
                // rows first, so a cancel or a skip has something to write to,
                // exactly as a static op's row exists from the launch on
                let created: Vec<String> = elements
                    .into_iter()
                    .enumerate()
                    .map(|(index, element)| {
                        let instance = format!("{name}[{index}]");
                        note(store.create_op_run(&run_id, &instance));
                        instances.insert(
                            instance.clone(),
                            Instance {
                                parent: name.clone(),
                                index,
                                element,
                            },
                        );
                        instance
                    })
                    .collect();
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
            for dep in op.deps() {
                let seen = op.dep_alias(dep).to_string();
                if let Some(held) = outputs.get(dep) {
                    // `outputs` carries handles, so this is where a dep's
                    // output is actually fetched back
                    match resolve(&runner.io, &job, &run_id, dep, held) {
                        Ok(v) => {
                            inputs.entry(seen.clone()).or_insert(v);
                        }
                        Err(e) if unresolved.is_none() => {
                            unresolved = Some((dep.clone(), e));
                        }
                        Err(_) => {}
                    }
                }
                if let Some(s) = statuses.get(dep) {
                    dep_statuses.entry(seen).or_insert(*s);
                }
            }
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
                note(store.op_finished(&run_id, &name, OpStatus::Failed, None, None, Some(&msg)));
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                give_up(
                    &name,
                    &instances,
                    &mut fanouts,
                    &job,
                    &pairs,
                    &mut pending,
                    &mut statuses,
                    &run_id,
                    &store,
                );
                continue;
            }
            let handle = tasks.spawn(run_op(
                op,
                name.clone(),
                instances.get(&name).map(|i| i.element.clone()),
                job.name().to_string(),
                run_id.clone(),
                params.clone(),
                Arc::new(inputs),
                Arc::new(dep_statuses),
                runner.resources.clone(),
                store.clone(),
                runner.pools.clone(),
                cancel.clone(),
            ));
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
            Ok((
                id,
                (
                    name,
                    Ok(Produced {
                        output,
                        state,
                        meta,
                    }),
                ),
            )) => {
                names.remove(&id);
                // persisted before the success is recorded: a row saying
                // success with an output that was never stored is a lie the
                // next run would trip over
                let unit = unit_op(&job, &instances, &name);
                let key = io_key(&run_id, &job, &name);
                match runner.io.manager(unit.io_name()).put(&key, output) {
                    Ok(handle) => {
                        note(store.op_finished(
                            &run_id,
                            &name,
                            OpStatus::Success,
                            Some(&handle),
                            meta.as_ref(),
                            None,
                        ));
                        // state second: a crash between the writes re-runs the op, never skips it
                        if let Some(state) = state {
                            note(store.set_op_state(job.name(), &name, &state));
                        }
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
                        );
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
                        note(store.op_finished(
                            &run_id,
                            &name,
                            OpStatus::Failed,
                            None,
                            None,
                            Some(&msg),
                        ));
                        if first_failure.is_none() {
                            first_failure = Some((name.clone(), msg));
                        }
                        failed = true;
                        give_up(
                            &name,
                            &instances,
                            &mut fanouts,
                            &job,
                            &pairs,
                            &mut pending,
                            &mut statuses,
                            &run_id,
                            &store,
                        );
                    }
                }
            }
            Ok((id, (name, Err(msg)))) => {
                names.remove(&id);
                note(store.op_finished(&run_id, &name, OpStatus::Failed, None, None, Some(&msg)));
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                give_up(
                    &name,
                    &instances,
                    &mut fanouts,
                    &job,
                    &pairs,
                    &mut pending,
                    &mut statuses,
                    &run_id,
                    &store,
                );
            }
            Err(join_err) => {
                let name = names.remove(&join_err.id()).expect("spawned with id");
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
                note(store.op_finished(&run_id, &name, OpStatus::Failed, None, None, Some(&msg)));
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                give_up(
                    &name,
                    &instances,
                    &mut fanouts,
                    &job,
                    &pairs,
                    &mut pending,
                    &mut statuses,
                    &run_id,
                    &store,
                );
            }
        }
    }

    if canceled {
        // abort lands at an op's next await point; an op that never awaits, and
        // blocking work an op spawned, never land at all
        tasks.abort_all();
        // a bounded grace period, so an op that really does stop is recorded as
        // whatever it really did rather than guessed at
        let deadline = tokio::time::Instant::now() + CANCEL_GRACE;
        loop {
            let joined = match tokio::time::timeout_at(deadline, tasks.join_next_with_id()).await {
                Ok(Some(joined)) => joined,
                // every task landed, or the grace ran out with some still running
                Ok(None) | Err(_) => break,
            };
            match joined {
                // won the race against the abort: record what really happened
                Ok((
                    id,
                    (
                        name,
                        Ok(Produced {
                            output,
                            state,
                            meta,
                        }),
                    ),
                )) => {
                    names.remove(&id);
                    // won the race against the abort, so it is persisted like
                    // any other success — or recorded failed if it cannot be
                    let unit = unit_op(&job, &instances, &name);
                    let key = io_key(&run_id, &job, &name);
                    match runner.io.manager(unit.io_name()).put(&key, output) {
                        Ok(handle) => {
                            note(store.op_finished(
                                &run_id,
                                &name,
                                OpStatus::Success,
                                Some(&handle),
                                meta.as_ref(),
                                None,
                            ));
                            if let Some(state) = state {
                                note(store.set_op_state(job.name(), &name, &state));
                            }
                        }
                        Err(e) => {
                            let msg = format!("could not persist the output: {e}");
                            note(store.op_finished(
                                &run_id,
                                &name,
                                OpStatus::Failed,
                                None,
                                None,
                                Some(&msg),
                            ));
                        }
                    }
                }
                Ok((id, (name, Err(msg)))) => {
                    names.remove(&id);
                    note(store.op_finished(
                        &run_id,
                        &name,
                        OpStatus::Failed,
                        None,
                        None,
                        Some(&msg),
                    ));
                }
                Err(join_err) if join_err.is_cancelled() => {
                    let name = names.remove(&join_err.id()).expect("spawned with id");
                    op_canceled(&store, &run_id, &name);
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
                    note(store.op_finished(
                        &run_id,
                        &name,
                        OpStatus::Failed,
                        None,
                        None,
                        Some(&msg),
                    ));
                }
            }
        }
        // whatever is still in `names` never joined. aborting cannot stop
        // blocking work, so hestan does not know if it is over — and says so
        // instead of stamping a finish time it never observed.
        let mut unstopped: Vec<String> = names.drain().map(|(_, name)| name).collect();
        unstopped.sort();
        for name in unstopped {
            op_unstopped(&store, &run_id, &name);
        }
        for name in pending.drain(..) {
            op_canceled(&store, &run_id, &name);
        }
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
    note(store.append_event(&run_id, None, level, kind, msg, None));
    note(store.run_finished(&run_id, status, error.as_deref()));
    runner.active.lock().unwrap().remove(&run_id);

    // canceled runs stay quiet, and the boot sweep never comes through here
    if status == RunStatus::Failed {
        let (failed_op, error) = match first_failure {
            Some((op, msg)) => (Some(op), Some(msg)),
            None => (None, None),
        };
        let failure = RunFailure {
            run_id,
            job: job.name().to_string(),
            trigger,
            failed_op,
            error,
            finished_at: Utc::now(),
        };
        for hook in runner.hooks.iter() {
            let hook = hook.clone();
            let failure = failure.clone();
            // spawn_blocking, not spawn: a hook that blocks would pin an async
            // worker and hang runtime shutdown
            tokio::task::spawn_blocking(move || {
                if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| hook(failure))) {
                    match panic_payload(panic.as_ref()) {
                        Some(s) => tracing::warn!("failure hook panicked: {s}"),
                        None => tracing::warn!("failure hook panicked"),
                    }
                }
            });
        }
    }
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
fn collect(
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
        let manager = io.manager(job.op(&instance.parent).and_then(Op::io_name));
        let mut collected: Vec<Value> = Vec::with_capacity(fan.slots.len());
        for (index, slot) in std::mem::take(&mut fan.slots).into_iter().enumerate() {
            let handle = slot.expect("every instance filled its slot");
            let key = IoKey {
                run_id: run_id.to_string(),
                job: job.name().to_string(),
                op: format!("{}[{index}]", instance.parent),
            };
            match manager.get(&key, &handle) {
                Ok(v) => collected.push(v),
                Err(e) => {
                    // the whole fan-out is unusable, and saying so beats
                    // handing downstream an array with a hole in it
                    tracing::warn!(
                        run = %run_id, op = %key.op, "instance output unreadable: {e}"
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
fn resolve(io: &Io, job: &Job, run_id: &str, op: &str, held: &Value) -> Result<Value, String> {
    let manager = io.manager(job.op(op).and_then(Op::io_name));
    manager
        .get(&io_key(run_id, job, op), held)
        .map_err(|e| e.to_string())
}

// a failed unit skips its downstream. one failing instance fails its whole
// mapped op — there is no partial array — and the siblings still in flight
// run to the end, exactly as an op's siblings do
#[allow(clippy::too_many_arguments)]
fn give_up(
    name: &str,
    instances: &HashMap<String, Instance>,
    fanouts: &mut HashMap<String, Fanout>,
    job: &Job,
    pairs: &[(String, Vec<String>)],
    pending: &mut Vec<String>,
    statuses: &mut HashMap<String, OpStatus>,
    run_id: &str,
    store: &Store,
) {
    statuses.insert(name.to_string(), OpStatus::Failed);
    let Some(instance) = instances.get(name) else {
        let reason = format!("skipped: upstream {name} failed");
        skip_downstream(job, pairs, name, &reason, pending, statuses, run_id, store);
        return;
    };
    let fan = fanouts
        .get_mut(&instance.parent)
        .expect("an instance belongs to a live fan-out");
    fan.remaining -= 1;
    // the first failure is what fails the mapped op and skips downstream;
    // later ones just close a slot
    if !fan.failed {
        fan.failed = true;
        let parent = instance.parent.clone();
        statuses.insert(parent.clone(), OpStatus::Failed);
        let reason = format!("skipped: upstream {parent} failed");
        skip_downstream(
            job, pairs, &parent, &reason, pending, statuses, run_id, store,
        );
    }
}

fn op_canceled(store: &Store, run_id: &str, name: &str) {
    note(store.op_finished(
        run_id,
        name,
        OpStatus::Canceled,
        None,
        None,
        Some("canceled"),
    ));
    note(store.append_event(
        run_id,
        Some(name),
        EventLevel::Warn,
        EventKind::OpCanceled,
        "canceled",
        None,
    ));
}

// canceled, but only the request is a fact: the op never joined, so it gets no
// finish time and an error that says exactly that
fn op_unstopped(store: &Store, run_id: &str, name: &str) {
    let msg = format!(
        "cancellation requested; this op was not observed to stop within {CANCEL_GRACE:?} \
         and may still be running (blocking work stops only if it polls ctx.is_cancelled())"
    );
    note(store.append_event(
        run_id,
        Some(name),
        EventLevel::Warn,
        EventKind::OpCanceled,
        &msg,
        None,
    ));
    note(store.op_unstopped(run_id, name, &msg));
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
    inputs: Arc<HashMap<String, Value>>,
    dep_statuses: Arc<HashMap<String, OpStatus>>,
    resources: Resources,
    store: Store,
    pools: Pools,
    cancel: watch::Receiver<bool>,
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
    note(store.op_started(&run_id, &name, attempt));
    note(store.append_event(
        &run_id,
        Some(&name),
        EventLevel::Info,
        EventKind::OpStarted,
        "starting",
        None,
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
                return (name, Err(msg));
            }
        },
    };
    loop {
        // fresh buffers per attempt: a failed attempt's staged state and
        // metadata must not leak into the one that works
        let new_state = Arc::new(Mutex::new(None));
        let new_meta: MetaBuf = Arc::new(Mutex::new(BTreeMap::new()));
        // one more stop signal per attempt, flipped by this attempt's timeout;
        // the run's own cancel channel is the other half
        let (expired, on_expiry) = watch::channel(false);
        let ctx = OpCtx {
            cancel: Cancel {
                run: cancel.clone(),
                attempt: on_expiry,
            },
            run_id: run_id.clone(),
            job: job.clone(),
            op: name.clone(),
            params: params.clone(),
            element: element.clone(),
            inputs: inputs.clone(),
            dep_statuses: dep_statuses.clone(),
            resources: resources.clone(),
            state: state.clone(),
            new_state: new_state.clone(),
            new_fingerprint: Arc::new(Mutex::new(None)),
            new_meta: new_meta.clone(),
            new_per_asset: Arc::new(Mutex::new(BTreeMap::new())),
            store: store.clone(),
        };
        // Err(limit) means the attempt timed out; the permit is scoped to this
        // block, so a retry sleep never sits on the resource it backed off from
        let caught = {
            let _permit = match &pool {
                None => None,
                Some((pool, sem)) => Some(match sem.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        // otherwise a queued op is just an op sitting in `running`
                        ctx.info(format!("waiting for a {pool} pool permit"));
                        sem.clone()
                            .acquire_owned()
                            .await
                            .expect("pool semaphores are never closed")
                    }
                }),
            };
            // the call sits inside the async block, so a closure that panics before
            // returning its future is caught by the retry policy too
            let call = AssertUnwindSafe(async { op.call(ctx).await }).catch_unwind();
            match op.timeout_after() {
                None => Ok(call.await),
                Some(limit) => match tokio::time::timeout(limit, call).await {
                    Ok(caught) => Ok(caught),
                    // dropping the future stops an async op here; a blocking one
                    // only stops if it polls, so flip the flag it polls
                    Err(_) => {
                        let _ = expired.send(true);
                        Err(limit)
                    }
                },
            }
        };
        let result = match caught {
            Ok(Ok(Ok(output))) => Ok(output),
            Ok(Ok(Err(e))) => Err(e.to_string()),
            // as_ref, not &: &Box<dyn Any> would downcast against the box itself
            Ok(Err(panic)) => Err(match panic_payload(panic.as_ref()) {
                Some(s) => format!("op panicked: {s}"),
                None => "op panicked".to_string(),
            }),
            Err(limit) => Err(format!("timed out after {limit:?}")),
        };
        match result {
            Ok(output) => {
                let data = op.output_type().map(|t| json!({ "output_type": t }));
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Info,
                    EventKind::OpSuccess,
                    "finished",
                    data.as_ref(),
                ));
                return (
                    name,
                    Ok(Produced {
                        output,
                        state: new_state.lock().unwrap().take(),
                        meta: op::staged_meta(&new_meta),
                    }),
                );
            }
            Err(msg) => {
                // retries are extra attempts after the first
                let retrying = attempt <= op.max_retries();
                let kind = if retrying {
                    EventKind::OpRetry
                } else {
                    EventKind::OpFailed
                };
                let data = if retrying {
                    json!({ "attempt": attempt })
                } else {
                    json!({ "error": msg })
                };
                note(store.append_event(
                    &run_id,
                    Some(&name),
                    EventLevel::Error,
                    kind,
                    &format!("attempt {attempt} failed: {msg}"),
                    Some(&data),
                ));
                if retrying {
                    tokio::time::sleep(op.delay(attempt)).await;
                    attempt += 1;
                    note(store.op_started(&run_id, &name, attempt));
                } else {
                    return (name, Err(msg));
                }
            }
        }
    }
}

fn panic_payload(panic: &(dyn std::any::Any + Send)) -> Option<&str> {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
}

/// mark what `root` not succeeding cuts off. propagation asks each candidate's
/// [trigger rule](When) instead of assuming: an op that would still run is not
/// skipped, and neither is anything hanging off it, which waits on what that
/// op does rather than on what happened above it. everything reached through
/// plain [`When::AllSucceeded`] ops is skipped as one, naming `root` — that is
/// one failure with one cause, not a chain of them.
#[allow(clippy::too_many_arguments)]
fn skip_downstream(
    job: &Job,
    pairs: &[(String, Vec<String>)],
    root: &str,
    reason: &str,
    pending: &mut Vec<String>,
    statuses: &mut HashMap<String, OpStatus>,
    run_id: &str,
    store: &Store,
) {
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
                None,
            ));
            note(store.op_finished(run_id, &name, OpStatus::Skipped, None, None, None));
            statuses.insert(name, OpStatus::Skipped);
        } else {
            i += 1;
        }
    }
}

fn note(res: Result<(), Error>) {
    if let Err(e) = res {
        tracing::warn!("store write failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::op::Op;
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

    #[tokio::test]
    async fn cancel_registry_registers_at_launch_and_drains() {
        let runner = Runner::new(
            [sleepy_job("slow", 30_000)],
            Store::open(":memory:").unwrap(),
        );
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
        );
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
        let runner = Runner::new([job], Store::open(":memory:").unwrap());
        let err = runner
            .launch("gated", json!({"threshold": "high"}), Trigger::Manual)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParams { .. }), "{err}");
        assert!(runner.active.lock().unwrap().is_empty());
        assert!(
            runner
                .store()
                .runs(None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_create_run_cleans_registry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap();
        let runner = Runner::new([sleepy_job("slow", 1)], Store::open(path).unwrap());
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

    #[tokio::test]
    async fn subset_rejects_unsatisfied_deps() {
        let runner = Runner::new([abc_job()], Store::open(":memory:").unwrap());
        let err = runner
            .launch_subset(
                "abc",
                HashSet::from(["b".into()]),
                HashMap::new(),
                json!({}),
                Trigger::Manual,
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
            )
            .unwrap_err();
        assert!(
            err.to_string().contains("both in the subset and seeded"),
            "{err}"
        );

        assert!(
            runner
                .store()
                .runs(None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn subset_runs_only_its_ops_with_seeded_inputs() {
        let runner = Runner::new([abc_job()], Store::open(":memory:").unwrap());
        let run = runner
            .run_subset(
                "abc",
                HashSet::from(["b".into(), "c".into()]),
                HashMap::from([("a".into(), json!(5))]),
                json!({}),
                Trigger::Manual,
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

    #[tokio::test]
    async fn cancel_registry_empty_after_normal_finish() {
        let runner = Runner::new([sleepy_job("quick", 1)], Store::open(":memory:").unwrap());
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
}
