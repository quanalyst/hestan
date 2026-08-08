use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use futures::FutureExt;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::watch;
use tokio::task::{Id, JoinSet};

use crate::error::Error;
use crate::graph;
use crate::job::Job;
use crate::model::{EventKind, EventLevel, OpStatus, Run, RunStatus, Trigger, new_run_id};
use crate::op::{Op, OpCtx};
use crate::store::Store;

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

type OpOutcome = (String, Result<(Value, Option<Value>), String>);

/// executes jobs against a store. cheap to clone.
#[derive(Clone)]
pub struct Runner {
    jobs: Arc<HashMap<String, Job>>,
    store: Store,
    // one watch sender per in-flight run; cancel() flips it to true
    active: Arc<Mutex<HashMap<String, watch::Sender<bool>>>>,
    hooks: Arc<Vec<FailureHook>>,
}

impl Runner {
    pub fn new(jobs: impl IntoIterator<Item = Job>, store: Store) -> Runner {
        Runner::with_failure_hooks(jobs, store, Vec::new())
    }

    /// like [`Runner::new`] with failure hooks attached: each is invoked on
    /// its own task whenever a run finishes failed. canceled runs don't fire.
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
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn jobs(&self) -> &HashMap<String, Job> {
        &self.jobs
    }

    /// create the run queued and execute it on a spawned task.
    pub fn launch(&self, job: &str, params: Value, trigger: Trigger) -> Result<String, Error> {
        let (id, fut) = self.prepare(job, None, params, trigger)?;
        tokio::spawn(fut);
        Ok(id)
    }

    /// like [`Runner::launch`] but awaits completion.
    pub async fn run(&self, job: &str, params: Value, trigger: Trigger) -> Result<Run, Error> {
        let (id, fut) = self.prepare(job, None, params, trigger)?;
        // spawned so that dropping this future (timeout, select) detaches the
        // run instead of aborting its ops mid-write
        let _ = tokio::spawn(fut).await;
        Ok(self.store.run(&id)?.expect("run row written at launch"))
    }

    /// launch over a subset of the job's ops with upstream outputs pre-seeded.
    /// every subset member's dep must be in the subset or seeded, else
    /// [`Error::Graph`]. asset builds are the one caller.
    pub(crate) fn launch_subset(
        &self,
        job: &str,
        ops: HashSet<String>,
        seeded: HashMap<String, Value>,
        params: Value,
        trigger: Trigger,
    ) -> Result<String, Error> {
        let (id, fut) = self.prepare(job, Some((ops, seeded)), params, trigger)?;
        tokio::spawn(fut);
        Ok(id)
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
        let (id, fut) = self.prepare(job, Some((ops, seeded)), params, trigger)?;
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
        };
        // registered before create_run so a cancel that can see the queued run
        // always finds a live sender
        let (cancel_tx, cancel_rx) = watch::channel(false);
        self.active
            .lock()
            .unwrap()
            .insert(run.id.clone(), cancel_tx);
        if let Err(e) = self.store.create_run(&run, &pending) {
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
    let mut failed = false;
    let mut canceled = false;
    let mut first_failure: Option<(String, String)> = None;
    let mut tasks: JoinSet<OpOutcome> = JoinSet::new();
    // JoinError only carries the task id, so remember which op each task is
    let mut names: HashMap<Id, String> = HashMap::new();

    loop {
        let (ready, rest): (Vec<String>, Vec<String>) = pending.into_iter().partition(|n| {
            let op = job.op(n).expect("op in topo order");
            op.deps().iter().all(|d| outputs.contains_key(d))
        });
        pending = rest;
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
            let op = job.op(&name).expect("op in topo order").clone();
            let inputs: HashMap<String, Value> = op
                .deps()
                .iter()
                .map(|d| (d.clone(), outputs[d].clone()))
                .collect();
            let handle = tasks.spawn(run_op(
                op,
                job.name().to_string(),
                run_id.clone(),
                params.clone(),
                Arc::new(inputs),
                store.clone(),
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
            Ok((id, (name, Ok((output, state))))) => {
                names.remove(&id);
                note(store.op_finished(&run_id, &name, OpStatus::Success, Some(&output), None));
                // state second: a crash between the writes re-runs the op, never skips it
                if let Some(state) = state {
                    note(store.set_op_state(job.name(), &name, &state));
                }
                outputs.insert(name, output);
            }
            Ok((id, (name, Err(msg)))) => {
                names.remove(&id);
                note(store.op_finished(&run_id, &name, OpStatus::Failed, None, Some(&msg)));
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                skip_downstream(&pairs, &name, &mut pending, &run_id, &store);
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
                note(store.op_finished(&run_id, &name, OpStatus::Failed, None, Some(&msg)));
                if first_failure.is_none() {
                    first_failure = Some((name.clone(), msg));
                }
                failed = true;
                skip_downstream(&pairs, &name, &mut pending, &run_id, &store);
            }
        }
    }

    if canceled {
        // abort lands at the next await point: a blocking section finishes its call
        tasks.abort_all();
        while let Some(joined) = tasks.join_next_with_id().await {
            match joined {
                // won the race against the abort: record what really happened
                Ok((id, (name, Ok((output, state))))) => {
                    names.remove(&id);
                    note(store.op_finished(&run_id, &name, OpStatus::Success, Some(&output), None));
                    if let Some(state) = state {
                        note(store.set_op_state(job.name(), &name, &state));
                    }
                }
                Ok((id, (name, Err(msg)))) => {
                    names.remove(&id);
                    note(store.op_finished(&run_id, &name, OpStatus::Failed, None, Some(&msg)));
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
                    note(store.op_finished(&run_id, &name, OpStatus::Failed, None, Some(&msg)));
                }
            }
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
    // event first: anyone who reads a terminal status must also see this line
    note(store.append_event(&run_id, None, level, kind, msg, None));
    note(store.run_finished(&run_id, status));
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

fn op_canceled(store: &Store, run_id: &str, name: &str) {
    note(store.op_finished(run_id, name, OpStatus::Canceled, None, Some("canceled")));
    note(store.append_event(
        run_id,
        Some(name),
        EventLevel::Warn,
        EventKind::OpCanceled,
        "canceled",
        None,
    ));
}

async fn run_op(
    op: Op,
    job: String,
    run_id: String,
    params: Value,
    inputs: Arc<HashMap<String, Value>>,
    store: Store,
) -> OpOutcome {
    let name = op.name().to_string();
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
    loop {
        // fresh buffer per attempt: a failed attempt's staged state must not leak
        let new_state = Arc::new(Mutex::new(None));
        let ctx = OpCtx {
            run_id: run_id.clone(),
            job: job.clone(),
            op: name.clone(),
            params: params.clone(),
            inputs: inputs.clone(),
            state: state.clone(),
            new_state: new_state.clone(),
            new_fingerprint: Arc::new(Mutex::new(None)),
            store: store.clone(),
        };
        // the call sits inside the async block, so a closure that panics before
        // returning its future is caught by the retry policy too
        let result = match AssertUnwindSafe(async { op.call(ctx).await })
            .catch_unwind()
            .await
        {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(e)) => Err(e.to_string()),
            // as_ref, not &: &Box<dyn Any> would downcast against the box itself
            Err(panic) => Err(match panic_payload(panic.as_ref()) {
                Some(s) => format!("op panicked: {s}"),
                None => "op panicked".to_string(),
            }),
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
                let staged = new_state.lock().unwrap().take();
                return (name, Ok((output, staged)));
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
                    tokio::time::sleep(op.delay()).await;
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

fn skip_downstream(
    pairs: &[(String, Vec<String>)],
    root: &str,
    pending: &mut Vec<String>,
    run_id: &str,
    store: &Store,
) {
    let down = graph::downstream(pairs, root);
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
                &format!("skipped: upstream {root} failed"),
                None,
            ));
            note(store.op_finished(run_id, &name, OpStatus::Skipped, None, None));
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
