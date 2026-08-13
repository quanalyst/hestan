use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::executor::Runner;
use crate::io::Io;

/// how often a sweep comes round unless
/// [`retention_interval`](crate::Hestan::retention_interval) says otherwise.
/// what retention deletes is days old, so an hour of lag is noise and sweeping
/// harder would only cost writes.
pub(crate) const DEFAULT_INTERVAL: Duration = Duration::from_secs(3600);

/// how many of the newest ticks each of the two tick logs keeps. both grow with
/// time rather than with what you keep, so the cap applies whether or not any
/// retention policy was asked for.
const TICKS_KEPT: usize = 5000;

/// how many of the newest events about something other than a run are kept, for
/// exactly the same reason and unconditionally for exactly the same reason.
///
/// a run's events go when the run does. everything the v17 log added belongs to
/// no run — an asset built every five minutes, a sensor evaluated every ten
/// seconds — so without this nothing would ever collect them. larger than the
/// tick caps because this is the table you read to find out what happened last
/// night, and a night is not a small number of events.
const EVENTS_KEPT: usize = 50_000;

/// how much history a job keeps: nothing is deleted unless one of these says so.
///
/// [`Hestan::retention`](crate::Hestan::retention) sets it for every job and
/// [`JobBuilder::retention`](crate::JobBuilder::retention) overrides it for one.
///
/// the knobs combine **conservatively**: a run is deleted only when every one
/// of them would delete it. `Retention::days(7).keep_last(50)` keeps a run that
/// is eight days old if it is among the last fifty, and keeps the last fifty
/// only until they are eight days old — whichever holds it back wins. the other
/// reading deletes history the moment either rule fires, which is a thing you
/// find out about afterwards.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Retention {
    days: Option<u32>,
    failed_days: Option<u32>,
    keep_last: Option<usize>,
}

impl Retention {
    /// delete a terminal run `days` days after it was created, with its op
    /// runs, events and captured output.
    ///
    /// the age is measured from `created_at` rather than from the finish, so a
    /// run that sat on the queue for a week ages while it waits — which is what
    /// "keep 30 days of history" means to whoever asked for it.
    pub fn days(days: u32) -> Retention {
        Retention {
            days: Some(days),
            ..Retention::default()
        }
    }

    /// hold the newest `n` finished runs of the job back from the age cutoff,
    /// whatever their age. a job that fires monthly keeps its history under
    /// `days(7)`, and nothing has to guess at a cadence.
    ///
    /// on its own this deletes nothing: with no age policy set there is nothing
    /// for it to hold anything back *from*, and reading it as "delete
    /// everything past the newest n" would make an unconfigured `Retention`
    /// delete a database. it counts finished runs only, so a job with a queue
    /// of pending work still keeps its last `n` results.
    pub fn keep_last(mut self, n: usize) -> Retention {
        self.keep_last = Some(n);
        self
    }

    /// a longer age for runs that failed or were canceled; without it they age
    /// like successes. a successful run is noise a week later, and the failure
    /// you want next quarter is the one about to go.
    pub fn failed_days(mut self, days: u32) -> Retention {
        self.failed_days = Some(days);
        self
    }

    /// what this comes to at `now`.
    pub(crate) fn cutoffs(&self, now: DateTime<Utc>) -> Cutoffs {
        let ago = |days: u32| now - chrono::Duration::days(i64::from(days));
        Cutoffs {
            success: self.days.map(ago),
            // a failure ages by `days` too when no longer age was asked for
            failed: self.failed_days.or(self.days).map(ago),
            keep_last: self.keep_last.unwrap_or(0) as i64,
        }
    }
}

/// one policy resolved against a clock, as the store compares it.
///
/// a `None` cutoff is **no age policy**, which keeps everything: the comparison
/// against it is null, so no row matches. that is the direction an absent
/// setting has to mean — the other one deletes the lot.
pub(crate) struct Cutoffs {
    pub success: Option<DateTime<Utc>>,
    pub failed: Option<DateTime<Utc>>,
    pub keep_last: i64,
}

impl Cutoffs {
    /// whether any age policy is in force at all; nothing below is worth doing
    /// when none is.
    pub(crate) fn any(&self) -> bool {
        self.success.is_some() || self.failed.is_some()
    }
}

/// one pass: the tick logs down to their caps, then whatever each job's policy
/// says it may no longer keep.
///
/// **role-gated.** pruning is a decision like firing a schedule is, and a
/// worker that took it would be deleting the history of runs it did not own —
/// several processes share one database, and only one of them decides.
pub(crate) fn sweep(runner: &Runner, policy: &Retention, now: DateTime<Utc>) {
    if !runner.role().decides() {
        return;
    }
    let store = runner.store();
    if let Err(e) = store.prune_ticks(TICKS_KEPT) {
        tracing::warn!("tick prune failed: {e}");
    }
    if let Err(e) = store.prune_sensor_ticks(TICKS_KEPT) {
        tracing::warn!("sensor tick prune failed: {e}");
    }
    if let Err(e) = store.prune_events(EVENTS_KEPT) {
        tracing::warn!("event prune failed: {e}");
    }
    let jobs = match store.run_jobs() {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::warn!("retention: reading the jobs with runs failed: {e}");
            return;
        }
    };
    let mut removed = 0;
    for job in &jobs {
        // a job this process does not define keeps the global policy: the runs
        // are still there and something has to decide about them
        let theirs = runner
            .jobs()
            .get(job)
            .and_then(|j| j.retention())
            .unwrap_or(*policy);
        let doomed = match store.doomed_runs(job, &theirs, now) {
            Ok(doomed) if doomed.is_empty() => continue,
            Ok(doomed) => doomed,
            Err(e) => {
                tracing::warn!(job = %job, "retention: reading what to prune failed: {e}");
                continue;
            }
        };
        // what the runs wrote before the rows that say they wrote it, and the
        // order is the whole of why this works. a run row is the only record
        // that the run existed: delete it first and a crash in between leaves
        // files nothing can ever name again. this way round a crash leaves
        // rows pointing at outputs that are gone — for runs already past
        // retention, which the next sweep deletes anyway.
        drop_outputs(runner.io(), job, &doomed);
        match store.delete_runs(job, &doomed, now) {
            Ok(n) => removed += n,
            Err(e) => tracing::warn!(job = %job, "retention sweep failed: {e}"),
        }
    }
    if removed > 0 {
        tracing::info!("retention: removed {removed} runs");
    }
    // sensor run keys are never collected on their own: a sensor keyed by the
    // day would keep a row per day for as long as the file lives
    if let Some(cutoff) = policy.cutoffs(now).success {
        match store.prune_sensor_run_keys(cutoff) {
            Ok(n) if n > 0 => tracing::info!("retention: removed {n} sensor run keys"),
            Err(e) => tracing::warn!("retention: sensor run keys: {e}"),
            Ok(_) => {}
        }
        // delivered notifications only. one that never got through is not
        // history, it is something outstanding, and it stays until somebody
        // has seen it
        match store.prune_notifications(cutoff) {
            Ok(n) if n > 0 => tracing::info!("retention: removed {n} delivered notifications"),
            Err(e) => tracing::warn!("retention: notifications: {e}"),
            Ok(_) => {}
        }
    }
}

/// hand every registered manager the runs about to be deleted.
///
/// a manager that cannot drop one is logged and the sweep carries on to the
/// rows. a file left behind is one run's worth of waste that the directory's
/// owner can still find and remove; a sweep that stopped at it would grow the
/// database forever behind one unwritable directory, and go on doing it every
/// hour without ever getting further.
fn drop_outputs(io: &Io, job: &str, runs: &[String]) {
    for manager in io.all() {
        for run in runs {
            if let Err(e) = manager.drop_run(run, job) {
                tracing::warn!(run = %run, "retention: dropping what the run wrote failed: {e}");
            }
        }
    }
}

/// the sweeper loop: its own task beside the scheduler, sweeping on `every`.
///
/// the loop is the whole point of this being a phase. retention used to run
/// once, at startup, so a server up for three months pruned nothing after boot
/// — the one deployment shape where a retention policy matters is the one where
/// it never ran.
pub(crate) async fn run_sweeper(runner: Runner, policy: Retention, every: Duration) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // the first tick lands immediately and startup has already swept
    ticker.tick().await;
    loop {
        ticker.tick().await;
        // off the runtime, for the reason every io manager call is: a sweep
        // is store writes and now file deletes, and the ops of this process's
        // runs should not be queued behind either
        let (runner, policy) = (runner.clone(), policy);
        if let Err(e) =
            tokio::task::spawn_blocking(move || sweep(&runner, &policy, Utc::now())).await
        {
            tracing::warn!("retention: the sweep did not finish: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::io::{IoDropped, IoKey, IoManager, IoResult};
    use crate::job::Job;
    use crate::model::{Role, Run, RunStatus, RunTags, Trigger};
    use crate::op::Op;
    use crate::store::Store;
    use serde_json::{Value, json};

    fn job(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new("noop", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap()
    }

    /// a finished run of `job`, created `days` ago.
    fn plant(store: &Store, id: &str, job: &str, days: i64) {
        let at = Utc::now() - chrono::Duration::days(days);
        store
            .create_run(
                &Run {
                    id: id.to_string(),
                    job: job.to_string(),
                    status: RunStatus::Queued,
                    trigger: Trigger::Manual,
                    params: json!({}),
                    created_at: at,
                    started_at: Some(at),
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
                },
                &["noop".to_string()],
            )
            .unwrap();
        store
            .run_finished(id, RunStatus::Success, None, Utc::now(), None)
            .unwrap();
    }

    fn ids(store: &Store) -> Vec<String> {
        store
            .runs(None, None, None, None, None, 100)
            .unwrap()
            .into_iter()
            .map(|r| r.id)
            .collect()
    }

    #[test]
    fn a_per_job_policy_beats_the_global_one() {
        let store = Store::open(":memory:").unwrap();
        plant(&store, "kept", "archive", 30);
        plant(&store, "swept", "chatty", 30);
        let runner = Runner::new(
            [
                Job::builder("archive")
                    .retention(Retention::days(365))
                    .op(Op::new("noop", |_| async { Ok(json!(null)) }))
                    .build()
                    .unwrap(),
                job("chatty"),
            ],
            store.clone(),
        )
        .unwrap();

        sweep(&runner, &Retention::days(7), Utc::now());
        assert_eq!(ids(&store), ["kept"]);
    }

    // the same class of mistake as the phase-17 boot sweep: several processes
    // share one database, and a worker deleting the scheduler's history is
    // data loss nothing would ever report
    #[test]
    fn a_worker_never_prunes() {
        let store = Store::open(":memory:").unwrap();
        plant(&store, "old", "chatty", 30);
        let worker = Runner::new([job("chatty")], store.clone())
            .unwrap()
            .with_role(Role::Worker, 4);

        sweep(&worker, &Retention::days(7), Utc::now());
        assert_eq!(ids(&store), ["old"], "a worker pruned the scheduler's runs");

        // and the same registry under a role that decides does prune it
        let scheduler = Runner::new([job("chatty")], store.clone())
            .unwrap()
            .with_role(Role::Scheduler, 4);
        sweep(&scheduler, &Retention::days(7), Utc::now());
        assert!(ids(&store).is_empty());
    }

    /// a manager that records which runs it was asked to drop, and whether the
    /// run's row was still in the log when it was asked — which is the whole
    /// of what "files before rows" means from a manager's side.
    struct Records {
        store: Store,
        dropped: Mutex<Vec<(String, bool)>>,
    }

    impl IoManager for Records {
        fn put(&self, _key: &IoKey, value: Value) -> IoResult {
            Ok(value)
        }
        fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
            Ok(handle.clone())
        }
        fn drop_run(&self, run_id: &str, _job: &str) -> IoDropped {
            let still_a_run = self.store.run(run_id).unwrap().is_some();
            self.dropped
                .lock()
                .unwrap()
                .push((run_id.to_string(), still_a_run));
            Ok(())
        }
    }

    /// a manager that cannot drop anything: an unwritable directory, a bucket
    /// this process has no credentials for, a disk that is gone.
    struct Fails;

    impl IoManager for Fails {
        fn put(&self, _key: &IoKey, value: Value) -> IoResult {
            Ok(value)
        }
        fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
            Ok(handle.clone())
        }
        fn drop_run(&self, _run_id: &str, _job: &str) -> IoDropped {
            Err("the bucket said no".into())
        }
    }

    // what a run wrote goes before the rows that name it, because the rows are
    // the only thing that knows the run existed. and a manager that cannot
    // drop its half does not hold up the other: a file left behind is one
    // run's waste, while a sweep that stopped would grow the database forever
    #[test]
    fn the_sweep_drops_what_a_run_wrote_first_and_prunes_it_anyway_if_that_fails() {
        let store = Store::open(":memory:").unwrap();
        plant(&store, "old", "chatty", 30);
        plant(&store, "recent", "chatty", 1);
        let records = Arc::new(Records {
            store: store.clone(),
            dropped: Mutex::new(Vec::new()),
        });
        let runner = Runner::with_io(
            [job("chatty")],
            store.clone(),
            Vec::new(),
            Vec::new(),
            records.clone(),
            [("archive".to_string(), Arc::new(Fails) as Arc<dyn IoManager>)],
        )
        .unwrap();

        sweep(&runner, &Retention::days(7), Utc::now());

        // the failing manager took nothing with it
        assert_eq!(ids(&store), ["recent"]);
        // asked about the run that went and nothing else, while that run was
        // still a row — which is what makes the order provable rather than
        // asserted about a comment
        assert_eq!(
            *records.dropped.lock().unwrap(),
            [("old".to_string(), true)]
        );
    }

    // the whole point of the loop: a run that appeared after boot is pruned by
    // a process that has been up for a while, which is the deployment shape
    // retention exists for and the one the startup-only sweep never covered
    #[tokio::test]
    async fn the_interval_sweep_prunes_a_run_that_appeared_after_boot() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([job("chatty")], store.clone()).unwrap();
        let loop_handle = tokio::spawn(run_sweeper(
            runner,
            Retention::days(7),
            Duration::from_millis(10),
        ));

        plant(&store, "after-boot", "chatty", 30);
        for _ in 0..200 {
            if ids(&store).is_empty() {
                loop_handle.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        loop_handle.abort();
        panic!("the sweeper never came round");
    }
}
