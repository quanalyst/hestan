//! which process is allowed to decide, and for how long it may go on believing
//! it.
//!
//! **the election is an optimisation.** what makes a duplicate decision
//! impossible is the [store](crate::Store): one `fired` tick per cron
//! occurrence and one run per `(sensor, run_key)`, both refused by a unique
//! index rather than by a check. this module is what stops the duplicate being
//! *attempted*, which is a different and much weaker guarantee, and it is
//! deliberately built second. a design where correctness rests on a
//! distributed lock is a design that fails during a gc pause, a slow disk or a
//! partition, because those are exactly the moments a lock holder is wrong
//! about holding it.
//!
//! so: the lease is the fast path and the constraint is the truth. a leader
//! that pauses past its expiry and wakes up still believing it leads writes
//! decisions that the store refuses, rather than decisions that land twice.
//!
//! the vocabulary is the run lease's, because it is the same mechanism aimed
//! at a different thing: `claimed_by`, `lease_until`, taken when free or
//! expired, lost by failing to renew. what it adds is a **term**, which counts
//! acquisitions, so a decision can name the stretch of time it was made in.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::executor::Runner;

/// how long the deciding lease is believed for, and how often its holder says
/// it is still there.
///
/// **a quarter of the run lease, deliberately.** the two want opposite
/// trade-offs. losing a run lease wrongly costs a whole run re-executed, so
/// that one is generous. losing a deciding lease wrongly costs one handover,
/// and a handover is cheap precisely because the decisions on the other side of
/// it are already backstopped: a duplicate fire is refused by a unique index
/// and a decision made under a stale term is refused in the transaction that
/// writes it. so this side is tuned for *liveness*: what a longer lease would
/// buy is fewer handovers, and what it costs is ten more seconds of nobody
/// firing anything after a process is killed.
///
/// five renewals inside one lease, so a slow store or a briefly busy process
/// misses four of them before anything is taken.
pub(crate) const DECIDE_LEASE: Duration = Duration::from_secs(10);
pub(crate) const DECIDE_RENEW: Duration = Duration::from_secs(2);

/// what a process believes about its right to decide.
///
/// cheap to clone and shared by every deciding loop in the process, so there is
/// one answer rather than one per loop. the answer is an atomic read: a loop
/// asking whether it may decide never touches the store.
#[derive(Clone, Debug)]
pub(crate) struct Deciding {
    /// `None` for a process that runs no election at all, which is every
    /// headless one-shot, every directly built [`Runner`] and every test that
    /// is not about the election. such a process decides, always, and its
    /// decisions name no term.
    ///
    /// this is not a shortcut. `Hestan::run_once` and `Hestan::build_asset`
    /// have to execute the run they were asked for; a lease held by a live
    /// server would leave them waiting for something that is never coming.
    held: Option<Arc<Held>>,
}

#[derive(Debug)]
struct Held {
    /// the term this process holds, or 0 for "not the leader". a watch rather
    /// than an atomic so a loop that is waiting is woken the instant the lease
    /// is taken rather than on its next poll.
    term: watch::Sender<u64>,
}

impl Deciding {
    /// a process that decides without asking anybody: the default, and what
    /// one process on one database has always been.
    pub(crate) fn sole() -> Deciding {
        Deciding { held: None }
    }

    /// a process that decides only while it holds the lease, starting out
    /// holding nothing.
    pub(crate) fn elected() -> Deciding {
        Deciding {
            held: Some(Arc::new(Held {
                term: watch::Sender::new(0),
            })),
        }
    }

    /// whether this process may decide right now.
    pub(crate) fn leading(&self) -> bool {
        self.held.as_ref().is_none_or(|h| *h.term.borrow() != 0)
    }

    /// the term to write a decision under, and `None` for a process running no
    /// election, whose decisions are fenced by nothing because there is nothing
    /// to fence them against.
    pub(crate) fn term(&self) -> Option<u64> {
        self.held.as_ref().map(|h| *h.term.borrow())
    }

    /// wait until this process may decide. returns at once for a process that
    /// runs no election, and at once for a leader.
    pub(crate) async fn wait(&self) {
        let Some(held) = &self.held else { return };
        let mut rx = held.term.subscribe();
        // `wait_for` checks the current value before it waits, so a leader
        // does not sleep for a change it has already had
        let _ = rx.wait_for(|term| *term != 0).await;
    }

    fn set(&self, term: u64) {
        if let Some(held) = &self.held {
            held.term.send_replace(term);
        }
    }
}

/// take the lease now, before any loop starts, and say whether it was taken.
///
/// synchronous and one attempt: a single process finds the row free and is
/// deciding by the time this returns, which is what keeps the common
/// deployment exactly as fast as it was. a process that finds the lease held
/// waits for [`run_decider`] to get it, and waits doing nothing.
pub(crate) fn take_now(runner: &Runner) -> bool {
    let deciding = runner.deciding();
    match runner
        .store()
        .take_decision_lease(runner.instance(), DECIDE_LEASE)
    {
        Ok(Some(term)) => {
            tracing::info!(term, "deciding: this process holds the lease");
            deciding.set(term);
            true
        }
        Ok(None) => {
            let held = runner.store().decider().ok().and_then(|d| d.holder);
            tracing::info!(
                holder = held.as_deref().unwrap_or("somebody"),
                "waiting for the deciding lease; this process decides nothing until it has it"
            );
            false
        }
        Err(e) => {
            tracing::warn!("deciding: the lease could not be read: {e}");
            false
        }
    }
}

/// hand the lease back, so whoever is next does not have to wait it out.
///
/// only ever reached when `serve` returns, which a long-lived deployment does
/// not do. a process that is killed leaves its lease to expire on its own, and
/// that is the case the expiry is for.
pub(crate) fn hand_back(runner: &Runner) {
    let deciding = runner.deciding();
    let Some(term) = deciding.term().filter(|t| *t != 0) else {
        return;
    };
    deciding.set(0);
    match runner
        .store()
        .release_decision_lease(runner.instance(), term)
    {
        Ok(true) => tracing::info!(term, "deciding: lease handed back"),
        Ok(false) => {}
        Err(e) => tracing::warn!("deciding: the lease could not be handed back: {e}"),
    }
}

/// the loop `serve` runs on a process that decides: hold the lease, or wait for
/// it, forever.
///
/// **losing it is what this is for.** a renewal that comes back false is
/// somebody else's lease now. a renewal that *errors* is a store this process
/// cannot reach, which is the same thing one lease later: the row's
/// `lease_until` stops moving, anybody may take it, and this process must stop
/// believing it leads at exactly the moment the row says it stopped. so the
/// clock, not the error, is what ends the term.
pub(crate) async fn run_decider(runner: Runner) {
    let deciding = runner.deciding().clone();
    if deciding.held.is_none() {
        return;
    }
    let who = runner.instance();
    // when the row's `lease_until` was last actually moved. a term survives a
    // failed renewal only while this is inside one lease
    let mut renewed = Instant::now();
    loop {
        tokio::time::sleep(DECIDE_RENEW).await;
        match deciding.term() {
            Some(0) | None => {
                if take_now(&runner) {
                    renewed = Instant::now();
                }
            }
            Some(term) => match runner.store().renew_decision_lease(who, term, DECIDE_LEASE) {
                Ok(true) => renewed = Instant::now(),
                Ok(false) => {
                    tracing::warn!(term, "deciding: the lease is no longer this process's");
                    deciding.set(0);
                }
                Err(e) => {
                    tracing::warn!(term, "deciding: renewal failed: {e}");
                    if renewed.elapsed() >= DECIDE_LEASE {
                        tracing::warn!(
                            term,
                            "deciding: nothing has renewed this lease for a whole lease; \
                             this process stops deciding until it can take one again"
                        );
                        deciding.set(0);
                    }
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::json;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::asset::{Asset, AssetRegistry};
    use crate::executor::Launched;
    use crate::hooks::RunEvent;
    use crate::job::Job;
    use crate::model::{Catchup, Role, Run, RunStatus, RunTags, Trigger};
    use crate::op::{Op, OpCtx};
    use crate::partition::Partitions;
    use crate::policy::AutoPolicy;
    use crate::schedule::Schedule;
    use crate::sensor::{RunRequest, Sensor, SensorEntry};
    use crate::store::Store;

    // ----------------------------------------------------------- the handle

    #[tokio::test]
    async fn a_process_that_runs_no_election_decides_and_names_no_term() {
        let sole = Deciding::sole();
        assert!(sole.leading());
        assert_eq!(sole.term(), None);
        // and never waits for anything, which is what every one-shot needs
        sole.wait().await;
    }

    #[tokio::test]
    async fn an_elected_process_leads_nothing_until_it_holds_a_term() {
        let elected = Deciding::elected();
        assert!(!elected.leading());
        assert_eq!(elected.term(), Some(0));

        elected.set(7);
        assert!(elected.leading());
        assert_eq!(elected.term(), Some(7));
        // a leader waiting for the lease it already holds does not wait
        elected.wait().await;

        elected.set(0);
        assert!(!elected.leading());
    }

    #[tokio::test]
    async fn waiting_for_the_lease_wakes_the_instant_it_is_taken() {
        let elected = Deciding::elected();
        let waiter = {
            let elected = elected.clone();
            tokio::spawn(async move { elected.wait().await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished(), "it did not wait at all");
        elected.set(1);
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("a waiter was not woken by the lease being taken")
            .unwrap();
    }

    // ------------------------------------------------------------- the loop

    // the whole life of a lease, on a clock the test owns: refused while
    // somebody else holds it, taken when they let go, and dropped the moment a
    // renewal says it belongs to somebody else
    #[tokio::test(start_paused = true)]
    async fn the_decider_loop_takes_the_lease_holds_it_and_gives_it_up_when_it_is_lost() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("hestan.db").to_str().unwrap()).unwrap();
        let runner = waiting(Runner::new([noop_job("etl")], store.clone()).unwrap());
        let me = runner.instance().to_string();

        // somebody else has it, so the boot attempt comes away with nothing
        assert_eq!(
            store
                .take_decision_lease("other", Duration::from_secs(600))
                .unwrap(),
            Some(1)
        );
        assert!(!take_now(&runner));
        let task = tokio::spawn(run_decider(runner.clone()));

        turn().await;
        assert!(!runner.may_decide(), "it decided while another process led");

        // they let go, and the next turn of the loop is this process's
        assert!(store.release_decision_lease("other", 1).unwrap());
        turn().await;
        assert!(runner.may_decide(), "a free lease was not taken");
        assert_eq!(runner.deciding().term(), Some(2));
        assert!(store.decider().unwrap().held_by(&me, Utc::now()));

        // a renewal keeps the term where it is: a decision made under it is
        // still good a moment later, which is the whole point of a term
        turn().await;
        assert_eq!(runner.deciding().term(), Some(2));

        // and now it is somebody else's. this process finds out at its next
        // renewal and stops deciding there and then
        assert!(store.release_decision_lease(&me, 2).unwrap());
        assert_eq!(
            store
                .take_decision_lease("other", Duration::from_secs(600))
                .unwrap(),
            Some(3)
        );
        turn().await;
        assert!(
            !runner.may_decide(),
            "it went on deciding after losing the lease"
        );
        task.abort();
    }

    /// one turn of [`run_decider`], on a clock the test owns.
    ///
    /// the loop sleeps [`DECIDE_RENEW`] and then makes one blocking store call.
    /// advancing wakes it; the yields are what let it finish before the
    /// assertion reads what it did.
    async fn turn() {
        tokio::time::advance(DECIDE_RENEW + Duration::from_millis(1)).await;
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
    }

    // ------------------------------------------------- and every deciding loop

    /// how long a loop is given to do the thing it must not do. every loop
    /// under test polls faster than this, and the sensor under test is due
    /// every 10ms, so a loop that was going to act has had several turns.
    const SEVERAL_TURNS: Duration = Duration::from_millis(300);

    /// a runner that runs an election and has not won one.
    fn waiting(runner: Runner) -> Runner {
        runner.with_deciding(Deciding::elected())
    }

    /// hand `runner` the lease, as [`run_decider`] would on taking one.
    fn elect(runner: &Runner) {
        runner.deciding().set(1);
    }

    /// give a loop its several turns and then assert it did nothing.
    async fn did_nothing(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::sleep(SEVERAL_TURNS).await;
        assert!(!done(), "{what} without the deciding lease");
    }

    /// and then that it does it once it may, so the case is about the lease
    /// rather than about a fixture that was never going to do anything.
    async fn then_does(what: &str, task: JoinHandle<()>, mut done: impl FnMut() -> bool) {
        for _ in 0..500 {
            if done() {
                task.abort();
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        task.abort();
        panic!("{what} never happened, so the case above proved nothing");
    }

    fn noop_job(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new("noop", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn the_scheduler_loop_fires_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let runner = waiting(Runner::new([noop_job("etl")], store.clone()).unwrap());
        let entries = vec![crate::schedule::parse("etl", "* * * * * *", "UTC").unwrap()];
        let fired = || !store.ticks(None, 10).unwrap().is_empty();

        let task = tokio::spawn(crate::schedule::run_scheduler(entries, runner.clone()));
        did_nothing("the scheduler wrote a tick", fired).await;
        elect(&runner);
        then_does("a tick", task, fired).await;
    }

    #[tokio::test]
    async fn the_sensor_loop_evaluates_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let runner = waiting(Runner::new([noop_job("etl")], store.clone()).unwrap());
        let entries = vec![SensorEntry::user(Sensor::new(
            "tireless",
            Duration::from_millis(10),
            |_| async { Ok(vec![RunRequest::new("etl")]) },
        ))];
        let ticked = || !store.sensor_ticks(None, 10).unwrap().is_empty();

        let task = tokio::spawn(crate::sensor::run_sensors(
            entries,
            runner.clone(),
            Arc::new(AssetRegistry::empty()),
        ));
        did_nothing("a sensor evaluated", ticked).await;
        elect(&runner);
        then_does("a sensor tick", task, ticked).await;
    }

    /// one partitioned asset over five keys, two to a chunk.
    fn regions() -> Arc<AssetRegistry> {
        let sales = Asset::new("sales", |ctx: OpCtx| async move {
            Ok(json!({ "region": ctx.partition() }))
        })
        .partitioned(Partitions::keys(["r1", "r2", "r3", "r4", "r5"]).build_limit(2));
        Arc::new(AssetRegistry::new(vec![sales], Vec::new(), Vec::new()).unwrap())
    }

    #[tokio::test]
    async fn the_backfill_chunker_launches_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let reg = regions();
        // scheduler, so the chunk it launches stays queued and the second pass
        // sees a build in flight exactly as a split deployment would
        let runner = waiting(
            Runner::new([reg.lower_job().unwrap()], store.clone())
                .unwrap()
                .with_role(Role::Scheduler, 1),
        );
        let keys: Vec<String> = ["r1", "r2", "r3"].iter().map(|k| k.to_string()).collect();
        store
            .create_backfill("sales", "r1", "r3", &keys, None)
            .unwrap();
        let launched = || {
            store
                .running_backfills()
                .unwrap()
                .first()
                .is_some_and(|b| b.launched > 0)
        };

        let task = tokio::spawn(crate::backfill::run_backfills(runner.clone(), reg.clone()));
        did_nothing("the chunker launched a chunk", launched).await;
        elect(&runner);
        then_does("a backfill chunk", task, launched).await;
    }

    #[tokio::test]
    async fn the_freshness_checker_records_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let job = Job::builder("etl")
            .fresh_within(Duration::from_secs(3600))
            .op(Op::new("noop", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap();
        let runner = waiting(Runner::new([job], store.clone()).unwrap());
        // a success from yesterday: an hourly policy has been missed since
        let old = Utc::now() - chrono::Duration::days(1);
        store.create_run(&planted("r1", "etl", old), &[]).unwrap();
        store
            .run_finished("r1", RunStatus::Success, None, old, None)
            .unwrap();
        store.backdate_run("r1", old).unwrap();
        let recorded = || !store.freshness_states().unwrap().is_empty();

        let task = tokio::spawn(crate::freshness::run_checker(
            runner.clone(),
            Arc::new(AssetRegistry::empty()),
            Arc::new(Vec::new()),
        ));
        did_nothing("the checker recorded a crossing", recorded).await;
        elect(&runner);
        then_does("a freshness crossing", task, recorded).await;
    }

    #[tokio::test]
    async fn the_policy_pass_builds_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let sales =
            Asset::new("sales", |_: OpCtx| async { Ok(json!(1)) }).policy(AutoPolicy::when_stale());
        let reg = Arc::new(AssetRegistry::new(vec![sales], Vec::new(), Vec::new()).unwrap());
        let runner = waiting(
            Runner::new([reg.lower_job().unwrap()], store.clone())
                .unwrap()
                .with_role(Role::Scheduler, 1),
        );
        let built = || {
            !store
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        };

        let task = tokio::spawn(crate::policy::run_policies(runner.clone(), reg.clone()));
        did_nothing("the policy pass launched a build", built).await;
        elect(&runner);
        then_does("a policy build", task, built).await;
    }

    #[tokio::test]
    async fn the_retention_sweeper_prunes_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let runner = waiting(Runner::new([noop_job("etl")], store.clone()).unwrap());
        let old = Utc::now() - chrono::Duration::days(30);
        store.create_run(&planted("r1", "etl", old), &[]).unwrap();
        store
            .run_finished("r1", RunStatus::Success, None, old, None)
            .unwrap();
        store.backdate_run("r1", old).unwrap();
        let pruned = || store.run("r1").unwrap().is_none();

        let task = tokio::spawn(crate::retention::run_sweeper(
            runner.clone(),
            crate::retention::Retention::days(1),
            Duration::from_millis(20),
        ));
        did_nothing("the sweeper deleted a run", pruned).await;
        elect(&runner);
        then_does("a sweep", task, pruned).await;
    }

    #[tokio::test]
    async fn the_delivery_loop_delivers_nothing_without_the_lease() {
        let store = Store::open(":memory:").unwrap();
        let sent = Arc::new(Mutex::new(0usize));
        let counting = {
            let sent = sent.clone();
            move |_: RunEvent| {
                *sent.lock().unwrap() += 1;
            }
        };
        let runner = waiting(
            Runner::new([noop_job("etl")], store.clone())
                .unwrap()
                .with_hooks(vec![Arc::new(counting)], Vec::new())
                .with_durable_notifications(),
        );
        let at = Utc::now();
        store.create_run(&planted("r1", "etl", at), &[]).unwrap();
        store
            .run_finished(
                "r1",
                RunStatus::Success,
                None,
                at,
                Some(&json!({
                    "run_id": "r1",
                    "job": "etl",
                    "trigger": "manual",
                    "status": "success",
                    "failed_op": null,
                    "error": null,
                    "started_at": null,
                    "finished_at": at,
                    "duration_secs": null,
                })),
            )
            .unwrap();
        let delivered = || *sent.lock().unwrap() > 0;

        let task = tokio::spawn(crate::hooks::run_delivery(runner.clone()));
        did_nothing("the delivery loop sent an alert", delivered).await;
        elect(&runner);
        then_does("a delivery", task, delivered).await;
    }

    // ------------------------------------------------------------- the fence

    // the handle the deciding loops launch through, and the one everything else
    // does. a process that has stopped being the decider and does not know it
    // is refused by the store on the first, and untouched on the second
    #[tokio::test]
    async fn a_deciding_handle_is_refused_once_its_term_has_moved_on() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path().join("hestan.db").to_str().unwrap()).unwrap();
        let runner = waiting(Runner::new([noop_job("etl")], store.clone()).unwrap());
        assert!(take_now(&runner));
        let decides = runner.as_decider();
        let hour = |h: u32| {
            chrono::DateTime::parse_from_rfc3339(&format!("2026-05-01T{h:02}:00:00+00:00"))
                .unwrap()
                .with_timezone(&Utc)
        };

        // while it holds the lease, a fire lands exactly as it always did
        assert!(matches!(
            decides
                .fire_scheduled("etl", "0 * * * *", hour(9), json!({}), false)
                .unwrap(),
            Launched::Queued(_)
        ));

        // the lease moves on. this handle still believes it holds term 1: the
        // whole point is that nothing in this process has noticed
        let term = runner.deciding().term().unwrap();
        assert!(
            store
                .release_decision_lease(runner.instance(), term)
                .unwrap()
        );
        assert_eq!(
            store
                .take_decision_lease("somebody-else", Duration::from_secs(600))
                .unwrap(),
            Some(term + 1)
        );
        assert!(
            runner.may_decide(),
            "the fixture is not the case being made"
        );

        assert_eq!(
            decides
                .fire_scheduled("etl", "0 * * * *", hour(10), json!({}), false)
                .unwrap(),
            Launched::Stale
        );
        assert!(
            store
                .ticks(Some("etl"), 50)
                .unwrap()
                .iter()
                .all(|t| t.scheduled_for != hour(10)),
            "a stale decider left a tick behind"
        );
        assert_eq!(
            store.runs(None, None, None, None, None, 50).unwrap().len(),
            1
        );

        // and a launch that is not a decision is not fenced by any of this:
        // the api and the ui go on working on a process that is not the
        // decider, which is most of them
        runner
            .launch("etl", json!({}), Trigger::Manual)
            .expect("a manual launch is nobody's decision to lose");
        assert_eq!(
            store.runs(None, None, None, None, None, 50).unwrap().len(),
            2
        );
    }

    // ------------------------------------------------------------- handover

    /// a scheduler on an hourly cron, with the cursor left where a decider
    /// that died `hours` ago left it, and this process not the decider yet.
    ///
    /// this is a handover as the store sees one: the cursor is the record of
    /// how far the *deployment* has accounted for, and it does not care which
    /// process wrote it.
    fn after_a_gap(
        catchup: Catchup,
        hours: i64,
    ) -> (Store, Runner, Vec<crate::schedule::ScheduleEntry>) {
        let store = Store::open(":memory:").unwrap();
        let schedule = Schedule::new("etl", "0 * * * *").catchup(catchup);
        store
            .sync_schedules(std::slice::from_ref(&schedule))
            .unwrap();
        store
            .set_schedule_cursor(
                "etl",
                "0 * * * *",
                Utc::now() - chrono::Duration::hours(hours),
            )
            .unwrap();
        // scheduler, so a caught-up run stays queued: this is the deployment
        // shape a handover happens in
        let runner = waiting(
            Runner::new([noop_job("etl")], store.clone())
                .unwrap()
                .with_role(Role::Scheduler, 1),
        );
        let entries = vec![
            crate::schedule::parse("etl", "0 * * * *", "UTC")
                .unwrap()
                .with_catchup(catchup),
        ];
        (store, runner, entries)
    }

    /// every occurrence the tick log has anything to say about, newest first.
    fn accounted(store: &Store) -> Vec<(crate::model::TickOutcome, chrono::DateTime<Utc>)> {
        store
            .ticks(Some("etl"), 50)
            .unwrap()
            .into_iter()
            .map(|t| (t.outcome, t.scheduled_for))
            .collect()
    }

    // **the documented answer**: an occurrence due while nobody was deciding is
    // an occurrence due during downtime, and the schedule's own catch-up policy
    // is what decides. the default is to skip it.
    #[tokio::test]
    async fn an_occurrence_due_during_a_handover_is_skipped_under_the_default_policy() {
        let (store, runner, entries) = after_a_gap(Catchup::Skip, 3);
        let task = tokio::spawn(crate::schedule::run_scheduler(entries, runner.clone()));
        did_nothing("the scheduler caught up", || !accounted(&store).is_empty()).await;

        elect(&runner);
        // the cursor jumping the gap is the whole of what `skip` does, so that
        // is what is waited for: no tick is ever written for a skipped gap
        let cursor = wait_cursor(&store).await;
        task.abort();
        assert!(
            cursor > Utc::now() - chrono::Duration::hours(1),
            "the gap was not accounted for: {cursor}"
        );
        assert_eq!(
            accounted(&store),
            Vec::new(),
            "skip fired something, or logged the occurrences it did not fire"
        );
        assert!(
            store
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    // and under `one`, it is fired late: the most recent missed occurrence,
    // and only that one
    #[tokio::test]
    async fn an_occurrence_due_during_a_handover_is_fired_late_under_catchup_one() {
        let (store, runner, entries) = after_a_gap(Catchup::One, 3);
        let task = tokio::spawn(crate::schedule::run_scheduler(entries, runner.clone()));
        did_nothing("the scheduler caught up", || !accounted(&store).is_empty()).await;

        elect(&runner);
        let ticks = wait_ticks(&store, 1).await;
        task.abort();
        assert_eq!(ticks.len(), 1, "{ticks:?}");
        assert_eq!(ticks[0].0, crate::model::TickOutcome::Fired);
        // the occurrence it stands for is the missed one, not the wall clock
        // it was fired at
        let run = store
            .runs(None, None, None, None, None, 10)
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(run.scheduled_for, Some(ticks[0].1));
        assert!(run.scheduled_for.unwrap() < Utc::now());
    }

    // and under `all`, every one of them is accounted for, oldest first: the
    // ones the job has room for fire and the rest are held, which is the same
    // thing `all` does after any other downtime
    #[tokio::test]
    async fn every_occurrence_due_during_a_handover_is_accounted_for_under_catchup_all() {
        let (store, runner, entries) = after_a_gap(Catchup::All { limit: 10 }, 3);
        let task = tokio::spawn(crate::schedule::run_scheduler(entries, runner.clone()));
        did_nothing("the scheduler caught up", || !accounted(&store).is_empty()).await;

        elect(&runner);
        let ticks = wait_ticks(&store, 3).await;
        task.abort();
        let mut occurrences: Vec<chrono::DateTime<Utc>> = ticks.iter().map(|(_, at)| *at).collect();
        occurrences.sort();
        occurrences.dedup();
        assert!(
            occurrences.len() >= 3,
            "the gap held three occurrences and {} were accounted for",
            occurrences.len()
        );
        assert_eq!(
            ticks
                .iter()
                .filter(|(o, _)| *o == crate::model::TickOutcome::Fired)
                .count(),
            1,
            "a job that overlaps by skipping launched more than one at a time"
        );
    }

    // the other half of the handover: a fire that was already *queued* when the
    // decider went away. the tick log is the queue (phase 18), so it survives,
    // and the new decider drains it as its own
    #[tokio::test]
    async fn a_fire_queued_before_a_handover_is_launched_after_it() {
        let store = Store::open(":memory:").unwrap();
        let schedule = Schedule::new("etl", "0 * * * *");
        store
            .sync_schedules(std::slice::from_ref(&schedule))
            .unwrap();
        // the cursor is up to date, so nothing here is catch-up: this is one
        // occurrence the dead decider deferred and nothing else
        let held = Utc::now() - chrono::Duration::minutes(20);
        store
            .set_schedule_cursor("etl", "0 * * * *", Utc::now())
            .unwrap();
        store
            .record_tick(
                "etl",
                "0 * * * *",
                held,
                crate::model::TickOutcome::Deferred,
                false,
                None,
                None,
            )
            .unwrap();
        let runner = waiting(
            Runner::new([noop_job("etl")], store.clone())
                .unwrap()
                .with_role(Role::Scheduler, 1),
        );
        let entries = vec![crate::schedule::parse("etl", "0 * * * *", "UTC").unwrap()];
        let drained =
            || {
                store.ticks(Some("etl"), 50).unwrap().iter().any(|t| {
                    t.outcome == crate::model::TickOutcome::Fired && t.scheduled_for == held
                })
            };

        let task = tokio::spawn(crate::schedule::run_scheduler(entries, runner.clone()));
        did_nothing("the held fire was drained", drained).await;
        assert!(
            !store.pending_fires().unwrap().is_empty(),
            "the fixture lost its queued fire before the handover"
        );
        elect(&runner);
        then_does("the queued fire launching", task, drained).await;
        assert!(
            store.pending_fires().unwrap().is_empty(),
            "the fire is still queued after it launched"
        );
    }

    /// this schedule's cursor, once it has moved off the gap.
    async fn wait_cursor(store: &Store) -> chrono::DateTime<Utc> {
        for _ in 0..500 {
            if let Some(at) = store
                .schedules()
                .unwrap()
                .first()
                .and_then(|r| r.cursor)
                .filter(|at| *at > Utc::now() - chrono::Duration::hours(2))
            {
                return at;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the cursor never moved off the gap");
    }

    /// the tick log once it holds at least `n` rows.
    async fn wait_ticks(
        store: &Store,
        n: usize,
    ) -> Vec<(crate::model::TickOutcome, chrono::DateTime<Utc>)> {
        for _ in 0..500 {
            let ticks = accounted(store);
            if ticks.len() >= n {
                // one more turn, so a pass that was going to write a fourth is
                // not read halfway through
                tokio::time::sleep(Duration::from_millis(50)).await;
                return accounted(store);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the tick log never reached {n} rows");
    }

    /// a queued run of `job`, created at `at`.
    fn planted(id: &str, job: &str, at: chrono::DateTime<Utc>) -> Run {
        Run {
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
        }
    }
}
