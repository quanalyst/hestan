use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::executor::{Runner, panic_payload};
use crate::model::{OpStatus, RunStatus, Trigger};

/// what an [`on_run_finished`](crate::Hestan::on_run_finished) hook receives:
/// a run reached a terminal status, whichever one.
///
/// success, failure and cancellation all arrive here and `status` says which —
/// which is the whole difference from [`RunFailure`], and the reason a hook
/// that wants only failures filters on it. a run the boot sweep marked failed
/// is not here: nothing executed it, and a restart after a crash should not
/// replay a morning of old failures into an alert channel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    /// the run, for anything that wants to link back to it.
    pub run_id: String,
    /// which job it was of.
    pub job: String,
    /// what caused the run — worth filtering on: a failed retry and a failed
    /// nightly are usually not the same page.
    pub trigger: Trigger,
    /// how it ended. this is the field a hook that only wants failures
    /// filters on.
    pub status: RunStatus,
    /// the first op that terminally failed this run; `None` unless one did.
    pub failed_op: Option<String>,
    /// that op's own error message, which is what the run row says after
    /// `op {failed_op} failed: `.
    pub error: Option<String>,
    /// when the run began executing. `None` for a run that never got that far
    /// — one whose claimer went away before it started, say.
    pub started_at: Option<DateTime<Utc>>,
    /// when it reached its terminal status, which is a moment before this hook
    /// was called.
    pub finished_at: DateTime<Utc>,
    /// how long it executed for, which is not how long it existed for: a run
    /// held on the queue by a limit was not running while it waited.
    #[serde(rename = "duration_secs", with = "maybe_secs")]
    pub duration: Option<Duration>,
}

/// what an [`on_op_finished`](crate::Hestan::on_op_finished) hook receives:
/// one **attempt** of one op ended.
///
/// per attempt, not per op, and that is the useful shape rather than an
/// accident: an op that failed twice and worked on the third try is three
/// facts, and a hook that only wants the last one filters on `status`. an op
/// skipped by its [trigger rule](crate::When), or one canceled before it was
/// ever spawned, produces nothing at all — there was no attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpEvent {
    /// the run this attempt belonged to.
    pub run_id: String,
    /// the job that run was of.
    pub job: String,
    /// the op, by its flattened name — `{instance}.{op}` inside a
    /// [`Graph`](crate::Graph), `{op}[{i}]` for one fan-out instance.
    pub op: String,
    /// which attempt this was, counting from 1.
    pub attempt: u32,
    /// how this attempt ended. a `failed` attempt with another to come and the
    /// one that ends the op look identical here — `attempt` against
    /// [`Op::max_retries`](crate::Op::max_retries) is the difference.
    pub status: OpStatus,
    /// what it failed with; `None` unless it did.
    pub error: Option<String>,
    /// when **this attempt** started, which on a retry is later than the
    /// `started_at` on the op run row: that one keeps the first attempt's.
    pub started_at: DateTime<Utc>,
    /// when this attempt ended.
    pub finished_at: DateTime<Utc>,
    /// how long this attempt took — the retries before it are their own
    /// events, with their own durations.
    #[serde(rename = "duration_secs", with = "secs")]
    pub duration: Duration,
}

/// what a failure hook receives when a run finishes failed.
///
/// the older, narrower shape of [`RunEvent`], kept because
/// [`on_failure`](crate::Hestan::on_failure) and the built-in
/// [notifiers][n] are written against it. it is dispatched as a run hook that
/// filters on the status, so there is one path an event can be missed from
/// rather than two.
///
#[cfg_attr(feature = "http", doc = "[n]: crate::notify")]
#[cfg_attr(not(feature = "http"), doc = "[n]: crate")]
#[derive(Debug, Clone, Serialize)]
pub struct RunFailure {
    /// the run that failed.
    pub run_id: String,
    /// which job it was of.
    pub job: String,
    /// what caused the run.
    pub trigger: Trigger,
    /// the first op that terminally failed this run.
    pub failed_op: Option<String>,
    /// that op's error message.
    pub error: Option<String>,
    /// when the run failed.
    pub finished_at: DateTime<Utc>,
}

/// a callback invoked on its own task when a run finishes failed.
pub type FailureHook = Arc<dyn Fn(RunFailure) + Send + Sync>;

/// a callback invoked on its own task when a run reaches a terminal status.
pub type RunHook = Arc<dyn Fn(RunEvent) + Send + Sync>;

/// a callback invoked on its own task when one attempt of an op ends.
pub type OpHook = Arc<dyn Fn(OpEvent) + Send + Sync>;

/// the hooks registered against one job, beside the ones the process
/// registered for every job.
#[derive(Clone, Default)]
pub(crate) struct Hooks {
    pub run: Vec<RunHook>,
    pub op: Vec<OpHook>,
}

// a closure has nothing to print, and a job that derives Debug still has to
// say something: how many were registered is the only fact here
impl std::fmt::Debug for Hooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hooks")
            .field("run", &self.run.len())
            .field("op", &self.op.len())
            .finish()
    }
}

/// an [`on_failure`](crate::Hestan::on_failure) hook as a run hook.
///
/// the old callback over the new path rather than beside it: one dispatch, one
/// place an event can be missed from, and no second traversal of the executor
/// to keep in step with this one. the filter is exactly what `on_failure` has
/// always promised — a canceled run notifies nobody, because somebody asked it
/// to stop and paging on that teaches people to ignore the page.
pub(crate) fn as_run_hook(hook: FailureHook) -> RunHook {
    Arc::new(move |e: RunEvent| {
        if e.status != RunStatus::Failed {
            return;
        }
        hook(RunFailure {
            run_id: e.run_id,
            job: e.job,
            trigger: e.trigger,
            failed_op: e.failed_op,
            error: e.error,
            finished_at: e.finished_at,
        });
    })
}

/// hand `event` to every hook, each on its own blocking task.
///
/// spawn_blocking, not spawn: a hook that blocks outright — a sync http post,
/// a database write, a sleep — would otherwise pin an async worker and hang
/// runtime shutdown. a panicking hook is caught and logged rather than taken
/// out on the others. `what` names the hook family in that warning.
pub(crate) fn fire_hooks<E: Clone + Send + 'static>(
    hooks: &[Arc<dyn Fn(E) + Send + Sync>],
    event: E,
    what: &'static str,
) {
    for hook in hooks {
        let hook = hook.clone();
        let event = event.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| hook(event))) {
                match panic_payload(panic.as_ref()) {
                    Some(s) => tracing::warn!("{what} hook panicked: {s}"),
                    None => tracing::warn!("{what} hook panicked"),
                }
            }
        });
    }
}

/// how often the delivery loop looks for work. a notification that waited five
/// seconds is still an alert; polling harder would only cost reads against a
/// table that is nearly always empty.
const DELIVER_EVERY: Duration = Duration::from_secs(5);

/// how many rows one pass takes. a cap rather than a preference: a hook that
/// posts to a slow endpoint would otherwise hold the loop for as long as the
/// backlog is, and the next pass is five seconds away.
const DELIVER_BATCH: u32 = 50;

/// how many attempts a notification gets before hestan stops trying.
///
/// eight attempts is seven gaps, so with the growth below that is about
/// twenty minutes of retrying at the outside and nearer ten once the jitter
/// is counted — which covers a restart of whatever is on the other end and
/// does not cover a url that was wrong when it was typed. past it the row stays, failed, with the
/// error that stopped it: giving up **loudly** is the whole difference from
/// the best-effort dispatch this exists beside.
const MAX_ATTEMPTS: u32 = 8;

/// the first retry gap, doubled per attempt up to [`RETRY_MAX`] with full
/// jitter — the same pacing an op's retries use, for the same reason: a
/// hundred notifications for the same outage must not retry in lockstep.
///
/// the ceiling is the shared one rather than a reachable limit here:
/// [`MAX_ATTEMPTS`] runs out at a 640-second gap, so nothing this loop
/// schedules ever meets it.
const RETRY_BASE: Duration = Duration::from_secs(10);
const RETRY_MAX: Duration = Duration::from_secs(30 * 60);

/// deliver whatever is due, and return how many rows were settled either way.
///
/// **at-least-once.** a crash between a hook returning and the row being
/// marked delivered re-delivers on the next pass, because the alternative is
/// marking first and losing the delivery instead — and of the two, a receiver
/// seeing an alert twice is the one you can do something about. exactly-once
/// needs the receiver's cooperation and hestan does not pretend to have it.
pub(crate) async fn deliver_once(runner: &Runner, now: DateTime<Utc>) -> usize {
    let store = runner.store();
    let due = match store.due_notifications(now, DELIVER_BATCH) {
        Ok(due) => due,
        Err(e) => {
            tracing::warn!("reading due notifications failed: {e}");
            return 0;
        }
    };
    let mut settled = 0;
    for row in due {
        let event: RunEvent = match serde_json::from_value(row.payload.clone()) {
            Ok(event) => event,
            // nothing will ever parse this, so retrying is a loop rather than
            // a hope: give up on it now, with the reason on the row
            Err(e) => {
                let why = format!("payload is not a run event: {e}");
                tracing::warn!(notification = row.id, "{why}");
                store
                    .landed("delivery_failed", || {
                        store.delivery_failed(row.id, row.attempts, None, &why)
                    })
                    .await;
                continue;
            }
        };
        let hooks = runner.run_hooks(&event.job);
        match deliver(&hooks, event).await {
            // a mark that does not land is the at-least-once window itself:
            // the row stays due and the next pass delivers it again, which is
            // the side of it this table was built to fall on
            Ok(()) => {
                store
                    .landed("delivered", || {
                        store.delivered(row.id, Utc::now()).map(|_| ())
                    })
                    .await;
                settled += 1;
            }
            Err(why) => {
                let attempts = row.attempts + 1;
                let next = (attempts < MAX_ATTEMPTS).then(|| {
                    now + chrono::Duration::from_std(crate::backoff::jittered_exponential(
                        RETRY_BASE,
                        attempts - 1,
                        RETRY_MAX,
                    ))
                    .unwrap_or(chrono::Duration::zero())
                });
                if next.is_none() {
                    tracing::warn!(
                        notification = row.id,
                        "giving up after {attempts} attempts: {why}"
                    );
                }
                store
                    .landed("delivery_failed", || {
                        store.delivery_failed(row.id, attempts, next, &why)
                    })
                    .await;
                settled += 1;
            }
        }
    }
    settled
}

/// hand one event to every hook and wait for all of them.
///
/// waited on, unlike the best-effort dispatch: the loop has to know whether
/// this row is delivered, and a hook that panics is how it is told the answer
/// is no. one failure fails the row, so the hooks that did work will see the
/// event again on the retry — which is what at-least-once means and why it is
/// written down where the api is.
async fn deliver(hooks: &[RunHook], event: RunEvent) -> Result<(), String> {
    for hook in hooks {
        let hook = hook.clone();
        let event = event.clone();
        // spawn_blocking for the same reason the other dispatch uses it: a
        // hook is allowed to block outright, and doing that on an async worker
        // would pin it
        let ran = tokio::task::spawn_blocking(move || {
            std::panic::catch_unwind(AssertUnwindSafe(|| hook(event)))
        })
        .await;
        match ran {
            Ok(Ok(())) => {}
            Ok(Err(panic)) => {
                return Err(panic_payload(panic.as_ref())
                    .unwrap_or("hook panicked")
                    .to_string());
            }
            Err(e) => return Err(format!("delivery task failed: {e}")),
        }
    }
    Ok(())
}

/// the delivery loop: its own task, so nothing it waits on is anything a run
/// waits on.
pub(crate) async fn run_delivery(runner: Runner) {
    let mut ticker = tokio::time::interval(DELIVER_EVERY);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        deliver_once(&runner, Utc::now()).await;
    }
}

/// a duration on the wire is seconds. a float, because plenty of ops finish
/// inside one and an integer would report every one of them as zero.
mod maybe_secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.serialize_f64(d.as_secs_f64()),
            None => s.serialize_none(),
        }
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<Option<Duration>, D::Error> {
        // a nan or a negative is not a duration; nothing hestan writes is
        // either, and a payload read back off a table nobody else writes to is
        // not worth an error path
        Ok(Option::<f64>::deserialize(d)?.and_then(|secs| Duration::try_from_secs_f64(secs).ok()))
    }
}

mod secs {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_f64(d.as_secs_f64())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        Ok(Duration::try_from_secs_f64(f64::deserialize(d)?).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DeliveryState, Run, RunTags};
    use crate::store::Store;
    use serde_json::json;
    use std::sync::Mutex;

    /// a failed run with its notification, written the way the executor writes
    /// them: one transaction, one row each.
    fn plant(store: &Store, run_id: &str) {
        let now = Utc::now();
        store
            .create_run(
                &Run {
                    id: run_id.to_string(),
                    job: "etl".into(),
                    status: RunStatus::Queued,
                    trigger: Trigger::Schedule,
                    params: json!({}),
                    created_at: now,
                    started_at: Some(now),
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
                },
                &["load".to_string()],
            )
            .unwrap();
        let payload = serde_json::to_value(RunEvent {
            run_id: run_id.to_string(),
            job: "etl".into(),
            trigger: Trigger::Schedule,
            status: RunStatus::Failed,
            failed_op: Some("load".into()),
            error: Some("no good".into()),
            started_at: Some(now),
            finished_at: now,
            duration: Some(Duration::from_secs(1)),
        })
        .unwrap();
        store
            .run_finished(run_id, RunStatus::Failed, None, now, Some(&payload))
            .unwrap();
    }

    fn collector() -> (Arc<Mutex<Vec<RunEvent>>>, RunHook) {
        let seen: Arc<Mutex<Vec<RunEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        (seen, Arc::new(move |e| sink.lock().unwrap().push(e)))
    }

    fn runner(store: Store, hook: RunHook) -> Runner {
        Runner::new(Vec::new(), store)
            .unwrap()
            .with_hooks(vec![hook], Vec::new())
            .with_durable_notifications()
    }

    // the hole the whole part closes: the process that decided to send the
    // alert did not survive to send it, and the next one does
    #[tokio::test]
    async fn a_notification_survives_a_restart_and_the_next_process_delivers_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap();

        // one process writes the run and dies with the alert unsent
        let first = Store::open(path).unwrap();
        plant(&first, "r1");
        drop(first);

        let (seen, hook) = collector();
        let next = runner(Store::open(path).unwrap(), hook);
        assert_eq!(deliver_once(&next, Utc::now()).await, 1);

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].run_id, "r1");
        assert_eq!(seen[0].status, RunStatus::Failed);
        assert_eq!(seen[0].failed_op.as_deref(), Some("load"));
        assert_eq!(seen[0].error.as_deref(), Some("no good"));
        assert_eq!(seen[0].duration, Some(Duration::from_secs(1)));
        let rows = next.store().notifications(None, 10).unwrap();
        assert_eq!(rows[0].state, DeliveryState::Delivered);
    }

    #[tokio::test]
    async fn delivery_marks_exactly_once_in_the_happy_path() {
        let store = Store::open(":memory:").unwrap();
        plant(&store, "r1");
        let (seen, hook) = collector();
        let runner = runner(store, hook);

        assert_eq!(deliver_once(&runner, Utc::now()).await, 1);
        // nothing is due any more, so a second pass finds nothing to do
        assert_eq!(deliver_once(&runner, Utc::now()).await, 0);
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    // at-least-once, exercised rather than asserted: a crash between the hook
    // returning and the mark landing re-delivers, and a hook has to be able to
    // see the same event twice
    #[tokio::test]
    async fn a_crash_before_the_mark_delivers_the_same_event_again() {
        let store = Store::open(":memory:").unwrap();
        plant(&store, "r1");
        let (seen, hook) = collector();
        let runner = runner(store.clone(), hook);

        deliver_once(&runner, Utc::now()).await;
        let id = store.notifications(None, 10).unwrap()[0].id;
        store.undeliver(id, Utc::now()).unwrap();

        assert_eq!(deliver_once(&runner, Utc::now()).await, 1);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2, "the redelivery never happened");
        assert_eq!(seen[0], seen[1]);
        // and the second one settles it, so it does not go round forever
        assert_eq!(
            store.notifications(None, 10).unwrap()[0].state,
            DeliveryState::Delivered
        );
    }

    #[tokio::test]
    async fn a_failing_hook_retries_and_gives_up_loudly() {
        let store = Store::open(":memory:").unwrap();
        plant(&store, "r1");
        let tries = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = tries.clone();
        let hook: RunHook = Arc::new(move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("the endpoint is down");
        });
        let runner = runner(store.clone(), hook);

        // far enough forward each pass that whatever backoff was set is due
        let mut now = Utc::now();
        for attempt in 1..=MAX_ATTEMPTS {
            assert_eq!(deliver_once(&runner, now).await, 1);
            let row = &store.notifications(None, 10).unwrap()[0];
            assert_eq!(row.attempts, attempt);
            assert_eq!(row.last_error.as_deref(), Some("the endpoint is down"));
            assert_eq!(
                row.state,
                if attempt < MAX_ATTEMPTS {
                    DeliveryState::Pending
                } else {
                    DeliveryState::Failed
                }
            );
            now += chrono::Duration::hours(2);
        }

        // given up on, and out of the loop's way rather than retried forever
        assert_eq!(
            tries.load(std::sync::atomic::Ordering::SeqCst),
            MAX_ATTEMPTS
        );
        assert_eq!(deliver_once(&runner, now).await, 0);
        assert_eq!(
            tries.load(std::sync::atomic::Ordering::SeqCst),
            MAX_ATTEMPTS
        );
        // and it stays visible, with what stopped it
        let failed = store
            .notifications(Some(DeliveryState::Failed), 10)
            .unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(
            failed[0].last_error.as_deref(),
            Some("the endpoint is down")
        );
    }

    fn event(status: RunStatus) -> RunEvent {
        RunEvent {
            run_id: "r1".into(),
            job: "etl".into(),
            trigger: Trigger::Schedule,
            status,
            failed_op: Some("load".into()),
            error: Some("no good".into()),
            started_at: Some(Utc::now()),
            finished_at: Utc::now(),
            duration: Some(Duration::from_millis(1500)),
        }
    }

    // what `on_failure` promised before there was anything else to promise
    #[test]
    fn a_failure_hook_over_the_new_path_still_sees_only_failures() {
        let seen: Arc<Mutex<Vec<RunFailure>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let hook = as_run_hook(Arc::new(move |f: RunFailure| {
            sink.lock().unwrap().push(f);
        }));

        for status in [RunStatus::Success, RunStatus::Canceled, RunStatus::Failed] {
            hook(event(status));
        }

        let got = seen.lock().unwrap();
        assert_eq!(got.len(), 1, "a success or a cancel reached on_failure");
        assert_eq!(got[0].run_id, "r1");
        assert_eq!(got[0].job, "etl");
        assert_eq!(got[0].trigger, Trigger::Schedule);
        assert_eq!(got[0].failed_op.as_deref(), Some("load"));
        assert_eq!(got[0].error.as_deref(), Some("no good"));
    }

    // the payload durable delivery stores and reads back later
    #[test]
    fn a_run_event_round_trips_through_json() {
        let event = event(RunStatus::Failed);
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["status"], "failed");
        assert_eq!(json["duration_secs"], 1.5);
        assert_eq!(serde_json::from_value::<RunEvent>(json).unwrap(), event);

        let never_started = RunEvent {
            started_at: None,
            duration: None,
            ..event
        };
        let json = serde_json::to_value(&never_started).unwrap();
        assert_eq!(json["duration_secs"], serde_json::Value::Null);
        assert_eq!(
            serde_json::from_value::<RunEvent>(json).unwrap(),
            never_started
        );
    }

    #[test]
    fn an_op_event_round_trips_through_json() {
        let event = OpEvent {
            run_id: "r1".into(),
            job: "etl".into(),
            op: "load".into(),
            attempt: 3,
            status: OpStatus::Success,
            error: None,
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration: Duration::from_millis(250),
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["attempt"], 3);
        assert_eq!(json["duration_secs"], 0.25);
        assert_eq!(serde_json::from_value::<OpEvent>(json).unwrap(), event);
    }
}
