use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::executor::panic_payload;
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
    pub run_id: String,
    pub job: String,
    pub trigger: Trigger,
    pub status: RunStatus,
    /// the first op that terminally failed this run; `None` unless one did.
    pub failed_op: Option<String>,
    /// that op's own error message, which is what the run row says after
    /// `op {failed_op} failed: `.
    pub error: Option<String>,
    /// when the run began executing. `None` for a run that never got that far
    /// — one whose claimer went away before it started, say.
    pub started_at: Option<DateTime<Utc>>,
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
    pub run_id: String,
    pub job: String,
    pub op: String,
    /// which attempt this was, counting from 1.
    pub attempt: u32,
    pub status: OpStatus,
    pub error: Option<String>,
    /// when **this attempt** started, which on a retry is later than the
    /// `started_at` on the op run row: that one keeps the first attempt's.
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    #[serde(rename = "duration_secs", with = "secs")]
    pub duration: Duration,
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
    use std::sync::Mutex;

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
