//! the `http` feature: two ready-made hooks, for the deployment that wants an
//! alert somewhere before it wants an integration.
//!
//! every hook hestan has takes a plain closure, and these are closures: there
//! is nothing here you could not have written, and nothing that knows more
//! than a hook does. what they save is the http client, the ten-second
//! timeout, the decision not to follow redirects, and the wording of the one
//! line a person actually reads.
//!
//! **delivery is best-effort by default.** a post that fails is logged and
//! gone: the process that recorded the event may not survive to retry it, and
//! a hook that blocked the executor to guarantee delivery would be worse than
//! a missed alert. [`durable_notifications`](crate::Hestan::durable_notifications)
//! is the other arrangement: the event is written down first and delivered by
//! a loop that retries.

use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;
use serde_json::json;

use crate::freshness::LateEvent;
use crate::hooks::{OpEvent, RunEvent, RunFailure};
use crate::model::{OpStatus, RunStatus};
use crate::whose::Owner;

fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            // reqwest follows redirects by default, replaying the POST as a
            // bodyless GET; a 3xx is a failed delivery, not a hop
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client")
    })
}

fn post(url: String, body: serde_json::Value) {
    tokio::spawn(async move {
        match client().post(&url).json(&body).send().await {
            Ok(resp) if !resp.status().is_success() => {
                tracing::warn!("failure notification to {url}: {}", resp.status());
            }
            Err(e) => tracing::warn!("failure notification to {url}: {e}"),
            Ok(_) => {}
        }
    });
}

/// what these hooks can deliver: something that serializes whole, and knows
/// how to say itself in one line. implemented for every event a hook receives,
/// so the same two helpers serve every one of
/// [`on_failure`](crate::Hestan::on_failure),
/// [`on_run_finished`](crate::Hestan::on_run_finished),
/// [`on_op_finished`](crate::Hestan::on_op_finished) and
/// [`on_late`](crate::Hestan::on_late). the call sites are identical either
/// way, and which one is meant is inferred from the hook it is handed to.
pub trait Alert: Serialize {
    /// one line, for a channel that shows text rather than json.
    fn summary(&self) -> String;
}

impl Alert for RunFailure {
    fn summary(&self) -> String {
        format!(
            "job {} failed at {}: {} ({}){}",
            self.job,
            self.failed_op.as_deref().unwrap_or("unknown op"),
            self.error.as_deref().unwrap_or("unknown error"),
            self.run_id,
            owned_by(self.owner.as_ref())
        )
    }
}

/// `, owned by ada of data-platform (#data-alerts)`, or nothing at all.
///
/// nothing, rather than "owned by nobody": an alert about a job nobody claimed
/// reads exactly as it did before owners existed, and a line that says who
/// owns it says so because somebody declared it.
fn owned_by(owner: Option<&Owner>) -> String {
    match owner {
        Some(owner) => format!(", owned by {owner}"),
        None => String::new(),
    }
}

impl Alert for RunEvent {
    fn summary(&self) -> String {
        // a run that succeeded should not read like an alarm: nothing is
        // wrong, and a channel where the good news looks like the bad news is
        // a channel people stop reading
        let what = match self.status {
            RunStatus::Failed => format!(
                "failed at {}: {}",
                self.failed_op.as_deref().unwrap_or("unknown op"),
                self.error.as_deref().unwrap_or("unknown error")
            ),
            RunStatus::Canceled => "was canceled".to_string(),
            RunStatus::Success => "succeeded".to_string(),
            // a hook fires on a terminal status, so neither of these reaches
            // here. they are named rather than absorbed by a `_` because
            // `RunStatus` is a closed set whose own documentation says a sixth
            // variant would change what terminal means: a catch-all would send
            // that variant out as good news instead of failing to compile
            RunStatus::Queued | RunStatus::Running => {
                format!("is {}", self.status.as_str())
            }
        };
        format!(
            "job {} {what}{} ({}){}",
            self.job,
            took(self.duration),
            self.run_id,
            // on every terminal status, not only a failure: a success line
            // that named nobody and a failure line that did would read as if
            // the owner were part of the alarm
            owned_by(self.owner.as_ref())
        )
    }
}

impl Alert for OpEvent {
    fn summary(&self) -> String {
        let what = match self.status {
            OpStatus::Failed => format!(
                "failed on attempt {}: {}",
                self.attempt,
                self.error.as_deref().unwrap_or("unknown error")
            ),
            OpStatus::Canceled => format!("was canceled on attempt {}", self.attempt),
            // an op that [skipped itself](crate::OpCtx::skip) ran and decided
            // there was nothing to do, which is not a success and must not be
            // reported as one
            OpStatus::Skipped => format!("skipped itself on attempt {}", self.attempt),
            OpStatus::Success => format!("succeeded on attempt {}", self.attempt),
            // as above: not reachable from a terminal hook, and named anyway
            // so that a new `OpStatus` is a compile error here rather than an
            // alert that says the op worked
            OpStatus::Pending | OpStatus::Running => {
                format!("is {} on attempt {}", self.status.as_str(), self.attempt)
            }
        };
        format!(
            "op {} of job {} {what}{} ({})",
            self.op,
            self.job,
            took(Some(self.duration)),
            self.run_id
        )
    }
}

/// ` in 1.4s`, or nothing at all for something that never ran long enough to
/// have a duration.
fn took(d: Option<Duration>) -> String {
    match d {
        Some(d) => format!(" in {:.1}s", d.as_secs_f64()),
        None => String::new(),
    }
}

impl Alert for LateEvent {
    fn summary(&self) -> String {
        let mins = self.late_by.as_secs() / 60;
        let owner = owned_by(self.owner.as_ref());
        match self.last_success {
            Some(t) => format!(
                "{} {} is {mins}m late (last success {}){owner}",
                self.kind.as_str(),
                self.name,
                t.to_rfc3339()
            ),
            None => format!(
                "{} {} is {mins}m late{owner}",
                self.kind.as_str(),
                self.name
            ),
        }
    }
}

/// a hook that POSTs the whole event as json to `url`.
///
/// ```no_run
/// # use hestan::Hestan;
/// Hestan::new()
///     .on_failure(hestan::notify::webhook("https://ops.example/hook"))
///     .on_late(hestan::notify::webhook("https://ops.example/hook"));
/// ```
pub fn webhook<A: Alert>(url: impl Into<String>) -> impl Fn(A) + Send + Sync {
    let url = url.into();
    move |a: A| {
        post(
            url.clone(),
            serde_json::to_value(&a).expect("alert is json"),
        );
    }
}

/// a hook for a slack incoming webhook: posts `{"text": <the alert's one-line
/// summary>}`. that summary is `job {job} failed at {failed_op}: {error}
/// ({run_id})` for a failure, `job {job} succeeded in {n}s ({run_id})` for a
/// run that worked, `op {op} of job {job} failed on attempt {n}: {error}
/// ({run_id})` for an op, and `{kind} {name} is {n}m late (last success {t})`
/// for a late one.
///
/// a run or a late alert about something with a declared
/// [`Owner`] ends `, owned by ada of data-platform
/// (#data-alerts)`. one about something nobody claimed ends where it always
/// did.
pub fn slack<A: Alert>(url: impl Into<String>) -> impl Fn(A) + Send + Sync {
    let url = url.into();
    move |a: A| {
        post(url.clone(), json!({ "text": a.summary() }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Trigger;
    use chrono::Utc;

    fn run(status: RunStatus) -> RunEvent {
        RunEvent {
            run_id: "0192-abc".into(),
            job: "orders_etl".into(),
            owner: None,
            trigger: Trigger::Schedule,
            status,
            failed_op: Some("load".into()),
            error: Some("connection refused".into()),
            started_at: Some(Utc::now()),
            finished_at: Utc::now(),
            duration: Some(Duration::from_millis(12_340)),
        }
    }

    // the good news must not read like the bad news, or the channel stops
    // being read
    #[test]
    fn a_run_says_what_it_did_rather_than_always_sounding_like_an_alarm() {
        assert_eq!(
            run(RunStatus::Success).summary(),
            "job orders_etl succeeded in 12.3s (0192-abc)"
        );
        assert_eq!(
            run(RunStatus::Failed).summary(),
            "job orders_etl failed at load: connection refused in 12.3s (0192-abc)"
        );
        assert_eq!(
            run(RunStatus::Canceled).summary(),
            "job orders_etl was canceled in 12.3s (0192-abc)"
        );

        // a run that never started has no duration to report, rather than 0.0s
        let never = RunEvent {
            started_at: None,
            duration: None,
            ..run(RunStatus::Failed)
        };
        assert_eq!(
            never.summary(),
            "job orders_etl failed at load: connection refused (0192-abc)"
        );
    }

    // the bug this guards: a `_` arm sent every status it did not name out as
    // "succeeded", so the first `OpStatus` nobody thought about was alerted as
    // good news. both enums are closed sets, so every variant is listed here
    // and a new one is a compile error in the summary before it is a wrong
    // alert in somebody's channel
    #[test]
    fn no_status_but_success_is_ever_alerted_as_a_success() {
        for status in [
            RunStatus::Queued,
            RunStatus::Running,
            RunStatus::Failed,
            RunStatus::Canceled,
        ] {
            let line = run(status).summary();
            assert!(
                !line.contains("succeeded"),
                "a {status} run was alerted as a success: {line}"
            );
        }
        assert!(run(RunStatus::Success).summary().contains("succeeded"));

        let op = |status| OpEvent {
            run_id: "0192-abc".into(),
            job: "orders_etl".into(),
            op: "load".into(),
            attempt: 1,
            status,
            error: None,
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration: Duration::from_millis(400),
        };
        for status in [
            OpStatus::Pending,
            OpStatus::Running,
            OpStatus::Failed,
            OpStatus::Skipped,
            OpStatus::Canceled,
        ] {
            let line = op(status).summary();
            assert!(
                !line.contains("succeeded"),
                "a {status} op was alerted as a success: {line}"
            );
        }
        assert!(op(OpStatus::Success).summary().contains("succeeded"));
    }

    // a skip is the one an alert used to get wrong outright: the op ran, found
    // nothing to do and said so, which is neither the failure that wakes
    // somebody nor the success that says work happened
    #[test]
    fn an_op_that_skipped_itself_is_alerted_as_a_skip() {
        let event = OpEvent {
            run_id: "0192-abc".into(),
            job: "orders_etl".into(),
            op: "load".into(),
            attempt: 2,
            status: OpStatus::Skipped,
            error: Some("no drop from the vendor yet".into()),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration: Duration::from_millis(400),
        };
        assert_eq!(
            event.summary(),
            "op load of job orders_etl skipped itself on attempt 2 in 0.4s (0192-abc)"
        );
    }

    #[test]
    fn an_op_says_which_attempt_it_was() {
        let event = OpEvent {
            run_id: "0192-abc".into(),
            job: "orders_etl".into(),
            op: "load".into(),
            attempt: 3,
            status: OpStatus::Success,
            error: None,
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration: Duration::from_millis(400),
        };
        assert_eq!(
            event.summary(),
            "op load of job orders_etl succeeded on attempt 3 in 0.4s (0192-abc)"
        );

        let failed = OpEvent {
            status: OpStatus::Failed,
            error: Some("connection refused".into()),
            attempt: 1,
            ..event
        };
        assert_eq!(
            failed.summary(),
            "op load of job orders_etl failed on attempt 1: connection refused in 0.4s (0192-abc)"
        );
    }
}
