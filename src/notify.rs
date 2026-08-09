use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;
use serde_json::json;

use crate::freshness::LateEvent;
use crate::hooks::{OpEvent, RunEvent, RunFailure};
use crate::model::{OpStatus, RunStatus};

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
/// [`on_late`](crate::Hestan::on_late) — the call sites are identical either
/// way, and which one is meant is inferred from the hook it is handed to.
pub trait Alert: Serialize {
    /// one line, for a channel that shows text rather than json.
    fn summary(&self) -> String;
}

impl Alert for RunFailure {
    fn summary(&self) -> String {
        format!(
            "job {} failed at {}: {} ({})",
            self.job,
            self.failed_op.as_deref().unwrap_or("unknown op"),
            self.error.as_deref().unwrap_or("unknown error"),
            self.run_id
        )
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
            _ => "succeeded".to_string(),
        };
        format!(
            "job {} {what}{} ({})",
            self.job,
            took(self.duration),
            self.run_id
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
            _ => format!("succeeded on attempt {}", self.attempt),
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
        match self.last_success {
            Some(t) => format!(
                "{} {} is {mins}m late (last success {})",
                self.kind.as_str(),
                self.name,
                t.to_rfc3339()
            ),
            None => format!("{} {} is {mins}m late", self.kind.as_str(), self.name),
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
/// summary>}` — `job {job} failed at {failed_op}: {error} ({run_id})` for a
/// failure, `job {job} succeeded in {n}s ({run_id})` for a run that worked,
/// `op {op} of job {job} failed on attempt {n}: {error} ({run_id})` for an op,
/// and `{kind} {name} is {n}m late (last success {t})` for a late one.
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
