use std::sync::OnceLock;
use std::time::Duration;

use serde::Serialize;
use serde_json::json;

use crate::executor::RunFailure;
use crate::freshness::LateEvent;

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
/// how to say itself in one line. implemented for [`RunFailure`] and
/// [`LateEvent`], so the same two helpers serve
/// [`on_failure`](crate::Hestan::on_failure) and
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
/// failure, `{kind} {name} is {n}m late (last success {t})` for a late one.
pub fn slack<A: Alert>(url: impl Into<String>) -> impl Fn(A) + Send + Sync {
    let url = url.into();
    move |a: A| {
        post(url.clone(), json!({ "text": a.summary() }));
    }
}
