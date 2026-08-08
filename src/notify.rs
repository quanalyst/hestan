use std::sync::OnceLock;
use std::time::Duration;

use serde_json::json;

use crate::executor::RunFailure;

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

/// a hook that POSTs the whole [`RunFailure`] as json to `url`.
///
/// ```no_run
/// # use hestan::Hestan;
/// Hestan::new().on_failure(hestan::notify::webhook("https://ops.example/hook"));
/// ```
pub fn webhook(url: impl Into<String>) -> impl Fn(RunFailure) + Send + Sync {
    let url = url.into();
    move |f: RunFailure| {
        post(
            url.clone(),
            serde_json::to_value(&f).expect("RunFailure is json"),
        );
    }
}

/// a hook for a slack incoming webhook: posts
/// `{"text": "job {job} failed at {failed_op}: {error} ({run_id})"}`.
pub fn slack(url: impl Into<String>) -> impl Fn(RunFailure) + Send + Sync {
    let url = url.into();
    move |f: RunFailure| {
        let text = format!(
            "job {} failed at {}: {} ({})",
            f.job,
            f.failed_op.as_deref().unwrap_or("unknown op"),
            f.error.as_deref().unwrap_or("unknown error"),
            f.run_id
        );
        post(url.clone(), json!({ "text": text }));
    }
}
