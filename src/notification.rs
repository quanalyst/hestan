//! Named, durable delivery of terminal run events.
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use futures::{
    FutureExt,
    future::BoxFuture,
    stream::{FuturesUnordered, StreamExt},
};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{Error, Job, RunEvent, RunStatus, Runner};

/// Retry and timeout settings, captured when a run is queued.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationPolicy {
    pub(crate) attempts: u32,
    pub(crate) timeout: Duration,
    pub(crate) base: Duration,
    pub(crate) cap: Duration,
    pub(crate) spacing: Duration,
}
impl Default for NotificationPolicy {
    fn default() -> Self {
        Self {
            attempts: 8,
            timeout: Duration::from_secs(10),
            base: Duration::from_secs(10),
            cap: Duration::from_secs(1800),
            spacing: Duration::ZERO,
        }
    }
}
impl NotificationPolicy {
    /// Maximum sends in one automatic retry cycle, including the first.
    pub fn attempts(mut self, attempts: u32) -> Self {
        self.attempts = attempts;
        self
    }
    /// Deadline for one sender invocation.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    /// Exponential backoff bounds; each delay uses full jitter.
    pub fn backoff(mut self, base: Duration, cap: Duration) -> Self {
        self.base = base;
        self.cap = cap;
        self
    }
    /// Minimum interval between sends to this destination, shared across processes.
    pub fn minimum_interval(mut self, spacing: Duration) -> Self {
        self.spacing = spacing;
        self
    }
    fn validate(&self) -> Result<(), Error> {
        if self.attempts == 0
            || self.timeout.is_zero()
            || self.base.is_zero()
            || self.cap < self.base
            || [self.timeout, self.cap, self.spacing]
                .iter()
                .any(|d| d.as_secs() > 86400 * 365)
        {
            return Err(Error::Graph("invalid notification policy".into()));
        }
        Ok(())
    }
}

/// A sender failure. Messages are scrubbed and bounded before persistence.
#[derive(Debug, Clone)]
pub struct NotificationDeliveryError {
    pub(crate) message: String,
    pub(crate) retry: bool,
    pub(crate) after: Option<Duration>,
    pub(crate) cooldown: bool,
    pub(crate) status: Option<u16>,
}
impl NotificationDeliveryError {
    /// A temporary failure that should be retried.
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retry: true,
            after: None,
            cooldown: false,
            status: None,
        }
    }
    /// A failure requiring an operator or configuration change.
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            retry: false,
            ..Self::retryable(message)
        }
    }
    /// Do not retry sooner than this delay; pause the destination as well.
    pub fn retry_after(mut self, delay: Duration) -> Self {
        self.after = Some(delay);
        self.cooldown = true;
        self
    }
}
impl std::fmt::Display for NotificationDeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for NotificationDeliveryError {}

/// Immutable notification content. No run parameters or outputs are included.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct NotificationEnvelope {
    /// Stable identifier shared by all destinations for this run outcome.
    pub event_id: String,
    /// Stable identifier for this destination delivery, preserved on retry.
    pub delivery_id: String,
    /// Event discriminator; currently `run.finished`.
    pub event_type: String,
    /// When the run became terminal.
    pub occurred_at: DateTime<Utc>,
    /// Readable job name captured when the run was queued.
    pub display_name: String,
    /// Explicit public link, if configured.
    pub run_url: Option<String>,
    /// Whether the recorded run error was shortened.
    pub error_truncated: bool,
    /// The immutable terminal run event.
    pub run: RunEvent,
}

/// Context for one attempt. Await submission before returning success.
#[derive(Clone)]
pub struct NotificationDeliveryCtx {
    /// The immutable event snapshot.
    pub event: Arc<NotificationEnvelope>,
    /// Stable destination identifier.
    pub destination: String,
    /// Lifetime attempt number, starting at one.
    pub attempt: u32,
    /// Maximum duration of this attempt.
    pub timeout: Duration,
    pub(crate) cancel: watch::Receiver<bool>,
}
impl NotificationDeliveryCtx {
    /// Wait until shutdown or the attempt deadline asks this sender to stop.
    pub async fn cancelled(&mut self) {
        let _ = self.cancel.wait_for(|v| *v).await;
    }
}

/// An asynchronous sender. Do not detach work or block the runtime.
pub trait NotificationSender: Send + Sync + 'static {
    /// Await remote submission and report its result.
    fn send(
        &self,
        ctx: NotificationDeliveryCtx,
    ) -> BoxFuture<'static, Result<(), NotificationDeliveryError>>;
    /// Identifies the persisted payload contract, not the endpoint or credentials.
    fn protocol(&self) -> &str {
        "custom"
    }
    /// Validate configuration without sending a message.
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }
    /// Provider-specific default pacing.
    fn minimum_interval(&self) -> Duration {
        Duration::ZERO
    }
}
impl<F, Fut> NotificationSender for F
where
    F: Fn(NotificationDeliveryCtx) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), NotificationDeliveryError>> + Send + 'static,
{
    /// Await remote submission and report its result.
    fn send(
        &self,
        ctx: NotificationDeliveryCtx,
    ) -> BoxFuture<'static, Result<(), NotificationDeliveryError>> {
        Box::pin(self(ctx))
    }
}

/// A stable, named subscription. Registration enables durable delivery.
#[derive(Clone)]
pub struct NotificationDestination {
    pub(crate) id: String,
    pub(crate) statuses: Vec<RunStatus>,
    pub(crate) jobs: Vec<String>,
    pub(crate) policy: NotificationPolicy,
    pub(crate) sender: Arc<dyn NotificationSender>,
    pub(crate) enabled: bool,
    pub(crate) base_url: Option<String>,
}
impl NotificationDestination {
    /// Begin an explicit outcome subscription.
    pub fn builder(id: impl Into<String>) -> NotificationDestinationBuilder {
        NotificationDestinationBuilder {
            id: id.into(),
            statuses: vec![],
            jobs: vec![],
            policy: None,
            sender: None,
            enabled: true,
            base_url: None,
        }
    }
}
/// Builder for a named destination.
pub struct NotificationDestinationBuilder {
    id: String,
    statuses: Vec<RunStatus>,
    jobs: Vec<String>,
    policy: Option<NotificationPolicy>,
    sender: Option<Arc<dyn NotificationSender>>,
    enabled: bool,
    base_url: Option<String>,
}
impl NotificationDestinationBuilder {
    /// Subscribe to failed runs.
    pub fn on_failure(mut self) -> Self {
        self.statuses = vec![RunStatus::Failed];
        self
    }
    /// Subscribe to success, failure, and cancellation.
    pub fn on_run_finished(mut self) -> Self {
        self.statuses = vec![RunStatus::Success, RunStatus::Failed, RunStatus::Canceled];
        self
    }
    /// Restrict subscriptions to these persistent job names; empty means all jobs.
    pub fn jobs(mut self, jobs: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.jobs = jobs.into_iter().map(Into::into).collect();
        self
    }
    /// Use this retry policy for newly queued runs.
    pub fn policy(mut self, policy: NotificationPolicy) -> Self {
        self.policy = Some(policy);
        self
    }
    /// Set the asynchronous sender.
    pub fn sender(mut self, sender: impl NotificationSender) -> Self {
        self.sender = Some(Arc::new(sender));
        self
    }
    /// Disable sending while retaining the declaration and owed deliveries.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
    /// Explicit public UI URL used for links; never inferred from HTTP requests.
    pub fn public_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = Some(url.into());
        self
    }
    /// Validate and construct the destination.
    pub fn build(self) -> Result<NotificationDestination, Error> {
        if self.id.trim().is_empty()
            || self.id.len() > 256
            || self.id.chars().any(char::is_control)
            || self.statuses.is_empty()
        {
            return Err(Error::Graph(
                "notification needs a nonempty ID (at most 256 bytes) and an outcome selector"
                    .into(),
            ));
        }
        if self.base_url.as_ref().is_some_and(|s| {
            s.parse::<axum::http::Uri>().map_or(true, |uri| {
                !matches!(uri.scheme_str(), Some("http" | "https")) || uri.authority().is_none()
            }) || s.contains(['?', '#', '@'])
                || s.len() > 2048
                || s.chars().any(char::is_control)
        }) {
            return Err(Error::Graph("notification public URL must be an HTTP(S) URL without credentials, query or fragment".into()));
        }
        let sender = self
            .sender
            .ok_or_else(|| Error::Graph("notification needs a sender".into()))?;
        sender.validate()?;
        if sender.protocol().is_empty() || sender.protocol().len() > 128 {
            return Err(Error::Graph("invalid notification sender protocol".into()));
        }
        let policy = self.policy.unwrap_or_else(|| {
            NotificationPolicy::default().minimum_interval(sender.minimum_interval())
        });
        policy.validate()?;
        Ok(NotificationDestination {
            id: self.id,
            statuses: self.statuses,
            jobs: self.jobs,
            policy,
            sender,
            enabled: self.enabled,
            base_url: self.base_url,
        })
    }
}

#[derive(Default, Clone)]
pub(crate) struct Registry {
    /// Destinations.
    pub destinations: Vec<NotificationDestination>,
    /// Jobs.
    pub jobs: HashMap<String, (String, Option<crate::Owner>)>,
}
impl Registry {
    pub fn new(
        destinations: Vec<NotificationDestination>,
        jobs: &HashMap<String, Job>,
    ) -> Result<Self, Error> {
        let mut ids = std::collections::HashSet::new();
        for d in &destinations {
            if !ids.insert(&d.id) {
                return Err(Error::Graph(format!(
                    "duplicate notification destination {}",
                    d.id
                )));
            }
            for name in &d.jobs {
                if !jobs.contains_key(name) {
                    return Err(Error::UnknownJob(name.clone()));
                }
            }
        }
        Ok(Self {
            destinations,
            jobs: jobs
                .iter()
                .map(|(id, j)| {
                    (
                        id.clone(),
                        (j.display_name().unwrap_or(id).into(), j.owner().cloned()),
                    )
                })
                .collect(),
        })
    }
}

/// Named-delivery lifecycle, independent of execution status and legacy callbacks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NotificationDeliveryState {
    /// Waiting for its first attempt or retry.
    Pending,
    /// A dispatcher holds a live delivery claim.
    InFlight,
    /// A compatible destination is unavailable.
    Blocked,
    /// The remote service acknowledged submission.
    Delivered,
    /// Permanently rejected or attempts exhausted.
    Failed,
    /// An administrator deliberately stopped delivery.
    Dismissed,
}

/// A recorded delivery. Fields are snapshots; credentials are never included.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct NotificationDelivery {
    /// Stable delivery identifier.
    pub id: String,
    /// Stable destination identifier.
    pub destination: String,
    /// The immutable event snapshot.
    pub event: NotificationEnvelope,
    /// Current delivery state.
    pub state: NotificationDeliveryState,
    /// Lifetime attempts, including attempts with unknown outcomes.
    pub attempts: u32,
    /// Attempts in the current automatic retry cycle.
    pub cycle_attempts: u32,
    /// Retry cycle, incremented by manual retry.
    pub cycle: u32,
    /// Optimistic concurrency token for operator actions.
    pub generation: i64,
    /// When the delivery was recorded.
    pub created_at: DateTime<Utc>,
    /// Earliest next automatic attempt, if pending.
    pub next_attempt_at: Option<DateTime<Utc>>,
    /// When submission was acknowledged.
    pub delivered_at: Option<DateTime<Utc>>,
    /// Most recent bounded, sanitized diagnostic.
    pub last_error: Option<String>,
    pub(crate) protocol: String,
    pub(crate) policy: NotificationPolicy,
}
/// A send attempt, including unknown outcomes after lease expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct NotificationAttempt {
    /// Stable identifier for this destination delivery, preserved on retry.
    pub delivery_id: String,
    /// Lifetime attempt number, starting at one.
    pub attempt: u32,
    /// Retry cycle, incremented by manual retry.
    pub cycle: u32,
    /// When this attempt was claimed.
    pub started_at: DateTime<Utc>,
    /// When its outcome was recorded.
    pub finished_at: Option<DateTime<Utc>>,
    /// Accepted, failed, active, or unknown outcome.
    pub outcome: String,
    /// HTTP status when reported by the sender.
    pub status: Option<u16>,
    /// Bounded, sanitized diagnostic.
    pub error: Option<String>,
}
/// Filters for named delivery history. Cursor is the last delivery ID of a page.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct NotificationQuery {
    /// The immutable terminal run event.
    pub run: Option<String>,
    /// Filter by persistent job name.
    pub job: Option<String>,
    /// Stable destination identifier.
    pub destination: Option<String>,
    /// Current delivery state.
    pub state: Option<String>,
    /// Exclusive delivery ID cursor for the next page.
    pub before: Option<String>,
    /// Include deliveries created at or after this UTC instant.
    pub since: Option<DateTime<Utc>>,
    /// Include deliveries created before this UTC instant.
    pub until: Option<DateTime<Utc>>,
    /// Page size, default 50 and maximum 500.
    pub limit: Option<u32>,
}
/// Result of a bounded delivery pass. Remaining work can require another process.
#[derive(Debug, Clone, Default, Serialize)]
pub struct NotificationFlush {
    /// Attempts started during this flush.
    pub attempted: usize,
    /// Undelivered records, including blocked and failed deliveries.
    pub remaining: usize,
}

pub(crate) struct ClaimedDelivery {
    /// Row.
    pub row: NotificationDelivery,
    /// Token.
    pub token: String,
}

pub(crate) fn bounded(s: &str, bytes: usize) -> String {
    let mut end = s.len().min(bytes);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_owned()
}

async fn attempt(runner: Runner, claim: ClaimedDelivery, sender: Arc<dyn NotificationSender>) {
    let (cancel, receiver) = watch::channel(false);
    struct Cancel(watch::Sender<bool>);
    impl Drop for Cancel {
        fn drop(&mut self) {
            let _ = self.0.send(true);
        }
    }
    let _cancel = Cancel(cancel);
    let ctx = NotificationDeliveryCtx {
        event: Arc::new(claim.row.event.clone()),
        destination: claim.row.destination.clone(),
        attempt: claim.row.attempts,
        timeout: claim.row.policy.timeout,
        cancel: receiver,
    };
    let future = std::panic::AssertUnwindSafe(async { sender.send(ctx).await }).catch_unwind();
    let result = match tokio::time::timeout(claim.row.policy.timeout, future).await {
        Ok(Ok(r)) => r,
        Ok(Err(_)) => Err(NotificationDeliveryError::retryable("sender panicked")),
        Err(_) => Err(NotificationDeliveryError::retryable(
            "sender timed out; remote outcome unknown",
        )),
    };
    // Retry acknowledgement, not the remote send, while this claim is valid.
    runner
        .store()
        .landed("notification acknowledgement", || {
            runner
                .store()
                .settle_delivery(&claim, result.as_ref().err())
        })
        .await;
}

pub(crate) async fn pass(
    runner: &Runner,
    only_run: Option<&str>,
    progress: &std::sync::atomic::AtomicUsize,
) -> Result<usize, Error> {
    let registry = runner.store().notification_registry();
    runner.store().reconcile_deliveries(&registry)?;
    let mut pending = FuturesUnordered::new();
    let mut count = 0;
    // A bounded pass, claiming only immediately runnable sends.
    let mut exhausted = false;
    loop {
        while pending.len() < 4
            && count < 50
            && !exhausted
            && runner.deciding().leading()
            && !runner.stopping()
        {
            let Some(claim) = runner.store().claim_delivery(&registry, only_run)? else {
                exhausted = true;
                break;
            };
            let sender = registry
                .destinations
                .iter()
                .find(|d| d.id == claim.row.destination)
                .expect("claim requires registered sender")
                .sender
                .clone();
            pending.push(attempt(runner.clone(), claim, sender));
            count += 1;
            progress.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if pending.next().await.is_none() {
            break;
        }
        if count < 50 {
            exhausted = false;
        }
    }
    Ok(count)
}

pub(crate) async fn run_delivery(runner: Runner) {
    loop {
        tokio::select! { _ = runner.stopped() => return, _ = runner.deciding().wait() => {} }
        let progress = std::sync::atomic::AtomicUsize::new(0);
        if let Err(e) = pass(&runner, None, &progress).await {
            tracing::warn!("notification dispatch failed: {e}");
        }
        tokio::select! { _ = runner.stopped() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
    }
}

impl Runner {
    /// Register named destinations. Does not change legacy callback delivery.
    pub fn with_notifications(
        self,
        destinations: impl IntoIterator<Item = NotificationDestination>,
    ) -> Result<Self, Error> {
        let registry = Registry::new(destinations.into_iter().collect(), self.jobs())?;
        self.store().set_notification_registry(registry);
        Ok(self)
    }
    /// Attempt due named deliveries within a time budget. No waiting through backoff.
    pub async fn flush_notifications(&self, budget: Duration) -> Result<NotificationFlush, Error> {
        self.flush_run_notifications(budget, None).await
    }
    pub(crate) async fn flush_run_notifications(
        &self,
        budget: Duration,
        run: Option<&str>,
    ) -> Result<NotificationFlush, Error> {
        let progress = std::sync::atomic::AtomicUsize::new(0);
        if let Ok(result) = tokio::time::timeout(budget, pass(self, run, &progress)).await {
            result?;
        }
        let attempted = progress.load(std::sync::atomic::Ordering::Relaxed);
        Ok(NotificationFlush {
            attempted,
            remaining: self.store().outstanding_deliveries()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Job, Op, RunStatus, Store, Trigger};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn retrying_database_acknowledgement_does_not_repeat_remote_send() {
        let store = Store::open(":memory:").unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let sent = calls.clone();
        let destination = NotificationDestination::builder("receiver")
            .on_run_finished()
            .sender(move |_: NotificationDeliveryCtx| {
                sent.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .build()
            .unwrap();
        let job = Job::builder("task")
            .op(Op::new("work", |_| async { Ok(serde_json::json!(1)) }))
            .build()
            .unwrap();
        let runner = Runner::new([job], store.clone())
            .unwrap()
            .with_notifications([destination])
            .unwrap();
        let run = runner
            .run("task", serde_json::json!({}), Trigger::Manual)
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        store.fail_writes_to("notification acknowledgement", 2);
        runner
            .flush_notifications(Duration::from_secs(5))
            .await
            .unwrap();
        let rows = store
            .notification_deliveries(&NotificationQuery::default())
            .unwrap();
        assert_eq!(rows[0].state, NotificationDeliveryState::Delivered);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(rows[0].attempts, 1);
    }
}
