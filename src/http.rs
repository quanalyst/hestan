use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::Error;
use crate::job::Job;
use crate::model::Overlap;
use crate::op::{Op, OpCtx, OpResult};

const MAX_BACKOFF: Duration = Duration::from_secs(30);
// a server-sent retry-after can say anything; never sleep longer than this
const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

type ParseFn = dyn Fn(Value) -> Result<Value, String> + Send + Sync;

#[derive(Clone)]
struct TypedParse {
    type_name: &'static str,
    parse: Arc<ParseFn>,
}

/// a declarative GET pull that lowers into ordinary ops: hand it to
/// `Hestan::source` for the one-block form, or lower it yourself via
/// [`into_ops`](Self::into_ops) / [`into_job`](Self::into_job).
pub struct HttpSource {
    pub(crate) name: Option<String>,
    pub(crate) cron: Option<(String, String)>,
    url: String,
    headers: Vec<(&'static str, String)>,
    bearer_env: Option<String>,
    query: Vec<(String, String)>,
    query_each: Option<(String, Vec<String>)>,
    expect: Option<TypedParse>,
    retries: u32,
    retry_delay: Duration,
    timeout: Duration,
    max_parallel: Option<usize>,
    overlap: Option<Overlap>,
}

impl HttpSource {
    /// a GET source (the only method in v1).
    pub fn get(url: impl Into<String>) -> HttpSource {
        HttpSource {
            name: None,
            cron: None,
            url: url.into(),
            headers: Vec::new(),
            bearer_env: None,
            query: Vec::new(),
            query_each: None,
            expect: None,
            retries: 2,
            retry_delay: Duration::from_secs(1),
            max_parallel: None,
            overlap: None,
            timeout: Duration::from_secs(30),
        }
    }

    /// name the source; required, and used as the op (and job) name.
    pub fn name(mut self, n: impl Into<String>) -> Self {
        self.name = Some(n.into());
        self
    }

    /// a header sent with every request. fixed at declaration — for a
    /// credential that may be rotated, use [`bearer_env`](Self::bearer_env).
    pub fn header(mut self, k: &'static str, v: impl Into<String>) -> Self {
        self.headers.push((k, v.into()));
        self
    }

    /// send `authorization: Bearer <token>` with the token read from this env
    /// var at request time, so a rotated token is picked up without a restart.
    /// a missing var fails the op.
    pub fn bearer_env(mut self, var: impl Into<String>) -> Self {
        self.bearer_env = Some(var.into());
        self
    }

    /// a query parameter sent on every request, the same for all of them —
    /// [`query_each`](Self::query_each) is the one that varies.
    pub fn query(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.query.push((k.into(), v.into()));
        self
    }

    /// fan out into one op per value, each sending its own `k=value`.
    pub fn query_each(
        mut self,
        k: impl Into<String>,
        vals: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.query_each = Some((k.into(), vals.into_iter().map(Into::into).collect()));
        self
    }

    /// declare the response shape: the body must deserialize into `T`, and the
    /// recorded output is `T` reserialized. a mismatch fails the op through the
    /// same `type check failed` path as [`Op::typed`].
    pub fn expect_json<T>(mut self) -> Self
    where
        T: DeserializeOwned + Serialize + Send + 'static,
    {
        self.expect = Some(TypedParse {
            type_name: std::any::type_name::<T>(),
            parse: Arc::new(|v| {
                let t: T = serde_json::from_value(v).map_err(|e| e.to_string())?;
                serde_json::to_value(t).map_err(|e| e.to_string())
            }),
        });
        self
    }

    /// run on a 5-field cron expression, evaluated in utc.
    pub fn cron(mut self, expr: impl Into<String>) -> Self {
        self.cron = Some((expr.into(), "UTC".into()));
        self
    }

    /// like [`cron`](Self::cron) but in a named iana timezone.
    pub fn cron_tz(mut self, expr: impl Into<String>, tz: impl Into<String>) -> Self {
        self.cron = Some((expr.into(), tz.into()));
        self
    }

    /// extra attempts inside the request loop (default 2).
    pub fn retries(mut self, n: u32) -> Self {
        self.retries = n;
        self
    }

    /// backoff base: the nth retry waits a jittered slice of
    /// `retry_delay * 2^n`, capped at 30s (default 1s). a numeric
    /// `Retry-After` from the server overrides it and is honored exactly.
    pub fn retry_delay(mut self, d: Duration) -> Self {
        self.retry_delay = d;
        self
    }

    /// cap concurrent requests when fanning out; most apis want this.
    pub fn max_parallel(mut self, n: usize) -> Self {
        self.max_parallel = Some(n.max(1));
        self
    }

    /// overlap policy for the generated job; skip is already the default.
    pub fn overlap(mut self, o: Overlap) -> Self {
        self.overlap = Some(o);
        self
    }

    /// per-request timeout (default 30s).
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = d;
        self
    }

    /// lower into plain ops: one op named `{name}`, or with
    /// [`query_each`](Self::query_each) one per value named `{name}_{value}`,
    /// where the value is lowercased and every char outside `[a-z0-9_]`
    /// becomes `_`.
    ///
    /// # Panics
    ///
    /// panics if [`name`](Self::name) was never called;
    /// [`into_job`](Self::into_job) and `Hestan::source` return an error
    /// instead.
    pub fn into_ops(&self) -> Vec<Op> {
        let name = self.name.as_deref().expect("http source needs a name");
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .build()
            .expect("reqwest client");
        match &self.query_each {
            None => vec![self.op(&client, name.to_string(), None)],
            Some((k, vals)) => vals
                .iter()
                .map(|v| {
                    self.op(
                        &client,
                        format!("{name}_{}", sanitize(v)),
                        Some((k.clone(), v.clone())),
                    )
                })
                .collect(),
        }
    }

    /// [`into_ops`](Self::into_ops) wrapped in a single job. a `cron` on the
    /// source is dropped (with a warning): manual lowering owns registration,
    /// so attach the schedule yourself — only `Hestan::source` consumes it.
    pub fn into_job(&self, job_name: &str) -> Result<Job, Error> {
        if let Some((expr, _)) = &self.cron {
            tracing::warn!(
                job = job_name,
                "cron {expr:?} on http source is dropped by into_job; \
                 attach the schedule yourself or use Hestan::source"
            );
        }
        self.build_job(job_name)
    }

    // the lowering itself, warn-free: Hestan::source is the one caller that
    // does consume the cron
    pub(crate) fn build_job(&self, job_name: &str) -> Result<Job, Error> {
        let Some(name) = self.name.as_deref() else {
            return Err(Error::Graph("http source needs a name".into()));
        };
        if let Some((_, vals)) = &self.query_each {
            if vals.is_empty() {
                return Err(Error::Graph(
                    "query_each has no values; source would do nothing".into(),
                ));
            }
            // caught here, where both raw values are still known: the graph error
            // would name a string the caller never wrote
            let mut seen: std::collections::HashMap<String, &str> =
                std::collections::HashMap::new();
            for v in vals {
                let s = sanitize(v);
                if let Some(prev) = seen.insert(s.clone(), v) {
                    return Err(Error::Graph(format!(
                        "query_each values {prev:?} and {v:?} both sanitize to op {:?}",
                        format!("{name}_{s}")
                    )));
                }
            }
        }
        let mut builder = Job::builder(job_name);
        if let Some(n) = self.max_parallel {
            builder = builder.max_parallel(n);
        }
        if let Some(o) = self.overlap {
            builder = builder.overlap(o);
        }
        for op in self.into_ops() {
            builder = builder.op(op);
        }
        builder.build()
    }

    fn op(&self, client: &reqwest::Client, name: String, extra: Option<(String, String)>) -> Op {
        let mut query = self.query.clone();
        query.extend(extra);
        let req = Arc::new(Request {
            client: client.clone(),
            url: self.url.clone(),
            headers: self.headers.clone(),
            bearer_env: self.bearer_env.clone(),
            query,
            expect: self.expect.clone(),
            retries: self.retries,
            retry_delay: self.retry_delay,
        });
        // the request loop owns retrying, so the op itself keeps retries 0
        let op = Op::new(name, move |ctx| {
            let req = req.clone();
            async move { req.run(&ctx).await }
        });
        match &self.expect {
            Some(t) => op.with_output_type(t.type_name),
            None => op,
        }
    }
}

struct Request {
    client: reqwest::Client,
    url: String,
    headers: Vec<(&'static str, String)>,
    bearer_env: Option<String>,
    query: Vec<(String, String)>,
    expect: Option<TypedParse>,
    retries: u32,
    retry_delay: Duration,
}

enum Failure {
    Retryable {
        brief: String,
        retry_after: Option<Duration>,
        msg: String,
    },
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

impl Request {
    async fn run(&self, ctx: &OpCtx) -> OpResult {
        let mut attempt = 0;
        loop {
            let (brief, retry_after, msg) = match self.attempt(ctx).await {
                Ok(v) => return Ok(v),
                Err(Failure::Fatal(e)) => return Err(e),
                Err(Failure::Retryable {
                    brief,
                    retry_after,
                    msg,
                }) => (brief, retry_after, msg),
            };
            if attempt >= self.retries {
                return Err(msg.into());
            }
            let backoff =
                crate::backoff::capped_exponential(self.retry_delay, attempt, MAX_BACKOFF);
            // a server that named a delay gets exactly that delay; one that did
            // not gets a jittered window, so a fan-out doesn't come back as a
            // herd on the same second
            let delay = match retry_after {
                Some(ra) => ra.max(backoff).min(MAX_RETRY_AFTER),
                None => crate::backoff::full_jitter(backoff),
            };
            ctx.warn(format!("{brief}, retrying in {delay:?}"));
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    async fn attempt(&self, ctx: &OpCtx) -> Result<Value, Failure> {
        let mut req = self.client.get(&self.url).query(&self.query);
        for (k, v) in &self.headers {
            req = req.header(*k, v);
        }
        if let Some(var) = &self.bearer_env {
            let token = std::env::var(var).unwrap_or_default();
            if token.is_empty() {
                return Err(Failure::Fatal(
                    format!("bearer env var {var} not set or empty").into(),
                ));
            }
            req = req.header("authorization", format!("Bearer {token}"));
        }
        let resp = req.send().await.map_err(request_failure)?;
        let status = resp.status();
        if !status.is_success() {
            let retry_after = if matches!(status.as_u16(), 429 | 503) {
                resp.headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .map(Duration::from_secs)
            } else {
                None
            };
            let body = resp.text().await.unwrap_or_default();
            let msg = format!("{status} from {}: {}", self.url, snippet(&body));
            // 429 and 5xx are worth another try; other client errors never improve
            return Err(if status.as_u16() == 429 || status.is_server_error() {
                Failure::Retryable {
                    brief: status.to_string(),
                    retry_after,
                    msg,
                }
            } else {
                Failure::Fatal(msg.into())
            });
        }
        let bytes = resp.bytes().await.map_err(request_failure)?;
        let value: Value = serde_json::from_slice(&bytes)
            .map_err(|e| Failure::Fatal(format!("bad json from {}: {e}", self.url).into()))?;
        let value = match &self.expect {
            Some(t) => (t.parse)(value).map_err(|e| Failure::Fatal(ctx.type_check_failed(e)))?,
            None => value,
        };
        ctx.info(format!("{status}, {} bytes", bytes.len()));
        Ok(value)
    }
}

// builder errors are deterministic: retrying rebuilds the same broken request
fn request_failure(e: reqwest::Error) -> Failure {
    let msg = error_chain(&e);
    if e.is_builder() {
        Failure::Fatal(msg.into())
    } else {
        Failure::Retryable {
            brief: e.to_string(),
            retry_after: None,
            msg,
        }
    }
}

// reqwest's Display stops at "builder error"; the cause is in the sources
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut msg = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        msg.push_str(": ");
        msg.push_str(&s.to_string());
        source = s.source();
    }
    msg
}

fn snippet(body: &str) -> &str {
    let body = body.trim();
    match body.char_indices().nth(200) {
        Some((i, _)) => &body[..i],
        None => body,
    }
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| match c.to_ascii_lowercase() {
            c @ ('a'..='z' | '0'..='9' | '_') => c,
            _ => '_',
        })
        .collect()
}
