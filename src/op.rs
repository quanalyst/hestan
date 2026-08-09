use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::backoff;
use crate::model::{EventKind, EventLevel, OpStatus, When};
use crate::resource::{self, Resources};
use crate::store::Store;

/// what an op body returns: json output on success, any error on failure.
pub type OpResult = Result<Value, Box<dyn std::error::Error + Send + Sync>>;

type OpFn = dyn Fn(OpCtx) -> BoxFuture<'static, OpResult> + Send + Sync;
type ParamsCheck = dyn Fn(&Value) -> Result<(), String> + Send + Sync;

/// the default retry pacing: the nth retry waits a random slice of
/// `1s * 2^n`, never more than 30s.
const DEFAULT_BACKOFF: Retry = Retry::Backoff {
    base: Duration::from_secs(1),
    max: Duration::from_secs(30),
};

/// how long an op waits between attempts.
#[derive(Clone, Copy, Debug)]
enum Retry {
    /// the same pause every time, from [`Op::retry_delay`].
    Fixed(Duration),
    /// `base * 2^attempt` capped at `max`, with full jitter, from
    /// [`Op::retry_backoff`].
    Backoff { base: Duration, max: Duration },
}

impl Retry {
    // `attempt` is the attempt that just failed, counting from 1
    fn delay(&self, attempt: u32) -> Duration {
        match *self {
            Retry::Fixed(d) => d,
            Retry::Backoff { base, max } => {
                backoff::jittered_exponential(base, attempt.saturating_sub(1), max)
            }
        }
    }
}

/// why a typed accessor on [`OpCtx`] came up empty.
#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("no input from op {0}")]
    Missing(String),
    #[error("type mismatch: {0}")]
    Mismatch(String),
    #[error("no resource named {0}")]
    NoResource(String),
    #[error("resource {name} is a {got}, not a {want}")]
    ResourceType {
        name: String,
        got: &'static str,
        want: &'static str,
    },
}

/// a named unit of work: an async fn plus the upstream ops it waits on.
#[derive(Clone)]
pub struct Op {
    name: String,
    deps: Vec<String>,
    requires: Vec<String>,
    io: Option<String>,
    // on an op flattened out of a Graph instance: (job-level dep name, the
    // name this body calls it). empty everywhere else, where they are the
    // same thing.
    aliases: Vec<(String, String)>,
    when: When,
    retries: u32,
    retry: Retry,
    timeout: Option<Duration>,
    pool: Option<String>,
    input_type: Option<&'static str>,
    output_type: Option<&'static str>,
    params_type: Option<&'static str>,
    params_check: Option<Arc<ParamsCheck>>,
    // built by `mapped`, so `over` missing is a build error rather than an op
    // that silently runs once over nothing
    mapped: bool,
    over: Option<String>,
    f: Arc<OpFn>,
}

impl Op {
    pub fn new<F, Fut>(name: impl Into<String>, f: F) -> Op
    where
        F: Fn(OpCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = OpResult> + Send + 'static,
    {
        Op {
            name: name.into(),
            deps: Vec::new(),
            requires: Vec::new(),
            io: None,
            aliases: Vec::new(),
            when: When::default(),
            retries: 0,
            retry: DEFAULT_BACKOFF,
            timeout: None,
            pool: None,
            input_type: None,
            output_type: None,
            params_type: None,
            params_check: None,
            mapped: false,
            over: None,
            f: Arc::new(move |ctx: OpCtx| -> BoxFuture<'static, OpResult> { Box::pin(f(ctx)) }),
        }
    }

    /// an op with typed io: upstream outputs are deserialized into `I` (one field
    /// per declared dep, named after it) and the return value serialized back to
    /// json. a mismatch fails the attempt and retries like any other failure.
    pub fn typed<I, O, F, Fut>(name: impl Into<String>, f: F) -> Op
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + 'static,
        F: Fn(OpCtx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let f = Arc::new(f);
        Op {
            name: name.into(),
            deps: Vec::new(),
            requires: Vec::new(),
            io: None,
            aliases: Vec::new(),
            when: When::default(),
            retries: 0,
            retry: DEFAULT_BACKOFF,
            timeout: None,
            pool: None,
            input_type: Some(std::any::type_name::<I>()),
            output_type: Some(std::any::type_name::<O>()),
            params_type: None,
            params_check: None,
            mapped: false,
            over: None,
            f: Arc::new(move |ctx: OpCtx| -> BoxFuture<'static, OpResult> {
                let f = f.clone();
                Box::pin(async move {
                    let fields: serde_json::Map<String, Value> = ctx
                        .inputs
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    let input: I = match serde_json::from_value(Value::Object(fields)) {
                        Ok(input) => input,
                        Err(e) => return Err(ctx.type_check_failed(e)),
                    };
                    let output = f(ctx, input).await?;
                    Ok(serde_json::to_value(output)?)
                })
            }),
        }
    }

    /// an op that fans out: the dep named by [`over`](Self::over) must produce
    /// a json array, and this op runs once per element with that element
    /// deserialized into `T` as its second argument. the other deps are read
    /// with `ctx.input` as usual, whole — including the mapped dep, whose
    /// entry is the entire array.
    ///
    /// each element becomes an instance named `{op}[{i}]`, zero-based, with
    /// its own `op_runs` row; the mapped op itself gets none. its output, seen
    /// downstream under its plain name, is the array of instance outputs in
    /// **element** order, and exists only if every instance succeeded. an
    /// empty array is legal: no instances, output `[]`.
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx};
    /// # use serde_json::json;
    /// Op::mapped("fetch_page", |_ctx: OpCtx, page: u32| async move {
    ///     Ok(json!({ "page": page }))
    /// })
    /// .over("pages");
    /// ```
    pub fn mapped<T, O, F, Fut>(name: impl Into<String>, f: F) -> Op
    where
        T: DeserializeOwned + Send + 'static,
        O: Serialize + 'static,
        F: Fn(OpCtx, T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let f = Arc::new(f);
        let mut op = Op::new(name, move |ctx: OpCtx| {
            let f = f.clone();
            async move {
                // the executor hands every instance its element; nothing else
                // ever calls a mapped body
                let Some(element) = ctx.element.clone() else {
                    return Err("mapped op ran without an element".into());
                };
                let element: T = match serde_json::from_value(element) {
                    Ok(element) => element,
                    Err(e) => return Err(ctx.type_check_failed(e)),
                };
                let output = f(ctx, element).await?;
                Ok(serde_json::to_value(output)?)
            }
        });
        op.input_type = Some(std::any::type_name::<T>());
        op.output_type = Some(std::any::type_name::<O>());
        op.mapped = true;
        op
    }

    /// the dep an [`Op::mapped`] expands over; it is added to the deps if it
    /// was not declared already. required on a mapped op, meaningless on any
    /// other, and either mistake fails the job build.
    pub fn over(mut self, dep: impl Into<String>) -> Op {
        let dep = dep.into();
        if !self.deps.contains(&dep) {
            self.deps.push(dep.clone());
        }
        self.over = Some(dep);
        self
    }

    /// declare upstream ops; appends to any already declared.
    pub fn after<I>(mut self, deps: I) -> Op
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.deps.extend(deps.into_iter().map(Into::into));
        self
    }

    /// the trigger rule: what the deps have to have done for this op to run.
    /// the default, [`When::AllSucceeded`], is the whole upstream working —
    /// what an op without a rule has always meant.
    ///
    /// readiness does not change: an op waits for every dep to reach a
    /// terminal status either way. the rule only decides what happens then,
    /// which is what makes a summary or a cleanup expressible — the thing you
    /// most want to run after a failure is exactly what the default skips.
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx, When};
    /// # use serde_json::json;
    /// Op::new("summary", |ctx: OpCtx| async move {
    ///     let load = ctx.dep_status("load");
    ///     Ok(json!({ "load": load.map(|s| s.as_str()) }))
    /// })
    /// .after(["extract", "load"])
    /// .when(When::Always);
    /// ```
    pub fn when(mut self, when: When) -> Op {
        self.when = when;
        self
    }

    /// declare the [resources](crate::ResourceCtx) this op reads, so a name
    /// that was never registered is a build error instead of a run that gets
    /// halfway and fails. ops may also just ask with
    /// [`OpCtx::resource`](OpCtx::resource) without declaring; declaring is
    /// how you find out at startup rather than at 3am.
    pub fn requires<I>(mut self, names: I) -> Op
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.requires.extend(names.into_iter().map(Into::into));
        self
    }

    /// persist this op's output through the [io manager](crate::IoManager)
    /// registered under `name` with `Hestan::io_named`, instead of the
    /// process default. naming one that was never registered fails the build.
    pub fn io(mut self, name: impl Into<String>) -> Op {
        self.io = Some(name.into());
        self
    }

    /// declare the params type; a launch whose params don't deserialize into
    /// `P` is rejected before any run row is written.
    pub fn params<P: DeserializeOwned + 'static>(mut self) -> Op {
        self.params_type = Some(std::any::type_name::<P>());
        self.params_check = Some(Arc::new(|v: &Value| {
            serde_json::from_value::<P>(v.clone())
                .map(|_| ())
                .map_err(|e| e.to_string())
        }));
        self
    }

    /// extra attempts after the first (default 0).
    pub fn retries(mut self, n: u32) -> Op {
        self.retries = n;
        self
    }

    /// the same pause between every attempt, with no jitter. this replaces the
    /// default backoff, so ops that fail together retry together — say it only
    /// when that is what you want.
    pub fn retry_delay(mut self, d: Duration) -> Op {
        self.retry = Retry::Fixed(d);
        self
    }

    /// pause `base * 2^attempt` between attempts, capped at `max`, with full
    /// jitter: the actual wait is uniform in `[0, that]`, so a group of ops
    /// knocked over by the same rate limit spreads out instead of retrying in
    /// lockstep. this is the default (`base` 1s, `max` 30s).
    pub fn retry_backoff(mut self, base: Duration, max: Duration) -> Op {
        self.retry = Retry::Backoff {
            base,
            max: max.max(base),
        };
        self
    }

    /// fail an attempt that runs longer than `d`. the attempt then goes
    /// through the normal retry policy, and the timeout error names the limit.
    ///
    /// expiry also trips [`OpCtx::is_cancelled`] for that attempt. an async op
    /// is dropped at its next await point; blocking work only stops if it
    /// polls that flag — see the cancellation section of the concepts doc.
    /// the clock starts once the op is running, so waiting for a
    /// [`pool`](Self::pool) permit never counts against it.
    pub fn timeout(mut self, d: Duration) -> Op {
        self.timeout = Some(d);
        self
    }

    /// take a permit from the process-wide pool `name` before running, and
    /// hold it until the attempt ends (however it ends). pools are declared
    /// with `Hestan::pool` and shared by every job in the process, so they
    /// cap what a job's own `max_parallel` cannot: concurrent use of one
    /// external resource. naming an undeclared pool fails the build.
    pub fn pool(mut self, name: impl Into<String>) -> Op {
        self.pool = Some(name.into());
        self
    }

    #[cfg(feature = "http")]
    pub(crate) fn with_output_type(mut self, t: &'static str) -> Op {
        self.output_type = Some(t);
        self
    }

    /// rename this op and replace every dep name it holds — including the one
    /// an [`Op::mapped`] fans out over, which is a dep name like any other.
    /// flattening a [`Graph`](crate::Graph) instance is the only caller.
    pub(crate) fn rebound(
        mut self,
        name: String,
        deps: Vec<String>,
        over: Option<String>,
        aliases: Vec<(String, String)>,
    ) -> Op {
        self.name = name;
        self.deps = deps;
        self.over = over;
        self.aliases = aliases;
        self
    }

    /// what this op's body calls the dep now named `dep`: inside a flattened
    /// [`Graph`](crate::Graph) instance that is the name the graph's author
    /// wrote, and everywhere else it is `dep` itself. this is what keeps a
    /// graph's ops readable — `ctx.input("parse")` inside a graph, not
    /// `ctx.input("clean_a.parse")`.
    pub(crate) fn dep_alias<'a>(&'a self, dep: &'a str) -> &'a str {
        self.aliases
            .iter()
            .find(|(flat, _)| flat == dep)
            .map_or(dep, |(_, local)| local.as_str())
    }

    /// copy `other`'s declared io types; the asset wrapper op uses this so a
    /// typed asset fn keeps its type names on events and job summaries.
    pub(crate) fn with_types_of(mut self, other: &Op) -> Op {
        self.input_type = other.input_type;
        self.output_type = other.output_type;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn deps(&self) -> &[String] {
        &self.deps
    }

    /// this op's trigger rule, from [`when`](Self::when).
    pub fn runs_when(&self) -> When {
        self.when
    }

    /// the resources this op declared with [`requires`](Self::requires).
    pub fn required_resources(&self) -> &[String] {
        &self.requires
    }

    /// the named io manager this op selected with [`io`](Self::io); `None`
    /// means the process default.
    pub fn io_name(&self) -> Option<&str> {
        self.io.as_deref()
    }

    pub fn max_retries(&self) -> u32 {
        self.retries
    }

    /// the per-attempt time limit from [`timeout`](Self::timeout), if any.
    pub fn timeout_after(&self) -> Option<Duration> {
        self.timeout
    }

    /// the pool this op takes a permit from, from [`pool`](Self::pool).
    pub fn pool_name(&self) -> Option<&str> {
        self.pool.as_deref()
    }

    pub fn input_type(&self) -> Option<&'static str> {
        self.input_type
    }

    pub fn output_type(&self) -> Option<&'static str> {
        self.output_type
    }

    pub fn params_type(&self) -> Option<&'static str> {
        self.params_type
    }

    /// the dep this op fans out over, from [`over`](Self::over). `Some` only
    /// on a built [`Op::mapped`], which makes it the runtime's test for one.
    pub fn mapped_over(&self) -> Option<&str> {
        self.over.as_deref()
    }

    pub(crate) fn is_mapped(&self) -> bool {
        self.mapped
    }

    pub(crate) fn validate_params(&self, params: &Value) -> Result<(), String> {
        match &self.params_check {
            Some(check) => check(params),
            None => Ok(()),
        }
    }

    /// how long to wait after `attempt` (counting from 1) failed.
    pub(crate) fn delay(&self, attempt: u32) -> Duration {
        self.retry.delay(attempt)
    }

    pub(crate) fn call(&self, ctx: OpCtx) -> BoxFuture<'static, OpResult> {
        (self.f)(ctx)
    }
}

impl fmt::Debug for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Op")
            .field("name", &self.name)
            .field("deps", &self.deps)
            .field("requires", &self.requires)
            .field("when", &self.when)
            .field("retries", &self.retries)
            .field("retry", &self.retry)
            .field("timeout", &self.timeout)
            .field("pool", &self.pool)
            .field("mapped_over", &self.over)
            .finish_non_exhaustive()
    }
}

/// the two signals that ask one op invocation to stop: the run being canceled,
/// and this attempt's timeout expiring. both are watch channels, so reading
/// one is a lock-free borrow and both stay readable after their sender is
/// gone — a blocking closure that outlives its run still sees the last value.
#[derive(Clone)]
pub(crate) struct Cancel {
    pub(crate) run: watch::Receiver<bool>,
    pub(crate) attempt: watch::Receiver<bool>,
}

// resolves the first time `rx` holds true; parks forever if it never can
async fn flipped(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        // an error means the sender is gone, so the value can never change again
        if rx.changed().await.is_err() {
            return std::future::pending().await;
        }
    }
}

/// handed to each op invocation: run params, upstream outputs, persisted
/// state, event logging.
#[derive(Clone)]
pub struct OpCtx {
    pub(crate) cancel: Cancel,
    pub(crate) run_id: String,
    pub(crate) job: String,
    pub(crate) op: String,
    pub(crate) params: Value,
    /// the one array element this invocation is for, on a fan-out instance;
    /// `None` for every ordinary op.
    pub(crate) element: Option<Value>,
    pub(crate) inputs: Arc<HashMap<String, Value>>,
    /// what each declared dep ended up doing, for an op with a
    /// [`when`](Op::when) rule that let it run anyway.
    pub(crate) dep_statuses: Arc<HashMap<String, OpStatus>>,
    pub(crate) resources: Resources,
    pub(crate) state: Arc<Option<Value>>,
    pub(crate) new_state: Arc<Mutex<Option<Value>>>,
    pub(crate) new_fingerprint: Arc<Mutex<Option<String>>>,
    pub(crate) store: Store,
}

impl OpCtx {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// true once this op has been asked to stop: the run was canceled, or the
    /// attempt's [`Op::timeout`] expired. cheap enough to call in a loop — it
    /// reads a watch channel and allocates nothing.
    ///
    /// an async op does not need this: it is dropped at its next await point.
    /// blocking work (`spawn_blocking`, a long computation, a synchronous
    /// driver) cannot be dropped at all, so polling this is the only way it
    /// ever stops early:
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx};
    /// # use serde_json::json;
    /// Op::new("crunch", |ctx: OpCtx| async move {
    ///     tokio::task::spawn_blocking(move || {
    ///         for chunk in 0..1_000 {
    ///             if ctx.is_cancelled() {
    ///                 return Err("canceled".into());
    ///             }
    ///             # let _ = chunk;
    ///         }
    ///         Ok(json!({"done": true}))
    ///     })
    ///     .await?
    /// });
    /// ```
    pub fn is_cancelled(&self) -> bool {
        *self.cancel.run.borrow() || *self.cancel.attempt.borrow()
    }

    /// a future that resolves once [`is_cancelled`](Self::is_cancelled) turns
    /// true, for async ops to `select!` on. it owns its handles, so it can
    /// outlive the borrow of `self`.
    pub fn cancelled(&self) -> impl Future<Output = ()> + Send + use<> {
        let (run, attempt) = (self.cancel.run.clone(), self.cancel.attempt.clone());
        async move {
            tokio::select! {
                _ = flipped(run) => {}
                _ = flipped(attempt) => {}
            }
        }
    }

    pub fn job(&self) -> &str {
        &self.job
    }

    pub fn params(&self) -> &Value {
        &self.params
    }

    /// the run params deserialized into `P`.
    pub fn params_as<P: DeserializeOwned>(&self) -> Result<P, InputError> {
        serde_json::from_value(self.params.clone()).map_err(|e| InputError::Mismatch(e.to_string()))
    }

    /// output of a declared upstream op.
    pub fn input(&self, op: &str) -> Option<&Value> {
        self.inputs.get(op)
    }

    /// every dep that produced output, name and value, sorted by name.
    ///
    /// an op inside a reusable [`Graph`](crate::Graph) is the reason this
    /// exists: the instance's external deps arrive under the names the *job*
    /// gave them, which the graph's author cannot know, so an input op reads
    /// whatever it was handed rather than a name it made up.
    pub fn inputs(&self) -> Vec<(&str, &Value)> {
        let mut all: Vec<(&str, &Value)> =
            self.inputs.iter().map(|(k, v)| (k.as_str(), v)).collect();
        all.sort_by_key(|(name, _)| *name);
        all
    }

    /// what a declared upstream op ended up doing. this is how an op with a
    /// [`when`](Op::when) rule reports on the run that reached it: `input` for
    /// a dep that produced nothing is `None`, but its status is still a fact.
    ///
    /// `None` means the name is not a declared dep of this op. a dep seeded
    /// from outside the run — a resume's reused output, an asset build's
    /// memoized value, a source asset — reads as
    /// [`Success`](crate::OpStatus::Success), since that is what it stands in
    /// for.
    pub fn dep_status(&self, op: &str) -> Option<OpStatus> {
        self.dep_statuses.get(op).copied()
    }

    /// output of a declared upstream op, deserialized into `T`.
    pub fn input_as<T: DeserializeOwned>(&self, op: &str) -> Result<T, InputError> {
        let v = self
            .inputs
            .get(op)
            .ok_or_else(|| InputError::Missing(op.to_string()))?;
        serde_json::from_value(v.clone()).map_err(|e| InputError::Mismatch(e.to_string()))
    }

    /// a process-wide resource, built once at startup by
    /// `Hestan::resource(name, ..)` and shared by every op that asks — the
    /// replacement for capturing a client in a closure.
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx};
    /// # use serde_json::json;
    /// # struct ApiClient;
    /// Op::new("query", |ctx: OpCtx| async move {
    ///     let api = ctx.resource::<ApiClient>("api")?;   // Arc<ApiClient>
    ///     # let _ = api;
    ///     Ok(json!(null))
    /// })
    /// .requires(["api"]);
    /// ```
    ///
    /// the error says which of the two things went wrong: there is no such
    /// resource, or there is and it is something else.
    pub fn resource<T: std::any::Any + Send + Sync>(
        &self,
        name: &str,
    ) -> Result<Arc<T>, InputError> {
        resource::lookup(&self.resources, name)
    }

    /// the state this op's last successful run committed via
    /// [`set_state`](Self::set_state). loaded once, before the first attempt.
    pub fn state(&self) -> Option<&Value> {
        (*self.state).as_ref()
    }

    /// [`state`](Self::state) deserialized into `T`; `Ok(None)` when no state
    /// has ever been committed.
    pub fn state_as<T: DeserializeOwned>(&self) -> Result<Option<T>, InputError> {
        match (*self.state).as_ref() {
            Some(v) => serde_json::from_value(v.clone())
                .map(Some)
                .map_err(|e| InputError::Mismatch(e.to_string())),
            None => Ok(None),
        }
    }

    /// stage state to persist if this attempt succeeds. the last call wins;
    /// nothing is written when the attempt fails, so the next attempt (and the
    /// next run) still reads the old value.
    pub fn set_state(&self, v: Value) {
        *self.new_state.lock().unwrap() = Some(v);
    }

    /// override the fingerprint an asset materialization records, instead of the
    /// default content hash. buffered like [`set_state`](Self::set_state).
    /// outside an asset op it does nothing.
    pub fn set_fingerprint(&self, s: String) {
        *self.new_fingerprint.lock().unwrap() = Some(s);
    }

    pub(crate) fn take_fingerprint(&self) -> Option<String> {
        self.new_fingerprint.lock().unwrap().take()
    }

    pub fn info(&self, msg: impl AsRef<str>) {
        self.log(EventLevel::Info, msg.as_ref());
    }

    pub fn warn(&self, msg: impl AsRef<str>) {
        self.log(EventLevel::Warn, msg.as_ref());
    }

    pub fn error(&self, msg: impl AsRef<str>) {
        self.log(EventLevel::Error, msg.as_ref());
    }

    fn log(&self, level: EventLevel, msg: &str) {
        self.event(level, EventKind::Log, msg, None);
    }

    /// emit the type_check_failed event and build the matching error.
    pub(crate) fn type_check_failed(
        &self,
        err: impl fmt::Display,
    ) -> Box<dyn std::error::Error + Send + Sync> {
        let err = err.to_string();
        let msg = format!("type check failed: {err}");
        self.event(
            EventLevel::Error,
            EventKind::TypeCheckFailed,
            &msg,
            Some(&json!({ "error": err })),
        );
        msg.into()
    }

    pub(crate) fn event(
        &self,
        level: EventLevel,
        kind: EventKind,
        msg: &str,
        data: Option<&Value>,
    ) {
        // a lost log line should not fail the op
        if let Err(e) =
            self.store
                .append_event(&self.run_id, Some(&self.op), level, kind, msg, data)
        {
            tracing::warn!(run = %self.run_id, op = %self.op, "event write failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn op() -> Op {
        Op::new("x", |_| async { Ok(json!(null)) })
    }

    fn ctx_with(cancel: Cancel) -> OpCtx {
        OpCtx {
            cancel,
            run_id: "r1".into(),
            job: "j".into(),
            op: "x".into(),
            params: json!({}),
            element: None,
            inputs: Arc::new(HashMap::new()),
            dep_statuses: Arc::new(HashMap::new()),
            resources: resource::none(),
            state: Arc::new(None),
            new_state: Arc::new(Mutex::new(None)),
            new_fingerprint: Arc::new(Mutex::new(None)),
            store: Store::open(":memory:").unwrap(),
        }
    }

    // the two halves are independent: a run cancel and this attempt's timeout
    #[tokio::test]
    async fn either_signal_cancels_and_neither_fires_on_its_own() {
        let brief = Duration::from_millis(20);
        for flip_the_run in [true, false] {
            let (run_tx, run) = watch::channel(false);
            let (attempt_tx, attempt) = watch::channel(false);
            let ctx = ctx_with(Cancel { run, attempt });

            assert!(!ctx.is_cancelled());
            assert!(
                tokio::time::timeout(brief, ctx.cancelled()).await.is_err(),
                "resolved before anything asked for a stop"
            );

            let _ = if flip_the_run {
                run_tx.send(true)
            } else {
                attempt_tx.send(true)
            };
            assert!(ctx.is_cancelled());
            tokio::time::timeout(brief, ctx.cancelled())
                .await
                .expect("cancelled() never resolved");
        }
    }

    // a run that ends without ever being canceled drops both senders; the
    // blocking closure still holding the ctx must not read that as a stop
    #[tokio::test]
    async fn a_dropped_sender_is_not_a_cancellation() {
        let (run_tx, run) = watch::channel(false);
        let (attempt_tx, attempt) = watch::channel(false);
        let ctx = ctx_with(Cancel { run, attempt });
        drop((run_tx, attempt_tx));

        assert!(!ctx.is_cancelled());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), ctx.cancelled())
                .await
                .is_err(),
            "a closed channel resolved as if it had been canceled"
        );
    }

    // and one that was canceled keeps saying so after the run is gone
    #[tokio::test]
    async fn cancellation_outlives_the_run_that_asked_for_it() {
        let (run_tx, run) = watch::channel(false);
        let (attempt_tx, attempt) = watch::channel(false);
        let ctx = ctx_with(Cancel { run, attempt });
        run_tx.send(true).unwrap();
        drop((run_tx, attempt_tx));

        assert!(ctx.is_cancelled());
        tokio::time::timeout(Duration::from_millis(20), ctx.cancelled())
            .await
            .expect("cancelled() never resolved");
    }

    #[test]
    fn default_policy_is_jittered_backoff() {
        let op = op();
        // every wait sits inside its attempt's window, and the windows double
        for attempt in 1..=5u32 {
            let window = Duration::from_secs(1) * 2u32.pow(attempt - 1);
            let samples: Vec<Duration> = (0..32).map(|_| op.delay(attempt)).collect();
            assert!(
                samples.iter().all(|d| *d <= window),
                "attempt {attempt} waited past {window:?}: {samples:?}"
            );
            assert!(
                samples.iter().any(|d| *d > window / 2),
                "attempt {attempt} never used its window: {samples:?}"
            );
        }
        // and the cap holds however many attempts pile up
        assert!(op.delay(20) <= Duration::from_secs(30));
    }

    #[test]
    fn fixed_delay_has_no_jitter() {
        let op = op().retry_delay(Duration::from_millis(250));
        for attempt in 1..=5 {
            assert_eq!(op.delay(attempt), Duration::from_millis(250));
        }
    }

    #[test]
    fn backoff_respects_its_own_base_and_cap() {
        let op = op().retry_backoff(Duration::from_millis(100), Duration::from_millis(400));
        assert!(op.delay(1) <= Duration::from_millis(100));
        assert!(op.delay(2) <= Duration::from_millis(200));
        assert!(op.delay(9) <= Duration::from_millis(400));
        // a cap below the base is nonsense; the base wins rather than pacing backwards
        let op = op.retry_backoff(Duration::from_secs(2), Duration::from_secs(1));
        assert!(op.delay(3) <= Duration::from_secs(2));
    }
}
