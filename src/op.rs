use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::model::{EventKind, EventLevel};
use crate::store::Store;

/// what an op body returns: json output on success, any error on failure.
pub type OpResult = Result<Value, Box<dyn std::error::Error + Send + Sync>>;

type OpFn = dyn Fn(OpCtx) -> BoxFuture<'static, OpResult> + Send + Sync;
type ParamsCheck = dyn Fn(&Value) -> Result<(), String> + Send + Sync;

/// why a typed accessor on [`OpCtx`] came up empty.
#[derive(Debug, thiserror::Error)]
pub enum InputError {
    #[error("no input from op {0}")]
    Missing(String),
    #[error("type mismatch: {0}")]
    Mismatch(String),
}

/// a named unit of work: an async fn plus the upstream ops it waits on.
#[derive(Clone)]
pub struct Op {
    name: String,
    deps: Vec<String>,
    retries: u32,
    retry_delay: Duration,
    input_type: Option<&'static str>,
    output_type: Option<&'static str>,
    params_type: Option<&'static str>,
    params_check: Option<Arc<ParamsCheck>>,
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
            retries: 0,
            retry_delay: Duration::from_secs(1),
            input_type: None,
            output_type: None,
            params_type: None,
            params_check: None,
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
            retries: 0,
            retry_delay: Duration::from_secs(1),
            input_type: Some(std::any::type_name::<I>()),
            output_type: Some(std::any::type_name::<O>()),
            params_type: None,
            params_check: None,
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

    /// declare upstream ops; appends to any already declared.
    pub fn after<I>(mut self, deps: I) -> Op
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.deps.extend(deps.into_iter().map(Into::into));
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

    /// pause between attempts (default 1s).
    pub fn retry_delay(mut self, d: Duration) -> Op {
        self.retry_delay = d;
        self
    }

    #[cfg(feature = "http")]
    pub(crate) fn with_output_type(mut self, t: &'static str) -> Op {
        self.output_type = Some(t);
        self
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

    pub fn max_retries(&self) -> u32 {
        self.retries
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

    pub(crate) fn validate_params(&self, params: &Value) -> Result<(), String> {
        match &self.params_check {
            Some(check) => check(params),
            None => Ok(()),
        }
    }

    pub(crate) fn delay(&self) -> Duration {
        self.retry_delay
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
            .field("retries", &self.retries)
            .finish_non_exhaustive()
    }
}

/// handed to each op invocation: run params, upstream outputs, persisted
/// state, event logging.
#[derive(Clone)]
pub struct OpCtx {
    pub(crate) run_id: String,
    pub(crate) job: String,
    pub(crate) op: String,
    pub(crate) params: Value,
    pub(crate) inputs: Arc<HashMap<String, Value>>,
    pub(crate) state: Arc<Option<Value>>,
    pub(crate) new_state: Arc<Mutex<Option<Value>>>,
    pub(crate) new_fingerprint: Arc<Mutex<Option<String>>>,
    pub(crate) store: Store,
}

impl OpCtx {
    pub fn run_id(&self) -> &str {
        &self.run_id
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

    /// output of a declared upstream op, deserialized into `T`.
    pub fn input_as<T: DeserializeOwned>(&self, op: &str) -> Result<T, InputError> {
        let v = self
            .inputs
            .get(op)
            .ok_or_else(|| InputError::Missing(op.to_string()))?;
        serde_json::from_value(v.clone()).map_err(|e| InputError::Mismatch(e.to_string()))
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
