use std::collections::{BTreeMap, HashMap};
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

/// how many rows a [`Meta::table`] keeps, applied at construction. a metadata
/// table is a sample you read at a glance — the top regions, the columns that
/// changed type — not a result set, and a pipeline that reports its whole
/// output here would put it in every run page, every history entry and every
/// api response.
pub const META_TABLE_ROWS: usize = 100;

/// one column of a [`MetaTable`]: a name, and the type it holds when the op
/// knows it. `"orders"` and `("orders", "int")` both convert, so a table's
/// columns are usually written as a literal array.
#[derive(Debug, Clone, PartialEq)]
pub struct MetaColumn {
    name: String,
    ty: Option<String>,
}

impl MetaColumn {
    pub fn new(name: impl Into<String>) -> MetaColumn {
        MetaColumn {
            name: name.into(),
            ty: None,
        }
    }

    /// a column that also names its type. the type is a label for whoever
    /// reads it — hestan never parses it, so it is your vocabulary, not one
    /// hestan imposes.
    pub fn typed(name: impl Into<String>, ty: impl Into<String>) -> MetaColumn {
        MetaColumn {
            name: name.into(),
            ty: Some(ty.into()),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn ty(&self) -> Option<&str> {
        self.ty.as_deref()
    }
}

impl From<&str> for MetaColumn {
    fn from(name: &str) -> MetaColumn {
        MetaColumn::new(name)
    }
}

impl From<String> for MetaColumn {
    fn from(name: String) -> MetaColumn {
        MetaColumn::new(name)
    }
}

impl<N: Into<String>, T: Into<String>> From<(N, T)> for MetaColumn {
    fn from((name, ty): (N, T)) -> MetaColumn {
        MetaColumn::typed(name, ty)
    }
}

/// a small table an op reports about what it produced — a sample of the rows,
/// a per-region breakdown, a schema. built with [`Meta::table`], which is the
/// only way to make one: the row cap and the rectangle below are invariants
/// rather than things a caller is asked to maintain.
///
/// rows are padded with `null` and truncated to the column count, so every row
/// has exactly one cell per column and nothing downstream has to decide what a
/// ragged row means.
#[derive(Debug, Clone, PartialEq)]
pub struct MetaTable {
    columns: Vec<MetaColumn>,
    rows: Vec<Vec<Value>>,
    truncated: bool,
}

impl MetaTable {
    pub fn columns(&self) -> &[MetaColumn] {
        &self.columns
    }

    pub fn rows(&self) -> &[Vec<Value>] {
        &self.rows
    }

    /// whether rows were dropped to fit [`META_TABLE_ROWS`]. carried through
    /// to the api and the ui, so a hundred-row table that is all there was and
    /// one that is the head of a million read differently.
    pub fn truncated(&self) -> bool {
        self.truncated
    }
}

/// a typed fact an op attaches to what it produced, via [`OpCtx::meta`]. the
/// type is not decoration: it is what lets a row count render as a number, a
/// source as a link, and a blob as a blob, without anything downstream
/// guessing from the value.
///
/// the obvious rust types convert on their own — `ctx.meta("rows", 1_234)`,
/// `ctx.meta("note", "backfilled")`, `ctx.meta("took", elapsed)` — and the
/// rest are named: `ctx.meta("source", Meta::Url(url))`,
/// `ctx.meta("rows", Meta::count(1_240))`. `u64` and `usize` deliberately do
/// not convert, since narrowing them is a lie waiting to happen; cast them
/// yourself, or say which kind of number you meant with
/// [`count`](Meta::count) or [`bytes`](Meta::bytes).
#[derive(Debug, Clone, PartialEq)]
pub enum Meta {
    Int(i64),
    Float(f64),
    Text(String),
    Url(String),
    /// markdown source, rendered by the ui as a
    /// [documented subset](../docs/metadata.md#the-markdown-subset).
    Markdown(String),
    Json(Value),
    /// a sample of rows with named columns; build it with [`Meta::table`].
    Table(MetaTable),
    /// a size in bytes, rendered `1.2 GB` rather than as the integer.
    Bytes(u64),
    /// an elapsed time, rendered `3.4s`. stored as seconds.
    Duration(Duration),
    /// a plain count, rendered `1,240`. the same number as an [`Int`](Meta::Int)
    /// and a different claim about it: a count is a quantity of things, so it
    /// is never negative and a delta against it means something.
    Count(u64),
    /// a filesystem path, rendered monospace with the basename emphasised.
    Path(String),
    /// another run of this deployment, by id, rendered as a link to it. dagster
    /// needs a url here; hestan knows its own graph.
    RunRef(String),
    /// an asset of this deployment, by name, rendered as a link to it.
    AssetRef(String),
}

impl Meta {
    /// a table of at most [`META_TABLE_ROWS`] rows. columns are names or
    /// `(name, type)` pairs; rows are json cells, padded and truncated to the
    /// column count:
    ///
    /// ```no_run
    /// # use hestan::{Meta, Op, OpCtx};
    /// # use serde_json::json;
    /// # let rows: Vec<Vec<serde_json::Value>> = vec![];
    /// Meta::table([("region", "text"), ("orders", "int")], rows);
    /// ```
    pub fn table<C, R>(columns: C, rows: R) -> Meta
    where
        C: IntoIterator,
        C::Item: Into<MetaColumn>,
        R: IntoIterator<Item = Vec<Value>>,
    {
        let columns: Vec<MetaColumn> = columns.into_iter().map(Into::into).collect();
        let width = columns.len();
        let mut kept: Vec<Vec<Value>> = Vec::new();
        let mut truncated = false;
        for mut row in rows {
            if kept.len() == META_TABLE_ROWS {
                truncated = true;
                break;
            }
            row.truncate(width);
            row.resize(width, Value::Null);
            kept.push(row);
        }
        Meta::Table(MetaTable {
            columns,
            rows: kept,
            truncated,
        })
    }

    /// a size in bytes: `Meta::bytes(1_288_490_188)` reads as `1.2 GB`.
    pub fn bytes(n: u64) -> Meta {
        Meta::Bytes(n)
    }

    /// a quantity of things: `Meta::count(1_240)` reads as `1,240`.
    pub fn count(n: u64) -> Meta {
        Meta::Count(n)
    }

    pub fn duration(d: Duration) -> Meta {
        Meta::Duration(d)
    }

    pub fn path(p: impl Into<String>) -> Meta {
        Meta::Path(p.into())
    }

    /// a link to another run of this deployment, by id.
    pub fn run_ref(id: impl Into<String>) -> Meta {
        Meta::RunRef(id.into())
    }

    /// a link to an asset of this deployment, by name.
    pub fn asset_ref(name: impl Into<String>) -> Meta {
        Meta::AssetRef(name.into())
    }

    /// the stored shape: one tagged value per name, so
    /// `{"rows": {"int": 1234}, "source": {"url": ".."}}`. written out rather
    /// than derived, because it is a wire format the api and the ui read — and
    /// a format nothing may quietly renumber, since rows written by an older
    /// hestan are still on disk. every tag ever emitted still reads
    /// ([`from_tagged`](Self::from_tagged)); the ones this phase added are
    /// `table`, `bytes`, `duration_secs`, `count`, `path`, `run` and `asset`.
    ///
    /// a duration's tag names its unit, because the number alone cannot: it is
    /// seconds, as a float.
    pub fn tagged(&self) -> Value {
        match self {
            Meta::Int(v) => json!({ "int": v }),
            Meta::Float(v) => json!({ "float": v }),
            Meta::Text(v) => json!({ "text": v }),
            Meta::Url(v) => json!({ "url": v }),
            Meta::Markdown(v) => json!({ "markdown": v }),
            Meta::Json(v) => json!({ "json": v }),
            Meta::Table(t) => json!({ "table": {
                "columns": t.columns.iter()
                    .map(|c| json!({ "name": c.name, "type": c.ty }))
                    .collect::<Vec<Value>>(),
                "rows": t.rows,
                "truncated": t.truncated,
            }}),
            Meta::Bytes(v) => json!({ "bytes": v }),
            Meta::Duration(d) => json!({ "duration_secs": d.as_secs_f64() }),
            Meta::Count(v) => json!({ "count": v }),
            Meta::Path(v) => json!({ "path": v }),
            Meta::RunRef(v) => json!({ "run": v }),
            Meta::AssetRef(v) => json!({ "asset": v }),
        }
    }

    /// read one stored value back, the inverse of [`tagged`](Self::tagged).
    /// `None` for anything that is not a one-key object with a tag this
    /// version knows — a value written by a *newer* hestan reads as unknown
    /// rather than as a guess.
    pub fn from_tagged(v: &Value) -> Option<Meta> {
        let object = v.as_object()?;
        if object.len() != 1 {
            return None;
        }
        let (tag, v) = object.iter().next()?;
        Some(match tag.as_str() {
            "int" => Meta::Int(v.as_i64()?),
            "float" => Meta::Float(v.as_f64()?),
            "text" => Meta::Text(v.as_str()?.to_string()),
            "url" => Meta::Url(v.as_str()?.to_string()),
            "markdown" => Meta::Markdown(v.as_str()?.to_string()),
            "json" => Meta::Json(v.clone()),
            "table" => {
                let t = v.as_object()?;
                let columns = t
                    .get("columns")?
                    .as_array()?
                    .iter()
                    .map(|c| {
                        let name = c.get("name")?.as_str()?.to_string();
                        let ty = match c.get("type") {
                            None | Some(Value::Null) => None,
                            Some(ty) => Some(ty.as_str()?.to_string()),
                        };
                        Some(MetaColumn { name, ty })
                    })
                    .collect::<Option<Vec<MetaColumn>>>()?;
                let rows = t
                    .get("rows")?
                    .as_array()?
                    .iter()
                    .map(|r| Some(r.as_array()?.clone()))
                    .collect::<Option<Vec<Vec<Value>>>>()?;
                Meta::Table(MetaTable {
                    columns,
                    rows,
                    truncated: t.get("truncated").and_then(Value::as_bool).unwrap_or(false),
                })
            }
            "bytes" => Meta::Bytes(v.as_u64()?),
            "duration_secs" => Meta::Duration(Duration::from_secs_f64(v.as_f64()?.max(0.0))),
            "count" => Meta::Count(v.as_u64()?),
            "path" => Meta::Path(v.as_str()?.to_string()),
            "run" => Meta::RunRef(v.as_str()?.to_string()),
            "asset" => Meta::AssetRef(v.as_str()?.to_string()),
            _ => return None,
        })
    }

    /// the number a numeric variant carries — `Int`, `Float`, `Bytes`,
    /// `Duration` (seconds) and `Count` — and `None` for every variant that is
    /// not a number. this is what deltas and trends are computed over, so the
    /// units are display types over the same one number and a `Count` compares
    /// against an `Int` of the same value.
    pub fn as_f64(&self) -> Option<f64> {
        let n = match self {
            Meta::Int(v) => *v as f64,
            Meta::Float(v) => *v,
            Meta::Bytes(v) | Meta::Count(v) => *v as f64,
            Meta::Duration(d) => d.as_secs_f64(),
            _ => return None,
        };
        // NaN and the infinities compare against nothing; a stored one is a
        // json null anyway, which never reads back as a Float
        n.is_finite().then_some(n)
    }
}

impl From<i64> for Meta {
    fn from(v: i64) -> Meta {
        Meta::Int(v)
    }
}

impl From<i32> for Meta {
    fn from(v: i32) -> Meta {
        Meta::Int(v.into())
    }
}

impl From<u32> for Meta {
    fn from(v: u32) -> Meta {
        Meta::Int(v.into())
    }
}

impl From<f64> for Meta {
    fn from(v: f64) -> Meta {
        Meta::Float(v)
    }
}

impl From<String> for Meta {
    fn from(v: String) -> Meta {
        Meta::Text(v)
    }
}

impl From<&str> for Meta {
    fn from(v: &str) -> Meta {
        Meta::Text(v.to_string())
    }
}

impl From<Value> for Meta {
    fn from(v: Value) -> Meta {
        Meta::Json(v)
    }
}

impl From<Duration> for Meta {
    fn from(d: Duration) -> Meta {
        Meta::Duration(d)
    }
}

/// one attempt's staged metadata, keyed by name. a `BTreeMap` so the stored
/// object's keys come out in a stable order.
pub(crate) type MetaBuf = Arc<Mutex<BTreeMap<String, Meta>>>;

/// what an attempt staged for one asset in particular, rather than for the op
/// as a whole — the form a [`MultiAsset`](crate::MultiAsset) needs, since its
/// several outputs each get their own fingerprint and their own facts.
#[derive(Default)]
pub(crate) struct AssetStage {
    fingerprint: Option<String>,
    meta: BTreeMap<String, Meta>,
}

/// one attempt's per-asset staging, keyed by asset name.
pub(crate) type AssetBuf = Arc<Mutex<BTreeMap<String, AssetStage>>>;

/// a metadata map as it is stored, or `None` when it is empty — which is a
/// null column, not an empty object.
pub(crate) fn tagged_map(map: &BTreeMap<String, Meta>) -> Option<Value> {
    if map.is_empty() {
        return None;
    }
    Some(Value::Object(
        map.iter()
            .map(|(name, meta)| (name.clone(), meta.tagged()))
            .collect(),
    ))
}

/// what one attempt staged, in stored form.
pub(crate) fn staged_meta(buf: &MetaBuf) -> Option<Value> {
    tagged_map(&buf.lock().unwrap())
}

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
    // run the body in a child process rather than in this one, from
    // `isolated`. the runtime's test for one.
    isolated: bool,
    // rlimits the child applies to itself before the body runs; both need
    // `isolated`, and the job build refuses them without it
    memory_limit: Option<u64>,
    cpu_limit: Option<Duration>,
    input_type: Option<&'static str>,
    output_type: Option<&'static str>,
    params_type: Option<&'static str>,
    params_check: Option<Arc<ParamsCheck>>,
    // a json schema for the launchpad to read; never consulted by the check
    params_schema: Option<Value>,
    // built by `mapped`, so `over` missing is a build error rather than an op
    // that silently runs once over nothing
    mapped: bool,
    over: Option<String>,
    // instances named by their element rather than their index; set only by
    // `fans_out_over`, which is how a partitioned asset expands
    labeled: bool,
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
            isolated: false,
            memory_limit: None,
            cpu_limit: None,
            input_type: None,
            output_type: None,
            params_type: None,
            params_check: None,
            params_schema: None,
            mapped: false,
            over: None,
            labeled: false,
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
            isolated: false,
            memory_limit: None,
            cpu_limit: None,
            input_type: Some(std::any::type_name::<I>()),
            output_type: Some(std::any::type_name::<O>()),
            params_type: None,
            params_check: None,
            params_schema: None,
            mapped: false,
            over: None,
            labeled: false,
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

    /// fan out over `dep` like [`Op::mapped`], but with instances named by
    /// their element rather than their index, and without the element being
    /// deserialized into the body's second argument — it reads its own key off
    /// the ctx instead. this is how a [partitioned
    /// asset](crate::Partitions) reuses the fan-out machinery whole:
    /// `daily_orders[2026-01-05]` rather than `daily_orders[4]`.
    ///
    /// elements that are not strings still fall back to the index, and
    /// repeated ones fail the expansion — two instances cannot share a name.
    pub(crate) fn fans_out_over(mut self, dep: impl Into<String>) -> Op {
        self.mapped = true;
        self.labeled = true;
        self.over(dep)
    }

    /// whether an instance of this op is named by its element.
    pub(crate) fn labels_instances(&self) -> bool {
        self.labeled
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

    /// a json schema for this op's params, carried through to the api so the
    /// launchpad can list the fields instead of showing an empty textarea and
    /// a type name.
    ///
    /// **it is a ui aid, not a second validator.** the authority is and stays
    /// the serde round-trip [`params`](Self::params) installs: every launch
    /// deserializes into `P`, so a schema that disagrees with `P` cannot admit
    /// anything `P` refuses — it can only describe it wrongly, which is a bad
    /// legend rather than a hole. nothing here is ever checked against the
    /// params.
    ///
    /// hestan takes no schemars dependency; the value is whatever you hand it,
    /// and `schemars::schema_for!(P)` produces exactly this in one line:
    ///
    /// ```ignore
    /// Op::new("fetch", body)
    ///     .params::<Fetch>()
    ///     .params_schema(serde_json::to_value(schemars::schema_for!(Fetch))?)
    /// ```
    ///
    /// only `properties`, `required` and `$defs`/`definitions` are read, to
    /// merge every op's schema into one for the job — see
    /// [`Job::params_schema`](crate::Job::params_schema). the rest is passed
    /// through untouched. the schema must be a json object, and two ops giving
    /// one property name different shapes is a build error.
    pub fn params_schema(mut self, schema: Value) -> Op {
        self.params_schema = Some(schema);
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

    /// run this op's body in a child process instead of in the orchestrator's.
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx};
    /// # use serde_json::json;
    /// Op::new("parse_untrusted", |_ctx: OpCtx| async { Ok(json!(null)) }).isolated();
    /// ```
    ///
    /// what it buys is containment. an op that segfaults, aborts or exhausts
    /// memory takes down the process it runs in, and in-process that process
    /// is hestan — every other run with it. isolated, the blast radius is one
    /// attempt: the child dies, the parent records **why** it died, and the
    /// forty other ops carry on.
    ///
    /// it also makes stopping real. cancelling a run or expiring an
    /// [`Op::timeout`] asks an in-process op to stop and cannot make it —
    /// see the cancellation section of the concepts doc. an isolated op is
    /// sent SIGTERM, given a few seconds, and then SIGKILLed, so for once
    /// "canceled" is something hestan watched rather than requested.
    ///
    /// the child is **this same binary**, re-executed with two environment
    /// variables set; it rebuilds the same jobs because it runs the same
    /// `main`. that is the whole cost model — a process spawn per attempt,
    /// milliseconds rather than the seconds an interpreter start costs — and
    /// the whole constraint: a `main` that registers a different set of jobs
    /// depending on argv, or that reads a different database, cannot host a
    /// worker. nothing is passed to the child but the run id and the op name.
    /// everything else — params, inputs, state — it reads out of the store,
    /// and everything it produces it writes back the same way.
    ///
    /// an isolated op is otherwise an ordinary unit: `max_parallel`, pools,
    /// retries (each attempt is a fresh child), [`When`] rules and the run's
    /// cancellation all apply unchanged. it may not be
    /// [mapped](Op::mapped) — a fan-out instance's element is the one input
    /// that is not a row a child could read — and hestan supports it on unix
    /// only, both refused at build rather than quietly ignored.
    ///
    /// see [isolation](../docs/isolation.md).
    pub fn isolated(mut self) -> Op {
        self.isolated = true;
        self
    }

    /// cap the address space of an [`isolated`](Self::isolated) op's child
    /// process at `bytes`, so a runaway allocation is that op's failure rather
    /// than the machine's problem.
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx};
    /// # use serde_json::json;
    /// Op::new("parse", |_ctx: OpCtx| async { Ok(json!(null)) })
    ///     .isolated()
    ///     .memory_limit(512 * 1024 * 1024);
    /// ```
    ///
    /// the child applies it to itself with `setrlimit(RLIMIT_AS)` just before
    /// the body runs. an allocation past it fails, which in rust aborts the
    /// process, and the parent records the death naming this limit rather than
    /// a bare signal number.
    ///
    /// two things it is not. it is **address space**, not resident memory:
    /// large reservations count even untouched, which is what makes the failure
    /// deterministic instead of a visit from the oom killer at some later
    /// moment of the kernel's choosing. and it covers the **whole child**, not
    /// the body alone — a few megabytes of hestan, sqlite and your process's
    /// own startup are inside it, so leave headroom.
    ///
    /// without [`isolated`](Self::isolated) this is a build error: the limit
    /// applies to a process, and in-process that process is the orchestrator.
    pub fn memory_limit(mut self, bytes: u64) -> Op {
        self.memory_limit = Some(bytes);
        self
    }

    /// cap the cpu time an [`isolated`](Self::isolated) op's child process may
    /// burn, via `setrlimit(RLIMIT_CPU)`. exceeding it arrives as SIGXCPU,
    /// which by default ends the process, and the parent records it naming this
    /// limit.
    ///
    /// this is **cpu time, not wall clock**: an op that waits an hour on a
    /// socket has spent no cpu at all and is untouched by it, which is exactly
    /// the difference from [`timeout`](Self::timeout). reach for this against a
    /// spin loop or a runaway regex, and for `timeout` against something slow.
    /// the two compose; they are measuring different things.
    ///
    /// the limit has one-second granularity — the kernel's, not hestan's — and
    /// anything under a second means one. without
    /// [`isolated`](Self::isolated) it is a build error, for the same reason a
    /// [`memory_limit`](Self::memory_limit) is.
    pub fn cpu_limit(mut self, d: Duration) -> Op {
        self.cpu_limit = Some(d);
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

    /// whether this op's body runs in a child process, from
    /// [`isolated`](Self::isolated).
    pub fn is_isolated(&self) -> bool {
        self.isolated
    }

    /// the address-space cap this op declared with
    /// [`memory_limit`](Self::memory_limit), in bytes.
    pub fn declared_memory_limit(&self) -> Option<u64> {
        self.memory_limit
    }

    /// the cpu-time cap this op declared with [`cpu_limit`](Self::cpu_limit).
    pub fn declared_cpu_limit(&self) -> Option<Duration> {
        self.cpu_limit
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

    /// the schema this op declared with [`params_schema`](Self::params_schema),
    /// exactly as it was handed over.
    pub fn declared_params_schema(&self) -> Option<&Value> {
        self.params_schema.as_ref()
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
            .field("isolated", &self.isolated)
            .field("memory_limit", &self.memory_limit)
            .field("cpu_limit", &self.cpu_limit)
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
pub(crate) async fn flipped(mut rx: watch::Receiver<bool>) {
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
    /// the cron occurrence this run stands for; `None` outside a scheduled or
    /// caught-up run.
    pub(crate) scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    /// the one array element this invocation is for, on a fan-out instance;
    /// `None` for every ordinary op.
    pub(crate) element: Option<Value>,
    /// the partition key this invocation is for, on a [partitioned
    /// asset](crate::Partitions); `None` everywhere else.
    pub(crate) partition: Option<String>,
    pub(crate) inputs: Arc<HashMap<String, Value>>,
    /// what each declared dep ended up doing, for an op with a
    /// [`when`](Op::when) rule that let it run anyway.
    pub(crate) dep_statuses: Arc<HashMap<String, OpStatus>>,
    pub(crate) resources: Resources,
    pub(crate) state: Arc<Option<Value>>,
    pub(crate) new_state: Arc<Mutex<Option<Value>>>,
    pub(crate) new_fingerprint: Arc<Mutex<Option<String>>>,
    pub(crate) new_meta: MetaBuf,
    /// fingerprints and metadata staged for one named asset, for an op that
    /// produces several.
    pub(crate) new_per_asset: AssetBuf,
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

    /// the key this invocation is materializing, inside a [partitioned
    /// asset](crate::Partitions). `None` in an unpartitioned asset and in
    /// every ordinary op, which is what it has always effectively said.
    pub fn partition(&self) -> Option<&str> {
        self.partition.as_deref()
    }

    /// the array element a fan-out instance was handed, before any
    /// deserialization; `None` for an ordinary op.
    pub(crate) fn element(&self) -> Option<&Value> {
        self.element.as_ref()
    }

    pub fn params(&self) -> &Value {
        &self.params
    }

    /// the logical time this run is for: the cron occurrence a scheduled fire
    /// fired for, which is **not** the wall clock it launched at once a
    /// schedule is [catching up](crate::Catchup) or a held fire drains. `None`
    /// on a manual launch, a retry, a resume, a build or a sensor fire.
    ///
    /// this is the difference between "pull yesterday's orders" and "pull the
    /// orders for the hour this run is standing in for":
    ///
    /// ```no_run
    /// # use hestan::{Op, OpCtx};
    /// # use serde_json::json;
    /// Op::new("pull", |ctx: OpCtx| async move {
    ///     let hour = ctx.scheduled_for().unwrap_or_else(chrono::Utc::now);
    ///     Ok(json!({ "hour": hour.to_rfc3339() }))
    /// });
    /// ```
    pub fn scheduled_for(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.scheduled_for
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

    /// override the fingerprint of one asset this op produces, for a
    /// [`MultiAsset`](crate::MultiAsset) whose outputs do not all want the
    /// same rule. [`set_fingerprint`](Self::set_fingerprint) on such an op
    /// covers every output that did not get one of these; on an op producing a
    /// single asset the two say the same thing.
    pub fn set_fingerprint_of(&self, asset: impl Into<String>, fingerprint: String) {
        self.new_per_asset
            .lock()
            .unwrap()
            .entry(asset.into())
            .or_default()
            .fingerprint = Some(fingerprint);
    }

    /// attach a typed fact to one asset this op produces, the per-asset form of
    /// [`meta`](Self::meta). it lands on that asset's materialization; the
    /// op run row still carries what plain `meta` staged, which is what an op
    /// producing several assets reports about the work as a whole.
    pub fn meta_of(
        &self,
        asset: impl Into<String>,
        name: impl Into<String>,
        value: impl Into<Meta>,
    ) {
        self.new_per_asset
            .lock()
            .unwrap()
            .entry(asset.into())
            .or_default()
            .meta
            .insert(name.into(), value.into());
    }

    /// what this attempt staged for `asset` in particular: its fingerprint
    /// override and its metadata, in stored form. read, not taken, so several
    /// produced assets can each read their own.
    pub(crate) fn staged_for(&self, asset: &str) -> (Option<String>, Option<Value>) {
        let staged = self.new_per_asset.lock().unwrap();
        match staged.get(asset) {
            None => (None, None),
            Some(s) => (s.fingerprint.clone(), tagged_map(&s.meta)),
        }
    }

    /// attach a typed fact to what this op produced — a row count, the url it
    /// read, a note about the shape of the data. the last call for a name
    /// wins, and everything staged is committed with the op's terminal write:
    ///
    /// ```no_run
    /// # use hestan::{Meta, Op, OpCtx};
    /// # use serde_json::json;
    /// Op::new("load", |ctx: OpCtx| async move {
    ///     ctx.meta("rows", 1_234);
    ///     ctx.meta("source", Meta::Url("https://example.test/orders".into()));
    ///     Ok(json!({"loaded": true}))
    /// });
    /// ```
    ///
    /// buffered per attempt like [`set_state`](Self::set_state), so a failed
    /// attempt's metadata is discarded and the retry starts from nothing. an
    /// asset op's metadata lands on its materialization as well, so the
    /// [history](crate::Store::materializations) carries what each build
    /// reported.
    pub fn meta(&self, name: impl Into<String>, value: impl Into<Meta>) {
        self.new_meta
            .lock()
            .unwrap()
            .insert(name.into(), value.into());
    }

    /// what this op has staged so far, in stored form. the asset wrapper
    /// reads it without taking it: the same map goes on the materialization
    /// and on the op run.
    pub(crate) fn staged_meta(&self) -> Option<Value> {
        staged_meta(&self.new_meta)
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
            scheduled_for: None,
            partition: None,
            inputs: Arc::new(HashMap::new()),
            dep_statuses: Arc::new(HashMap::new()),
            resources: resource::none(),
            state: Arc::new(None),
            new_state: Arc::new(Mutex::new(None)),
            new_fingerprint: Arc::new(Mutex::new(None)),
            new_meta: Arc::new(Mutex::new(BTreeMap::new())),
            new_per_asset: Arc::new(Mutex::new(BTreeMap::new())),
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

    // exactly what phase 12 wrote, and what is on disk in every database made
    // since. the enum has grown four times over since; these six rows have to
    // read back as the same six values or history stops being readable
    const PHASE12_ROW: &str = r##"{
        "rows": {"int": 1234},
        "ratio": {"float": 0.5},
        "note": {"text": "backfilled from the archive"},
        "source": {"url": "https://example.test/orders"},
        "report": {"markdown": "# heading\n\nbody"},
        "shape": {"json": {"cols": 3}}
    }"##;

    #[test]
    fn phase12_metadata_still_reads_after_the_enum_grew() {
        let stored: Value = serde_json::from_str(PHASE12_ROW).unwrap();
        let read: BTreeMap<String, Meta> = stored
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), Meta::from_tagged(v).expect(k)))
            .collect();

        assert_eq!(read["rows"], Meta::Int(1234));
        assert_eq!(read["ratio"], Meta::Float(0.5));
        assert_eq!(
            read["note"],
            Meta::Text("backfilled from the archive".into())
        );
        assert_eq!(
            read["source"],
            Meta::Url("https://example.test/orders".into())
        );
        assert_eq!(read["report"], Meta::Markdown("# heading\n\nbody".into()));
        assert_eq!(read["shape"], Meta::Json(json!({"cols": 3})));

        // and writing them again produces byte-for-byte the same row: the tags
        // this phase added are new tags, not renamed ones
        assert_eq!(tagged_map(&read), Some(stored));
        // the numbers among them are still numbers, which is what deltas read
        assert_eq!(read["rows"].as_f64(), Some(1234.0));
        assert_eq!(read["ratio"].as_f64(), Some(0.5));
        assert_eq!(read["note"].as_f64(), None);
    }

    #[test]
    fn every_variant_round_trips_through_its_tag() {
        let all = [
            Meta::Int(-3),
            Meta::Float(1.5),
            Meta::Text("t".into()),
            Meta::Url("https://example.test".into()),
            Meta::Markdown("# h".into()),
            Meta::Json(json!([1, 2])),
            Meta::table(["a", "b"], [vec![json!(1), json!("x")]]),
            Meta::table([("a", "int")], [vec![json!(1)]]),
            Meta::bytes(1_288_490_188),
            Meta::duration(Duration::from_millis(3_400)),
            Meta::count(1_240),
            Meta::path("/tmp/orders.parquet".to_string()),
            Meta::run_ref("019fe109"),
            Meta::asset_ref("orders"),
        ];
        for meta in all {
            assert_eq!(
                Meta::from_tagged(&meta.tagged()),
                Some(meta.clone()),
                "{meta:?} did not survive its own tag"
            );
        }

        // the tags themselves, since they are the wire format
        assert_eq!(Meta::count(7).tagged(), json!({"count": 7}));
        assert_eq!(Meta::bytes(7).tagged(), json!({"bytes": 7}));
        assert_eq!(
            Meta::duration(Duration::from_millis(1500)).tagged(),
            json!({"duration_secs": 1.5})
        );
        assert_eq!(Meta::run_ref("r1").tagged(), json!({"run": "r1"}));
        assert_eq!(Meta::asset_ref("a").tagged(), json!({"asset": "a"}));
    }

    // a value from a newer hestan, and a few shapes that are not values at all
    #[test]
    fn an_unknown_tag_reads_as_nothing_rather_than_a_guess() {
        for v in [
            json!({"histogram": [1, 2, 3]}),
            json!({"int": 1, "count": 2}),
            json!({}),
            json!({"int": "twelve"}),
            json!(12),
            json!(null),
        ] {
            assert_eq!(Meta::from_tagged(&v), None, "{v} read as something");
        }
    }

    #[test]
    fn a_table_is_capped_and_rectangular_at_construction() {
        let rows = (0..META_TABLE_ROWS as i64 + 40).map(|i| vec![json!(i), json!("x")]);
        let Meta::Table(t) = Meta::table(["n", "s"], rows) else {
            unreachable!()
        };
        assert_eq!(t.rows().len(), META_TABLE_ROWS);
        assert!(t.truncated(), "a table that lost rows says so");
        assert_eq!(t.rows()[0], [json!(0), json!("x")]);

        // exactly the cap is not truncation: nothing was dropped
        let rows = (0..META_TABLE_ROWS as i64).map(|i| vec![json!(i)]);
        let Meta::Table(t) = Meta::table(["n"], rows) else {
            unreachable!()
        };
        assert_eq!(t.rows().len(), META_TABLE_ROWS);
        assert!(!t.truncated());

        // ragged rows are padded and trimmed to the columns, so nothing
        // downstream has to decide what a short row means
        let Meta::Table(t) = Meta::table(
            ["a", "b"],
            [vec![json!(1)], vec![json!(1), json!(2), json!(3)]],
        ) else {
            unreachable!()
        };
        assert_eq!(t.rows()[0], [json!(1), Value::Null]);
        assert_eq!(t.rows()[1], [json!(1), json!(2)]);
        assert_eq!(t.columns()[1].name(), "b");
        assert_eq!(t.columns()[1].ty(), None);
        assert_eq!(MetaColumn::from(("orders", "int")).ty(), Some("int"));
    }

    // the units are display types over one number, so a delta can be computed
    // between them; everything else has no number to report
    #[test]
    fn only_the_numeric_variants_carry_a_number() {
        assert_eq!(Meta::count(1_240).as_f64(), Some(1_240.0));
        assert_eq!(Meta::bytes(1_024).as_f64(), Some(1_024.0));
        assert_eq!(
            Meta::duration(Duration::from_millis(3_400)).as_f64(),
            Some(3.4)
        );
        assert_eq!(Meta::Int(-3).as_f64(), Some(-3.0));
        assert_eq!(Meta::Float(f64::NAN).as_f64(), None);
        for meta in [
            Meta::Text("12".into()),
            Meta::Url("https://example.test".into()),
            Meta::Markdown("12".into()),
            Meta::Json(json!(12)),
            Meta::path("/tmp/12"),
            Meta::run_ref("12"),
            Meta::asset_ref("12"),
            Meta::table(["n"], [vec![json!(12)]]),
        ] {
            assert_eq!(meta.as_f64(), None, "{meta:?} reported a number");
        }
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
