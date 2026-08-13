use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::error::Error;
use crate::graph;
use crate::job::Job;
use crate::model::{CheckStatus, Materialization, RunTags, Severity, Trigger};
use crate::op::{self, Meta, Op, OpCtx};
use crate::partition::Partitions;
use crate::store::{Built, Store};

/// the internal job every asset build runs under.
pub(crate) const ASSETS_JOB: &str = "assets";

/// the external name a partitioned asset fans out over: the keys one build
/// targets, which only a [`BuildPlan`] can work out.
pub(crate) fn partition_keys_name(asset: &str) -> String {
    format!("partitions:{asset}")
}

pub(crate) type ProbeFn = dyn Fn() -> BoxFuture<'static, Result<String, Box<dyn std::error::Error + Send + Sync>>>
    + Send
    + Sync;

/// an op with identity: a persisted latest value, a fingerprint, and explicit
/// lineage. derived assets ([`Asset::new`] / [`Asset::typed`]) have a body
/// and deps; source assets ([`Asset::source`]) stand for external data and
/// carry only a cheap [`probe`](Asset::probe) that fingerprints it. register
/// with `Hestan::assets`.
///
/// ```no_run
/// # use hestan::{Asset, Hestan, OpCtx};
/// # use serde_json::json;
/// # async fn f() -> Result<(), hestan::Error> {
/// let orders = Asset::source("orders")
///     .probe(|| async { Ok("2026-08-11T03:00:00Z".to_string()) });
///
/// let daily = Asset::new("daily_revenue", |ctx: OpCtx| async move {
///     let raw = ctx.input("orders").cloned().unwrap_or(json!(null));
///     Ok(json!({ "total": raw["total"].as_f64().unwrap_or(0.0) }))
/// })
/// .from(&orders)
/// .auto();
///
/// Hestan::new().assets([orders, daily]).serve(([127, 0, 0, 1], 4000)).await
/// # }
/// ```
///
/// nothing above says when anything runs. that is the difference from a
/// [`Job`](crate::Job): an asset declares what it is made of, and
/// [`auto`](Asset::auto) lets hestan rebuild it when what it is made of
/// changes. `docs/choosing.md` is about which of the two a given piece of
/// work wants.
pub struct Asset {
    name: String,
    source: bool,
    deps: Vec<String>,
    op: Option<Op>,
    probe: Option<Arc<ProbeFn>>,
    probe_every: Duration,
    auto: bool,
    retries: u32,
    retry_delay: Option<Duration>,
    partitions: Option<Partitions>,
    fresh_within: Option<Duration>,
}

impl Asset {
    /// a source asset: external data with no fn body. give it a
    /// [`probe`](Asset::probe) so changes are noticed; without one it never
    /// fingerprints and derived assets depending on it stay stale.
    pub fn source(name: impl Into<String>) -> Asset {
        Asset {
            name: name.into(),
            source: true,
            deps: Vec::new(),
            op: None,
            probe: None,
            probe_every: Duration::from_secs(60),
            auto: false,
            retries: 0,
            retry_delay: None,
            partitions: None,
            fresh_within: None,
        }
    }

    /// a derived asset; `f` has the same bounds as [`Op::new`] and gets an
    /// [`OpCtx`]: dep values via `ctx.input("<dep asset name>")`, plus
    /// [`OpCtx::set_fingerprint`] to override the default content hash.
    pub fn new<F, Fut>(name: impl Into<String>, f: F) -> Asset
    where
        F: Fn(OpCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = crate::op::OpResult> + Send + 'static,
    {
        let name = name.into();
        Asset {
            op: Some(Op::new(name.clone(), f)),
            name,
            source: false,
            deps: Vec::new(),
            probe: None,
            probe_every: Duration::from_secs(60),
            auto: false,
            retries: 0,
            retry_delay: None,
            partitions: None,
            fresh_within: None,
        }
    }

    /// a derived asset with typed io — the same machinery as [`Op::typed`]:
    /// dep values are deserialized into `I` (one field per dep, named after
    /// it) and the return value is serialized back to json.
    pub fn typed<I, O, F, Fut>(name: impl Into<String>, f: F) -> Asset
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + 'static,
        F: Fn(OpCtx, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let name = name.into();
        Asset {
            op: Some(Op::typed(name.clone(), f)),
            name,
            source: false,
            deps: Vec::new(),
            probe: None,
            probe_every: Duration::from_secs(60),
            auto: false,
            retries: 0,
            retry_delay: None,
            partitions: None,
            fresh_within: None,
        }
    }

    /// attach the fingerprint probe (sources only): a cheap async fn returning a
    /// fingerprint for the external data, evaluated every
    /// [`probe_every`](Asset::probe_every). a change records a new materialization.
    pub fn probe<F, Fut>(mut self, f: F) -> Asset
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        self.probe = Some(Arc::new(move || Box::pin(f())));
        self
    }

    /// how often the probe runs (default 60s).
    pub fn probe_every(mut self, d: Duration) -> Asset {
        self.probe_every = d;
        self
    }

    /// declare lineage on another asset; repeatable.
    pub fn from(mut self, dep: &Asset) -> Asset {
        self.deps.push(dep.name.clone());
        self
    }

    /// declare lineage on an asset by name — the way to depend on one output
    /// of a [`MultiAsset`], which produces names rather than [`Asset`] values.
    /// a name nothing registers is a build error, exactly as with
    /// [`from`](Self::from).
    pub fn from_named(mut self, dep: impl Into<String>) -> Asset {
        self.deps.push(dep.into());
        self
    }

    /// the name this asset is registered and materialized under.
    ///
    /// worth having for assets you did not name yourself: a
    /// [dbt project][dbt] hands back a vec of them, and this is how you
    /// find the one you want to say something more about.
    ///
    #[cfg_attr(feature = "dbt", doc = "[dbt]: crate::dbt")]
    #[cfg_attr(not(feature = "dbt"), doc = "[dbt]: crate")]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// what it is made of, in declaration order — the same names
    /// [`from`](Asset::from) and [`from_named`](Asset::from_named) took.
    pub fn deps(&self) -> &[String] {
        &self.deps
    }

    /// extra attempts for the materializing op (default 0).
    pub fn retries(mut self, n: u32) -> Asset {
        self.retries = n;
        self
    }

    /// pause between attempts (default 1s).
    pub fn retry_delay(mut self, d: Duration) -> Asset {
        self.retry_delay = Some(d);
        self
    }

    /// rebuild this asset automatically when a probe upstream makes it stale.
    /// without a probed source somewhere upstream it just waits forever.
    pub fn auto(mut self) -> Asset {
        self.auto = true;
        self
    }

    /// materialize this asset once per key of `partitions` instead of once:
    /// its own materialization, fingerprint, history and checks per key, and
    /// `ctx.partition()` inside the body telling it which one it is for.
    ///
    /// ```no_run
    /// # use hestan::{Asset, OpCtx, Partitions};
    /// # use serde_json::json;
    /// Asset::new("daily_orders", |ctx: OpCtx| async move {
    ///     let day = ctx.partition().expect("partitioned");
    ///     Ok(json!({ "day": day }))
    /// })
    /// .partitioned(Partitions::daily("2026-01-01"));
    /// ```
    ///
    /// a build expands into one fan-out instance per target key, named
    /// `{asset}[{key}]`. sources cannot be partitioned — a probe fingerprints
    /// the whole thing — and neither can a [`MultiAsset`], which has no
    /// `partitioned` at all.
    pub fn partitioned(mut self, partitions: Partitions) -> Asset {
        self.partitions = Some(partitions);
        self
    }

    /// declare how old this asset's latest materialization may get before the
    /// asset is late: `fresh_within(Duration::from_secs(3600))` says it should
    /// be rebuilt hourly, however that rebuild is triggered. on a [partitioned
    /// asset](Self::partitioned) the policy applies per key, so the asset is
    /// late as soon as any one key is — see
    /// [freshness](../docs/freshness.md). staleness is a different question:
    /// stale means a dep moved, late means time passed.
    pub fn fresh_within(mut self, d: Duration) -> Asset {
        self.fresh_within = Some(d);
        self
    }
}

/// one computation, several assets: a query or a pull whose result splits into
/// tables you do not want to fetch twice.
///
/// ```no_run
/// # use hestan::{Asset, MultiAsset, OpCtx};
/// # use serde_json::json;
/// # let raw = Asset::source("raw_orders");
/// MultiAsset::new("split_orders", |_ctx: OpCtx| async move {
///     Ok(json!({
///         "orders_clean":    {"rows": 2},
///         "orders_rejected": {"rows": 0},
///     }))
/// })
/// .produces(["orders_clean", "orders_rejected"])
/// .from(&raw);
/// ```
///
/// the body returns a json **object** whose keys are exactly the produced
/// names — a missing or extra key fails the op and says which. it lowers to
/// one op of the internal `assets` job, and each produced asset gets its own
/// materialization, its own fingerprint (the hash of that key's value, or
/// [`OpCtx::set_fingerprint_of`]) and its own metadata
/// ([`OpCtx::meta_of`]). downstream assets depend on the produced names with
/// [`Asset::from_named`] and read them with `ctx.input("<name>")`.
///
/// register with `Hestan::multi_assets`.
pub struct MultiAsset {
    name: String,
    produces: Vec<String>,
    deps: Vec<String>,
    op: Op,
    auto: bool,
    retries: u32,
    retry_delay: Option<Duration>,
}

impl MultiAsset {
    /// `name` names the *op*, not an asset: nothing is materialized under it.
    /// `f` has the same bounds as [`Asset::new`].
    pub fn new<F, Fut>(name: impl Into<String>, f: F) -> MultiAsset
    where
        F: Fn(OpCtx) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = crate::op::OpResult> + Send + 'static,
    {
        let name = name.into();
        MultiAsset {
            op: Op::new(name.clone(), f),
            name,
            produces: Vec::new(),
            deps: Vec::new(),
            auto: false,
            retries: 0,
            retry_delay: None,
        }
    }

    /// the assets this op produces, one per key of the object it returns.
    /// repeatable; declaring none is a build error.
    pub fn produces<I>(mut self, names: I) -> MultiAsset
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        self.produces.extend(names.into_iter().map(Into::into));
        self
    }

    /// declare lineage; every produced asset gets it. repeatable.
    pub fn from(mut self, dep: &Asset) -> MultiAsset {
        self.deps.push(dep.name.clone());
        self
    }

    /// [`from`](Self::from) by name, for depending on another multi-asset's
    /// output.
    pub fn from_named(mut self, dep: impl Into<String>) -> MultiAsset {
        self.deps.push(dep.into());
        self
    }

    /// extra attempts for the materializing op (default 0).
    pub fn retries(mut self, n: u32) -> MultiAsset {
        self.retries = n;
        self
    }

    /// pause between attempts (default 1s).
    pub fn retry_delay(mut self, d: Duration) -> MultiAsset {
        self.retry_delay = Some(d);
        self
    }

    /// rebuild automatically when a probe upstream makes any produced asset
    /// stale; the flag lands on all of them, since one op produces them all.
    pub fn auto(mut self) -> MultiAsset {
        self.auto = true;
        self
    }
}

pub(crate) type CheckFn = dyn Fn(OpCtx, Value) -> BoxFuture<'static, CheckOutcome> + Send + Sync;

/// what a check body returns.
pub type CheckOutcome = Result<CheckResult, Box<dyn std::error::Error + Send + Sync>>;

/// what a check said about the value it was handed: passed or failed, with an
/// optional message and any [metadata](Meta) worth recording alongside.
#[derive(Debug, Clone, Default)]
pub struct CheckResult {
    passed: bool,
    message: Option<String>,
    metadata: BTreeMap<String, Meta>,
}

impl CheckResult {
    /// it passed. hang a [`meta`](CheckResult::meta) on it to record what it
    /// saw — a check that reports the number it was satisfied by is worth more
    /// three months later than one that only ever says yes.
    pub fn pass() -> CheckResult {
        CheckResult {
            passed: true,
            ..CheckResult::default()
        }
    }

    /// a failure, and why. the message is recorded either way and is what the
    /// op's error says when the severity is [`Severity::Error`].
    pub fn fail(message: impl Into<String>) -> CheckResult {
        CheckResult {
            passed: false,
            message: Some(message.into()),
            metadata: BTreeMap::new(),
        }
    }

    /// attach a typed fact to the result — the number that failed the
    /// threshold, usually. same values as [`OpCtx::meta`](crate::OpCtx::meta),
    /// recorded on the check row rather than the op run.
    pub fn meta(mut self, name: impl Into<String>, value: impl Into<Meta>) -> CheckResult {
        self.metadata.insert(name.into(), value.into());
        self
    }
}

/// an assertion bound to an asset, run right after it materializes and handed
/// the value it just produced.
///
/// ```no_run
/// # use hestan::{AssetCheck, CheckResult, OpCtx, Severity};
/// # use serde_json::Value;
/// AssetCheck::new("rows_present", "orders_clean", |_ctx: OpCtx, value: Value| async move {
///     let n = value.get("rows").and_then(Value::as_u64).unwrap_or(0);
///     if n > 0 {
///         Ok(CheckResult::pass().meta("rows", n as i64))
///     } else {
///         Ok(CheckResult::fail("no rows"))
///     }
/// })
/// .severity(Severity::Error);
/// ```
///
/// register with `Hestan::check`. checks lower into ops of the internal
/// `assets` job named `check:{asset}:{check}`, depending on the asset's own
/// op, so retries, the gantt, cancellation and the rest of the run machinery
/// apply to them unchanged.
pub struct AssetCheck {
    name: String,
    asset: String,
    severity: Severity,
    f: Arc<CheckFn>,
}

impl AssetCheck {
    /// `f` is handed the value the asset just materialized. naming an asset
    /// that is not registered, or a source, is a build error, as is two
    /// checks with the same name on one asset.
    pub fn new<F, Fut>(name: impl Into<String>, asset: impl Into<String>, f: F) -> AssetCheck
    where
        F: Fn(OpCtx, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = CheckOutcome> + Send + 'static,
    {
        AssetCheck {
            name: name.into(),
            asset: asset.into(),
            severity: Severity::default(),
            f: Arc::new(move |ctx, value| Box::pin(f(ctx, value))),
        }
    }

    /// what a failure costs (default [`Severity::Error`]).
    pub fn severity(mut self, severity: Severity) -> AssetCheck {
        self.severity = severity;
        self
    }

    /// what it was declared as. checks are identified by `(asset, name)`, so
    /// renaming one starts its history over.
    pub fn name(&self) -> &str {
        &self.name
    }
}

pub(crate) struct CheckMeta {
    pub name: String,
    pub asset: String,
    pub severity: Severity,
    f: Arc<CheckFn>,
}

/// the op name a check lowers to.
pub(crate) fn check_op_name(asset: &str, check: &str) -> String {
    format!("check:{asset}:{check}")
}

pub(crate) struct AssetMeta {
    pub name: String,
    pub source: bool,
    pub deps: Vec<String>,
    pub auto: bool,
    pub probe: Option<Arc<ProbeFn>>,
    pub probe_every: Duration,
    /// the op that materializes this asset: its own name when it has an op to
    /// itself, the [`MultiAsset`]'s name when several assets share one, and
    /// `None` for a source, which has no op at all.
    pub op: Option<String>,
    /// the key set this asset is materialized over, one materialization per
    /// key; `None` for an unpartitioned asset, which is one of everything.
    pub partitions: Option<Partitions>,
    /// how old the latest materialization may get before this asset is late,
    /// from [`Asset::fresh_within`]; `None` when nothing was declared.
    pub fresh_within: Option<Duration>,
}

/// one op of the lowered `assets` job and the assets it produces — one for a
/// plain [`Asset`], several for a [`MultiAsset`]. the registry is asset -> op
/// N:1, and this is the op side of it.
pub(crate) struct OpMeta {
    pub name: String,
    pub produces: Vec<String>,
    /// the assets this op reads, shared by everything it produces.
    pub deps: Vec<String>,
    op: Op,
    retries: u32,
    retry_delay: Option<Duration>,
    /// set only on a single-asset op: a multi-asset is never partitioned.
    pub partitions: Option<Partitions>,
}

/// how one dep asset reaches the op that reads it: the op that produces it
/// (which is the name the run knows it by), the key to take out of that op's
/// output when the producer is a multi-asset, and whether the dep is itself
/// partitioned — in which case its value is read per key from the store
/// rather than out of the run.
#[derive(Clone)]
struct DepLink {
    asset: String,
    op: String,
    key: Option<String>,
    partitioned: bool,
}

/// the validated asset graph, in topo order, with the checks bound to it.
/// built once by `Hestan::build`.
pub(crate) struct AssetRegistry {
    metas: Vec<AssetMeta>,
    by_name: HashMap<String, usize>,
    ops: Vec<OpMeta>,
    by_op: HashMap<String, usize>,
    checks: Vec<CheckMeta>,
}

impl AssetRegistry {
    pub(crate) fn empty() -> AssetRegistry {
        AssetRegistry {
            metas: Vec::new(),
            by_name: HashMap::new(),
            ops: Vec::new(),
            by_op: HashMap::new(),
            checks: Vec::new(),
        }
    }

    pub(crate) fn new(
        assets: Vec<Asset>,
        multis: Vec<MultiAsset>,
        checks: Vec<AssetCheck>,
    ) -> Result<AssetRegistry, Error> {
        let mut metas: Vec<AssetMeta> = Vec::with_capacity(assets.len());
        let mut ops: Vec<OpMeta> = Vec::new();
        for a in assets {
            if a.source && a.auto {
                return Err(Error::Graph(format!(
                    "asset {}: auto on a source (sources are probed, never built)",
                    a.name
                )));
            }
            if !a.source && a.probe.is_some() {
                return Err(Error::Graph(format!(
                    "asset {}: probe on a derived asset (probes belong to sources)",
                    a.name
                )));
            }
            if a.source && !a.deps.is_empty() {
                return Err(Error::Graph(format!(
                    "asset {}: a source cannot depend on other assets",
                    a.name
                )));
            }
            if let Some(spec) = &a.partitions {
                if a.source {
                    return Err(Error::Graph(format!(
                        "asset {}: a source cannot be partitioned (a probe fingerprints \
                         the whole of it)",
                        a.name
                    )));
                }
                spec.validate(&a.name)?;
            }
            if let Some(op) = a.op {
                ops.push(OpMeta {
                    name: a.name.clone(),
                    produces: vec![a.name.clone()],
                    deps: a.deps.clone(),
                    op,
                    retries: a.retries,
                    retry_delay: a.retry_delay,
                    partitions: a.partitions.clone(),
                });
            }
            metas.push(AssetMeta {
                name: a.name.clone(),
                source: a.source,
                deps: a.deps,
                auto: a.auto,
                probe: a.probe,
                probe_every: a.probe_every,
                op: (!a.source).then_some(a.name),
                partitions: a.partitions,
                fresh_within: a.fresh_within,
            });
        }
        for m in multis {
            if m.produces.is_empty() {
                return Err(Error::Graph(format!(
                    "multi-asset {}: produces nothing; name its outputs with .produces([..])",
                    m.name
                )));
            }
            for produced in &m.produces {
                metas.push(AssetMeta {
                    name: produced.clone(),
                    source: false,
                    deps: m.deps.clone(),
                    auto: m.auto,
                    probe: None,
                    probe_every: Duration::from_secs(60),
                    op: Some(m.name.clone()),
                    partitions: None,
                    // a multi-asset produces names, not `Asset` values, so
                    // there is nowhere to hang a policy on one of them
                    fresh_within: None,
                });
            }
            ops.push(OpMeta {
                name: m.name,
                produces: m.produces,
                deps: m.deps,
                op: m.op,
                retries: m.retries,
                retry_delay: m.retry_delay,
                partitions: None,
            });
        }
        let pairs: Vec<(String, Vec<String>)> = metas
            .iter()
            .map(|m| (m.name.clone(), m.deps.clone()))
            .collect();
        // first, so a duplicate asset name reads as one rather than as
        // whatever the op-level checks below would make of it
        let order = graph::topo_order(&pairs).map_err(|e| Error::Graph(format!("assets: {e}")))?;
        check_partition_deps(&metas)?;
        let mut seen_ops: HashSet<&str> = HashSet::new();
        for o in &ops {
            // ops and assets share one namespace inside the job, so a
            // multi-asset named after an asset it does not produce collides
            // there — said before the duplicate check below, which would
            // otherwise report the collision as two multi-assets
            if !o.produces.contains(&o.name) && order.contains(&o.name) {
                return Err(Error::Graph(format!(
                    "multi-asset {}: an asset is already called that; \
                     a multi-asset names the op, not one of its outputs",
                    o.name
                )));
            }
            if !seen_ops.insert(&o.name) {
                return Err(Error::Graph(format!("duplicate multi-asset {}", o.name)));
            }
        }
        let mut by_declared: HashMap<String, AssetMeta> =
            metas.into_iter().map(|m| (m.name.clone(), m)).collect();
        let ordered: Vec<AssetMeta> = order
            .iter()
            .map(|name| {
                by_declared
                    .remove(name)
                    .expect("order names each asset once")
            })
            .collect();
        let by_name: HashMap<String, usize> = ordered
            .iter()
            .enumerate()
            .map(|(i, m)| (m.name.clone(), i))
            .collect();
        // ops in the topo order of the first asset each produces, so lowering
        // and planning list them the same way every time
        ops.sort_by_key(|o| {
            o.produces
                .iter()
                .filter_map(|a| by_name.get(a))
                .min()
                .copied()
                .unwrap_or(usize::MAX)
        });
        let by_op: HashMap<String, usize> = ops
            .iter()
            .enumerate()
            .map(|(i, o)| (o.name.clone(), i))
            .collect();

        // checks in asset topo order then declaration order, so the ops a
        // build lowers to are the same every time
        let mut checked: Vec<CheckMeta> = Vec::with_capacity(checks.len());
        for c in checks {
            let Some(&index) = by_name.get(&c.asset) else {
                return Err(Error::Graph(format!(
                    "check {}: no asset named {}",
                    c.name, c.asset
                )));
            };
            if ordered[index].source {
                return Err(Error::Graph(format!(
                    "check {}: {} is a source, and a check runs on what a build produced",
                    c.name, c.asset
                )));
            }
            if checked
                .iter()
                .any(|d| d.asset == c.asset && d.name == c.name)
            {
                return Err(Error::Graph(format!(
                    "duplicate check {} on asset {}",
                    c.name, c.asset
                )));
            }
            checked.push(CheckMeta {
                name: c.name,
                asset: c.asset,
                severity: c.severity,
                f: c.f,
            });
        }
        checked.sort_by_key(|c| by_name[&c.asset]);

        Ok(AssetRegistry {
            metas: ordered,
            by_name,
            ops,
            by_op,
            checks: checked,
        })
    }

    /// every check bound to `asset`, in lowering order.
    pub(crate) fn checks_on(&self, asset: &str) -> impl Iterator<Item = &CheckMeta> {
        self.checks.iter().filter(move |c| c.asset == asset)
    }

    pub(crate) fn topo(&self) -> impl Iterator<Item = &AssetMeta> {
        self.metas.iter()
    }

    pub(crate) fn get(&self, name: &str) -> Option<&AssetMeta> {
        self.by_name.get(name).map(|&i| &self.metas[i])
    }

    /// every lowered op, in the topo order of the assets it produces.
    pub(crate) fn ops(&self) -> impl Iterator<Item = &OpMeta> {
        self.ops.iter()
    }

    pub(crate) fn op(&self, name: &str) -> Option<&OpMeta> {
        self.by_op.get(name).map(|&i| &self.ops[i])
    }

    /// what the run knows `asset` by: the op that produces it, or — for a
    /// source, which has no op — the asset's own name, seeded as an external.
    fn producer(&self, asset: &str) -> String {
        match self.get(asset).and_then(|m| m.op.clone()) {
            Some(op) => op,
            None => asset.to_string(),
        }
    }

    /// how an op reads one of its dep assets.
    fn dep_link(&self, asset: &str) -> DepLink {
        let op = self.producer(asset);
        // a multi-asset's output is one object keyed by what it produces, so a
        // dep on one of them is a key of it; everything else is the whole value
        let key = self
            .op(&op)
            .filter(|o| o.produces.len() > 1)
            .map(|_| asset.to_string());
        DepLink {
            asset: asset.to_string(),
            op,
            key,
            partitioned: self.get(asset).is_some_and(|m| m.partitions.is_some()),
        }
    }

    /// transitive dependents of `name`, excluding itself.
    pub(crate) fn downstream(&self, name: &str) -> HashSet<String> {
        let pairs: Vec<(String, Vec<String>)> = self
            .metas
            .iter()
            .map(|m| (m.name.clone(), m.deps.clone()))
            .collect();
        graph::downstream(&pairs, name)
    }

    /// lower into the internal "assets" job: one wrapped op per materializing
    /// op — which is one asset, or all of a multi-asset's — one more per check
    /// hanging off the asset it checks, and sources as external deps that a
    /// full launch seeds null.
    pub(crate) fn lower_job(&self) -> Result<Job, Error> {
        let ops: Vec<Op> = self
            .ops
            .iter()
            .map(|m| wrap_op(self, m))
            .chain(self.checks.iter().map(|c| check_op(self, c)))
            .collect();
        // sources seed null: their value is lineage, not data. a partitioned
        // asset's key list seeds `[]`, so a full launch of the job — which
        // computes no plan and so no targets — expands it into nothing rather
        // than guessing at a range
        let external: Vec<(String, Value)> = self
            .metas
            .iter()
            .filter(|m| m.source)
            .map(|m| (m.name.clone(), Value::Null))
            .chain(
                self.metas
                    .iter()
                    .filter(|m| m.partitions.is_some())
                    .map(|m| (partition_keys_name(&m.name), json!([]))),
            )
            .collect();
        Job::assemble(
            ASSETS_JOB,
            Some("internal: asset materializations".into()),
            ops,
            external,
        )
    }
}

/// what lineage across a partition boundary is allowed to look like.
///
/// dependencies between partitioned assets are **identity mapping only**: a
/// partition takes the same key from every partitioned dep it reads. that
/// rules out two shapes, and both are refused here rather than left to
/// produce something plausible and wrong.
fn check_partition_deps(metas: &[AssetMeta]) -> Result<(), Error> {
    let spec = |name: &str| {
        metas
            .iter()
            .find(|m| m.name == name)
            .and_then(|m| m.partitions.as_ref())
    };
    for meta in metas {
        for dep in &meta.deps {
            let Some(dep_spec) = spec(dep) else { continue };
            let Some(own) = &meta.partitions else {
                return Err(Error::Graph(format!(
                    "asset {}: it is not partitioned but its dep {dep} is. reading every \
                     partition of {dep} at once is an aggregation, and hestan has no \
                     semantics for one yet — partition {} too, or aggregate inside the \
                     body from a source",
                    meta.name, meta.name
                )));
            };
            if !own.same_kind(dep_spec) {
                return Err(Error::Graph(format!(
                    "asset {}: partitioned {}, but its dep {dep} is partitioned {}. \
                     a partition reads the same key from its dep, which two kinds of key \
                     set cannot agree on",
                    meta.name,
                    own.kind_label(),
                    dep_spec.kind_label()
                )));
            }
        }
    }
    Ok(())
}

/// the value one dep asset has, out of what the run handed the op that reads
/// it: a multi-asset's output is an object keyed by what it produces, so the
/// dep is one key of it, and everything else is the whole value.
fn dep_value(ctx: &OpCtx, link: &DepLink) -> Option<Value> {
    let held = ctx.input(&link.op)?;
    Some(match &link.key {
        Some(key) => held.get(key).cloned().unwrap_or(Value::Null),
        None => held.clone(),
    })
}

/// the ctx an asset body sees: inputs keyed by the *asset* names it declared,
/// whatever ops the run actually ran to produce them, plus the partition key
/// this invocation is for. `ctx.input("orders")` reads the same inside an
/// asset whether `orders` has an op to itself or is one output of a
/// multi-asset.
///
/// a **partitioned** dep is read from the store at the same key rather than
/// out of the run. that is what makes identity mapping mean one thing: the
/// consumer reads `dep[k]` whether `dep[k]` was rebuilt by this run — its
/// materialization is written inside its own op, which has finished by now —
/// or was already fresh and never ran at all.
fn with_dep_inputs(ctx: &OpCtx, links: &[DepLink], key: Option<&str>) -> Result<OpCtx, Error> {
    let mut inputs: HashMap<String, Value> = HashMap::new();
    let mut dep_statuses: HashMap<String, crate::model::OpStatus> = HashMap::new();
    for link in links {
        let value = match (link.partitioned, key) {
            (true, Some(key)) => ctx
                .store
                .materialization(&link.asset, Some(key))?
                .and_then(|m| m.value),
            _ => dep_value(ctx, link),
        };
        if let Some(v) = value {
            inputs.insert(link.asset.clone(), v);
        }
        if let Some(s) = ctx.dep_status(&link.op) {
            dep_statuses.insert(link.asset.clone(), s);
        }
    }
    Ok(OpCtx {
        inputs: Arc::new(inputs),
        dep_statuses: Arc::new(dep_statuses),
        partition: key.map(str::to_string),
        ..ctx.clone()
    })
}

/// the dep fingerprints one materialization records: the key it consumed for a
/// partitioned dep, the whole asset otherwise.
fn dep_fingerprints(ctx: &OpCtx, links: &[DepLink], key: Option<&str>) -> Result<Value, Error> {
    let mut inputs = Map::new();
    for link in links {
        let at = key.filter(|_| link.partitioned);
        let fp = ctx
            .store
            .materialization(&link.asset, at)?
            .map(|m| m.fingerprint);
        inputs.insert(
            link.asset.clone(),
            fp.map(Value::String).unwrap_or(Value::Null),
        );
    }
    Ok(Value::Object(inputs))
}

/// what each produced asset's value is, out of the op's output. one asset is
/// the whole output; a multi-asset splits by key, and a key it did not return
/// — or one nothing declared — fails the op naming the discrepancy, because
/// the alternative is a materialization of `null` nobody asked for.
fn split_output<'a>(
    op: &str,
    produces: &'a [String],
    output: &'a Value,
) -> Result<Vec<(&'a str, &'a Value)>, String> {
    if produces.len() == 1 {
        return Ok(vec![(produces[0].as_str(), output)]);
    }
    let Some(map) = output.as_object() else {
        return Err(format!(
            "multi-asset {op}: returned {}, not an object keyed by the assets it produces",
            json_type(output)
        ));
    };
    let missing: Vec<&str> = produces
        .iter()
        .map(String::as_str)
        .filter(|a| !map.contains_key(*a))
        .collect();
    let extra: Vec<&str> = map
        .keys()
        .map(String::as_str)
        .filter(|k| !produces.iter().any(|a| a == k))
        .collect();
    if !missing.is_empty() || !extra.is_empty() {
        let mut parts = Vec::new();
        if !missing.is_empty() {
            parts.push(format!("no key for {}", missing.join(", ")));
        }
        if !extra.is_empty() {
            parts.push(format!(
                "a key for {}, which it does not produce",
                extra.join(", ")
            ));
        }
        return Err(format!(
            "multi-asset {op}: its output has {}",
            parts.join("; ")
        ));
    }
    Ok(produces
        .iter()
        .map(|a| (a.as_str(), &map[a.as_str()]))
        .collect())
}

fn json_type(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

// the body computes what only the body can know and stages it; the executor
// writes it in the transaction that records the op succeeding. so a build
// that fails, is cancelled, times out, panics or cannot be persisted leaves
// no materialization, and the next run rebuilds it — at-least-once, like op
// state
fn wrap_op(reg: &AssetRegistry, meta: &OpMeta) -> Op {
    let inner = meta.op.clone();
    let name = meta.name.clone();
    let produces = meta.produces.clone();
    let links: Vec<DepLink> = meta.deps.iter().map(|d| reg.dep_link(d)).collect();
    // one entry per op the run knows, however many asset deps reach it
    let mut after: Vec<String> = Vec::new();
    for link in &links {
        if !after.contains(&link.op) {
            after.push(link.op.clone());
        }
    }
    let partitioned = meta.partitions.is_some();
    let mut op = Op::new(name.clone(), move |ctx: OpCtx| {
        let inner = inner.clone();
        let name = name.clone();
        let produces = produces.clone();
        let links = links.clone();
        async move {
            // on a partitioned asset this op is one fan-out instance, and the
            // element it was handed is the key it is for
            let key = match partitioned {
                false => None,
                true => Some(partition_of(&ctx)?),
            };
            let inner_ctx = with_dep_inputs(&ctx, &links, key.as_deref())?;
            let output = inner.call(inner_ctx).await?;
            let values = split_output(&name, &produces, &output)?;
            // deps' current fingerprints: ancestors in this run already wrote
            // theirs. one entry per dep asset, not per op, so lineage reads in
            // the names the asset graph uses
            let inputs = dep_fingerprints(&ctx, &links, key.as_deref())?;
            // the op-wide override, which covers every output that did not
            // stage one of its own
            let shared = ctx.take_fingerprint();
            for (asset, value) in values {
                let (fingerprint, meta) = ctx.staged_for(asset);
                let fingerprint = fingerprint
                    .or_else(|| shared.clone())
                    .unwrap_or_else(|| content_fingerprint(value));
                // read, not taken: the same map goes on this materialization
                // and on the op run the executor writes when this op reports
                // success
                let meta = match (meta, produces.len()) {
                    (Some(m), _) => Some(m),
                    (None, 1) => ctx.staged_meta(),
                    (None, _) => None,
                };
                ctx.stage_build(Built {
                    asset: asset.to_string(),
                    partition: key.clone(),
                    fingerprint,
                    inputs: inputs.clone(),
                    value: Some(value.clone()),
                    meta,
                });
            }
            Ok(output)
        }
    })
    .after(after)
    .retries(meta.retries);
    if let Some(spec) = &meta.partitions {
        let _ = spec;
        // the same expansion a mapped op gets, over the keys the plan chose
        op = op.fans_out_over(partition_keys_name(&meta.name));
    }
    if let Some(d) = meta.retry_delay {
        op = op.retry_delay(d);
    }
    op.with_types_of(&meta.op)
}

/// the key a partitioned asset's instance is for. the executor hands every
/// instance its element, and for a partitioned asset that element is the key.
fn partition_of(ctx: &OpCtx) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    ctx.element()
        .and_then(|v| v.as_str().map(str::to_string))
        .ok_or_else(|| "a partitioned asset ran without a partition key".into())
}

// a check is an op that depends on the op that materializes the asset it
// checks, so it receives the freshly materialized value as an input and reuses
// the whole run machinery. nothing depends on it in turn, which is what lets an
// error check fail the run without un-materializing anything or cutting off
// downstream assets.
fn check_op(reg: &AssetRegistry, meta: &CheckMeta) -> Op {
    let asset = meta.asset.clone();
    let link = reg.dep_link(&meta.asset);
    let producer = link.op.clone();
    let check = meta.name.clone();
    let severity = meta.severity;
    let f = meta.f.clone();
    // a check on a partitioned asset expands the same way the asset does, over
    // the same keys: one check per partition, on the value that partition
    // just produced
    let partitioned = link.partitioned;
    let op = Op::new(check_op_name(&meta.asset, &meta.name), move |ctx: OpCtx| {
        let (asset, check, f, link) = (asset.clone(), check.clone(), f.clone(), link.clone());
        async move {
            let key = match partitioned {
                false => None,
                true => Some(partition_of(&ctx)?),
            };
            let value = match &key {
                Some(key) => ctx
                    .store
                    .materialization(&asset, Some(key))?
                    .and_then(|m| m.value)
                    .unwrap_or(Value::Null),
                None => dep_value(&ctx, &link).unwrap_or(Value::Null),
            };
            let ctx = match &key {
                None => ctx.clone(),
                Some(key) => OpCtx {
                    partition: Some(key.clone()),
                    ..ctx.clone()
                },
            };
            let result = f(ctx.clone(), value).await?;
            let status = if result.passed {
                CheckStatus::Passed
            } else {
                CheckStatus::Failed
            };
            // recorded before the verdict is acted on, so an error check
            // that fails the run still leaves behind what it found
            ctx.store.record_check(
                &asset,
                key.as_deref(),
                &check,
                ctx.run_id(),
                status,
                severity,
                result.message.as_deref(),
                op::tagged_map(&result.metadata).as_ref(),
            )?;
            if !result.passed && severity == Severity::Error {
                let why = result.message.as_deref().unwrap_or("no message");
                return Err(format!("check {check} failed: {why}").into());
            }
            Ok(json!({
                "check": check,
                "status": status.as_str(),
                "message": result.message,
            }))
        }
    })
    .after([producer]);
    match partitioned {
        false => op,
        true => op.fans_out_over(partition_keys_name(&meta.asset)),
    }
}

/// the default fingerprint: sha256 hex of the output's json text. serde_json
/// sorts map keys (preserve_order is off here), so a value always hashes the same.
pub(crate) fn content_fingerprint(v: &Value) -> String {
    let digest = Sha256::digest(v.to_string().as_bytes());
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// every asset's current materializations: one for an unpartitioned asset, one
/// per key for a partitioned one. this is what staleness, seeding and the
/// assets api all read.
#[derive(Debug, Default)]
pub(crate) struct Mats {
    whole: HashMap<String, Materialization>,
    parts: HashMap<String, BTreeMap<String, Materialization>>,
}

impl Mats {
    /// the current materialization of one `(asset, partition)` pair.
    pub(crate) fn get(&self, asset: &str, partition: Option<&str>) -> Option<&Materialization> {
        match partition {
            None => self.whole.get(asset),
            Some(key) => self.parts.get(asset)?.get(key),
        }
    }

    fn insert(&mut self, m: Materialization) {
        match m.partition.clone() {
            None => {
                self.whole.insert(m.asset.clone(), m);
            }
            Some(key) => {
                self.parts
                    .entry(m.asset.clone())
                    .or_default()
                    .insert(key, m);
            }
        }
    }
}

impl FromIterator<Materialization> for Mats {
    fn from_iter<I: IntoIterator<Item = Materialization>>(iter: I) -> Mats {
        let mut mats = Mats::default();
        for m in iter {
            mats.insert(m);
        }
        mats
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Staleness {
    pub stale: bool,
    pub reasons: Vec<StaleReason>,
    /// one verdict per key of a [partitioned asset](crate::Partitions), and
    /// empty for an unpartitioned one. the asset as a whole is stale exactly
    /// when one of its keys is, which is why `reasons` is empty here — the
    /// evidence lives per key.
    pub parts: BTreeMap<String, Staleness>,
}

/// why an asset is stale: dep's fingerprint when this asset last consumed it
/// (`had`) vs the dep's current one (`now`). equal fingerprints appear when
/// the dep itself is stale — staleness propagates ahead of rebuilds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleReason {
    pub dep: String,
    pub had: Option<String>,
    pub now: Option<String>,
}

/// staleness for every asset, keyed by name: stale if it never materialized, if
/// a dep's fingerprint moved or went missing, or if a dep is itself stale.
/// computed in topo order, so staleness propagates before anything rebuilds.
///
/// a partitioned asset is judged one key at a time, and is stale as a whole
/// when any of its keys is. a partitioned dep is read at the *same* key —
/// identity mapping — and an unpartitioned one whole, which is why a probe
/// moving a source's fingerprint makes every partition of every descendant
/// stale at once.
pub(crate) fn staleness(reg: &AssetRegistry, mats: &Mats) -> HashMap<String, Staleness> {
    let mut out: HashMap<String, Staleness> = HashMap::new();
    for meta in reg.topo() {
        let s = match &meta.partitions {
            None => one_staleness(reg, mats, meta, &out, None),
            Some(spec) => {
                let parts: BTreeMap<String, Staleness> = spec
                    .keys_now()
                    .into_iter()
                    .map(|key| {
                        let s = one_staleness(reg, mats, meta, &out, Some(&key));
                        (key, s)
                    })
                    .collect();
                Staleness {
                    stale: parts.values().any(|s| s.stale),
                    reasons: Vec::new(),
                    parts,
                }
            }
        };
        out.insert(meta.name.clone(), s);
    }
    out
}

/// one verdict: the whole of an unpartitioned asset, or one key of a
/// partitioned one. `done` holds the verdicts of everything upstream, which
/// topo order guarantees is already there.
fn one_staleness(
    reg: &AssetRegistry,
    mats: &Mats,
    meta: &AssetMeta,
    done: &HashMap<String, Staleness>,
    key: Option<&str>,
) -> Staleness {
    let Some(mat) = mats.get(&meta.name, key) else {
        return Staleness {
            stale: true,
            ..Staleness::default()
        };
    };
    let mut reasons = Vec::new();
    for dep in &meta.deps {
        let dep_partitioned = reg.get(dep).is_some_and(|m| m.partitions.is_some());
        let at = key.filter(|_| dep_partitioned);
        let had = mat
            .inputs
            .get(dep)
            .and_then(Value::as_str)
            .map(String::from);
        let now = mats.get(dep, at).map(|m| m.fingerprint.clone());
        let dep_stale = match (done.get(dep), at) {
            // a key the dep's own set does not hold can never be fresh:
            // identity mapping has nothing to read there
            (Some(s), Some(key)) => s.parts.get(key).is_none_or(|s| s.stale),
            (Some(s), None) => s.stale,
            (None, _) => true,
        };
        if dep_stale || now.is_none() || had != now {
            reasons.push(StaleReason {
                dep: dep.clone(),
                had,
                now,
            });
        }
    }
    Staleness {
        stale: !reasons.is_empty(),
        reasons,
        parts: BTreeMap::new(),
    }
}

/// what one build run executes: materializing ops in topo order with their
/// check ops, plus seeds for everything they read that won't run — stored
/// values for fresh derived deps, null for sources. every name here is an op
/// of the `assets` job, which for a multi-asset is not the name of any asset.
#[derive(Debug)]
pub(crate) struct BuildPlan {
    pub ops: Vec<String>,
    pub seeds: HashMap<String, Value>,
}

/// the planned ops plus the check ops hanging off the assets they produce. a
/// check is in the plan exactly when the asset it checks is, which is the whole
/// of the memoization story for checks: an asset that was seeded rather than
/// rebuilt produced no new value this run, so nothing re-checks it and its last
/// recorded result still describes the value that is still current.
fn with_checks(reg: &AssetRegistry, ops: Vec<String>) -> Vec<String> {
    let mut all = Vec::with_capacity(ops.len());
    for op in ops {
        // each op ahead of the checks that read what it produced, as the run
        // runs them
        let checks: Vec<String> = reg
            .op(&op)
            .into_iter()
            .flat_map(|m| m.produces.iter())
            .flat_map(|asset| reg.checks_on(asset))
            .map(|c| check_op_name(&c.asset, &c.name))
            .collect();
        all.push(op);
        all.extend(checks);
    }
    all
}

/// what a fresh dep the plan does not re-run is seeded with, under the name the
/// run knows it by: a source is null, an asset with an op to itself is its
/// stored value, and a multi-asset is the object its op returns — every asset
/// it produces, so that whichever key a consumer reads is there.
///
/// a partitioned asset is seeded null whatever it holds: its consumers read it
/// per key from the store, and no single value could stand for the set.
fn seed_value(reg: &AssetRegistry, mats: &Mats, op: &str) -> Value {
    let Some(meta) = reg.op(op) else {
        // no op of that name: a source, whose value is null everywhere
        return Value::Null;
    };
    if meta.partitions.is_some() {
        return Value::Null;
    }
    let stored = |asset: &String| {
        mats.get(asset, None)
            .and_then(|m| m.value.clone())
            .unwrap_or(Value::Null)
    };
    match meta.produces.as_slice() {
        [one] => stored(one),
        several => Value::Object(
            several
                .iter()
                .map(|asset| (asset.clone(), stored(asset)))
                .collect(),
        ),
    }
}

fn seeds_for(
    reg: &AssetRegistry,
    mats: &Mats,
    ops: &[String],
    keys: &HashMap<String, Vec<String>>,
) -> HashMap<String, Value> {
    let in_plan: HashSet<&str> = ops.iter().map(String::as_str).collect();
    let mut seeds = HashMap::new();
    for name in ops {
        let meta = reg.op(name).expect("planned op is registered");
        // the keys this op fans out over, which is the whole of how a plan
        // reaches the expansion
        if meta.partitions.is_some() {
            let targets = keys.get(name).cloned().unwrap_or_default();
            seeds.insert(partition_keys_name(name), json!(targets));
        }
        for dep in &meta.deps {
            let producer = reg.producer(dep);
            if in_plan.contains(producer.as_str()) || seeds.contains_key(&producer) {
                continue;
            }
            let value = seed_value(reg, mats, &producer);
            seeds.insert(producer, value);
        }
    }
    seeds
}

/// the keys a build of `asset` targets when the caller names none: the ones
/// that are missing or stale, newest first, capped by the set's
/// [build limit](crate::Partitions::build_limit). the cap is what stops an
/// unbounded daily range starting a thousand instances by accident.
fn default_keys(spec: &Partitions, verdict: &Staleness) -> Vec<String> {
    let mut keys: Vec<String> = verdict
        .parts
        .iter()
        .filter(|(_, s)| s.stale)
        .map(|(key, _)| key.clone())
        .collect();
    keys.reverse(); // parts are oldest first; a build wants the newest
    keys.truncate(spec.limit());
    keys
}

/// which keys each partitioned op in the plan will build.
///
/// walked from the sinks up, because identity mapping runs that way: an
/// upstream partitioned asset has to cover every key its consumers are about
/// to read, and only the keys of *its* that are actually stale are worth
/// rebuilding. a target with no keys named takes its default set; anything
/// upstream takes what its consumers need.
fn key_targets(
    reg: &AssetRegistry,
    stale: &HashMap<String, Staleness>,
    ops: &[String],
    targets: &[String],
    named: &HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    let in_plan: HashSet<&str> = ops.iter().map(String::as_str).collect();
    let mut keys: HashMap<String, Vec<String>> = HashMap::new();
    for meta in reg.ops().collect::<Vec<_>>().into_iter().rev() {
        let Some(spec) = &meta.partitions else {
            continue;
        };
        if !in_plan.contains(meta.name.as_str()) {
            continue;
        }
        let asset = &meta.name; // a partitioned op produces exactly one asset
        let mut want: Vec<String> = match named.get(asset) {
            Some(explicit) => explicit.clone(),
            None if targets.contains(asset) => default_keys(spec, &stale[asset]),
            None => Vec::new(),
        };
        // every key a consumer downstream will read, that this asset owes
        for consumer in reg.ops() {
            if !in_plan.contains(consumer.name.as_str()) || !consumer.deps.contains(asset) {
                continue;
            }
            for key in keys.get(&consumer.name).into_iter().flatten() {
                let owed = stale[asset].parts.get(key).is_none_or(|s| s.stale);
                if owed && !want.contains(key) {
                    want.push(key.clone());
                }
            }
        }
        keys.insert(meta.name.clone(), want);
    }
    keys
}

/// the plan for one target: its stale derived ancestors plus the target itself,
/// always. errors on an unknown or source target.
pub(crate) fn plan_target(
    reg: &AssetRegistry,
    mats: &Mats,
    target: &str,
) -> Result<BuildPlan, Error> {
    plan_targets(reg, mats, &[target.to_string()])
}

/// one plan for several targets: the union of their stale derived ancestors plus
/// every target itself. one plan means one run — overlapping per-target plans
/// would each re-run the shared ancestors. errors on an unknown or source target.
pub(crate) fn plan_targets(
    reg: &AssetRegistry,
    mats: &Mats,
    targets: &[String],
) -> Result<BuildPlan, Error> {
    plan_partitions(reg, mats, targets, &HashMap::new())
}

/// [`plan_targets`] with the partitions of some targets named outright rather
/// than defaulted — what `POST /api/assets/{name}/build` with a `partitions`
/// body and what a [backfill](crate::Hestan) launch build. a key that is not
/// in the asset's set, or named for an asset that is not partitioned, is an
/// error rather than an instance that could never mean anything.
pub(crate) fn plan_partitions(
    reg: &AssetRegistry,
    mats: &Mats,
    targets: &[String],
    named: &HashMap<String, Vec<String>>,
) -> Result<BuildPlan, Error> {
    let mut want: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = Vec::new();
    for target in targets {
        let Some(meta) = reg.get(target) else {
            return Err(Error::UnknownAsset(target.clone()));
        };
        if meta.source {
            return Err(Error::Graph(format!(
                "asset {target} is a source; sources are probed, never built"
            )));
        }
        stack.push(target.clone());
    }
    for (asset, keys) in named {
        let Some(meta) = reg.get(asset) else {
            return Err(Error::UnknownAsset(asset.clone()));
        };
        let Some(spec) = &meta.partitions else {
            return Err(Error::Graph(format!("asset {asset} is not partitioned")));
        };
        if let Some(unknown) = keys.iter().find(|k| !spec.contains(k)) {
            return Err(Error::Graph(format!(
                "asset {asset} has no partition {unknown:?}"
            )));
        }
    }
    while let Some(n) = stack.pop() {
        if want.insert(n.clone()) {
            for d in &reg.get(&n).expect("dep names validated").deps {
                stack.push(d.clone());
            }
        }
    }
    let stale = staleness(reg, mats);
    // an op is in the plan once however many of its assets ask for it: a
    // multi-asset is one computation, and running it twice would be two
    // computations recording each other's lineage
    let ops: Vec<String> = reg
        .ops()
        .filter(|o| {
            o.produces
                .iter()
                .any(|a| want.contains(a) && (targets.contains(a) || stale[a].stale))
        })
        .map(|o| o.name.clone())
        .collect();
    let keys = key_targets(reg, &stale, &ops, targets, named);
    // seeds before checks: a check op produces no asset and seeds nothing
    let seeds = seeds_for(reg, mats, &ops, &keys);
    Ok(BuildPlan {
        ops: with_checks(reg, ops),
        seeds,
    })
}

/// one plan covering every stale derived asset; `None` when nothing is stale.
pub(crate) fn plan_all(reg: &AssetRegistry, mats: &Mats) -> Option<BuildPlan> {
    let stale = staleness(reg, mats);
    let ops: Vec<String> = reg
        .ops()
        .filter(|o| o.produces.iter().any(|a| stale[a].stale))
        .map(|o| o.name.clone())
        .collect();
    if ops.is_empty() {
        return None;
    }
    // every stale asset is a target of a build-all, partitioned ones included
    let targets: Vec<String> = reg
        .ops()
        .flat_map(|o| o.produces.iter())
        .filter(|a| stale[*a].stale)
        .cloned()
        .collect();
    let keys = key_targets(reg, &stale, &ops, &targets, &HashMap::new());
    let seeds = seeds_for(reg, mats, &ops, &keys);
    Some(BuildPlan {
        ops: with_checks(reg, ops),
        seeds,
    })
}

pub(crate) fn mats_map(store: &Store) -> Result<Mats, Error> {
    Ok(store.latest_materializations()?.into_iter().collect())
}

/// launch a plan as one subset run of the assets job, [tagged](RunTags) with
/// whatever the caller can say that `Trigger::Build` cannot — which asset it
/// was asked for, which backfill it is a chunk of, which sensor set it off.
pub(crate) fn launch_plan(
    runner: &crate::executor::Runner,
    plan: BuildPlan,
    trigger: Trigger,
    tags: RunTags,
) -> Result<String, Error> {
    runner.launch_subset(
        ASSETS_JOB,
        plan.ops.into_iter().collect(),
        plan.seeds,
        json!({}),
        trigger,
        None,
        tags,
        None,
    )
}

/// launch a build of one asset: it, plus whatever upstream of it is stale, as
/// one run.
///
/// `Ok(None)` is an asset that is already up to date and had nothing to do —
/// not a refusal and not a run. named `keys` are a rebuild of exactly those
/// partitions whatever staleness says, which is the point of naming them.
///
/// the api handler and the command line both come through here, so "build this
/// asset" cannot come to mean two things depending on which one asked.
pub(crate) fn build_one(
    runner: &crate::executor::Runner,
    reg: &AssetRegistry,
    name: &str,
    keys: &[String],
) -> Result<Option<String>, Error> {
    let Some(meta) = reg.get(name) else {
        return Err(Error::UnknownAsset(name.to_string()));
    };
    if meta.source {
        return Err(Error::Graph("sources are probed, never built".into()));
    }
    let named: HashMap<String, Vec<String>> = match keys.is_empty() {
        true => HashMap::new(),
        false => {
            if meta.partitions.is_none() {
                return Err(Error::Graph(format!("asset {name} is not partitioned")));
            }
            HashMap::from([(name.to_string(), keys.to_vec())])
        }
    };
    // one build at a time: they share the assets job, and two overlapping ones
    // would materialize the same asset twice from different plans
    if runner.store().has_active_run(ASSETS_JOB)? {
        return Err(Error::Conflict("asset build already running".into()));
    }
    let mats = mats_map(runner.store())?;
    if named.is_empty() && !staleness(reg, &mats)[name].stale {
        return Ok(None);
    }
    let plan = plan_partitions(reg, &mats, std::slice::from_ref(&name.to_string()), &named)?;
    launch_plan(runner, plan, Trigger::Build, asset_tag(name)).map(Some)
}

/// the tag a build of one named asset carries.
pub(crate) fn asset_tag(asset: &str) -> RunTags {
    RunTags::from([("asset".to_string(), asset.to_string())])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Runner;
    use crate::model::{OpStatus, RunStatus};
    use chrono::Utc;

    fn mat(asset: &str, fp: &str, inputs: Value, value: Option<Value>) -> Materialization {
        Materialization {
            id: 0,
            asset: asset.into(),
            partition: None,
            fingerprint: fp.into(),
            inputs,
            value,
            run_id: None,
            built_at: Utc::now(),
            metadata: None,
        }
    }

    fn mats(list: Vec<Materialization>) -> Mats {
        list.into_iter().collect()
    }

    fn part(asset: &str, key: &str, fp: &str, inputs: Value) -> Materialization {
        Materialization {
            partition: Some(key.into()),
            ..mat(asset, fp, inputs, Some(json!({"key": key})))
        }
    }

    fn echo(name: &str) -> Asset {
        let out = name.to_string();
        Asset::new(name, move |_| {
            let out = out.clone();
            async move { Ok(json!(out)) }
        })
    }

    // source -> a -> b, all names as values
    fn chain() -> AssetRegistry {
        let s = Asset::source("s");
        let a = echo("a").from(&s);
        let b = echo("b").from(&a);
        AssetRegistry::new(vec![s, a, b], Vec::new(), Vec::new()).unwrap()
    }

    fn reg_err(assets: Vec<Asset>) -> Error {
        checked_err(assets, Vec::new())
    }

    fn multi_err(assets: Vec<Asset>, multis: Vec<MultiAsset>) -> Error {
        match AssetRegistry::new(assets, multis, Vec::new()) {
            Err(e) => e,
            Ok(_) => panic!("expected a graph error"),
        }
    }

    fn checked_err(assets: Vec<Asset>, checks: Vec<AssetCheck>) -> Error {
        match AssetRegistry::new(assets, Vec::new(), checks) {
            Err(e) => e,
            Ok(_) => panic!("expected a graph error"),
        }
    }

    #[test]
    fn registry_orders_topologically_and_validates() {
        // declared sink-first; the registry reorders
        let s = Asset::source("s");
        let a = echo("a").from(&s);
        let b = echo("b").from(&a);
        let reg = AssetRegistry::new(vec![b, a, s], Vec::new(), Vec::new()).unwrap();
        let names: Vec<&str> = reg.topo().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["s", "a", "b"]);

        let dup = reg_err(vec![echo("x"), echo("x")]);
        assert!(dup.to_string().contains("duplicate"), "{dup}");

        let ghost = Asset::source("ghost");
        let unknown = reg_err(vec![echo("x").from(&ghost)]);
        assert!(unknown.to_string().contains("unknown"), "{unknown}");

        // a cycle via from on already-built assets
        let x = echo("x");
        let y = echo("y").from(&x);
        let x = x.from(&y);
        let cycle = reg_err(vec![x, y]);
        assert!(cycle.to_string().contains("cycle"), "{cycle}");

        let probed =
            Asset::new("d", |_| async { Ok(json!(null)) }).probe(|| async { Ok("fp".to_string()) });
        let err = reg_err(vec![probed]);
        assert!(err.to_string().contains("probe on a derived"), "{err}");

        let base = echo("base");
        let err = reg_err(vec![Asset::source("s").from(&base), base]);
        assert!(err.to_string().contains("source cannot depend"), "{err}");

        let err = reg_err(vec![Asset::source("s").auto()]);
        assert!(err.to_string().contains("auto on a source"), "{err}");
    }

    #[test]
    fn staleness_never_built_and_first_probe() {
        let reg = chain();
        let s = staleness(&reg, &Mats::default());
        for name in ["s", "a", "b"] {
            assert!(s[name].stale, "{name} should be stale");
            assert!(s[name].reasons.is_empty(), "{name} has no dep reasons");
        }
        let m = mats(vec![mat("s", "s1", json!({}), None)]);
        let s = staleness(&reg, &m);
        assert!(!s["s"].stale);
        assert!(s["a"].stale && s["b"].stale);
    }

    #[test]
    fn staleness_dep_changed_and_fresh() {
        let reg = chain();
        let fresh = mats(vec![
            mat("s", "s1", json!({}), None),
            mat("a", "a1", json!({"s": "s1"}), Some(json!("a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("b"))),
        ]);
        let s = staleness(&reg, &fresh);
        assert!(["s", "a", "b"].iter().all(|n| !s[*n].stale));

        let moved = mats(vec![
            mat("s", "s2", json!({}), None),
            mat("a", "a1", json!({"s": "s1"}), Some(json!("a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("b"))),
        ]);
        let s = staleness(&reg, &moved);
        assert!(!s["s"].stale);
        assert_eq!(
            s["a"].reasons,
            vec![StaleReason {
                dep: "s".into(),
                had: Some("s1".into()),
                now: Some("s2".into()),
            }]
        );
        // b's recorded input still matches a's current fingerprint: only the
        // dep-stale propagation makes it stale
        assert!(s["b"].stale);
        assert_eq!(
            s["b"].reasons,
            vec![StaleReason {
                dep: "a".into(),
                had: Some("a1".into()),
                now: Some("a1".into()),
            }]
        );
    }

    #[test]
    fn staleness_missing_dep_mat() {
        let reg = chain();
        // a materialized against a source row that has since vanished
        let m = mats(vec![
            mat("a", "a1", json!({"s": "s1"}), Some(json!("a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("b"))),
        ]);
        let s = staleness(&reg, &m);
        assert!(s["a"].stale);
        assert_eq!(
            s["a"].reasons,
            vec![StaleReason {
                dep: "s".into(),
                had: Some("s1".into()),
                now: None,
            }]
        );
        assert!(s["b"].stale);
    }

    #[test]
    fn staleness_diamond_reaches_the_sink_through_both_arms() {
        let s = Asset::source("s");
        let left = echo("left").from(&s);
        let right = echo("right").from(&s);
        let sink = echo("sink").from(&left).from(&right);
        let reg = AssetRegistry::new(vec![s, left, right, sink], Vec::new(), Vec::new()).unwrap();
        let m = mats(vec![
            mat("s", "s2", json!({}), None),
            mat("left", "l1", json!({"s": "s1"}), Some(json!("left"))),
            mat("right", "r1", json!({"s": "s1"}), Some(json!("right"))),
            mat(
                "sink",
                "k1",
                json!({"left": "l1", "right": "r1"}),
                Some(json!("sink")),
            ),
        ]);
        let st = staleness(&reg, &m);
        assert!(st["left"].stale && st["right"].stale && st["sink"].stale);
        let deps: Vec<&str> = st["sink"].reasons.iter().map(|r| r.dep.as_str()).collect();
        assert_eq!(deps, ["left", "right"]);
    }

    #[test]
    fn plan_always_includes_the_target_and_seeds_fresh_deps() {
        let reg = chain();
        let m = mats(vec![
            mat("s", "s1", json!({}), None),
            mat("a", "a1", json!({"s": "s1"}), Some(json!("stored-a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("stored-b"))),
        ]);
        let plan = plan_target(&reg, &m, "b").unwrap();
        assert_eq!(plan.ops, ["b"]);
        assert_eq!(plan.seeds, HashMap::from([("a".into(), json!("stored-a"))]));

        let moved = mats(vec![
            mat("s", "s2", json!({}), None),
            mat("a", "a1", json!({"s": "s1"}), Some(json!("stored-a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("stored-b"))),
        ]);
        let plan = plan_target(&reg, &moved, "b").unwrap();
        assert_eq!(plan.ops, ["a", "b"]);
        assert_eq!(plan.seeds, HashMap::from([("s".into(), Value::Null)]));

        assert!(matches!(
            plan_target(&reg, &m, "nope").unwrap_err(),
            Error::UnknownAsset(n) if n == "nope"
        ));
        let err = plan_target(&reg, &m, "s").unwrap_err();
        assert!(err.to_string().contains("is a source"), "{err}");
    }

    #[test]
    fn plan_targets_unions_shared_ancestors_into_one_plan() {
        // s -> a -> {b, c}: both targets lean on the same ancestor
        let s = Asset::source("s");
        let a = echo("a").from(&s);
        let b = echo("b").from(&a);
        let c = echo("c").from(&a);
        let reg = AssetRegistry::new(vec![s, a, b, c], Vec::new(), Vec::new()).unwrap();

        let m = mats(vec![mat("s", "s1", json!({}), None)]);
        let plan = plan_targets(&reg, &m, &["b".into(), "c".into()]).unwrap();
        assert_eq!(plan.ops, ["a", "b", "c"]);
        assert_eq!(plan.seeds, HashMap::from([("s".into(), Value::Null)]));

        let m = mats(vec![
            mat("s", "s1", json!({}), None),
            mat("a", "a1", json!({"s": "s1"}), Some(json!("stored-a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("b"))),
            mat("c", "c1", json!({"a": "a1"}), Some(json!("c"))),
        ]);
        let plan = plan_targets(&reg, &m, &["b".into(), "c".into()]).unwrap();
        assert_eq!(plan.ops, ["b", "c"]);
        assert_eq!(plan.seeds, HashMap::from([("a".into(), json!("stored-a"))]));

        let err = plan_targets(&reg, &m, &["b".into(), "s".into()]).unwrap_err();
        assert!(err.to_string().contains("is a source"), "{err}");
    }

    #[test]
    fn plan_all_covers_every_stale_asset_in_one_plan() {
        let reg = chain();
        let fresh = mats(vec![
            mat("s", "s1", json!({}), None),
            mat("a", "a1", json!({"s": "s1"}), Some(json!("stored-a"))),
            mat("b", "b1", json!({"a": "a1"}), Some(json!("stored-b"))),
        ]);
        assert!(plan_all(&reg, &fresh).is_none());

        let plan = plan_all(&reg, &Mats::default()).unwrap();
        assert_eq!(plan.ops, ["a", "b"]);
        assert_eq!(plan.seeds, HashMap::from([("s".into(), Value::Null)]));
    }

    #[test]
    fn content_fingerprint_is_stable_sha256_of_the_json() {
        let a = json!({"b": 2, "a": 1});
        let b = json!({"a": 1, "b": 2});
        assert_eq!(content_fingerprint(&a), content_fingerprint(&b));
        // hand-checked: echo -n '{"a":1,"b":2}' | sha256sum
        assert_eq!(
            content_fingerprint(&a),
            "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"
        );
    }

    async fn build_all(reg: &AssetRegistry, runner: &Runner) -> crate::model::Run {
        let m = mats_map(runner.store()).unwrap();
        let plan = plan_all(reg, &m).expect("something stale");
        runner
            .run_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                RunTags::new(),
            )
            .await
            .unwrap()
    }

    /// for the cases that launch rather than run, because they cancel or
    /// interfere with what the run is doing while it does it.
    async fn settled(runner: &Runner, id: &str) -> crate::model::Run {
        for _ in 0..1_000 {
            let run = runner.store().run(id).unwrap().unwrap();
            if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
                return run;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("run {id} never settled");
    }

    async fn build_target(reg: &AssetRegistry, runner: &Runner, target: &str) -> crate::model::Run {
        let m = mats_map(runner.store()).unwrap();
        let plan = plan_target(reg, &m, target).unwrap();
        runner
            .run_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                RunTags::new(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn rebuilds_append_history_while_the_latest_still_decides_staleness() {
        let store = Store::open(":memory:").unwrap();
        let pinned = Arc::new(std::sync::Mutex::new("v1".to_string()));
        let fp = pinned.clone();
        let s = Asset::source("s");
        let a = Asset::new("a", move |ctx: OpCtx| {
            let fp = fp.clone();
            async move {
                ctx.set_fingerprint(fp.lock().unwrap().clone());
                Ok(json!({"rows": 1}))
            }
        })
        .from(&s);
        let b = Asset::new("b", |_| async { Ok(json!("b")) }).from(&a);
        let reg = AssetRegistry::new(vec![s, a, b], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        store
            .record_materialization("s", None, "s-fp", &json!({}), None, None, None)
            .unwrap();
        build_all(&reg, &runner).await;

        // built twice more: once to the same fingerprint, once to a new one
        build_target(&reg, &runner, "a").await;
        *pinned.lock().unwrap() = "v2".to_string();
        build_target(&reg, &runner, "a").await;

        let history = store.materializations("a", None, 10).unwrap();
        let seen: Vec<(&str, bool)> = history
            .iter()
            .map(|e| (e.mat.fingerprint.as_str(), e.changed))
            .collect();
        assert_eq!(seen, [("v2", true), ("v1", false), ("v1", true)]);
        // every entry names the run that built it, and they are distinct runs
        assert!(history.iter().all(|e| e.mat.run_id.is_some()));

        // the latest entry is the current one, and it is the only one
        // staleness reads: b consumed v1 and a now says v2
        let m = mats_map(&store).unwrap();
        assert_eq!(m.get("a", None).unwrap().fingerprint, "v2");
        let st = staleness(&reg, &m);
        assert!(!st["a"].stale);
        assert_eq!(
            st["b"].reasons,
            vec![StaleReason {
                dep: "a".into(),
                had: Some("v1".into()),
                now: Some("v2".into()),
            }]
        );
    }

    #[tokio::test]
    async fn build_writes_materializations_with_dep_fingerprints() {
        let store = Store::open(":memory:").unwrap();
        let s = Asset::source("s");
        let a = Asset::new("a", |ctx| async move {
            // the source dep is lineage, not data: its input is null
            assert_eq!(ctx.input("s"), Some(&Value::Null));
            Ok(json!({"rows": 3}))
        })
        .from(&s);
        let b = Asset::new("b", |ctx| async move {
            let rows = ctx.input("a").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"doubled": rows * 2}))
        })
        .from(&a);
        let reg = AssetRegistry::new(vec![s, a, b], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        // the source was probed before this build
        store
            .record_materialization("s", None, "s-fp", &json!({}), None, None, None)
            .unwrap();

        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(run.job, ASSETS_JOB);
        assert_eq!(run.trigger, Trigger::Build);

        let ma = store.materialization("a", None).unwrap().unwrap();
        assert_eq!(ma.inputs, json!({"s": "s-fp"}));
        assert_eq!(ma.value, Some(json!({"rows": 3})));
        assert_eq!(ma.run_id.as_deref(), Some(run.id.as_str()));
        assert_eq!(ma.fingerprint, content_fingerprint(&json!({"rows": 3})));

        let mb = store.materialization("b", None).unwrap().unwrap();
        assert_eq!(mb.inputs, json!({"a": ma.fingerprint}));
        assert_eq!(mb.value, Some(json!({"doubled": 6})));

        let m = mats_map(&store).unwrap();
        let st = staleness(&reg, &m);
        assert!(st.values().all(|s| !s.stale));
    }

    /// a manager with nowhere to put anything, which is the last thing that
    /// can go wrong between an asset body returning and the op being recorded
    /// as having succeeded.
    struct Refuses;

    impl crate::io::IoManager for Refuses {
        fn put(&self, _key: &crate::IoKey, _value: Value) -> crate::IoResult {
            Err("nowhere to put it".into())
        }
        fn get(&self, _key: &crate::IoKey, handle: &Value) -> crate::IoResult {
            Ok(handle.clone())
        }
        fn drop_run(&self, _run_id: &str, _job: &str) -> crate::IoDropped {
            Ok(())
        }
    }

    // the value was computed and its fingerprint taken, and then the output
    // went nowhere. a materialization here would say the asset is current
    // while nothing holds what it is current with, and the next build would
    // read that and skip it.
    #[tokio::test]
    async fn a_build_whose_output_cannot_be_stored_records_nothing() {
        let store = Store::open(":memory:").unwrap();
        let a = Asset::new("a", |_| async { Ok(json!({"rows": 3})) });
        let reg = AssetRegistry::new(vec![a], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::with_io(
            [reg.lower_job().unwrap()],
            store.clone(),
            Vec::new(),
            Vec::new(),
            Arc::new(Refuses),
            Vec::new(),
        )
        .unwrap();

        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Failed);
        let op = &store.op_runs(&run.id).unwrap()[0];
        assert_eq!(op.status, OpStatus::Failed);
        assert!(
            op.error.as_deref().unwrap_or_default().contains("persist"),
            "{:?}",
            op.error
        );
        assert!(
            store.materialization("a", None).unwrap().is_none(),
            "an asset nobody stored the value of was recorded as built"
        );
        // and it is still what the next build is for
        let m = mats_map(&store).unwrap();
        assert!(staleness(&reg, &m)["a"].stale);
    }

    // an attempt that never reached the end of the work has nothing to say
    // about what was built, however it ended.
    #[tokio::test]
    async fn a_build_that_fails_or_panics_records_nothing_and_its_retry_records_one() {
        let store = Store::open(":memory:").unwrap();
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counted = attempts.clone();
        let broken = Asset::new("broken", |_| async { Err("no data".into()) });
        let panicky = Asset::new("panicky", |_| async { panic!("mid-build") });
        // fails once, then works: the value it built the second time is the
        // one entry, and the attempt that failed left nothing behind it
        let flaky = Asset::new("flaky", move |_| {
            let counted = counted.clone();
            async move {
                match counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                    0 => Err("not yet".into()),
                    _ => Ok(json!({"rows": 1})),
                }
            }
        })
        .retries(1);
        let reg = AssetRegistry::new(vec![broken, panicky, flaky], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();

        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Failed);
        for asset in ["broken", "panicky"] {
            assert!(
                store.materialization(asset, None).unwrap().is_none(),
                "{asset} was recorded as built"
            );
        }
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(store.materializations("flaky", None, 10).unwrap().len(), 1);
        assert_eq!(
            store.materialization("flaky", None).unwrap().unwrap().value,
            Some(json!({"rows": 1}))
        );
    }

    // a cancelled run stops its ops where they stand, and an op that was
    // stopped built nothing — whatever it had computed by then.
    #[tokio::test]
    async fn a_canceled_build_records_nothing() {
        let store = Store::open(":memory:").unwrap();
        let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let started = running.clone();
        let slow = Asset::new("slow", move |ctx: OpCtx| {
            let started = started.clone();
            async move {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                ctx.cancelled().await;
                // the abort lands on this await, so the value is computed and
                // never returned — which is every op that is stopped mid-work
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(json!({"rows": 1}))
            }
        });
        let reg = AssetRegistry::new(vec![slow], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();

        let m = mats_map(&store).unwrap();
        let plan = plan_all(&reg, &m).expect("something stale");
        let id = runner
            .launch_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                None,
                RunTags::new(),
                None,
            )
            .unwrap();
        while !running.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        runner.cancel(&id).unwrap();

        let run = settled(&runner, &id).await;
        assert_eq!(run.status, RunStatus::Canceled);
        assert_eq!(
            store.op_run(&id, "slow").unwrap().unwrap().status,
            OpStatus::Canceled
        );
        assert!(store.materialization("slow", None).unwrap().is_none());
    }

    // several assets out of one op are one fact about one op run, so the
    // history gains all of them or none. asserted rather than described: a
    // trigger refuses one insert, and everything the op wrote goes back with
    // it — the other materialization and the op run's own terminal row.
    //
    // and the run stops where that write stopped. it does not go on to report
    // a status the store never took: it is left `running`, claimed, for a
    // reclaimer to settle, which is what a build that hestan cannot record
    // now leaves behind.
    #[tokio::test]
    async fn a_multi_asset_records_all_of_its_outputs_or_none_of_them() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap();
        let store = Store::open(path).unwrap();
        rusqlite::Connection::open(path)
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER refuse_rejected BEFORE INSERT ON asset_materializations
                 WHEN NEW.asset = 'rejected'
                 BEGIN SELECT RAISE(ABORT, 'this insert is refused'); END",
            )
            .unwrap();

        let split = MultiAsset::new("split", |_| async {
            Ok(json!({"clean": {"rows": 2}, "rejected": {"rows": 1}}))
        })
        .produces(["clean", "rejected"]);
        let reg = AssetRegistry::new(Vec::new(), vec![split], Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();

        let m = mats_map(&store).unwrap();
        let plan = plan_all(&reg, &m).expect("something stale");
        let id = runner
            .launch_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                None,
                RunTags::new(),
                None,
            )
            .unwrap();
        // a constraint is not a lock: it says the same thing every time, so
        // the write is attempted once and the run stops on it
        for _ in 0..1_000 {
            if store.health().unrecorded_writes() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        for asset in ["clean", "rejected"] {
            assert!(
                store.materialization(asset, None).unwrap().is_none(),
                "{asset} was recorded on its own"
            );
        }
        // the op run row is where it was before the write: the same
        // transaction carried both, so neither landed
        let op = store.op_run(&id, "split").unwrap().unwrap();
        assert_eq!(op.status, OpStatus::Running);
        assert_eq!(op.finished_at, None);
        // and the run says nothing about itself, because there is nothing this
        // process can honestly say
        let run = store.run(&id).unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Running);
        assert_eq!(run.finished_at, None);
        assert!(
            run.claimed_by.is_some(),
            "its claim is what a reclaim needs"
        );
    }

    #[tokio::test]
    async fn asset_metadata_lands_on_both_the_op_run_and_the_materialization() {
        use crate::op::Meta;

        let store = Store::open(":memory:").unwrap();
        let counted = Asset::new("counted", |ctx: OpCtx| async move {
            ctx.meta("rows", 3);
            ctx.meta("source", Meta::Url("https://example.test/rows".into()));
            Ok(json!({"rows": 3}))
        });
        let quiet = Asset::new("quiet", |_| async { Ok(json!(null)) });
        let reg = AssetRegistry::new(vec![counted, quiet], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;

        let reported = json!({
            "rows": {"int": 3},
            "source": {"url": "https://example.test/rows"},
        });
        let ops = store.op_runs(&run.id).unwrap();
        let op = ops.iter().find(|o| o.op == "counted").unwrap();
        assert_eq!(op.metadata, Some(reported.clone()));
        let m = store.materialization("counted", None).unwrap().unwrap();
        assert_eq!(m.metadata, Some(reported));

        // and an asset that reported nothing carries null in both places
        let quiet = ops.iter().find(|o| o.op == "quiet").unwrap();
        assert_eq!(quiet.metadata, None);
        assert_eq!(
            store
                .materialization("quiet", None)
                .unwrap()
                .unwrap()
                .metadata,
            None
        );
    }

    #[tokio::test]
    async fn set_fingerprint_overrides_the_content_hash() {
        let store = Store::open(":memory:").unwrap();
        let pinned = Asset::new("pinned", |ctx| async move {
            ctx.set_fingerprint("version-7".into());
            Ok(json!({"data": [1, 2]}))
        });
        let hashed = Asset::new("hashed", |_| async { Ok(json!({"data": [1, 2]})) });
        let reg = AssetRegistry::new(vec![pinned, hashed], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        build_all(&reg, &runner).await;

        assert_eq!(
            store
                .materialization("pinned", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "version-7"
        );
        assert_eq!(
            store
                .materialization("hashed", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            content_fingerprint(&json!({"data": [1, 2]}))
        );
    }

    #[tokio::test]
    async fn memoized_build_seeds_fresh_assets_and_skips_their_ops() {
        let store = Store::open(":memory:").unwrap();
        let s = Asset::source("s");
        let a = Asset::new("a", |_| async { Ok(json!({"rows": 3})) }).from(&s);
        let b = Asset::new("b", |ctx| async move {
            let rows = ctx.input("a").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"doubled": rows * 2}))
        })
        .from(&a);
        let reg = AssetRegistry::new(vec![s, a, b], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        store
            .record_materialization("s", None, "s-fp", &json!({}), None, None, None)
            .unwrap();
        build_all(&reg, &runner).await;

        // poke only b stale: pretend it consumed an older a
        store
            .record_materialization(
                "b",
                None,
                "b-old",
                &json!({"a": "older"}),
                Some(&json!({})),
                None,
                None,
            )
            .unwrap();
        let m = mats_map(&store).unwrap();
        let st = staleness(&reg, &m);
        assert!(!st["a"].stale && st["b"].stale);

        let plan = plan_target(&reg, &m, "b").unwrap();
        assert_eq!(plan.ops, ["b"]);
        assert_eq!(
            plan.seeds,
            HashMap::from([("a".into(), json!({"rows": 3}))])
        );

        let run = runner
            .run_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                RunTags::new(),
            )
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        let ops = runner.store().op_runs(&run.id).unwrap();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].op, "b");
        assert_eq!(ops[0].output, Some(json!({"doubled": 6})));
    }

    #[tokio::test]
    async fn full_launch_of_the_assets_job_rebuilds_everything() {
        let store = Store::open(":memory:").unwrap();
        let s = Asset::source("s");
        let a = Asset::new("a", |_| async { Ok(json!(1)) }).from(&s);
        let reg = AssetRegistry::new(vec![s, a], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = runner
            .run(ASSETS_JOB, json!({}), crate::model::Trigger::Manual)
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        let m = store.materialization("a", None).unwrap().unwrap();
        assert_eq!(m.inputs, json!({"s": null}));
    }

    fn rows_check(name: &str, asset: &str, want: u64) -> AssetCheck {
        AssetCheck::new(name, asset, move |_ctx: OpCtx, value: Value| async move {
            let rows = value.get("rows").and_then(Value::as_u64).unwrap_or(0);
            if rows >= want {
                Ok(CheckResult::pass().meta("rows", rows as i64))
            } else {
                Ok(CheckResult::fail(format!("{rows} rows, wanted {want}")))
            }
        })
    }

    #[test]
    fn checks_validate_their_asset_and_their_name() {
        let unknown = checked_err(vec![echo("a")], vec![rows_check("rows", "ghost", 1)]);
        assert!(
            unknown.to_string().contains("no asset named ghost"),
            "{unknown}"
        );

        let s = Asset::source("s");
        let on_source = checked_err(vec![s, echo("a")], vec![rows_check("rows", "s", 1)]);
        assert!(on_source.to_string().contains("is a source"), "{on_source}");

        let dup = checked_err(
            vec![echo("a")],
            vec![rows_check("rows", "a", 1), rows_check("rows", "a", 2)],
        );
        assert!(dup.to_string().contains("duplicate check rows"), "{dup}");

        // the same name on two different assets is two different checks
        AssetRegistry::new(
            vec![echo("a"), echo("b")],
            Vec::new(),
            vec![rows_check("rows", "a", 1), rows_check("rows", "b", 1)],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn checks_record_what_they_saw_and_error_severity_fails_the_run() {
        let store = Store::open(":memory:").unwrap();
        let a = Asset::new("a", |_| async { Ok(json!({"rows": 3})) });
        let checks = vec![
            rows_check("has_rows", "a", 1),
            rows_check("has_many", "a", 100).severity(Severity::Warn),
        ];
        let reg = AssetRegistry::new(vec![a], Vec::new(), checks).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;

        // one passed, one failed, and the warn failure did not fail the run
        assert_eq!(run.status, RunStatus::Success);
        let results = store.asset_checks("a", None, 10).unwrap();
        assert_eq!(results.len(), 2);
        let passed = results.iter().find(|c| c.check == "has_rows").unwrap();
        assert_eq!(passed.status, CheckStatus::Passed);
        assert_eq!(passed.severity, Severity::Error);
        assert_eq!(passed.message, None);
        // the check saw the value the asset had just produced
        assert_eq!(passed.metadata, Some(json!({"rows": {"int": 3}})));
        assert_eq!(passed.run_id, run.id);
        let failed = results.iter().find(|c| c.check == "has_many").unwrap();
        assert_eq!(failed.status, CheckStatus::Failed);
        assert_eq!(failed.severity, Severity::Warn);
        assert_eq!(failed.message.as_deref(), Some("3 rows, wanted 100"));
        // a warn failure is a check that failed inside an op that succeeded
        let ops = store.op_runs(&run.id).unwrap();
        let warn_op = ops.iter().find(|o| o.op == "check:a:has_many").unwrap();
        assert_eq!(warn_op.status, OpStatus::Success);

        // the same check at error severity fails its op and the run
        let store = Store::open(":memory:").unwrap();
        let a = Asset::new("a", |_| async { Ok(json!({"rows": 3})) });
        let reg = AssetRegistry::new(vec![a], Vec::new(), vec![rows_check("has_many", "a", 100)])
            .unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Failed);
        assert!(
            run.error.as_deref().unwrap().contains("3 rows, wanted 100"),
            "{:?}",
            run.error
        );
        // the verdict is recorded even though the op failed
        assert_eq!(
            store.asset_checks("a", None, 10).unwrap()[0].status,
            CheckStatus::Failed
        );
        // and the asset is materialized regardless: a failing check fails the
        // run that produced it, it does not un-produce the asset
        assert_eq!(
            store.materialization("a", None).unwrap().unwrap().value,
            Some(json!({"rows": 3}))
        );
    }

    #[tokio::test]
    async fn a_failing_error_check_leaves_downstream_assets_alone() {
        let store = Store::open(":memory:").unwrap();
        let a = Asset::new("a", |_| async { Ok(json!({"rows": 3})) });
        let b = Asset::new("b", |ctx| async move {
            let rows = ctx.input("a").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"rows": rows * 2}))
        })
        .from(&a);
        let reg = AssetRegistry::new(
            vec![a, b],
            Vec::new(),
            vec![rows_check("has_many", "a", 100)],
        )
        .unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;

        assert_eq!(run.status, RunStatus::Failed);
        // checks hang off the asset op rather than feeding it, so nothing
        // downstream of the asset is downstream of the check
        let ops = store.op_runs(&run.id).unwrap();
        let b_op = ops.iter().find(|o| o.op == "b").unwrap();
        assert_eq!(b_op.status, OpStatus::Success);
        assert_eq!(
            store.materialization("b", None).unwrap().unwrap().value,
            Some(json!({"rows": 6}))
        );
    }

    #[tokio::test]
    async fn a_memoized_asset_is_not_re_checked() {
        let store = Store::open(":memory:").unwrap();
        let s = Asset::source("s");
        let a = Asset::new("a", |_| async { Ok(json!({"rows": 3})) }).from(&s);
        let b = Asset::new("b", |ctx| async move {
            let rows = ctx.input("a").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"rows": rows * 2}))
        })
        .from(&a);
        let checks = vec![
            rows_check("has_rows", "a", 1),
            rows_check("has_rows", "b", 1),
        ];
        let reg = AssetRegistry::new(vec![s, a, b], Vec::new(), checks).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        store
            .record_materialization("s", None, "s-fp", &json!({}), None, None, None)
            .unwrap();

        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);
        let ran = store.op_runs(&run.id).unwrap();
        let planned: Vec<&str> = ran.iter().map(|o| o.op.as_str()).collect();
        assert_eq!(planned, ["a", "b", "check:a:has_rows", "check:b:has_rows"]);
        assert_eq!(store.asset_checks("a", None, 10).unwrap().len(), 1);

        // poke only b stale, so a is seeded rather than rebuilt
        store
            .record_materialization(
                "b",
                None,
                "b-old",
                &json!({"a": "older"}),
                Some(&json!({})),
                None,
                None,
            )
            .unwrap();
        let m = mats_map(&store).unwrap();
        let plan = plan_target(&reg, &m, "b").unwrap();
        // a check is in the plan exactly when its asset is
        assert_eq!(plan.ops, ["b", "check:b:has_rows"]);

        let run = runner
            .run_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                RunTags::new(),
            )
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        // b re-checked, a not: it produced no new value for a check to see
        assert_eq!(store.asset_checks("a", None, 10).unwrap().len(), 1);
        assert_eq!(store.asset_checks("b", None, 10).unwrap().len(), 2);
    }

    // one pull, two tables: the motivating shape for a multi-asset
    fn split() -> MultiAsset {
        MultiAsset::new("split_orders", |_| async {
            Ok(json!({
                "orders_clean": {"rows": 2},
                "orders_rejected": {"rows": 1},
            }))
        })
        .produces(["orders_clean", "orders_rejected"])
    }

    #[test]
    fn multi_assets_validate_what_they_produce() {
        let bare = MultiAsset::new("split", |_| async { Ok(json!({})) });
        let err = multi_err(Vec::new(), vec![bare]);
        assert!(err.to_string().contains("produces nothing"), "{err}");

        // one name, two ops: the job would have no way to tell them apart
        let twin = MultiAsset::new("split_orders", |_| async { Ok(json!({})) })
            .produces(["other_clean", "other_rejected"]);
        let err = multi_err(Vec::new(), vec![split(), twin]);
        assert!(
            err.to_string()
                .contains("duplicate multi-asset split_orders"),
            "{err}"
        );

        // two multi-assets claiming one output is a duplicate asset, which the
        // asset-level topo check names before anything op-level does
        let other = MultiAsset::new("other", |_| async { Ok(json!({})) })
            .produces(["orders_clean", "extra"]);
        let err = multi_err(Vec::new(), vec![split(), other]);
        assert!(err.to_string().contains("duplicate"), "{err}");

        // an op and the assets share one namespace inside the job
        let err = multi_err(vec![echo("split_orders")], vec![split()]);
        assert!(
            err.to_string().contains("an asset is already called that"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn one_op_materializes_every_asset_it_produces() {
        let store = Store::open(":memory:").unwrap();
        let reg = AssetRegistry::new(Vec::new(), vec![split()], Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);

        // one op run, two materializations, each fingerprinting its own key
        let ops = store.op_runs(&run.id).unwrap();
        let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
        assert_eq!(names, ["split_orders"]);
        let clean = store
            .materialization("orders_clean", None)
            .unwrap()
            .unwrap();
        assert_eq!(clean.value, Some(json!({"rows": 2})));
        assert_eq!(clean.fingerprint, content_fingerprint(&json!({"rows": 2})));
        assert_eq!(clean.run_id.as_deref(), Some(run.id.as_str()));
        let rejected = store
            .materialization("orders_rejected", None)
            .unwrap()
            .unwrap();
        assert_eq!(rejected.value, Some(json!({"rows": 1})));
        assert_ne!(clean.fingerprint, rejected.fingerprint);

        // nothing is materialized under the op's own name
        assert!(
            store
                .materialization("split_orders", None)
                .unwrap()
                .is_none()
        );
        let m = mats_map(&store).unwrap();
        assert!(staleness(&reg, &m).values().all(|s| !s.stale));
    }

    #[tokio::test]
    async fn a_multi_asset_output_that_does_not_match_what_it_produces_fails() {
        let store = Store::open(":memory:").unwrap();
        let short = MultiAsset::new("split_orders", |_| async {
            Ok(json!({"orders_clean": {"rows": 2}, "surprise": 1}))
        })
        .produces(["orders_clean", "orders_rejected"]);
        let reg = AssetRegistry::new(Vec::new(), vec![short], Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;

        assert_eq!(run.status, RunStatus::Failed);
        let why = run.error.unwrap();
        assert!(why.contains("no key for orders_rejected"), "{why}");
        assert!(why.contains("a key for surprise"), "{why}");
        // and neither asset materialized: the op never got that far
        assert!(
            store
                .materialization("orders_clean", None)
                .unwrap()
                .is_none()
        );

        // the same for an output that is not an object at all
        let store = Store::open(":memory:").unwrap();
        let flat = MultiAsset::new("split_orders", |_| async { Ok(json!("done")) })
            .produces(["orders_clean", "orders_rejected"]);
        let reg = AssetRegistry::new(Vec::new(), vec![flat], Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;
        assert!(
            run.error
                .unwrap()
                .contains("returned a string, not an object"),
            "wrong error"
        );
    }

    #[tokio::test]
    async fn a_downstream_asset_reads_the_key_it_depends_on() {
        let store = Store::open(":memory:").unwrap();
        let report = Asset::new("report", |ctx: OpCtx| async move {
            // the dep is named as the asset, whatever op produced it
            let rows = ctx.input("orders_clean").unwrap()["rows"].as_u64().unwrap();
            assert_eq!(ctx.input("orders_rejected"), None, "undeclared dep leaked");
            Ok(json!({"kept": rows}))
        })
        .from_named("orders_clean");
        let reg = AssetRegistry::new(vec![report], vec![split()], Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(
            store
                .materialization("report", None)
                .unwrap()
                .unwrap()
                .value,
            Some(json!({"kept": 2}))
        );
        // lineage is recorded against the asset, not the op that produced it
        let clean = store
            .materialization("orders_clean", None)
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .materialization("report", None)
                .unwrap()
                .unwrap()
                .inputs,
            json!({"orders_clean": clean.fingerprint})
        );

        // and a memoized rebuild seeds that key from the store rather than
        // re-running the pull
        store
            .record_materialization(
                "report",
                None,
                "stale",
                &json!({"orders_clean": "older"}),
                Some(&json!({})),
                None,
                None,
            )
            .unwrap();
        let m = mats_map(&store).unwrap();
        let plan = plan_target(&reg, &m, "report").unwrap();
        assert_eq!(plan.ops, ["report"]);
        assert_eq!(
            plan.seeds,
            HashMap::from([(
                "split_orders".into(),
                json!({"orders_clean": {"rows": 2}, "orders_rejected": {"rows": 1}}),
            )])
        );
        let run = runner
            .run_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                RunTags::new(),
            )
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(store.op_runs(&run.id).unwrap().len(), 1);
    }

    #[test]
    fn one_stale_output_plans_the_op_once() {
        let reg = AssetRegistry::new(Vec::new(), vec![split()], Vec::new()).unwrap();
        // both outputs stale: still one op
        let plan = plan_all(&reg, &Mats::default()).unwrap();
        assert_eq!(plan.ops, ["split_orders"]);

        // and so is one of them
        let m = mats(vec![mat(
            "orders_clean",
            "c1",
            json!({}),
            Some(json!({"rows": 2})),
        )]);
        let plan = plan_all(&reg, &m).unwrap();
        assert_eq!(plan.ops, ["split_orders"]);
        let plan = plan_targets(&reg, &m, &["orders_rejected".into()]).unwrap();
        assert_eq!(plan.ops, ["split_orders"]);
        // naming both targets is still one run of one computation
        let plan =
            plan_targets(&reg, &m, &["orders_clean".into(), "orders_rejected".into()]).unwrap();
        assert_eq!(plan.ops, ["split_orders"]);
    }

    #[tokio::test]
    async fn per_asset_fingerprints_and_metadata_land_on_their_own_rows() {
        let store = Store::open(":memory:").unwrap();
        let tagged = MultiAsset::new("split_orders", |ctx: OpCtx| async move {
            ctx.meta("pulled", 3);
            ctx.set_fingerprint_of("orders_clean", "clean-v7".into());
            ctx.meta_of("orders_clean", "rows", 2);
            ctx.set_fingerprint("shared".into());
            Ok(json!({"orders_clean": {"rows": 2}, "orders_rejected": {"rows": 1}}))
        })
        .produces(["orders_clean", "orders_rejected"]);
        let reg = AssetRegistry::new(Vec::new(), vec![tagged], Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);

        let clean = store
            .materialization("orders_clean", None)
            .unwrap()
            .unwrap();
        assert_eq!(clean.fingerprint, "clean-v7");
        assert_eq!(clean.metadata, Some(json!({"rows": {"int": 2}})));
        // the op-wide override covers the output that staged none of its own
        let rejected = store
            .materialization("orders_rejected", None)
            .unwrap()
            .unwrap();
        assert_eq!(rejected.fingerprint, "shared");
        assert_eq!(rejected.metadata, None);
        // and what the op reported about the work as a whole is on its run row
        let ops = store.op_runs(&run.id).unwrap();
        assert_eq!(ops[0].metadata, Some(json!({"pulled": {"int": 3}})));
    }

    #[tokio::test]
    async fn checks_bind_to_a_produced_asset() {
        let store = Store::open(":memory:").unwrap();
        let reg = AssetRegistry::new(
            Vec::new(),
            vec![split()],
            vec![rows_check("has_rows", "orders_clean", 1)],
        )
        .unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);
        let results = store.asset_checks("orders_clean", None, 10).unwrap();
        // the check saw that key's value, not the whole object the op returned
        assert_eq!(results[0].metadata, Some(json!({"rows": {"int": 2}})));
    }

    // three keys, so "only the one targeted" is a claim about two others
    fn keyed(name: &str) -> Asset {
        Asset::new(name, |ctx: OpCtx| async move {
            let key = ctx.partition().expect("a partitioned body has its key");
            Ok(json!({ "key": key }))
        })
        .partitioned(Partitions::keys(["k1", "k2", "k3"]))
    }

    async fn build_plan(runner: &Runner, plan: BuildPlan) -> crate::model::Run {
        runner
            .run_subset(
                ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                json!({}),
                Trigger::Build,
                RunTags::new(),
            )
            .await
            .unwrap()
    }

    fn on(asset: &str, keys: [&str; 1]) -> HashMap<String, Vec<String>> {
        HashMap::from([(
            asset.to_string(),
            keys.iter().map(|k| k.to_string()).collect(),
        )])
    }

    #[test]
    fn staleness_is_per_key_and_the_asset_is_stale_when_any_key_is() {
        let s = Asset::source("s");
        let a = keyed("a").from(&s);
        let reg = AssetRegistry::new(vec![s, a], Vec::new(), Vec::new()).unwrap();
        let m = mats(vec![
            mat("s", "s1", json!({}), None),
            part("a", "k1", "a1", json!({"s": "s1"})),
            part("a", "k2", "a2", json!({"s": "s0"})),
        ]);
        let st = staleness(&reg, &m);
        assert!(!st["a"].parts["k1"].stale, "fresh key read as stale");
        assert!(
            st["a"].parts["k2"].stale,
            "a moved dep leaves the key stale"
        );
        assert!(st["a"].parts["k3"].stale, "a key that never built is stale");
        assert!(st["a"].stale, "any stale key makes the asset stale");
        // the whole-asset verdict carries no reasons: the evidence is per key
        assert!(st["a"].reasons.is_empty());
        assert_eq!(
            st["a"].parts["k2"].reasons,
            vec![StaleReason {
                dep: "s".into(),
                had: Some("s0".into()),
                now: Some("s1".into()),
            }]
        );
    }

    #[test]
    fn a_moved_source_fingerprint_makes_every_key_stale() {
        let s = Asset::source("s");
        let a = keyed("a").from(&s);
        let reg = AssetRegistry::new(vec![s, a], Vec::new(), Vec::new()).unwrap();
        let fresh = mats(vec![
            mat("s", "s1", json!({}), None),
            part("a", "k1", "a1", json!({"s": "s1"})),
            part("a", "k2", "a2", json!({"s": "s1"})),
            part("a", "k3", "a3", json!({"s": "s1"})),
        ]);
        assert!(!staleness(&reg, &fresh)["a"].stale);

        let probed = mats(vec![
            mat("s", "s2", json!({}), None),
            part("a", "k1", "a1", json!({"s": "s1"})),
            part("a", "k2", "a2", json!({"s": "s1"})),
            part("a", "k3", "a3", json!({"s": "s1"})),
        ]);
        let st = staleness(&reg, &probed);
        // crude but honest: an unpartitioned dep is read whole, so every key
        // of every descendant is stale at once
        assert!(st["a"].parts.values().all(|s| s.stale));
    }

    #[test]
    fn the_default_target_set_is_the_newest_stale_keys_capped() {
        let daily = Asset::new("daily", |_| async { Ok(json!(null)) })
            .partitioned(Partitions::daily("2026-01-01").build_limit(3));
        let reg = AssetRegistry::new(vec![daily], Vec::new(), Vec::new()).unwrap();
        // nothing built, so every key of an unbounded range is stale — and the
        // build limit is what stops that being a thousand instances
        let plan = plan_target(&reg, &Mats::default(), "daily").unwrap();
        let keys = plan.seeds["partitions:daily"].as_array().unwrap().clone();
        assert_eq!(keys.len(), 3, "the build limit did not cap the range");
        let keys: Vec<&str> = keys.iter().map(|k| k.as_str().unwrap()).collect();
        let mut newest_first = keys.clone();
        newest_first.sort_by(|a, b| b.cmp(a));
        assert_eq!(keys, newest_first, "keys are not newest first");
    }

    #[tokio::test]
    async fn a_build_materializes_only_the_keys_it_targets() {
        let store = Store::open(":memory:").unwrap();
        let reg = AssetRegistry::new(vec![keyed("a")], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["a".into()], &on("a", ["k2"])).unwrap();
        let run = build_plan(&runner, plan).await;
        assert_eq!(run.status, RunStatus::Success);

        // one instance, named for its key rather than its index
        let ops = store.op_runs(&run.id).unwrap();
        let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
        assert_eq!(names, ["a[k2]"]);
        assert_eq!(
            store
                .materialization("a", Some("k2"))
                .unwrap()
                .unwrap()
                .value,
            Some(json!({"key": "k2"}))
        );
        for untouched in ["k1", "k3"] {
            assert!(
                store
                    .materialization("a", Some(untouched))
                    .unwrap()
                    .is_none(),
                "{untouched} was built without being asked for"
            );
        }
        // and nothing lands under the asset's unpartitioned name
        assert!(store.materialization("a", None).unwrap().is_none());

        // a key the set does not hold is refused rather than expanded
        let err = plan_partitions(&reg, &m, &["a".into()], &on("a", ["k9"])).unwrap_err();
        assert!(err.to_string().contains("no partition \"k9\""), "{err}");
    }

    #[tokio::test]
    async fn identity_mapping_reads_the_matching_upstream_partition() {
        let store = Store::open(":memory:").unwrap();
        let a = keyed("a");
        let b = Asset::new("b", |ctx: OpCtx| async move {
            let up = ctx.input("a").expect("the upstream partition");
            Ok(json!({"from": up["key"], "at": ctx.partition()}))
        })
        .from_named("a")
        .partitioned(Partitions::keys(["k1", "k2", "k3"]));
        let reg = AssetRegistry::new(vec![a, b], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();

        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["b".into()], &on("b", ["k2"])).unwrap();
        // the upstream key b needs comes along, and only that one
        assert_eq!(plan.seeds["partitions:a"], json!(["k2"]));
        let run = build_plan(&runner, plan).await;
        assert_eq!(run.status, RunStatus::Success);
        let ops = store.op_runs(&run.id).unwrap();
        let mut names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
        names.sort();
        assert_eq!(names, ["a[k2]", "b[k2]"]);
        assert_eq!(
            store
                .materialization("b", Some("k2"))
                .unwrap()
                .unwrap()
                .value,
            Some(json!({"from": "k2", "at": "k2"}))
        );
        // lineage records the fingerprint of the key it consumed, not the asset
        let a2 = store.materialization("a", Some("k2")).unwrap().unwrap();
        assert_eq!(
            store
                .materialization("b", Some("k2"))
                .unwrap()
                .unwrap()
                .inputs,
            json!({"a": a2.fingerprint})
        );

        // with a fresh upstream, the same build runs b alone and still reads a[k1]
        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["a".into()], &on("a", ["k1"])).unwrap();
        build_plan(&runner, plan).await;
        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["b".into()], &on("b", ["k1"])).unwrap();
        assert_eq!(
            plan.seeds["partitions:a"],
            json!([]),
            "a fresh upstream key was rebuilt anyway"
        );
        let run = build_plan(&runner, plan).await;
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(
            store
                .materialization("b", Some("k1"))
                .unwrap()
                .unwrap()
                .value,
            Some(json!({"from": "k1", "at": "k1"}))
        );
    }

    #[test]
    fn lineage_across_a_partition_boundary_is_identity_or_nothing() {
        let a = keyed("a");
        let flat = Asset::new("flat", |_| async { Ok(json!(null)) }).from_named("a");
        let err = reg_err(vec![a, flat]);
        assert!(
            err.to_string()
                .contains("it is not partitioned but its dep a is"),
            "{err}"
        );

        // two kinds of key set cannot agree on "the same key"
        let day = Asset::new("day", |_| async { Ok(json!(null)) })
            .partitioned(Partitions::daily("2026-01-01"));
        let hour = Asset::new("hour", |_| async { Ok(json!(null)) })
            .from_named("day")
            .partitioned(Partitions::hourly("2026-01-01T00"));
        let err = reg_err(vec![day, hour]);
        assert!(err.to_string().contains("partitioned hourly"), "{err}");

        // a probe fingerprints the whole of a source, so there is no key to be
        let err = reg_err(vec![
            Asset::source("s").partitioned(Partitions::keys(["k"])),
        ]);
        assert!(
            err.to_string().contains("source cannot be partitioned"),
            "{err}"
        );

        // partitioned on unpartitioned is the fine direction
        let s = Asset::source("s");
        AssetRegistry::new(vec![s, keyed("a")], Vec::new(), Vec::new()).unwrap();
    }

    #[tokio::test]
    async fn checks_metadata_and_history_all_key_on_the_partition() {
        let store = Store::open(":memory:").unwrap();
        let a = Asset::new("a", |ctx: OpCtx| async move {
            let key = ctx.partition().unwrap().to_string();
            ctx.meta("built", key.clone());
            Ok(json!({"key": key, "rows": 1}))
        })
        .partitioned(Partitions::keys(["k1", "k2", "k3"]));
        let reg =
            AssetRegistry::new(vec![a], Vec::new(), vec![rows_check("has_rows", "a", 1)]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["a".into()], &on("a", ["k2"])).unwrap();
        let run = build_plan(&runner, plan).await;
        assert_eq!(run.status, RunStatus::Success);

        // the check expanded over the same key and saw that key's value
        let ops = store.op_runs(&run.id).unwrap();
        let mut names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
        names.sort();
        assert_eq!(names, ["a[k2]", "check:a:has_rows[k2]"]);
        let results = store.asset_checks("a", None, 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition.as_deref(), Some("k2"));
        assert_eq!(results[0].status, CheckStatus::Passed);
        assert!(store.asset_checks("a", Some("k1"), 10).unwrap().is_empty());

        // metadata lands on the key's own materialization
        let m2 = store.materialization("a", Some("k2")).unwrap().unwrap();
        assert_eq!(m2.metadata, Some(json!({"built": {"text": "k2"}})));

        // history is per key: k2 twice, k1 never
        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["a".into()], &on("a", ["k2"])).unwrap();
        build_plan(&runner, plan).await;
        let history = store.materializations("a", Some("k2"), 10).unwrap();
        assert_eq!(history.len(), 2);
        // same value twice, so the second build is not a change
        assert_eq!(
            history.iter().map(|e| e.changed).collect::<Vec<bool>>(),
            [false, true]
        );
        assert!(
            store
                .materializations("a", Some("k1"), 10)
                .unwrap()
                .is_empty()
        );
        // and asking for every key interleaves them
        assert_eq!(store.materializations("a", None, 10).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn typed_assets_keep_their_type_names() {
        #[derive(serde::Deserialize)]
        struct In {
            #[allow(dead_code)]
            base: Value,
        }
        #[derive(serde::Serialize)]
        struct Out {
            n: u32,
        }
        let base = echo("base");
        let t = Asset::typed("t", |_ctx: OpCtx, _input: In| async { Ok(Out { n: 5 }) }).from(&base);
        let reg = AssetRegistry::new(vec![base, t], Vec::new(), Vec::new()).unwrap();
        let job = reg.lower_job().unwrap();
        let op = job.op("t").unwrap();
        assert!(op.input_type().unwrap().ends_with("In"));
        assert!(op.output_type().unwrap().ends_with("Out"));

        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([job], store.clone()).unwrap();
        build_all(&reg, &runner).await;
        assert_eq!(
            store.materialization("t", None).unwrap().unwrap().value,
            Some(json!({"n": 5}))
        );
    }
}
