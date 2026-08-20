use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::error::Error;
use crate::executor::Lineage;
use crate::graph;
use crate::job::Job;
use crate::model::{CheckStatus, Materialization, RunTags, Severity, Trigger};
use crate::op::{self, Meta, Op, OpCtx};
use crate::partition::{KeySet, PartitionMapping, Partitions, Reads};
use crate::policy::AutoPolicy;
use crate::schedule::Cron;
use crate::store::{Built, Store};

/// the internal job every asset build runs under.
pub(crate) const ASSETS_JOB: &str = "assets";

/// the external name a partitioned asset fans out over: the keys one build
/// targets, which only a [`BuildPlan`] can work out.
pub(crate) fn partition_keys_name(asset: &str) -> String {
    format!("partitions:{asset}")
}

/// the character a name uses to say which group it is in when nothing was
/// declared: everything before the first one names the group. one character,
/// and the one every catalog already uses.
pub(crate) const GROUP_SEPARATOR: char = '/';

/// where an asset belongs: the group it declared, else the part of its name
/// before the first [`GROUP_SEPARATOR`], else nothing.
///
/// an empty prefix is not a group. `/orders` has nothing before the separator
/// to name a group with, so it is ungrouped, which is what it has always been.
pub(crate) fn resolve_group<'a>(declared: Option<&'a str>, name: &'a str) -> Option<&'a str> {
    match declared {
        Some(group) => Some(group),
        None => name
            .split_once(GROUP_SEPARATOR)
            .map(|(prefix, _)| prefix)
            .filter(|prefix| !prefix.is_empty()),
    }
}

/// the hue a name is drawn in, 0..=359 degrees around the colour wheel.
///
/// **a pure function of the name and nothing else**, so a group keeps its
/// colour across restarts, across processes, across machines and across
/// however many other groups appear beside it. an index into a palette would
/// not: adding one group renumbers every group after it, and a graph that
/// repaints itself when somebody adds an asset is a graph nobody trusts the
/// colours of.
///
/// the number is a hue and not a colour. what lightness is legible depends on
/// the ground it is drawn on, and the server does not know the reader's theme,
/// so the reader picks saturation and lightness and this picks the angle. the
/// web ui does that in css; a terminal doing it in ansi gets the same angle.
///
/// sha-256 rather than a hasher out of `std`, because [`DefaultHasher`] is
/// documented as free to change between releases and is seeded per process:
/// either would repaint every graph, one on a toolchain upgrade and one on a
/// restart. the first eight bytes of the digest, big-endian, modulo 360.
///
/// **the limit, stated**: two names can hash close enough together to be hard
/// to tell apart, and no pure function of one name can prevent it, because
/// preventing it needs the whole set of names and a function of the whole set
/// is exactly the unstable thing above. `hestan doctor` reports the pairs it
/// finds rather than leaving them to be noticed on a screen, and
/// [`Asset::hue`] pins one of the two.
///
/// ```
/// // the same name, the same angle, wherever it is asked
/// assert_eq!(hestan::hue("warehouse"), hestan::hue("warehouse"));
/// assert!(hestan::hue("warehouse") <= 359);
/// ```
///
/// [`DefaultHasher`]: std::collections::hash_map::DefaultHasher
pub fn hue(name: &str) -> u16 {
    let digest = Sha256::digest(name.as_bytes());
    let head = u64::from_be_bytes(digest[..8].try_into().expect("sha-256 is 32 bytes"));
    (head % 360) as u16
}

/// the widest a hue may be, past which [`Asset::hue`] fails the build.
pub(crate) const MAX_HUE: u16 = 359;

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
    group: Option<String>,
    hue: Option<u16>,
    deps: Vec<String>,
    maps: BTreeMap<String, PartitionMapping>,
    op: Option<Op>,
    io: Option<String>,
    probe: Option<Arc<ProbeFn>>,
    probe_every: Duration,
    policy: Option<AutoPolicy>,
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
            group: None,
            hue: None,
            deps: Vec::new(),
            maps: BTreeMap::new(),
            op: None,
            io: None,
            probe: None,
            probe_every: Duration::from_secs(60),
            policy: None,
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
            group: None,
            hue: None,
            deps: Vec::new(),
            maps: BTreeMap::new(),
            io: None,
            probe: None,
            probe_every: Duration::from_secs(60),
            policy: None,
            retries: 0,
            retry_delay: None,
            partitions: None,
            fresh_within: None,
        }
    }

    /// a derived asset with typed io, the same machinery as [`Op::typed`]:
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
            group: None,
            hue: None,
            deps: Vec::new(),
            maps: BTreeMap::new(),
            io: None,
            probe: None,
            probe_every: Duration::from_secs(60),
            policy: None,
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

    /// declare lineage on an asset by name: the way to depend on one output
    /// of a [`MultiAsset`], which produces names rather than [`Asset`] values.
    /// a name nothing registers is a build error, exactly as with
    /// [`from`](Self::from).
    pub fn from_named(mut self, dep: impl Into<String>) -> Asset {
        self.deps.push(dep.into());
        self
    }

    /// declare lineage on another asset *and which of its keys* this asset's
    /// partitions read: a daily key
    /// [covering](PartitionMapping::covering) its 24 hours, yesterday's key at
    /// an [offset](PartitionMapping::offset), or
    /// [all](PartitionMapping::all) of them at once.
    ///
    /// ```no_run
    /// # use hestan::{Asset, OpCtx, PartitionMapping, Partitions};
    /// # use serde_json::json;
    /// # let hourly = Asset::new("hourly_traffic", |_: OpCtx| async { Ok(json!(null)) })
    /// #     .partitioned(Partitions::hourly("2026-01-01"));
    /// Asset::new("daily_traffic", |_: OpCtx| async { Ok(json!(null)) })
    ///     .reads(&hourly, PartitionMapping::covering())
    ///     .partitioned(Partitions::daily("2026-01-01"));
    /// ```
    ///
    /// [`from`](Self::from) is this with
    /// [`identity`](PartitionMapping::identity): the same key, which is what
    /// a dep between two partitioned assets meant before there was anything
    /// else it could mean. repeatable, and a mapping the two key sets could
    /// never resolve fails the build.
    pub fn reads(mut self, dep: &Asset, mapping: PartitionMapping) -> Asset {
        self.maps.insert(dep.name.clone(), mapping);
        self.deps.push(dep.name.clone());
        self
    }

    /// [`reads`](Self::reads) by name, as [`from_named`](Self::from_named) is
    /// [`from`](Self::from) by name.
    pub fn reads_named(mut self, dep: impl Into<String>, mapping: PartitionMapping) -> Asset {
        let dep = dep.into();
        self.maps.insert(dep.clone(), mapping);
        self.deps.push(dep);
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

    /// what it is made of, in declaration order: the same names
    /// [`from`](Asset::from) and [`from_named`](Asset::from_named) took.
    pub fn deps(&self) -> &[String] {
        &self.deps
    }

    /// the group this asset belongs to: one flat name, with nothing nested
    /// inside it and no separator parsed out of it.
    ///
    /// **the resolution order is the declared group, else the part of the name
    /// before the first `/`, else no group at all**, so a graph that never
    /// calls this groups exactly as it always did.
    ///
    /// declaring it is what makes regrouping cheap. the name is the key in
    /// every materialization, every lineage ref and every op run, so moving
    /// `sales/orders` into `finance` by renaming it starts the asset's history
    /// over; moving it with this leaves the history where it is.
    ///
    /// on a [source](Asset::source) the group names the external system the
    /// data stands for, which is what makes two tables out of one warehouse
    /// read as one origin downstream.
    ///
    /// ```
    /// # use hestan::Asset;
    /// let orders = Asset::source("orders").group("warehouse");
    /// let returns = Asset::source("returns").group("warehouse");
    /// let fx = Asset::source("fx_rates").group("vendor");
    /// # let _ = (orders, returns, fx);
    /// ```
    ///
    /// the build refuses an empty group, one containing `/`, and one that is
    /// the name of an ungrouped source, since an origin label is a group name
    /// falling back to a bare source name and the two would be one entry.
    pub fn group(mut self, name: impl Into<String>) -> Asset {
        self.group = Some(name.into());
        self
    }

    /// pin the [hue](crate::hue) this asset's label is drawn in, 0..=359
    /// degrees; outside that range fails the build.
    ///
    /// the label is the group where there is one and the asset's own name
    /// where there is not, so this pins a colour for a whole group and two
    /// assets in one group may not pin it to two different angles.
    ///
    /// worth reaching for in one case: `hestan doctor` named two labels whose
    /// hashed hues sit too close to tell apart, and one of them should move.
    /// pinning every group by hand is the palette-index failure the hash
    /// exists to avoid, only done by hand.
    pub fn hue(mut self, hue: u16) -> Asset {
        self.hue = Some(hue);
        self
    }

    /// store this asset's value through the [io manager](crate::IoManager)
    /// registered under `name` with `Hestan::io_named`, instead of the process
    /// default. naming one that was never registered fails the build, and so
    /// does naming one on a [source](Asset::source), which stores no value of
    /// its own.
    ///
    /// ```no_run
    /// # use hestan::{Asset, FileIo, Hestan, OpCtx};
    /// # use serde_json::json;
    /// let orders = Asset::new("orders", |_: OpCtx| async {
    ///     Ok(json!([{"id": 1, "total": 9.99}]))
    /// })
    /// .io("files");
    ///
    /// Hestan::new()
    ///     .io_named("files", FileIo::new("/var/lib/hestan/io"))
    ///     .assets([orders]);
    /// ```
    ///
    /// the materialization then records the handle the manager returned, and a
    /// later build that memoizes this asset reads the value back through the
    /// same manager rather than out of the run log, so an asset of rows can
    /// be stored as [parquet][parquet] and nothing downstream reads json.
    /// `docs/io-managers.md` says what that means for
    /// [retention](crate::Retention), which takes what a manager wrote when it
    /// prunes the run that wrote it.
    ///
    #[cfg_attr(feature = "parquet", doc = "[parquet]: crate::ParquetIo")]
    #[cfg_attr(not(feature = "parquet"), doc = "[parquet]: crate")]
    pub fn io(mut self, name: impl Into<String>) -> Asset {
        self.io = Some(name.into());
        self
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
    ///
    /// this is [`AutoPolicy::when_stale`] and always was; the policy is the
    /// same rule with the others beside it, and either spelling behaves
    /// identically.
    pub fn auto(self) -> Asset {
        self.policy(AutoPolicy::when_stale())
    }

    /// when hestan may rebuild this asset on its own: stale, never built, after
    /// a cron, and any of them held until everything the build reads is there.
    ///
    /// ```no_run
    /// # use hestan::{Asset, AutoPolicy, OpCtx};
    /// # use serde_json::json;
    /// Asset::new("daily_revenue", |_: OpCtx| async { Ok(json!(null)) })
    ///     .policy(AutoPolicy::after_cron("0 2 * * *"));
    /// ```
    ///
    /// the deciding process evaluates it, key by key on a
    /// [partitioned](Self::partitioned) asset, and builds what it says to
    /// build; `docs/assets.md` says what it does when one is already building
    /// and what it does with a rule that cannot be satisfied. declaring one on
    /// a [source](Self::source) is a build error: sources are probed, never
    /// built.
    pub fn policy(mut self, policy: AutoPolicy) -> Asset {
        self.policy = Some(policy);
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
    /// `{asset}[{key}]`. sources cannot be partitioned (a probe fingerprints
    /// the whole thing), and neither can a [`MultiAsset`], which has no
    /// `partitioned` at all.
    pub fn partitioned(mut self, partitions: Partitions) -> Asset {
        self.partitions = Some(partitions);
        self
    }

    /// declare how old this asset's latest materialization may get before the
    /// asset is late: `fresh_within(Duration::from_secs(3600))` says it should
    /// be rebuilt hourly, however that rebuild is triggered. on a [partitioned
    /// asset](Self::partitioned) the policy applies per key, so the asset is
    /// late as soon as any one key is; see
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
/// names: a missing or extra key fails the op and says which. it lowers to
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
    io: Option<String>,
    policy: Option<AutoPolicy>,
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
            io: None,
            policy: None,
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

    /// store what this op produces through the [io manager](crate::IoManager)
    /// registered under `name`, as [`Asset::io`] does: one output holding
    /// every asset, since one op returns it.
    ///
    /// each produced asset's materialization keeps its own slice of that
    /// output rather than a handle: the manager has one handle for the whole
    /// object and no part of it has one of its own.
    pub fn io(mut self, name: impl Into<String>) -> MultiAsset {
        self.io = Some(name.into());
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
    /// stale, which is [`policy`](Self::policy) with the stale rule.
    pub fn auto(self) -> MultiAsset {
        self.policy(AutoPolicy::when_stale())
    }

    /// when hestan may rebuild these on its own, as [`Asset::policy`]. the
    /// policy lands on every produced asset, since one op produces them all,
    /// and a pass that wants any of them runs that one op.
    pub fn policy(mut self, policy: AutoPolicy) -> MultiAsset {
        self.policy = Some(policy);
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
    /// saw: a check that reports the number it was satisfied by is worth more
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

    /// attach a typed fact to the result: the number that failed the
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
    /// the group [`Asset::group`] declared; `None` when nothing was declared,
    /// in which case the name prefix answers instead. kept as declared rather
    /// than resolved so `doctor` can see the two disagree.
    pub declared_group: Option<String>,
    /// the hue [`Asset::hue`] pinned on this asset's label; `None` when
    /// nothing was pinned, in which case [`hue`] answers from the label.
    pub declared_hue: Option<u16>,
    pub deps: Vec<String>,
    /// the [mapping](PartitionMapping) declared on each dep it reads, for the
    /// deps that declared one; everything else is identity.
    maps: BTreeMap<String, PartitionMapping>,
    /// when hestan rebuilds this on its own, from [`Asset::policy`]; `None`
    /// when nothing was declared, which is an asset only a person builds.
    pub policy: Option<AutoPolicy>,
    /// the parsed form of an [`AutoPolicy::after_cron`] rule, so an expression
    /// that does not resolve is a boot error rather than a pass at 2am, and the
    /// evaluator reads a parse rather than repeating one every minute.
    pub cron: Option<Cron>,
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
    /// where this asset came from: the source groups it descends from
    /// transitively, sorted by name, filled in by [`walk_provenance`] once the
    /// whole graph is in topo order. empty is a real answer, and it means no
    /// source is upstream of this asset at all.
    pub provenance: Vec<String>,
}

impl AssetMeta {
    /// where this asset belongs: what [`Asset::group`] declared, else the part
    /// of the name before the first `/`, else nothing.
    pub(crate) fn group(&self) -> Option<&str> {
        resolve_group(self.declared_group.as_deref(), &self.name)
    }

    /// the name this asset's colour hangs on: its group, or its own name where
    /// it is in no group. for a source that is exactly the label its origin is
    /// reported under, which is what makes one hue serve both channels.
    pub(crate) fn label(&self) -> &str {
        self.group().unwrap_or(&self.name)
    }

    /// which of `dep`'s keys one of this asset's partitions reads. identity
    /// unless the dep was declared with [`Asset::reads`], which is what makes
    /// every graph written before mappings existed read exactly as it did.
    pub(crate) fn mapping(&self, dep: &str) -> PartitionMapping {
        self.maps.get(dep).cloned().unwrap_or_default()
    }
}

/// one op of the lowered `assets` job and the assets it produces: one for a
/// plain [`Asset`], several for a [`MultiAsset`]. the registry is asset -> op
/// N:1, and this is the op side of it.
pub(crate) struct OpMeta {
    pub name: String,
    pub produces: Vec<String>,
    /// the assets this op reads, shared by everything it produces.
    pub deps: Vec<String>,
    /// how it reads each of them, for the deps that declared a mapping.
    maps: BTreeMap<String, PartitionMapping>,
    op: Op,
    /// the io manager this asset's value is stored through, from
    /// [`Asset::io`]; `None` for the process default.
    pub io: Option<String>,
    retries: u32,
    retry_delay: Option<Duration>,
    /// set only on a single-asset op: a multi-asset is never partitioned.
    pub partitions: Option<Partitions>,
}

impl OpMeta {
    /// which of `dep`'s keys this op reads, the same answer its assets give.
    pub(crate) fn mapping(&self, dep: &str) -> PartitionMapping {
        self.maps.get(dep).cloned().unwrap_or_default()
    }
}

/// how one dep asset reaches the op that reads it: the op that produces it
/// (which is the name the run knows it by), the key to take out of that op's
/// output when the producer is a multi-asset, and whether the dep is itself
/// partitioned, in which case its value is read per key from the store
/// rather than out of the run.
#[derive(Clone)]
struct DepLink {
    asset: String,
    op: String,
    key: Option<String>,
    /// the dep's own key set when it is partitioned, which is what a mapping
    /// resolves against; `None` for an unpartitioned dep, read whole.
    partitions: Option<Partitions>,
    /// which of those keys the reader takes.
    mapping: PartitionMapping,
    /// the manager the producer stores through, needed to read a partitioned
    /// dep's value back out of the store.
    io: Option<String>,
}

/// the validated asset graph, in topo order, with the checks bound to it.
/// built once by `Hestan::build`.
pub(crate) struct AssetRegistry {
    metas: Vec<AssetMeta>,
    by_name: HashMap<String, usize>,
    /// the labels somebody pinned a hue on, from [`Asset::hue`]. everything
    /// else answers from [`hue`], so this holds only the exceptions.
    hues: BTreeMap<String, u16>,
    ops: Vec<OpMeta>,
    by_op: HashMap<String, usize>,
    checks: Vec<CheckMeta>,
}

impl AssetRegistry {
    pub(crate) fn empty() -> AssetRegistry {
        AssetRegistry {
            metas: Vec::new(),
            by_name: HashMap::new(),
            hues: BTreeMap::new(),
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
            if a.source && a.policy.is_some() {
                return Err(Error::Graph(format!(
                    "asset {}: an automation policy on a source (sources are probed, \
                     never built)",
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
            if a.source && a.io.is_some() {
                return Err(Error::Graph(format!(
                    "asset {}: io on a source (a source has no value of its own to store)",
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
                    maps: a.maps.clone(),
                    op,
                    io: a.io,
                    retries: a.retries,
                    retry_delay: a.retry_delay,
                    partitions: a.partitions.clone(),
                });
            }
            let cron = match &a.policy {
                Some(policy) => policy.parse_cron(&a.name)?,
                None => None,
            };
            metas.push(AssetMeta {
                name: a.name.clone(),
                source: a.source,
                declared_group: a.group,
                declared_hue: a.hue,
                deps: a.deps,
                maps: a.maps,
                policy: a.policy,
                cron,
                probe: a.probe,
                probe_every: a.probe_every,
                op: (!a.source).then_some(a.name),
                partitions: a.partitions,
                fresh_within: a.fresh_within,
                provenance: Vec::new(),
            });
        }
        for m in multis {
            if m.produces.is_empty() {
                return Err(Error::Graph(format!(
                    "multi-asset {}: produces nothing; name its outputs with .produces([..])",
                    m.name
                )));
            }
            let cron = match &m.policy {
                Some(policy) => policy.parse_cron(&m.name)?,
                None => None,
            };
            for produced in &m.produces {
                metas.push(AssetMeta {
                    name: produced.clone(),
                    source: false,
                    // a multi-asset produces names rather than `Asset` values,
                    // so there is nowhere to declare a group on one of them;
                    // the name prefix is what answers for them
                    declared_group: None,
                    declared_hue: None,
                    deps: m.deps.clone(),
                    maps: BTreeMap::new(),
                    policy: m.policy.clone(),
                    cron: cron.clone(),
                    probe: None,
                    probe_every: Duration::from_secs(60),
                    op: Some(m.name.clone()),
                    partitions: None,
                    // a multi-asset produces names, not `Asset` values, so
                    // there is nowhere to hang a freshness policy on one of them
                    fresh_within: None,
                    provenance: Vec::new(),
                });
            }
            ops.push(OpMeta {
                name: m.name,
                produces: m.produces,
                deps: m.deps,
                maps: BTreeMap::new(),
                op: m.op,
                io: m.io,
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
        check_groups(&metas)?;
        let mut seen_ops: HashSet<&str> = HashSet::new();
        for o in &ops {
            // ops and assets share one namespace inside the job, so a
            // multi-asset named after an asset it does not produce collides
            // there, said before the duplicate check below, which would
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
        let mut ordered: Vec<AssetMeta> = order
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
        walk_provenance(&mut ordered, &by_name);
        let pinned = check_hues(&ordered)?;
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
            hues: pinned,
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

    /// the hue a label is drawn in: what somebody pinned on it, else what
    /// [`hue`] makes of the name. one answer, so the api, the ui and the
    /// command line cannot disagree about what colour a group is.
    pub(crate) fn hue_of(&self, label: &str) -> u16 {
        self.hues.get(label).copied().unwrap_or_else(|| hue(label))
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

    /// what the run knows `asset` by: the op that produces it, or (for a
    /// source, which has no op) the asset's own name, seeded as an external.
    fn producer(&self, asset: &str) -> String {
        match self.get(asset).and_then(|m| m.op.clone()) {
            Some(op) => op,
            None => asset.to_string(),
        }
    }

    /// how an op reads one of its dep assets, under the mapping the edge
    /// declared.
    fn dep_link(&self, asset: &str, mapping: PartitionMapping) -> DepLink {
        let op = self.producer(asset);
        // a multi-asset's output is one object keyed by what it produces, so a
        // dep on one of them is a key of it; everything else is the whole value
        let key = self
            .op(&op)
            .filter(|o| o.produces.len() > 1)
            .map(|_| asset.to_string());
        DepLink {
            io: self.op(&op).and_then(|o| o.io.clone()),
            asset: asset.to_string(),
            op,
            key,
            partitions: self.get(asset).and_then(|m| m.partitions.clone()),
            mapping,
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
    /// op (which is one asset, or all of a multi-asset's), one more per check
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
        // asset's key list seeds `[]`, so a full launch of the job (which
        // computes no plan and so no targets) expands it into nothing rather
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

/// the hues somebody pinned, and the two ways a pinned one is refused.
///
/// a hue is an angle, so a number that is not on the wheel is a mistake rather
/// than a value to wrap around: 400 could mean 40, and quietly meaning 40 is
/// how a typo becomes a colour nobody chose. and a hue is pinned on a label
/// rather than on an asset, so two assets in one group pinning two angles is a
/// question with no answer, said here rather than resolved by declaration
/// order.
fn check_hues(metas: &[AssetMeta]) -> Result<BTreeMap<String, u16>, Error> {
    let mut pinned: BTreeMap<String, u16> = BTreeMap::new();
    let mut by_whom: HashMap<&str, &str> = HashMap::new();
    for meta in metas {
        let Some(hue) = meta.declared_hue else {
            continue;
        };
        if hue > MAX_HUE {
            return Err(Error::Graph(format!(
                "asset {}: hue {hue} is not on the wheel, which is 0..={MAX_HUE} degrees",
                meta.name
            )));
        }
        let label = meta.label();
        match pinned.insert(label.to_string(), hue) {
            Some(held) if held != hue => {
                return Err(Error::Graph(format!(
                    "asset {}: hue {hue} on {label}, which {} already pinned to \
                     {held}. a hue belongs to the label rather than to one \
                     asset, so a group has one",
                    meta.name, by_whom[label]
                )));
            }
            _ => {
                by_whom.insert(label, &meta.name);
            }
        }
    }
    Ok(pinned)
}

/// where every asset came from, in one forward pass over the topo order.
///
/// a source contributes one label, its group where it has one and its own name
/// where it has none, and every other asset is the union of what its deps
/// contribute. one pass suffices because `metas` is already sorted so that a
/// dep is seen before anything that reads it, which is the order
/// [`graph::topo_order`] produced a few lines above.
///
/// **it is a forward pass and not a traversal.** each asset is visited once
/// and each edge is read once, over a graph the build is already walking. the
/// honest bound is not quite O(V+E): each edge copies a set rather than a
/// value, so it is O((V+E)·L) with L the number of distinct origin labels a
/// node can reach, and L is the number of source groups in the deployment.
/// `the_origin_pass_stays_flat_on_a_wide_graph` prints the number for 2040
/// assets and 8000 edges rather than leaving this as an assertion.
///
/// one implementation, because the api, the command line and `doctor` all want
/// the same answer and two would drift.
///
/// the labels are collected in a [`BTreeSet`], so what comes out is sorted by
/// name and identical between two calls. a set that reorders between requests
/// makes a swatch flicker.
fn walk_provenance(metas: &mut [AssetMeta], by_name: &HashMap<String, usize>) {
    let mut sets: Vec<BTreeSet<String>> = Vec::with_capacity(metas.len());
    for meta in metas.iter() {
        let mut from = BTreeSet::new();
        if meta.source {
            // a source is its own origin: the system its group names, or
            // itself where it names no system
            from.insert(meta.group().unwrap_or(&meta.name).to_string());
        } else {
            for dep in &meta.deps {
                // topo order, so the dep's own set is already final. a
                // partition mapping is not consulted: it says which keys a
                // read takes, and where the data came from is the same answer
                // at every key
                if let Some(&at) = by_name.get(dep) {
                    from.extend(sets[at].iter().cloned());
                }
            }
        }
        sets.push(from);
    }
    for (meta, from) in metas.iter_mut().zip(sets) {
        meta.provenance = from.into_iter().collect();
    }
}

/// what a declared group is allowed to be.
///
/// three refusals, and each is about something the group would otherwise be
/// drawn as: a name with nothing in it, a name with a separator in it that a
/// folded node would draw as nesting, and a name a legend entry could not
/// point at unambiguously.
fn check_groups(metas: &[AssetMeta]) -> Result<(), Error> {
    // the sources whose bare names an origin label falls back to, which is the
    // set a declared group must not walk into
    let bare: HashSet<&str> = metas
        .iter()
        .filter(|m| m.source && m.group().is_none())
        .map(|m| m.name.as_str())
        .collect();
    for meta in metas {
        let Some(group) = meta.declared_group.as_deref() else {
            continue;
        };
        if group.trim().is_empty() {
            return Err(Error::Graph(format!(
                "asset {}: declared group {group:?} has no name in it, and an asset in a group \
                 with no name is an asset in no group",
                meta.name
            )));
        }
        if group.contains(GROUP_SEPARATOR) {
            return Err(Error::Graph(format!(
                "asset {}: declared group {group:?} contains {GROUP_SEPARATOR:?}, and a folded \
                 group is drawn as {group}{GROUP_SEPARATOR}, which reads as nesting that is not \
                 there. a group is flat",
                meta.name
            )));
        }
        if bare.contains(group) {
            return Err(Error::Graph(format!(
                "asset {}: declared group {group} is also the name of the ungrouped source \
                 {group}. an origin label is a group name falling back to a bare source name, \
                 so one legend entry would point at both: give the source a group of its own, \
                 or rename one of the two",
                meta.name
            )));
        }
    }
    Ok(())
}

/// what lineage across a partition boundary is allowed to look like.
///
/// every dep carries a [mapping](PartitionMapping), identity unless
/// [`Asset::reads`] said otherwise, and a pairing the mapping could never
/// resolve is refused here rather than left to produce something plausible and
/// wrong: a day covering a set of keys that span no time, an offset along a
/// set with no order, a window on a dep whose keys are coarser than its own.
/// each refusal names both partitionings, since which two they are is the
/// whole of the answer.
fn check_partition_deps(metas: &[AssetMeta]) -> Result<(), Error> {
    let spec = |name: &str| {
        metas
            .iter()
            .find(|m| m.name == name)
            .and_then(|m| m.partitions.as_ref())
    };
    for meta in metas {
        for dep in &meta.deps {
            let mapping = meta.mapping(dep);
            let bad = |why: String| Err(Error::Graph(format!("asset {}: {why}", meta.name)));
            let Some(dep_spec) = spec(dep) else {
                // an unpartitioned dep has no keys to choose between: its whole
                // value arrives at every key, which is identity's other half
                if !mapping.is_identity() {
                    return bad(format!(
                        "it reads {dep} by {}, but {dep} is not partitioned at all. an \
                         unpartitioned dep has no keys to map onto: its whole value \
                         arrives at every key of {}",
                        mapping.label(),
                        meta.name
                    ));
                }
                continue;
            };
            let Some(own) = &meta.partitions else {
                // every key at once is what an unpartitioned consumer can mean;
                // nothing else resolves from a key it does not have
                if mapping.is_all() {
                    continue;
                }
                if !mapping.is_identity() {
                    return bad(format!(
                        "it is not partitioned, but reads its {} dep {dep} by {}. a \
                         mapping resolves from one key to another and {} has no key of \
                         its own: read every key with PartitionMapping::all, or \
                         partition {} too",
                        dep_spec.kind_label(),
                        mapping.label(),
                        meta.name,
                        meta.name
                    ));
                }
                return Err(Error::Graph(format!(
                    "asset {}: it is not partitioned but its dep {dep} is. reading every \
                     partition of {dep} at once is an aggregation, and hestan has no \
                     semantics for one yet: partition {} too, or aggregate inside the \
                     body from a source",
                    meta.name, meta.name
                )));
            };
            // all pairs any two key sets, which is the point of it
            let (own_kind, dep_kind) = (own.kind_label(), dep_spec.kind_label());
            if mapping.is_covering() {
                let (Some(mine), Some(theirs)) = (own.grain(), dep_spec.grain()) else {
                    return bad(format!(
                        "partitioned {own_kind}, covering its dep {dep}, partitioned \
                         {dep_kind}. a window covers a span of time, and a {} key set \
                         spans none",
                        match own.grain() {
                            None => own_kind,
                            Some(_) => dep_kind,
                        }
                    ));
                };
                if mine < theirs {
                    return bad(format!(
                        "partitioned {own_kind}, but it cannot cover its dep {dep}, \
                         partitioned {dep_kind}: one {own_kind} key sits inside one \
                         {dep_kind} key rather than the other way round"
                    ));
                }
            }
            if mapping.is_offset() {
                if !own.same_kind(dep_spec) {
                    return bad(format!(
                        "partitioned {own_kind}, but its dep {dep} is partitioned \
                         {dep_kind}. an offset steps along one kind of key, which two \
                         kinds cannot agree on"
                    ));
                }
                if !own.ordered() {
                    return bad(format!(
                        "partitioned {own_kind}, and so is its dep {dep}. an offset \
                         steps along the order of a key set, and a {own_kind} one is in \
                         the order it was written rather than one to step along"
                    ));
                }
            }
            if mapping.is_identity() && !own.same_kind(dep_spec) {
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

/// one asset's stored value, read back through the manager that stored it.
///
/// a materialization holds what the manager returned: a handle under a file
/// manager, the value itself under [`Inline`](crate::Inline), and the raw
/// value on any row written before an asset's value went through one. `get` is
/// total over all three, which is what makes an old row keep working.
async fn stored_value(
    ctx: &OpCtx,
    link: &DepLink,
    held: Value,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let key = crate::io::IoKey {
        run_id: ctx.run_id().to_string(),
        job: ASSETS_JOB.to_string(),
        op: link.op.clone(),
    };
    crate::io::get(&ctx.io, link.io.as_deref(), key, held)
        .await
        .map_err(|e| format!("could not read the value of {}: {e}", link.asset).into())
}

/// one key of a partitioned dep, out of the store and back through the manager
/// that stored it; `None` when nothing has materialized there.
async fn partition_value(
    ctx: &OpCtx,
    link: &DepLink,
    key: &str,
) -> Result<Option<Value>, Box<dyn std::error::Error + Send + Sync>> {
    let held = ctx
        .store
        .materialization(&link.asset, Some(key))?
        .and_then(|m| m.value);
    match held {
        Some(held) => Ok(Some(stored_value(ctx, link, held).await?)),
        None => Ok(None),
    }
}

/// the ctx an asset body sees: inputs keyed by the *asset* names it declared,
/// whatever ops the run actually ran to produce them, plus the partition key
/// this invocation is for. `ctx.input("orders")` reads the same inside an
/// asset whether `orders` has an op to itself or is one output of a
/// multi-asset.
///
/// a **partitioned** dep is read from the store, at whatever keys this
/// partition's [mapping](PartitionMapping) resolves to rather than out of the
/// run. that is what makes a mapping mean one thing: the consumer reads
/// `dep[k]` whether `dep[k]` was rebuilt by this run (its materialization is
/// written inside its own op, which has finished by now) or was already fresh
/// and never ran at all. what that row holds is what the manager returned, so
/// it is read back through the manager like any other input.
///
/// a mapping that names one key hands the body that key's value. one that
/// names a set hands it an object keyed by partition, holding the keys that
/// have materialized: an empty object rather than nothing when none have, so
/// "the set was empty" and "there is no such dep" stay different facts.
async fn with_dep_inputs(
    ctx: &OpCtx,
    links: &[DepLink],
    own: Option<&Partitions>,
    key: Option<&str>,
) -> Result<OpCtx, Box<dyn std::error::Error + Send + Sync>> {
    let mut inputs: HashMap<String, Value> = HashMap::new();
    let mut dep_statuses: HashMap<String, crate::model::OpStatus> = HashMap::new();
    for link in links {
        let value = match &link.partitions {
            Some(spec) => {
                let reads = match link.mapping.is_identity() {
                    true => Reads::at(key),
                    false => link.mapping.reads(own, key, &KeySet::of(spec)),
                };
                match link.mapping.reads_one() {
                    true => match reads.keys.first() {
                        Some(key) => partition_value(ctx, link, key).await?,
                        None => None,
                    },
                    false => {
                        let mut by_key = Map::new();
                        for key in reads.keys {
                            if let Some(v) = partition_value(ctx, link, &key).await? {
                                by_key.insert(key, v);
                            }
                        }
                        Some(Value::Object(by_key))
                    }
                }
            }
            None => dep_value(ctx, link),
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
/// partitioned dep read at one key, the whole asset for an unpartitioned one,
/// and (for a dep read through a mapping that names a set) one fingerprint
/// per key it consumed, as an object keyed by partition.
///
/// this is what staleness compares against, so a mapped read has to record
/// every key it read and not only the one that matches its own: a rollup that
/// recorded a day would report fresh while the hours under it moved.
fn dep_fingerprints(
    ctx: &OpCtx,
    links: &[DepLink],
    own: Option<&Partitions>,
    key: Option<&str>,
) -> Result<Value, Error> {
    let mut inputs = Map::new();
    for link in links {
        let fp = |at: Option<&str>| -> Result<Value, Error> {
            let fp = ctx
                .store
                .materialization(&link.asset, at)?
                .map(|m| m.fingerprint);
            Ok(fp.map(Value::String).unwrap_or(Value::Null))
        };
        let recorded = match &link.partitions {
            None => fp(None)?,
            Some(spec) => {
                let reads = match link.mapping.is_identity() {
                    true => Reads::at(key),
                    false => link.mapping.reads(own, key, &KeySet::of(spec)),
                };
                match link.mapping.reads_one() {
                    // one key arrives as one fingerprint, exactly as identity
                    // has always recorded it
                    true => match reads.keys.first() {
                        Some(at) => fp(Some(at))?,
                        None => Value::Null,
                    },
                    false => {
                        let mut by_key = Map::new();
                        for at in reads.keys {
                            let held = fp(Some(&at))?;
                            by_key.insert(at, held);
                        }
                        Value::Object(by_key)
                    }
                }
            }
        };
        inputs.insert(link.asset.clone(), recorded);
    }
    Ok(Value::Object(inputs))
}

/// what each produced asset's value is, out of the op's output. one asset is
/// the whole output; a multi-asset splits by key, and a key it did not return
/// (or one nothing declared) fails the op naming the discrepancy, because
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
// no materialization, and the next run rebuilds it: at-least-once, like op
// state
fn wrap_op(reg: &AssetRegistry, meta: &OpMeta) -> Op {
    let inner = meta.op.clone();
    let name = meta.name.clone();
    let produces = meta.produces.clone();
    let links: Vec<DepLink> = meta
        .deps
        .iter()
        .map(|d| reg.dep_link(d, meta.mapping(d)))
        .collect();
    // one entry per op the run knows, however many asset deps reach it
    let mut after: Vec<String> = Vec::new();
    for link in &links {
        if !after.contains(&link.op) {
            after.push(link.op.clone());
        }
    }
    let own = meta.partitions.clone();
    let mut op = Op::new(name.clone(), move |ctx: OpCtx| {
        let inner = inner.clone();
        let name = name.clone();
        let produces = produces.clone();
        let links = links.clone();
        let own = own.clone();
        async move {
            // on a partitioned asset this op is one fan-out instance, and the
            // element it was handed is the key it is for
            let key = match own.is_some() {
                false => None,
                true => Some(partition_of(&ctx)?),
            };
            let inner_ctx = with_dep_inputs(&ctx, &links, own.as_ref(), key.as_deref()).await?;
            let output = inner.call(inner_ctx).await?;
            let values = split_output(&name, &produces, &output)?;
            // deps' current fingerprints: ancestors in this run already wrote
            // theirs. one entry per dep asset, not per op, so lineage reads in
            // the names the asset graph uses
            let inputs = dep_fingerprints(&ctx, &links, own.as_ref(), key.as_deref())?;
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
                    // one asset is its op's whole output, so the executor
                    // records the handle that output got and the value is
                    // stored once. a multi-asset's slice has no handle of its
                    // own and is recorded as the value it is
                    value: (produces.len() > 1).then(|| value.clone()),
                    is_output: produces.len() == 1,
                    meta,
                });
            }
            Ok(output)
        }
    })
    .after(after)
    .retries(meta.retries);
    if let Some(name) = &meta.io {
        op = op.io(name);
    }
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
    // a check reads the asset it checks at its own key, whatever mappings the
    // asset itself declared upstream
    let link = reg.dep_link(&meta.asset, PartitionMapping::identity());
    let producer = link.op.clone();
    let check = meta.name.clone();
    let severity = meta.severity;
    let f = meta.f.clone();
    // a check on a partitioned asset expands the same way the asset does, over
    // the same keys: one check per partition, on the value that partition
    // just produced
    let partitioned = link.partitions.is_some();
    let op = Op::new(check_op_name(&meta.asset, &meta.name), move |ctx: OpCtx| {
        let (asset, check, f, link) = (asset.clone(), check.clone(), f.clone(), link.clone());
        async move {
            let key = match partitioned {
                false => None,
                true => Some(partition_of(&ctx)?),
            };
            // the value this key just produced, read back the way its consumer
            // reads it: the row holds what the manager returned, not the rows
            // themselves
            let value = match &key {
                Some(key) => match ctx
                    .store
                    .materialization(&asset, Some(key))?
                    .and_then(|m| m.value)
                {
                    Some(held) => stored_value(&ctx, &link, held).await?,
                    None => Value::Null,
                },
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
    /// when one of its keys is, which is why `reasons` is empty here: the
    /// evidence lives per key.
    pub parts: BTreeMap<String, Staleness>,
}

/// why an asset is stale: dep's fingerprint when this asset last consumed it
/// (`had`) vs the dep's current one (`now`). equal fingerprints appear when
/// the dep itself is stale: staleness propagates ahead of rebuilds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleReason {
    pub dep: String,
    /// which of the dep's keys this is about, when the dep was read through a
    /// [mapping](PartitionMapping) that reads a key other than this asset's
    /// own: the hour that moved under a daily rollup, rather than the day.
    /// `None` under identity, where the key is the reader's own.
    pub partition: Option<String>,
    pub had: Option<String>,
    pub now: Option<String>,
}

/// staleness for every asset, keyed by name: stale if it never materialized, if
/// a dep's fingerprint moved or went missing, or if a dep is itself stale.
/// computed in topo order, so staleness propagates before anything rebuilds.
///
/// a partitioned asset is judged one key at a time, and is stale as a whole
/// when any of its keys is. a partitioned dep is read at whatever keys the
/// edge's [mapping](PartitionMapping) resolves to (the same key under
/// identity), and an unpartitioned one whole, which is why a probe moving a
/// source's fingerprint makes every partition of every descendant stale at
/// once.
pub(crate) fn staleness(reg: &AssetRegistry, mats: &Mats) -> HashMap<String, Staleness> {
    let sets = key_sets(reg);
    let mut out: HashMap<String, Staleness> = HashMap::new();
    for meta in reg.topo() {
        let s = match sets.get(&meta.name) {
            None => one_staleness(mats, &sets, meta, &out, None),
            Some(set) => {
                let parts: BTreeMap<String, Staleness> = set
                    .keys()
                    .iter()
                    .map(|key| {
                        let s = one_staleness(mats, &sets, meta, &out, Some(key));
                        (key.clone(), s)
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

/// every partitioned asset's keys, worked out once. a mapping resolves against
/// what its dep holds, and asking the dep one key at a time would rebuild its
/// whole set for every key of every consumer.
pub(crate) fn key_sets(reg: &AssetRegistry) -> HashMap<String, KeySet> {
    reg.topo()
        .filter_map(|m| Some((m.name.clone(), KeySet::of(m.partitions.as_ref()?))))
        .collect()
}

/// one verdict: the whole of an unpartitioned asset, or one key of a
/// partitioned one. `done` holds the verdicts of everything upstream, which
/// topo order guarantees is already there.
fn one_staleness(
    mats: &Mats,
    sets: &HashMap<String, KeySet>,
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
    // whether the dep's key `at` has moved since this build consumed the
    // fingerprint `had`, and what to say about it if it has
    let moved = |dep: &str, at: Option<&str>, had: Option<String>| {
        let now = mats.get(dep, at).map(|m| m.fingerprint.clone());
        let dep_stale = match (done.get(dep), at) {
            // a key the dep's own set does not hold can never be fresh: there
            // is nothing there to read
            (Some(s), Some(key)) => s.parts.get(key).is_none_or(|s| s.stale),
            (Some(s), None) => s.stale,
            (None, _) => true,
        };
        (dep_stale || now.is_none() || had != now).then_some((had, now))
    };
    let mut reasons = Vec::new();
    for dep in &meta.deps {
        let mapping = meta.mapping(dep);
        let held = mat.inputs.get(dep);
        let Some(set) = sets.get(dep).filter(|_| !mapping.is_identity()) else {
            // identity, and every dep that has no keys to map: the same key of
            // a partitioned dep, the whole of an unpartitioned one, which is
            // what a dep meant before there were mappings
            let at = key.filter(|_| sets.contains_key(dep));
            let had = held.and_then(Value::as_str).map(String::from);
            if let Some((had, now)) = moved(dep, at, had) {
                reasons.push(StaleReason {
                    dep: dep.clone(),
                    partition: None,
                    had,
                    now,
                });
            }
            continue;
        };
        // a mapped read consumed a fingerprint per key, so the same question is
        // asked of every key it read, and of every key it wanted and the dep
        // does not hold, which can never be fresh either. one reason per dep
        // still: the first key that moved is what made this one stale, and a
        // window of ten thousand is not a list anybody reads
        let reads = mapping.reads(meta.partitions.as_ref(), key, set);
        let culprit = reads
            .keys
            .iter()
            .chain(reads.missing.iter())
            .find_map(|up| {
                let had = held
                    .and_then(|v| v.get(up))
                    .and_then(Value::as_str)
                    .map(String::from);
                moved(dep, Some(up), had).map(|(had, now)| StaleReason {
                    dep: dep.clone(),
                    partition: Some(up.clone()),
                    had,
                    now,
                })
            });
        reasons.extend(culprit);
    }
    Staleness {
        stale: !reasons.is_empty(),
        reasons,
        parts: BTreeMap::new(),
    }
}

/// what one build run executes: materializing ops in topo order with their
/// check ops, plus seeds for everything they read that won't run: stored
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
/// run knows it by: a source is null, an asset with an op to itself is what its
/// materialization holds, and a multi-asset is the object its op returns:
/// every asset it produces, so that whichever key a consumer reads is there.
///
/// what a materialization holds is what the [manager](crate::IoManager)
/// returned, so this seeds a handle where the value lives in one and the run
/// resolves it exactly as it resolves an output of its own. that is also why
/// nothing here has to know which kind a row is: the manager passes through
/// anything it did not write.
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
///
/// a key whose [window](PartitionMapping::covering) reaches past what its dep
/// holds is not in the set: nothing could materialize it, and a default build
/// that targeted it would refuse every time rather than build the rest.
fn default_keys(
    sets: &HashMap<String, KeySet>,
    meta: &AssetMeta,
    spec: &Partitions,
    verdict: &Staleness,
) -> Vec<String> {
    let mut keys: Vec<String> = verdict
        .parts
        .iter()
        .filter(|(key, s)| s.stale && unheld(sets, meta, key).is_none())
        .map(|(key, _)| key.clone())
        .collect();
    keys.reverse(); // parts are oldest first; a build wants the newest
    keys.truncate(spec.limit());
    keys
}

/// the first key one partition of `meta` promises to read that its dep does not
/// hold, as `(dep, key)`. a [window](PartitionMapping::covering) is the only
/// mapping that promises one: identity names a key whose absence is a dep that
/// never arrives, and an offset off the end of a set reads nothing on purpose.
fn unheld(sets: &HashMap<String, KeySet>, meta: &AssetMeta, key: &str) -> Option<(String, String)> {
    for dep in &meta.deps {
        let mapping = meta.mapping(dep);
        let Some(set) = sets.get(dep).filter(|_| !mapping.is_identity()) else {
            continue;
        };
        let reads = mapping.reads(meta.partitions.as_ref(), Some(key), set);
        if let Some(missing) = reads.missing.first() {
            return Some((dep.clone(), missing.clone()));
        }
    }
    None
}

/// which keys each partitioned op in the plan will build.
///
/// walked from the sinks up, because a mapping runs that way: an upstream
/// partitioned asset has to cover every key its consumers are about to read
/// (its own key under identity, the 24 hours under a daily window), and only
/// the keys of *its* that are actually stale are worth rebuilding. a target
/// with no keys named takes its default set; anything upstream takes what its
/// consumers need.
fn key_targets(
    reg: &AssetRegistry,
    stale: &HashMap<String, Staleness>,
    ops: &[String],
    targets: &[String],
    named: &HashMap<String, Vec<String>>,
) -> HashMap<String, Vec<String>> {
    let sets = key_sets(reg);
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
        let own = reg.get(asset).expect("a planned op produces its asset");
        let mut want: Vec<String> = match named.get(asset) {
            Some(explicit) => explicit.clone(),
            None if targets.contains(asset) => default_keys(&sets, own, spec, &stale[asset]),
            None => Vec::new(),
        };
        // every key a consumer downstream will read, that this asset owes
        for consumer in reg.ops() {
            if !in_plan.contains(consumer.name.as_str()) || !consumer.deps.contains(asset) {
                continue;
            }
            let mapping = consumer.mapping(asset);
            let set = sets.get(asset);
            // an unpartitioned consumer makes one read, at no key of its own,
            // which only a mapping over every key resolves to anything
            let reading: Vec<Option<&str>> = match consumer.partitions.is_some() {
                false => vec![None],
                true => keys
                    .get(&consumer.name)
                    .into_iter()
                    .flatten()
                    .map(|k| Some(k.as_str()))
                    .collect(),
            };
            for key in reading {
                // what that read takes of this asset: the consumer's own key
                // under identity, and whatever the mapping resolves to otherwise
                let reads = match (set, mapping.is_identity()) {
                    (Some(set), false) => mapping.reads(consumer.partitions.as_ref(), key, set),
                    _ => Reads::at(key),
                };
                for up in reads.keys {
                    let owed = stale[asset].parts.get(&up).is_none_or(|s| s.stale);
                    if owed && !want.contains(&up) {
                        want.push(up);
                    }
                }
            }
        }
        // a key this asset cannot read its own deps at is not one it can build,
        // however it got into the list
        want.retain(|key| unheld(&sets, own, key).is_none());
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
/// every target itself. one plan means one run: overlapping per-target plans
/// would each re-run the shared ancestors. errors on an unknown or source target.
pub(crate) fn plan_targets(
    reg: &AssetRegistry,
    mats: &Mats,
    targets: &[String],
) -> Result<BuildPlan, Error> {
    plan_partitions(reg, mats, targets, &HashMap::new())
}

/// [`plan_targets`] with the partitions of some targets named outright rather
/// than defaulted: what `POST /api/assets/{name}/build` with a `partitions`
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
/// whatever the caller can say that `Trigger::Build` cannot: which asset it
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
        Lineage::None,
        tags,
        None,
    )
}

/// launch a build of one asset: it, plus whatever upstream of it is stale, as
/// one run.
///
/// `Ok(None)` is an asset that is already up to date and had nothing to do,
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
            check_named_keys(reg, name, keys)?;
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

/// what one key of a partitioned asset reads of one mapped dep.
pub(crate) struct MappedRead {
    pub dep: String,
    pub mapping: String,
    /// the dep keys it resolves to, oldest first.
    pub keys: Vec<String>,
    /// how many it promised that the dep does not hold, which is what makes a
    /// key unbuildable rather than merely unbuilt.
    pub missing: usize,
}

/// what each of `keys` reads of the deps it maps, for the api and the grid.
/// deps read at the same key are left out: identity says nothing the key does
/// not say itself.
pub(crate) fn mapped_reads(
    reg: &AssetRegistry,
    asset: &str,
    keys: &[String],
) -> HashMap<String, Vec<MappedRead>> {
    let Some(meta) = reg.get(asset) else {
        return HashMap::new();
    };
    let mapped: Vec<(&String, PartitionMapping)> = meta
        .deps
        .iter()
        .map(|dep| (dep, meta.mapping(dep)))
        .filter(|(_, m)| !m.is_identity())
        .collect();
    if mapped.is_empty() {
        return HashMap::new();
    }
    let sets = key_sets(reg);
    keys.iter()
        .map(|key| {
            let reads = mapped
                .iter()
                .filter_map(|(dep, mapping)| {
                    let set = sets.get(dep.as_str())?;
                    let reads = mapping.reads(meta.partitions.as_ref(), Some(key), set);
                    Some(MappedRead {
                        dep: dep.to_string(),
                        mapping: mapping.label(),
                        keys: reads.keys,
                        missing: reads.missing.len(),
                    })
                })
                .collect();
            (key.clone(), reads)
        })
        .collect()
}

/// what naming a key outright is refused for: a partition whose
/// [window](PartitionMapping::covering) reaches a key its dep does not hold.
/// nothing could ever materialize it (a window is its whole range or it is a
/// different number), so the answer is which key is missing, rather than a
/// rollup of the part that happened to be there.
///
/// both places a caller names keys come through here: a build of named
/// partitions and the range a [backfill](crate::Hestan) resolves. a build that
/// names none of them leaves such keys out of its target set instead, since
/// refusing the whole build for one key it never asked for would leave the
/// asset unbuildable.
pub(crate) fn check_named_keys(
    reg: &AssetRegistry,
    asset: &str,
    keys: &[String],
) -> Result<(), Error> {
    let Some(meta) = reg.get(asset) else {
        return Ok(());
    };
    let sets = key_sets(reg);
    for key in keys {
        if let Some((dep, missing)) = unheld(&sets, meta, key) {
            return Err(Error::Graph(format!(
                "asset {asset}: partition {key:?} reads {dep}[{missing}], which is not \
                 one of {dep}'s partitions. a window covers its whole range or it is a \
                 different number"
            )));
        }
    }
    Ok(())
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
        assert!(err.to_string().contains("policy on a source"), "{err}");

        let err = reg_err(vec![Asset::source("s").io("parquet")]);
        assert!(err.to_string().contains("io on a source"), "{err}");
    }

    // ------------------------------------------------------------ groups

    // the whole point of declaring a group: the name is the key in every
    // materialization row, so regrouping by renaming starts the history over
    // and regrouping by declaring does not
    #[tokio::test]
    async fn moving_a_group_keeps_the_history_and_renaming_into_one_loses_it() {
        let store = Store::open(":memory:").unwrap();
        let reg = AssetRegistry::new(vec![echo("sales/orders")], Vec::new(), Vec::new()).unwrap();
        // as it groups today, with nothing declared
        assert_eq!(reg.get("sales/orders").unwrap().group(), Some("sales"));
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        assert_eq!(build_all(&reg, &runner).await.status, RunStatus::Success);
        let before = store
            .materialization("sales/orders", None)
            .unwrap()
            .expect("a build recorded one");

        // moved to another group, name untouched
        let moved = AssetRegistry::new(
            vec![echo("sales/orders").group("finance")],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        assert_eq!(moved.get("sales/orders").unwrap().group(), Some("finance"));
        let after = store
            .materialization("sales/orders", None)
            .unwrap()
            .expect("still there");
        assert_eq!(after.fingerprint, before.fingerprint);
        assert_eq!(after.built_at, before.built_at);
        assert_eq!(
            store
                .materializations("sales/orders", None, 10)
                .unwrap()
                .len(),
            1
        );

        // the same move made by renaming: the group is right and the past is
        // gone, which is the thing declaring a group is for
        let renamed =
            AssetRegistry::new(vec![echo("finance/orders")], Vec::new(), Vec::new()).unwrap();
        assert_eq!(
            renamed.get("finance/orders").unwrap().group(),
            Some("finance")
        );
        assert!(
            store
                .materialization("finance/orders", None)
                .unwrap()
                .is_none()
        );
    }

    // every deployment that never calls `group` has to group exactly as it did
    #[test]
    fn a_name_with_no_declaration_groups_by_its_prefix_as_it_always_has() {
        let group = |name: &str| {
            let reg = AssetRegistry::new(vec![echo(name)], Vec::new(), Vec::new()).unwrap();
            reg.get(name).unwrap().group().map(str::to_string)
        };
        assert_eq!(group("sales/orders").as_deref(), Some("sales"));
        // only the first separator names the group: deeper ones are inside it
        assert_eq!(group("sales/eu/orders").as_deref(), Some("sales"));
        assert_eq!(group("heartbeat"), None);
        // nothing before the separator is nothing to name a group with
        assert_eq!(group("/orders"), None);
        // and a name that ends in one still names a group, as the prefix
        // read has always given it
        assert_eq!(group("orders/").as_deref(), Some("orders"));
    }

    #[test]
    fn a_declared_group_wins_over_the_prefix_in_the_name() {
        let reg = AssetRegistry::new(
            vec![echo("sales/orders").group("finance")],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        let meta = reg.get("sales/orders").unwrap();
        assert_eq!(meta.group(), Some("finance"));
        // and the name is untouched, which is what the store is keyed by
        assert_eq!(meta.name, "sales/orders");
        // the declaration is kept as declared, so a tool can see the two
        // disagree rather than only the answer
        assert_eq!(meta.declared_group.as_deref(), Some("finance"));
    }

    #[test]
    fn a_group_that_could_not_be_drawn_fails_the_build_naming_both() {
        let empty = reg_err(vec![echo("daily").group("")]);
        let said = empty.to_string();
        assert!(
            said.contains("daily") && said.contains("no name in it"),
            "{said}"
        );
        let blank = reg_err(vec![echo("daily").group("   ")]).to_string();
        assert!(
            blank.contains("daily") && blank.contains("no name in it"),
            "{blank}"
        );

        // a folded group is drawn as "a/b/", which reads as nesting there is
        // no such thing as
        let nested = reg_err(vec![echo("daily").group("a/b")]).to_string();
        assert!(
            nested.contains("daily") && nested.contains("a/b"),
            "{nested}"
        );

        // an origin label falls back to a bare source name, so a group called
        // after one would be a legend entry pointing at two things
        let clash = reg_err(vec![Asset::source("orders"), echo("daily").group("orders")]);
        let said = clash.to_string();
        assert!(said.contains("daily") && said.contains("orders"), "{said}");
        // the same source in a group of its own is no longer bare, and the
        // name is free again
        AssetRegistry::new(
            vec![
                Asset::source("orders").group("warehouse"),
                echo("daily").group("orders"),
            ],
            Vec::new(),
            Vec::new(),
        )
        .expect("no collision left");
    }

    // ------------------------------------------------------------- hues

    // **the point of the stability claim.** these numbers are what somebody's
    // graph is painted with, so a refactor of the hash is a repaint of every
    // deployment, and it has to be a decision rather than a side effect. if
    // this fails, the hash changed and everybody's colours changed with it.
    #[test]
    fn a_name_hashes_to_the_same_angle_it_always_has() {
        assert_eq!(hue("sales"), 1);
        assert_eq!(hue("finance"), 216);
        assert_eq!(hue("marketing"), 120);
        assert_eq!(hue("warehouse"), 274);
        assert_eq!(hue("vendor"), 105);
        assert_eq!(hue("orders"), 268);
        assert_eq!(hue("fx_rates"), 357);
        assert_eq!(hue("heartbeat"), 141);
        // and it is a function, so asking twice is asking once
        assert_eq!(hue("sales"), hue("sales"));
        // every answer is on the wheel, including the empty name
        for name in ["", "a", "warehouse", "a much longer label than that one"] {
            assert!(hue(name) <= MAX_HUE, "{name} hashed off the wheel");
        }
        // adding a group does not move any other group, which an index into a
        // palette could not promise
        assert_eq!(hue("sales"), 1);
    }

    #[test]
    fn a_pinned_hue_is_refused_off_the_wheel_and_when_two_assets_disagree() {
        let off = reg_err(vec![echo("daily").group("finance").hue(360)]).to_string();
        assert!(off.contains("daily") && off.contains("360"), "{off}");
        assert!(off.contains("0..=359"), "{off}");

        // 359 is the last one that is on it
        AssetRegistry::new(
            vec![echo("daily").group("finance").hue(359)],
            Vec::new(),
            Vec::new(),
        )
        .expect("359 is on the wheel");

        // a hue belongs to the label, so a group cannot have two
        let clash = reg_err(vec![
            echo("daily").group("finance").hue(10),
            echo("weekly").group("finance").hue(200),
        ])
        .to_string();
        assert!(
            clash.contains("weekly") && clash.contains("daily"),
            "{clash}"
        );
        assert!(clash.contains("finance"), "{clash}");
        // a message somebody reads at a build failure, so it reads as prose:
        // a wrapped literal that loses its continuation leaves a run of
        // spaces, which no assertion on a substring would ever notice
        assert!(!clash.contains("  "), "gap in the message: {clash}");
        // the same angle twice is one answer, not a disagreement
        AssetRegistry::new(
            vec![
                echo("daily").group("finance").hue(10),
                echo("weekly").group("finance").hue(10),
            ],
            Vec::new(),
            Vec::new(),
        )
        .expect("agreeing is not a clash");
    }

    #[test]
    fn a_pinned_hue_answers_for_the_whole_label_and_the_rest_hash() {
        let orders = Asset::source("orders").group("warehouse").hue(30);
        let daily = echo("sales/daily").from(&orders);
        let loose = echo("heartbeat");
        let reg = AssetRegistry::new(vec![orders, daily, loose], Vec::new(), Vec::new()).unwrap();
        // the label the pin was on, which is both a group and an origin
        assert_eq!(reg.hue_of("warehouse"), 30);
        // and everything else is the hash of its own name
        assert_eq!(reg.hue_of("sales"), hue("sales"));
        assert_eq!(reg.hue_of("heartbeat"), hue("heartbeat"));
    }

    // ------------------------------------------------------- provenance

    fn origins(reg: &AssetRegistry, name: &str) -> Vec<String> {
        reg.get(name).unwrap().provenance.clone()
    }

    #[test]
    fn a_diamond_names_both_the_sources_it_descends_from_once_each() {
        let orders = Asset::source("orders").group("warehouse");
        let returns = Asset::source("returns").group("warehouse");
        let fx = Asset::source("fx_rates").group("vendor");
        let priced = echo("priced").from(&orders).from(&fx);
        let netted = echo("netted").from(&returns).from(&fx);
        let margin = echo("margin").from(&priced).from(&netted);
        let reg = AssetRegistry::new(
            vec![orders, returns, fx, priced, netted, margin],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();

        // two tables out of one warehouse are one origin, which is the whole
        // reason a source's group names the system rather than the table
        assert_eq!(origins(&reg, "orders"), ["warehouse"]);
        assert_eq!(origins(&reg, "returns"), ["warehouse"]);
        assert_eq!(origins(&reg, "fx_rates"), ["vendor"]);
        assert_eq!(origins(&reg, "priced"), ["vendor", "warehouse"]);
        // the diamond reaches the warehouse down both sides and says it once
        assert_eq!(origins(&reg, "margin"), ["vendor", "warehouse"]);
    }

    #[test]
    fn an_origin_carries_ten_hops_down_and_an_ungrouped_source_is_its_own_name() {
        let root = Asset::source("root");
        let mut assets = vec![root];
        for i in 0..10 {
            let dep = assets.last().unwrap().name().to_string();
            assets.push(echo(&format!("step{i}")).from_named(dep));
        }
        let reg = AssetRegistry::new(assets, Vec::new(), Vec::new()).unwrap();
        // a source in no group contributes its own name, so the label at the
        // far end names the thing it actually came from
        assert_eq!(origins(&reg, "root"), ["root"]);
        assert_eq!(origins(&reg, "step9"), ["root"]);
    }

    #[test]
    fn an_asset_with_no_source_upstream_has_an_empty_origin_and_that_is_an_answer() {
        let seed = echo("seed");
        let leaf = echo("leaf").from(&seed);
        let reg = AssetRegistry::new(vec![seed, leaf], Vec::new(), Vec::new()).unwrap();
        assert!(origins(&reg, "seed").is_empty());
        assert!(origins(&reg, "leaf").is_empty());
    }

    // a set that reorders between two requests makes a swatch flicker, so the
    // order is part of the answer rather than a detail of the container
    #[test]
    fn the_origin_order_is_by_name_and_the_same_every_time() {
        let build = || {
            let zulu = Asset::source("zulu").group("zeta");
            let alpha = Asset::source("alpha").group("alpha");
            let mid = Asset::source("mid").group("mid");
            let sink = echo("sink").from(&zulu).from(&mid).from(&alpha);
            AssetRegistry::new(vec![zulu, mid, alpha, sink], Vec::new(), Vec::new()).unwrap()
        };
        let once = origins(&build(), "sink");
        let twice = origins(&build(), "sink");
        assert_eq!(once, ["alpha", "mid", "zeta"]);
        assert_eq!(once, twice);
    }

    // a mapping says which keys a read takes; where the data came from is the
    // same answer at every key
    #[test]
    fn a_mapped_partition_edge_changes_nothing_about_where_the_data_came_from() {
        let raw = Asset::source("raw").group("warehouse");
        let hours = hourly("hours").from(&raw);
        let rollup = daily("rollup", "hours", PartitionMapping::covering()).from(&raw);
        let identity = Asset::new("mirror", |_| async { Ok(json!(null)) })
            .from_named("hours")
            .partitioned(Partitions::hourly("2026-01-01T00"));
        let reg =
            AssetRegistry::new(vec![raw, hours, rollup, identity], Vec::new(), Vec::new()).unwrap();
        assert_eq!(origins(&reg, "rollup"), ["warehouse"]);
        assert_eq!(origins(&reg, "mirror"), origins(&reg, "rollup"));
    }

    // the honest version of "it costs nothing": a wide graph, timed. this
    // rules out a pass that walks upward from every node, which is what a
    // second traversal would have been; it does not measure the constant
    #[test]
    fn the_origin_pass_stays_flat_on_a_wide_graph() {
        // 40 sources, then 8 layers of 250, each reading four of the layer
        // below: 2040 assets and 8000 edges
        let (sources, layers, wide, fan) = (40, 8, 250, 4);
        let mut assets: Vec<Asset> = (0..sources)
            .map(|i| Asset::source(format!("src{i}")).group(format!("system{i}")))
            .collect();
        for layer in 0..layers {
            for i in 0..wide {
                let mut node = echo(&format!("n{layer}_{i}"));
                for f in 0..fan {
                    let at = (i * fan + f) % if layer == 0 { sources } else { wide };
                    node = node.from_named(match layer {
                        0 => format!("src{at}"),
                        _ => format!("n{}_{at}", layer - 1),
                    });
                }
                assets.push(node);
            }
        }
        let count = assets.len();
        let whole = std::time::Instant::now();
        let reg = AssetRegistry::new(assets, Vec::new(), Vec::new()).unwrap();
        let whole = whole.elapsed();
        assert_eq!(count, 2040);
        // every sink reaches every system through eight layers of fan-in
        assert_eq!(reg.get("n7_0").unwrap().provenance.len(), sources);

        // and the pass on its own, over the same graph, which is the number
        // the claim is about. it is run a second time on an already-filled
        // registry, so this also says the pass is idempotent
        let AssetRegistry {
            mut metas, by_name, ..
        } = reg;
        let before: Vec<Vec<String>> = metas.iter().map(|m| m.provenance.clone()).collect();
        let pass = std::time::Instant::now();
        walk_provenance(&mut metas, &by_name);
        let pass = pass.elapsed();
        let after: Vec<Vec<String>> = metas.iter().map(|m| m.provenance.clone()).collect();
        assert_eq!(before, after);
        println!(
            "{count} assets, {} edges: {pass:?} for the pass, {whole:?} for the whole build",
            layers * wide * fan
        );
        // a bound loose enough that a busy machine cannot fail it. it guards
        // against a blow-up rather than measuring the constant: the number
        // printed above is the measurement
        assert!(
            pass < std::time::Duration::from_secs(2),
            "the pass over {count} assets took {pass:?}"
        );
    }

    // the manager an asset selected has to reach the op the run executes, or
    // the value would go to the process default and the row would name a file
    // nothing wrote
    #[test]
    fn a_wrapped_asset_op_keeps_the_manager_the_asset_selected() {
        let a = echo("a").io("parquet");
        let b = echo("b").from(&a);
        let reg = AssetRegistry::new(vec![a, b], Vec::new(), Vec::new()).unwrap();
        assert_eq!(
            wrap_op(&reg, reg.op("a").unwrap()).io_name(),
            Some("parquet")
        );
        assert_eq!(wrap_op(&reg, reg.op("b").unwrap()).io_name(), None);
        // and the consumer knows where its dep's value went, which is how a
        // partitioned dep is read back off its row
        assert_eq!(
            reg.dep_link("a", PartitionMapping::identity())
                .io
                .as_deref(),
            Some("parquet")
        );
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
                partition: None,
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
                partition: None,
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
                partition: None,
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
                partition: None,
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
    // stopped built nothing, whatever it had computed by then.
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
                // never returned, which is every op that is stopped mid-work
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
                Lineage::None,
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
    // it: the other materialization and the op run's own terminal row.
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
                Lineage::None,
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
                partition: None,
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
        // nothing built, so every key of an unbounded range is stale, and the
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

    fn hourly(name: &str) -> Asset {
        Asset::new(name, |ctx: OpCtx| async move {
            Ok(json!({ "hour": ctx.partition() }))
        })
        .partitioned(Partitions::hourly("2026-01-01T00"))
    }

    fn daily(name: &str, dep: &str, mapping: PartitionMapping) -> Asset {
        Asset::new(name, |_| async { Ok(json!(null)) })
            .reads_named(dep, mapping)
            .partitioned(Partitions::daily("2026-01-01"))
    }

    #[test]
    fn a_mapping_is_declared_on_the_dep_and_is_identity_unless_it_says_otherwise() {
        let hours = hourly("hours");
        let rollup = daily("rollup", "hours", PartitionMapping::covering());
        let plain = Asset::new("plain", |_| async { Ok(json!(null)) })
            .from(&hours)
            .partitioned(Partitions::hourly("2026-01-01T00"));
        // the mapping rides on the edge, and the dep is declared by declaring it
        assert_eq!(rollup.deps(), ["hours"]);
        let reg = AssetRegistry::new(vec![hours, rollup, plain], Vec::new(), Vec::new()).unwrap();
        assert_eq!(
            reg.get("rollup").unwrap().mapping("hours"),
            PartitionMapping::covering()
        );
        // a dep declared the way every dep was declared before mappings existed
        assert!(reg.get("plain").unwrap().mapping("hours").is_identity());
        // and so is a dep of an asset that has never heard of one
        assert!(reg.get("rollup").unwrap().mapping("nothing").is_identity());
        // the op the asset lowers to gives the same answer
        assert_eq!(
            reg.op("rollup").unwrap().mapping("hours"),
            PartitionMapping::covering()
        );

        // a name nothing registers is still a build error
        let err = reg_err(vec![daily("rollup", "ghost", PartitionMapping::all())]);
        assert!(err.to_string().contains("unknown"), "{err}");
    }

    #[test]
    fn a_pairing_no_mapping_could_resolve_fails_the_build() {
        // a window covers a span of time, and a static set spans none
        let err = reg_err(vec![
            keyed("a"),
            daily("rollup", "a", PartitionMapping::covering()),
        ]);
        assert!(err.to_string().contains("partitioned daily"), "{err}");
        assert!(
            err.to_string().contains("static key set spans none"),
            "{err}"
        );

        // and an hour sits inside a day rather than covering one
        let hours = Asset::new("hours", |_| async { Ok(json!(null)) })
            .reads_named("days", PartitionMapping::covering())
            .partitioned(Partitions::hourly("2026-01-01T00"));
        let days = Asset::new("days", |_| async { Ok(json!(null)) })
            .partitioned(Partitions::daily("2026-01-01"));
        let err = reg_err(vec![days, hours]);
        assert!(
            err.to_string()
                .contains("one hourly key sits inside one daily key"),
            "{err}"
        );

        // an offset needs one kind of key
        let hours = hourly("hours");
        let err = reg_err(vec![
            hours,
            daily("rollup", "hours", PartitionMapping::offset(-1)),
        ]);
        assert!(
            err.to_string()
                .contains("partitioned daily, but its dep hours is partitioned hourly"),
            "{err}"
        );
        assert!(
            err.to_string().contains("an offset steps along one kind"),
            "{err}"
        );

        // and an order to step along
        let previous = Asset::new("previous", |_| async { Ok(json!(null)) })
            .reads_named("a", PartitionMapping::offset(-1))
            .partitioned(Partitions::keys(["k1", "k2", "k3"]));
        let err = reg_err(vec![keyed("a"), previous]);
        assert!(
            err.to_string()
                .contains("partitioned static, and so is its dep a"),
            "{err}"
        );

        // an unpartitioned dep has no keys for any of it to choose between
        let err = reg_err(vec![
            Asset::source("s"),
            Asset::new("rollup", |_| async { Ok(json!(null)) })
                .reads_named("s", PartitionMapping::covering())
                .partitioned(Partitions::daily("2026-01-01")),
        ]);
        assert!(
            err.to_string().contains("s is not partitioned at all"),
            "{err}"
        );

        // nor has an unpartitioned consumer a key of its own to resolve from
        let err = reg_err(vec![
            keyed("a"),
            Asset::new("flat", |_| async { Ok(json!(null)) })
                .reads_named("a", PartitionMapping::offset(-1)),
        ]);
        assert!(
            err.to_string()
                .contains("it is not partitioned, but reads its static dep a"),
            "{err}"
        );

        // the pairings that do resolve
        let hours = hourly("hours");
        AssetRegistry::new(
            vec![
                hours,
                daily("rollup", "hours", PartitionMapping::covering()),
                daily("yesterday", "rollup", PartitionMapping::offset(-1)),
                Asset::new("flat", |_| async { Ok(json!(null)) })
                    .reads_named("hours", PartitionMapping::all()),
            ],
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn an_unpartitioned_asset_reads_every_key_of_a_partitioned_dep() {
        let store = Store::open(":memory:").unwrap();
        let a = keyed("a");
        let flat = Asset::new("flat", |ctx: OpCtx| async move {
            Ok(ctx.input("a").cloned().unwrap_or(json!(null)))
        })
        .reads_named("a", PartitionMapping::all());
        let reg = AssetRegistry::new(vec![a, flat], Vec::new(), Vec::new()).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();

        let two = HashMap::from([("a".to_string(), vec!["k1".to_string(), "k2".to_string()])]);
        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["a".into()], &two).unwrap();
        assert_eq!(build_plan(&runner, plan).await.status, RunStatus::Success);

        let m = mats_map(&store).unwrap();
        let plan = plan_target(&reg, &m, "flat").unwrap();
        // the key it has not read yet comes along: reading every key is a
        // claim about all of them, so the build owes the one that is missing
        assert_eq!(plan.seeds["partitions:a"], json!(["k3"]));
        assert_eq!(build_plan(&runner, plan).await.status, RunStatus::Success);
        // one object keyed by partition
        assert_eq!(
            store.materialization("flat", None).unwrap().unwrap().value,
            Some(json!({"k1": {"key": "k1"}, "k2": {"key": "k2"}, "k3": {"key": "k3"}}))
        );
        // and with every key read and recorded, the aggregation is fresh
        let st = staleness(&reg, &mats_map(&store).unwrap());
        assert!(!st["flat"].stale, "{:?}", st["flat"].reasons);
    }

    // a window needs time, so these build their key sets around the clock the
    // sets themselves are generated from: yesterday is a whole day of hours
    // whatever hour it is now
    fn yesterday() -> String {
        Utc::now()
            .date_naive()
            .pred_opt()
            .expect("a day before today")
            .format("%Y-%m-%d")
            .to_string()
    }

    fn hours_of(day: &str) -> Vec<String> {
        (0..24).map(|h| format!("{day}T{h:02}")).collect()
    }

    /// hourly `hours` from `start`, and a daily `rollup` covering it from
    /// yesterday.
    fn rollup_over(start: &str) -> AssetRegistry {
        let hours = Asset::new("hours", |ctx: OpCtx| async move {
            Ok(json!({ "hour": ctx.partition() }))
        })
        .partitioned(Partitions::hourly(start.to_string()));
        let rollup = Asset::new("rollup", |ctx: OpCtx| async move {
            let hours = ctx.input("hours").cloned().unwrap_or(json!({}));
            Ok(json!({ "hours": hours.as_object().map_or(0, |h| h.len()) }))
        })
        .reads_named("hours", PartitionMapping::covering())
        .partitioned(Partitions::daily(yesterday()));
        AssetRegistry::new(vec![hours, rollup], Vec::new(), Vec::new()).unwrap()
    }

    fn consumed(hours: &[String]) -> Value {
        Value::Object(
            hours
                .iter()
                .map(|h| (h.clone(), json!(format!("fp-{h}"))))
                .collect(),
        )
    }

    fn covered_rows(day: &str, hours: &[String]) -> Vec<Materialization> {
        let mut rows: Vec<Materialization> = hours
            .iter()
            .map(|h| part("hours", h, &format!("fp-{h}"), json!({})))
            .collect();
        rows.push(part(
            "rollup",
            day,
            "r1",
            json!({ "hours": consumed(hours) }),
        ));
        rows
    }

    #[test]
    fn a_rollup_is_stale_when_an_hour_it_covers_moves_and_not_when_another_does() {
        let day = yesterday();
        let hours = hours_of(&day);
        let reg = rollup_over(&format!("{day}T00"));
        let fresh = covered_rows(&day, &hours);
        let st = staleness(&reg, &mats(fresh.clone()));
        assert!(
            !st["rollup"].parts[&day].stale,
            "a rollup of hours that have not moved: {:?}",
            st["rollup"].parts[&day].reasons
        );

        // one covered hour rebuilt to different content
        let mut moved = fresh.clone();
        moved[7] = part("hours", &hours[7], "fp-rebuilt", json!({}));
        let st = staleness(&reg, &mats(moved));
        assert!(st["rollup"].parts[&day].stale);
        // and the chain names the hour rather than the day
        assert_eq!(
            st["rollup"].parts[&day].reasons,
            vec![StaleReason {
                dep: "hours".into(),
                partition: Some(hours[7].clone()),
                had: Some(format!("fp-{}", hours[7])),
                now: Some("fp-rebuilt".into()),
            }]
        );

        // an hour of *today* is not one this key covers, and moving it leaves
        // the rollup of yesterday alone
        let mut elsewhere = fresh;
        let today = Utc::now().format("%Y-%m-%dT%H").to_string();
        elsewhere.push(part("hours", &today, "fp-today", json!({})));
        let st = staleness(&reg, &mats(elsewhere));
        assert!(
            !st["rollup"].parts[&day].stale,
            "an hour outside the window moved the rollup"
        );
    }

    #[tokio::test]
    async fn building_a_daily_key_builds_the_hours_it_covers_and_records_every_one() {
        let store = Store::open(":memory:").unwrap();
        let day = yesterday();
        let hours = hours_of(&day);
        let reg = rollup_over(&format!("{day}T00"));
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();

        let m = mats_map(&store).unwrap();
        let plan = plan_partitions(&reg, &m, &["rollup".into()], &on("rollup", [&day])).unwrap();
        // the hours the key covers come along, and no others
        assert_eq!(plan.seeds["partitions:hours"], json!(hours));
        let run = build_plan(&runner, plan).await;
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(store.op_runs(&run.id).unwrap().len(), 25);

        // the body saw all 24 of them, keyed by hour
        let built = store
            .materialization("rollup", Some(&day))
            .unwrap()
            .unwrap();
        assert_eq!(built.value, Some(json!({"hours": 24})));
        // and the lineage records the fingerprint of every hour it consumed,
        // which is what lets one of them moving be noticed
        let recorded = built.inputs["hours"].as_object().expect("one per hour");
        assert_eq!(recorded.len(), 24);
        for hour in &hours {
            let fp = store.materialization("hours", Some(hour)).unwrap().unwrap();
            assert_eq!(recorded[hour], json!(fp.fingerprint));
        }
        let st = staleness(&reg, &mats_map(&store).unwrap());
        assert!(!st["rollup"].parts[&day].stale);

        // one covered hour rebuilt on its own
        let hour = &hours[3];
        let plan = plan_partitions(
            &reg,
            &mats_map(&store).unwrap(),
            &["hours".into()],
            &on("hours", [hour]),
        )
        .unwrap();
        build_plan(&runner, plan).await;
        let st = staleness(&reg, &mats_map(&store).unwrap());
        // it produced the same bytes, so the fingerprint it recorded is the
        // one the day consumed and the day is still fresh: what makes a
        // rollup stale is content moving, not an hour being run again
        assert!(!st["rollup"].parts[&day].stale);
    }

    #[test]
    fn a_key_whose_hours_the_dep_does_not_hold_is_refused_by_name_and_skipped_by_default() {
        let day = yesterday();
        // the hourly set starts at 06:00, so the first six hours of the day are
        // not keys of it and never will be
        let reg = rollup_over(&format!("{day}T06"));
        let err = check_named_keys(&reg, "rollup", std::slice::from_ref(&day)).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("partition {day:?} reads hours[{day}T00]")),
            "{err}"
        );

        // a build that names nothing leaves it out rather than refusing: the
        // day after it is a whole day of hours and is buildable
        let plan = plan_target(&reg, &Mats::default(), "rollup").unwrap();
        let keys = plan.seeds["partitions:rollup"].as_array().unwrap();
        assert!(
            !keys.contains(&json!(day)),
            "a day whose hours are missing was targeted anyway: {keys:?}"
        );
        // and it stays stale rather than quietly reading the hours that are
        // there
        let st = staleness(&reg, &Mats::default());
        assert!(st["rollup"].parts[&day].stale);
    }

    #[tokio::test]
    async fn a_day_the_clock_is_still_inside_rolls_up_the_hours_it_has() {
        let store = Store::open(":memory:").unwrap();
        let today = Utc::now().format("%Y-%m-%d").to_string();
        let reg = rollup_over(&format!("{}T00", yesterday()));
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone()).unwrap();
        // the hours left in today have not happened, which is not the same as
        // an hour that will never be a key of anything: this builds
        check_named_keys(&reg, "rollup", std::slice::from_ref(&today)).unwrap();
        let plan = plan_partitions(
            &reg,
            &mats_map(&store).unwrap(),
            &["rollup".into()],
            &on("rollup", [&today]),
        )
        .unwrap();
        let hours = plan.seeds["partitions:hours"].as_array().unwrap().clone();
        assert!(!hours.is_empty(), "the hours of today so far");
        assert!(
            hours
                .iter()
                .all(|h| h.as_str().unwrap().starts_with(&today)),
            "an hour of another day came along: {hours:?}"
        );
        let run = build_plan(&runner, plan).await;
        assert_eq!(run.status, RunStatus::Success);
        let built = store
            .materialization("rollup", Some(&today))
            .unwrap()
            .unwrap();
        assert_eq!(built.value, Some(json!({ "hours": hours.len() })));
        // and it is fresh against the hours that exist, until the next lands
        let st = staleness(&reg, &mats_map(&store).unwrap());
        assert!(!st["rollup"].parts[&today].stale);
    }

    #[test]
    fn the_build_limit_counts_the_keys_of_the_target_and_not_what_they_read() {
        let day = yesterday();
        let hours = Asset::new("hours", |_| async { Ok(json!(null)) })
            .partitioned(Partitions::hourly(format!("{day}T00")));
        let rollup = Asset::new("rollup", |_| async { Ok(json!(null)) })
            .reads_named("hours", PartitionMapping::covering())
            .partitioned(Partitions::daily(day.clone()).build_limit(1));
        let reg = AssetRegistry::new(vec![hours, rollup], Vec::new(), Vec::new()).unwrap();
        let plan = plan_target(&reg, &Mats::default(), "rollup").unwrap();
        // one key of the target, as the limit says
        assert_eq!(plan.seeds["partitions:rollup"].as_array().unwrap().len(), 1);
        // and the hours under it, which the limit says nothing about: a mapped
        // chunk is as many instances as its keys read, and the ceiling on that
        // is Hestan::max_instances. a whole day is 24 of them
        let plan = plan_partitions(
            &reg,
            &Mats::default(),
            &["rollup".into()],
            &on("rollup", [&day]),
        )
        .unwrap();
        assert_eq!(plan.seeds["partitions:rollup"], json!([day]));
        assert_eq!(plan.seeds["partitions:hours"].as_array().unwrap().len(), 24);
    }

    #[test]
    fn a_dep_that_declares_no_mapping_is_identity_or_nothing() {
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
