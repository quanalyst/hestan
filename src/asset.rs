use std::collections::{HashMap, HashSet};
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
use crate::model::{Materialization, Trigger};
use crate::op::{Op, OpCtx};
use crate::store::Store;

/// the internal job every asset build runs under.
pub(crate) const ASSETS_JOB: &str = "assets";

pub(crate) type ProbeFn = dyn Fn() -> BoxFuture<'static, Result<String, Box<dyn std::error::Error + Send + Sync>>>
    + Send
    + Sync;

/// an op with identity: a persisted latest value, a fingerprint, and explicit
/// lineage. derived assets ([`Asset::new`] / [`Asset::typed`]) have a body
/// and deps; source assets ([`Asset::source`]) stand for external data and
/// carry only a cheap [`probe`](Asset::probe) that fingerprints it. register
/// with `Hestan::assets`.
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
}

pub(crate) struct AssetMeta {
    pub name: String,
    pub source: bool,
    pub deps: Vec<String>,
    pub auto: bool,
    pub probe: Option<Arc<ProbeFn>>,
    pub probe_every: Duration,
    op: Option<Op>,
    retries: u32,
    retry_delay: Option<Duration>,
}

/// the validated asset graph, in topo order. built once by `Hestan::build`.
pub(crate) struct AssetRegistry {
    metas: Vec<AssetMeta>,
    by_name: HashMap<String, usize>,
}

impl AssetRegistry {
    pub(crate) fn empty() -> AssetRegistry {
        AssetRegistry {
            metas: Vec::new(),
            by_name: HashMap::new(),
        }
    }

    pub(crate) fn new(assets: Vec<Asset>) -> Result<AssetRegistry, Error> {
        let mut metas: Vec<AssetMeta> = Vec::with_capacity(assets.len());
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
            metas.push(AssetMeta {
                name: a.name,
                source: a.source,
                deps: a.deps,
                auto: a.auto,
                probe: a.probe,
                probe_every: a.probe_every,
                op: a.op,
                retries: a.retries,
                retry_delay: a.retry_delay,
            });
        }
        let pairs: Vec<(String, Vec<String>)> = metas
            .iter()
            .map(|m| (m.name.clone(), m.deps.clone()))
            .collect();
        let order = graph::topo_order(&pairs).map_err(|e| Error::Graph(format!("assets: {e}")))?;
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
        let by_name = ordered
            .iter()
            .enumerate()
            .map(|(i, m)| (m.name.clone(), i))
            .collect();
        Ok(AssetRegistry {
            metas: ordered,
            by_name,
        })
    }

    pub(crate) fn topo(&self) -> impl Iterator<Item = &AssetMeta> {
        self.metas.iter()
    }

    pub(crate) fn get(&self, name: &str) -> Option<&AssetMeta> {
        self.by_name.get(name).map(|&i| &self.metas[i])
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

    /// lower into the internal "assets" job: one wrapped op per derived asset,
    /// sources as external deps that a full launch seeds null.
    pub(crate) fn lower_job(&self) -> Result<Job, Error> {
        let ops: Vec<Op> = self
            .metas
            .iter()
            .filter(|m| !m.source)
            .map(wrap_op)
            .collect();
        let external: Vec<String> = self
            .metas
            .iter()
            .filter(|m| m.source)
            .map(|m| m.name.clone())
            .collect();
        Job::assemble(
            ASSETS_JOB,
            Some("internal: asset materializations".into()),
            ops,
            external,
        )
    }
}

// the materialization write lives inside the op body, so a crash before
// op_finished re-runs the op next build — at-least-once, like op state
fn wrap_op(meta: &AssetMeta) -> Op {
    let inner = meta.op.clone().expect("derived asset has an op");
    let name = meta.name.clone();
    let deps = meta.deps.clone();
    let mut op = Op::new(name.clone(), move |ctx: OpCtx| {
        let inner = inner.clone();
        let name = name.clone();
        let deps = deps.clone();
        async move {
            let output = inner.call(ctx.clone()).await?;
            let fingerprint = ctx
                .take_fingerprint()
                .unwrap_or_else(|| content_fingerprint(&output));
            // deps' current fingerprints: ancestors in this run already wrote theirs
            let mut inputs = Map::new();
            for dep in &deps {
                let fp = ctx.store.materialization(dep)?.map(|m| m.fingerprint);
                inputs.insert(dep.clone(), fp.map(Value::String).unwrap_or(Value::Null));
            }
            // read, not taken: the same map goes on this materialization and
            // on the op run the executor writes when this op reports success
            ctx.store.record_materialization(
                &name,
                &fingerprint,
                &Value::Object(inputs),
                Some(&output),
                Some(ctx.run_id()),
                ctx.staged_meta().as_ref(),
            )?;
            Ok(output)
        }
    })
    .after(meta.deps.clone())
    .retries(meta.retries);
    if let Some(d) = meta.retry_delay {
        op = op.retry_delay(d);
    }
    op.with_types_of(meta.op.as_ref().expect("derived asset has an op"))
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Staleness {
    pub stale: bool,
    pub reasons: Vec<StaleReason>,
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
pub(crate) fn staleness(
    reg: &AssetRegistry,
    mats: &HashMap<String, Materialization>,
) -> HashMap<String, Staleness> {
    let mut out: HashMap<String, Staleness> = HashMap::new();
    for meta in reg.topo() {
        let s = match mats.get(&meta.name) {
            None => Staleness {
                stale: true,
                reasons: Vec::new(),
            },
            Some(mat) => {
                let mut reasons = Vec::new();
                for dep in &meta.deps {
                    let had = mat
                        .inputs
                        .get(dep)
                        .and_then(Value::as_str)
                        .map(String::from);
                    let now = mats.get(dep).map(|m| m.fingerprint.clone());
                    let dep_stale = out.get(dep).is_some_and(|s| s.stale);
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
                }
            }
        };
        out.insert(meta.name.clone(), s);
    }
    out
}

/// what one build run executes: derived assets in topo order, plus seeds for
/// everything they read that won't run — stored values for fresh derived deps,
/// null for sources.
#[derive(Debug)]
pub(crate) struct BuildPlan {
    pub ops: Vec<String>,
    pub seeds: HashMap<String, Value>,
}

fn seeds_for(
    reg: &AssetRegistry,
    mats: &HashMap<String, Materialization>,
    ops: &[String],
) -> HashMap<String, Value> {
    let in_plan: HashSet<&str> = ops.iter().map(String::as_str).collect();
    let mut seeds = HashMap::new();
    for name in ops {
        let meta = reg.get(name).expect("planned asset is registered");
        for dep in &meta.deps {
            if in_plan.contains(dep.as_str()) || seeds.contains_key(dep) {
                continue;
            }
            let value = match reg.get(dep) {
                Some(d) if d.source => Value::Null,
                _ => mats
                    .get(dep)
                    .and_then(|m| m.value.clone())
                    .unwrap_or(Value::Null),
            };
            seeds.insert(dep.clone(), value);
        }
    }
    seeds
}

/// the plan for one target: its stale derived ancestors plus the target itself,
/// always. errors on an unknown or source target.
pub(crate) fn plan_target(
    reg: &AssetRegistry,
    mats: &HashMap<String, Materialization>,
    target: &str,
) -> Result<BuildPlan, Error> {
    plan_targets(reg, mats, &[target.to_string()])
}

/// one plan for several targets: the union of their stale derived ancestors plus
/// every target itself. one plan means one run — overlapping per-target plans
/// would each re-run the shared ancestors. errors on an unknown or source target.
pub(crate) fn plan_targets(
    reg: &AssetRegistry,
    mats: &HashMap<String, Materialization>,
    targets: &[String],
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
    while let Some(n) = stack.pop() {
        if want.insert(n.clone()) {
            for d in &reg.get(&n).expect("dep names validated").deps {
                stack.push(d.clone());
            }
        }
    }
    let stale = staleness(reg, mats);
    let ops: Vec<String> = reg
        .topo()
        .filter(|m| !m.source && want.contains(&m.name))
        .filter(|m| targets.contains(&m.name) || stale[&m.name].stale)
        .map(|m| m.name.clone())
        .collect();
    let seeds = seeds_for(reg, mats, &ops);
    Ok(BuildPlan { ops, seeds })
}

/// one plan covering every stale derived asset; `None` when nothing is stale.
pub(crate) fn plan_all(
    reg: &AssetRegistry,
    mats: &HashMap<String, Materialization>,
) -> Option<BuildPlan> {
    let stale = staleness(reg, mats);
    let ops: Vec<String> = reg
        .topo()
        .filter(|m| !m.source && stale[&m.name].stale)
        .map(|m| m.name.clone())
        .collect();
    if ops.is_empty() {
        return None;
    }
    let seeds = seeds_for(reg, mats, &ops);
    Some(BuildPlan { ops, seeds })
}

pub(crate) fn mats_map(store: &Store) -> Result<HashMap<String, Materialization>, Error> {
    Ok(store
        .latest_materializations()?
        .into_iter()
        .map(|m| (m.asset.clone(), m))
        .collect())
}

/// launch a plan as one subset run of the assets job.
pub(crate) fn launch_plan(
    runner: &crate::executor::Runner,
    plan: BuildPlan,
    trigger: Trigger,
) -> Result<String, Error> {
    runner.launch_subset(
        ASSETS_JOB,
        plan.ops.into_iter().collect(),
        plan.seeds,
        json!({}),
        trigger,
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Runner;
    use crate::model::RunStatus;
    use chrono::Utc;

    fn mat(asset: &str, fp: &str, inputs: Value, value: Option<Value>) -> Materialization {
        Materialization {
            id: 0,
            asset: asset.into(),
            fingerprint: fp.into(),
            inputs,
            value,
            run_id: None,
            built_at: Utc::now(),
            metadata: None,
        }
    }

    fn mats(list: Vec<Materialization>) -> HashMap<String, Materialization> {
        list.into_iter().map(|m| (m.asset.clone(), m)).collect()
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
        AssetRegistry::new(vec![s, a, b]).unwrap()
    }

    fn reg_err(assets: Vec<Asset>) -> Error {
        match AssetRegistry::new(assets) {
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
        let reg = AssetRegistry::new(vec![b, a, s]).unwrap();
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
        let s = staleness(&reg, &HashMap::new());
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
        let reg = AssetRegistry::new(vec![s, left, right, sink]).unwrap();
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
        let reg = AssetRegistry::new(vec![s, a, b, c]).unwrap();

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

        let plan = plan_all(&reg, &HashMap::new()).unwrap();
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
            )
            .await
            .unwrap()
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
        let reg = AssetRegistry::new(vec![s, a, b]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        store
            .record_materialization("s", "s-fp", &json!({}), None, None, None)
            .unwrap();
        build_all(&reg, &runner).await;

        // built twice more: once to the same fingerprint, once to a new one
        build_target(&reg, &runner, "a").await;
        *pinned.lock().unwrap() = "v2".to_string();
        build_target(&reg, &runner, "a").await;

        let history = store.materializations("a", 10).unwrap();
        let seen: Vec<(&str, bool)> = history
            .iter()
            .map(|(m, changed)| (m.fingerprint.as_str(), *changed))
            .collect();
        assert_eq!(seen, [("v2", true), ("v1", false), ("v1", true)]);
        // every entry names the run that built it, and they are distinct runs
        assert!(history.iter().all(|(m, _)| m.run_id.is_some()));

        // the latest entry is the current one, and it is the only one
        // staleness reads: b consumed v1 and a now says v2
        let m = mats_map(&store).unwrap();
        assert_eq!(m["a"].fingerprint, "v2");
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
        let reg = AssetRegistry::new(vec![s, a, b]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        // the source was probed before this build
        store
            .record_materialization("s", "s-fp", &json!({}), None, None, None)
            .unwrap();

        let run = build_all(&reg, &runner).await;
        assert_eq!(run.status, RunStatus::Success);
        assert_eq!(run.job, ASSETS_JOB);
        assert_eq!(run.trigger, Trigger::Build);

        let ma = store.materialization("a").unwrap().unwrap();
        assert_eq!(ma.inputs, json!({"s": "s-fp"}));
        assert_eq!(ma.value, Some(json!({"rows": 3})));
        assert_eq!(ma.run_id.as_deref(), Some(run.id.as_str()));
        assert_eq!(ma.fingerprint, content_fingerprint(&json!({"rows": 3})));

        let mb = store.materialization("b").unwrap().unwrap();
        assert_eq!(mb.inputs, json!({"a": ma.fingerprint}));
        assert_eq!(mb.value, Some(json!({"doubled": 6})));

        let m = mats_map(&store).unwrap();
        let st = staleness(&reg, &m);
        assert!(st.values().all(|s| !s.stale));
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
        let reg = AssetRegistry::new(vec![counted, quiet]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let run = build_all(&reg, &runner).await;

        let reported = json!({
            "rows": {"int": 3},
            "source": {"url": "https://example.test/rows"},
        });
        let ops = store.op_runs(&run.id).unwrap();
        let op = ops.iter().find(|o| o.op == "counted").unwrap();
        assert_eq!(op.metadata, Some(reported.clone()));
        let m = store.materialization("counted").unwrap().unwrap();
        assert_eq!(m.metadata, Some(reported));

        // and an asset that reported nothing carries null in both places
        let quiet = ops.iter().find(|o| o.op == "quiet").unwrap();
        assert_eq!(quiet.metadata, None);
        assert_eq!(
            store.materialization("quiet").unwrap().unwrap().metadata,
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
        let reg = AssetRegistry::new(vec![pinned, hashed]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        build_all(&reg, &runner).await;

        assert_eq!(
            store
                .materialization("pinned")
                .unwrap()
                .unwrap()
                .fingerprint,
            "version-7"
        );
        assert_eq!(
            store
                .materialization("hashed")
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
        let reg = AssetRegistry::new(vec![s, a, b]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        store
            .record_materialization("s", "s-fp", &json!({}), None, None, None)
            .unwrap();
        build_all(&reg, &runner).await;

        // poke only b stale: pretend it consumed an older a
        store
            .record_materialization(
                "b",
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
        let reg = AssetRegistry::new(vec![s, a]).unwrap();
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let run = runner
            .run(ASSETS_JOB, json!({}), crate::model::Trigger::Manual)
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success);
        let m = store.materialization("a").unwrap().unwrap();
        assert_eq!(m.inputs, json!({"s": null}));
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
        let reg = AssetRegistry::new(vec![base, t]).unwrap();
        let job = reg.lower_job().unwrap();
        let op = job.op("t").unwrap();
        assert!(op.input_type().unwrap().ends_with("In"));
        assert!(op.output_type().unwrap().ends_with("Out"));

        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([job], store.clone());
        build_all(&reg, &runner).await;
        assert_eq!(
            store.materialization("t").unwrap().unwrap().value,
            Some(json!({"n": 5}))
        );
    }
}
