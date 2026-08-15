use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};

use crate::asset::{
    ASSETS_JOB, AssetMeta, AssetRegistry, Mats, Staleness, key_sets, launch_plan, plan_partitions,
    staleness,
};
use crate::error::Error;
use crate::executor::Runner;
use crate::model::{RunTags, Trigger};
use crate::partition::KeySet;
use crate::schedule::Cron;

/// when hestan rebuilds an asset on its own, declared with
/// [`Asset::policy`](crate::Asset::policy).
///
/// ```no_run
/// # use hestan::{Asset, AutoPolicy, OpCtx, PartitionMapping, Partitions};
/// # use serde_json::json;
/// # let hourly = Asset::new("hourly_traffic", |_: OpCtx| async { Ok(json!(null)) })
/// #     .partitioned(Partitions::hourly("2026-01-01"));
/// Asset::new("daily_traffic", |_: OpCtx| async { Ok(json!(null)) })
///     .reads(&hourly, PartitionMapping::covering())
///     .partitioned(Partitions::daily("2026-01-01"))
///     .policy(AutoPolicy::when_stale().and_upstream_ready());
/// ```
///
/// four shapes, and each of them is a thing somebody would otherwise write a
/// sensor for: [`when_stale`](Self::when_stale), which is what
/// [`Asset::auto`](crate::Asset::auto) has always meant;
/// [`when_missing`](Self::when_missing), which is the fresh deployment and the
/// newly added asset; [`after_cron`](Self::after_cron), which is "nightly, but
/// do not rebuild what has not moved"; and any of them held back by
/// [`and_upstream_ready`](Self::and_upstream_ready) until everything the build
/// would read is there, which is what makes a daily rollup wait for its last
/// hour instead of recording a partial day.
///
/// a [partitioned asset](crate::Partitions) is evaluated **one key at a time**,
/// so a rule builds the keys that qualify and leaves the ones that do not: on a
/// daily rollup whose last two days are stale, a stale rule builds two days and
/// says nothing about the other four hundred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AutoPolicy {
    rule: Rule,
    tz: String,
    upstream_ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Rule {
    Stale,
    Missing,
    Cron(String),
}

impl AutoPolicy {
    fn of(rule: Rule) -> AutoPolicy {
        AutoPolicy {
            rule,
            tz: "UTC".into(),
            upstream_ready: false,
        }
    }

    /// rebuild a key whenever it is stale: a dep it read moved, or it has never
    /// been built at all. this is what [`Asset::auto`](crate::Asset::auto) sets
    /// and what it has always meant.
    pub fn when_stale() -> AutoPolicy {
        AutoPolicy::of(Rule::Stale)
    }

    /// build a key that has never been built, and nothing else: a fresh
    /// deployment and a newly declared asset both land here, and neither is a
    /// dep that moved. once it exists this rule has nothing more to say, however
    /// stale it goes.
    ///
    /// on a partitioned asset that is every key with no materialization, newest
    /// first and capped by the set's
    /// [build limit](crate::Partitions::build_limit) per pass, so declaring it
    /// on two years of days fills them a chunk at a time rather than at once.
    pub fn when_missing() -> AutoPolicy {
        AutoPolicy::of(Rule::Missing)
    }

    /// rebuild a key after `expr` comes round, and only if it is stale by then:
    /// "nightly, but do not rebuild what has not moved".
    ///
    /// the same 5-field crontab a [`Schedule`](crate::Schedule) takes, read in
    /// utc until [`tz`](Self::tz) says otherwise, and an expression that does
    /// not parse fails the build rather than a pass at 2am. a key builds when
    /// the last occurrence at or before now is newer than that key's last build,
    /// so an occurrence that arrived while something else was building is picked
    /// up by the next pass rather than lost.
    pub fn after_cron(expr: impl Into<String>) -> AutoPolicy {
        AutoPolicy::of(Rule::Cron(expr.into()))
    }

    /// the timezone [`after_cron`](Self::after_cron) is read in, utc by
    /// default. nothing else in a policy reads a clock, so this says nothing
    /// about the other rules.
    pub fn tz(mut self, tz: impl Into<String>) -> AutoPolicy {
        self.tz = tz.into();
        self
    }

    /// hold the build until every upstream key it reads has been built.
    ///
    /// which keys those are is the mapping's answer, the same one staleness
    /// asks: a daily key [covering](crate::PartitionMapping::covering) hourly
    /// data waits for all 24 hours of its day, an
    /// [offset](crate::PartitionMapping::offset) waits for the one key it
    /// names, and an identity read waits for the dep at its own key. without
    /// this a rollup builds from the hours that happen to be there and goes
    /// stale as each of the rest lands, which is the right default and the
    /// wrong thing to write a daily total from.
    pub fn and_upstream_ready(mut self) -> AutoPolicy {
        self.upstream_ready = true;
        self
    }

    /// the word the api, the ui and the event log call this rule.
    pub(crate) fn rule_word(&self) -> &'static str {
        match self.rule {
            Rule::Stale => "stale",
            Rule::Missing => "missing",
            Rule::Cron(_) => "cron",
        }
    }

    /// whether a probe upstream may launch this, which is the one trigger
    /// [`Asset::auto`](crate::Asset::auto) has ever had. a probe answers "has
    /// the data moved", which is what the stale and missing rules turn on; when
    /// a cron last came round is the policy loop's to read.
    pub(crate) fn on_probe(&self) -> bool {
        !matches!(self.rule, Rule::Cron(_))
    }

    /// the parsed cron, checked where the asset graph is: an expression or a
    /// timezone that does not resolve is a boot error, and the evaluator gets
    /// the parse rather than repeating it every minute.
    pub(crate) fn parse_cron(&self, asset: &str) -> Result<Option<Cron>, Error> {
        match &self.rule {
            Rule::Cron(expr) => Cron::parse(expr, &self.tz)
                .map(Some)
                .map_err(|e| Error::Graph(format!("asset {asset}: {e}"))),
            // the timezone is still checked: one set on a rule that does not
            // read a clock is a mistake worth naming, not a field to ignore
            _ => match self.tz.parse::<chrono_tz::Tz>() {
                Ok(_) => Ok(None),
                Err(_) => Err(Error::Timezone(self.tz.clone())),
            },
        }
    }
}

/// what a policy says about one key right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// the rule does not fire: there is nothing to build.
    Idle,
    /// the rule fires and nothing is in the way.
    Build,
    /// the rule fires and something the build would read is not there.
    Waiting(Waiting),
}

/// what a key that wants a build is waiting for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Waiting {
    /// an upstream key that has not been built yet: the last hour of a day.
    Key {
        dep: String,
        /// which key of it, and `None` where the dep has no keys.
        key: Option<String>,
    },
    /// a key the dep's set does not hold and never will. only a
    /// [window](crate::PartitionMapping::covering) promises a range, so only a
    /// window can promise one nothing could ever fill.
    Never { dep: String, key: String },
    /// a [source](crate::Asset::source) nothing has observed. its fingerprint
    /// is what everything below it is compared against, so until a probe writes
    /// one a build would consume null and leave the asset exactly as stale as
    /// it found it.
    Source(String),
}

impl fmt::Display for Waiting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Waiting::Key { dep, key: None } => write!(f, "{dep}"),
            Waiting::Key {
                dep,
                key: Some(key),
            }
            | Waiting::Never { dep, key } => write!(f, "{dep}[{key}]"),
            Waiting::Source(name) => write!(f, "{name}"),
        }
    }
}

/// one asset a pass wants built, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Want {
    pub asset: String,
    /// the rule that fired, as the event log names it.
    pub rule: &'static str,
    /// which keys, newest first and capped by the
    /// [build limit](crate::Partitions::build_limit); empty on an unpartitioned
    /// asset, which is one of everything.
    pub keys: Vec<String>,
}

/// everything evaluating a policy needs, worked out once for the whole pass.
///
/// staleness and the key sets are the expensive halves and neither is per
/// policy, so a pass over a hundred assets reads them once. `now` is a
/// parameter for the same reason it is everywhere else here: a rule about a
/// cron is a rule about a clock, and a test cannot wait until 2am.
pub(crate) struct Pass<'a> {
    reg: &'a AssetRegistry,
    mats: &'a Mats,
    sets: HashMap<String, KeySet>,
    stale: HashMap<String, Staleness>,
    /// asset -> the source upstream of it that nothing has observed.
    unobserved: HashMap<String, String>,
    now: DateTime<Utc>,
}

impl<'a> Pass<'a> {
    pub(crate) fn new(reg: &'a AssetRegistry, mats: &'a Mats, now: DateTime<Utc>) -> Pass<'a> {
        Pass {
            sets: key_sets(reg),
            stale: staleness(reg, mats),
            unobserved: unobserved_sources(reg, mats),
            reg,
            mats,
            now,
        }
    }

    /// what `meta`'s policy says about one key, or about the whole of an
    /// unpartitioned asset. [`Verdict::Idle`] when it declared none.
    pub(crate) fn verdict(&self, meta: &AssetMeta, key: Option<&str>) -> Verdict {
        let Some(policy) = &meta.policy else {
            return Verdict::Idle;
        };
        let built = self.mats.get(&meta.name, key);
        let stale = match key {
            None => self.stale[&meta.name].stale,
            Some(key) => self.stale[&meta.name]
                .parts
                .get(key)
                .is_none_or(|s| s.stale),
        };
        let fires = match &policy.rule {
            Rule::Stale => stale,
            Rule::Missing => built.is_none(),
            Rule::Cron(_) => {
                stale
                    && match (
                        meta.cron.as_ref().and_then(|c| c.last_before(self.now)),
                        built,
                    ) {
                        // never built, and the cron has come round at least
                        // once: there is nothing to have built it since
                        (Some(_), None) => true,
                        (Some(at), Some(m)) => at > m.built_at,
                        (None, _) => false,
                    }
            }
        };
        if !fires {
            return Verdict::Idle;
        }
        // a source nothing has observed first, since it is the answer for every
        // key at once and the one a person can act on
        if let Some(source) = self.unobserved.get(&meta.name) {
            return Verdict::Waiting(Waiting::Source(source.clone()));
        }
        match self.gap(meta, key) {
            // a window over keys the dep will never hold is unbuildable however
            // the policy reads, and is left out of a default build for the same
            // reason
            Some(never @ Waiting::Never { .. }) => Verdict::Waiting(never),
            Some(gap) if policy.upstream_ready => Verdict::Waiting(gap),
            _ => Verdict::Build,
        }
    }

    /// the first thing one key reads that is not there.
    ///
    /// [`Reads`](crate::PartitionMapping) is what a key reads and what of it the
    /// dep will never hold, which is the question staleness asks of every
    /// mapping; this asks the same mappings the same question and looks the
    /// answers up in what has actually been built. asking it any other way
    /// would be a second opinion about upstream, free to disagree with the one
    /// that decides whether the key is stale in the first place.
    fn gap(&self, meta: &AssetMeta, key: Option<&str>) -> Option<Waiting> {
        for dep in &meta.deps {
            let mapping = meta.mapping(dep);
            let Some(set) = self.sets.get(dep).filter(|_| !mapping.is_identity()) else {
                // identity, and every dep with no keys to choose between: the
                // same key of a partitioned dep, the whole of an unpartitioned
                // one
                let at = key.filter(|_| self.sets.contains_key(dep));
                if self.mats.get(dep, at).is_none() {
                    return Some(Waiting::Key {
                        dep: dep.clone(),
                        key: at.map(String::from),
                    });
                }
                continue;
            };
            let reads = mapping.reads(meta.partitions.as_ref(), key, set);
            if let Some(missing) = reads.missing.first() {
                return Some(Waiting::Never {
                    dep: dep.clone(),
                    key: missing.clone(),
                });
            }
            if let Some(unbuilt) = reads
                .keys
                .iter()
                .find(|up| self.mats.get(dep, Some(up)).is_none())
            {
                return Some(Waiting::Key {
                    dep: dep.clone(),
                    key: Some(unbuilt.clone()),
                });
            }
        }
        None
    }

    /// every key a policy is evaluated over: one `None` for an unpartitioned
    /// asset, and the set's keys for a partitioned one.
    fn keys_of(&self, meta: &AssetMeta) -> Vec<Option<String>> {
        match self.sets.get(&meta.name) {
            None => vec![None],
            Some(set) => set.keys().iter().map(|k| Some(k.clone())).collect(),
        }
    }

    /// every asset whose policy wants a build now, in topo order so that a plan
    /// built from them runs upstream first.
    ///
    /// `only` is the descendants of one probed source, on the path a probe
    /// takes; `None` is everything, on the pass the deciding process makes.
    pub(crate) fn wants(&self, only: Option<&std::collections::HashSet<String>>) -> Vec<Want> {
        let mut out = Vec::new();
        for meta in self.reg.topo() {
            let Some(policy) = &meta.policy else {
                continue;
            };
            if only.is_some_and(|set| !set.contains(&meta.name)) {
                continue;
            }
            if only.is_some() && !policy.on_probe() {
                continue;
            }
            let mut keys: Vec<String> = Vec::new();
            let mut wanted = false;
            for key in self.keys_of(meta) {
                if self.verdict(meta, key.as_deref()) != Verdict::Build {
                    continue;
                }
                wanted = true;
                if let Some(key) = key {
                    keys.push(key);
                }
            }
            if !wanted {
                continue;
            }
            // newest first and capped, exactly as a build that names no keys
            // chooses them: a set that has run for two years is not a run
            keys.reverse();
            if let Some(spec) = &meta.partitions {
                keys.truncate(spec.limit());
            }
            out.push(Want {
                asset: meta.name.clone(),
                rule: policy.rule_word(),
                keys,
            });
        }
        out
    }
}

/// launch what a pass wants, as one run.
///
/// one plan for all of them, never one per asset: overlapping plans would share
/// stale ancestors and race each other's lineage writes. `Ok(None)` is a pass
/// that wanted nothing, or one that found a build already going, which is the
/// same answer as far as the caller is concerned: nothing was launched and the
/// next pass asks again of fresher data.
pub(crate) fn launch(
    runner: &Runner,
    reg: &AssetRegistry,
    mats: &Mats,
    wants: &[Want],
    tags: RunTags,
) -> Result<Option<String>, Error> {
    if wants.is_empty() {
        return Ok(None);
    }
    // the same gate the build endpoints answer 409 on. checked before planning
    // rather than after: `build_one` refuses with an error a person is reading,
    // and a loop that tripped that every pass would be a log nobody reads
    if runner.store().has_active_run(ASSETS_JOB)? {
        tracing::debug!(
            assets = %wants.iter().map(|w| w.asset.as_str()).collect::<Vec<_>>().join(", "),
            "policy build held: an asset build is already running"
        );
        return Ok(None);
    }
    let targets: Vec<String> = wants.iter().map(|w| w.asset.clone()).collect();
    let named: HashMap<String, Vec<String>> = wants
        .iter()
        .filter(|w| !w.keys.is_empty())
        .map(|w| (w.asset.clone(), w.keys.clone()))
        .collect();
    let plan = plan_partitions(reg, mats, &targets, &named)?;
    let run_id = launch_plan(runner, plan, Trigger::Build, tags)?;
    Ok(Some(run_id))
}

/// every asset a source nothing has observed is holding back, mapped to that
/// source. computed in topo order, so it reaches as far down the graph as the
/// source does.
///
/// a source's fingerprint is what everything under it is compared against.
/// until a probe has written one there is nothing to compare, so those assets
/// are stale, will be stale again the moment they are built, and would be
/// rebuilt by a stale rule on every pass forever. that is the shape of an
/// [`auto`](crate::Asset::auto) asset with no probe upstream, which has never
/// fired and still does not.
fn unobserved_sources(reg: &AssetRegistry, mats: &Mats) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for meta in reg.topo() {
        if meta.source && mats.get(&meta.name, None).is_none() {
            out.insert(meta.name.clone(), meta.name.clone());
            continue;
        }
        if let Some(source) = meta.deps.iter().find_map(|dep| out.get(dep)).cloned() {
            out.insert(meta.name.clone(), source);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::model::Materialization;
    use crate::op::OpCtx;
    use crate::partition::{PartitionMapping, Partitions};
    use serde_json::{Value, json};

    fn at(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn mat(
        asset: &str,
        key: Option<&str>,
        fp: &str,
        inputs: Value,
        built: &str,
    ) -> Materialization {
        Materialization {
            id: 0,
            asset: asset.into(),
            partition: key.map(String::from),
            fingerprint: fp.into(),
            inputs,
            value: None,
            run_id: None,
            built_at: at(built),
            metadata: None,
        }
    }

    fn body(name: &str) -> Asset {
        Asset::new(name, |_: OpCtx| async { Ok(json!(null)) })
    }

    fn reg(assets: Vec<Asset>) -> AssetRegistry {
        AssetRegistry::new(assets, Vec::new(), Vec::new()).unwrap()
    }

    /// the verdict on the whole of an unpartitioned asset.
    fn verdict(reg: &AssetRegistry, rows: Vec<Materialization>, asset: &str, now: &str) -> Verdict {
        let mats: Mats = rows.into_iter().collect();
        Pass::new(reg, &mats, at(now)).verdict(reg.get(asset).expect("declared"), None)
    }

    /// a probe has run against the source, so everything below it is judged on
    /// its own merits rather than held back by it
    fn observed(fp: &str) -> Materialization {
        mat("s", None, fp, json!({}), "2026-08-14T00:00:00Z")
    }

    /// `a`, built at midnight against the source's first fingerprint
    fn built_a() -> Materialization {
        mat("a", None, "a1", json!({"s": "s1"}), "2026-08-14T00:00:00Z")
    }

    fn source() -> Asset {
        Asset::source("s").probe(|| async { Ok("s1".to_string()) })
    }

    /// source -> a, with a's policy the thing under test
    fn chain(policy: AutoPolicy) -> AssetRegistry {
        let s = source();
        let a = body("a").from(&s).policy(policy);
        reg(vec![s, a])
    }

    #[test]
    fn the_stale_rule_fires_on_a_dep_that_moved_and_not_on_one_that_did_not() {
        let reg = chain(AutoPolicy::when_stale());
        let now = "2026-08-14T09:00:00Z";

        // never built is stale, and stale is what this rule is about
        assert_eq!(
            verdict(&reg, vec![observed("s1")], "a", now),
            Verdict::Build
        );
        assert_eq!(
            verdict(&reg, vec![observed("s1"), built_a()], "a", now),
            Verdict::Idle
        );
        // the source's fingerprint moves and the same build is owed again
        assert_eq!(
            verdict(&reg, vec![observed("s2"), built_a()], "a", now),
            Verdict::Build
        );
    }

    #[test]
    fn the_missing_rule_fires_once_and_says_nothing_about_a_dep_that_moved() {
        let reg = chain(AutoPolicy::when_missing());
        let now = "2026-08-14T09:00:00Z";
        assert_eq!(
            verdict(&reg, vec![observed("s1")], "a", now),
            Verdict::Build
        );
        // built against a source that has since moved: stale, and not this
        // rule's business
        assert_eq!(
            verdict(&reg, vec![observed("s2"), built_a()], "a", now),
            Verdict::Idle
        );
    }

    #[test]
    fn the_cron_rule_waits_for_its_hour_and_does_not_build_a_fresh_asset() {
        let reg = chain(AutoPolicy::after_cron("0 2 * * *"));
        // built at midnight against a source that has since moved, so it is
        // stale all day
        let stale = vec![observed("s2"), built_a()];
        assert_eq!(
            verdict(&reg, stale.clone(), "a", "2026-08-14T01:00:00Z"),
            Verdict::Idle,
            "not 2am yet"
        );
        assert_eq!(
            verdict(&reg, stale, "a", "2026-08-14T02:30:00Z"),
            Verdict::Build
        );

        // once it has built for that occurrence it waits for the next one,
        // however stale it is
        let after = vec![
            observed("s2"),
            mat("a", None, "a1", json!({"s": "s1"}), "2026-08-14T02:05:00Z"),
        ];
        assert_eq!(
            verdict(&reg, after, "a", "2026-08-14T09:00:00Z"),
            Verdict::Idle
        );

        // and a fresh asset is nothing to rebuild, which is the whole of "and
        // only if stale by then"
        assert_eq!(
            verdict(
                &reg,
                vec![observed("s1"), built_a()],
                "a",
                "2026-08-14T02:30:00Z"
            ),
            Verdict::Idle
        );
    }

    #[test]
    fn a_cron_reads_the_clock_its_timezone_names() {
        let reg = chain(AutoPolicy::after_cron("0 2 * * *").tz("Europe/London"));
        // 02:00 in london is 01:00 utc in august, and the rule reads london
        assert_eq!(
            verdict(
                &reg,
                vec![observed("s2"), built_a()],
                "a",
                "2026-08-14T01:30:00Z"
            ),
            Verdict::Build
        );
    }

    #[test]
    fn a_cron_nothing_could_read_fails_the_build() {
        let bad = AssetRegistry::new(
            vec![body("a").policy(AutoPolicy::after_cron("not a cron"))],
            Vec::new(),
            Vec::new(),
        );
        let err = bad.err().expect("an unparseable expression").to_string();
        assert!(err.contains("asset a"), "{err}");
        assert!(err.contains("not a cron"), "{err}");

        let bad = AssetRegistry::new(
            vec![body("a").policy(AutoPolicy::after_cron("0 2 * * *").tz("Mars/Olympus"))],
            Vec::new(),
            Vec::new(),
        );
        let err = bad.err().expect("an unknown timezone").to_string();
        assert!(err.contains("Mars/Olympus"), "{err}");
    }

    #[test]
    fn a_policy_on_a_source_is_refused_however_it_is_spelled() {
        for asset in [
            Asset::source("s").policy(AutoPolicy::when_stale()),
            Asset::source("s").auto(),
        ] {
            let err = AssetRegistry::new(vec![asset], Vec::new(), Vec::new())
                .err()
                .expect("a source is probed, never built")
                .to_string();
            assert!(err.contains("policy on a source"), "{err}");
        }
    }

    // auto is the stale rule and nothing else, so a graph written before there
    // were policies decides exactly as it did
    #[test]
    fn auto_is_the_stale_rule() {
        let s = source();
        let a = body("a").from(&s).auto();
        let reg = reg(vec![s, a]);
        let policy = reg.get("a").unwrap().policy.clone();
        assert_eq!(policy, Some(AutoPolicy::when_stale()));
        assert_eq!(policy.unwrap().rule_word(), "stale");
        assert_eq!(
            verdict(&reg, vec![observed("s1")], "a", "2026-08-14T09:00:00Z"),
            Verdict::Build
        );
    }

    // the case that has never fired and still does not: with no probe upstream
    // there is nothing to compare a build against, so building would leave the
    // asset exactly as stale as it found it
    #[test]
    fn a_source_nothing_has_observed_holds_back_everything_under_it() {
        let s = Asset::source("s");
        let a = body("a").from(&s);
        let b = body("b").from(&a).auto();
        let reg = reg(vec![s, a, b]);
        let now = "2026-08-14T09:00:00Z";
        assert_eq!(
            verdict(&reg, Vec::new(), "b", now),
            Verdict::Waiting(Waiting::Source("s".into())),
            "the source is two edges up and still the answer"
        );
        // and the moment a probe writes one, the same policy goes
        assert_eq!(
            verdict(&reg, vec![observed("s1")], "b", now),
            Verdict::Build
        );
    }

    #[test]
    fn a_partitioned_asset_builds_the_keys_that_qualify_and_leaves_the_rest() {
        let s = source();
        let sales = Asset::new("sales", |_: OpCtx| async { Ok(json!(null)) })
            .from(&s)
            .partitioned(Partitions::keys(["r1", "r2", "r3"]))
            .policy(AutoPolicy::when_stale());
        let reg = reg(vec![s, sales]);
        // r1 was built against the source as it is: fresh. r2 against a
        // fingerprint that has moved on: stale. r3 has never been built
        let mats: Mats = vec![
            observed("s1"),
            mat(
                "sales",
                Some("r1"),
                "f1",
                json!({"s": "s1"}),
                "2026-08-14T01:00:00Z",
            ),
            mat(
                "sales",
                Some("r2"),
                "f2",
                json!({"s": "s0"}),
                "2026-08-14T01:00:00Z",
            ),
        ]
        .into_iter()
        .collect();

        let pass = Pass::new(&reg, &mats, at("2026-08-14T09:00:00Z"));
        let meta = reg.get("sales").unwrap();
        assert_eq!(pass.verdict(meta, Some("r1")), Verdict::Idle);
        assert_eq!(pass.verdict(meta, Some("r2")), Verdict::Build);
        assert_eq!(pass.verdict(meta, Some("r3")), Verdict::Build);
        // and the pass wants exactly those two, newest key first
        let wants = pass.wants(None);
        assert_eq!(wants.len(), 1);
        assert_eq!(wants[0].asset, "sales");
        assert_eq!(wants[0].rule, "stale");
        assert_eq!(wants[0].keys, ["r3", "r2"]);
    }

    #[test]
    fn a_pass_wants_no_more_keys_than_the_build_limit() {
        let reg = reg(vec![
            Asset::new("sales", |_: OpCtx| async { Ok(json!(null)) })
                .partitioned(Partitions::keys(["r1", "r2", "r3", "r4", "r5"]).build_limit(2))
                .policy(AutoPolicy::when_missing()),
        ]);
        let mats = Mats::default();
        let wants = Pass::new(&reg, &mats, at("2026-08-14T09:00:00Z")).wants(None);
        assert_eq!(wants[0].keys, ["r5", "r4"], "the newest keys, and no more");
    }

    /// a day, as its key
    fn day(back: i64) -> String {
        (Utc::now() - chrono::Duration::days(back))
            .format("%Y-%m-%d")
            .to_string()
    }

    /// the hours of a day, as their keys
    fn hours(day: &str, range: std::ops::Range<u32>) -> Vec<Materialization> {
        range
            .map(|n| {
                mat(
                    "hours",
                    Some(&format!("{day}T{n:02}")),
                    &format!("h{n}"),
                    json!({}),
                    "2026-08-14T00:00:00Z",
                )
            })
            .collect()
    }

    // the sets run from a few days back to now, so they are small and the day
    // before this one is a key whose 24 hours the hourly set holds in full
    fn rollup(policy: AutoPolicy) -> AssetRegistry {
        let hours = Asset::new("hours", |_: OpCtx| async { Ok(json!(null)) })
            .partitioned(Partitions::hourly(format!("{}T00", day(3))));
        let daily = Asset::new("daily", |_: OpCtx| async { Ok(json!(null)) })
            .reads(&hours, PartitionMapping::covering())
            .partitioned(Partitions::daily(day(3)))
            .policy(policy);
        reg(vec![hours, daily])
    }

    #[test]
    fn the_readiness_rule_waits_for_a_missing_hour_and_then_goes() {
        let reg = rollup(AutoPolicy::when_stale().and_upstream_ready());
        let yesterday = day(1);
        let meta = reg.get("daily").unwrap();

        let mats: Mats = hours(&yesterday, 0..23).into_iter().collect();
        let pass = Pass::new(&reg, &mats, Utc::now());
        assert_eq!(
            pass.verdict(meta, Some(&yesterday)),
            Verdict::Waiting(Waiting::Key {
                dep: "hours".into(),
                key: Some(format!("{yesterday}T23")),
            }),
            "23 of the 24 hours is a partial day, not a day"
        );
        assert!(
            pass.wants(None).is_empty(),
            "nothing to build while a key it reads is missing"
        );

        // the last hour lands
        let mats: Mats = hours(&yesterday, 0..24).into_iter().collect();
        let pass = Pass::new(&reg, &mats, Utc::now());
        assert_eq!(pass.verdict(meta, Some(&yesterday)), Verdict::Build);
        let wants = pass.wants(None);
        assert_eq!(wants.len(), 1);
        assert_eq!(
            wants[0].keys,
            [yesterday],
            "the day whose hours are all there, and no other"
        );
    }

    // the same graph without the guard: a rollup of the hours that happen to be
    // there is the default, and it goes stale as each of the rest lands
    #[test]
    fn without_the_guard_a_rollup_builds_the_part_of_the_day_it_has() {
        let reg = rollup(AutoPolicy::when_stale());
        let yesterday = day(1);
        let mats: Mats = hours(&yesterday, 0..1).into_iter().collect();
        assert_eq!(
            Pass::new(&reg, &mats, Utc::now()).verdict(reg.get("daily").unwrap(), Some(&yesterday)),
            Verdict::Build
        );
    }

    // a window over hours its dep will never hold can never be satisfied: the
    // day before the hourly set starts is not a day this asset can ever build
    #[test]
    fn a_window_that_can_never_be_filled_waits_rather_than_building_a_part_of_it() {
        let hours = Asset::new("hours", |_: OpCtx| async { Ok(json!(null)) })
            .partitioned(Partitions::hourly(format!("{}T12", day(2))));
        let daily = Asset::new("daily", |_: OpCtx| async { Ok(json!(null)) })
            .reads(&hours, PartitionMapping::covering())
            .partitioned(Partitions::daily(day(3)))
            .policy(AutoPolicy::when_stale());
        let reg = reg(vec![hours, daily]);
        let two_back = day(2);
        let mats = Mats::default();
        assert_eq!(
            Pass::new(&reg, &mats, Utc::now()).verdict(reg.get("daily").unwrap(), Some(&two_back)),
            Verdict::Waiting(Waiting::Never {
                dep: "hours".into(),
                key: format!("{two_back}T00"),
            }),
            "the hours before the set starts are not hours that arrive late"
        );
    }
}
