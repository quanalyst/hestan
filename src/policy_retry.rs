//! Automatic builds use retained run history for restart-safe failure backoff.
//! Manual builds bypass this boundary. No second retry store is maintained.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use serde_json::{Value, json};

use crate::asset::{
    ASSETS_JOB, AssetRegistry, BuildPlan, Mats, claimed_by, content_fingerprint, plan_partitions,
};
use crate::error::Error;
use crate::executor::Runner;
use crate::model::{OpStatus, RunTags};
use crate::partition::KeySet;

use super::Want;

/// Include transitive inputs: a failed, unmaterialized intermediate still has
/// to become eligible when the source beneath it changes.
fn inputs(reg: &AssetRegistry, mats: &Mats, asset: &str, key: Option<&str>) -> Value {
    fn visit(
        reg: &AssetRegistry,
        mats: &Mats,
        asset: &str,
        key: Option<&str>,
        held: &mut BTreeMap<(String, Option<String>), Option<String>>,
    ) {
        let identity = (asset.to_owned(), key.map(str::to_owned));
        if held.contains_key(&identity) {
            return;
        }
        held.insert(
            identity,
            mats.get(asset, key).map(|m| m.fingerprint.clone()),
        );
        let meta = reg.get(asset).expect("validated asset graph");
        for dep in &meta.deps {
            let upstream = reg.get(dep).expect("validated dependency");
            match &upstream.partitions {
                None => visit(reg, mats, dep, None, held),
                Some(spec) => {
                    let mapping = meta.mapping(dep);
                    if mapping.is_identity() {
                        visit(reg, mats, dep, key, held);
                    } else {
                        for at in mapping
                            .reads(meta.partitions.as_ref(), key, &KeySet::of(spec))
                            .keys
                        {
                            visit(reg, mats, dep, Some(&at), held);
                        }
                    }
                }
            }
        }
    }
    let mut held = BTreeMap::new();
    visit(reg, mats, asset, key, &mut held);
    json!(
        held.into_iter()
            .map(|((asset, key), fingerprint)| json!([asset, key, fingerprint]))
            .collect::<Vec<_>>()
    )
}

struct Admission<'a> {
    runner: &'a Runner,
    reg: &'a AssetRegistry,
    mats: &'a Mats,
    now: DateTime<Utc>,
    // The same ancestor can occur in many target plans; inspect its history once.
    checked: HashMap<String, (String, bool)>,
}

impl Admission<'_> {
    fn claim(&mut self, asset: &str, key: Option<&str>) -> Result<(String, String, bool), Error> {
        let tag = format!("auto:{}", content_fingerprint(&json!([asset, key])));
        if let Some((fingerprint, ready)) = self.checked.get(&tag) {
            return Ok((tag, fingerprint.clone(), *ready));
        }
        let fingerprint = content_fingerprint(&json!({
            "build": self.runner.deployment().build_id(),
            "inputs": inputs(self.reg, self.mats, asset, key),
        }));
        let op = self
            .reg
            .get(asset)
            .and_then(|m| m.op.as_deref())
            .expect("derived asset");
        let operation = key.map_or_else(|| op.to_owned(), |key| format!("{op}[{key}]"));
        let history = self.runner.store().runs(
            Some(ASSETS_JOB),
            None,
            None,
            None,
            Some((&tag, &fingerprint)),
            None,
            5,
        )?;
        let mut failures: u32 = 0;
        let mut finished = None;
        for run in history {
            let Some(at) = run.finished_at else { break };
            let failed = self
                .runner
                .store()
                .op_run(&run.id, &operation)?
                .is_some_and(|op| matches!(op.status, OpStatus::Failed | OpStatus::Canceled));
            if !failed {
                break;
            }
            finished.get_or_insert(at);
            failures += 1;
        }
        // Five minutes after the first exhausted run, doubling to one hour.
        // The operation's own retry policy has already run before this applies.
        let delay = (300_i64 * (1 << failures.saturating_sub(1))).min(3600);
        let ready = finished.is_none_or(|at| self.now >= at + chrono::Duration::seconds(delay));
        self.checked
            .insert(tag.clone(), (fingerprint.clone(), ready));
        Ok((tag, fingerprint, ready))
    }

    fn plan(&mut self, plan: &BuildPlan, tags: &mut RunTags) -> Result<bool, Error> {
        for claim in claimed_by(self.reg, plan).as_array().expect("claims array") {
            let (tag, fingerprint, ready) = self.claim(
                claim["asset"].as_str().expect("asset name"),
                claim["partition"].as_str(),
            )?;
            if !ready {
                return Ok(false);
            }
            tags.insert(tag, fingerprint);
        }
        Ok(true)
    }
}

/// Filter each requested key against its entire dependency plan, then apply
/// the build limit. A failing upstream cannot be retried through a descendant.
pub(super) fn admit(
    runner: &Runner,
    reg: &AssetRegistry,
    mats: &Mats,
    wants: &[Want],
    now: DateTime<Utc>,
) -> Result<(Vec<Want>, RunTags), Error> {
    let mut admission = Admission {
        runner,
        reg,
        mats,
        now,
        checked: HashMap::new(),
    };
    let mut accepted = Vec::new();
    let mut tags = RunTags::new();
    for want in wants {
        let mut eligible = Want {
            asset: want.asset.clone(),
            rule: want.rule,
            keys: vec![],
        };
        let keys = if want.keys.is_empty() {
            vec![None]
        } else {
            want.keys.iter().map(|key| Some(key.as_str())).collect()
        };
        let limit = reg
            .get(&want.asset)
            .and_then(|m| m.partitions.as_ref())
            .map_or(1, |spec| spec.limit());
        let mut count = 0;
        for key in keys {
            let named = key
                .map(|key| HashMap::from([(want.asset.clone(), vec![key.to_owned()])]))
                .unwrap_or_default();
            let plan = plan_partitions(reg, mats, std::slice::from_ref(&want.asset), &named)?;
            let mut attempt_tags = RunTags::new();
            if !admission.plan(&plan, &mut attempt_tags)? {
                continue;
            }
            if let Some(key) = key {
                eligible.keys.push(key.to_owned());
            }
            tags.extend(attempt_tags);
            count += 1;
            if count >= limit {
                break;
            }
        }
        if count > 0 {
            accepted.push(eligible);
        }
    }
    Ok((accepted, tags))
}
