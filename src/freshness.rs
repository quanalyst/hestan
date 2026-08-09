use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};

use crate::asset::{AssetMeta, AssetRegistry, Mats, mats_map};
use crate::error::Error;
use crate::executor::{Runner, fire_hooks};
use crate::model::Freshness;

/// how often the checker re-evaluates every declared policy. a policy is a
/// claim about hours or days, so a minute of lag on noticing one broke is
/// noise, and polling harder would only cost database reads.
const CHECK_EVERY: Duration = Duration::from_secs(60);

/// which side of the api a [`LateEvent`] is about. jobs are late against their
/// last successful run, assets against their last materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LateKind {
    Asset,
    Job,
}

impl LateKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            LateKind::Asset => "asset",
            LateKind::Job => "job",
        }
    }
}

/// what an [`on_late`](crate::Hestan::on_late) hook receives: something with a
/// declared freshness policy has just crossed from fresh to late. one event per
/// crossing, not one per poll — the crossing is the news.
#[derive(Debug, Clone, Serialize)]
pub struct LateEvent {
    pub kind: LateKind,
    pub name: String,
    /// how far past the policy's deadline it is, the moment it crossed.
    #[serde(rename = "late_by_secs", serialize_with = "as_secs")]
    pub late_by: Duration,
    /// the success the deadline was measured from; `None` never happens here,
    /// since something that never succeeded is [`Freshness::Never`] and not
    /// late, but the shape matches what the api reports.
    pub last_success: Option<DateTime<Utc>>,
}

fn as_secs<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(d.as_secs())
}

/// a callback invoked on its own task when something crosses into late.
pub type LateHook = Arc<dyn Fn(LateEvent) + Send + Sync>;

/// one thing that declared a policy, and what that policy says right now.
pub(crate) struct Verdict {
    pub kind: LateKind,
    pub name: String,
    pub freshness: Freshness,
    pub last_success: Option<DateTime<Utc>>,
}

impl Verdict {
    /// the event a crossing into late would carry. `None` unless it is late.
    fn late_event(&self) -> Option<LateEvent> {
        Some(LateEvent {
            kind: self.kind,
            name: self.name.clone(),
            late_by: self.freshness.late_by()?,
            last_success: self.last_success,
        })
    }
}

/// the success an asset's policy is measured from: its **oldest** key's build
/// time, so a partitioned asset is late as soon as any one key is. keys that
/// were never built are ignored — a key with no build has no age, and the
/// missing count already says so — and an asset with no build at all reports
/// `None`, which reads as [`Freshness::Never`].
pub(crate) fn asset_last_success(mats: &Mats, meta: &AssetMeta) -> Option<DateTime<Utc>> {
    match &meta.partitions {
        None => mats.get(&meta.name, None).map(|m| m.built_at),
        Some(spec) => spec
            .keys_now()
            .iter()
            .filter_map(|key| mats.get(&meta.name, Some(key)).map(|m| m.built_at))
            .min(),
    }
}

/// what an asset's declared policy says at `now`; `None` when it declared none.
pub(crate) fn asset_freshness(
    mats: &Mats,
    meta: &AssetMeta,
    now: DateTime<Utc>,
) -> Option<(Freshness, Option<DateTime<Utc>>)> {
    let within = meta.fresh_within?;
    let last = asset_last_success(mats, meta);
    Some((Freshness::of(last, within, now), last))
}

/// every declared policy's verdict right now, jobs before assets and each
/// group by name. this is what the checker walks and what `GET /api/late`
/// reports, so the badge and the alert can never disagree.
pub(crate) fn verdicts(
    runner: &Runner,
    registry: &AssetRegistry,
    now: DateTime<Utc>,
) -> Result<Vec<Verdict>, Error> {
    let mut out = Vec::new();
    let mut jobs: Vec<&String> = runner.jobs().keys().collect();
    jobs.sort();
    for name in jobs {
        let Some(within) = runner.jobs()[name].fresh_within() else {
            continue;
        };
        let last = runner.store().last_success(name)?;
        out.push(Verdict {
            kind: LateKind::Job,
            name: name.clone(),
            freshness: Freshness::of(last, within, now),
            last_success: last,
        });
    }
    // one read for every asset, like the assets page does: a per-asset query
    // would be one round trip per key on a partitioned one
    if registry.topo().any(|m| m.fresh_within.is_some()) {
        let mats = mats_map(runner.store())?;
        let mut metas: Vec<&AssetMeta> = registry.topo().collect();
        metas.sort_by(|a, b| a.name.cmp(&b.name));
        for meta in metas {
            if let Some((freshness, last)) = asset_freshness(&mats, meta, now) {
                out.push(Verdict {
                    kind: LateKind::Asset,
                    name: meta.name.clone(),
                    freshness,
                    last_success: last,
                });
            }
        }
    }
    Ok(out)
}

/// evaluate every policy once and record the crossings, returning the ones
/// that just went late. the stored state is what makes this fire once per
/// transition: a job late for a week matches its row every minute and says
/// nothing, and going fresh again clears the row so the next relapse is news.
pub(crate) fn check_once(
    runner: &Runner,
    registry: &AssetRegistry,
    now: DateTime<Utc>,
) -> Vec<LateEvent> {
    let verdicts = match verdicts(runner, registry, now) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("freshness check failed: {e}");
            return Vec::new();
        }
    };
    let known: HashMap<(String, String), bool> = match runner.store().freshness_states() {
        Ok(rows) => rows
            .into_iter()
            .map(|r| ((r.kind, r.name), r.late))
            .collect(),
        Err(e) => {
            // without the stored state every late thing would look like a fresh
            // crossing, so say nothing this pass rather than page the world
            tracing::warn!("freshness state read failed: {e}");
            return Vec::new();
        }
    };
    let mut fired = Vec::new();
    for v in verdicts {
        let late = v.freshness.is_late();
        let was = known
            .get(&(v.kind.as_str().to_string(), v.name.clone()))
            .copied()
            .unwrap_or(false);
        if late == was {
            continue;
        }
        // the write comes first: a hook that takes a second must not leave a
        // crash re-announcing the same crossing on the next boot
        if let Err(e) =
            runner
                .store()
                .set_freshness_state(v.kind.as_str(), &v.name, late, late.then_some(now))
        {
            tracing::warn!(name = %v.name, "freshness state write failed: {e}");
            continue;
        }
        match v.late_event() {
            Some(event) => {
                tracing::warn!(
                    kind = %v.kind.as_str(),
                    name = %v.name,
                    late_by_secs = event.late_by.as_secs(),
                    "freshness policy missed"
                );
                fired.push(event);
            }
            None => tracing::info!(kind = %v.kind.as_str(), name = %v.name, "freshness recovered"),
        }
    }
    fired
}

/// the checker loop: its own task next to the scheduler and the sensor loop,
/// evaluating every declared policy on [`CHECK_EVERY`] and handing each
/// crossing to the hooks.
pub(crate) async fn run_checker(
    runner: Runner,
    registry: Arc<AssetRegistry>,
    hooks: Arc<Vec<LateHook>>,
) {
    let declared = runner.jobs().values().any(|j| j.fresh_within().is_some())
        || registry.topo().any(|m| m.fresh_within.is_some());
    if !declared {
        return;
    }
    loop {
        for event in check_once(&runner, &registry, Utc::now()) {
            fire_hooks(&hooks, event, "late");
        }
        tokio::time::sleep(CHECK_EVERY).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::job::Job;
    use crate::model::{Run, RunStatus, Trigger};
    use crate::op::Op;
    use crate::partition::Partitions;
    use crate::store::Store;
    use serde_json::json;
    use std::sync::Mutex;

    const HOUR: Duration = Duration::from_secs(3600);

    fn hourly_job(name: &str) -> Job {
        Job::builder(name)
            .fresh_within(HOUR)
            .op(Op::new("noop", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap()
    }

    // a success finished at a chosen time: freshness is entirely about how old
    // one is, and waiting an hour for a real one is not a test
    fn plant_success(store: &Store, job: &str, at: DateTime<Utc>) {
        let run = Run {
            id: crate::model::new_run_id(),
            job: job.into(),
            status: RunStatus::Queued,
            trigger: Trigger::Schedule,
            params: json!({}),
            created_at: at,
            started_at: Some(at),
            finished_at: None,
            error: None,
            resumed_from: None,
        };
        store.create_run(&run, &[]).unwrap();
        store
            .run_finished(&run.id, RunStatus::Success, None)
            .unwrap();
        store.backdate_run(&run.id, at).unwrap();
    }

    fn plant_materialization(store: &Store, asset: &str, key: Option<&str>, at: DateTime<Utc>) {
        store
            .record_materialization(asset, key, "fp", &json!({}), None, None, None)
            .unwrap();
        store.backdate_materialization(asset, key, at).unwrap();
    }

    fn registry(assets: Vec<Asset>) -> Arc<AssetRegistry> {
        Arc::new(AssetRegistry::new(assets, Vec::new(), Vec::new()).unwrap())
    }

    #[test]
    fn a_job_is_fresh_late_or_never_against_its_last_success() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([hourly_job("etl")], store.clone());
        let reg = AssetRegistry::empty();
        let now = Utc::now();

        let never = verdicts(&runner, &reg, now).unwrap();
        assert_eq!(never.len(), 1);
        assert_eq!(never[0].freshness, Freshness::Never);
        assert_eq!(never[0].freshness.status(), "never");
        assert!(!never[0].freshness.is_late());

        plant_success(&store, "etl", now - chrono::Duration::minutes(90));
        let late = &verdicts(&runner, &reg, now).unwrap()[0];
        assert_eq!(late.freshness.status(), "late");
        // 90 minutes since, an hour allowed: half an hour past the deadline
        assert_eq!(late.freshness.late_by().unwrap().as_secs(), 1800);
        assert_eq!(late.kind, LateKind::Job);

        // the newest success is the one that counts, not the one just read
        plant_success(&store, "etl", now - chrono::Duration::minutes(10));
        assert_eq!(
            verdicts(&runner, &reg, now).unwrap()[0].freshness,
            Freshness::Fresh
        );
    }

    #[test]
    fn an_asset_is_fresh_late_or_never_against_its_last_build() {
        let store = Store::open(":memory:").unwrap();
        let reg = registry(vec![
            Asset::new("report", |_| async { Ok(json!(1)) }).fresh_within(HOUR),
        ]);
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let now = Utc::now();

        assert_eq!(
            verdicts(&runner, &reg, now).unwrap()[0].freshness,
            Freshness::Never
        );
        plant_materialization(&store, "report", None, now - chrono::Duration::hours(3));
        let v = &verdicts(&runner, &reg, now).unwrap()[0];
        assert_eq!(v.kind, LateKind::Asset);
        assert_eq!(v.freshness.late_by().unwrap().as_secs(), 7200);

        plant_materialization(&store, "report", None, now - chrono::Duration::minutes(10));
        assert_eq!(
            verdicts(&runner, &reg, now).unwrap()[0].freshness,
            Freshness::Fresh
        );
    }

    #[test]
    fn one_late_partition_makes_the_whole_asset_late() {
        let store = Store::open(":memory:").unwrap();
        let reg = registry(vec![
            Asset::new("daily", |_| async { Ok(json!(1)) })
                .partitioned(Partitions::keys(["a", "b", "c"]))
                .fresh_within(HOUR),
        ]);
        let runner = Runner::new([reg.lower_job().unwrap()], store.clone());
        let now = Utc::now();

        let fresh = now - chrono::Duration::minutes(5);
        plant_materialization(&store, "daily", Some("a"), fresh);
        plant_materialization(&store, "daily", Some("b"), fresh);
        assert_eq!(
            verdicts(&runner, &reg, now).unwrap()[0].freshness,
            Freshness::Fresh,
            "a key that was never built has no age to be late by"
        );

        plant_materialization(&store, "daily", Some("c"), now - chrono::Duration::hours(2));
        let v = &verdicts(&runner, &reg, now).unwrap()[0];
        assert!(v.freshness.is_late(), "one stale key must carry the asset");
        assert_eq!(v.freshness.late_by().unwrap().as_secs(), 3600);
        assert_eq!(v.last_success, Some(now - chrono::Duration::hours(2)));
    }

    #[test]
    fn the_hook_fires_once_per_transition_and_again_after_a_relapse() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([hourly_job("etl")], store.clone());
        let reg = AssetRegistry::empty();
        let now = Utc::now();

        // never succeeded: not late, so nothing to say
        assert!(check_once(&runner, &reg, now).is_empty());

        plant_success(&store, "etl", now - chrono::Duration::hours(2));
        let fired = check_once(&runner, &reg, now);
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].name, "etl");
        assert_eq!(fired[0].kind, LateKind::Job);
        assert_eq!(fired[0].late_by.as_secs(), 3600);
        assert_eq!(
            fired[0].last_success,
            Some(now - chrono::Duration::hours(2))
        );

        // still late an hour later, and every poll in between: silence
        assert!(check_once(&runner, &reg, now).is_empty());
        assert!(
            check_once(&runner, &reg, now + chrono::Duration::hours(1)).is_empty(),
            "a job late for a week must not page hourly"
        );

        // recovery is not an alert, but it does re-arm the next one
        plant_success(&store, "etl", now);
        assert!(check_once(&runner, &reg, now).is_empty());
        let state = &store.freshness_states().unwrap()[0];
        assert!(!state.late);
        assert_eq!(state.since, None);

        let later = now + chrono::Duration::hours(3);
        let fired = check_once(&runner, &reg, later);
        assert_eq!(fired.len(), 1, "a relapse is news again");
        assert_eq!(fired[0].late_by.as_secs(), 7200);
    }

    #[test]
    fn late_state_survives_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hestan.db");
        let path = path.to_str().unwrap().to_string();
        let now = Utc::now();

        let store = Store::open(&path).unwrap();
        let runner = Runner::new([hourly_job("etl")], store.clone());
        plant_success(&store, "etl", now - chrono::Duration::hours(2));
        assert_eq!(check_once(&runner, &AssetRegistry::empty(), now).len(), 1);
        let since = store.freshness_states().unwrap()[0].since;
        assert!(since.is_some());
        drop(runner);
        drop(store);

        let store = Store::open(&path).unwrap();
        let runner = Runner::new([hourly_job("etl")], store.clone());
        assert!(
            check_once(
                &runner,
                &AssetRegistry::empty(),
                now + chrono::Duration::hours(1)
            )
            .is_empty(),
            "the crossing was already announced before the restart"
        );
        assert_eq!(store.freshness_states().unwrap()[0].since, since);
    }

    #[tokio::test]
    async fn the_checker_hands_each_crossing_to_the_hooks() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([hourly_job("etl")], store.clone());
        let seen: Arc<Mutex<Vec<LateEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let hooks: Arc<Vec<LateHook>> = Arc::new(vec![Arc::new(move |e: LateEvent| {
            sink.lock().unwrap().push(e);
        })]);
        plant_success(&store, "etl", Utc::now() - chrono::Duration::hours(2));

        let handle = tokio::spawn(run_checker(
            runner,
            Arc::new(AssetRegistry::empty()),
            hooks.clone(),
        ));
        for _ in 0..100 {
            if !seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        handle.abort();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].name, "etl");
        assert_eq!(
            serde_json::to_value(&seen[0]).unwrap()["late_by_secs"],
            json!(3600)
        );
    }

    #[tokio::test]
    async fn the_checker_stays_asleep_when_nothing_declares_a_policy() {
        let store = Store::open(":memory:").unwrap();
        let plain = Job::builder("etl")
            .op(Op::new("noop", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap();
        let runner = Runner::new([plain], store.clone());
        let handle = tokio::spawn(run_checker(
            runner,
            Arc::new(AssetRegistry::empty()),
            Arc::new(Vec::new()),
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(handle.is_finished());
        assert!(store.freshness_states().unwrap().is_empty());
    }
}
