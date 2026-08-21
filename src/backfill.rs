use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::asset::{
    ASSETS_JOB, AssetRegistry, asset_tag, check_named_keys, launch_plan, mats_map, plan_partitions,
    staleness,
};
use crate::error::Error;
use crate::executor::Runner;
use crate::model::{Backfill, BackfillStatus, RunStatus, Trigger};

/// how often the chunker looks at what its runs did. a backfill's runs are
/// minutes-long things; this only decides how quickly the next chunk follows
/// the last, and polling the store is cheap.
const TICK: Duration = Duration::from_secs(2);

/// start a backfill of `asset` over the key range `from..=to`.
///
/// the range resolves against the asset's key set now, and is then fixed: a
/// daily set grows, and a backfill should build what it was asked for rather
/// than whatever the range means tomorrow. `only_missing` drops the keys that
/// are already materialized and fresh, which is what makes re-running a
/// backfill after a partial failure cheap.
///
/// the first chunk goes out here if nothing else is building; otherwise the
/// record simply waits for [`tick`] to pick it up, the same self-heal the
/// probe path uses.
pub(crate) fn start(
    runner: &Runner,
    registry: &AssetRegistry,
    asset: &str,
    from: &str,
    to: &str,
    only_missing: bool,
) -> Result<Backfill, Error> {
    let Some(meta) = registry.get(asset) else {
        return Err(Error::UnknownAsset(asset.to_string()));
    };
    let Some(spec) = &meta.partitions else {
        return Err(Error::Graph(format!(
            "asset {asset} is not partitioned; there is no range to backfill"
        )));
    };
    // one at a time per asset: two backfills of one asset would interleave
    // their chunks and record lineage neither of them asked for
    if let Some(running) = runner
        .store()
        .running_backfills()?
        .into_iter()
        .find(|b| b.asset == asset)
    {
        return Err(Error::Conflict(format!(
            "backfill {} of {asset} is still running",
            running.id
        )));
    }
    let mut keys = spec.range(from, to)?;
    // the range names its keys, so a key nothing could build is said here
    // rather than skipped in every chunk that reaches it
    check_named_keys(registry, asset, &keys)?;
    if only_missing {
        let mats = mats_map(runner.store())?;
        let verdict = &staleness(registry, &mats)[asset];
        keys.retain(|key| verdict.parts.get(key).is_none_or(|s| s.stale));
    }
    let id = runner
        .store()
        .create_backfill(asset, from, to, &keys, runner.actor())?;
    let backfill = runner
        .store()
        .backfill(id)?
        .expect("the row was just written");
    if backfill.status == BackfillStatus::Running {
        launch_next(runner, registry, &backfill)?;
    }
    runner
        .store()
        .backfill(id)?
        .ok_or(Error::UnknownBackfill(id))
}

/// stop a running backfill: the run in flight is asked to cancel, and no
/// further chunk goes out. a finished backfill is left alone.
pub(crate) fn cancel(runner: &Runner, id: i64) -> Result<bool, Error> {
    let Some(backfill) = runner.store().backfill(id)? else {
        return Err(Error::UnknownBackfill(id));
    };
    if backfill.status != BackfillStatus::Running {
        return Ok(false);
    }
    if let Some(run_id) = backfill.run_ids.last() {
        runner.cancel(run_id)?;
    }
    // recorded before the run finishes: the status is the request, and the
    // chunker must not send another one in the gap
    close(runner, &backfill, BackfillStatus::Canceled)?;
    Ok(true)
}

/// one pass over every running backfill: close the ones whose run finished
/// badly, and send the next chunk for the ones whose run finished well.
pub(crate) fn tick(runner: &Runner, registry: &AssetRegistry) -> Result<(), Error> {
    for backfill in runner.store().running_backfills()? {
        let Some(run_id) = backfill.run_ids.last() else {
            // nothing launched yet: the gate was closed when it was made
            launch_next(runner, registry, &backfill)?;
            continue;
        };
        let status = match runner.store().run(run_id)? {
            Some(run) => run.status,
            // the run it was waiting on is gone (retention, a wiped db). the
            // backfill cannot be finished honestly, so it is failed rather
            // than left running forever
            None => RunStatus::Failed,
        };
        match status {
            RunStatus::Queued | RunStatus::Running => {}
            RunStatus::Success if backfill.launched < backfill.total => {
                launch_next(runner, registry, &backfill)?;
            }
            RunStatus::Success => close(runner, &backfill, BackfillStatus::Complete)?,
            RunStatus::Failed => close(runner, &backfill, BackfillStatus::Failed)?,
            RunStatus::Canceled => close(runner, &backfill, BackfillStatus::Canceled)?,
        }
    }
    Ok(())
}

/// close a backfill out, with what it managed to launch. the store wants the
/// asset and the counts for the [event](crate::EventKind::BackfillFinished) it
/// writes beside the status, and every caller here has the row in hand.
fn close(runner: &Runner, backfill: &Backfill, status: BackfillStatus) -> Result<(), Error> {
    runner.store().finish_backfill(
        backfill.id,
        &backfill.asset,
        status,
        backfill.launched,
        backfill.total,
    )
}

/// launch the next chunk of `backfill`, capped at the asset's build limit,
/// which is the point of the whole exercise, since a 400-day range fired as
/// one run
/// would be 400 instances at somebody's api at once.
fn launch_next(
    runner: &Runner,
    registry: &AssetRegistry,
    backfill: &Backfill,
) -> Result<(), Error> {
    // the same gate the build endpoints answer 409 on: while any assets run is
    // active, wait for the next tick rather than overlapping lineage writes
    if runner.store().has_active_run(ASSETS_JOB)? {
        return Ok(());
    }
    let limit = registry
        .get(&backfill.asset)
        .and_then(|m| m.partitions.as_ref())
        .map_or(1, |spec| spec.limit());
    let chunk: Vec<String> = backfill
        .partitions
        .iter()
        .skip(backfill.launched)
        .take(limit)
        .cloned()
        .collect();
    if chunk.is_empty() {
        close(runner, backfill, BackfillStatus::Complete)?;
        return Ok(());
    }
    let mats = mats_map(runner.store())?;
    let named = HashMap::from([(backfill.asset.clone(), chunk.clone())]);
    let targets = std::slice::from_ref(&backfill.asset);
    let plan = plan_partitions(registry, &mats, targets, &named)?;
    // which asset and which backfill: `build` says neither, and a chunk with
    // no way back to the backfill it belongs to is a run you cannot follow
    let mut tags = asset_tag(&backfill.asset);
    tags.insert("backfill".to_string(), backfill.id.to_string());
    let run_id = launch_plan(runner, plan, Trigger::Build, tags)?;
    runner.store().backfill_launched(
        backfill.id,
        &backfill.asset,
        &run_id,
        backfill.launched + chunk.len(),
        backfill.total,
    )?;
    tracing::info!(
        backfill = backfill.id,
        asset = %backfill.asset,
        run = %run_id,
        "backfill chunk of {} partitions launched",
        chunk.len()
    );
    Ok(())
}

/// the loop `serve` runs: [`tick`] every [`TICK`], forever.
pub(crate) async fn run_backfills(runner: Runner, registry: Arc<AssetRegistry>) {
    let mut ticker = tokio::time::interval(TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        // a chunk is a decision: two processes sending the next one is two
        // runs over the same partitions, writing each other's lineage. waited
        // on rather than polled, so a process that takes the lease sends the
        // next chunk at once rather than on its next tick
        runner.deciding().wait().await;
        if let Err(e) = tick(&runner, &registry) {
            tracing::warn!("backfill tick failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::Asset;
    use crate::op::OpCtx;
    use crate::partition::Partitions;
    use crate::store::Store;
    use serde_json::json;

    // five keys and a build limit of two: enough for three chunks
    fn regions() -> Arc<AssetRegistry> {
        let sales = Asset::new("sales", |ctx: OpCtx| async move {
            Ok(json!({ "region": ctx.partition() }))
        })
        .partitioned(Partitions::keys(["r1", "r2", "r3", "r4", "r5"]).build_limit(2));
        Arc::new(AssetRegistry::new(vec![sales], Vec::new(), Vec::new()).unwrap())
    }

    fn runner_for(reg: &AssetRegistry, store: Store) -> Runner {
        Runner::new([reg.lower_job().unwrap()], store).unwrap()
    }

    async fn settle(runner: &Runner, reg: &AssetRegistry) {
        // let the launched run finish, then let the chunker see that it did
        for _ in 0..300 {
            let active = runner.store().has_active_run(ASSETS_JOB).unwrap();
            if !active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tick(runner, reg).unwrap();
    }

    // `build` is what a chunk's trigger says; which asset and which backfill
    // is what it cannot, and a chunk you cannot trace back is a run adrift
    #[tokio::test]
    async fn a_chunk_run_is_tagged_with_its_asset_and_backfill() {
        let store = Store::open(":memory:").unwrap();
        let reg = regions();
        let runner = runner_for(&reg, store.clone());

        let b = start(&runner, &reg, "sales", "r1", "r5", true).unwrap();
        let run = store.run(&b.run_ids[0]).unwrap().unwrap();
        assert_eq!(run.trigger, Trigger::Build);
        assert_eq!(run.tags["asset"], "sales");
        assert_eq!(run.tags["backfill"], b.id.to_string());

        // and the filter finds every chunk of one backfill, which is the point
        settle(&runner, &reg).await;
        let tag = Some(("backfill", b.id.to_string()));
        let chunks = store
            .runs(
                None,
                None,
                None,
                None,
                tag.as_ref().map(|(k, v)| (*k, v.as_str())),
                10,
            )
            .unwrap();
        assert_eq!(chunks.len(), 2);
    }

    #[tokio::test]
    async fn a_range_resolves_and_chunks_into_successive_runs() {
        let store = Store::open(":memory:").unwrap();
        let reg = regions();
        let runner = runner_for(&reg, store.clone());

        let b = start(&runner, &reg, "sales", "r1", "r5", true).unwrap();
        assert_eq!(b.partitions, ["r1", "r2", "r3", "r4", "r5"]);
        assert_eq!(b.total, 5);
        // the first chunk went out at once, and it is one chunk, not five
        assert_eq!(b.launched, 2);
        assert_eq!(b.run_ids.len(), 1);
        assert_eq!(b.status, BackfillStatus::Running);

        settle(&runner, &reg).await;
        let b = store.backfill(b.id).unwrap().unwrap();
        assert_eq!((b.launched, b.run_ids.len()), (4, 2));
        settle(&runner, &reg).await;
        let b = store.backfill(b.id).unwrap().unwrap();
        assert_eq!((b.launched, b.run_ids.len()), (5, 3));
        // the last chunk finishing is what completes the record
        assert_eq!(b.status, BackfillStatus::Running);
        settle(&runner, &reg).await;
        let b = store.backfill(b.id).unwrap().unwrap();
        assert_eq!(b.status, BackfillStatus::Complete);
        assert!(b.finished_at.is_some());

        // every key of the range materialized, one run apiece for its chunk
        for key in ["r1", "r2", "r3", "r4", "r5"] {
            assert_eq!(
                store
                    .materialization("sales", Some(key))
                    .unwrap()
                    .unwrap()
                    .value,
                Some(json!({ "region": key }))
            );
        }
        // and a further tick does not resurrect a finished backfill
        tick(&runner, &reg).unwrap();
        assert_eq!(store.backfills(10).unwrap()[0].run_ids.len(), 3);
    }

    #[tokio::test]
    async fn only_missing_skips_what_is_already_materialized() {
        let store = Store::open(":memory:").unwrap();
        let reg = regions();
        let runner = runner_for(&reg, store.clone());
        for key in ["r1", "r2"] {
            store
                .record_materialization("sales", Some(key), "fp", &json!({}), None, None, None)
                .unwrap();
        }

        let b = start(&runner, &reg, "sales", "r1", "r4", true).unwrap();
        assert_eq!(b.partitions, ["r3", "r4"], "fresh keys were backfilled");

        // and asking for the range regardless takes it whole
        cancel(&runner, b.id).unwrap();
        let b = start(&runner, &reg, "sales", "r1", "r4", false).unwrap();
        assert_eq!(b.partitions, ["r1", "r2", "r3", "r4"]);

        // a range with nothing missing is complete on arrival rather than a
        // record that never finishes
        cancel(&runner, b.id).unwrap();
        let b = start(&runner, &reg, "sales", "r1", "r2", true).unwrap();
        assert_eq!(b.total, 0);
        assert_eq!(b.status, BackfillStatus::Complete);
    }

    #[tokio::test]
    async fn one_backfill_per_asset_at_a_time() {
        let store = Store::open(":memory:").unwrap();
        let reg = regions();
        let runner = runner_for(&reg, store.clone());
        let first = start(&runner, &reg, "sales", "r1", "r5", true).unwrap();

        let err = start(&runner, &reg, "sales", "r1", "r2", true).unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err}");
        assert!(err.to_string().contains("is still running"), "{err}");

        // once it is over, another may start
        cancel(&runner, first.id).unwrap();
        start(&runner, &reg, "sales", "r1", "r2", false).unwrap();
    }

    #[tokio::test]
    async fn cancel_stops_the_chunking_and_the_run_in_flight() {
        let store = Store::open(":memory:").unwrap();
        let reg = regions();
        let runner = runner_for(&reg, store.clone());
        let b = start(&runner, &reg, "sales", "r1", "r5", true).unwrap();
        assert_eq!(b.launched, 2);

        assert!(cancel(&runner, b.id).unwrap());
        let b = store.backfill(b.id).unwrap().unwrap();
        assert_eq!(b.status, BackfillStatus::Canceled);
        assert!(b.finished_at.is_some());
        assert!(b.launched < b.total, "the whole range went out anyway");

        // no further chunk, however many ticks go by
        settle(&runner, &reg).await;
        tick(&runner, &reg).unwrap();
        let after = store.backfill(b.id).unwrap().unwrap();
        assert_eq!(after.launched, b.launched);
        assert_eq!(after.run_ids.len(), 1);
        assert_eq!(after.status, BackfillStatus::Canceled);

        // cancelling a finished backfill says so rather than pretending
        assert!(!cancel(&runner, b.id).unwrap());
        let err = cancel(&runner, 999).unwrap_err();
        assert!(matches!(err, Error::UnknownBackfill(999)), "{err}");
    }

    #[tokio::test]
    async fn a_failed_run_fails_the_backfill() {
        let store = Store::open(":memory:").unwrap();
        let broken = Asset::new("sales", |_| async { Err("no data".into()) })
            .partitioned(Partitions::keys(["r1", "r2", "r3"]).build_limit(1));
        let reg = Arc::new(AssetRegistry::new(vec![broken], Vec::new(), Vec::new()).unwrap());
        let runner = runner_for(&reg, store.clone());

        let b = start(&runner, &reg, "sales", "r1", "r3", true).unwrap();
        settle(&runner, &reg).await;
        let b = store.backfill(b.id).unwrap().unwrap();
        assert_eq!(b.status, BackfillStatus::Failed);
        assert_eq!(b.launched, 1, "chunking carried on past a failure");
    }

    #[tokio::test]
    async fn a_backfill_needs_a_partitioned_asset() {
        let store = Store::open(":memory:").unwrap();
        let flat = Asset::new("flat", |_| async { Ok(json!(null)) });
        let reg = Arc::new(AssetRegistry::new(vec![flat], Vec::new(), Vec::new()).unwrap());
        let runner = runner_for(&reg, store);

        let err = start(&runner, &reg, "flat", "a", "b", true).unwrap_err();
        assert!(err.to_string().contains("is not partitioned"), "{err}");
        let err = start(&runner, &reg, "ghost", "a", "b", true).unwrap_err();
        assert!(matches!(err, Error::UnknownAsset(_)), "{err}");
    }
}
