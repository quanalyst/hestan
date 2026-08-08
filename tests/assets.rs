use std::time::Duration;

use hestan::prelude::*;
use hestan::{Error, RunStatus, Store, Trigger};

fn doc_assets() -> Vec<Asset> {
    let docs = Asset::source("docs");
    let stats = Asset::new("stats", |ctx| async move {
        // a source dep is lineage, not data: its input is null
        assert_eq!(ctx.input("docs"), Some(&Value::Null));
        Ok(json!({"files": 3}))
    })
    .from(&docs);
    let totals = Asset::new("totals", |ctx| async move {
        let files = ctx.input("stats").unwrap()["files"].as_u64().unwrap();
        Ok(json!({"total": files * 2}))
    })
    .from(&stats);
    vec![docs, stats, totals]
}

#[tokio::test]
async fn build_asset_runs_headless_like_run_once() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();

    // the write api is crate-internal, so plant the probe row by hand
    drop(Store::open(db).unwrap()); // create + migrate the file
    rusqlite::Connection::open(db)
        .unwrap()
        .execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, built_at)
             VALUES ('docs', 'd1', '{}', ?1)",
            [chrono::Utc::now().to_rfc3339()],
        )
        .unwrap();

    let run = Hestan::new()
        .assets(doc_assets())
        .db(db)
        .build_asset("totals")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(run.job, "assets");
    assert_eq!(run.trigger, Trigger::Build);

    let store = Store::open(db).unwrap();
    let ops = store.op_runs(&run.id).unwrap();
    let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    assert_eq!(names, ["stats", "totals"]);
    let stats = store.materialization("stats").unwrap().unwrap();
    assert_eq!(stats.inputs, json!({"docs": "d1"}));
    let totals = store.materialization("totals").unwrap().unwrap();
    assert_eq!(totals.value, Some(json!({"total": 6})));
    assert_eq!(totals.run_id.as_deref(), Some(run.id.as_str()));
    let docs = store.materialization("docs").unwrap().unwrap();
    assert_eq!(docs.value, None);
    assert_eq!(docs.run_id, None);

    let run = Hestan::new()
        .assets(doc_assets())
        .db(db)
        .build_asset("totals")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let ops = store.op_runs(&run.id).unwrap();
    let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    assert_eq!(names, ["totals"]);
}

#[tokio::test]
async fn build_asset_unknown_name_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let err = Hestan::new()
        .assets(doc_assets())
        .db(db.to_str().unwrap())
        .build_asset("nope")
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::UnknownAsset(n) if n == "nope"),
        "wrong error"
    );
}

#[tokio::test]
async fn user_job_named_assets_collides_only_when_assets_exist() {
    let assets_job = || {
        Job::builder("assets")
            .op(Op::new("noop", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();

    let run = Hestan::new()
        .job(assets_job())
        .db(db)
        .run_once("assets", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let err = Hestan::new()
        .job(assets_job())
        .assets(doc_assets())
        .db(db)
        .run_once("assets", json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::DuplicateJob(ref n) if n == "assets"),
        "{err}"
    );
}

#[tokio::test]
async fn duplicate_sensor_names_rejected_at_build() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let watcher = || {
        Sensor::new("watch", Duration::from_secs(60), |_ctx| async {
            Ok(Vec::<RunRequest>::new())
        })
    };
    let err = Hestan::new()
        .sensor(watcher())
        .sensor(watcher())
        .db(db.to_str().unwrap())
        .run_once("anything", json!({}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("duplicate sensor watch"), "{err}");
}

#[tokio::test]
async fn asset_graph_validation_happens_at_build() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let ghost = Asset::source("ghost");
    let orphan = Asset::new("orphan", |_| async { Ok(json!(null)) }).from(&ghost);
    // ghost itself is never registered
    let err = Hestan::new()
        .assets(vec![orphan])
        .db(db.to_str().unwrap())
        .build_asset("orphan")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown op ghost"), "{err}");
}
