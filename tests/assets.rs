use std::time::Duration;

use std::sync::{Arc, Mutex};

use hestan::prelude::*;
use hestan::{CheckStatus, Error, FileIo, RunStatus, Severity, Store, Trigger};

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

// an asset op's metadata is written twice, and a sample is metadata: the op
// run is what that run did, the materialization is what that build reported
// and outlives the run
#[tokio::test]
async fn a_saved_sample_lands_on_the_op_run_and_on_the_materialization() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();

    let rows = Asset::new("rows", |ctx: OpCtx| async move {
        ctx.meta("rows", 2);
        // the op holds the connection, so it selects its own sample back;
        // nothing here asks hestan to run a query
        ctx.saved(
            "head",
            Meta::table([("id", "int")], [vec![json!(1)], vec![json!(2)]]),
        );
        Ok(json!({"rows": 2}))
    });
    let run = Hestan::new()
        .assets(vec![rows])
        .db(db)
        .build_asset("rows")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    let op = store.op_runs(&run.id).unwrap()[0].metadata.clone().unwrap();
    let history = store.materializations("rows", None, 10).unwrap();
    let built = history[0].mat.metadata.clone().unwrap();
    assert_eq!(op, built, "the two copies are the same map");

    let head = built.get("head").unwrap().get("saved").expect("not marked");
    chrono::DateTime::parse_from_rfc3339(head["taken_at"].as_str().unwrap()).unwrap();
    assert_eq!(head["value"]["table"]["rows"], json!([[1], [2]]));
    // the fact beside it is stored exactly as it was before samples existed
    assert_eq!(built["rows"], json!({"int": 2}));
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
    let stats = store.materialization("stats", None).unwrap().unwrap();
    assert_eq!(stats.inputs, json!({"docs": "d1"}));
    let totals = store.materialization("totals", None).unwrap().unwrap();
    assert_eq!(totals.value, Some(json!({"total": 6})));
    assert_eq!(totals.run_id.as_deref(), Some(run.id.as_str()));
    let docs = store.materialization("docs", None).unwrap().unwrap();
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
async fn checks_run_with_the_build_and_are_validated_at_build() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();
    drop(Store::open(db).unwrap());
    rusqlite::Connection::open(db)
        .unwrap()
        .execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, built_at)
             VALUES ('docs', 'd1', '{}', ?1)",
            [chrono::Utc::now().to_rfc3339()],
        )
        .unwrap();

    let has_files = || {
        AssetCheck::new(
            "has_files",
            "stats",
            |_ctx: OpCtx, value: Value| async move {
                let files = value["files"].as_u64().unwrap_or(0);
                if files > 0 {
                    Ok(CheckResult::pass().meta("files", files as i64))
                } else {
                    Ok(CheckResult::fail("no files"))
                }
            },
        )
    };

    let run = Hestan::new()
        .assets(doc_assets())
        .check(has_files())
        .db(db)
        .build_asset("totals")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    let ops = store.op_runs(&run.id).unwrap();
    let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    assert_eq!(names, ["check:stats:has_files", "stats", "totals"]);
    let results = store.asset_checks("stats", None, 10).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].status, CheckStatus::Passed);
    assert_eq!(results[0].severity, Severity::Error);
    assert_eq!(results[0].metadata, Some(json!({"files": {"int": 3}})));
    assert_eq!(results[0].run_id, run.id);

    // a check naming an asset nobody registered is a build error
    let err = Hestan::new()
        .assets(doc_assets())
        .check(AssetCheck::new("ghost", "nowhere", |_, _| async {
            Ok(CheckResult::pass())
        }))
        .db(db)
        .build_asset("totals")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no asset named nowhere"), "{err}");

    // and so is one declared where no assets exist at all
    let err = Hestan::new()
        .check(has_files())
        .db(db)
        .build_asset("totals")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("no assets are registered"),
        "{err}"
    );
}

#[tokio::test]
async fn a_multi_asset_materializes_its_outputs_and_feeds_one_of_them_downstream() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();

    let pipeline = || {
        let split = MultiAsset::new("split_orders", |_ctx: OpCtx| async move {
            Ok(json!({
                "orders_clean": {"rows": 2},
                "orders_rejected": {"rows": 1},
            }))
        })
        .produces(["orders_clean", "orders_rejected"]);
        let report = Asset::new("report", |ctx: OpCtx| async move {
            let rows = ctx.input("orders_clean").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"kept": rows}))
        })
        .from_named("orders_clean");
        Hestan::new().assets([report]).multi_assets([split]).db(db)
    };

    let run = pipeline().build_asset("report").await.unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    // one op run for the pull, one for the report, not one per output
    let ops = store.op_runs(&run.id).unwrap();
    let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    assert_eq!(names, ["report", "split_orders"]);
    let clean = store
        .materialization("orders_clean", None)
        .unwrap()
        .unwrap();
    assert_eq!(clean.value, Some(json!({"rows": 2})));
    assert_eq!(
        store
            .materialization("orders_rejected", None)
            .unwrap()
            .unwrap()
            .value,
        Some(json!({"rows": 1}))
    );
    assert_eq!(
        store
            .materialization("report", None)
            .unwrap()
            .unwrap()
            .value,
        Some(json!({"kept": 2}))
    );

    // rebuilding the report alone seeds the pull from what it stored
    let run = pipeline().build_asset("report").await.unwrap();
    let ops = store.op_runs(&run.id).unwrap();
    let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    assert_eq!(names, ["report"]);
}

#[tokio::test]
async fn a_partitioned_build_runs_one_instance_per_key_through_the_fan_out() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();

    let region = |name: &str| {
        Asset::new(name, |ctx: OpCtx| async move {
            let key = ctx.partition().expect("a partitioned body has its key");
            Ok(json!({ "region": key }))
        })
        .partitioned(Partitions::keys(["emea", "amer"]))
    };
    // the default target set: nothing built, so both keys are missing
    let run = Hestan::new()
        .assets([region("sales")])
        .db(db)
        .build_asset("sales")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    let ops = store.op_runs(&run.id).unwrap();
    let mut names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    names.sort();
    // one op_runs row per instance, named for its key: the same rows a mapped
    // op's fan-out writes, because it is the same expansion
    assert_eq!(names, ["sales[amer]", "sales[emea]"]);
    for key in ["emea", "amer"] {
        assert_eq!(
            store
                .materialization("sales", Some(key))
                .unwrap()
                .unwrap()
                .value,
            Some(json!({ "region": key }))
        );
    }
    // nothing is materialized for the asset as a whole
    assert!(store.materialization("sales", None).unwrap().is_none());

    // everything fresh: the second build has no keys to target and runs none
    let run = Hestan::new()
        .assets([region("sales")])
        .db(db)
        .build_asset("sales")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert!(store.op_runs(&run.id).unwrap().is_empty());
}

#[tokio::test]
async fn an_unpartitioned_asset_may_not_depend_on_a_partitioned_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let daily = Asset::new("daily", |_| async { Ok(json!(null)) })
        .partitioned(Partitions::daily("2026-01-01"));
    let total = Asset::new("total", |_| async { Ok(json!(null)) }).from_named("daily");
    let err = Hestan::new()
        .assets([daily, total])
        .db(db.to_str().unwrap())
        .build_asset("total")
        .await
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("it is not partitioned but its dep daily is"),
        "{err}"
    );
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

// a database written before an asset's value went through a manager holds the
// value in the row, and a database written after it may hold a handle. nothing
// tells them apart and nothing needs to: `get` hands back what it did not
// write, which is what makes this a phase with no migration in it
#[tokio::test]
async fn a_materialization_written_before_any_of_this_still_seeds_a_build() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();
    let builds = Arc::new(Mutex::new(Vec::<&str>::new()));
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));

    // the row a v8-era build wrote: the value itself, under a run whose rows
    // are long gone, and none of the columns added since
    drop(Store::open(db).unwrap());
    rusqlite::Connection::open(db)
        .unwrap()
        .execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, value, run_id, built_at)
             VALUES ('orders', 'o1', '{}', '{\"rows\": 3}', 'a-run-since-pruned', ?1)",
            [chrono::Utc::now().to_rfc3339()],
        )
        .unwrap();

    let ran = builds.clone();
    let saw = seen.clone();
    let orders = Asset::new("orders", move |_| {
        let ran = ran.clone();
        async move {
            ran.lock().unwrap().push("orders");
            Ok(json!({"rows": 99}))
        }
    });
    let totals = Asset::new("totals", move |ctx: OpCtx| {
        let saw = saw.clone();
        async move {
            let orders = ctx.input("orders").cloned().unwrap_or(Value::Null);
            *saw.lock().unwrap() = Some(orders.clone());
            Ok(json!({ "doubled": orders["rows"].as_u64().unwrap_or(0) * 2 }))
        }
    })
    .from(&orders);

    let run = Hestan::new()
        .io(FileIo::new(dir.path().join("io")))
        .assets([orders, totals])
        .db(db)
        .build_asset("totals")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    // seeded from the old row, not rebuilt, and what the body read is the
    // value that row holds rather than something shaped like a handle
    assert!(builds.lock().unwrap().is_empty(), "orders was rebuilt");
    assert_eq!(*seen.lock().unwrap(), Some(json!({"rows": 3})));

    // and the build it seeded wrote the new kind of row beside the old one
    let store = Store::open(db).unwrap();
    let old = store.materialization("orders", None).unwrap().unwrap();
    assert_eq!(old.value, Some(json!({"rows": 3})));
    let new = store.materialization("totals", None).unwrap().unwrap();
    let path = new.value.unwrap()["path"].as_str().unwrap().to_string();
    assert_eq!(std::fs::read_to_string(path).unwrap(), r#"{"doubled":6}"#);
}

// a skip is not a build. if a skipping asset op wrote a materialization,
// staleness would take the asset as refreshed and suppress the next real
// build, which is the one that would have done the work
#[tokio::test]
async fn an_asset_op_that_skips_writes_no_materialization_and_stays_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();

    let ready = Arc::new(Mutex::new(false));
    let assets = |ready: Arc<Mutex<bool>>| {
        let docs = Asset::source("docs");
        let stats = Asset::new("stats", move |ctx: OpCtx| {
            let ready = ready.clone();
            async move {
                let go = *ready.lock().unwrap();
                match go {
                    false => Err(ctx.skip("upstream file has not landed")),
                    true => Ok(json!({"files": 3})),
                }
            }
        })
        .from(&docs);
        vec![docs, stats]
    };

    // the probe row a source would have written, planted by hand: the write
    // api is crate-internal
    drop(Store::open(db).unwrap());
    rusqlite::Connection::open(db)
        .unwrap()
        .execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, built_at)
             VALUES ('docs', 'd1', '{}', ?1)",
            [chrono::Utc::now().to_rfc3339()],
        )
        .unwrap();

    let run = Hestan::new()
        .assets(assets(ready.clone()))
        .db(db)
        .build_asset("stats")
        .await
        .unwrap();
    // the run did nothing and failed nothing
    assert_eq!(run.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    let row = store.op_run(&run.id, "stats").unwrap().unwrap();
    assert_eq!(row.status, hestan::OpStatus::Skipped);
    assert_eq!(row.error.as_deref(), Some("upstream file has not landed"));
    assert!(
        store.materialization("stats", None).unwrap().is_none(),
        "a skip wrote a materialization"
    );

    // so the asset is still stale, and the next build is not suppressed
    *ready.lock().unwrap() = true;
    let run = Hestan::new()
        .assets(assets(ready.clone()))
        .db(db)
        .build_asset("stats")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let ops = store.op_runs(&run.id).unwrap();
    let names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    assert_eq!(names, ["stats"], "the second build had nothing to do");
    let mat = store.materialization("stats", None).unwrap().unwrap();
    assert_eq!(mat.value, Some(json!({"files": 3})));
    assert_eq!(mat.run_id.as_deref(), Some(run.id.as_str()));
}
