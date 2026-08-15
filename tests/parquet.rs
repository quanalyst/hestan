//! the parquet io manager where it actually sits: between two ops of a run.
//!
//! the unit tests in `src/io.rs` are the round trip on its own. these are the
//! paths a manager only meets in a run: a downstream op reading a handle
//! back, a resume seeding one written by a run that is over, and what
//! retention does to the files when it takes the run.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use hestan::prelude::*;
use hestan::{ParquetIo, Retention, RunStatus, Runner, Store, Trigger};

/// three rows an op could plausibly have queried out of somewhere, with a
/// null and a column of each family in them.
fn rows() -> Value {
    json!([
        {"id": 1, "region": "eu", "revenue": 10.5, "vip": true, "note": null},
        {"id": 2, "region": "us", "revenue": -3.25, "vip": false, "note": "refunded"},
        {"id": 3, "region": null, "revenue": 0.0, "vip": true, "note": null},
    ])
}

#[tokio::test]
async fn an_op_downstream_reads_back_what_the_file_holds_and_not_the_handle() {
    let dir = tempfile::tempdir().unwrap();
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let saw = seen.clone();

    let job = Job::builder("etl")
        .op(Op::new("extract", |_| async { Ok(rows()) }).io("parquet"))
        .op(Op::new("load", move |ctx: OpCtx| {
            let saw = saw.clone();
            async move {
                *saw.lock().unwrap() = ctx.input("extract").cloned();
                Ok(json!(null))
            }
        })
        .after(["extract"]))
        .build()
        .unwrap();

    let run = Hestan::new()
        .io_named("parquet", ParquetIo::new(dir.path()))
        .job(job)
        .db(":memory:")
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(*seen.lock().unwrap(), Some(rows()));
}

#[tokio::test]
async fn the_run_log_keeps_a_handle_and_the_op_run_says_how_much_it_stored() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();
    let job = Job::builder("etl")
        .op(Op::new("extract", |_| async { Ok(rows()) }))
        .build()
        .unwrap();

    let run = Hestan::new()
        .io(ParquetIo::new(dir.path().join("io")))
        .job(job)
        .db(db)
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    let op = store.op_runs(&run.id).unwrap().remove(0);
    let path = dir.path().join("io").join(&run.id).join("extract.parquet");
    let handle = op.output.unwrap();
    assert_eq!(handle["$io"], "parquet");
    assert_eq!(handle["path"], path.to_string_lossy().as_ref());
    assert!(path.exists(), "no file at {path:?}");

    // and the two numbers the manager knew and the op did not
    let meta = op.metadata.unwrap();
    assert_eq!(meta["rows"], json!({"count": 3}));
    assert_eq!(
        meta["bytes"],
        json!({"bytes": std::fs::metadata(&path).unwrap().len()})
    );
}

// a resume seeds the ops that already succeeded from the run before it, and
// what it seeds is a handle to a file that run wrote, resolved by this run,
// under this run's id, which the handle must not be looked up by
#[tokio::test]
async fn a_handle_written_by_the_run_before_survives_a_resume() {
    let dir = tempfile::tempdir().unwrap();
    let fixed = Arc::new(AtomicBool::new(false));
    let extracts = Arc::new(AtomicU32::new(0));
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));

    let counted = extracts.clone();
    let saw = seen.clone();
    let ok = fixed.clone();
    let job = Job::builder("etl")
        .op(Op::new("extract", move |_| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(rows())
            }
        }))
        .op(Op::new("load", move |ctx: OpCtx| {
            let (saw, ok) = (saw.clone(), ok.clone());
            async move {
                *saw.lock().unwrap() = ctx.input("extract").cloned();
                match ok.load(Ordering::SeqCst) {
                    true => Ok(json!(null)),
                    false => Err("the warehouse was down".into()),
                }
            }
        })
        .after(["extract"]))
        .build()
        .unwrap();

    let runner = Runner::with_io(
        vec![job],
        Store::open(":memory:").unwrap(),
        Vec::new(),
        Vec::new(),
        Arc::new(ParquetIo::new(dir.path())),
        Vec::new(),
    )
    .unwrap();
    let first = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    fixed.store(true, Ordering::SeqCst);
    let second = runner.resume(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    // the extract did not run again, and load still saw every row of it
    assert_eq!(extracts.load(Ordering::SeqCst), 1);
    assert_eq!(*seen.lock().unwrap(), Some(rows()));
    // read out of the first run's directory, because that is where the handle
    // says the file is
    assert!(dir.path().join(&first.id).join("extract.parquet").exists());
    assert!(!dir.path().join(&second.id).exists());
}

// retention takes the file with the run. the run row is the only record of
// which files the run wrote, so a sweep that took the row and left the file
// would be leaving something nothing could ever find again, and this is
// `FileIo`'s behaviour too, asserted beside it in `tests/pipeline.rs`, because
// a run collected under one manager and left under the other would be two
// answers to one question
#[tokio::test]
async fn retention_takes_the_file_with_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();
    let io_dir = dir.path().join("io");
    let boot = || {
        Hestan::new()
            .io(ParquetIo::new(&io_dir))
            .job(
                Job::builder("etl")
                    .op(Op::new("extract", |_| async { Ok(rows()) }))
                    .build()
                    .unwrap(),
            )
            .db(db)
    };

    let first = boot().run_once("etl", json!({})).await.unwrap();
    let second = boot().run_once("etl", json!({})).await.unwrap();
    let path = io_dir.join(&first.id).join("extract.parquet");
    assert!(path.exists(), "no file at {path:?}");

    // days(0) takes everything terminal that is already in the past and
    // keep_last(1) holds the newest of them back, so this prunes exactly the
    // first run. the sweep runs at startup, before the third run launches
    let third = boot()
        .retention(Retention::days(0).keep_last(1))
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert_eq!(third.status, RunStatus::Success);
    let store = Store::open(db).unwrap();
    assert!(store.run(&first.id).unwrap().is_none(), "the run survived");
    assert!(!path.exists(), "the file outlived the run row");
    assert!(
        !io_dir.join(&first.id).exists(),
        "the run's directory outlived it"
    );

    // the run the policy kept still has everything it wrote: a sweep that took
    // a live run's file would be worse than the leak it is fixing
    assert!(
        store.run(&second.id).unwrap().is_some(),
        "the wrong run went"
    );
    let kept = io_dir.join(&second.id).join("extract.parquet");
    assert!(kept.exists(), "no file at {kept:?}");
}

// an asset's value goes where every other output goes. the second build does
// not run the upstream at all, so what it reads is the file the first build
// wrote: the whole of the point, since a memoized value used to be a second
// copy of it in the run log
#[tokio::test]
async fn a_later_build_seeds_an_asset_from_the_file_its_value_lives_in() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let io_dir = dir.path().join("io");
    let builds = Arc::new(AtomicU32::new(0));
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let boot = || {
        let (counted, saw) = (builds.clone(), seen.clone());
        let orders = Asset::new("orders", move |_| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(rows())
            }
        })
        .io("parquet");
        let totals = Asset::new("totals", move |ctx: OpCtx| {
            let saw = saw.clone();
            async move {
                let orders = ctx.input("orders").cloned().unwrap_or(Value::Null);
                *saw.lock().unwrap() = Some(orders.clone());
                Ok(json!({ "rows": orders.as_array().map_or(0, Vec::len) }))
            }
        })
        .from(&orders);
        Hestan::new()
            .io_named("parquet", ParquetIo::new(&io_dir))
            .assets([orders, totals])
            .db(db.to_str().unwrap())
    };

    let first = boot().build_asset("totals").await.unwrap();
    assert_eq!(first.status, RunStatus::Success);
    let store = Store::open(db.to_str().unwrap()).unwrap();
    let held = store
        .materialization("orders", None)
        .unwrap()
        .unwrap()
        .value;
    let path = io_dir.join(&first.id).join("orders.parquet");
    assert_eq!(held.as_ref().unwrap()["$io"], "parquet");
    assert_eq!(held.unwrap()["path"], path.to_string_lossy().as_ref());
    assert!(path.exists(), "no file at {path:?}");
    drop(store);

    // orders is fresh, so the second build seeds it, out of the file, which
    // is the only place its value is
    *seen.lock().unwrap() = None;
    let second = boot().build_asset("totals").await.unwrap();
    assert_eq!(second.status, RunStatus::Success);
    assert_eq!(builds.load(Ordering::SeqCst), 1, "orders was rebuilt");
    assert_eq!(*seen.lock().unwrap(), Some(rows()));
    let store = Store::open(db.to_str().unwrap()).unwrap();
    let ops = store.op_runs(&second.id).unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].op, "totals");
}

// where the value ended up is not part of what the value is: an asset stored
// as parquet and the same asset stored in the run log are the same asset, and
// a fingerprint that moved with the manager would make every downstream asset
// stale the day somebody configured one
#[tokio::test]
async fn the_fingerprint_is_the_same_wherever_the_value_lives() {
    let dir = tempfile::tempdir().unwrap();
    let fingerprint = |manager: Option<String>, db: &str| {
        let io_dir = dir.path().join("io");
        let db = db.to_string();
        async move {
            let orders = Asset::new("orders", |_| async { Ok(rows()) });
            let orders = match &manager {
                Some(name) => orders.io(name),
                None => orders,
            };
            let run = Hestan::new()
                .io_named("parquet", ParquetIo::new(&io_dir))
                .assets([orders])
                .db(&db)
                .build_asset("orders")
                .await
                .unwrap();
            assert_eq!(run.status, RunStatus::Success);
            Store::open(&db)
                .unwrap()
                .materialization("orders", None)
                .unwrap()
                .unwrap()
                .fingerprint
        }
    };

    let inline = fingerprint(None, dir.path().join("inline.db").to_str().unwrap()).await;
    let stored = fingerprint(
        Some("parquet".to_string()),
        dir.path().join("parquet.db").to_str().unwrap(),
    )
    .await;
    assert_eq!(inline, stored);
}

// retention takes what a run wrote when it takes the run, and an asset's value
// is now one of those things. so the sweep leaves a run an asset's current
// value is inside, and takes it once a rebuild has moved that value on, which
// is what keeps this from being a leak dressed up as a policy
#[tokio::test]
async fn retention_leaves_the_run_a_later_build_would_seed_from() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let io_dir = dir.path().join("io");
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let boot = |policy: Option<Retention>| {
        let saw = seen.clone();
        let orders = Asset::new("orders", |_| async { Ok(rows()) }).io("parquet");
        let totals = Asset::new("totals", move |ctx: OpCtx| {
            let saw = saw.clone();
            async move {
                *saw.lock().unwrap() = ctx.input("orders").cloned();
                Ok(json!({ "counted": true }))
            }
        })
        .from(&orders);
        let app = Hestan::new()
            .io_named("parquet", ParquetIo::new(&io_dir))
            .assets([orders, totals])
            .db(db.to_str().unwrap());
        match policy {
            // everything terminal is already past days(0), and nothing is held
            // back by count, so the only thing between a run and the sweep is
            // whether an asset still reads what it wrote
            Some(policy) => app.retention(policy),
            None => app,
        }
    };
    let sweeping = || boot(Some(Retention::days(0).keep_last(0)));

    let first = boot(None).build_asset("orders").await.unwrap();
    let older = io_dir.join(&first.id).join("orders.parquet");
    assert!(older.exists(), "no file at {older:?}");

    // the sweep at this boot would take the first run on age alone; the
    // value the next build reads is inside it, so it stays
    let second = sweeping().build_asset("orders").await.unwrap();
    let store = Store::open(db.to_str().unwrap()).unwrap();
    assert!(store.run(&first.id).unwrap().is_some(), "the value went");
    assert!(older.exists(), "the file the materialization named went");
    let newer = io_dir.join(&second.id).join("orders.parquet");
    assert!(newer.exists(), "no file at {newer:?}");
    drop(store);

    // and the rebuild released it: the current value is in the second run now,
    // so the first is history like any other run past its policy, while the
    // build this sweep runs still seeds from the value that is current
    let third = sweeping().build_asset("totals").await.unwrap();
    assert_eq!(third.status, RunStatus::Success);
    assert_eq!(*seen.lock().unwrap(), Some(rows()));
    let store = Store::open(db.to_str().unwrap()).unwrap();
    assert!(store.run(&first.id).unwrap().is_none(), "the run survived");
    assert!(!io_dir.join(&first.id).exists(), "its directory survived");
    assert!(
        store.run(&second.id).unwrap().is_some(),
        "the wrong run went"
    );
    assert!(newer.exists(), "the value a build just read was collected");
}

/// the run again once it has reached a terminal status.
async fn settled(runner: &Runner, id: &str) -> hestan::Run {
    for _ in 0..300 {
        let run = runner.store().run(id).unwrap().unwrap();
        if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
            return run;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the run never settled");
}
