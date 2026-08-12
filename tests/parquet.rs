//! the parquet io manager where it actually sits: between two ops of a run.
//!
//! the unit tests in `src/io.rs` are the round trip on its own. these are the
//! paths a manager only meets in a run — a downstream op reading a handle
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
// what it seeds is a handle to a file that run wrote — resolved by this run,
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

// what retention does about the files is: nothing. this is `FileIo`'s
// behaviour and `ParquetIo` matches it deliberately rather than growing a
// second answer — see `docs/io-managers.md`, which says so where somebody
// choosing a directory will read it
#[tokio::test]
async fn retention_takes_the_run_and_leaves_the_file_behind() {
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
    let path = io_dir.join(&first.id).join("extract.parquet");
    assert!(path.exists(), "no file at {path:?}");

    // days(0) prunes everything terminal that is already in the past, and the
    // sweep runs at startup
    let second = boot()
        .retention(Retention::days(0))
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert_eq!(second.status, RunStatus::Success);
    let store = Store::open(db).unwrap();
    assert!(store.run(&first.id).unwrap().is_none(), "the run survived");
    assert!(path.exists(), "the file went with the run row");
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
