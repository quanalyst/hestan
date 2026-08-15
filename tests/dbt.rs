//! a dbt project's models built as assets, with a script standing in for dbt.
//!
//! **dbt is not installed here and must not need to be.** what these assert is
//! everything on hestan's side of the boundary: that the graph is dbt's graph,
//! that the models are built in its order, that each one is invoked with the
//! arguments and in the directory it was promised, that what the process
//! printed lands on the run page, and that a non-zero exit fails the asset
//! rather than recording a materialization of nothing.
//!
//! what they cannot assert is the other side: that `dbt run --select x` builds
//! x in your warehouse. that is dbt's, and a test of it here would be a test
//! of whether dbt happened to be installed on the machine that ran it.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

use hestan::prelude::*;
use hestan::{LogStream, RunStatus, Store, dbt::Dbt};

/// the committed fixture project: a diamond over a source, with a seed, a
/// data test and a disabled model beside it.
fn manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dbt/target/manifest.json")
}

/// a script in `dir` that behaves like dbt in one respect and no others.
fn fake_dbt(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("dbt");
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.to_string_lossy().into_owned()
}

/// the fixture project as a deployment, with `program` where dbt goes.
fn project(db: &str, program: &str) -> Hestan {
    let dbt = Dbt::from_manifest(manifest()).unwrap().command(program);
    Hestan::new().assets(dbt.assets()).db(db)
}

fn db(dir: &tempfile::TempDir) -> String {
    dir.path().join("hestan.db").to_string_lossy().into_owned()
}

/// the ops of a run, in the order they were started rather than the order the
/// rows come back in.
fn built(store: &Store, run: &str) -> Vec<String> {
    let mut ops = store.op_runs(run).unwrap();
    ops.sort_by_key(|o| o.started_at);
    ops.into_iter().map(|o| o.op).collect()
}

// the whole of part 2 in one run: dbt's lineage decides what is built and in
// what order, and each model is invoked for by name, in the project directory
#[tokio::test]
async fn building_a_model_builds_what_it_is_made_of_first_and_invokes_dbt_for_each() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(&dir);
    // what it was run in, and what it was run with
    let dbt = fake_dbt(&dir, "pwd\necho \"$@\"");

    let run = project(&db, &dbt)
        .build_asset("orders_summary")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);

    let store = Store::open(&db).unwrap();
    let order = built(&store, &run.id);
    // stg_orders before the two that read it, and both of those before the
    // one that reads them: dbt's `depends_on`, not the order anything was
    // written down in
    assert_eq!(order.len(), 4, "{order:?}");
    assert_eq!(order[0], "stg_orders");
    assert_eq!(order[3], "orders_summary");
    assert!(order.contains(&"orders_daily".to_string()));
    assert!(order.contains(&"orders_by_region".to_string()));

    let printed: Vec<String> = store
        .op_logs(&run.id, None, 0, 100)
        .unwrap()
        .into_iter()
        .map(|line| format!("{} {}", line.op, line.message))
        .collect();
    let root = manifest().parent().unwrap().parent().unwrap().to_path_buf();
    for model in &order {
        // dbt runs where dbt_project.yml is, which is not the directory
        // hestan was started in
        assert!(
            printed.contains(&format!("{model} {}", root.display())),
            "{model} did not run in {root:?}: {printed:?}"
        );
        assert!(
            printed.contains(&format!("{model} run --select {model}")),
            "{model} was invoked with something else: {printed:?}"
        );
    }
    assert_eq!(printed.len(), 8, "{printed:?}");

    // the source the manifest says stg_orders reads is in the graph with no
    // materialization of its own: nothing built it, dbt read it
    assert!(
        store.materialization("raw.orders", None).unwrap().is_none(),
        "a source was materialized"
    );
    // and what a model recorded is the model it was
    let summary = store.materialization("orders_summary", None).unwrap();
    assert_eq!(
        summary.unwrap().value,
        Some(json!({"model": "orders_summary"}))
    );
}

#[tokio::test]
async fn what_dbt_printed_on_either_stream_is_on_the_run_page() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(&dir);
    let dbt = fake_dbt(
        &dir,
        "echo '1 of 1 START sql view model stg_orders'\necho 'Database Error' >&2\nexit 1",
    );

    let run = project(&db, &dbt).build_asset("stg_orders").await.unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let store = Store::open(&db).unwrap();
    let printed = store.op_logs(&run.id, None, 0, 100).unwrap();
    assert_eq!(printed.len(), 2, "{printed:?}");
    let out = printed.iter().find(|l| l.stream == Some(LogStream::Stdout));
    let err = printed.iter().find(|l| l.stream == Some(LogStream::Stderr));
    assert_eq!(
        out.map(|l| l.message.as_str()),
        Some("1 of 1 START sql view model stg_orders")
    );
    assert_eq!(err.map(|l| l.message.as_str()), Some("Database Error"));

    // the exit status is the op's, and nothing was materialized
    let op = store.op_run(&run.id, "stg_orders").unwrap().unwrap();
    let failure = op.error.unwrap();
    assert!(
        failure.contains("run --select stg_orders exited 1"),
        "{failure}"
    );
    assert!(
        store.materialization("stg_orders", None).unwrap().is_none(),
        "a model that never built was recorded as built"
    );
}

// the most likely failure of all on a fresh machine, and it says which
// program it could not start rather than "no such file or directory"
#[tokio::test]
async fn a_dbt_that_is_not_installed_fails_the_asset_naming_it() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(&dir);
    let run = project(&db, "dbt-that-is-not-installed")
        .build_asset("stg_orders")
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let store = Store::open(&db).unwrap();
    let err = store
        .op_run(&run.id, "stg_orders")
        .unwrap()
        .unwrap()
        .error
        .unwrap();
    assert!(
        err.contains("could not start dbt-that-is-not-installed in"),
        "{err}"
    );
}

// nothing fingerprints a dbt source unless you say how, so everything
// downstream of one is stale on every plan, which is what `dbt run --select`
// does anyway, and is the behaviour to know about before wondering why dbt ran
// again
#[tokio::test]
async fn a_source_nothing_fingerprints_leaves_every_model_stale() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(&dir);
    let dbt = fake_dbt(&dir, "exit 0");
    for _ in 0..2 {
        let run = project(&db, &dbt)
            .build_asset("orders_daily")
            .await
            .unwrap();
        assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
        assert_eq!(
            built(&Store::open(&db).unwrap(), &run.id),
            ["stg_orders", "orders_daily"]
        );
    }
}

// and with something fingerprinting the source, a dbt graph is incremental:
// dbt runs for the models a change reaches and for nothing else. a model
// hestan rebuilt makes what reads it stale, because hestan cannot see into
// the warehouse and will not assume the table is what it was
#[tokio::test]
async fn a_model_is_rebuilt_when_what_it_reads_changed_and_not_otherwise() {
    let dir = tempfile::tempdir().unwrap();
    let db = db(&dir);
    let dbt = fake_dbt(&dir, "exit 0");
    // the probe api is a running deployment's, so stand in for it: this is
    // the row a `probe` on the source asset would have written
    drop(Store::open(&db).unwrap());
    let loaded = |fingerprint: &str| {
        rusqlite::Connection::open(&db)
            .unwrap()
            .execute(
                "INSERT INTO asset_materializations (asset, fingerprint, inputs, built_at)
                 VALUES ('raw.orders', ?1, '{}', ?2)",
                rusqlite::params![fingerprint, chrono::Utc::now().to_rfc3339()],
            )
            .unwrap();
    };
    let build = |name: &'static str| {
        let (db, dbt) = (db.clone(), dbt.clone());
        async move {
            let run = project(&db, &dbt).build_asset(name).await.unwrap();
            assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
            built(&Store::open(&db).unwrap(), &run.id)
        }
    };

    loaded("2026-08-11T03:00:00Z");
    assert_eq!(build("orders_daily").await, ["stg_orders", "orders_daily"]);
    // the source has not moved, so the model upstream is not run again;
    // the one that was asked for always is, since that is what asking means
    assert_eq!(build("orders_daily").await, ["orders_daily"]);
    // it has moved now, and dbt runs for everything the change reaches
    loaded("2026-08-12T03:00:00Z");
    assert_eq!(build("orders_daily").await, ["stg_orders", "orders_daily"]);
}
