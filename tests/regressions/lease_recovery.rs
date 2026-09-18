use hestan::prelude::*;
use hestan::{RunStatus, Runner, Store, Trigger};
use std::{sync::Arc, time::Duration};
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_executor_cannot_overwrite_recovery_outcome() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("lease.db");
    let path = path.to_str().unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let job = Job::builder("slow")
        .op(Op::new("work", {
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(json!("finished"))
                }
            }
        }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(path).unwrap()).unwrap();
    let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    entered.notified().await;
    // Model a suspended worker whose run lease has elapsed.
    rusqlite::Connection::open(path)
        .unwrap()
        .execute(
            "UPDATE runs SET lease_until='2000-01-01T00:00:00+00:00' WHERE id=?1",
            [&id],
        )
        .unwrap();
    let recovery = Job::builder("recovery")
        .op(Op::new("noop", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    Hestan::new()
        .db(path)
        .job(recovery)
        .run_once("recovery", json!({}))
        .await
        .unwrap();
    assert_eq!(
        runner.store().run(&id).unwrap().unwrap().status,
        RunStatus::Failed
    );
    release.notify_one();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let row = runner.store().run(&id).unwrap().unwrap();
    let ops = runner.store().op_runs(&id).unwrap();
    assert_eq!(
        row.status,
        RunStatus::Failed,
        "recovery was overwritten: run={:?}, op={:?}",
        row.status,
        ops[0].status
    );
}
