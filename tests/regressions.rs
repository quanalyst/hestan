//! Regressions for interactions between execution, persistence and presentation.
#[cfg(feature = "http")]
#[path = "regressions/api_behaviour.rs"]
mod api_behaviour;
#[path = "regressions/io_resume.rs"]
mod io_resume;
#[path = "regressions/lease_recovery.rs"]
mod lease_recovery;
#[path = "regressions/queue_starvation.rs"]
mod queue_starvation;
#[path = "regressions/startup_secrets.rs"]
mod startup_secrets;

#[tokio::test(start_paused = true)]
async fn headless_jobs_and_assets_maintain_execution_leases() {
    use hestan::prelude::*;
    use std::{sync::Arc, time::Duration};
    for asset in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("headless.db").display().to_string();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let body = {
            let entered = entered.clone();
            let release = release.clone();
            move |_| {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(json!(1))
                }
            }
        };
        let app = Hestan::new().db(&db);
        let app = if asset {
            app.assets([Asset::new("work", body)])
        } else {
            app.job(
                Job::builder("work")
                    .op(Op::new("work", body))
                    .build()
                    .unwrap(),
            )
        };
        let task = tokio::spawn(async move {
            if asset {
                app.build_asset("work").await
            } else {
                app.run_once("work", json!({})).await
            }
        });
        entered.notified().await;
        let store = hestan::Store::open(&db).unwrap();
        let initial = store
            .runs(None, None, None, None, None, None, 1)
            .unwrap()
            .remove(0);
        tokio::time::advance(Duration::from_secs(16)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let renewed = store.run(&initial.id).unwrap().unwrap();
        release.notify_one();
        assert_eq!(
            task.await.unwrap().unwrap().status,
            hestan::RunStatus::Success
        );
        assert!(
            renewed.lease_until > initial.lease_until,
            "headless lease did not advance (asset={asset})"
        );
    }
}

#[tokio::test]
async fn a_panicking_run_resource_fails_instead_of_stranding_the_run() {
    use hestan::prelude::*;
    async fn broken(
        _: hestan::ResourceCtx,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        panic!("synthetic constructor panic");
    }
    let dir = tempfile::tempdir().unwrap();
    let run = Hestan::new()
        .db(dir.path().join("panic.db").display().to_string())
        .job(
            Job::builder("job")
                .op(Op::new("op", |_| async { Ok(json!(1)) }))
                .build()
                .unwrap(),
        )
        .run_resource("broken", broken)
        .run_once("job", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, hestan::RunStatus::Failed);
    assert!(run.error.unwrap().contains("constructor panicked"));
}
