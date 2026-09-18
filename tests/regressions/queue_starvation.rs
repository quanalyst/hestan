use hestan::prelude::*;
use hestan::{Limits, Role, RunStatus, Runner, Store, Trigger};
use std::time::Duration;

#[tokio::test]
async fn runnable_job_behind_blocked_prefix_gets_dispatched() {
    let store = Store::open(":memory:").unwrap();
    let jobs = || {
        ["blocked", "ready"]
            .into_iter()
            .map(|name| {
                Job::builder(name)
                    .op(Op::new("op", move |_| async move {
                        if name == "blocked" {
                            std::future::pending::<()>().await;
                        }
                        Ok(json!(null))
                    }))
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let scheduler = Runner::new(jobs(), store.clone())
        .unwrap()
        .with_role(Role::Scheduler, 1);
    for _ in 0..501 {
        scheduler
            .launch("blocked", json!({}), Trigger::Manual)
            .unwrap();
    }
    let worker = Runner::new(jobs(), store.clone())
        .unwrap()
        .with_limits(Limits::new().job("blocked", 1), 0);
    let ready = worker.launch("ready", json!({}), Trigger::Manual).unwrap();
    for _ in 0..200 {
        if store.run(&ready).unwrap().unwrap().status == RunStatus::Success {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        store.run(&ready).unwrap().unwrap().status,
        RunStatus::Success
    );
}
