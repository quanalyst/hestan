use hestan::prelude::*;
use hestan::{IoDropped, IoKey, IoManager, IoResult, RunStatus, Runner, Store, Trigger};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
#[derive(Default)]
pub(super) struct Keyed(Mutex<HashMap<(String, String), serde_json::Value>>);
impl IoManager for Keyed {
    fn put(&self, key: &IoKey, v: serde_json::Value) -> IoResult {
        self.0
            .lock()
            .unwrap()
            .insert((key.run_id.clone(), key.op.clone()), v);
        Ok(json!({"$io":"review-keyed"}))
    }
    fn get(&self, key: &IoKey, h: &serde_json::Value) -> IoResult {
        if h["$io"] != "review-keyed" {
            return Ok(h.clone());
        }
        self.0
            .lock()
            .unwrap()
            .get(&(key.run_id.clone(), key.op.clone()))
            .cloned()
            .ok_or_else(|| "read used a different IoKey than write".into())
    }
    fn drop_run(&self, _: &str, _: &str) -> IoDropped {
        Ok(())
    }
}
#[tokio::test]
async fn resume_reads_with_the_producing_runs_io_key() {
    let store = Store::open(":memory:").unwrap();
    let succeed = Arc::new(AtomicBool::new(false));
    let job = Job::builder("job")
        .op(Op::new("produce", |_| async { Ok(json!(42)) }))
        .op(Op::new("consume", {
            let succeed = succeed.clone();
            move |ctx: OpCtx| {
                let succeed = succeed.clone();
                async move {
                    if !succeed.load(Ordering::SeqCst) {
                        return Err("synthetic initial failure".into());
                    }
                    Ok(ctx.input("produce").unwrap().clone())
                }
            }
        })
        .after(["produce"]))
        .build()
        .unwrap();
    let runner = Runner::with_io(
        [job],
        store.clone(),
        vec![],
        [],
        Arc::new(Keyed::default()),
        [],
    )
    .unwrap();
    let first = runner.run("job", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(first.status, RunStatus::Failed);
    succeed.store(true, Ordering::SeqCst);
    let id = runner.resume(&first.id).unwrap();
    for _ in 0..100 {
        let r = store.run(&id).unwrap().unwrap();
        if !matches!(r.status, RunStatus::Queued | RunStatus::Running) {
            assert_eq!(
                r.status,
                RunStatus::Success,
                "resumed run failed: {:?}",
                r.error
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("resume did not finish");
}

#[tokio::test]
async fn asset_reuse_reads_with_the_producing_run_and_partition_key() {
    use hestan::Partitions;
    for partitioned in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("assets.db").display().to_string();
        let manager = Arc::new(Keyed::default());
        #[derive(Clone)]
        struct Shared(Arc<Keyed>);
        impl IoManager for Shared {
            fn put(&self, key: &IoKey, v: serde_json::Value) -> IoResult {
                self.0.put(key, v)
            }
            fn get(&self, key: &IoKey, h: &serde_json::Value) -> IoResult {
                self.0.get(key, h)
            }
            fn drop_run(&self, id: &str, job: &str) -> IoDropped {
                self.0.drop_run(id, job)
            }
        }
        let app = || {
            let source = Asset::new("source", |_| async { Ok(json!(42)) });
            let source = if partitioned {
                source.partitioned(Partitions::keys(["one"]))
            } else {
                source
            };
            let target = Asset::new("target", |ctx: OpCtx| async move {
                assert_eq!(ctx.input("source"), Some(&json!(42)));
                Ok(json!(true))
            })
            .from(&source);
            let target = if partitioned {
                target.partitioned(Partitions::keys(["one"]))
            } else {
                target
            };
            Hestan::new()
                .db(&path)
                .io(Shared(manager.clone()))
                .assets([source, target])
        };
        assert_eq!(
            app().build_asset("source").await.unwrap().status,
            RunStatus::Success
        );
        assert_eq!(
            app().build_asset("target").await.unwrap().status,
            RunStatus::Success
        );
    }
}
