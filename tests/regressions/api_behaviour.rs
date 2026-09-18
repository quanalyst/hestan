use hestan::Partitions;
use hestan::prelude::*;
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
type Error = Box<dyn std::error::Error + Send + Sync>;
async fn serve(app: Hestan) -> (String, tokio::task::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let t = tokio::spawn(async move {
        app.serve(addr).await.unwrap();
    });
    let url = format!("http://{addr}");
    for _ in 0..200 {
        if reqwest::get(format!("{url}/api/health")).await.is_ok() {
            return (url, t);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("server did not start");
}
async fn assets(url: &str) -> Value {
    reqwest::get(format!("{url}/api/assets"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn run(url: &str, id: &str) -> Value {
    reqwest::get(format!("{url}/api/runs/{id}"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
async fn ended(url: &str, id: &str) -> Value {
    for _ in 0..400 {
        let r = run(url, id).await;
        if !matches!(r["run"]["status"].as_str(), Some("queued" | "running")) {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run did not end");
}
#[tokio::test]
async fn failed_partition_is_reported_in_asset_execution_summary() {
    let tmp = tempfile::tempdir().unwrap();
    let a = Asset::new("partitioned", |_| async { Err("synthetic failure".into()) })
        .partitioned(Partitions::keys(["one"]));
    let (url, server) = serve(
        Hestan::new()
            .assets([a])
            .db(tmp.path().join("a.db").to_str().unwrap()),
    )
    .await;
    let r: Value = reqwest::Client::new()
        .post(format!("{url}/api/assets/partitioned/build"))
        .json(&json!({"partitions":["one"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let row = ended(&url, r["run_id"].as_str().unwrap()).await;
    assert_eq!(row["run"]["status"], "failed");
    let list = assets(&url).await;
    server.abort();
    assert_eq!(
        list["assets"][0]["execution"]["failed"], 1,
        "failed partition hidden: {list}"
    );
}
#[tokio::test]
async fn materialization_records_inputs_consumed_not_latest_at_completion() {
    let tmp = tempfile::tempdir().unwrap();
    let version = Arc::new(AtomicUsize::new(1));
    let begun = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let source = Asset::source("source")
        .probe({
            let version = version.clone();
            move || {
                let v = version.load(Ordering::SeqCst);
                async move { Ok(v.to_string()) }
            }
        })
        .probe_every(Duration::from_millis(20));
    let target = Asset::new("target", {
        let version = version.clone();
        let begun = begun.clone();
        let release = release.clone();
        move |_| {
            let v = version.load(Ordering::SeqCst);
            let begun = begun.clone();
            let release = release.clone();
            async move {
                begun.notify_one();
                release.notified().await;
                Ok(json!(v))
            }
        }
    })
    .from(&source);
    let (url, server) = serve(
        Hestan::new()
            .assets([source, target])
            .db(tmp.path().join("a.db").to_str().unwrap()),
    )
    .await;
    for _ in 0..100 {
        if assets(&url).await["assets"][0]["fingerprint"] == "1" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let r: Value = reqwest::Client::new()
        .post(format!("{url}/api/assets/target/build"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    begun.notified().await;
    version.store(2, Ordering::SeqCst);
    let mut seen = false;
    for _ in 0..100 {
        if assets(&url).await["assets"][0]["fingerprint"] == "2" {
            seen = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(seen);
    release.notify_one();
    ended(&url, r["run_id"].as_str().unwrap()).await;
    let list = assets(&url).await;
    server.abort();
    assert_eq!(
        list["assets"][1]["stale"], true,
        "source changed during build but target reported fresh: {list}"
    );
}
#[tokio::test]
async fn canceled_run_can_leave_a_pending_resource_constructor() {
    let tmp = tempfile::tempdir().unwrap();
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    struct Resource(Arc<AtomicUsize>);
    impl Drop for Resource {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let job = Job::builder("job")
        .op(Op::new("op", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let app = Hestan::new()
        .job(job)
        .db(tmp.path().join("a.db").to_str().unwrap())
        .run_resource("first", {
            let dropped = dropped.clone();
            move |_| {
                let dropped = dropped.clone();
                async move { Ok::<_, Error>(Resource(dropped)) }
            }
        })
        .run_resource("r", {
            let entered = entered.clone();
            move |_| {
                let entered = entered.clone();
                async move {
                    entered.notify_one();
                    std::future::pending::<Result<(), Error>>().await
                }
            }
        });
    let (url, server) = serve(app).await;
    let r: Value = reqwest::Client::new()
        .post(format!("{url}/api/jobs/job/runs"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = r["run_id"].as_str().unwrap();
    entered.notified().await;
    assert_eq!(
        reqwest::Client::new()
            .post(format!("{url}/api/runs/{id}/cancel"))
            .send()
            .await
            .unwrap()
            .status(),
        202
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    let row = run(&url, id).await;
    server.abort();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(
        row["run"]["status"], "canceled",
        "cancel acknowledged but resource constructor ignored it"
    );
}

#[tokio::test]
async fn asset_replay_uses_captured_inputs_after_an_upstream_rebuild() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("replay.db").display().to_string();
    let version = Arc::new(AtomicUsize::new(1));
    let succeeds = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source = Asset::new("source", {
        let version = version.clone();
        move |_| {
            let value = version.load(Ordering::SeqCst);
            async move { Ok(json!(value)) }
        }
    });
    let target = Asset::new("target", {
        let succeeds = succeeds.clone();
        move |ctx: OpCtx| {
            let succeeds = succeeds.clone();
            async move {
                if !succeeds.load(Ordering::SeqCst) {
                    return Err("synthetic initial failure".into());
                }
                Ok(ctx.input("source").unwrap().clone())
            }
        }
    })
    .from(&source);
    let (url, server) = serve(Hestan::new().db(&path).assets([source, target])).await;
    let client = reqwest::Client::new();
    let first: Value = client
        .post(format!("{url}/api/assets/target/build"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let first_id = first["run_id"].as_str().unwrap();
    assert_eq!(ended(&url, first_id).await["run"]["status"], "failed");
    version.store(2, Ordering::SeqCst);
    let upstream: Value = client
        .post(format!("{url}/api/runs/{first_id}/replay"))
        .json(&json!({"ops": ["source"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        ended(&url, upstream["run_id"].as_str().unwrap()).await["run"]["status"],
        "success"
    );
    succeeds.store(true, Ordering::SeqCst);
    let replay: Value = client
        .post(format!("{url}/api/runs/{first_id}/replay"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        ended(&url, replay["run_id"].as_str().unwrap()).await["run"]["status"],
        "success"
    );
    let value = hestan::Store::open(&path)
        .unwrap()
        .materialization("target", None)
        .unwrap()
        .unwrap();
    assert_eq!(value.value, Some(json!(1)));
    let list = assets(&url).await;
    assert_eq!(list["assets"][1]["stale"], true);
    server.abort();
}

#[tokio::test]
async fn asset_replay_reads_cached_inputs_with_the_original_named_io_key() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("cached-replay.db").display().to_string();
    let succeeds = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let source = Asset::new("source", |_| async { Ok(json!(42)) }).io("keyed");
    let target = Asset::new("target", {
        let succeeds = succeeds.clone();
        move |ctx: OpCtx| {
            let succeeds = succeeds.clone();
            async move {
                if !succeeds.load(Ordering::SeqCst) {
                    return Err("synthetic initial failure".into());
                }
                Ok(ctx.input("source").unwrap().clone())
            }
        }
    })
    .from(&source);
    let (url, server) = serve(
        Hestan::new()
            .db(&path)
            .io_named("keyed", super::io_resume::Keyed::default())
            .assets([source, target]),
    )
    .await;
    let client = reqwest::Client::new();
    let mut failed_id = String::new();
    for (asset, expected) in [("source", "success"), ("target", "failed")] {
        let response: Value = client
            .post(format!("{url}/api/assets/{asset}/build"))
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let id = response["run_id"].as_str().unwrap();
        assert_eq!(ended(&url, id).await["run"]["status"], expected);
        failed_id = id.to_owned();
    }
    succeeds.store(true, Ordering::SeqCst);
    let replay: Value = client
        .post(format!("{url}/api/runs/{failed_id}/replay"))
        .json(&json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        ended(&url, replay["run_id"].as_str().unwrap()).await["run"]["status"],
        "success"
    );
    assert_eq!(
        hestan::Store::open(&path)
            .unwrap()
            .materialization("target", None)
            .unwrap()
            .unwrap()
            .value,
        Some(json!(42))
    );
    server.abort();
}

#[tokio::test]
async fn partition_summary_keeps_failure_and_running_counts_distinct() {
    let tmp = tempfile::tempdir().unwrap();
    let started = Arc::new(Notify::new());
    let asset = Asset::new("partitioned", {
        let started = started.clone();
        move |ctx: OpCtx| {
            let started = started.clone();
            async move {
                if ctx.partition() == Some("busy") {
                    started.notify_one();
                    std::future::pending::<()>().await;
                }
                Err("synthetic failure".into())
            }
        }
    })
    .partitioned(Partitions::keys(["bad", "busy"]));
    let (url, server) = serve(
        Hestan::new()
            .assets([asset])
            .db(tmp.path().join("mixed.db").display().to_string()),
    )
    .await;
    let r: Value = reqwest::Client::new()
        .post(format!("{url}/api/assets/partitioned/build"))
        .json(&json!({"partitions": ["bad", "busy"]}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    started.notified().await;
    let mut summary = json!({});
    for _ in 0..100 {
        summary = assets(&url).await["assets"][0]["execution"].clone();
        if summary["failed"] == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(summary, json!({"failed":1,"running":1}));
    let id = r["run_id"].as_str().unwrap();
    reqwest::Client::new()
        .post(format!("{url}/api/runs/{id}/cancel"))
        .send()
        .await
        .unwrap();
    ended(&url, id).await;
    server.abort();
}
