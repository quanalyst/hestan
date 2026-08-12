use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::http::{StatusCode, header};
use axum::routing::{any, post};
use hestan::prelude::*;
use hestan::{RunStatus, Runner, Store, Trigger, notify};
use tokio::net::TcpListener;

// warn-and-up log lines, captured process-wide so a test can assert on them
struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn captured_logs() -> Arc<Mutex<Vec<u8>>> {
    static LOGS: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    LOGS.get_or_init(|| {
        let buf: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(move || LogWriter(writer.clone()))
            .finish();
        tracing::subscriber::set_global_default(subscriber).expect("first global subscriber");
        buf
    })
    .clone()
}

async fn serve(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn sink_route(seen: &Arc<Mutex<Option<Value>>>) -> Router {
    let seen = seen.clone();
    Router::new().route(
        "/hook",
        post(move |axum::Json(v): axum::Json<Value>| {
            let seen = seen.clone();
            async move {
                *seen.lock().unwrap() = Some(v);
                "ok"
            }
        }),
    )
}

fn failing_job() -> Job {
    Job::builder("brittle")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .build()
        .unwrap()
}

async fn wait_for(seen: &Arc<Mutex<Option<Value>>>) -> Value {
    for _ in 0..300 {
        if let Some(v) = seen.lock().unwrap().clone() {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("no notification arrived within 3s");
}

#[tokio::test]
async fn webhook_posts_run_failure_json() {
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let base = serve(sink_route(&seen)).await;

    let runner = Runner::with_failure_hooks(
        [failing_job()],
        Store::open(":memory:").unwrap(),
        vec![Arc::new(notify::webhook(format!("{base}/hook")))],
    )
    .unwrap();
    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let body = wait_for(&seen).await;
    assert_eq!(body["run_id"], json!(run.id));
    assert_eq!(body["job"], "brittle");
    assert_eq!(body["trigger"], "manual");
    assert_eq!(body["failed_op"], "boom");
    assert_eq!(body["error"], "no good");
    assert!(body["finished_at"].is_string());
}

#[tokio::test]
async fn slack_message_shape() {
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let base = serve(sink_route(&seen)).await;

    let runner = Runner::with_failure_hooks(
        [failing_job()],
        Store::open(":memory:").unwrap(),
        vec![Arc::new(notify::slack(format!("{base}/hook")))],
    )
    .unwrap();
    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();

    let body = wait_for(&seen).await;
    assert_eq!(
        body,
        json!({
            "text": format!("job brittle failed at boom: no good ({})", run.id)
        })
    );
}

#[tokio::test]
async fn webhook_does_not_follow_redirects() {
    // a followed 302 would replay the POST as a bodyless GET at the Location
    let logs = captured_logs();
    let posts = Arc::new(AtomicU32::new(0));
    let redirected = Arc::new(AtomicU32::new(0));

    let counter = posts.clone();
    let target = redirected.clone();
    let app = Router::new()
        .route(
            "/hook",
            post(move || {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    (StatusCode::FOUND, [(header::LOCATION, "/elsewhere")])
                }
            }),
        )
        .route(
            "/elsewhere",
            any(move || {
                let target = target.clone();
                async move {
                    target.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        );
    let base = serve(app).await;

    let runner = Runner::with_failure_hooks(
        [failing_job()],
        Store::open(":memory:").unwrap(),
        vec![Arc::new(notify::webhook(format!("{base}/hook")))],
    )
    .unwrap();
    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let expected = format!("failure notification to {base}/hook: 302");
    for _ in 0..300 {
        if String::from_utf8_lossy(&logs.lock().unwrap()).contains(&expected) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let captured = String::from_utf8_lossy(&logs.lock().unwrap()).into_owned();
    assert!(
        captured.contains(&expected),
        "no redirect warn in: {captured}"
    );

    // give a stray follow-up request time to show itself
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    assert_eq!(
        redirected.load(Ordering::SeqCst),
        0,
        "the redirect target must never see a request"
    );
}
