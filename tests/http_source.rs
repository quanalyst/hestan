use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::extract::RawQuery;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use hestan::prelude::*;
use hestan::{Error, EventLevel, HttpSource, RunStatus, Runner, Store, Trigger};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

async fn serve(app: Router) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// a server that answers with bytes axum would never write: a content-length
/// that does not describe the body, a body that never arrives.
///
/// the connection is held open afterwards rather than closed, so a client that
/// reads further than it needs to waits for its own timeout instead of finding
/// an eof — which is what makes "it stopped reading" something a test can
/// assert rather than hope for.
async fn serve_raw(response: Vec<u8>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let response = response.clone();
            tokio::spawn(async move {
                // the request has to be taken off the socket before the answer
                // goes back down it
                let mut req = [0u8; 1024];
                let _ = sock.read(&mut req).await;
                let _ = sock.write_all(&response).await;
                tokio::time::sleep(Duration::from_secs(30)).await;
            });
        }
    });
    format!("http://{addr}")
}

#[derive(Serialize, Deserialize)]
struct Payload {
    ok: bool,
    n: u32,
}

#[tokio::test]
async fn success_typed() {
    let app = Router::new().route(
        "/data",
        get(|| async { axum::Json(json!({"ok": true, "n": 7})) }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/data"))
        .name("pull")
        .expect_json::<Payload>();
    let runner = Runner::new(
        [src.into_job("pull").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    assert_eq!(
        runner.jobs()["pull"].op("pull").unwrap().output_type(),
        Some("http_source::Payload")
    );

    let run = runner
        .run("pull", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].output, Some(json!({"ok": true, "n": 7})));
}

#[tokio::test]
async fn retries_then_succeeds() {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let app = Router::new().route(
        "/flaky",
        get(move || {
            let h = h.clone();
            async move {
                if h.fetch_add(1, Ordering::SeqCst) < 2 {
                    (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
                } else {
                    axum::Json(json!({"ok": true})).into_response()
                }
            }
        }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/flaky"))
        .name("flaky")
        .retries(3)
        .retry_delay(Duration::from_millis(10));
    let runner = Runner::new(
        [src.into_job("flaky").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("flaky", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    let events = runner.store().events(&run.id, 0).unwrap();
    let warns = events
        .iter()
        .filter(|e| e.level == EventLevel::Warn && e.message.contains("500"))
        .count();
    assert_eq!(warns, 2);
    // the http loop owns retrying, so the op itself records one attempt
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].attempts, 1);
}

#[tokio::test]
async fn client_error_fails_fast() {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let app = Router::new().route(
        "/gone",
        get(move || {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                (StatusCode::NOT_FOUND, "nothing here")
            }
        }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/gone"))
        .name("gone")
        .retries(3)
        .retry_delay(Duration::from_millis(10));
    let runner = Runner::new(
        [src.into_job("gone").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("gone", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let error = ops[0].error.as_deref().unwrap();
    assert!(error.contains("404"), "{error}");
    assert!(error.contains("nothing here"), "{error}");
}

#[tokio::test]
async fn an_error_body_far_larger_than_the_snippet_is_read_only_that_far() {
    // content-length promises a gigabyte and 256 KiB of it turns up. an
    // attempt that read to the end of what it was promised would sit on the
    // held-open connection until the timeout, so having the snippet at all is
    // what says it stopped where the snippet stops
    let mut response =
        b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 1073741824\r\n\r\n".to_vec();
    response.extend(b"boom: the database is on fire\n");
    response.extend(vec![b'x'; 256 * 1024]);
    let base = serve_raw(response).await;

    let src = HttpSource::get(format!("{base}/boom"))
        .name("boom")
        .retries(0)
        .timeout(Duration::from_secs(5));
    let runner = Runner::new(
        [src.into_job("boom").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("boom", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let error = ops[0].error.as_deref().unwrap();
    assert!(error.contains("500"), "{error}");
    assert!(error.contains("boom: the database is on fire"), "{error}");
    // and the padding is not in it: what gets printed is what gets held
    assert!(error.chars().count() < 300, "{error}");
}

#[tokio::test]
async fn a_content_length_past_the_ceiling_is_refused_before_the_body_is_read() {
    // headers and then nothing at all. the body never arrives, so an attempt
    // that waited to see how big it really was would fail on the timeout
    let response =
        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 1073741824\r\n\r\n"
            .to_vec();
    let base = serve_raw(response).await;

    let src = HttpSource::get(format!("{base}/pages"))
        .name("pages")
        .max_body(64 * 1024)
        .retries(0)
        .timeout(Duration::from_secs(5));
    let runner = Runner::new(
        [src.into_job("pages").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("pages", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let error = ops[0].error.as_deref().unwrap();
    assert!(error.contains("content-length 1073741824"), "{error}");
    assert!(
        error.contains("65536") && error.contains("/pages"),
        "{error}"
    );
}

#[tokio::test]
async fn a_body_past_the_ceiling_with_no_content_length_fails_naming_the_limit() {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let app = Router::new().route(
        "/pages",
        get(move || {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                // chunked, so there is no length to refuse it by up front, and
                // endless, so the ceiling is the only thing that can end it
                let chunks =
                    futures::stream::repeat_with(|| Ok::<_, std::io::Error>(vec![b'0'; 8 * 1024]));
                axum::body::Body::from_stream(chunks)
            }
        }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/pages"))
        .name("pages")
        .max_body(64 * 1024)
        .retries(3)
        .retry_delay(Duration::from_millis(10))
        .timeout(Duration::from_secs(5));
    let runner = Runner::new(
        [src.into_job("pages").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("pages", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let error = ops[0].error.as_deref().unwrap();
    assert!(
        error.contains("65536") && error.contains("/pages"),
        "{error}"
    );
    // the same request would fetch the same body, so it is not retried
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_body_under_the_ceiling_is_unaffected() {
    let rows: Value = json!(
        (0..1_000)
            .map(|i| json!({ "i": i }))
            .collect::<Vec<Value>>()
    );
    let payload = rows.clone();
    let app = Router::new().route(
        "/rows",
        get(move || {
            let payload = payload.clone();
            async move { axum::Json(payload) }
        }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/rows"))
        .name("rows")
        .max_body(64 * 1024);
    let runner = Runner::new(
        [src.into_job("rows").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("rows", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].output, Some(rows));
}

#[tokio::test]
async fn a_content_length_that_understates_the_body_cannot_get_past_the_ceiling() {
    // the header says twenty bytes and 64 KiB follow them, against a ceiling
    // of one. what the response framing says is one document is what gets
    // parsed either way — a header that lies cannot make it more than that
    let mut response =
        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 20\r\n\r\n".to_vec();
    response.extend(b"[1,2,3,4,5,6,7,8,9]\n");
    response.extend(vec![b'x'; 64 * 1024]);
    let base = serve_raw(response).await;

    let src = HttpSource::get(format!("{base}/short"))
        .name("short")
        .max_body(1024)
        .retries(0)
        .timeout(Duration::from_secs(5));
    let runner = Runner::new(
        [src.into_job("short").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("short", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].output, Some(json!([1, 2, 3, 4, 5, 6, 7, 8, 9])));
}

#[tokio::test]
async fn fan_out_names() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let s = seen.clone();
    let app = Router::new().route(
        "/regions",
        get(move |RawQuery(q): RawQuery| {
            let s = s.clone();
            async move {
                s.lock().unwrap().push(q.unwrap_or_default());
                axum::Json(json!({"ok": true}))
            }
        }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/regions"))
        .name("region")
        .query("fmt", "json")
        .query_each("r", ["EMEA", "us-east"]);
    let names: Vec<String> = src
        .into_ops()
        .iter()
        .map(|o| o.name().to_string())
        .collect();
    assert_eq!(names, ["region_emea", "region_us_east"]);

    let runner = Runner::new(
        [src.into_job("regions").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("regions", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let mut queries = seen.lock().unwrap().clone();
    queries.sort();
    assert_eq!(queries, ["fmt=json&r=EMEA", "fmt=json&r=us-east"]);
}

#[tokio::test]
async fn bearer_env() {
    let seen: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let s = seen.clone();
    let app = Router::new().route(
        "/private",
        get(move |headers: HeaderMap| {
            let s = s.clone();
            async move {
                *s.lock().unwrap() = headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(String::from);
                axum::Json(json!({"ok": true}))
            }
        }),
    );
    let base = serve(app).await;

    unsafe { std::env::set_var("HESTAN_TEST_TOKEN", "secret") };
    let src = HttpSource::get(format!("{base}/private"))
        .name("private")
        .bearer_env("HESTAN_TEST_TOKEN");
    let runner = Runner::new(
        [src.into_job("private").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("private", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(seen.lock().unwrap().as_deref(), Some("Bearer secret"));
}

#[tokio::test]
async fn huge_retry_after_capped() {
    // this retry-after asks for a 10-day sleep
    let app = Router::new().route(
        "/limited",
        get(|| async {
            (
                StatusCode::TOO_MANY_REQUESTS,
                [("retry-after", "864000")],
                "slow down",
            )
        }),
    );
    let base = serve(app).await;

    let src = HttpSource::get(format!("{base}/limited"))
        .name("limited")
        .retries(1)
        .retry_delay(Duration::from_millis(10));
    let runner = Runner::new(
        [src.into_job("limited").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let id = runner
        .launch("limited", json!({}), Trigger::Manual)
        .unwrap();

    // the retry warn is written before the sleep; the capped delay shows there
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let events = runner.store().events(&id, 0).unwrap();
        if let Some(e) = events.iter().find(|e| e.message.contains("retrying in")) {
            assert!(e.message.contains("retrying in 300s"), "{}", e.message);
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no retry event appeared"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn builder_error_fails_fast_with_cause() {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let app = Router::new().route(
        "/ok",
        get(move || {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                axum::Json(json!({"ok": true}))
            }
        }),
    );
    let base = serve(app).await;

    // a header value reqwest can never send: deterministic builder error
    let src = HttpSource::get(format!("{base}/ok"))
        .name("broken")
        .header("x-key", "bad\nvalue")
        .retries(3)
        .retry_delay(Duration::from_millis(10));
    let runner = Runner::new(
        [src.into_job("broken").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("broken", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(!events.iter().any(|e| e.message.contains("retrying")));
    let ops = runner.store().op_runs(&run.id).unwrap();
    let error = ops[0].error.as_deref().unwrap();
    assert!(error.contains("failed to parse header value"), "{error}");
}

#[tokio::test]
async fn empty_bearer_env_fatal() {
    let hits = Arc::new(AtomicU32::new(0));
    let h = hits.clone();
    let app = Router::new().route(
        "/private",
        get(move || {
            let h = h.clone();
            async move {
                h.fetch_add(1, Ordering::SeqCst);
                axum::Json(json!({"ok": true}))
            }
        }),
    );
    let base = serve(app).await;

    unsafe { std::env::set_var("HESTAN_TEST_EMPTY_TOKEN", "") };
    let src = HttpSource::get(format!("{base}/private"))
        .name("private")
        .bearer_env("HESTAN_TEST_EMPTY_TOKEN");
    let runner = Runner::new(
        [src.into_job("private").unwrap()],
        Store::open(":memory:").unwrap(),
    )
    .unwrap();
    let run = runner
        .run("private", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(
        ops[0].error.as_deref(),
        Some("bearer env var HESTAN_TEST_EMPTY_TOKEN not set or empty")
    );
}

#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn into_job_warns_on_dropped_cron() {
    let logs = LogBuf::default();
    let sink = logs.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || sink.clone())
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        HttpSource::get("http://127.0.0.1:9/x")
            .name("plain")
            .into_job("plain")
            .unwrap();
        assert!(logs.0.lock().unwrap().is_empty());
        HttpSource::get("http://127.0.0.1:9/x")
            .name("timed")
            .cron("*/5 * * * *")
            .into_job("timed")
            .unwrap();
    });
    let out = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
    assert!(out.contains("WARN"), "{out}");
    assert!(
        out.contains("*/5 * * * *") && out.contains("Hestan::source"),
        "{out}"
    );
}

#[test]
fn query_each_collision_rejected() {
    // both values sanitize to region_us_east
    let src = HttpSource::get("http://127.0.0.1:9/x")
        .name("region")
        .query_each("r", ["us-east", "us_east"]);
    let err = src.into_job("regions").unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("\"us-east\"") && msg.contains("\"us_east\""),
        "{msg}"
    );
    assert!(msg.contains("region_us_east"), "{msg}");
}

#[test]
fn query_each_empty_rejected() {
    let src = HttpSource::get("http://127.0.0.1:9/x")
        .name("region")
        .query_each("r", Vec::<String>::new());
    let err = src.into_job("regions").unwrap_err();
    assert!(
        err.to_string().contains("query_each has no values"),
        "{err}"
    );
}

#[tokio::test]
async fn missing_name_rejected() {
    let err = Hestan::new()
        .source(HttpSource::get("http://127.0.0.1:9/nope"))
        .db(":memory:")
        .run_once("nope", json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Graph(ref msg) if msg.contains("needs a name")),
        "{err}"
    );
}
