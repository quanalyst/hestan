//! the `capture` feature's tracing layer, against a real subscriber.
//!
//! its own test binary on purpose, and not only because the feature is
//! optional. `tracing` caches a callsite's interest the first time that
//! callsite is hit, using whatever subscriber the thread that hit it had —
//! so in a binary where hundreds of other tests run ops with no subscriber
//! installed, the executor's op span would be registered as "nobody is
//! interested" by whichever thread got there first, and every test here would
//! fail perhaps one run in three. here the only code that opens that span is
//! these cases, each of which installs a subscriber before it does.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use hestan::prelude::*;
use hestan::{EventLevel, OpLog, Runner, Store, Trigger};
use tracing_subscriber::layer::SubscriberExt;

/// the layer is a thread-local default subscriber in each case, and tracing's
/// maximum level is one global value recomputed whenever a scoped default is
/// set or unset. two cases overlapping would reset each other's, so they take
/// turns. poison is ignored: one failing case should fail alone.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn turn() -> std::sync::MutexGuard<'static, ()> {
    ONE_AT_A_TIME.lock().unwrap_or_else(|e| e.into_inner())
}

/// a current-thread runtime inside `with_default`, so every op body polls on
/// the thread the subscriber is the default for. hestan installs no
/// subscriber of its own anywhere — that is the whole claim of the feature —
/// so a test has to install one to see anything at all.
fn run_under_capture(store: &Store, job: Job) -> String {
    let _turn = turn();
    let runner = Runner::new(vec![job], store.clone());
    let subscriber = tracing_subscriber::registry().with(hestan::capture_layer(store));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    tracing::subscriber::with_default(subscriber, || {
        rt.block_on(runner.run("etl", json!({}), Trigger::Manual))
            .unwrap()
            .id
    })
}

/// the writer is a thread of its own, so a case waits for what it is about to
/// assert on rather than racing it.
fn lines(store: &Store, run_id: &str, want: usize) -> Vec<OpLog> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let rows = store.op_logs(run_id, None, 0, 10_000).unwrap();
        if rows.len() >= want || Instant::now() > deadline {
            return rows;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn job_of(op: Op) -> Job {
    Job::builder("etl").op(op).build().unwrap()
}

#[test]
fn an_event_inside_an_op_is_captured_with_its_level_target_and_message() {
    let store = Store::open(":memory:").unwrap();
    let run = run_under_capture(
        &store,
        job_of(Op::new("load", |_| async {
            tracing::info!(rows = 12, "loaded the orders");
            tracing::warn!("but three of them were odd");
            Ok(json!("done"))
        })),
    );

    let rows = lines(&store, &run, 2);
    assert_eq!(rows.len(), 2, "{rows:?}");
    // the message, then whatever else the event carried, in the order it reads
    assert_eq!(rows[0].message, "loaded the orders rows=12");
    assert_eq!(rows[0].level, Some(EventLevel::Info));
    assert_eq!(rows[0].target.as_deref(), Some("capture"));
    assert_eq!(rows[0].op, "load");
    assert_eq!(rows[0].attempt, 1);
    // an event was never on a pipe, so it carries no stream at all
    assert_eq!(rows[0].stream, None);
    assert_eq!(rows[1].level, Some(EventLevel::Warn));
    assert_eq!(rows[1].message, "but three of them were odd");
}

#[test]
fn the_hosts_own_logging_is_not_captured() {
    let store = Store::open(":memory:").unwrap();
    let runner = Runner::new(
        vec![job_of(Op::new("load", |_| async {
            tracing::info!("the op's own line");
            Ok(json!("done"))
        }))],
        store.clone(),
    );
    let _turn = turn();
    let subscriber = tracing_subscriber::registry().with(hestan::capture_layer(&store));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let run = tracing::subscriber::with_default(subscriber, || {
        // the host's own logging, including inside a span of its own that the
        // op then runs under: a scope hestan is part of is still not a scope
        // hestan may claim
        tracing::error!("something the host said before any run existed");
        let host = tracing::info_span!("the host's own work", user = "someone");
        host.in_scope(|| {
            tracing::error!("and something it said in a span of its own");
            rt.block_on(runner.run("etl", json!({}), Trigger::Manual))
                .unwrap()
                .id
        })
    });

    let rows = lines(&store, &run, 1);
    assert_eq!(
        rows.iter().map(|r| r.message.as_str()).collect::<Vec<_>>(),
        ["the op's own line"],
        "hestan captured the host application's logging"
    );
}

#[test]
fn a_task_the_op_spawned_is_not_captured() {
    let store = Store::open(":memory:").unwrap();
    let run = run_under_capture(
        &store,
        job_of(Op::new("load", |_| async {
            let spawned = tokio::spawn(async {
                tracing::info!("from a task that did not take the span with it");
            });
            spawned.await.unwrap();
            tracing::info!("from the op itself");
            Ok(json!("done"))
        })),
    );

    // the documented limit, asserted rather than described: `tokio::spawn`
    // carries no span, so the event has no op to belong to. adding
    // `.instrument(tracing::Span::current())` to the spawned future is how a
    // caller gets it back
    let rows = lines(&store, &run, 1);
    assert_eq!(
        rows.iter().map(|r| r.message.as_str()).collect::<Vec<_>>(),
        ["from the op itself"]
    );
}

#[test]
fn a_span_the_op_opened_itself_still_attributes_to_the_op() {
    let store = Store::open(":memory:").unwrap();
    let run = run_under_capture(
        &store,
        job_of(Op::new("load", |_| async {
            let inner = tracing::info_span!("parsing", file = "orders.csv");
            inner.in_scope(|| tracing::info!("row 4 was malformed"));
            Ok(json!("done"))
        })),
    );

    // the walk is outward to the first attempt it finds, so an op's own spans
    // nest inside the attempt rather than hiding it
    let rows = lines(&store, &run, 1);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0].message, "row 4 was malformed");
    assert_eq!(rows[0].op, "load");
}

/// through the public builder, because that is the whole point: the layer was
/// handed a `Store` the test opened itself, and `log_lines` was set on a
/// `Hestan` that opened its own — a cap that lived on either object would not
/// reach the other. it is a process-wide limit, which is also why this is the
/// only case here that moves it, and why it moves it to a number every other
/// case is comfortably under.
#[test]
fn captured_events_are_capped_like_everything_else() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("capped.db").display().to_string();
    let _turn = turn();
    let store = Store::open(&db).unwrap();
    let subscriber = tracing_subscriber::registry().with(hestan::capture_layer(&store));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let run = tracing::subscriber::with_default(subscriber, || {
        rt.block_on(
            Hestan::new()
                .db(&db)
                .log_lines(5)
                .job(job_of(Op::new("load", |_| async {
                    for i in 0..50 {
                        tracing::info!("line {i}");
                    }
                    Ok(json!("done"))
                })))
                .run_once("etl", json!({})),
        )
        .unwrap()
        .id
    });

    let rows = lines(&store, &run, 6);
    assert_eq!(rows.len(), 6, "five lines and one explanation: {rows:?}");
    assert_eq!(rows[4].message, "line 4");
    // and the explanation is hestan speaking, not the op
    assert_eq!(rows[5].target.as_deref(), Some("hestan"));
    assert_eq!(rows[5].stream, None);
    assert!(
        rows[5].message.contains("cap of 5 lines"),
        "{}",
        rows[5].message
    );
}
