use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hestan::prelude::*;
use hestan::{
    CancelOutcome, DeliveryState, Error, EventKind, FailureHook, FileIo, Graph, IoDropped, IoKey,
    IoManager, IoResult, Meta, OpEvent, OpHook, OpStatus, Retention, Run, RunEvent, RunFailure,
    RunHook, RunStatus, Runner, Store, Trigger, When,
};
use serde::{Deserialize, Serialize};

async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..300 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached within 3s");
}

#[tokio::test]
async fn linear_job_passes_outputs() {
    let seen: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let seen2 = seen.clone();
    let job = Job::builder("etl")
        .op(Op::new("extract", |_| async { Ok(json!({"rows": 3})) }))
        .op(Op::new("transform", move |ctx| {
            let seen = seen2.clone();
            async move {
                *seen.lock().unwrap() = ctx.input("extract").cloned();
                Ok(json!("done"))
            }
        })
        .after(["extract"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(*seen.lock().unwrap(), Some(json!({"rows": 3})));
}

#[tokio::test]
async fn diamond_runs_in_dependency_order() {
    let order: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let track = |name: &'static str| {
        let order = order.clone();
        move |_: OpCtx| {
            let order = order.clone();
            async move {
                order.lock().unwrap().push(name);
                Ok(json!(null))
            }
        }
    };

    let job = Job::builder("diamond")
        .op(Op::new("a", track("a")))
        .op(Op::new("b", track("b")).after(["a"]))
        .op(Op::new("c", track("c")).after(["a"]))
        .op(Op::new("d", track("d")).after(["b", "c"]))
        .build()
        .unwrap();

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let run = Hestan::new()
        .job(job)
        .db(db.to_str().unwrap())
        .run_once("diamond", json!({}))
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let order = order.lock().unwrap();
    assert_eq!(order.len(), 4);
    assert_eq!(order[0], "a");
    assert_eq!(order[3], "d");
    let mid: HashSet<_> = order[1..3].iter().copied().collect();
    assert_eq!(mid, ["b", "c"].into_iter().collect());
}

#[tokio::test]
async fn failure_skips_downstream_and_fails_run() {
    let job = Job::builder("brittle")
        .op(Op::new("ok", |_| async { Ok(json!(1)) }))
        .op(Op::new("boom", |_| async { Err("no good".into()) }).after(["ok"]))
        .op(Op::new("never", |_| async { Ok(json!(2)) }).after(["boom"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let status = |name: &str| ops.iter().find(|o| o.op == name).unwrap().status;
    assert_eq!(status("ok"), OpStatus::Success);
    assert_eq!(status("boom"), OpStatus::Failed);
    assert_eq!(status("never"), OpStatus::Skipped);

    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(!events.is_empty());
    assert!(events.iter().any(|e| e.message.contains("no good")));
}

#[tokio::test]
async fn retries_then_succeeds() {
    let calls = Arc::new(AtomicU32::new(0));
    let calls2 = calls.clone();
    let job = Job::builder("flaky")
        .op(Op::new("wobble", move |_| {
            let calls = calls2.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err("transient".into());
                }
                Ok(json!("finally"))
            }
        })
        .retries(2)
        .retry_delay(Duration::from_millis(10)))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("flaky", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].attempts, 3);
    assert_eq!(ops[0].status, OpStatus::Success);
}

#[tokio::test]
async fn unknown_job_errors() {
    let job = Job::builder("real")
        .op(Op::new("noop", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let err = runner
        .launch("nope", json!({}), Trigger::Manual)
        .unwrap_err();
    assert!(matches!(err, Error::UnknownJob(name) if name == "nope"));
}

#[test]
fn cycle_rejected_at_build() {
    let err = Job::builder("loopy")
        .op(Op::new("a", |_| async { Ok(json!(null)) }).after(["b"]))
        .op(Op::new("b", |_| async { Ok(json!(null)) }).after(["a"]))
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("cycle"));
}

#[derive(Serialize, Deserialize)]
struct Extract {
    rows: Vec<u32>,
}

#[derive(Deserialize)]
struct TotalIn {
    extract: Extract,
}

#[derive(Serialize)]
struct Total {
    total: u32,
}

#[tokio::test]
async fn typed_ops_roundtrip() {
    let job = Job::builder("typed")
        .op(Op::new("extract", |_| async {
            Ok(json!({"rows": [1, 2, 3]}))
        }))
        .op(
            Op::typed("total", |_ctx: OpCtx, input: TotalIn| async move {
                Ok(Total {
                    total: input.extract.rows.iter().sum(),
                })
            })
            .after(["extract"]),
        )
        .build()
        .unwrap();

    assert_eq!(
        job.op("total").unwrap().output_type(),
        Some("pipeline::Total")
    );

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("typed", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let total = ops.iter().find(|o| o.op == "total").unwrap();
    assert_eq!(total.output, Some(json!({"total": 6})));

    let events = runner.store().events(&run.id, 0).unwrap();
    assert_eq!(events[0].kind, EventKind::RunQueued);
    // the payload names the type the op declared it returns, and says which
    // attempt produced it — `meta` is null because this op reported no facts
    assert!(events.iter().any(|e| e.kind == EventKind::OpSuccess
        && e.data == Some(json!({"attempt": 1, "output_type": "pipeline::Total", "meta": null}))));
}

#[tokio::test]
async fn type_mismatch_fails_op_with_event() {
    let job = Job::builder("typed")
        .op(Op::new("extract", |_| async {
            Ok(json!({"rows": "not a list"}))
        }))
        .op(
            Op::typed("total", |_ctx: OpCtx, input: TotalIn| async move {
                Ok(Total {
                    total: input.extract.rows.iter().sum(),
                })
            })
            .after(["extract"]),
        )
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("typed", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let total = ops.iter().find(|o| o.op == "total").unwrap();
    assert_eq!(total.status, OpStatus::Failed);
    assert!(
        total
            .error
            .as_deref()
            .unwrap()
            .starts_with("type check failed:")
    );

    let events = runner.store().events(&run.id, 0).unwrap();
    let tcf = events
        .iter()
        .find(|e| e.kind == EventKind::TypeCheckFailed)
        .unwrap();
    assert!(tcf.message.starts_with("type check failed:"));
    assert!(tcf.data.as_ref().unwrap()["error"].is_string());
}

#[tokio::test]
async fn invalid_params_rejected_before_launch() {
    #[derive(Deserialize)]
    struct Params {
        threshold: u32,
    }

    let job = Job::builder("gated")
        .op(Op::new("check", |ctx| async move {
            let p = ctx.params_as::<Params>()?;
            Ok(json!({"threshold": p.threshold}))
        })
        .params::<Params>())
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let err = runner
        .launch("gated", json!({"threshold": "high"}), Trigger::Manual)
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidParams { ref op, .. } if op == "check"),
        "{err}"
    );
    assert!(
        runner
            .store()
            .runs(None, None, None, None, None, 10)
            .unwrap()
            .is_empty()
    );

    let run = runner
        .run("gated", json!({"threshold": 3}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
}

#[tokio::test]
async fn panicking_op_retries_like_an_error() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let job = Job::builder("jumpy")
        .op(Op::new("panicky", move |_| {
            let calls = counter.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("index out of range");
                }
                Ok(json!("recovered"))
            }
        })
        .retries(1))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("jumpy", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].attempts, 2);
    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.message.contains("op panicked: index out of range"))
    );
}

#[tokio::test]
async fn eager_panic_goes_through_retry_policy() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let job = Job::builder("eager")
        .op(Op::new("boom", move |_| {
            let calls = counter.clone();
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("eager panic");
            }
            async move { Ok(json!("recovered")) }
        })
        .retries(1))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("eager", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].attempts, 2);
    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.kind == EventKind::OpRetry
                && e.message.contains("op panicked: eager panic"))
    );
}

#[tokio::test]
async fn skip_event_commits_before_status() {
    let job = Job::builder("brittle")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .op(Op::new("never", |_| async { Ok(json!(null)) }).after(["boom"]))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();

    let ops = runner.store().op_runs(&run.id).unwrap();
    let skipped = ops.iter().find(|o| o.op == "never").unwrap();
    let events = runner.store().events(&run.id, 0).unwrap();
    let ev = events
        .iter()
        .find(|e| e.kind == EventKind::OpSkipped)
        .unwrap();
    assert!(
        ev.ts <= skipped.finished_at.unwrap(),
        "op_skipped event written after the skipped status"
    );
}

#[tokio::test]
async fn max_parallel_caps_in_flight_ops() {
    let gauge = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let mut builder = Job::builder("wide").max_parallel(2);
    for i in 0..6 {
        let gauge = gauge.clone();
        let peak = peak.clone();
        builder = builder.op(Op::new(format!("op{i}"), move |_| {
            let gauge = gauge.clone();
            let peak = peak.clone();
            async move {
                let now = gauge.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(60)).await;
                gauge.fetch_sub(1, Ordering::SeqCst);
                Ok(json!(null))
            }
        }));
    }
    let runner = Runner::new([builder.build().unwrap()], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("wide", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak was {}",
        peak.load(Ordering::SeqCst)
    );
    // success must mean all six actually ran, not that the survivors passed
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(
        ops.iter().filter(|o| o.status == OpStatus::Success).count(),
        6
    );
}

#[tokio::test]
async fn watermark_persists_across_runs() {
    let job = Job::builder("inc")
        .op(Op::new("count", |ctx| async move {
            let n = ctx.state_as::<u64>()?.unwrap_or(0);
            ctx.set_state(json!(n + 1));
            Ok(json!({ "job": ctx.job(), "seen": n }))
        }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();

    let r1 = runner.run("inc", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(r1.status, RunStatus::Success);
    let ops = runner.store().op_runs(&r1.id).unwrap();
    assert_eq!(ops[0].output, Some(json!({"job": "inc", "seen": 0})));

    let r2 = runner.run("inc", json!({}), Trigger::Manual).await.unwrap();
    let ops = runner.store().op_runs(&r2.id).unwrap();
    assert_eq!(ops[0].output, Some(json!({"job": "inc", "seen": 1})));
    assert_eq!(
        runner.store().op_state("inc", "count").unwrap(),
        Some(json!(2))
    );
}

#[tokio::test]
async fn failed_op_commits_no_state() {
    let job = Job::builder("stateful")
        .op(Op::new("poison", |ctx| async move {
            ctx.set_state(json!("never"));
            Err("boom".into())
        }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("stateful", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(runner.store().op_state("stateful", "poison").unwrap(), None);
}

#[tokio::test]
async fn failed_attempt_state_dropped_on_retry() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let job = Job::builder("wobbly")
        .op(Op::new("cursor", move |ctx| {
            let calls = counter.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    ctx.set_state(json!("poison"));
                    return Err("transient".into());
                }
                Ok(json!(null))
            }
        })
        .retries(1)
        .retry_delay(Duration::from_millis(10)))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("wobbly", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // attempt 2 succeeded without staging, so nothing was committed
    assert_eq!(runner.store().op_state("wobbly", "cursor").unwrap(), None);
}

#[tokio::test]
async fn every_metadata_variant_round_trips_on_the_op_run() {
    let job = Job::builder("report")
        .op(Op::new("emit", |ctx: OpCtx| async move {
            ctx.meta("rows", 1_234);
            ctx.meta("ratio", 0.5);
            ctx.meta("note", "backfilled");
            ctx.meta("source", Meta::Url("https://example.test/orders".into()));
            ctx.meta("summary", Meta::Markdown("# totals\n\n2 rows".into()));
            ctx.meta("shape", json!({"cols": ["a", "b"]}));
            // last call for a name wins, like set_state
            ctx.meta("rows", 1_235);
            Ok(json!(null))
        }))
        .op(Op::new("quiet", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("report", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let ops = runner.store().op_runs(&run.id).unwrap();
    let emit = ops.iter().find(|o| o.op == "emit").unwrap();
    assert_eq!(
        emit.metadata,
        Some(json!({
            "rows": {"int": 1235},
            "ratio": {"float": 0.5},
            "note": {"text": "backfilled"},
            "source": {"url": "https://example.test/orders"},
            "summary": {"markdown": "# totals\n\n2 rows"},
            "shape": {"json": {"cols": ["a", "b"]}},
        }))
    );
    // an op that reported nothing stores null, not an empty object
    let quiet = ops.iter().find(|o| o.op == "quiet").unwrap();
    assert_eq!(quiet.metadata, None);
}

#[tokio::test]
async fn failed_attempt_metadata_dropped_on_retry() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let job = Job::builder("wobbly")
        .op(Op::new("load", move |ctx: OpCtx| {
            let calls = counter.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    ctx.meta("rows", 0);
                    ctx.meta("attempt", "first");
                    return Err("transient".into());
                }
                ctx.meta("rows", 12);
                Ok(json!(null))
            }
        })
        .retries(1)
        .retry_delay(Duration::from_millis(10)))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("wobbly", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // only the attempt that worked reported anything, and `attempt` — staged
    // by the failure and never restaged — is gone entirely
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(ops[0].metadata, Some(json!({"rows": {"int": 12}})));
}

#[tokio::test]
async fn cancel_stops_running_and_pending_ops() {
    let job = Job::builder("slow")
        .op(Op::new("long", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        }))
        .op(Op::new("tail", |_| async { Ok(json!(null)) }).after(["long"]))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    {
        let (runner, id) = (runner.clone(), id.clone());
        wait_until(move || {
            runner
                .store()
                .op_runs(&id)
                .unwrap()
                .iter()
                .any(|o| o.op == "long" && o.status == OpStatus::Running)
        })
        .await;
    }

    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
    {
        let (runner, id) = (runner.clone(), id.clone());
        wait_until(move || runner.store().run(&id).unwrap().unwrap().status == RunStatus::Canceled)
            .await;
    }

    // the event commits before the status flip, so it must already be readable
    let events = runner.store().events(&id, 0).unwrap();
    assert!(events.iter().any(|e| e.kind == EventKind::RunCanceled));
    let op_canceled: Vec<&str> = events
        .iter()
        .filter(|e| e.kind == EventKind::OpCanceled)
        .map(|e| e.op.as_deref().unwrap())
        .collect();
    assert!(op_canceled.contains(&"long"), "{op_canceled:?}");
    assert!(op_canceled.contains(&"tail"), "{op_canceled:?}");

    let ops = runner.store().op_runs(&id).unwrap();
    let by_name = |name: &str| ops.iter().find(|o| o.op == name).unwrap();
    assert_eq!(by_name("long").status, OpStatus::Canceled);
    assert_eq!(by_name("long").error.as_deref(), Some("canceled"));
    assert_eq!(by_name("tail").status, OpStatus::Canceled);

    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::AlreadyFinished);
}

#[tokio::test]
async fn cancel_unknown_and_finished_runs() {
    let job = Job::builder("quick")
        .op(Op::new("noop", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    assert_eq!(runner.cancel("nope").unwrap(), CancelOutcome::Unknown);

    let run = runner
        .run("quick", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(
        runner.cancel(&run.id).unwrap(),
        CancelOutcome::AlreadyFinished
    );
}

fn collector() -> (Arc<Mutex<Vec<RunFailure>>>, FailureHook) {
    let seen: Arc<Mutex<Vec<RunFailure>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let hook: FailureHook = Arc::new(move |f| sink.lock().unwrap().push(f));
    (seen, hook)
}

#[tokio::test]
async fn failure_hook_fires_once_with_details() {
    let (seen, hook) = collector();
    let job = Job::builder("brittle")
        .op(Op::new("ok", |_| async { Ok(json!(1)) }))
        .op(Op::new("boom", |_| async { Err("no good".into()) }).after(["ok"]))
        .build()
        .unwrap();
    let runner =
        Runner::with_failure_hooks([job], Store::open(":memory:").unwrap(), vec![hook]).unwrap();
    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    {
        let seen = seen.clone();
        wait_until(move || seen.lock().unwrap().len() == 1).await;
    }
    // exactly once: give a stray second fire time to show up
    tokio::time::sleep(Duration::from_millis(50)).await;
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    let f = &seen[0];
    assert_eq!(f.run_id, run.id);
    assert_eq!(f.job, "brittle");
    assert_eq!(f.trigger, Trigger::Manual);
    assert_eq!(f.failed_op.as_deref(), Some("boom"));
    assert_eq!(f.error.as_deref(), Some("no good"));
}

#[tokio::test]
async fn failure_hooks_stack_and_survive_panics() {
    let (seen, hook) = collector();
    let job = Job::builder("brittle")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .build()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let run = Hestan::new()
        .job(job)
        .db(db.to_str().unwrap())
        .on_failure(|_| panic!("bad hook"))
        .on_failure(move |f| hook(f))
        .run_once("brittle", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    wait_until(move || seen.lock().unwrap().len() == 1).await;
}

#[tokio::test]
async fn blocking_hook_does_not_stall_other_runs() {
    // on tokio::spawn a blocking hook pinned the only async worker of this
    // single-threaded test runtime, stalling the napper below
    let fired = Arc::new(AtomicU32::new(0));
    let counter = fired.clone();
    let hook: FailureHook = Arc::new(move |_| {
        std::thread::sleep(Duration::from_millis(300));
        counter.fetch_add(1, Ordering::SeqCst);
    });
    let brittle = Job::builder("brittle")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .build()
        .unwrap();
    let napper = Job::builder("napper")
        .op(Op::new("nap", |_| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(json!(null))
        }))
        .build()
        .unwrap();
    let runner = Runner::with_failure_hooks(
        [brittle, napper],
        Store::open(":memory:").unwrap(),
        vec![hook],
    )
    .unwrap();

    // both in flight: brittle fires the hook well inside the napper's 50ms window
    let started = std::time::Instant::now();
    let brittle_id = runner
        .launch("brittle", json!({}), Trigger::Manual)
        .unwrap();
    let run = runner
        .run("napper", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert!(
        started.elapsed() < Duration::from_millis(250),
        "napper stalled {:?} behind a blocking hook",
        started.elapsed()
    );

    // let the hook finish before the runtime drops
    wait_until(move || fired.load(Ordering::SeqCst) == 1).await;
    let (runner, id) = (runner.clone(), brittle_id);
    wait_until(move || runner.store().run(&id).unwrap().unwrap().status == RunStatus::Failed).await;
}

#[tokio::test]
async fn no_hook_on_success_or_cancel() {
    let (seen, hook) = collector();
    let quick = Job::builder("quick")
        .op(Op::new("noop", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let slow = Job::builder("slow")
        .op(Op::new("nap", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        }))
        .build()
        .unwrap();
    let runner =
        Runner::with_failure_hooks([quick, slow], Store::open(":memory:").unwrap(), vec![hook])
            .unwrap();

    let run = runner
        .run("quick", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
    {
        let (runner, id) = (runner.clone(), id.clone());
        wait_until(move || runner.store().run(&id).unwrap().unwrap().status == RunStatus::Canceled)
            .await;
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(seen.lock().unwrap().is_empty());
}

fn run_events() -> (Arc<Mutex<Vec<RunEvent>>>, RunHook) {
    let seen: Arc<Mutex<Vec<RunEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let hook: RunHook = Arc::new(move |e| sink.lock().unwrap().push(e));
    (seen, hook)
}

fn op_events() -> (Arc<Mutex<Vec<OpEvent>>>, OpHook) {
    let seen: Arc<Mutex<Vec<OpEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    let hook: OpHook = Arc::new(move |e| sink.lock().unwrap().push(e));
    (seen, hook)
}

#[tokio::test]
async fn every_terminal_status_reaches_the_run_hook_exactly_once() {
    let (seen, hook) = run_events();
    let quick = Job::builder("quick")
        .op(Op::new("noop", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let brittle = Job::builder("brittle")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .build()
        .unwrap();
    let slow = Job::builder("slow")
        .op(Op::new("nap", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        }))
        .build()
        .unwrap();
    let runner = Runner::new([quick, brittle, slow], Store::open(":memory:").unwrap())
        .unwrap()
        .with_hooks(vec![hook], Vec::new());

    runner
        .run("quick", json!({}), Trigger::Manual)
        .await
        .unwrap();
    runner
        .run("brittle", json!({}), Trigger::Schedule)
        .await
        .unwrap();
    let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
    {
        let (runner, id) = (runner.clone(), id.clone());
        wait_until(move || runner.store().run(&id).unwrap().unwrap().status == RunStatus::Canceled)
            .await;
    }

    {
        let seen = seen.clone();
        wait_until(move || seen.lock().unwrap().len() == 3).await;
    }
    // and give a stray fourth time to show itself
    tokio::time::sleep(Duration::from_millis(50)).await;
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    // by status rather than by arrival: each hook is its own blocking task and
    // three of them are in no particular order
    let of = |status| seen.iter().find(|e| e.status == status).expect("one each");

    let succeeded = of(RunStatus::Success);
    assert_eq!(succeeded.job, "quick");
    assert_eq!(succeeded.failed_op, None);
    assert_eq!(succeeded.error, None);
    assert!(succeeded.started_at.is_some());
    assert!(succeeded.duration.is_some());

    let failed = of(RunStatus::Failed);
    assert_eq!(failed.trigger, Trigger::Schedule);
    assert_eq!(failed.failed_op.as_deref(), Some("boom"));
    assert_eq!(failed.error.as_deref(), Some("no good"));

    // a cancel is news too, and the status is what says so: `on_failure` is
    // the hook that stays quiet about one
    assert_eq!(of(RunStatus::Canceled).run_id, id);
}

#[tokio::test]
async fn an_op_hook_fires_per_attempt_with_the_attempt_number() {
    let (seen, hook) = op_events();
    let tries = Arc::new(AtomicU32::new(0));
    let counter = tries.clone();
    let job = Job::builder("flaky")
        .op(Op::new("wobble", move |_| {
            let counter = counter.clone();
            async move {
                if counter.fetch_add(1, Ordering::SeqCst) < 2 {
                    return Err("not yet".into());
                }
                Ok(json!("finally"))
            }
        })
        .retries(2)
        .retry_delay(Duration::from_millis(1)))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap())
        .unwrap()
        .with_hooks(Vec::new(), vec![hook]);

    let run = runner
        .run("flaky", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    {
        let seen = seen.clone();
        wait_until(move || seen.lock().unwrap().len() == 3).await;
    }
    // by attempt rather than by arrival: each hook is its own blocking task
    let mut seen = seen.lock().unwrap().clone();
    seen.sort_by_key(|e| e.attempt);
    // one per attempt, and the failed ones are not swallowed by the retry
    // that made up for them: three attempts is three facts
    let shape: Vec<(u32, OpStatus)> = seen.iter().map(|e| (e.attempt, e.status)).collect();
    assert_eq!(
        shape,
        [
            (1, OpStatus::Failed),
            (2, OpStatus::Failed),
            (3, OpStatus::Success)
        ]
    );
    assert_eq!(seen[0].error.as_deref(), Some("not yet"));
    assert_eq!(seen[2].error, None);
    assert_eq!(seen[2].op, "wobble");
    assert_eq!(seen[2].job, "flaky");
    assert_eq!(seen[2].run_id, run.id);
    // each attempt is timed on its own, so the retry's start is after the
    // first attempt's finish rather than the row's `started_at`
    assert!(seen[1].started_at >= seen[0].finished_at);
}

// the point of per-job scoping: an alert covers prod without covering every
// backfill beside it, and without a hook that has to keep a job list by hand
#[tokio::test]
async fn a_job_scoped_hook_never_sees_another_jobs_runs() {
    let (runs, run_hook) = run_events();
    let (ops, op_hook) = op_events();
    let watched = Job::builder("prod")
        .on_run_finished(move |e| run_hook(e))
        .on_op_finished(move |e| op_hook(e))
        .op(Op::new("load", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let other = Job::builder("backfill")
        .op(Op::new("load", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let runner = Runner::new([watched, other], Store::open(":memory:").unwrap()).unwrap();

    runner
        .run("backfill", json!({}), Trigger::Manual)
        .await
        .unwrap();
    let run = runner
        .run("prod", json!({}), Trigger::Manual)
        .await
        .unwrap();

    {
        let runs = runs.clone();
        wait_until(move || runs.lock().unwrap().len() == 1).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let runs = runs.lock().unwrap();
    assert_eq!(runs.len(), 1, "a job-scoped hook saw another job's run");
    assert_eq!(runs[0].run_id, run.id);
    assert_eq!(runs[0].job, "prod");
    let ops = ops.lock().unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].job, "prod");
}

#[tokio::test]
async fn a_panicking_op_hook_leaves_the_run_alone() {
    let (seen, hook) = op_events();
    let job = Job::builder("etl")
        .op(Op::new("load", |_| async { Ok(json!("done")) }))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap())
        .unwrap()
        .with_hooks(Vec::new(), vec![Arc::new(|_| panic!("bad hook")), hook]);

    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(run.status, RunStatus::Success);
    // and the hook beside the panicking one still ran
    wait_until(move || seen.lock().unwrap().len() == 1).await;
}

// there was no attempt, so there is nothing to report about one
#[tokio::test]
async fn ops_skipped_by_a_rule_reach_no_op_hook() {
    let (seen, hook) = op_events();
    let job = Job::builder("etl")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .op(Op::new("downstream", |_| async { Ok(json!(null)) }).after(["boom"]))
        .op(Op::new("cleanup", |_| async { Ok(json!(null)) })
            .after(["boom"])
            .when(When::AnyFailed))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap())
        .unwrap()
        .with_hooks(Vec::new(), vec![hook]);

    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(
        runner
            .store()
            .op_run(&run.id, "downstream")
            .unwrap()
            .unwrap()
            .status,
        OpStatus::Skipped
    );

    {
        let seen = seen.clone();
        wait_until(move || seen.lock().unwrap().len() == 2).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let seen = seen.lock().unwrap();
    let mut named: Vec<&str> = seen.iter().map(|e| e.op.as_str()).collect();
    named.sort();
    assert_eq!(
        named,
        ["boom", "cleanup"],
        "a skipped op reported an attempt"
    );
}

// the whole path, from the builder knob to the hook: the event is a row
// written with the run's terminal row, and the delivery loop is what calls the
// hook. a `:memory:` store would be a conversation with itself
#[tokio::test]
async fn a_durable_notification_is_a_row_first_and_a_hook_after() {
    let (seen, hook) = run_events();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let job = Job::builder("brittle")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .build()
        .unwrap();

    let run = Hestan::new()
        .job(job)
        .db(db.to_str().unwrap())
        .durable_notifications()
        .on_run_finished(move |e| hook(e))
        .run_once("brittle", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].run_id, run.id);
    assert_eq!(seen[0].failed_op.as_deref(), Some("boom"));

    let store = Store::open(db.to_str().unwrap()).unwrap();
    let rows = store.notifications(None, 10).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].state, DeliveryState::Delivered);
    assert_eq!(rows[0].payload["run_id"], json!(run.id));
    assert_eq!(rows[0].payload["status"], json!("failed"));
}

#[tokio::test]
async fn retention_prunes_old_terminal_runs_but_never_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();
    let job = || {
        Job::builder("inc")
            .op(Op::new("count", |ctx| async move {
                let n = ctx.state_as::<u64>()?.unwrap_or(0);
                ctx.set_state(json!(n + 1));
                Ok(json!({ "seen": n }))
            }))
            .build()
            .unwrap()
    };

    let old = Hestan::new()
        .job(job())
        .db(db)
        .run_once("inc", json!({}))
        .await
        .unwrap();
    assert_eq!(old.status, RunStatus::Success);

    // age the run past the window
    let backdated = (chrono::Utc::now() - chrono::Duration::days(10)).to_rfc3339();
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.execute(
        "UPDATE runs SET created_at = ?1 WHERE id = ?2",
        rusqlite::params![backdated, old.id],
    )
    .unwrap();
    drop(conn);

    let fresh = Hestan::new()
        .job(job())
        .db(db)
        .retention_days(7)
        .run_once("inc", json!({}))
        .await
        .unwrap();
    assert_eq!(fresh.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    assert!(store.run(&old.id).unwrap().is_none());
    assert!(store.op_runs(&old.id).unwrap().is_empty());
    assert!(store.events(&old.id, 0).unwrap().is_empty());
    // op_state is never pruned, so this run read the pruned run's watermark
    let ops = store.op_runs(&fresh.id).unwrap();
    assert_eq!(ops[0].output, Some(json!({"seen": 1})));
    assert_eq!(store.op_state("inc", "count").unwrap(), Some(json!(2)));
}

#[tokio::test]
async fn max_parallel_zero_still_makes_progress() {
    let job = Job::builder("narrow")
        .max_parallel(0)
        .op(Op::new("a", |_| async { Ok(json!(1)) }))
        .op(Op::new("b", |_| async { Ok(json!(2)) }).after(["a"]))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("narrow", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    assert!(
        ops.iter().all(|o| o.status == OpStatus::Success),
        "ops were silently dropped"
    );
}

// a -> b -> c, where b fails until `fixed` flips. a's output counts its own
// calls, so a reused seed and a recomputed one are told apart by value.
struct Chain {
    job: Job,
    a_calls: Arc<AtomicU32>,
    b_saw: Arc<Mutex<Option<Value>>>,
    fixed: Arc<AtomicBool>,
}

fn chain() -> Chain {
    let a_calls = Arc::new(AtomicU32::new(0));
    let b_saw: Arc<Mutex<Option<Value>>> = Arc::new(Mutex::new(None));
    let fixed = Arc::new(AtomicBool::new(false));
    let (calls, saw, ok) = (a_calls.clone(), b_saw.clone(), fixed.clone());
    let job = Job::builder("chain")
        .op(Op::new("a", move |_| {
            let calls = calls.clone();
            async move { Ok(json!({ "rows": calls.fetch_add(1, Ordering::SeqCst) })) }
        }))
        .op(Op::new("b", move |ctx| {
            let (saw, ok) = (saw.clone(), ok.clone());
            async move {
                *saw.lock().unwrap() = ctx.input("a").cloned();
                if !ok.load(Ordering::SeqCst) {
                    return Err("b exploded".into());
                }
                Ok(json!("b done"))
            }
        })
        .after(["a"]))
        .op(Op::new(
            "c",
            |ctx| async move { Ok(ctx.input("b").cloned().unwrap()) },
        )
        .after(["b"]))
        .build()
        .unwrap();
    Chain {
        job,
        a_calls,
        b_saw,
        fixed,
    }
}

async fn settled(runner: &Runner, id: &str) -> Run {
    let (store, wanted) = (runner.store().clone(), id.to_string());
    wait_until(move || {
        let status = store.run(&wanted).unwrap().unwrap().status;
        !matches!(status, RunStatus::Queued | RunStatus::Running)
    })
    .await;
    runner.store().run(id).unwrap().unwrap()
}

fn op_names(runner: &Runner, run_id: &str) -> Vec<String> {
    runner
        .store()
        .op_runs(run_id)
        .unwrap()
        .into_iter()
        .map(|o| o.op)
        .collect()
}

#[tokio::test]
async fn resume_reruns_only_the_failed_subset() {
    let Chain {
        job,
        a_calls,
        fixed,
        ..
    } = chain();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);

    fixed.store(true, Ordering::SeqCst);
    let second = runner.resume(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    // the failed op and its downstream, and nothing that already succeeded
    assert_eq!(op_names(&runner, &second.id), ["b", "c"]);
    assert_eq!(a_calls.load(Ordering::SeqCst), 1, "a ran again");
}

#[tokio::test]
async fn resume_seeds_the_recorded_upstream_output() {
    let Chain {
        job, b_saw, fixed, ..
    } = chain();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    let recorded = runner
        .store()
        .op_runs(&first.id)
        .unwrap()
        .into_iter()
        .find(|o| o.op == "a")
        .unwrap()
        .output;
    assert_eq!(recorded, Some(json!({"rows": 0})));

    fixed.store(true, Ordering::SeqCst);
    let second = runner.resume(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    // b read the first run's output through ctx.input, value for value
    assert_eq!(*b_saw.lock().unwrap(), recorded);
    let ops = runner.store().op_runs(&second.id).unwrap();
    assert_eq!(ops[1].output, Some(json!("b done")));
}

#[tokio::test]
async fn resumed_run_records_trigger_params_and_parent() {
    let Chain { job, fixed, .. } = chain();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({"n": 5}), Trigger::Manual)
        .await
        .unwrap();
    fixed.store(true, Ordering::SeqCst);
    let second = runner.resume(&first.id).unwrap();
    let second = settled(&runner, &second).await;

    assert_eq!(second.trigger, Trigger::Resume);
    assert_eq!(second.params, json!({"n": 5}));
    assert_eq!(second.resumed_from.as_deref(), Some(first.id.as_str()));
    assert_eq!(first.resumed_from, None);
}

#[tokio::test]
async fn chained_resume_seeds_from_two_runs_back() {
    let ran: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let two_ok = Arc::new(AtomicBool::new(false));
    let three_ok = Arc::new(AtomicBool::new(false));
    // one -> two -> three -> four, breaking one op further along each run
    let step = |name: &'static str, dep: Option<&'static str>, ok: Option<Arc<AtomicBool>>| {
        let ran = ran.clone();
        let op = Op::new(name, move |ctx: OpCtx| {
            let (ran, ok) = (ran.clone(), ok.clone());
            async move {
                ran.lock().unwrap().push(name.to_string());
                if ok.is_some_and(|f| !f.load(Ordering::SeqCst)) {
                    return Err(format!("{name} exploded").into());
                }
                Ok(json!({ "op": name, "from": dep.and_then(|d| ctx.input(d).cloned()) }))
            }
        });
        match dep {
            Some(d) => op.after([d]),
            None => op,
        }
    };
    let job = Job::builder("steps")
        .op(step("one", None, None))
        .op(step("two", Some("one"), Some(two_ok.clone())))
        .op(step("three", Some("two"), Some(three_ok.clone())))
        .op(step("four", Some("three"), None))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();

    let first = runner
        .run("steps", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    two_ok.store(true, Ordering::SeqCst);
    let second = runner.resume(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Failed);
    assert_eq!(op_names(&runner, &second.id), ["four", "three", "two"]);

    three_ok.store(true, Ordering::SeqCst);
    let third = runner.resume(&second.id).unwrap();
    let third = settled(&runner, &third).await;
    assert_eq!(third.status, RunStatus::Success);
    assert_eq!(op_names(&runner, &third.id), ["four", "three"]);
    assert_eq!(third.resumed_from.as_deref(), Some(second.id.as_str()));

    // one succeeded two hops back and two one hop back; neither ran again
    assert_eq!(
        *ran.lock().unwrap(),
        ["one", "two", "two", "three", "three", "four"]
    );
    let ops = runner.store().op_runs(&third.id).unwrap();
    let three = ops.iter().find(|o| o.op == "three").unwrap();
    // three's input carries two's output, which carries one's: the seed for
    // two came from the original run, through the resume chain
    assert_eq!(
        three.output,
        Some(json!({
            "op": "three",
            "from": { "op": "two", "from": { "op": "one", "from": null } }
        }))
    );
}

#[tokio::test]
async fn resume_from_reruns_the_chosen_op_and_downstream() {
    let Chain {
        job,
        a_calls,
        b_saw,
        fixed,
    } = chain();
    fixed.store(true, Ordering::SeqCst);
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Success);
    *b_saw.lock().unwrap() = None;

    // a succeeded, but the ask is to run again from b whatever its last status
    let second = runner
        .resume_from(&first.id, Some(&["b".to_string()]))
        .unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    assert_eq!(op_names(&runner, &second.id), ["b", "c"]);
    assert_eq!(a_calls.load(Ordering::SeqCst), 1, "a ran again");
    assert_eq!(*b_saw.lock().unwrap(), Some(json!({"rows": 0})));

    let err = runner
        .resume_from(&first.id, Some(&["ghost".to_string()]))
        .unwrap_err();
    assert!(matches!(err, Error::Graph(_)), "{err}");
    assert!(err.to_string().contains("ghost"), "{err}");
}

#[tokio::test]
async fn resume_rejects_unknown_active_and_successful_runs() {
    let Chain { job, fixed, .. } = chain();
    let slow = Job::builder("slow")
        .op(Op::new("nap", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        }))
        .build()
        .unwrap();
    let runner = Runner::new([job, slow], Store::open(":memory:").unwrap()).unwrap();

    let err = runner.resume("nope").unwrap_err();
    assert!(matches!(err, Error::UnknownRun(_)), "{err}");
    assert_eq!(err.to_string(), "unknown run: nope");

    let live = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    let err = runner.resume(&live).unwrap_err();
    assert!(matches!(err, Error::RunActive(_)), "{err}");
    assert_eq!(runner.cancel(&live).unwrap(), CancelOutcome::Requested);

    fixed.store(true, Ordering::SeqCst);
    let good = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(good.status, RunStatus::Success);
    let err = runner.resume(&good.id).unwrap_err();
    assert!(matches!(err, Error::RunNotFailed(_)), "{err}");
    // the same run is still a valid starting point for a targeted re-run
    assert!(
        runner
            .resume_from(&good.id, Some(&["c".to_string()]))
            .is_ok()
    );
}

#[tokio::test]
async fn resume_refuses_a_changed_graph() {
    let store = Store::open(":memory:").unwrap();
    let Chain { job, .. } = chain();
    let runner = Runner::new([job], store.clone()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    // the job has since grown an op the run never knew about
    let grown = Job::builder("chain")
        .op(Op::new("a", |_| async { Ok(json!(1)) }))
        .op(Op::new("b", |_| async { Ok(json!(2)) }).after(["a"]))
        .op(Op::new("c", |_| async { Ok(json!(3)) }).after(["b"]))
        .op(Op::new("d", |_| async { Ok(json!(4)) }).after(["c"]))
        .build()
        .unwrap();
    let err = Runner::new([grown], store.clone())
        .unwrap()
        .resume(&first.id)
        .unwrap_err();
    assert!(matches!(err, Error::Graph(_)), "{err}");
    assert!(err.to_string().contains("only in the job: d"), "{err}");

    // and the other way: an op the run recorded is gone
    let shrunk = Job::builder("chain")
        .op(Op::new("a", |_| async { Ok(json!(1)) }))
        .build()
        .unwrap();
    let err = Runner::new([shrunk], store.clone())
        .unwrap()
        .resume(&first.id)
        .unwrap_err();
    assert!(err.to_string().contains("only in the run: b, c"), "{err}");

    // no half-built run survives a refusal
    assert_eq!(
        store.runs(None, None, None, None, None, 10).unwrap().len(),
        1
    );
}

// ---- replay ----

/// everything the run log holds about one run, as text: the row, its op runs
/// and every event of it.
///
/// what a replay must not change. compared as strings rather than field by
/// field, so a column added later is covered by this without anybody
/// remembering to add it.
fn history(runner: &Runner, run_id: &str) -> String {
    let store = runner.store();
    let run = store.run(run_id).unwrap().unwrap();
    let ops = store.op_runs(run_id).unwrap();
    let events = store.events(run_id, 0).unwrap();
    format!("{:?}\n{:?}\n{:?}", run, ops, events)
}

#[tokio::test]
async fn a_replayed_op_reads_the_input_the_original_gave_it() {
    let Chain {
        job,
        a_calls,
        b_saw,
        fixed,
    } = chain();
    fixed.store(true, Ordering::SeqCst);
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Success);
    let recorded = runner
        .store()
        .op_runs(&first.id)
        .unwrap()
        .into_iter()
        .find(|o| o.op == "a")
        .unwrap()
        .output;
    assert_eq!(recorded, Some(json!({"rows": 0})));
    *b_saw.lock().unwrap() = None;

    let second = runner
        .replay_ops(&first.id, Some(&["b".to_string()]))
        .unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    // b read what the original run handed it, and a — which would have
    // produced a different number this time — never ran
    assert_eq!(*b_saw.lock().unwrap(), recorded);
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
}

// exactly the ops asked for. this is the whole of what separates a replay
// from a resume: `resume_from` would take b and everything below it
#[tokio::test]
async fn a_replay_runs_the_ops_it_was_given_and_nothing_downstream() {
    let Chain {
        job,
        a_calls,
        fixed,
        ..
    } = chain();
    fixed.store(true, Ordering::SeqCst);
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Success);

    let second = runner
        .replay_ops(&first.id, Some(&["b".to_string()]))
        .unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    assert_eq!(op_names(&runner, &second.id), ["b"]);
    assert_eq!(a_calls.load(Ordering::SeqCst), 1, "a ran again");

    // and the same run resumed from b takes c with it, which is the other thing
    let resumed = runner
        .resume_from(&first.id, Some(&["b".to_string()]))
        .unwrap();
    let resumed = settled(&runner, &resumed).await;
    assert_eq!(op_names(&runner, &resumed.id), ["b", "c"]);
}

#[tokio::test]
async fn a_replay_of_a_failed_op_succeeds_while_the_original_stays_failed() {
    let Chain { job, fixed, .. } = chain();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);
    let before = history(&runner, &first.id);

    // the fix
    fixed.store(true, Ordering::SeqCst);
    let second = runner.replay(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    // the one op that failed, on the input it failed on
    assert_eq!(op_names(&runner, &second.id), ["b"]);

    // and the run that failed is still the run that failed, to the byte
    assert_eq!(history(&runner, &first.id), before);
    let first = runner.store().run(&first.id).unwrap().unwrap();
    assert_eq!(first.status, RunStatus::Failed);
    assert!(first.error.unwrap().contains("b exploded"));
}

// the run log has to be able to say which of the two happened, because they
// mean opposite things: a resume re-runs what did not succeed, and a replay
// re-runs what did
#[tokio::test]
async fn a_replayed_run_says_it_is_a_replay_and_a_resumed_one_says_it_resumed() {
    let Chain { job, fixed, .. } = chain();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({"n": 5}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    fixed.store(true, Ordering::SeqCst);
    let replayed = runner.replay(&first.id).unwrap();
    let replayed = settled(&runner, &replayed).await;
    assert_eq!(replayed.trigger, Trigger::Replay);
    assert_eq!(replayed.replay_of.as_deref(), Some(first.id.as_str()));
    assert_eq!(replayed.resumed_from, None);
    // and it carries what the original was launched with
    assert_eq!(replayed.params, json!({"n": 5}));

    let resumed = runner.resume(&first.id).unwrap();
    let resumed = settled(&runner, &resumed).await;
    assert_eq!(resumed.trigger, Trigger::Resume);
    assert_eq!(resumed.resumed_from.as_deref(), Some(first.id.as_str()));
    assert_eq!(resumed.replay_of, None);
}

#[tokio::test]
async fn a_replay_writes_nothing_to_the_run_it_replays() {
    let dir = tempfile::tempdir().unwrap();
    let runner = file_backed(dir.path(), vec![chain_job("etl")]);
    let first = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(first.status, RunStatus::Success);
    let before = history(&runner, &first.id);
    let written = dir.path().join(&first.id).join("extract.json");
    let bytes = std::fs::read(&written).unwrap();

    let second = runner
        .replay_ops(&first.id, Some(&["load".to_string()]))
        .unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);

    // not a row, not an event, not a byte of what it wrote
    assert_eq!(history(&runner, &first.id), before);
    assert_eq!(std::fs::read(&written).unwrap(), bytes);
    // the new run's own output went under the new run, as any run's does
    assert!(dir.path().join(&second.id).join("load.json").exists());
    assert!(!dir.path().join(&second.id).join("extract.json").exists());
}

// retention takes an io manager's files with the run, so an old run's values
// go when its rows do. a replay of one would run an op on a value it never
// received, which is not a replay of anything
#[tokio::test]
async fn a_replay_whose_inputs_are_gone_refuses_and_names_them() {
    let dir = tempfile::tempdir().unwrap();
    let runner = file_backed(dir.path(), vec![chain_job("etl")]);
    let first = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(first.status, RunStatus::Success);

    // what a retention sweep does to the run's outputs
    std::fs::remove_dir_all(dir.path().join(&first.id)).unwrap();

    let err = runner
        .replay_ops(&first.id, Some(&["load".to_string()]))
        .unwrap_err();
    let said = err.to_string();
    assert!(
        matches!(&err, Error::ReplayInput { op, dep, .. } if op == "load" && dep == "extract"),
        "{err}"
    );
    assert!(said.contains("load"), "{said}");
    assert!(said.contains("extract"), "{said}");
    // and nothing launched: the refusal is instead of a run, not after one
    assert_eq!(
        runner
            .store()
            .runs(None, None, None, None, None, 10)
            .unwrap()
            .len(),
        1
    );
}

// a run that was itself a subset was handed values it did not produce. they
// are on its plan and nowhere else, and they are what its ops actually read
#[tokio::test]
async fn a_replay_of_a_resumed_run_reads_what_that_run_was_seeded_with() {
    let Chain {
        job,
        a_calls,
        b_saw,
        fixed,
    } = chain();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    fixed.store(true, Ordering::SeqCst);
    let second = runner.resume(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    assert_eq!(op_names(&runner, &second.id), ["b", "c"]);
    *b_saw.lock().unwrap() = None;

    // b's input in that run came from the run before it, through the seed the
    // resume recorded
    let third = runner
        .replay_ops(&second.id, Some(&["b".to_string()]))
        .unwrap();
    let third = settled(&runner, &third).await;
    assert_eq!(third.status, RunStatus::Success);
    assert_eq!(*b_saw.lock().unwrap(), Some(json!({"rows": 0})));
    assert_eq!(a_calls.load(Ordering::SeqCst), 1);
}

// a mapped op is its instances, and what it fanned out over is an ordinary
// dep — so a replay of one re-expands over the array the original run
// expanded over, rather than over whatever the source says today
#[tokio::test]
async fn a_replay_of_a_mapped_op_expands_over_the_array_it_expanded_over() {
    let pages: Arc<Mutex<Value>> = Arc::new(Mutex::new(json!([1, 2, 3])));
    let seen: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let (listed, saw, broken) = (
        pages.clone(),
        seen.clone(),
        Arc::new(AtomicBool::new(false)),
    );
    let fails = broken.clone();
    let job = Job::builder("fanout")
        .op(Op::new("pages", move |_| {
            let listed = listed.clone();
            let pages = listed.lock().unwrap().clone();
            async move { Ok(pages) }
        }))
        .op(Op::mapped("process", move |_ctx: OpCtx, page: u32| {
            let (saw, fails) = (saw.clone(), fails.clone());
            async move {
                saw.lock().unwrap().push(page);
                if !fails.load(Ordering::SeqCst) && page == 2 {
                    return Err("page 2 exploded".into());
                }
                Ok(json!(page * 10))
            }
        })
        .over("pages"))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    // the source has moved on, which is exactly what a replay must not read
    *pages.lock().unwrap() = json!([9]);
    broken.store(true, Ordering::SeqCst);
    seen.lock().unwrap().clear();

    let second = runner.replay(&first.id).unwrap();
    let second = settled(&runner, &second).await;
    assert_eq!(second.status, RunStatus::Success);
    let mut ran = seen.lock().unwrap().clone();
    ran.sort_unstable();
    assert_eq!(ran, [1, 2, 3], "the replay expanded over today's pages");
    // one row per instance and none for the op that listed them
    let mut names = op_names(&runner, &second.id);
    names.sort();
    assert_eq!(names, ["process[0]", "process[1]", "process[2]"]);
}

#[tokio::test]
async fn replay_refuses_what_it_cannot_reproduce() {
    let Chain { job, fixed, .. } = chain();
    let slow = Job::builder("slow")
        .op(Op::new("nap", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        }))
        .build()
        .unwrap();
    let runner = Runner::new([job, slow], Store::open(":memory:").unwrap()).unwrap();

    let err = runner.replay("nope").unwrap_err();
    assert!(matches!(err, Error::UnknownRun(_)), "{err}");

    let live = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    let err = runner.replay(&live).unwrap_err();
    assert!(matches!(err, Error::RunActive(_)), "{err}");
    assert_eq!(runner.cancel(&live).unwrap(), CancelOutcome::Requested);

    // a run where nothing failed has nothing a plain replay would re-run
    fixed.store(true, Ordering::SeqCst);
    let good = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(good.status, RunStatus::Success);
    let err = runner.replay(&good.id).unwrap_err();
    assert!(matches!(err, Error::NothingToReplay(_)), "{err}");
    assert!(err.to_string().contains("no op of run"), "{err}");

    // an op the job does not have, and one it has that this run never ran
    let err = runner
        .replay_ops(&good.id, Some(&["ghost".to_string()]))
        .unwrap_err();
    assert!(err.to_string().contains("does not have: ghost"), "{err}");
    let partial = runner
        .replay_ops(&good.id, Some(&["c".to_string()]))
        .unwrap();
    settled(&runner, &partial).await;
    let err = runner
        .replay_ops(&partial, Some(&["a".to_string()]))
        .unwrap_err();
    assert!(err.to_string().contains("never ran: a"), "{err}");
}

// ---- cancellation, pools, timeouts, run errors ----

// waits past `wait_until`'s three seconds: a canceled run holds its grace
// period open before it will say anything about ops that never came back
async fn settled_slowly(runner: &Runner, id: &str) -> Run {
    for _ in 0..2000 {
        let run = runner.store().run(id).unwrap().unwrap();
        if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {id} never reached a terminal status");
}

// blocking work cannot be aborted. one op polls the cancel signal and stops,
// the other never yields at all, and the record has to tell them apart instead
// of stamping a finish time on both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_stops_polling_blocking_work_and_owns_up_to_the_rest() {
    let stopped_at: Arc<Mutex<Option<std::time::Instant>>> = Arc::new(Mutex::new(None));
    let release = Arc::new(AtomicBool::new(false));
    let (mark, freed) = (stopped_at.clone(), release.clone());
    let job = Job::builder("blocking")
        .op(Op::new("polls", move |ctx| {
            let mark = mark.clone();
            async move {
                let verdict = tokio::task::spawn_blocking(move || {
                    for _ in 0..600 {
                        if ctx.is_cancelled() {
                            *mark.lock().unwrap() = Some(std::time::Instant::now());
                            return "stopped on request";
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    "ran to completion"
                })
                .await?;
                Ok(json!(verdict))
            }
        }))
        .op(Op::new("ignores", move |_| {
            let freed = freed.clone();
            async move {
                // never awaits and never reads the signal: nothing can stop it,
                // and the test itself is the only thing that ever will
                for _ in 0..3000 {
                    if freed.load(Ordering::SeqCst) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
                Ok(json!("ran to completion"))
            }
        }))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let id = runner
        .launch("blocking", json!({}), Trigger::Manual)
        .unwrap();
    {
        let (runner, id) = (runner.clone(), id.clone());
        wait_until(move || {
            runner
                .store()
                .op_runs(&id)
                .unwrap()
                .iter()
                .filter(|o| o.status == OpStatus::Running)
                .count()
                == 2
        })
        .await;
    }

    let asked = std::time::Instant::now();
    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
    let run = settled_slowly(&runner, &id).await;
    assert_eq!(run.status, RunStatus::Canceled);

    let ops = runner.store().op_runs(&run.id).unwrap();
    let by_name = |name: &str| ops.iter().find(|o| o.op == name).unwrap();

    // the polling op saw the signal and stopped, well inside the grace period
    let stopped = stopped_at
        .lock()
        .unwrap()
        .expect("blocking work never saw the cancel");
    assert!(
        stopped.duration_since(asked) < Duration::from_secs(2),
        "polling op took {:?} to notice",
        stopped.duration_since(asked)
    );
    let polls = by_name("polls");
    assert_eq!(polls.status, OpStatus::Canceled);
    assert!(
        polls.finished_at.is_some(),
        "an op that stopped has a finish"
    );
    assert_eq!(polls.error.as_deref(), Some("canceled"));

    // the op that never yielded is still running right now, and says so
    let ignores = by_name("ignores");
    assert_eq!(ignores.status, OpStatus::Canceled);
    let msg = ignores.error.as_deref().unwrap_or_default();
    assert!(msg.contains("not observed to stop"), "{msg}");
    assert!(msg.contains("is_cancelled"), "{msg}");
    assert_eq!(
        ignores.finished_at, None,
        "claimed a finish time for work that was still running"
    );
    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.op.as_deref() == Some("ignores")
                && e.message.contains("not observed to stop")),
        "the log never mentions the op that would not stop"
    );

    release.store(true, Ordering::SeqCst);
}

fn pooled_job(name: &str, gauge: &Arc<AtomicU32>, peak: &Arc<AtomicU32>) -> Job {
    let mut builder = Job::builder(name);
    for i in 0..4 {
        let (gauge, peak) = (gauge.clone(), peak.clone());
        builder = builder.op(Op::new(format!("call{i}"), move |_| {
            let (gauge, peak) = (gauge.clone(), peak.clone());
            async move {
                let now = gauge.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(40)).await;
                gauge.fetch_sub(1, Ordering::SeqCst);
                Ok(json!(null))
            }
        })
        .pool("api"));
    }
    builder.build().unwrap()
}

// max_parallel is per job, so two jobs overlapping used to add up. a pool is
// the whole process's budget for one external resource.
#[tokio::test]
async fn a_pool_caps_ops_across_two_overlapping_jobs() {
    let gauge = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let runner = Runner::with_pools(
        [
            pooled_job("pull_a", &gauge, &peak),
            pooled_job("pull_b", &gauge, &peak),
        ],
        Store::open(":memory:").unwrap(),
        vec![],
        [("api".to_string(), 2)],
    )
    .unwrap();

    let a = runner.launch("pull_a", json!({}), Trigger::Manual).unwrap();
    let b = runner.launch("pull_b", json!({}), Trigger::Manual).unwrap();
    assert_eq!(settled(&runner, &a).await.status, RunStatus::Success);
    assert_eq!(settled(&runner, &b).await.status, RunStatus::Success);

    assert!(
        peak.load(Ordering::SeqCst) <= 2,
        "peak was {}, over a pool limit of 2",
        peak.load(Ordering::SeqCst)
    );
    // success has to mean all eight ran, not that the survivors passed
    let ran = [&a, &b]
        .iter()
        .flat_map(|id| runner.store().op_runs(id).unwrap())
        .filter(|o| o.status == OpStatus::Success)
        .count();
    assert_eq!(ran, 8);
    // an op that queued for a permit says so rather than sitting silently
    let events = runner.store().events(&a, 0).unwrap();
    assert!(
        events
            .iter()
            .any(|e| e.message.contains("waiting for a api")),
        "no sign of the queue in the log"
    );
}

// one op per job, so the two can only overlap through the pool they share
fn one_slot_job(name: &str, op: Op) -> Job {
    Job::builder(name).op(op.pool("solo")).build().unwrap()
}

fn takes_the_slot(started: &Arc<AtomicBool>) -> Job {
    let started = started.clone();
    one_slot_job(
        "next",
        Op::new("call", move |_: OpCtx| {
            let started = started.clone();
            async move {
                started.store(true, Ordering::SeqCst);
                Ok(json!(null))
            }
        }),
    )
}

fn one_slot_runner(jobs: [Job; 2]) -> Runner {
    Runner::with_pools(
        jobs,
        Store::open(":memory:").unwrap(),
        vec![],
        [("solo".to_string(), 1)],
    )
    .unwrap()
}

// a pool caps what is calling the api, not what is waiting to be told it has
// stopped. a cancel aborts the op's task at its next await, and blocking work
// the body started is still running when it does — so the slot has to outlive
// the task. it does because blocking work holds the ctx it polls for the
// cancel, and the slot rides that ctx.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_slot_is_held_until_the_work_it_admitted_stops() {
    let calling = Arc::new(AtomicBool::new(false));
    let finish = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let (busy, until) = (calling.clone(), finish.clone());
    let blocker = one_slot_job(
        "long_call",
        Op::new("call", move |ctx: OpCtx| {
            let (busy, until) = (busy.clone(), until.clone());
            async move {
                tokio::task::spawn_blocking(move || {
                    busy.store(true, Ordering::SeqCst);
                    // one chunk of an api call, which polling cannot interrupt
                    // half way through: the cancel is only seen after it. the
                    // ceiling is so that a failing assertion below reports
                    // rather than leaving a thread nobody can join
                    for _ in 0..1_000 {
                        if until.load(Ordering::SeqCst) {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    let stopping = ctx.is_cancelled();
                    busy.store(false, Ordering::SeqCst);
                    stopping
                })
                .await?;
                Ok(json!(null))
            }
        }),
    );

    let runner = one_slot_runner([blocker, takes_the_slot(&started)]);
    let id = runner
        .launch("long_call", json!({}), Trigger::Manual)
        .unwrap();
    {
        let calling = calling.clone();
        wait_until(move || calling.load(Ordering::SeqCst)).await;
    }
    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);

    // the run is over and its task is gone; the call it made is not
    let next = runner.launch("next", json!({}), Trigger::Manual).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        calling.load(Ordering::SeqCst),
        "the blocking call ended before the test could prove anything"
    );
    assert!(
        !started.load(Ordering::SeqCst),
        "a second op entered a pool of one while the first was still calling"
    );
    let waited = runner.store().events(&next, 0).unwrap();
    assert!(
        waited
            .iter()
            .any(|e| e.message.contains("waiting for a solo")),
        "the second op was not waiting on the pool at all"
    );

    // and it is a slot, not a leak: the call ends, and the next op gets in
    finish.store(true, Ordering::SeqCst);
    assert_eq!(settled(&runner, &next).await.status, RunStatus::Success);
    assert!(started.load(Ordering::SeqCst));
}

// the other half of the same rule: an op that yields is aborted as promptly as
// it ever was, and its slot goes back with it rather than waiting on anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_canceled_op_that_yields_gives_its_slot_back_at_once() {
    let running = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let live = running.clone();
    let blocker = one_slot_job(
        "sleeper",
        Op::new("call", move |_: OpCtx| {
            let live = live.clone();
            async move {
                live.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(json!(null))
            }
        }),
    );

    let runner = one_slot_runner([blocker, takes_the_slot(&started)]);
    let id = runner
        .launch("sleeper", json!({}), Trigger::Manual)
        .unwrap();
    {
        let running = running.clone();
        wait_until(move || running.load(Ordering::SeqCst)).await;
    }
    let asked = std::time::Instant::now();
    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
    let next = runner.launch("next", json!({}), Trigger::Manual).unwrap();
    assert_eq!(settled(&runner, &next).await.status, RunStatus::Success);
    assert!(
        asked.elapsed() < Duration::from_secs(2),
        "the slot took {:?} to come back",
        asked.elapsed()
    );
}

#[tokio::test]
async fn an_undeclared_pool_is_refused() {
    let job = || {
        Job::builder("pull")
            .op(Op::new("call", |_| async { Ok(json!(null)) }).pool("api"))
            .build()
            .unwrap()
    };
    let err = Runner::with_pools(
        [job()],
        Store::open(":memory:").unwrap(),
        vec![],
        Vec::new(),
    )
    .err()
    .unwrap();
    assert!(matches!(err, Error::Graph(_)), "{err}");
    assert!(err.to_string().contains("not declared"), "{err}");

    let err = Runner::with_pools(
        [job()],
        Store::open(":memory:").unwrap(),
        vec![],
        [("api".to_string(), 1), ("api".to_string(), 3)],
    )
    .err()
    .unwrap();
    assert!(err.to_string().contains("declared twice"), "{err}");

    // a runner assembled without pools at all must not quietly run unlimited
    let runner = Runner::new([job()], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("pull", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let op = &runner.store().op_runs(&run.id).unwrap()[0];
    assert!(
        op.error.as_deref().unwrap_or_default().contains("api"),
        "{:?}",
        op.error
    );
}

/// a job of `ops` calls that each record when they were let through, all of
/// them ready at once so the limit is the only thing spacing them.
fn throttled_job(name: &str, ops: usize, at: &Arc<Mutex<Vec<Instant>>>) -> Job {
    let mut builder = Job::builder(name);
    for i in 0..ops {
        let at = at.clone();
        builder = builder.op(Op::new(format!("call{i}"), move |_| {
            let at = at.clone();
            async move {
                at.lock().unwrap().push(Instant::now());
                Ok(json!(null))
            }
        })
        .rate("api"));
    }
    builder.build().unwrap()
}

// a pool caps how many calls are in flight, which is a rate only if you know
// how long each one takes. this is the limit the api publishes.
#[tokio::test]
async fn a_rate_lets_a_burst_through_and_spaces_out_the_rest() {
    let at: Arc<Mutex<Vec<Instant>>> = Arc::new(Mutex::new(Vec::new()));
    // two per 200ms: one token every 100ms once the burst is gone
    let runner = Runner::new(
        [throttled_job("pull", 6, &at)],
        Store::open(":memory:").unwrap(),
    )
    .unwrap()
    .with_rates([("api".to_string(), 2, Duration::from_millis(200))])
    .unwrap();

    let started = Instant::now();
    let run = runner
        .run("pull", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let at = at.lock().unwrap().clone();
    assert_eq!(at.len(), 6, "not every op ran");
    // four of them had to wait for a token, which is four spacings — a lower
    // bound, because a busy machine can only make a run take longer
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "six calls at two per 200ms took {:?}",
        started.elapsed()
    );
    // and they were let through in order rather than in a clump at the end
    assert!(at.windows(2).all(|w| w[0] <= w[1]));

    // an op waiting for a token says so, exactly as one waiting for a pool
    // permit does: an op sitting in `running` with nothing happening is what
    // makes people stop believing a scheduler
    let waited = runner
        .store()
        .events(&run.id, 0)
        .unwrap()
        .into_iter()
        .filter(|e| e.message == "waiting for a api token")
        .count();
    assert_eq!(
        waited, 4,
        "the log does not say what the queued ops waited on"
    );
}

// the two limits are about different things, and an op that declares both
// answers to both
#[tokio::test]
async fn an_op_that_takes_a_permit_and_a_token_waits_for_both() {
    let gauge = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let mut builder = Job::builder("pull");
    for i in 0..4 {
        let (gauge, peak) = (gauge.clone(), peak.clone());
        builder = builder.op(Op::new(format!("call{i}"), move |_| {
            let (gauge, peak) = (gauge.clone(), peak.clone());
            async move {
                let now = gauge.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(10)).await;
                gauge.fetch_sub(1, Ordering::SeqCst);
                Ok(json!(null))
            }
        })
        .pool("api")
        .rate("api"));
    }
    let runner = Runner::with_pools(
        [builder.build().unwrap()],
        Store::open(":memory:").unwrap(),
        vec![],
        [("api".to_string(), 1)],
    )
    .unwrap()
    .with_rates([("api".to_string(), 2, Duration::from_millis(400))])
    .unwrap();

    let started = Instant::now();
    let run = runner
        .run("pull", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    // the pool: never two calls at once
    assert_eq!(peak.load(Ordering::SeqCst), 1);
    // the rate: the third and fourth waited 200ms and 400ms for a token, which
    // forty milliseconds of work through a pool of one would never have taken
    assert!(
        started.elapsed() >= Duration::from_millis(400),
        "the rate did not bind: {:?}",
        started.elapsed()
    );
}

/// a job of one call that records nothing and takes a token, for the cases
/// that are about the queue rather than about what ran.
fn one_call_job(name: &str) -> Job {
    Job::builder(name)
        .op(Op::new("call", |_| async { Ok(json!(null)) }).rate("api"))
        .build()
        .unwrap()
}

// a token is spent rather than returned, so one taken by an op that is already
// dying is a call nobody makes — and a call the op behind it should have been
// making.
#[tokio::test]
async fn a_run_canceled_while_it_waits_for_a_token_takes_none_with_it() {
    let runner = Runner::new([one_call_job("pull")], Store::open(":memory:").unwrap())
        .unwrap()
        .with_rates([("api".to_string(), 1, Duration::from_secs(3))])
        .unwrap();

    // the first has the only token in the bucket; the second and third are
    // queued behind it, three and six seconds out
    let first = runner.launch("pull", json!({}), Trigger::Manual).unwrap();
    assert_eq!(settled(&runner, &first).await.status, RunStatus::Success);
    let doomed = runner.launch("pull", json!({}), Trigger::Manual).unwrap();
    let behind = runner.launch("pull", json!({}), Trigger::Manual).unwrap();
    let queued = runner.rates();
    wait_until(|| runner.rates()[0].waiting == 2).await;

    let canceled = Instant::now();
    assert_eq!(
        runner.cancel(&doomed).unwrap(),
        CancelOutcome::Requested,
        "{queued:?}"
    );
    assert_eq!(settled(&runner, &doomed).await.status, RunStatus::Canceled);
    // it stopped waiting rather than waiting out the period it was queued for
    assert!(
        canceled.elapsed() < Duration::from_secs(2),
        "a canceled run took {:?} to stop waiting",
        canceled.elapsed()
    );
    wait_until(|| runner.rates()[0].waiting == 1).await;

    // and the token it was holding went to the op behind it: that one goes at
    // three seconds, where it would have gone if the canceled run had never
    // asked, rather than at six. settled_slowly, so a token spent on nobody
    // fails on what it cost rather than on a timeout that says nothing
    let ran = settled_slowly(&runner, &behind).await;
    assert_eq!(ran.status, RunStatus::Success);
    assert!(
        canceled.elapsed() < Duration::from_secs(5),
        "the op behind waited {:?}, so the canceled run spent a token on nothing",
        canceled.elapsed()
    );
    assert_eq!(runner.rates()[0].waiting, 0);
}

// a queue that lets a latecomer in first is a queue that can starve whoever is
// at the head of it
#[tokio::test]
async fn the_op_that_waited_longest_for_a_token_is_served_first() {
    let order: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen = order.clone();
    let job = Job::builder("pull")
        .op(Op::new("call", move |ctx: OpCtx| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(ctx.run_id().to_string());
                Ok(json!(null))
            }
        })
        .rate("api"))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap())
        .unwrap()
        .with_rates([("api".to_string(), 1, Duration::from_millis(400))])
        .unwrap();

    // launched far enough apart that which one asked first is not a race, and
    // well inside one period so that all three are queued at once
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(runner.launch("pull", json!({}), Trigger::Manual).unwrap());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for id in &ids {
        assert_eq!(settled(&runner, id).await.status, RunStatus::Success);
    }
    assert_eq!(
        *order.lock().unwrap(),
        ids,
        "the tokens went out in an order the arrivals were not in"
    );
}

#[tokio::test]
async fn an_undeclared_rate_is_refused() {
    let job = || {
        Job::builder("pull")
            .op(Op::new("call", |_| async { Ok(json!(null)) }).rate("api"))
            .build()
            .unwrap()
    };
    let err = Runner::new([job()], Store::open(":memory:").unwrap())
        .unwrap()
        .with_rates([])
        .err()
        .unwrap();
    assert!(matches!(err, Error::Graph(_)), "{err}");
    assert!(err.to_string().contains("not declared"), "{err}");

    let err = Runner::new([job()], Store::open(":memory:").unwrap())
        .unwrap()
        .with_rates([
            ("api".to_string(), 1, Duration::from_secs(1)),
            ("api".to_string(), 3, Duration::from_secs(1)),
        ])
        .err()
        .unwrap();
    assert!(err.to_string().contains("declared twice"), "{err}");

    // and a runner assembled without rates at all must not quietly run
    // unthrottled, exactly as it must not run an undeclared pool unlimited
    let runner = Runner::new([job()], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("pull", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let op = &runner.store().op_runs(&run.id).unwrap()[0];
    assert!(
        op.error.as_deref().unwrap_or_default().contains("api"),
        "{:?}",
        op.error
    );
}

// a hung op used to run forever, holding its slot and blocking Overlap::Skip
#[tokio::test]
async fn op_timeout_fires_retries_and_trips_the_cancel_signal() {
    let noticed = Arc::new(AtomicBool::new(false));
    let seen = noticed.clone();
    let job = Job::builder("hung")
        .op(Op::new("hang", move |ctx| {
            let noticed = noticed.clone();
            async move {
                // blocking work the timeout can only ask to stop
                tokio::task::spawn_blocking(move || {
                    for _ in 0..300 {
                        if ctx.is_cancelled() {
                            noticed.store(true, Ordering::SeqCst);
                            return;
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                });
                // and an op that never returns on its own
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(json!("never"))
            }
        })
        .timeout(Duration::from_millis(120))
        .retries(1)
        .retry_delay(Duration::from_millis(10)))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("hung", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let op = &runner.store().op_runs(&run.id).unwrap()[0];
    assert_eq!(op.status, OpStatus::Failed);
    assert_eq!(op.attempts, 2, "a timeout must go through the retry policy");
    let err = op.error.as_deref().unwrap_or_default();
    assert!(err.contains("timed out after 120ms"), "{err}");

    let events = runner.store().events(&run.id, 0).unwrap();
    let retry = events
        .iter()
        .find(|e| e.kind == EventKind::OpRetry)
        .expect("no retry event");
    assert!(retry.message.contains("timed out after 120ms"), "{retry:?}");

    // the timeout trips the same signal cancellation does
    wait_until(move || seen.load(Ordering::SeqCst)).await;
}

// an on_failure hook could name the op, the run row could not
#[tokio::test]
async fn a_failed_run_names_the_failing_op_in_its_error() {
    let (seen, hook) = collector();
    let brittle = Job::builder("brittle")
        .op(Op::new("pull", |_| async { Ok(json!(1)) }))
        .op(Op::new("push", |_| async { Err("429 too many requests".into()) }).after(["pull"]))
        .op(Op::new("report", |_| async { Ok(json!(null)) }).after(["push"]))
        .build()
        .unwrap();
    let clean = Job::builder("clean")
        .op(Op::new("noop", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
    let runner = Runner::with_failure_hooks(
        [brittle, clean],
        Store::open(":memory:").unwrap(),
        vec![hook],
    )
    .unwrap();

    let run = runner
        .run("brittle", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let error = run
        .error
        .as_deref()
        .expect("a failed run with a null error is the bug");
    assert!(error.contains("push"), "{error}");
    assert!(error.contains("429 too many requests"), "{error}");

    // the hook and the run row must not tell different stories
    {
        let seen = seen.clone();
        wait_until(move || seen.lock().unwrap().len() == 1).await;
    }
    let failure = seen.lock().unwrap()[0].clone();
    assert_eq!(
        error,
        format!(
            "op {} failed: {}",
            failure.failed_op.unwrap(),
            failure.error.unwrap()
        )
    );
    // and it reads the same back out of the list endpoint's query
    let listed = runner
        .store()
        .runs(None, None, None, None, None, 10)
        .unwrap();
    assert_eq!(listed[0].error.as_deref(), Some(error));

    let ok = runner
        .run("clean", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(ok.status, RunStatus::Success);
    assert_eq!(ok.error, None, "a run that worked carries no error");
}

// ---- dynamic fan-out -------------------------------------------------------

// pages -> process (mapped over pages) -> total. `body` decides what one
// instance does, so each test below varies only that.
fn fanout_job<F, Fut>(pages: Value, body: F) -> Job
where
    F: Fn(OpCtx, u32) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
{
    Job::builder("fanout")
        .op(Op::new("pages", move |_| {
            let pages = pages.clone();
            async move { Ok(pages) }
        }))
        .op(Op::mapped("process", body).over("pages"))
        .op(Op::new("total", |ctx| async move {
            let rows = ctx.input("process").unwrap().as_array().unwrap().len();
            Ok(json!({ "instances": rows }))
        })
        .after(["process"]))
        .build()
        .unwrap()
}

async fn doubling(_: OpCtx, page: u32) -> OpResult {
    Ok(json!(page * 2))
}

fn op_row<'a>(rows: &'a [hestan::OpRun], name: &str) -> &'a hestan::OpRun {
    rows.iter()
        .find(|r| r.op == name)
        .unwrap_or_else(|| panic!("no op_runs row for {name}"))
}

#[tokio::test]
async fn fan_out_runs_one_instance_per_element_in_element_order() {
    let job = fanout_job(json!([3, 1, 2]), |_ctx, page: u32| async move {
        // the slowest element is first, so completion order is not element order
        tokio::time::sleep(Duration::from_millis(u64::from(page) * 40)).await;
        Ok(json!({ "page": page }))
    });
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    // the mapped op itself has no row; its instances are the record
    assert_eq!(
        names,
        ["pages", "process[0]", "process[1]", "process[2]", "total"]
    );
    assert!(rows.iter().all(|r| r.status == OpStatus::Success));
    assert_eq!(op_row(&rows, "process[0]").output, Some(json!({"page": 3})));

    // downstream sees the collected array in element order, not finish order
    assert_eq!(op_row(&rows, "total").output, Some(json!({"instances": 3})));
    let events = runner.store().events(&run.id, 0).unwrap();
    let expanded = events
        .iter()
        .find(|e| e.kind == EventKind::OpExpanded)
        .expect("no expansion event");
    assert_eq!(expanded.op.as_deref(), Some("process"));
    assert_eq!(expanded.data.as_ref().unwrap()["instances"], json!(3));
}

#[tokio::test]
async fn a_mapped_op_hands_downstream_its_outputs_in_element_order() {
    let job = Job::builder("ordered")
        .op(Op::new("pages", |_| async { Ok(json!([3, 1, 2])) }))
        .op(Op::mapped("process", |_ctx: OpCtx, page: u32| async move {
            tokio::time::sleep(Duration::from_millis(u64::from(page) * 40)).await;
            Ok(json!(page))
        })
        .over("pages"))
        .op(Op::new("echo", |ctx| async move {
            Ok(ctx.input("process").cloned().unwrap())
        })
        .after(["process"]))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("ordered", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(op_row(&rows, "echo").output, Some(json!([3, 1, 2])));
}

#[tokio::test]
async fn an_empty_array_fans_out_to_nothing_and_downstream_still_runs() {
    let job = fanout_job(json!([]), doubling);
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["pages", "total"]);
    assert_eq!(op_row(&rows, "total").output, Some(json!({"instances": 0})));
}

#[tokio::test]
async fn mapping_over_a_non_array_fails_the_op_and_skips_downstream() {
    let job = fanout_job(json!("not a list"), doubling);
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    let error = run.error.as_deref().unwrap();
    assert_eq!(
        error,
        "op process failed: mapped over pages, which produced a string rather than an array"
    );
    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["pages", "total"]);
    assert_eq!(op_row(&rows, "total").status, OpStatus::Skipped);
}

#[tokio::test]
async fn one_failed_instance_fails_the_run_and_skips_downstream() {
    let job = fanout_job(json!([1, 2, 3]), |_ctx, page: u32| async move {
        if page == 2 {
            return Err("page 2 is gone".into());
        }
        Ok(json!(page))
    });
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(
        run.error.as_deref(),
        Some("op process[1] failed: page 2 is gone")
    );
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(op_row(&rows, "process[1]").status, OpStatus::Failed);
    // the siblings are ordinary tasks: a failure never cancels them
    assert_eq!(op_row(&rows, "process[0]").status, OpStatus::Success);
    assert_eq!(op_row(&rows, "process[2]").status, OpStatus::Success);
    assert_eq!(op_row(&rows, "total").status, OpStatus::Skipped);
    assert_eq!(op_row(&rows, "total").output, None);
}

#[tokio::test]
async fn an_element_that_does_not_deserialize_names_its_instance() {
    let job = fanout_job(json!([1, "two", 3]), doubling);
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert!(
        run.error
            .as_deref()
            .unwrap()
            .starts_with("op process[1] failed: type check failed:"),
        "{:?}",
        run.error
    );
    let events = runner.store().events(&run.id, 0).unwrap();
    let tcf = events
        .iter()
        .find(|e| e.kind == EventKind::TypeCheckFailed)
        .unwrap();
    assert_eq!(tcf.op.as_deref(), Some("process[1]"));
}

#[tokio::test]
async fn instances_respect_max_parallel() {
    let live = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let (l, p) = (live.clone(), peak.clone());
    let job = Job::builder("capped")
        .max_parallel(2)
        .op(Op::new("pages", |_| async {
            Ok(json!([1, 2, 3, 4, 5, 6]))
        }))
        .op(Op::mapped("process", move |_ctx: OpCtx, page: u32| {
            let (live, peak) = (l.clone(), p.clone());
            async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(60)).await;
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(json!(page))
            }
        })
        .over("pages"))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("capped", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(peak.load(Ordering::SeqCst), 2);
    assert_eq!(runner.store().op_runs(&run.id).unwrap().len(), 7);
}

#[tokio::test]
async fn instances_take_pool_permits_like_any_other_op() {
    let live = Arc::new(AtomicU32::new(0));
    let peak = Arc::new(AtomicU32::new(0));
    let (l, p) = (live.clone(), peak.clone());
    let job = Job::builder("pooled")
        .op(Op::new("pages", |_| async { Ok(json!([1, 2, 3, 4])) }))
        .op(Op::mapped("process", move |_ctx: OpCtx, page: u32| {
            let (live, peak) = (l.clone(), p.clone());
            async move {
                let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(60)).await;
                live.fetch_sub(1, Ordering::SeqCst);
                Ok(json!(page))
            }
        })
        .over("pages")
        .pool("api"))
        .build()
        .unwrap();

    let runner = Runner::with_pools(
        [job],
        Store::open(":memory:").unwrap(),
        vec![],
        [("api".to_string(), 1)],
    )
    .unwrap();
    let run = runner
        .run("pooled", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(peak.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_resume_re_expands_a_mapped_op_and_reuses_a_whole_one() {
    let fail_late = Arc::new(AtomicBool::new(true));
    let flag = fail_late.clone();
    let job = Job::builder("resumable")
        .op(Op::new("pages", |_| async { Ok(json!([1, 2])) }))
        .op(Op::mapped("process", |_ctx: OpCtx, page: u32| async move {
            Ok(json!(page * 10))
        })
        .over("pages"))
        .op(Op::new("total", move |ctx| {
            let flag = flag.clone();
            async move {
                if flag.swap(false, Ordering::SeqCst) {
                    return Err("downstream blew up".into());
                }
                Ok(ctx.input("process").cloned().unwrap())
            }
        })
        .after(["process"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("resumable", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    // every instance succeeded, so the mapped op is reused whole
    let plan = runner.resume_plan(&first.id, None).unwrap();
    assert_eq!(plan.reuse, ["pages", "process"]);
    assert_eq!(plan.rerun, ["total"]);

    let second = runner.resume(&first.id).unwrap();
    assert_eq!(settled(&runner, &second).await.status, RunStatus::Success);
    let rows = runner.store().op_runs(&second).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["total"], "a reused mapped op must not re-expand");
    assert_eq!(op_row(&rows, "total").output, Some(json!([10, 20])));

    // re-running from the mapped op expands it again, instances and all
    let third = runner
        .resume_from(&first.id, Some(&["process".to_string()]))
        .unwrap();
    assert_eq!(settled(&runner, &third).await.status, RunStatus::Success);
    let names: Vec<String> = runner
        .store()
        .op_runs(&third)
        .unwrap()
        .into_iter()
        .map(|r| r.op)
        .collect();
    assert_eq!(names, ["process[0]", "process[1]", "total"]);
}

#[tokio::test]
async fn a_partly_failed_mapped_op_re_expands_on_resume() {
    let fail_once = Arc::new(AtomicBool::new(true));
    let flag = fail_once.clone();
    let job = Job::builder("flaky")
        .op(Op::new("pages", |_| async { Ok(json!([1, 2])) }))
        .op(Op::mapped("process", move |_ctx: OpCtx, page: u32| {
            let flag = flag.clone();
            async move {
                if page == 2 && flag.swap(false, Ordering::SeqCst) {
                    return Err("page 2 is gone".into());
                }
                Ok(json!(page * 10))
            }
        })
        .over("pages"))
        .op(Op::new("total", |ctx| async move {
            Ok(ctx.input("process").cloned().unwrap())
        })
        .after(["process"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let first = runner
        .run("flaky", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    // one instance failed, so there is no output to reuse: it expands again
    let plan = runner.resume_plan(&first.id, None).unwrap();
    assert_eq!(plan.reuse, ["pages"]);
    assert_eq!(plan.rerun, ["process", "total"]);

    let second = runner.resume(&first.id).unwrap();
    assert_eq!(settled(&runner, &second).await.status, RunStatus::Success);
    let rows = runner.store().op_runs(&second).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["process[0]", "process[1]", "total"]);
    assert_eq!(op_row(&rows, "total").output, Some(json!([10, 20])));
}

#[tokio::test]
async fn canceling_a_run_stops_and_records_every_instance() {
    let job = Job::builder("slow")
        .max_parallel(1)
        .op(Op::new("pages", |_| async { Ok(json!([1, 2, 3])) }))
        .op(Op::mapped("process", |_ctx: OpCtx, _page: u32| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        })
        .over("pages"))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let id = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    {
        let (runner, id) = (runner.clone(), id.clone());
        wait_until(move || {
            runner
                .store()
                .op_runs(&id)
                .unwrap()
                .iter()
                .any(|r| r.op == "process[0]" && r.status == OpStatus::Running)
        })
        .await;
    }
    assert_eq!(runner.cancel(&id).unwrap(), CancelOutcome::Requested);
    assert_eq!(settled(&runner, &id).await.status, RunStatus::Canceled);

    let rows = runner.store().op_runs(&id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["pages", "process[0]", "process[1]", "process[2]"]);
    // the running one was aborted, the two still queued were never started
    assert_eq!(op_row(&rows, "process[0]").status, OpStatus::Canceled);
    assert_eq!(op_row(&rows, "process[1]").status, OpStatus::Canceled);
    assert_eq!(op_row(&rows, "process[2]").status, OpStatus::Canceled);
}

#[test]
fn a_mapped_op_must_say_what_it_maps_over() {
    let err = Job::builder("bad")
        .op(Op::new("pages", |_| async { Ok(json!([])) }))
        .op(Op::mapped(
            "process",
            |_ctx: OpCtx, _n: u32| async move { Ok(json!(null)) },
        )
        .after(["pages"]))
        .build()
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("names no dep to map over"),
        "{err}"
    );

    // and .over on an op that isn't mapped means nothing, so it is refused
    let err = Job::builder("bad")
        .op(Op::new("pages", |_| async { Ok(json!([])) }))
        .op(Op::new("process", |_| async { Ok(json!(null)) }).over("pages"))
        .build()
        .err()
        .unwrap();
    assert!(
        err.to_string().contains("without being an Op::mapped"),
        "{err}"
    );
}

// an instance is `{op}[{label}]` and that name is the whole record of it, so
// an op called what an instance would be called is read as one — for the life
// of the deployment, in the ui, in op stats and on a resume
#[test]
fn an_op_named_like_an_instance_of_a_mapped_op_is_refused() {
    let err = Job::builder("collide")
        .op(Op::new("pages", |_| async { Ok(json!([])) }))
        .op(Op::mapped(
            "process",
            |_ctx: OpCtx, _n: u32| async move { Ok(json!(null)) },
        )
        .over("pages"))
        .op(Op::new("process[extra]", |_| async { Ok(json!(null)) }))
        .build()
        .err()
        .unwrap();
    assert!(
        err.to_string()
            .contains("is named what an instance of the mapped op process is named"),
        "{err}"
    );

    // and `pages` is what a fan-out expands over rather than a fan-out, so an
    // op named after one of its would-be instances is just an op
    Job::builder("fine")
        .op(Op::new("pages", |_| async { Ok(json!([])) }))
        .op(Op::mapped(
            "process",
            |_ctx: OpCtx, _n: u32| async move { Ok(json!(null)) },
        )
        .over("pages"))
        .op(Op::new("pages[extra]", |_| async { Ok(json!(null)) }))
        .build()
        .unwrap();
}

// one region per element, one site list per region, one probe per site. the
// collected value keeps the shape it expanded in — flattened it would say
// nothing about which region a reading came from, which is the only reason to
// nest a fan-out at all
fn nested_job(sites: fn(u32) -> Value) -> Job {
    Job::builder("nested")
        .op(Op::new("regions", |_| async { Ok(json!([2, 1])) }))
        .op(
            Op::mapped("sites", move |_ctx: OpCtx, region: u32| async move {
                Ok(sites(region))
            })
            .over("regions"),
        )
        .op(Op::mapped("probe", |_ctx: OpCtx, site: u32| async move {
            // the earlier the site, the longer it takes, so completion order
            // is not element order at either level
            tokio::time::sleep(Duration::from_millis(u64::from(60 - site))).await;
            Ok(json!(site))
        })
        .over("sites"))
        .op(Op::new("report", |ctx: OpCtx| async move {
            Ok(ctx.input("probe").cloned().unwrap())
        })
        .after(["probe"]))
        .build()
        .unwrap()
}

#[tokio::test]
async fn a_fan_out_inside_a_fan_out_collects_in_element_order_at_both_levels() {
    let job = nested_job(|region| json!((0..=region).map(|i| region * 10 + i).collect::<Vec<_>>()));
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nested", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    // an inner instance carries the outer one it belongs to; neither mapped op
    // has a row of its own
    assert_eq!(
        names,
        [
            "probe[0][0]",
            "probe[0][1]",
            "probe[0][2]",
            "probe[1][0]",
            "probe[1][1]",
            "regions",
            "report",
            "sites[0]",
            "sites[1]",
        ]
    );
    assert_eq!(op_row(&rows, "probe[1][1]").output, Some(json!(11)));
    // nested, not flattened: `[20, 21, 22, 10, 11]` would have lost the region
    assert_eq!(
        op_row(&rows, "report").output,
        Some(json!([[20, 21, 22], [10, 11]]))
    );
}

#[tokio::test]
async fn a_failing_inner_instance_fails_the_fan_out_it_belongs_to_and_not_the_one_beside_it() {
    let job = Job::builder("nested")
        .op(Op::new("regions", |_| async { Ok(json!([2, 1])) }))
        .op(Op::mapped("sites", |_ctx: OpCtx, region: u32| async move {
            Ok(json!([region * 10, region * 10 + 1]))
        })
        .over("regions"))
        .op(Op::mapped("probe", |_ctx: OpCtx, site: u32| async move {
            match site {
                11 => Err("site 11 is unreachable".into()),
                _ => Ok(json!(site)),
            }
        })
        .over("sites"))
        .op(Op::new("report", |_| async { Ok(json!(null)) }).after(["probe"]))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nested", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(
        run.error.as_deref(),
        Some("op probe[1][1] failed: site 11 is unreachable")
    );
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(op_row(&rows, "probe[1][1]").status, OpStatus::Failed);
    // the sibling in its own fan-out and both of the fan-out beside it are
    // ordinary tasks: a failure never cancels them
    assert_eq!(op_row(&rows, "probe[1][0]").status, OpStatus::Success);
    assert_eq!(op_row(&rows, "probe[0][0]").status, OpStatus::Success);
    assert_eq!(op_row(&rows, "probe[0][1]").status, OpStatus::Success);
    // and there is no partial array at either level
    assert_eq!(op_row(&rows, "report").status, OpStatus::Skipped);
}

#[tokio::test]
async fn an_outer_element_that_yields_nothing_makes_no_inner_instances() {
    let job = nested_job(|region| match region {
        1 => json!([]),
        _ => json!([region * 10]),
    });
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nested", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(
        names,
        ["probe[0][0]", "regions", "report", "sites[0]", "sites[1]"]
    );
    // an empty fan-out inside one is an empty array in its place, not a gap
    assert_eq!(op_row(&rows, "report").output, Some(json!([[20], []])));
}

// a fan-out is one line whose size is decided at run time, so the run has to
// be able to refuse one — while refusing it still costs nothing
#[tokio::test]
async fn an_expansion_past_the_ceiling_fails_without_writing_an_instance_row() {
    let runner = Runner::new(
        [fanout_job(json!([1, 2, 3, 4]), doubling)],
        Store::open(":memory:").unwrap(),
    )
    .unwrap()
    .with_max_instances(3);
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(
        run.error.as_deref(),
        Some(
            "op process failed: op process expands over pages into 4 instances, one for each of \
             its 4 elements; with the 0 this run has already made that is past the ceiling of 3 \
             op runs one run may expand to. flattening inside pages is usually the better shape, \
             and Hestan::max_instances raises the ceiling"
        )
    );
    // the point of failing at the expansion: none of it happened
    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["pages", "total"]);
    assert_eq!(op_row(&rows, "total").status, OpStatus::Skipped);

    // and the same fan-out with room for it is a fan-out like any other
    let runner = Runner::new(
        [fanout_job(json!([1, 2, 3, 4]), doubling)],
        Store::open(":memory:").unwrap(),
    )
    .unwrap()
    .with_max_instances(4);
    let run = runner
        .run("fanout", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    assert_eq!(runner.store().op_runs(&run.id).unwrap().len(), 6);
}

#[tokio::test]
async fn the_ceiling_counts_every_level_of_a_nesting() {
    // two regions, three sites and two sites: seven op runs of fan-out
    let job =
        || nested_job(|region| json!((0..=region).map(|i| region * 10 + i).collect::<Vec<_>>()));
    let runner = Runner::new([job()], Store::open(":memory:").unwrap())
        .unwrap()
        .with_max_instances(6);
    let run = runner
        .run("nested", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    // the outer fan-out is spent budget by the time the inner one is counted,
    // and the inner count is every instance of it across every outer element
    assert_eq!(
        run.error.as_deref(),
        Some(
            "op probe failed: op probe expands over sites into 5 instances, one for each of its \
             5 elements; with the 2 this run has already made that is past the ceiling of 6 op \
             runs one run may expand to. flattening inside sites is usually the better shape, \
             and Hestan::max_instances raises the ceiling"
        )
    );
    let rows = runner.store().op_runs(&run.id).unwrap();
    let names: Vec<&str> = rows.iter().map(|r| r.op.as_str()).collect();
    assert_eq!(names, ["regions", "report", "sites[0]", "sites[1]"]);

    // seven is exactly what it comes to, so seven is enough
    let runner = Runner::new([job()], Store::open(":memory:").unwrap())
        .unwrap()
        .with_max_instances(7);
    let run = runner
        .run("nested", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
}

#[tokio::test]
async fn a_nested_instance_takes_its_element_and_retries_on_its_own() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let job = Job::builder("nested")
        .op(Op::new("regions", |_| async { Ok(json!([2, 1])) }))
        .op(Op::mapped("sites", |_ctx: OpCtx, region: u32| async move {
            Ok(json!([region * 10, region * 10 + 1]))
        })
        .over("regions"))
        .op(Op::mapped("probe", move |ctx: OpCtx, site: u32| {
            let calls = counter.clone();
            async move {
                if site == 11 && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("flaky".into());
                }
                // the other deps are read whole, exactly as at one level
                let regions = ctx.input("regions").cloned().unwrap();
                Ok(json!({ "site": site, "regions": regions }))
            }
        })
        .over("sites")
        .after(["regions"])
        .retries(1)
        .retry_delay(Duration::from_millis(10)))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nested", json!({}), Trigger::Manual)
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(op_row(&rows, "probe[1][0]").attempts, 1);
    assert_eq!(op_row(&rows, "probe[1][1]").attempts, 2);
    assert_eq!(
        op_row(&rows, "probe[0][1]").output,
        Some(json!({ "site": 21, "regions": [2, 1] }))
    );
}

#[tokio::test]
async fn an_instance_reads_its_other_deps_whole() {
    let job = Job::builder("mixed")
        .op(Op::new("pages", |_| async { Ok(json!([1, 2])) }))
        .op(Op::new("config", |_| async { Ok(json!({"scale": 100})) }))
        .op(Op::mapped("process", |ctx: OpCtx, page: u32| async move {
            let scale = ctx.input("config").unwrap()["scale"].as_u64().unwrap();
            // the mapped dep itself still reads as the whole array
            let all = ctx.input("pages").unwrap().as_array().unwrap().len();
            Ok(json!({ "value": u64::from(page) * scale, "of": all }))
        })
        .over("pages")
        .after(["config"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("mixed", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(
        op_row(&rows, "process[1]").output,
        Some(json!({"value": 200, "of": 2}))
    );
}

#[tokio::test]
async fn an_instance_retries_on_its_own_like_a_static_op() {
    let calls = Arc::new(AtomicU32::new(0));
    let counter = calls.clone();
    let job = Job::builder("retried")
        .op(Op::new("pages", |_| async { Ok(json!([1, 2])) }))
        .op(Op::mapped("process", move |_ctx: OpCtx, page: u32| {
            let calls = counter.clone();
            async move {
                if page == 2 && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err("flaky".into());
                }
                Ok(json!(page))
            }
        })
        .over("pages")
        .retries(1)
        .retry_delay(Duration::from_millis(10)))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("retried", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(op_row(&rows, "process[0]").attempts, 1);
    assert_eq!(op_row(&rows, "process[1]").attempts, 2);
}

// ---- trigger rules ----

// the whole point: the op you most want after a failure is the one the
// default rule skips
#[tokio::test]
async fn always_op_runs_after_a_failure_and_reports_dep_statuses() {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let job = Job::builder("nightly")
        .op(Op::new("extract", |_| async { Ok(json!({"rows": 3})) }))
        .op(Op::new("load", |_| async { Err("disk full".into()) }).after(["extract"]))
        .op(Op::new("summary", move |ctx: OpCtx| {
            let seen = seen2.clone();
            async move {
                let mut seen = seen.lock().unwrap();
                for dep in ["extract", "load"] {
                    let status = ctx.dep_status(dep).map_or("none", |s| s.as_str());
                    seen.push(format!("{dep}={status}"));
                }
                // a dep that produced nothing has no input, only a status
                assert_eq!(ctx.input("extract"), Some(&json!({"rows": 3})));
                assert_eq!(ctx.input("load"), None);
                // and a name that isn't a dep at all has neither
                assert_eq!(ctx.dep_status("ghost"), None);
                Ok(json!("reported"))
            }
        })
        .after(["extract", "load"])
        .when(When::Always))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nightly", json!({}), Trigger::Manual)
        .await
        .unwrap();

    // a cleanup that worked does not launder the failure that called it
    assert_eq!(run.status, RunStatus::Failed);
    assert!(run.error.unwrap().contains("op load failed: disk full"));
    let ops = runner.store().op_runs(&run.id).unwrap();
    let row = |name: &str| ops.iter().find(|o| o.op == name).unwrap();
    assert_eq!(row("load").status, OpStatus::Failed);
    assert_eq!(row("summary").status, OpStatus::Success);
    assert_eq!(row("summary").output, Some(json!("reported")));
    assert_eq!(*seen.lock().unwrap(), ["extract=success", "load=failed"]);
}

#[tokio::test]
async fn any_failed_is_skipped_when_everything_succeeded() {
    let alerted = Arc::new(AtomicBool::new(false));
    let flag = alerted.clone();
    let job = Job::builder("watched")
        .op(Op::new("work", |_| async { Ok(json!(1)) }))
        .op(Op::new("alert", move |_| {
            let flag = flag.clone();
            async move {
                flag.store(true, Ordering::SeqCst);
                Ok(json!(null))
            }
        })
        .after(["work"])
        .when(When::AnyFailed))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("watched", json!({}), Trigger::Manual)
        .await
        .unwrap();

    // nothing failed, so the run succeeds even though an op was skipped
    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let row = |name: &str| ops.iter().find(|o| o.op == name).unwrap();
    assert_eq!(row("work").status, OpStatus::Success);
    assert_eq!(row("alert").status, OpStatus::Skipped);
    assert!(!alerted.load(Ordering::SeqCst), "the alert body ran");
}

// a rule-declined skip has to be tellable from an upstream-failure skip, or
// the log cannot say why an op did not run
#[tokio::test]
async fn a_rule_declined_op_records_its_own_skip_event() {
    let job = Job::builder("mixed")
        .op(Op::new("boom", |_| async { Err("no good".into()) }))
        .op(Op::new("cut_off", |_| async { Ok(json!(null)) }).after(["boom"]))
        .op(Op::new("clean", |_| async { Ok(json!(null)) })
            .after(["boom"])
            .when(When::Always))
        .op(Op::new("idle", |_| async { Ok(json!(null)) })
            .after(["clean"])
            .when(When::AnyFailed))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("mixed", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);

    let events = runner.store().events(&run.id, 0).unwrap();
    let skip = |op: &str| {
        events
            .iter()
            .find(|e| e.kind == EventKind::OpSkipped && e.op.as_deref() == Some(op))
            .unwrap_or_else(|| panic!("no skip event for {op}"))
            .clone()
    };
    // the upstream-failure wording names the op that broke, and so does its
    // payload: propagation skips carry which upstream did it
    assert_eq!(skip("cut_off").message, "skipped: upstream boom failed");
    assert_eq!(
        skip("cut_off").data,
        Some(json!({"reason": "skipped: upstream boom failed", "upstream": "boom"}))
    );
    // the rule wording names the rule that was asked, and carries it
    assert_eq!(
        skip("idle").message,
        "skipped by rule any_failed: every dep succeeded"
    );
    assert_eq!(
        skip("idle").data,
        Some(json!({
            "reason": "skipped by rule any_failed: every dep succeeded",
            "when": "any_failed"
        }))
    );
}

// propagation must ask each candidate's rule instead of blanket-skipping a
// whole downstream, and must reach the downstream of whatever it stops at
#[tokio::test]
async fn skip_propagation_stops_at_a_rule_that_still_runs() {
    // boom -> cleanup(always) -> after_cleanup, plus a plain branch either side
    let build = |cleanup_works: bool| {
        Job::builder("chain")
            .op(Op::new("boom", |_| async { Err("no good".into()) }))
            .op(Op::new("cut_off", |_| async { Ok(json!(null)) }).after(["boom"]))
            .op(Op::new("deeper", |_| async { Ok(json!(null)) }).after(["cut_off"]))
            .op(Op::new("cleanup", move |_| async move {
                if cleanup_works {
                    Ok(json!("swept"))
                } else {
                    Err("sweep failed".into())
                }
            })
            .after(["boom"])
            .when(When::Always))
            .op(Op::new("after_cleanup", |ctx: OpCtx| async move {
                Ok(ctx.input("cleanup").cloned().unwrap_or(json!(null)))
            })
            .after(["cleanup"]))
            .build()
            .unwrap()
    };

    let runner = Runner::new([build(true)], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let status = |name: &str| ops.iter().find(|o| o.op == name).unwrap().status;
    // the blanket skip stops at cleanup and never reached what hangs off it
    assert_eq!(status("cut_off"), OpStatus::Skipped);
    assert_eq!(status("deeper"), OpStatus::Skipped);
    assert_eq!(status("cleanup"), OpStatus::Success);
    assert_eq!(status("after_cleanup"), OpStatus::Success);
    let out = ops
        .iter()
        .find(|o| o.op == "after_cleanup")
        .unwrap()
        .output
        .clone();
    assert_eq!(out, Some(json!("swept")));

    // and when the op that stopped it fails, its own downstream is cut off
    let runner = Runner::new([build(false)], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("chain", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let row = ops.iter().find(|o| o.op == "after_cleanup").unwrap();
    assert_eq!(row.status, OpStatus::Skipped);
    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(
        events.iter().any(|e| e.kind == EventKind::OpSkipped
            && e.op.as_deref() == Some("after_cleanup")
            && e.message == "skipped: upstream cleanup failed"),
        "the second failure did not name itself"
    );
}

// a rule applies to a mapped op whole; with no array to expand there is
// nothing to run it over, so it expands into nothing
#[tokio::test]
async fn always_on_a_mapped_op_whose_array_never_arrived_runs_zero_instances() {
    let ran = Arc::new(AtomicU32::new(0));
    let count = ran.clone();
    let job = Job::builder("fanned")
        .op(Op::new("pages", |_| async { Err("no pages".into()) }))
        .op(Op::mapped("process", move |_ctx: OpCtx, page: u32| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(json!(page))
            }
        })
        .over("pages")
        .when(When::Always))
        .op(Op::new("collect", |ctx: OpCtx| async move {
            Ok(ctx.input("process").cloned().unwrap_or(json!(null)))
        })
        .after(["process"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("fanned", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    assert_eq!(ran.load(Ordering::SeqCst), 0, "an instance body ran");

    let ops = runner.store().op_runs(&run.id).unwrap();
    // no instance rows, and the mapped op still has none of its own
    let mut names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["collect", "pages"]);
    // downstream sees the empty fan-out like any other
    let collect = ops.iter().find(|o| o.op == "collect").unwrap();
    assert_eq!(collect.status, OpStatus::Success);
    assert_eq!(collect.output, Some(json!([])));
    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(
        events.iter().any(|e| e.kind == EventKind::OpExpanded
            && e.op.as_deref() == Some("process")
            && e.data == Some(json!({"instances": 0, "over": "pages"}))),
        "no zero-instance expansion recorded"
    );
}

// ---- reusable graphs ----

// a graph instance is flattened at build, so a run only ever sees ops
#[tokio::test]
async fn two_graph_instances_run_independently_end_to_end() {
    let double = Graph::builder("double")
        // a reusable graph cannot know what the job named its dep, so it
        // reads whatever it was handed
        .op(Op::new("parse", |ctx: OpCtx| async move {
            let n: i64 = ctx.inputs().iter().filter_map(|(_, v)| v.as_i64()).sum();
            Ok(json!(n * 2))
        }))
        .op(Op::new("dedupe", |ctx: OpCtx| async move {
            Ok(json!(ctx.input("parse").unwrap().as_i64().unwrap() + 1))
        })
        .after(["parse"]))
        .input("parse")
        .output("dedupe")
        .build()
        .unwrap();

    let job = Job::builder("nightly")
        .op(Op::new("fetch_a", |_| async { Ok(json!(10)) }))
        .op(Op::new("fetch_b", |_| async { Ok(json!(100)) }))
        .graph("clean_a", &double)
        .after(["fetch_a"])
        .graph("clean_b", &double)
        .after(["fetch_b"])
        .op(Op::new("merge", |ctx: OpCtx| async move {
            // an instance is read under the name the job gave it
            Ok(json!([ctx.input("clean_a"), ctx.input("clean_b")]))
        })
        .after(["clean_a", "clean_b"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nightly", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let ops = runner.store().op_runs(&run.id).unwrap();
    let out = |name: &str| {
        ops.iter()
            .find(|o| o.op == name)
            .unwrap_or_else(|| panic!("no op run for {name}"))
            .output
            .clone()
    };
    // each instance saw only its own external dep
    assert_eq!(out("clean_a.parse"), Some(json!(20)));
    assert_eq!(out("clean_b.parse"), Some(json!(200)));
    assert_eq!(out("clean_a.dedupe"), Some(json!(21)));
    assert_eq!(out("clean_b.dedupe"), Some(json!(201)));
    // and the outside got each instance's declared output
    assert_eq!(out("merge"), Some(json!([21, 201])));
}

// nesting is the same transformation one level down, and fan-out inside a
// graph is just a mapped op with a prefixed dep
#[tokio::test]
async fn a_nested_graph_with_fan_out_runs_flattened() {
    let paged = Graph::builder("paged")
        .op(Op::new("pages", |ctx: OpCtx| async move {
            let n = ctx.input("config").and_then(Value::as_u64).unwrap_or(0);
            Ok(json!((0..n).collect::<Vec<u64>>()))
        }))
        .op(Op::mapped("fetch", |_ctx: OpCtx, page: u64| async move {
            Ok(json!(page * 10))
        })
        .over("pages"))
        .input("pages")
        .output("fetch")
        .build()
        .unwrap();
    let stage = Graph::builder("stage")
        .graph("inner", &paged)
        .op(Op::new("total", |ctx: OpCtx| async move {
            let pages = ctx.input("inner").unwrap().as_array().unwrap();
            Ok(json!(pages.iter().filter_map(Value::as_i64).sum::<i64>()))
        })
        .after(["inner"]))
        .input("inner")
        .output("total")
        .build()
        .unwrap();

    let job = Job::builder("nightly")
        .op(Op::new("config", |_| async { Ok(json!(3)) }))
        .graph("s", &stage)
        .after(["config"])
        .op(Op::new("report", |ctx: OpCtx| async move {
            Ok(json!({"total": ctx.input("s")}))
        })
        .after(["s"]))
        .build()
        .unwrap();

    let runner = Runner::new([job], Store::open(":memory:").unwrap()).unwrap();
    let run = runner
        .run("nightly", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let ops = runner.store().op_runs(&run.id).unwrap();
    let mut names: Vec<&str> = ops.iter().map(|o| o.op.as_str()).collect();
    names.sort_unstable();
    // the mapped op has no row of its own; its instances carry the prefix
    assert_eq!(
        names,
        [
            "config",
            "report",
            "s.inner.fetch[0]",
            "s.inner.fetch[1]",
            "s.inner.fetch[2]",
            "s.inner.pages",
            "s.total",
        ]
    );
    let out = |name: &str| ops.iter().find(|o| o.op == name).unwrap().output.clone();
    assert_eq!(out("s.total"), Some(json!(30)));
    assert_eq!(out("report"), Some(json!({"total": 30})));
}

// ---- resources ----

#[derive(Debug)]
struct ApiClient {
    base: String,
}

#[derive(Debug)]
struct Config {
    retries: u32,
}

// one construction, one value, however many ops read it
#[tokio::test]
async fn one_resource_reaches_every_op_that_asks_for_it() {
    let built = Arc::new(AtomicU32::new(0));
    let counter = built.clone();
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (a, b) = (seen.clone(), seen.clone());
    // the same Arc reaching both ops is the claim; addresses prove it
    let addrs: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let (pa, pb) = (addrs.clone(), addrs.clone());

    let job = Job::builder("api_work")
        .op(Op::new("first", move |ctx: OpCtx| {
            let (seen, addrs) = (a.clone(), pa.clone());
            async move {
                let api = ctx.resource::<ApiClient>("api")?;
                seen.lock().unwrap().push(api.base.clone());
                addrs.lock().unwrap().push(Arc::as_ptr(&api) as usize);
                Ok(json!(null))
            }
        })
        .requires(["api"]))
        .op(Op::new("second", move |ctx: OpCtx| {
            let (seen, addrs) = (b.clone(), pb.clone());
            async move {
                let api = ctx.resource::<ApiClient>("api")?;
                seen.lock().unwrap().push(api.base.clone());
                addrs.lock().unwrap().push(Arc::as_ptr(&api) as usize);
                Ok(json!(null))
            }
        })
        .after(["first"])
        .requires(["api"]))
        .build()
        .unwrap();

    let run = Hestan::new()
        .resource("config", |_| async { Ok(Config { retries: 4 }) })
        // a resource may lean on one declared before it
        .resource("api", move |ctx: hestan::ResourceCtx| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                let config = ctx.resource::<Config>("config")?;
                Ok(ApiClient {
                    base: format!("https://api/{}", config.retries),
                })
            }
        })
        .job(job)
        .db(":memory:")
        .run_once("api_work", json!({}))
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(built.load(Ordering::SeqCst), 1, "built more than once");
    assert_eq!(
        *seen.lock().unwrap(),
        ["https://api/4", "https://api/4"],
        "the two ops saw different values"
    );
    let addrs = addrs.lock().unwrap();
    assert_eq!(addrs.len(), 2);
    assert_eq!(addrs[0], addrs[1], "the ops got two different instances");
}

#[tokio::test]
async fn asking_for_the_wrong_type_or_an_unknown_name_says_which() {
    let job = Job::builder("confused")
        .op(Op::new("wrong_type", |ctx: OpCtx| async move {
            let err = ctx.resource::<ApiClient>("config").unwrap_err().to_string();
            assert!(err.contains("resource config is a"), "{err}");
            assert!(err.contains("Config"), "{err}");
            assert!(err.contains("ApiClient"), "{err}");
            let err = ctx.resource::<Config>("ghost").unwrap_err().to_string();
            assert_eq!(err, "no resource named ghost");
            Ok(json!("checked"))
        }))
        .build()
        .unwrap();

    let run = Hestan::new()
        .resource("config", |_| async { Ok(Config { retries: 1 }) })
        .job(job)
        .db(":memory:")
        .run_once("confused", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
}

// declaring is how you find out at startup instead of at 3am
#[tokio::test]
async fn a_required_resource_that_is_not_registered_fails_the_build() {
    let job = || {
        Job::builder("needy")
            .op(Op::new("call", |_| async { Ok(json!(null)) }).requires(["api"]))
            .build()
            .unwrap()
    };

    let err = Hestan::new()
        .job(job())
        .db(":memory:")
        .run_once("needy", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Graph(_)), "{err}");
    assert!(
        err.to_string()
            .contains("op call requires resource api, which is not registered"),
        "{err}"
    );

    let run = Hestan::new()
        .resource("api", |_| async {
            Ok(ApiClient {
                base: "https://api".into(),
            })
        })
        .job(job())
        .db(":memory:")
        .run_once("needy", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    // and a run-scoped one satisfies the same declaration: `requires` is about
    // whether the name is registered, not about how long the value lives
    let run = Hestan::new()
        .run_resource("api", |_| async {
            Ok(ApiClient {
                base: "https://api".into(),
            })
        })
        .job(job())
        .db(":memory:")
        .run_once("needy", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
}

// the two scopes at one call site: a client every run shares and a scratch
// directory none of them does, read the same way by the same op
#[tokio::test]
async fn a_run_scoped_resource_is_one_value_for_the_whole_run_and_names_it() {
    let scratches = Arc::new(AtomicU32::new(0));
    let counted = scratches.clone();
    let seen: Arc<Mutex<Vec<(String, usize)>>> = Arc::new(Mutex::new(Vec::new()));
    let (first, second) = (seen.clone(), seen.clone());
    let reader = move |seen: Arc<Mutex<Vec<(String, usize)>>>| {
        move |ctx: OpCtx| {
            let seen = seen.clone();
            async move {
                let api = ctx.resource::<ApiClient>("api")?;
                let scratch = ctx.resource::<String>("scratch")?;
                assert_eq!(api.base, "https://api");
                seen.lock()
                    .unwrap()
                    .push((scratch.to_string(), Arc::as_ptr(&scratch) as usize));
                Ok(json!(null))
            }
        }
    };
    let job = Job::builder("work")
        .op(Op::new("head", reader(first)).requires(["api", "scratch"]))
        .op(Op::new("tail", reader(second))
            .after(["head"])
            .requires(["api", "scratch"]))
        .build()
        .unwrap();

    let run = Hestan::new()
        .resource("api", |_| async {
            Ok(ApiClient {
                base: "https://api".into(),
            })
        })
        .run_resource("scratch", move |ctx: hestan::ResourceCtx| {
            let counted = counted.clone();
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                // the run it belongs to, which is what a scratch directory
                // wants to be named after
                Ok(format!("/tmp/{}", ctx.run_id().unwrap_or("nowhere")))
            }
        })
        .job(job)
        .db(":memory:")
        .run_once("work", json!({}))
        .await
        .unwrap();

    assert_eq!(run.status, RunStatus::Success);
    // one value for the run, not one per op that asked
    assert_eq!(scratches.load(Ordering::SeqCst), 1);
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].0, format!("/tmp/{}", run.id));
    assert_eq!(seen[0], seen[1], "the two ops got two different values");
}

// a process whose client could not be built has nothing useful to serve, and
// must not leave a database behind saying otherwise
#[tokio::test]
async fn a_failing_constructor_aborts_startup() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();

    let err = Hestan::new()
        .resource("api", |_| async {
            Err::<ApiClient, _>("no credentials".into())
        })
        .job(
            Job::builder("work")
                .op(Op::new("go", move |_| {
                    let flag = flag.clone();
                    async move {
                        flag.store(true, Ordering::SeqCst);
                        Ok(json!(null))
                    }
                }))
                .build()
                .unwrap(),
        )
        .db(db.to_str().unwrap())
        .run_once("work", json!({}))
        .await
        .unwrap_err();

    assert!(
        matches!(&err, Error::Resource { name, .. } if name == "api"),
        "{err}"
    );
    assert!(err.to_string().contains("no credentials"), "{err}");
    assert!(!ran.load(Ordering::SeqCst), "an op ran anyway");
    assert!(!db.exists(), "the store opened despite the failed startup");

    // and a name declared twice is caught the same way
    let err = Hestan::new()
        .resource("api", |_| async { Ok(Config { retries: 1 }) })
        .resource("api", |_| async { Ok(Config { retries: 2 }) })
        .job(
            Job::builder("work")
                .op(Op::new("go", |_| async { Ok(json!(null)) }))
                .build()
                .unwrap(),
        )
        .db(":memory:")
        .run_once("work", json!({}))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("declared twice"), "{err}");
}

// ---- io managers ----

// a manager that refuses to persist anything, to prove a failed put is a
// failed op rather than a success with a lost output
struct Refuses;

impl IoManager for Refuses {
    fn put(&self, _key: &IoKey, _value: Value) -> IoResult {
        Err("nowhere to put it".into())
    }
    fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
        Ok(handle.clone())
    }
    fn drop_run(&self, _run_id: &str, _job: &str) -> IoDropped {
        Ok(())
    }
}

fn file_backed(dir: &std::path::Path, jobs: Vec<Job>) -> Runner {
    Runner::with_io(
        jobs,
        Store::open(":memory:").unwrap(),
        Vec::new(),
        Vec::new(),
        Arc::new(FileIo::new(dir)),
        Vec::new(),
    )
    .unwrap()
}

fn chain_job(name: &str) -> Job {
    Job::builder(name)
        .op(Op::new("extract", |_| async {
            Ok(json!({"rows": [1, 2, 3]}))
        }))
        .op(Op::new("load", |ctx: OpCtx| async move {
            let rows = ctx.input("extract").unwrap()["rows"].as_array().unwrap();
            Ok(json!({"loaded": rows.len()}))
        })
        .after(["extract"]))
        .build()
        .unwrap()
}

#[tokio::test]
async fn file_io_keeps_the_value_out_of_the_run_log_and_hands_it_downstream() {
    let dir = tempfile::tempdir().unwrap();
    let runner = file_backed(dir.path(), vec![chain_job("etl")]);
    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let ops = runner.store().op_runs(&run.id).unwrap();
    let out = |name: &str| ops.iter().find(|o| o.op == name).unwrap().output.clone();
    // the run log holds a reference, not the value
    let path = dir.path().join(&run.id).join("extract.json");
    assert_eq!(
        out("extract"),
        Some(json!({ "$io": "file", "path": path.to_string_lossy() }))
    );
    assert!(path.exists(), "no file at {path:?}");
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        r#"{"rows":[1,2,3]}"#
    );
    // and downstream still saw the value, so `get` ran on the way in
    let loaded = out("load").unwrap();
    let loaded = std::fs::read_to_string(loaded["path"].as_str().unwrap()).unwrap();
    assert_eq!(loaded, r#"{"loaded":3}"#);
}

// recording success for a value that was not persisted would strand the next
// run, which would seed a handle to nothing
#[tokio::test]
async fn a_failing_put_fails_the_op_and_skips_its_downstream() {
    let runner = Runner::with_io(
        [chain_job("etl")],
        Store::open(":memory:").unwrap(),
        Vec::new(),
        Vec::new(),
        Arc::new(Refuses),
        Vec::new(),
    )
    .unwrap();
    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();

    assert_eq!(run.status, RunStatus::Failed);
    assert!(
        run.error.as_deref().unwrap().contains("nowhere to put it"),
        "{:?}",
        run.error
    );
    let ops = runner.store().op_runs(&run.id).unwrap();
    let row = |name: &str| ops.iter().find(|o| o.op == name).unwrap();
    assert_eq!(row("extract").status, OpStatus::Failed);
    assert_eq!(row("extract").output, None);
    assert!(
        row("extract")
            .error
            .as_deref()
            .unwrap()
            .contains("could not persist the output")
    );
    assert_eq!(row("load").status, OpStatus::Skipped);
}

// the resume seed is a handle from an earlier run, so it only works if the
// seeding path resolves it
#[tokio::test]
async fn resume_reads_a_seeded_output_back_out_of_file_io() {
    let dir = tempfile::tempdir().unwrap();
    let fail_once = Arc::new(AtomicBool::new(true));
    let flag = fail_once.clone();
    let job = Job::builder("etl")
        .op(Op::new("extract", |_| async {
            Ok(json!({"rows": [1, 2, 3]}))
        }))
        .op(Op::new("load", move |ctx: OpCtx| {
            let flag = flag.clone();
            async move {
                if flag.swap(false, Ordering::SeqCst) {
                    return Err("first time is unlucky".into());
                }
                let rows = ctx.input("extract").unwrap()["rows"].as_array().unwrap();
                Ok(json!({"loaded": rows.len()}))
            }
        })
        .after(["extract"]))
        .build()
        .unwrap();

    let runner = file_backed(dir.path(), vec![job]);
    let first = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();
    assert_eq!(first.status, RunStatus::Failed);

    let plan = runner.resume_plan(&first.id, None).unwrap();
    assert_eq!(plan.rerun, ["load"]);
    assert_eq!(plan.reuse, ["extract"]);

    let id = runner.resume(&first.id).unwrap();
    let resumed = settled(&runner, &id).await;
    assert_eq!(resumed.status, RunStatus::Success);

    // load ran again and read the value behind the handle the first run wrote
    let ops = runner.store().op_runs(&id).unwrap();
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].op, "load");
    let path = ops[0].output.clone().unwrap();
    assert_eq!(
        std::fs::read_to_string(path["path"].as_str().unwrap()).unwrap(),
        r#"{"loaded":3}"#
    );
    // the seed really did come from the first run's directory
    assert!(dir.path().join(&first.id).join("extract.json").exists());
}

// a memoized build seeds a fresh dep from its materialization while file io
// is the default, and what that row holds is the handle the op run holds:
// one file, named twice, rather than a second copy of the value in the log
#[tokio::test]
async fn asset_memoization_seeds_a_fresh_dep_under_file_io() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let io_dir = dir.path().join("io");
    let builds = Arc::new(AtomicU32::new(0));
    let boot = || {
        let counter = builds.clone();
        let a = Asset::new("a", move |_| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"rows": 3}))
            }
        });
        let b = Asset::new("b", |ctx: OpCtx| async move {
            let rows = ctx.input("a").unwrap()["rows"].as_u64().unwrap();
            Ok(json!({"doubled": rows * 2}))
        })
        .from(&a);
        Hestan::new()
            .assets([a, b])
            .io(FileIo::new(&io_dir))
            .db(db.to_str().unwrap())
    };

    let run = boot().build_asset("b").await.unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let store = Store::open(db.to_str().unwrap()).unwrap();
    // the materialization and the op run name the same file
    let rows = store.op_runs(&run.id).unwrap();
    let a_out = rows.iter().find(|o| o.op == "a").unwrap().output.clone();
    assert_eq!(a_out.as_ref().unwrap()["$io"], "file");
    assert_eq!(
        store.materialization("a", None).unwrap().unwrap().value,
        a_out
    );
    let held = store.materialization("b", None).unwrap().unwrap().value;
    let path = held.unwrap()["path"].as_str().unwrap().to_string();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        r#"{"doubled":6}"#,
        "the value is not in the file the row names"
    );
    drop(store);

    // a is fresh now, so the second build seeds it instead of rebuilding it
    let run = boot().build_asset("b").await.unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert_eq!(builds.load(Ordering::SeqCst), 1, "a was rebuilt");
    let store = Store::open(db.to_str().unwrap()).unwrap();
    let rows = store.op_runs(&run.id).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].op, "b");
    // and b, reading the seed back through the manager, got the same answer
    // as before out of a file of its own
    let held = store.materialization("b", None).unwrap().unwrap().value;
    let rebuilt = held.unwrap()["path"].as_str().unwrap().to_string();
    assert_ne!(rebuilt, path, "the second build wrote where the first did");
    assert_eq!(
        std::fs::read_to_string(&rebuilt).unwrap(),
        r#"{"doubled":6}"#
    );
}

#[tokio::test]
async fn an_op_naming_an_unregistered_io_manager_fails_the_build() {
    let job = || {
        Job::builder("etl")
            .op(Op::new("extract", |_| async { Ok(json!(1)) }).io("archive"))
            .build()
            .unwrap()
    };

    let err = Hestan::new()
        .job(job())
        .db(":memory:")
        .run_once("etl", json!({}))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Graph(_)), "{err}");
    assert!(
        err.to_string()
            .contains("op extract persists through io manager archive, which is not registered"),
        "{err}"
    );

    // registered, and the op's output goes there rather than to the default
    let dir = tempfile::tempdir().unwrap();
    let run = Hestan::new()
        .io_named("archive", FileIo::new(dir.path()))
        .job(job())
        .db(":memory:")
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    assert!(dir.path().join(&run.id).join("extract.json").exists());
}

// a mapped op has no row and never puts anything of its own, so the array
// downstream sees is assembled from what its instances' handles resolve to
#[tokio::test]
async fn fan_out_under_file_io_collects_values_not_handles() {
    let dir = tempfile::tempdir().unwrap();
    let job = Job::builder("fanned")
        .op(Op::new("pages", |_| async { Ok(json!([1, 2, 3])) }))
        .op(Op::mapped("fetch", |_ctx: OpCtx, page: u64| async move {
            Ok(json!({"page": page}))
        })
        .over("pages"))
        .op(Op::new("total", |ctx: OpCtx| async move {
            let pages = ctx.input("fetch").unwrap().as_array().unwrap();
            let sum: u64 = pages.iter().map(|p| p["page"].as_u64().unwrap()).sum();
            Ok(json!({"sum": sum}))
        })
        .after(["fetch"]))
        .build()
        .unwrap();

    let runner = file_backed(dir.path(), vec![job]);
    let run = runner
        .run("fanned", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);

    let ops = runner.store().op_runs(&run.id).unwrap();
    // every instance persisted under its own name
    for i in 0..3 {
        let path = dir.path().join(&run.id).join(format!("fetch[{i}].json"));
        assert!(path.exists(), "no file at {path:?}");
    }
    let total = ops.iter().find(|o| o.op == "total").unwrap();
    let path = total.output.clone().unwrap();
    assert_eq!(
        std::fs::read_to_string(path["path"].as_str().unwrap()).unwrap(),
        r#"{"sum":6}"#
    );

    // and a resume past the fan-out seeds the same collected array
    let plan = runner
        .resume_plan(&run.id, Some(&["total".into()]))
        .unwrap();
    assert_eq!(plan.rerun, ["total"]);
    assert_eq!(plan.reuse, ["pages", "fetch"]);
    let id = runner
        .resume_from(&run.id, Some(&["total".into()]))
        .unwrap();
    let resumed = settled(&runner, &id).await;
    assert_eq!(resumed.status, RunStatus::Success);
    let rows = runner.store().op_runs(&id).unwrap();
    let path = rows[0].output.clone().unwrap();
    assert_eq!(
        std::fs::read_to_string(path["path"].as_str().unwrap()).unwrap(),
        r#"{"sum":6}"#
    );
}

/// two gates the manager blocks on, each announcing that it is stuck before
/// it waits — so the op that opens one can be sure it did not open it early.
#[derive(Default)]
struct Gates {
    put_blocked: AtomicBool,
    put_open: AtomicBool,
    get_blocked: AtomicBool,
    get_open: AtomicBool,
}

/// a manager whose calls block until another op of the same run has run.
/// nothing here is a timing assertion: the only way out of a `put` is another
/// op making progress, which it cannot do if this call is on the task driving
/// the run.
struct Blocks(Arc<Gates>);

impl IoManager for Blocks {
    fn put(&self, _key: &IoKey, value: Value) -> IoResult {
        stuck(&self.0.put_blocked, &self.0.put_open)?;
        Ok(value)
    }
    fn get(&self, _key: &IoKey, handle: &Value) -> IoResult {
        stuck(&self.0.get_blocked, &self.0.get_open)?;
        Ok(handle.clone())
    }
    fn drop_run(&self, _run_id: &str, _job: &str) -> IoDropped {
        Ok(())
    }
}

/// block this thread until `open`, saying on `blocked` that it is waiting. a
/// bounded wait, so a run that cannot get past this fails the test with a
/// sentence rather than hanging the suite.
fn stuck(blocked: &AtomicBool, open: &AtomicBool) -> Result<(), String> {
    blocked.store(true, Ordering::SeqCst);
    for _ in 0..1000 {
        if open.load(Ordering::SeqCst) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Err("nothing else in the run ran while the manager was blocked".into())
}

// a manager doing something slow must not stop the rest of the run. the ops
// here are the proof rather than a stopwatch: `slow` cannot be persisted until
// `opens_the_put` has run, and `after`'s input cannot be fetched until
// `opens_the_get` has, so a manager called on the run's own task deadlocks
// until the wait above gives up
#[tokio::test]
async fn a_manager_that_blocks_does_not_stop_the_rest_of_the_run() {
    let gates = Arc::new(Gates::default());
    // an op that waits for the manager to be stuck on one gate, then opens it
    let opener =
        |name: &'static str, gates: &Arc<Gates>, pick: fn(&Gates) -> (&AtomicBool, &AtomicBool)| {
            let gates = gates.clone();
            Op::new(name, move |_| {
                let gates = gates.clone();
                async move {
                    let (blocked, open) = pick(&gates);
                    wait_until(|| blocked.load(Ordering::SeqCst)).await;
                    open.store(true, Ordering::SeqCst);
                    Ok(json!(null))
                }
            })
        };

    let job = Job::builder("etl")
        .op(Op::new("slow", |_| async { Ok(json!({"rows": 3})) }))
        .op(opener("opens_the_put", &gates, |g| {
            (&g.put_blocked, &g.put_open)
        }))
        .op(opener("opens_the_get", &gates, |g| {
            (&g.get_blocked, &g.get_open)
        }))
        .op(Op::new("after", |ctx: OpCtx| async move {
            Ok(ctx.input("slow").cloned().unwrap())
        })
        .after(["slow"]))
        .build()
        .unwrap();

    let runner = Runner::with_io(
        vec![job],
        Store::open(":memory:").unwrap(),
        Vec::new(),
        Vec::new(),
        Arc::new(Blocks(gates.clone())),
        Vec::new(),
    )
    .unwrap();
    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();

    let rows = runner.store().op_runs(&run.id).unwrap();
    let why: Vec<String> = rows.iter().filter_map(|o| o.error.clone()).collect();
    assert_eq!(run.status, RunStatus::Success, "{why:?}");
    // both calls really did block, so neither passed by being quick
    assert!(gates.put_blocked.load(Ordering::SeqCst));
    assert!(gates.get_blocked.load(Ordering::SeqCst));
    // and the value still arrived downstream through the same manager
    let after = rows.iter().find(|o| o.op == "after").unwrap();
    assert_eq!(after.output, Some(json!({"rows": 3})));
}

// the same thing `ParquetIo` is held to in tests/parquet.rs: retention takes
// what the run wrote with the rows that name it, and takes nothing belonging
// to a run it kept
#[tokio::test]
async fn retention_takes_what_the_run_wrote_and_leaves_the_runs_it_keeps() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("hestan.db");
    let db = db.to_str().unwrap();
    let io_dir = dir.path().join("io");
    let boot = || {
        Hestan::new()
            .io(FileIo::new(&io_dir))
            .job(chain_job("etl"))
            .db(db)
    };

    let first = boot().run_once("etl", json!({})).await.unwrap();
    let second = boot().run_once("etl", json!({})).await.unwrap();
    assert!(io_dir.join(&first.id).join("extract.json").exists());

    // days(0) takes every terminal run already in the past and keep_last(1)
    // holds the newest of them back, so the startup sweep takes exactly the
    // first run — before this third one launches
    let third = boot()
        .retention(Retention::days(0).keep_last(1))
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert_eq!(third.status, RunStatus::Success);

    let store = Store::open(db).unwrap();
    assert!(store.run(&first.id).unwrap().is_none(), "the run survived");
    assert!(
        !io_dir.join(&first.id).exists(),
        "what the run wrote outlived it"
    );
    assert!(
        store.run(&second.id).unwrap().is_some(),
        "the wrong run went"
    );
    assert!(io_dir.join(&second.id).join("extract.json").exists());

    // and a run whose files somebody else already took is pruned like any
    // other: a sweep has to be able to come round again
    std::fs::remove_dir_all(io_dir.join(&second.id)).unwrap();
    boot()
        .retention(Retention::days(0))
        .run_once("etl", json!({}))
        .await
        .unwrap();
    assert!(
        store.run(&second.id).unwrap().is_none(),
        "the row stayed behind a directory that was already gone"
    );
}
