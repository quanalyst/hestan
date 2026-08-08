use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hestan::prelude::*;
use hestan::{
    CancelOutcome, Error, EventKind, FailureHook, OpStatus, Run, RunFailure, RunStatus, Runner,
    Store, Trigger,
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    assert!(events.iter().any(|e| e.kind == EventKind::OpSuccess
        && e.data == Some(json!({"output_type": "pipeline::Total"}))));
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
            .runs(None, None, None, None, 10)
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([builder.build().unwrap()], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());

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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
async fn cancel_stops_running_and_pending_ops() {
    let job = Job::builder("slow")
        .op(Op::new("long", |_| async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok(json!(null))
        }))
        .op(Op::new("tail", |_| async { Ok(json!(null)) }).after(["long"]))
        .build()
        .unwrap();
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::with_failure_hooks([job], Store::open(":memory:").unwrap(), vec![hook]);
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
    );

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
        Runner::with_failure_hooks([quick, slow], Store::open(":memory:").unwrap(), vec![hook]);

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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());

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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job, slow], Store::open(":memory:").unwrap());

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
    let runner = Runner::new([job], store.clone());
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
        .resume(&first.id)
        .unwrap_err();
    assert!(err.to_string().contains("only in the run: b, c"), "{err}");

    // no half-built run survives a refusal
    assert_eq!(store.runs(None, None, None, None, 10).unwrap().len(), 1);
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job()], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    );

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
    let listed = runner.store().runs(None, None, None, None, 10).unwrap();
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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
    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

#[test]
fn fan_out_does_not_nest() {
    let err = Job::builder("nested")
        .op(Op::new("pages", |_| async { Ok(json!([])) }))
        .op(Op::mapped(
            "outer",
            |_ctx: OpCtx, _n: u32| async move { Ok(json!(null)) },
        )
        .over("pages"))
        .op(Op::mapped(
            "inner",
            |_ctx: OpCtx, _n: u32| async move { Ok(json!(null)) },
        )
        .over("outer"))
        .build()
        .err()
        .unwrap();
    assert!(err.to_string().contains("fan-out does not nest"), "{err}");
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
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

    let runner = Runner::new([job], Store::open(":memory:").unwrap());
    let run = runner
        .run("retried", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success);
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(op_row(&rows, "process[0]").attempts, 1);
    assert_eq!(op_row(&rows, "process[1]").attempts, 2);
}
