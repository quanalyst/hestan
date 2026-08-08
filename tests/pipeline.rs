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
