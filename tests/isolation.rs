//! isolated ops, against real child processes.
//!
//! `harness = false` on purpose. an isolated op re-executes `current_exe()`,
//! so the binary the parent spawns has to be one whose `main` rebuilds the
//! same jobs against the same database — libtest's `main` cannot, and a test
//! binary that ran its whole suite as a worker child would be no test at all.
//! the `main` below is the same `main` on both sides of the spawn, which is
//! exactly the constraint isolation puts on a real deployment.

use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hestan::prelude::*;
use hestan::{OpRun, Runner, Store, Trigger};

/// where every process in this test — the parent and each worker child it
/// spawns — finds the run log. a deployment's `main` reads this out of its own
/// config; a test has to hand it to its children somehow, and the environment
/// is what children inherit.
const DB: &str = "HESTAN_ISOLATION_DB";

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    // a worker child lands here: same builder, same jobs, same database, and
    // `serve` hands it to the worker path before it binds or recovers anything
    if std::env::var_os("HESTAN_WORKER_RUN").is_some() {
        let db = std::env::var(DB).expect("a worker child inherits the test database path");
        let _ = rt.block_on(app(&db).serve(([127, 0, 0, 1], 0)));
        unreachable!("the worker guard exits the process before serve gets an address");
    }

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("isolation.db").display().to_string();
    // SAFETY: single-threaded, before the runtime starts and long before any
    // child exists to inherit a half-written environment
    unsafe { std::env::set_var(DB, &db) };
    rt.block_on(cases(&db));
    println!("isolation: every case passed");
}

/// the registry both sides of the spawn build. a child rebuilds it by running
/// this same `main`, which is the whole of the same-binary constraint.
fn app(db: &str) -> Hestan {
    Hestan::new().jobs(jobs()).pool("solo", 1).db(db)
}

fn jobs() -> Vec<Job> {
    vec![
        // an isolated op, its output, and an ordinary op reading it
        Job::builder("feed")
            .op(Op::new("produce", |_| async {
                Ok(json!({ "pid": std::process::id(), "rows": [1, 2, 3] }))
            })
            .isolated())
            .op(Op::new("consume", |ctx: OpCtx| async move {
                let rows = ctx.input("produce").unwrap()["rows"]
                    .as_array()
                    .unwrap()
                    .len();
                Ok(json!({ "seen": rows, "here": std::process::id() }))
            })
            .after(["produce"]))
            .build()
            .unwrap(),
        // the containment case: a body that takes its whole process down
        Job::builder("boom")
            .op(Op::new("explode", |_| async {
                std::process::abort();
            })
            .isolated())
            .build()
            .unwrap(),
        // something to kill from outside
        Job::builder("sleeper")
            .op(Op::new("nap", |_| async {
                tokio::time::sleep(Duration::from_secs(120)).await;
                Ok(json!(null))
            })
            .isolated())
            .build()
            .unwrap(),
        // fails its first attempt by dying, succeeds on the child that follows
        Job::builder("flaky")
            .op(Op::new("sometimes", |_| async {
                let marker = marker();
                if !marker.exists() {
                    std::fs::write(&marker, b"first attempt was here").unwrap();
                    std::process::abort();
                }
                Ok(json!({ "attempt": 2 }))
            })
            .isolated()
            .retries(1)
            .retry_delay(Duration::from_millis(10)))
            .build()
            .unwrap(),
        // one permit, one isolated op and one in-process op that both want it
        Job::builder("pooled")
            .op(
                Op::new("iso", |ctx: OpCtx| async move { timed(&ctx).await })
                    .isolated()
                    .pool("solo"),
            )
            .op(Op::new("local", |ctx: OpCtx| async move { timed(&ctx).await }).pool("solo"))
            .build()
            .unwrap(),
        // a child running beside one of its parent's own ops, which it must
        // leave entirely alone
        Job::builder("guarded")
            .op(Op::new("nap_local", |_| async {
                tokio::time::sleep(Duration::from_millis(1_200)).await;
                Ok(json!("still here"))
            }))
            .op(Op::new("quick", |_| async { Ok(json!("done")) }).isolated())
            .build()
            .unwrap(),
        // work that cannot be asked to stop: it never awaits, never polls the
        // cancellation flag, and holds its thread until it is killed
        Job::builder("stubborn")
            .op(Op::new("grind", |_| async { deaf_to_signals().await }).isolated())
            .build()
            .unwrap(),
        // the same work under a timeout, beside an ordinary op that must be
        // left to finish
        Job::builder("impatient")
            .op(Op::new("grind", |_| async { deaf_to_signals().await })
                .isolated()
                .timeout(Duration::from_millis(300)))
            .op(Op::new("beside_it", |_| async {
                tokio::time::sleep(Duration::from_millis(900)).await;
                Ok(json!("finished anyway"))
            }))
            .build()
            .unwrap(),
    ]
}

/// blocking work that ignores every request to stop: no await point to drop it
/// at, and nothing polling `ctx.is_cancelled()`. in-process this runs to the
/// end whatever the run log says; isolated, it is killed.
async fn deaf_to_signals() -> hestan::OpResult {
    tokio::task::spawn_blocking(|| {
        std::thread::sleep(Duration::from_secs(120));
        Ok(json!("nobody should ever see this"))
    })
    .await
    .unwrap()
}

/// the file the flaky op's first attempt leaves behind. beside the database,
/// so every process in the test agrees where it is.
fn marker() -> PathBuf {
    PathBuf::from(std::env::var(DB).unwrap() + ".flaky")
}

/// record the window this op actually occupied, which is what a pool permit is
/// supposed to keep two ops from sharing.
async fn timed(ctx: &OpCtx) -> hestan::OpResult {
    ctx.meta("began", now_ms());
    tokio::time::sleep(Duration::from_millis(300)).await;
    ctx.meta("ended", now_ms());
    Ok(json!(null))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

async fn cases(db: &str) {
    let runner = Runner::with_pools(
        jobs(),
        Store::open(db).unwrap(),
        Vec::new(),
        [("solo".to_string(), 1)],
    )
    .unwrap();

    case(
        "an_isolated_op_runs_elsewhere_and_its_output_reaches_downstream",
        runs_elsewhere(&runner),
    )
    .await;
    case(
        "a_child_that_aborts_fails_its_op_and_leaves_the_parent_running",
        an_abort_is_contained(&runner),
    )
    .await;
    case(
        "a_child_killed_from_outside_is_recorded_with_its_signal",
        a_kill_is_reported(&runner),
    )
    .await;
    case("a_retry_spawns_another_child", a_retry_respawns(&runner)).await;
    case(
        "an_isolated_op_holds_its_pool_permit_for_as_long_as_its_child_runs",
        the_permit_is_held(&runner),
    )
    .await;
    case(
        "a_worker_child_leaves_its_parents_run_alone",
        the_parents_run_is_untouched(&runner),
    )
    .await;
    case(
        "cancelling_a_run_kills_an_isolated_op_that_will_not_stop",
        a_cancel_kills(&runner),
    )
    .await;
    case(
        "a_timeout_kills_an_isolated_op_and_leaves_its_siblings_alone",
        a_timeout_kills(&runner),
    )
    .await;
}

// ------------------------------------------------------------------ the cases

async fn runs_elsewhere(runner: &Runner) {
    let run = runner
        .run("feed", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let rows = runner.store().op_runs(&run.id).unwrap();

    let produce = row(&rows, "produce");
    let elsewhere = produce.output.as_ref().unwrap()["pid"].as_u64().unwrap();
    assert_ne!(
        elsewhere,
        u64::from(std::process::id()),
        "the isolated op ran in the orchestrator's own process"
    );
    // the pid column says what is running where, so a finished op has none
    assert_eq!(produce.pid, None, "a finished op still claims a process");

    let consume = row(&rows, "consume").output.clone().unwrap();
    assert_eq!(
        consume["seen"],
        json!(3),
        "the output did not reach downstream"
    );
    assert_eq!(
        consume["here"],
        json!(std::process::id()),
        "an ordinary op did not run in the orchestrator"
    );
}

async fn an_abort_is_contained(runner: &Runner) {
    let run = runner
        .run("boom", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let err = row(&rows, "explode").error.clone().unwrap();
    assert!(
        err.contains("signal 6 (aborted)") && err.contains("without recording a result"),
        "the parent did not say how the child died: {err}"
    );
    // and the parent is still a working orchestrator, which is the whole point
    let after = runner
        .run("feed", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(after.status, RunStatus::Success, "{:?}", after.error);
}

async fn a_kill_is_reported(runner: &Runner) {
    let id = runner
        .launch("sleeper", json!({}), Trigger::Manual)
        .unwrap();
    let pid = wait_for_pid(runner, &id, "nap").await;
    // SAFETY: kill(2) on a pid this test read off a row that says it is running
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0, "kill failed");

    let run = wait_terminal(runner, &id).await;
    assert_eq!(run.status, RunStatus::Failed);
    let rows = runner.store().op_runs(&id).unwrap();
    let err = row(&rows, "nap").error.clone().unwrap();
    assert!(
        err.contains("signal 9 (killed)") && err.contains("without recording a result"),
        "an externally killed child was not reported as one: {err}"
    );
}

async fn a_retry_respawns(runner: &Runner) {
    let _ = std::fs::remove_file(marker());
    let run = runner
        .run("flaky", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let op = row(&rows, "sometimes");
    assert_eq!(op.attempts, 2, "the second attempt never happened");
    assert_eq!(op.output, Some(json!({ "attempt": 2 })));
    assert!(marker().exists());
}

async fn the_permit_is_held(runner: &Runner) {
    let run = runner
        .run("pooled", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let (iso, local) = (window(&rows, "iso"), window(&rows, "local"));
    assert!(
        iso.1 <= local.0 || local.1 <= iso.0,
        "the isolated op and the in-process op overlapped inside a pool of one: \
         iso {iso:?}, local {local:?}"
    );
}

async fn the_parents_run_is_untouched(runner: &Runner) {
    let run = runner
        .run("guarded", json!({}), Trigger::Manual)
        .await
        .unwrap();
    // a child that reached boot recovery would have failed this very run,
    // announced it, and skipped the op still napping beside it
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    assert_eq!(run.error, None);
    let rows = runner.store().op_runs(&run.id).unwrap();
    assert_eq!(row(&rows, "nap_local").output, Some(json!("still here")));
    let events = runner.store().events(&run.id, 0).unwrap();
    let announced: Vec<&String> = events
        .iter()
        .map(|e| &e.message)
        .filter(|m| m.contains("interrupted"))
        .collect();
    assert!(
        announced.is_empty(),
        "a worker child ran boot recovery on its own parent: {announced:?}"
    );
}

async fn a_cancel_kills(runner: &Runner) {
    let id = runner
        .launch("stubborn", json!({}), Trigger::Manual)
        .unwrap();
    let pid = wait_for_pid(runner, &id, "grind").await;
    assert!(alive(pid), "the child was not running to begin with");

    assert_eq!(
        runner.cancel(&id).unwrap(),
        hestan::CancelOutcome::Requested
    );
    let run = wait_terminal(runner, &id).await;
    assert_eq!(run.status, RunStatus::Canceled);

    // the point of the whole feature: work that ignores every request to stop
    // is gone anyway, and hestan knows it is
    assert!(
        !alive(pid),
        "process {pid} survived the cancellation of run {id}"
    );
    let row = runner.store().op_run(&id, "grind").unwrap().unwrap();
    assert_eq!(row.status, hestan::OpStatus::Canceled);
    assert!(
        row.finished_at.is_some(),
        "a killed op has a finish time, because this one was watched to stop"
    );
    let err = row.error.clone().unwrap();
    assert!(
        err.contains("canceled: it ignored SIGTERM") && err.contains("was killed"),
        "the row does not say how it stopped: {err}"
    );
    assert_eq!(row.pid, None);
}

async fn a_timeout_kills(runner: &Runner) {
    let id = runner
        .launch("impatient", json!({}), Trigger::Manual)
        .unwrap();
    let pid = wait_for_pid(runner, &id, "grind").await;
    let run = wait_terminal(runner, &id).await;
    assert_eq!(run.status, RunStatus::Failed);

    assert!(!alive(pid), "process {pid} outlived its op's timeout");
    let rows = runner.store().op_runs(&id).unwrap();
    let err = row(&rows, "grind").error.clone().unwrap();
    assert!(
        err.contains("timed out after 300ms") && err.contains("was killed"),
        "the timeout did not say what it did: {err}"
    );
    // one op's process dying is one op's problem: the run failed, and the op
    // running beside it still finished
    assert_eq!(
        row(&rows, "beside_it").output,
        Some(json!("finished anyway"))
    );
}

// ---------------------------------------------------------------- the harness

/// whether a process still exists. a reaped child is gone; an unreaped one is
/// a zombie and still answers, which is why the parent reaps what it kills.
fn alive(pid: libc::pid_t) -> bool {
    // SAFETY: signal 0 sends nothing and only asks whether the pid is there
    unsafe { libc::kill(pid, 0) == 0 }
}

async fn case(name: &str, body: impl Future<Output = ()>) {
    let started = Instant::now();
    body.await;
    println!("test {name} ... ok ({:?})", started.elapsed());
}

fn row<'a>(rows: &'a [OpRun], op: &str) -> &'a OpRun {
    rows.iter()
        .find(|r| r.op == op)
        .unwrap_or_else(|| panic!("no op run row for {op}"))
}

/// the window an op reported for itself with `ctx.meta`.
fn window(rows: &[OpRun], op: &str) -> (i64, i64) {
    let meta = row(rows, op).metadata.clone().unwrap();
    (
        meta["began"]["int"].as_i64().unwrap(),
        meta["ended"]["int"].as_i64().unwrap(),
    )
}

/// the pid an isolated op is running in, once its row carries one.
async fn wait_for_pid(runner: &Runner, run_id: &str, op: &str) -> libc::pid_t {
    for _ in 0..600 {
        if let Some(pid) = runner
            .store()
            .op_run(run_id, op)
            .unwrap()
            .and_then(|r| r.pid)
        {
            return pid as libc::pid_t;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("op {op} of run {run_id} never recorded a process");
}

async fn wait_terminal(runner: &Runner, id: &str) -> hestan::Run {
    for _ in 0..1_000 {
        let run = runner.store().run(id).unwrap().unwrap();
        if !matches!(run.status, RunStatus::Queued | RunStatus::Running) {
            return run;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {id} never finished");
}
