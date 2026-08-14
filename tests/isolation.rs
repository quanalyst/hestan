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
use hestan::{LogStream, OpRun, Runner, Store, Trigger};

/// where every process in this test — the parent and each op subprocess it
/// spawns — finds the run log. a deployment's `main` reads this out of its own
/// config; a test has to hand it to its children somehow, and the environment
/// is what children inherit.
const DB: &str = "HESTAN_ISOLATION_DB";

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    // an op subprocess lands here: same builder, same jobs, same database, and
    // `serve` hands it to the op-subprocess path before it binds, recovers or
    // claims anything
    if std::env::var_os("HESTAN_ISOLATED_RUN").is_some() {
        let db = std::env::var(DB).expect("an op subprocess inherits the test database path");
        let _ = rt.block_on(app(&db).serve(([127, 0, 0, 1], 0)));
        unreachable!("the op-subprocess guard exits before serve gets an address");
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
        // something to kill from outside. it says so first, because what a
        // killed process printed before it went is the whole of the evidence
        Job::builder("sleeper")
            .op(Op::new("nap", |_| async {
                println!("napping, and not expecting to wake up");
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
        // an op subprocess running beside one of its parent's own ops, which it
        // must leave entirely alone
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
        // an allocation nothing could satisfy, under a limit that makes it this
        // op's failure rather than the machine's problem
        Job::builder("greedy")
            .op(Op::new("hog", |_| async {
                let huge: Vec<u8> = vec![0; 2 * 1024 * 1024 * 1024];
                Ok(json!(huge.len()))
            })
            .isolated()
            .memory_limit(512 * 1024 * 1024))
            .build()
            .unwrap(),
        // both pipes, in order, from a body that only ever prints
        Job::builder("talker")
            .op(Op::new("say", |_| async {
                for i in 0..3 {
                    println!("out {i}");
                    eprintln!("err {i}");
                }
                // no newline on the end: a child that stops mid-line still
                // said what it managed to say
                print!("half a line");
                use std::io::Write;
                std::io::stdout().flush().unwrap();
                Ok(json!("said it"))
            })
            .isolated())
            .build()
            .unwrap(),
        // more output than the default byte cap allows, from a process that
        // then finishes perfectly well: capture stopping is not the op failing.
        //
        // it floods *both* pipes on purpose, and that is what makes this a
        // test of the concurrent drain rather than only of the cap: 600 KiB a
        // side is ten times a pipe buffer, so a parent that read one pipe to
        // its end before touching the other would leave this child blocked on
        // a full pipe forever, and the case would hang instead of failing
        Job::builder("chatty")
            .op(Op::new("flood", |_| async {
                for i in 0..100 {
                    println!("{i:04} {}", "a great deal of output. ".repeat(250));
                    eprintln!("{i:04} {}", "and rather a lot beside it. ".repeat(214));
                }
                Ok(json!("finished anyway"))
            })
            .isolated())
            .build()
            .unwrap(),
        // prints, then takes its process down without recording anything
        Job::builder("dying_words")
            .op(Op::new("shout", |_| async {
                println!("about to do the thing");
                eprintln!("the thing went badly");
                std::process::abort();
            })
            .isolated())
            .build()
            .unwrap(),
        // prints which attempt it is, and fails the first one
        Job::builder("twice")
            .op(Op::new("again", |_| async {
                let marker = marker_named("twice");
                if !marker.exists() {
                    std::fs::write(&marker, b"first attempt was here").unwrap();
                    println!("attempt one, and it is going to fail");
                    use std::io::Write;
                    std::io::stdout().flush().unwrap();
                    std::process::abort();
                }
                println!("attempt two, and it worked");
                Ok(json!("done"))
            })
            .isolated()
            .retries(1)
            .retry_delay(Duration::from_millis(10)))
            .build()
            .unwrap(),
        // and a loop that will never stop on its own
        Job::builder("spinner")
            .op(Op::new("spin", |_| async {
                tokio::task::spawn_blocking(|| -> hestan::OpResult {
                    let mut n: u64 = 0;
                    loop {
                        n = std::hint::black_box(n.wrapping_mul(6_364_136_223_846_793_005) + 1);
                    }
                })
                .await
                .unwrap()
            })
            .isolated()
            .cpu_limit(Duration::from_secs(1)))
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
/// blocking work that will not stop for a signal — and a note, first, that it
/// is in a position to ignore one.
///
/// the child installs its SIGTERM handler before it calls the body, so a body
/// that has written this is a child that can hear one. the parent records a pid
/// the moment it spawns, which is earlier, and a stop that lands in between is
/// the default disposition killing the child rather than the case's stubborn
/// op ignoring anything.
async fn deaf_to_signals() -> hestan::OpResult {
    std::fs::write(marker_named("deaf"), "it can hear")?;
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
    marker_named("flaky")
}

fn marker_named(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(DB).unwrap() + "." + name)
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
        "an_op_subprocess_leaves_its_parents_run_alone",
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
    case(
        "an_op_past_its_memory_limit_fails_naming_the_limit",
        the_memory_limit_bites(&runner),
    )
    .await;
    case(
        "an_op_past_its_cpu_limit_fails_naming_the_limit",
        the_cpu_limit_bites(&runner),
    )
    .await;
    case(
        "both_of_a_childs_pipes_are_captured_in_order",
        both_pipes_are_captured(&runner),
    )
    .await;
    case(
        "a_chatty_child_is_capped_and_finishes_anyway",
        the_cap_holds(&runner),
    )
    .await;
    case(
        "a_child_that_dies_keeps_what_it_printed",
        last_words_survive(&runner),
    )
    .await;
    case(
        "a_retry_captures_its_output_as_a_separate_attempt",
        attempts_are_separate(&runner),
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
    // the row carries a pid the moment the child is spawned, which is a good
    // while before that child has booted far enough to run the op body. wait
    // for the line it prints, or this kills a process that has printed nothing
    // and asserts nothing about what a killed child keeps
    wait_for_line(runner, &id, "napping").await;
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
    // and what it printed before it was killed is still there
    let lines = runner.store().op_logs(&id, None, 0, 100).unwrap();
    assert_eq!(
        said(&lines, LogStream::Stdout),
        ["napping, and not expecting to wake up"]
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
    // a subprocess that reached boot recovery would have failed this very run,
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
        "an op subprocess ran boot recovery on its own parent: {announced:?}"
    );
}

async fn a_cancel_kills(runner: &Runner) {
    let _ = std::fs::remove_file(marker_named("deaf"));
    let id = runner
        .launch("stubborn", json!({}), Trigger::Manual)
        .unwrap();
    let pid = wait_for_pid(runner, &id, "grind").await;
    assert!(alive(pid), "the child was not running to begin with");
    // not before the child can ignore it: see `deaf_to_signals`
    wait_for_marker(&marker_named("deaf")).await;

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

async fn the_memory_limit_bites(runner: &Runner) {
    let run = runner
        .run("greedy", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let err = row(&rows, "hog").error.clone().unwrap();
    // the limit, not the signal it arrived as
    assert!(
        err.contains("memory limit of 512 MiB"),
        "the failure does not name the limit it hit: {err}"
    );
}

async fn the_cpu_limit_bites(runner: &Runner) {
    let run = runner
        .run("spinner", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    let rows = runner.store().op_runs(&run.id).unwrap();
    let err = row(&rows, "spin").error.clone().unwrap();
    assert!(
        err.contains("cpu limit of 1s"),
        "the failure does not name the limit it hit: {err}"
    );
}

async fn both_pipes_are_captured(runner: &Runner) {
    let run = runner
        .run("talker", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);

    let lines = runner.store().op_logs(&run.id, None, 0, 1_000).unwrap();
    assert_eq!(
        said(&lines, LogStream::Stdout),
        ["out 0", "out 1", "out 2", "half a line"],
        "stdout is not in the order the child printed it"
    );
    assert_eq!(
        said(&lines, LogStream::Stderr),
        ["err 0", "err 1", "err 2"],
        "stderr is not in the order the child printed it"
    );
    // a captured line is the op's own output, not an event: no level, no
    // target, and the attempt it belongs to
    assert!(
        lines
            .iter()
            .all(|l| l.level.is_none() && l.target.is_none())
    );
    assert!(lines.iter().all(|l| l.attempt == 1 && l.op == "say"));

    // an op that printed nothing has nothing stored, rather than a marker
    // saying it was quiet
    let quiet = runner
        .run("feed", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert!(
        runner
            .store()
            .op_logs(&quiet.id, None, 0, 100)
            .unwrap()
            .is_empty(),
        "a silent child left rows behind"
    );
}

async fn the_cap_holds(runner: &Runner) {
    let run = runner
        .run("chatty", json!({}), Trigger::Manual)
        .await
        .unwrap();
    // the op is not the thing that failed here: it printed too much, which is
    // hestan's problem and not the run's
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    assert_eq!(
        runner
            .store()
            .op_run(&run.id, "flood")
            .unwrap()
            .unwrap()
            .output,
        Some(json!("finished anyway"))
    );

    let lines = runner.store().op_logs(&run.id, None, 0, 10_000).unwrap();
    assert!(
        lines.len() < 200,
        "the 1 MiB cap stored all 200 lines: {}",
        lines.len()
    );
    // both pipes were being read, or the child would still be blocked on one
    assert!(
        lines.iter().any(|l| l.stream == Some(LogStream::Stdout))
            && lines.iter().any(|l| l.stream == Some(LogStream::Stderr)),
        "only one of the two pipes was drained"
    );
    let stored: usize = lines.iter().map(|l| l.message.len()).sum();
    assert!(stored <= (1 << 20) + 8 * 1024, "stored {stored} bytes");
    // exactly one line explains it, and it is hestan speaking
    let markers: Vec<&str> = lines
        .iter()
        .filter(|l| l.target.as_deref() == Some("hestan"))
        .map(|l| l.message.as_str())
        .collect();
    assert_eq!(markers.len(), 1, "{markers:?}");
    assert!(markers[0].contains("cap of 1 MiB"), "{}", markers[0]);
    assert_eq!(lines.last().unwrap().message, markers[0]);
}

async fn last_words_survive(runner: &Runner) {
    let run = runner
        .run("dying_words", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Failed);
    // it recorded no result at all, so what it printed is the whole of what
    // anyone has to go on
    let lines = runner.store().op_logs(&run.id, None, 0, 100).unwrap();
    assert_eq!(said(&lines, LogStream::Stdout), ["about to do the thing"]);
    assert_eq!(said(&lines, LogStream::Stderr), ["the thing went badly"]);
}

async fn attempts_are_separate(runner: &Runner) {
    let _ = std::fs::remove_file(marker_named("twice"));
    let run = runner
        .run("twice", json!({}), Trigger::Manual)
        .await
        .unwrap();
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);

    let lines = runner
        .store()
        .op_logs(&run.id, Some("again"), 0, 100)
        .unwrap();
    let by_attempt: Vec<(u32, &str)> = lines
        .iter()
        .map(|l| (l.attempt, l.message.as_str()))
        .collect();
    assert_eq!(
        by_attempt,
        [
            (1, "attempt one, and it is going to fail"),
            (2, "attempt two, and it worked"),
        ],
        "a retry's output is not separable from the attempt it replaced"
    );
}

/// what a child said on one of its pipes, in the order it said it.
fn said(lines: &[hestan::OpLog], stream: LogStream) -> Vec<&str> {
    lines
        .iter()
        .filter(|l| l.stream == Some(stream))
        .map(|l| l.message.as_str())
        .collect()
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
async fn wait_for_marker(path: &std::path::Path) {
    for _ in 0..600 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{} never appeared", path.display());
}

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

/// wait until a run has captured a line containing `needle`.
async fn wait_for_line(runner: &Runner, run_id: &str, needle: &str) {
    for _ in 0..600 {
        let lines = runner.store().op_logs(run_id, None, 0, 100).unwrap();
        if lines.iter().any(|l| l.message.contains(needle)) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("run {run_id} never printed anything containing {needle}");
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
