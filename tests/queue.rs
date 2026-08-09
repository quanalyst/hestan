//! the run queue, across real processes.
//!
//! `harness = false` for the same reason [`isolation`](isolation.rs) needs it:
//! every process in this test is this same binary, re-executed, and its `main`
//! has to rebuild the same jobs against the same database. libtest's `main`
//! cannot, and the constraint is exactly the one a real deployment is under —
//! a scheduler and its workers are one image with one registry, started with
//! different roles.
//!
//! what is being tested is that **the process that decides a run and the
//! process that executes it need not be the same process**. the parent here
//! only ever enqueues; the children only ever execute.

use std::future::Future;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use hestan::prelude::*;
use hestan::{Role, RunStatus, Runner, Store, Trigger};

/// where every process in this test finds the run log. a deployment reads this
/// out of its own config; a test has to hand it to its children somehow, and
/// the environment is what children inherit.
const DB: &str = "HESTAN_QUEUE_DB";
/// what a child is to be. absent means "run the cases".
const ROLE: &str = "HESTAN_QUEUE_ROLE";
/// where every op appends a line saying which run it was and which process ran
/// it — the double-run detector.
const MARKS: &str = "HESTAN_QUEUE_MARKS";

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Ok(role) = std::env::var(ROLE) {
        let db = std::env::var(DB).expect("a child inherits the test database path");
        let result = match role.as_str() {
            // a queue worker with no socket: claims runs, executes them, and
            // fires nothing
            "worker" => rt.block_on(app(&db).slots(2).work(None)),
            // the other half: decides, enqueues, executes nothing
            "scheduler" => rt.block_on(app(&db).role(Role::Scheduler).serve(([127, 0, 0, 1], 0))),
            other => panic!("unknown role {other}"),
        };
        // only ever reached by a bind failure or a store that would not open
        panic!("a {role} returned: {result:?}");
    }

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("queue.db").display().to_string();
    let marks = dir.path().join("marks.txt").display().to_string();
    // SAFETY: single-threaded, before the runtime starts and long before any
    // child exists to inherit a half-written environment
    unsafe {
        std::env::set_var(DB, &db);
        std::env::set_var(MARKS, &marks);
    }
    rt.block_on(cases(&db));
    println!("queue: every case passed");
}

/// the registry every process in this test builds — the parent that enqueues
/// and each child that executes. one image, one registry, different roles.
fn app(db: &str) -> Hestan {
    Hestan::new()
        .jobs(jobs())
        // a sensor that would fire constantly, so "a worker fires nothing" is a
        // claim with something to disprove it
        .sensor(Sensor::new(
            "tireless",
            Duration::from_millis(100),
            |_| async { Ok(vec![RunRequest::new("chunk")]) },
        ))
        // and a schedule, so the same holds for the tick log
        .schedule("chunk", "* * * * *")
        .db(db)
}

fn jobs() -> Vec<Job> {
    vec![
        Job::builder("chunk")
            .op(Op::new("mark", |ctx: OpCtx| async move {
                let pid = std::process::id();
                // append rather than write: two processes running the same run
                // twice must both leave a line, or the detector detects nothing
                let line = format!("{} {pid}\n", ctx.run_id());
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(marks())?;
                std::io::Write::write_all(&mut file, line.as_bytes())?;
                // long enough that a worker holding two runs is holding them at
                // the same time, which is what makes the split observable
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok(json!({ "pid": pid }))
            }))
            // one job's runs may all execute at once; the per-process slots are
            // what spread them
            .max_concurrent_runs(8)
            .build()
            .unwrap(),
    ]
}

fn marks() -> PathBuf {
    PathBuf::from(std::env::var(MARKS).unwrap())
}

/// a runner that enqueues and never executes — the scheduler half of a split
/// deployment, in this process, so a case can put work on the queue and be sure
/// nothing here took it.
fn enqueuer(db: &str) -> Runner {
    Runner::new(jobs(), Store::open(db).unwrap()).with_role(Role::Scheduler, 1)
}

async fn cases(db: &str) {
    case(
        "a_worker_runs_a_queued_run_another_process_wrote",
        another_process_executes(db),
    )
    .await;
    case(
        "two_workers_split_a_queue_without_double_running_anything",
        two_workers_split_it(db),
    )
    .await;
    case(
        "a_worker_does_not_fire_schedules_or_sensors",
        a_worker_decides_nothing(db),
    )
    .await;
}

// ------------------------------------------------------------------ the cases

async fn another_process_executes(db: &str) {
    let store = Store::open(db).unwrap();
    let id = enqueuer(db)
        .launch("chunk", json!({}), Trigger::Manual)
        .unwrap();

    // nothing in this process will ever start it: the row is the whole of what
    // was handed over
    tokio::time::sleep(Duration::from_millis(300)).await;
    let queued = store.run(&id).unwrap().unwrap();
    assert_eq!(queued.status, RunStatus::Queued);
    assert_eq!(queued.claimed_by, None);

    let mut worker = spawn("worker");
    let run = wait_terminal(&store, &id).await;
    assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    let claimer = run.claimed_by.expect("a run that executed was claimed");
    assert!(!claimer.is_empty());

    // and it really did run elsewhere
    let op = store.op_run(&id, "mark").unwrap().unwrap();
    let pid = op.output.unwrap()["pid"].as_u64().unwrap();
    assert_ne!(
        pid,
        u64::from(std::process::id()),
        "the run executed in the process that enqueued it"
    );
    stop(&mut worker);
}

async fn two_workers_split_it(db: &str) {
    let store = Store::open(db).unwrap();
    let runner = enqueuer(db);
    let ids: Vec<String> = (0..8)
        .map(|_| runner.launch("chunk", json!({}), Trigger::Manual).unwrap())
        .collect();

    let mut workers = [spawn("worker"), spawn("worker")];
    for id in &ids {
        let run = wait_terminal(&store, id).await;
        assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    }
    for worker in &mut workers {
        stop(worker);
    }

    // the hard invariant: one line per run, ever. a run claimed twice would
    // have left two.
    let lines: Vec<(String, String)> = std::fs::read_to_string(marks())
        .unwrap()
        .lines()
        .filter_map(|l| l.split_once(' '))
        .map(|(run, pid)| (run.to_string(), pid.to_string()))
        .collect();
    for id in &ids {
        let ran = lines.iter().filter(|(run, _)| run == id).count();
        assert_eq!(ran, 1, "run {id} executed {ran} times");
    }

    // and both workers did some of it, which is what "split" means
    let mut pids: Vec<&str> = ids
        .iter()
        .filter_map(|id| lines.iter().find(|(run, _)| run == id))
        .map(|(_, pid)| pid.as_str())
        .collect();
    pids.sort_unstable();
    pids.dedup();
    assert_eq!(
        pids.len(),
        2,
        "one worker took the whole queue: {pids:?} — per-process slots did not bind"
    );
}

async fn a_worker_decides_nothing(db: &str) {
    let store = Store::open(db).unwrap();
    let before = store.runs(None, None, None, None, None, 500).unwrap().len();
    let ticks_before = store.ticks(None, 500).unwrap().len();

    // a worker, alone, for long enough that a sensor due every 100ms would have
    // fired twenty times
    let mut worker = spawn("worker");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = store.runs(None, None, None, None, None, 500).unwrap().len();
    assert_eq!(
        after, before,
        "a worker launched something: it is supposed to decide nothing"
    );
    assert!(
        store.sensor_ticks(None, 10).unwrap().is_empty(),
        "a worker evaluated a sensor"
    );
    assert_eq!(
        store.ticks(None, 500).unwrap().len(),
        ticks_before,
        "a worker touched the schedule tick log"
    );
    stop(&mut worker);

    // the fixture is real: the same registry under the scheduler role fires the
    // same sensor within a beat
    let mut scheduler = spawn("scheduler");
    let fired = wait_for("a sensor fire", || {
        store
            .runs(None, None, None, None, None, 500)
            .unwrap()
            .into_iter()
            .find(|r| r.trigger == Trigger::Sensor)
    })
    .await;
    // and it only decided: nothing in a scheduler executes, so the run it made
    // is still sitting on the queue
    assert_eq!(fired.status, RunStatus::Queued);
    assert_eq!(fired.claimed_by, None);
    stop(&mut scheduler);
}

// ---------------------------------------------------------------- the harness

fn spawn(role: &str) -> Child {
    let exe = std::env::current_exe().expect("this binary");
    Command::new(exe)
        .env(ROLE, role)
        .spawn()
        .unwrap_or_else(|e| panic!("could not start a {role}: {e}"))
}

/// stop a child and reap it, so the next case does not race a process that is
/// still claiming.
fn stop(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

async fn case(name: &str, body: impl Future<Output = ()>) {
    let started = Instant::now();
    body.await;
    println!("test {name} ... ok ({:?})", started.elapsed());
}

async fn wait_terminal(store: &Store, id: &str) -> hestan::Run {
    wait_for(&format!("run {id} to finish"), || {
        store
            .run(id)
            .unwrap()
            .filter(|r| !matches!(r.status, RunStatus::Queued | RunStatus::Running))
    })
    .await
}

async fn wait_for<T>(what: &str, mut ready: impl FnMut() -> Option<T>) -> T {
    for _ in 0..2_000 {
        if let Some(value) = ready() {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("waited 20s for {what}");
}
