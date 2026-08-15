//! the run queue, across real processes.
//!
//! `harness = false` for the same reason [`isolation`](isolation.rs) needs it:
//! every process in this test is this same binary, re-executed, and its `main`
//! has to rebuild the same jobs against the same database. libtest's `main`
//! cannot, and the constraint is exactly the one a real deployment is under:
//! a scheduler and its workers are one image with one registry, started with
//! different roles.
//!
//! what is being tested is that **the process that decides a run and the
//! process that executes it need not be the same process**. the parent here
//! only ever enqueues; the children only ever execute.
//!
//! every case runs twice where there is a postgres to run it against: once on
//! a sqlite file, once on a postgres schema of its own, the same processes
//! racing the same queue either way. sqlite serializes writers for us and
//! postgres does not, so "nothing was run twice" is a different claim on each
//! and has to be made on each.

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
/// it: the double-run detector.
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
    run_on(&rt, &db, dir.path().join("sqlite-marks.txt"));
    println!("queue: every case passed on sqlite");

    #[cfg(feature = "postgres")]
    if let Some(pg) = Scratch::new() {
        run_on(&rt, &pg.url, dir.path().join("postgres-marks.txt"));
        println!("queue: every case passed on postgres");
    }
}

/// the whole of the suite against one database, with a mark file of its own so
/// that the double-run detector is only ever reading this run of it.
fn run_on(rt: &tokio::runtime::Runtime, db: &str, marks: PathBuf) {
    // SAFETY: single-threaded, between runs, and long before any child exists
    // to inherit a half-written environment
    unsafe {
        std::env::set_var(DB, db);
        std::env::set_var(MARKS, marks.display().to_string());
    }
    rt.block_on(cases(db));
}

/// a schema of its own on the server `HESTAN_TEST_PG` names, dropped with the
/// fixture. unset means no postgres here and the postgres half is skipped,
/// which is what lets this test pass on a machine without one.
#[cfg(feature = "postgres")]
struct Scratch {
    server: String,
    url: String,
    schema: String,
}

#[cfg(feature = "postgres")]
impl Scratch {
    fn new() -> Option<Scratch> {
        let server = std::env::var("HESTAN_TEST_PG").ok()?;
        let schema = format!("hestan_queue_{}", std::process::id());
        admin(&server, &format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
        admin(&server, &format!("CREATE SCHEMA {schema}"));
        // `options` is how a url carries a session setting, which is what puts
        // this process and every worker it starts in the same schema
        let sep = match server.contains('?') {
            true => '&',
            false => '?',
        };
        let url = format!("{server}{sep}options=-c%20search_path%3D{schema}");
        Some(Scratch {
            server,
            url,
            schema,
        })
    }
}

#[cfg(feature = "postgres")]
impl Drop for Scratch {
    fn drop(&mut self) {
        admin(
            &self.server,
            &format!("DROP SCHEMA {} CASCADE", self.schema),
        );
    }
}

/// one statement against the server itself, outside anything hestan opened.
/// the fixture has to make the schema before a store can be pointed at it and
/// take it away afterwards, and a `Store` is not the tool for either.
#[cfg(feature = "postgres")]
fn admin(server: &str, sql: &str) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let (client, connection) = tokio_postgres::connect(server, tokio_postgres::NoTls)
            .await
            .expect("HESTAN_TEST_PG names a server this test can reach");
        tokio::spawn(async {
            let _ = connection.await;
        });
        client.batch_execute(sql).await.unwrap();
    });
}

/// a handle on whichever backend `target` names: a path or a url. what
/// [`Hestan::db`] does for a whole app, for the cases that want a store
/// beside one.
fn open(target: &str) -> Store {
    #[cfg(feature = "postgres")]
    if target.starts_with("postgres://") || target.starts_with("postgresql://") {
        return Store::connect(target).unwrap();
    }
    Store::open(target).unwrap()
}

/// the registry every process in this test builds: the parent that enqueues
/// and each child that executes. one image, one registry, different roles.
fn app(db: &str) -> Hestan {
    Hestan::new()
        .jobs(jobs())
        // one call every two seconds, declared once in a registry every process
        // in this test builds, which is exactly the deployment shape that
        // makes it per process
        .rate("api", 1, Duration::from_secs(2))
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
        Job::builder("throttled")
            .op(Op::new("call", |_ctx: OpCtx| async move {
                // when this call went, and from which process. the api on
                // the other side sees exactly this list
                let pid = std::process::id();
                let at = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                let mut file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(rate_marks())?;
                std::io::Write::write_all(&mut file, format!("{pid} {at}\n").as_bytes())?;
                Ok(json!({ "pid": pid }))
            })
            .rate("api"))
            .max_concurrent_runs(8)
            .build()
            .unwrap(),
    ]
}

fn marks() -> PathBuf {
    PathBuf::from(std::env::var(MARKS).unwrap())
}

/// where every throttled call records itself, beside the other mark file so
/// each backend's run of the suite reads only its own.
fn rate_marks() -> PathBuf {
    marks().with_extension("rate")
}

/// a runner that enqueues and never executes: the scheduler half of a split
/// deployment, in this process, so a case can put work on the queue and be sure
/// nothing here took it.
fn enqueuer(db: &str) -> Runner {
    Runner::new(jobs(), open(db))
        .unwrap()
        .with_role(Role::Scheduler, 1)
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
    case(
        "a_declared_rate_is_per_process_and_two_workers_are_two_of_them",
        a_rate_is_per_process(db),
    )
    .await;
}

// ------------------------------------------------------------------ the cases

async fn another_process_executes(db: &str) {
    let store = open(db);
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
    let store = open(db);
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
        "one worker took the whole queue: {pids:?}; per-process slots did not bind"
    );
}

async fn a_worker_decides_nothing(db: &str) {
    let store = open(db);
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

/// the honest limit, asserted rather than described.
///
/// a rate is a bucket in a process's memory. two workers each honouring "one
/// call every two seconds" make two calls in two seconds, and the api on the
/// other side has no idea it was talking to two of anything, so the thing to
/// divide is the limit, and `docs/scaling.md` says so where somebody sizing a
/// deployment will read it.
async fn a_rate_is_per_process(db: &str) {
    let store = open(db);
    let runner = enqueuer(db);
    let ids: Vec<String> = (0..4)
        .map(|_| {
            runner
                .launch("throttled", json!({}), Trigger::Manual)
                .unwrap()
        })
        .collect();

    let mut workers = [spawn("worker"), spawn("worker")];
    for id in &ids {
        let run = wait_terminal(&store, id).await;
        assert_eq!(run.status, RunStatus::Success, "{:?}", run.error);
    }
    for worker in &mut workers {
        stop(worker);
    }

    // every call that was let through: which process made it, and when
    let calls: Vec<(String, u64)> = std::fs::read_to_string(rate_marks())
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(' '))
        .map(|(pid, at)| (pid.to_string(), at.parse().unwrap()))
        .collect();
    assert_eq!(calls.len(), 4, "not every op recorded a call: {calls:?}");

    // each process kept the promise it was given: its own calls are a period
    // apart, whatever the other one was doing
    let mut pids: Vec<&str> = calls.iter().map(|(pid, _)| pid.as_str()).collect();
    pids.sort_unstable();
    pids.dedup();
    assert_eq!(pids.len(), 2, "one worker took the whole queue: {pids:?}");
    for pid in &pids {
        let mut theirs: Vec<u64> = calls
            .iter()
            .filter(|(who, _)| who == pid)
            .map(|(_, at)| *at)
            .collect();
        theirs.sort_unstable();
        for pair in theirs.windows(2) {
            assert!(
                pair[1] - pair[0] >= 1_900,
                "process {pid} made two calls {}ms apart, inside its own period",
                pair[1] - pair[0]
            );
        }
    }

    // and the api saw the sum: two calls inside one period, because there were
    // two buckets. this is the limit of what one process can promise
    let mut at: Vec<u64> = calls.iter().map(|(_, at)| *at).collect();
    at.sort_unstable();
    assert!(
        at[1] - at[0] < 2_000,
        "two workers spaced their calls as if they shared a bucket: {at:?}"
    );
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
