//! stopping a process, across real processes.
//!
//! `harness = false` for the same reason [`queue`](queue.rs) needs it: what is
//! being tested is a **process**, signalled from outside and watched for how it
//! exits, and libtest's `main` cannot be a deployment. every child here is this
//! same binary re-executed with a role in its environment, which is the shape a
//! real deployment is in: one image, one registry, several processes.
//!
//! what these are about is the difference between a process that is killed and
//! one that is asked. a killed process leaves its lease held and its claims
//! outstanding until they expire; an asked one leaves neither.
//!
//! sqlite only, deliberately. nothing here is about a race between writers, so
//! the second backend would buy a second copy of the same wall clock. the sql
//! a release is made of runs on both, and `src/store.rs` has that case.

use std::future::Future;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus};
use std::time::{Duration, Instant};

use hestan::prelude::*;
use hestan::{OpStatus, Role, RunStatus, Runner, Store, Trigger};

/// where every process in this test finds the run log.
const DB: &str = "HESTAN_STOPPING_DB";
/// what a child is to be. absent means "run the cases".
const ROLE: &str = "HESTAN_STOPPING_ROLE";
/// where an op says it started and, if it got that far, that it finished.
const MARKS: &str = "HESTAN_STOPPING_MARKS";
/// how long the one-shot child's op sleeps for, since a one-shot is handed a
/// job name rather than params.
const ONE_SHOT_MS: &str = "HESTAN_STOPPING_ONE_SHOT_MS";
/// what a child passes to `Hestan::stop_within`, so a case about the deadline
/// does not have to sit through the default one.
const WITHIN_MS: &str = "HESTAN_STOPPING_WITHIN_MS";
/// where a served child puts its ui. a real port rather than 0, because the
/// parent has to be able to ask it something: see [`answering`].
const PORT: &str = "HESTAN_STOPPING_PORT";

/// the deadline `src/stop.rs` gives a stopping process when nothing says
/// otherwise. not imported, because it is not public: this is the number the
/// cases are written against, and a change to one that is not a change to the
/// other is what these assertions are for.
const WITHIN: Duration = Duration::from_secs(8);

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Ok(role) = std::env::var(ROLE) {
        let db = std::env::var(DB).expect("a child inherits the test database path");
        match role.as_str() {
            // one process doing everything: it decides, and it claims and
            // executes what it decided. the ordinary deployment
            "served" => rt
                .block_on(app(&db).serve(([127, 0, 0, 1], port())))
                .expect("a served process returns only by stopping"),
            // a queue worker with no socket: it claims and executes, and
            // decides nothing
            "worker" => rt
                .block_on(app(&db).work(None))
                .expect("a worker returns only by stopping"),
            // and the one that must not have changed: a headless one-shot,
            // which runs the thing it was asked for and exits
            "one_shot" => {
                let ms: u64 = std::env::var(ONE_SHOT_MS)
                    .expect("a one-shot child is told how long to work for")
                    .parse()
                    .unwrap();
                let run = rt
                    .block_on(app(&db).run_once("sleep", json!({ "ms": ms })))
                    .expect("the one-shot ran");
                println!("one_shot {} {:?}", run.id, run.status);
            }
            other => panic!("unknown role {other}"),
        }
        // a child that gets here stopped of its own accord, which is what the
        // parent reads off its exit status.
        //
        // **returned from rather than exited.** dropping the runtime is what
        // returning from a `#[tokio::main]` does, and it is the backstop that
        // kills an op subprocess the abort did not get to. a child calling
        // `process::exit` here would be testing a shape no hestan application
        // has
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stopping.db").display().to_string();
    // SAFETY: single-threaded, before the runtime starts and long before any
    // child exists to inherit a half-written environment
    unsafe {
        std::env::set_var(DB, &db);
        std::env::set_var(MARKS, dir.path().join("marks.txt").display().to_string());
    }
    rt.block_on(cases(&db));
    println!("stopping: every case passed");
}

/// the registry every process in this test builds.
fn app(db: &str) -> Hestan {
    let app = Hestan::new().jobs(jobs()).db(db);
    match std::env::var(WITHIN_MS) {
        Ok(ms) => app.stop_within(Duration::from_millis(ms.parse().unwrap())),
        Err(_) => app,
    }
}

fn jobs() -> Vec<Job> {
    vec![
        Job::builder("sleep")
            .op(Op::new("work", |ctx: OpCtx| async move {
                let ms = ctx.params()["ms"].as_u64().unwrap_or(0);
                mark(&format!("started {} {}", ctx.run_id(), std::process::id()));
                // not `ctx.cancelled()`: this op is deliberately one that does
                // not watch for anything. an op that stopped when asked would
                // prove nothing about a deadline it never reached
                tokio::time::sleep(Duration::from_millis(ms)).await;
                mark(&format!("finished {} {}", ctx.run_id(), std::process::id()));
                Ok(json!({ "ms": ms }))
            }))
            .max_concurrent_runs(8)
            .build()
            .unwrap(),
        // the same work in a child process, for the one question a stop has to
        // answer about a process hestan did not start and must not leave
        // running
        Job::builder("sleep_isolated")
            .op(Op::new("work", |ctx: OpCtx| async move {
                let ms = ctx.params()["ms"].as_u64().unwrap_or(0);
                mark(&format!(
                    "subprocess {} {}",
                    ctx.run_id(),
                    std::process::id()
                ));
                tokio::time::sleep(Duration::from_millis(ms)).await;
                Ok(json!({ "ms": ms }))
            })
            .isolated())
            .max_concurrent_runs(8)
            .build()
            .unwrap(),
    ]
}

/// a runner that enqueues and never executes, so a case can put work on the
/// queue and be sure nothing in this process took it.
fn enqueuer(db: &str) -> Runner {
    Runner::new(jobs(), Store::open(db).unwrap())
        .unwrap()
        .with_role(Role::Scheduler, 1)
}

async fn cases(db: &str) {
    case(
        db,
        "a_served_process_exits_when_it_is_asked_to",
        it_exits_on_a_signal(db),
    )
    .await;
    case(
        db,
        "a_one_shot_runs_what_it_was_asked_for_and_still_dies_on_a_signal",
        a_one_shot_is_unchanged(db),
    )
    .await;
    case(
        db,
        "a_second_signal_stops_a_process_that_is_still_finishing_work",
        a_second_signal_short_circuits_the_wait(db),
    )
    .await;
    case(
        db,
        "a_stop_hands_the_lease_back_and_the_next_process_takes_the_term",
        the_lease_is_handed_back(db),
    )
    .await;
    case(
        db,
        "a_run_that_can_finish_inside_the_deadline_finishes",
        work_in_flight_is_finished(db),
    )
    .await;
    case(
        db,
        "a_run_that_cannot_finish_inside_the_deadline_is_released_not_failed",
        a_run_it_cannot_finish_is_released(db),
    )
    .await;
    case(
        db,
        "another_worker_takes_a_released_run_without_waiting_for_the_lease",
        another_worker_takes_it(db),
    )
    .await;
    case(
        db,
        "a_stopping_process_waits_the_deadline_it_was_given",
        the_deadline_is_honoured(db),
    )
    .await;
    case(
        db,
        "an_isolated_ops_child_process_does_not_outlive_the_stop",
        the_child_goes_too(db),
    )
    .await;
}

// ------------------------------------------------------------------ the cases

/// the finding this phase came from, as an assertion: a served process sent
/// SIGTERM exits on the signal, rather than sitting there until something
/// kills it.
async fn it_exits_on_a_signal(db: &str) {
    let store = Store::open(db).unwrap();
    let port = free_port();
    let mut child = served(port, None).await;
    assert!(
        store.decider().unwrap().holder.is_some(),
        "a served process that is answering is one that has taken the lease"
    );

    let asked = Instant::now();
    term(&child);
    let status = exited(&mut child, Duration::from_secs(20)).await;
    let took = asked.elapsed();
    println!("     it stopped in {took:?}");

    assert!(
        status.success(),
        "a stopped process exited with {status:?} rather than cleanly"
    );
    assert_eq!(
        status.signal(),
        None,
        "the process was killed by a signal rather than exiting on one"
    );
    // it had nothing in flight, so the drain had nothing to wait for and the
    // deadline should be nowhere near this number
    assert!(
        took < Duration::from_secs(5),
        "a process with nothing to finish took {took:?} to stop"
    );
}

/// the regression this phase could most easily have introduced.
///
/// `run_once` installs no handler, so a one-shot does the work it was asked
/// for and is then killed by SIGTERM exactly as any program with no handler
/// is. both halves are asserted, because a handler installed process-wide
/// would break them in opposite directions: it would either make the one-shot
/// exit before its work was done, or make it swallow the signal that was
/// supposed to end it.
async fn a_one_shot_is_unchanged(db: &str) {
    let store = Store::open(db).unwrap();

    // left alone: it runs the job and exits, and the run is a success
    let mut quick = spawn_one_shot(200);
    let quick_pid = quick.id();
    let status = exited(&mut quick, Duration::from_secs(30)).await;
    assert!(status.success(), "a one-shot exited with {status:?}");
    assert!(
        finished_in(quick_pid),
        "the one-shot exited without finishing its op"
    );

    // and signalled halfway through: the default action, which is what a
    // process with no handler has and what a one-shot still has
    let before = marks().len();
    let mut slow = spawn_one_shot(20_000);
    let slow_pid = slow.id();
    wait_for("the one-shot's op to start", || {
        (marks().len() > before).then_some(())
    })
    .await;
    term(&slow);
    let status = exited(&mut slow, Duration::from_secs(20)).await;
    assert_eq!(
        status.signal(),
        Some(libc::SIGTERM),
        "a one-shot did not die on SIGTERM: {status:?}. something installed a \
         handler on a path that must not have one"
    );
    assert!(
        !finished_in(slow_pid),
        "the one-shot's op finished after the process was killed"
    );
    // nothing it was doing reached a terminal status, since nothing recorded
    // one: the point is that the process was gone, not what it left behind
    let running = store
        .runs(None, None, None, None, None, 200)
        .unwrap()
        .into_iter()
        .filter(|r| r.status == RunStatus::Running)
        .count();
    assert!(running >= 1, "the killed one-shot left no run behind");
}

/// ctrl-c twice means stop now, and the second one is not swallowed.
async fn a_second_signal_short_circuits_the_wait(db: &str) {
    let store = Store::open(db).unwrap();
    // far longer than the deadline, so what ends this process is a decision
    // rather than the op finishing
    let id = enqueuer(db)
        .launch("sleep", json!({ "ms": 60_000 }), Trigger::Manual)
        .unwrap();
    let mut child = served(free_port(), None).await;
    started(&store, &id).await;

    let asked = Instant::now();
    term(&child);
    // it is finishing a sixty-second op, so it is still here
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "the process exited without waiting for the work it was doing"
    );

    let again = Instant::now();
    term(&child);
    let status = exited(&mut child, Duration::from_secs(20)).await;
    let after_the_second = again.elapsed();
    let total = asked.elapsed();

    assert!(
        status.success(),
        "a process stopped twice exited with {status:?}"
    );
    assert!(
        after_the_second < Duration::from_secs(3),
        "the second signal took {after_the_second:?} to be acted on"
    );
    assert!(
        total < WITHIN,
        "the process waited {total:?}, which is the whole deadline: the second \
         signal was swallowed"
    );
}

/// the part of a stop the rest of the deployment feels: the deciding lease
/// goes back, so the next process takes the term now rather than after the
/// expiry a kill leaves behind.
async fn the_lease_is_handed_back(db: &str) {
    let store = Store::open(db).unwrap();
    let mut leader = served(free_port(), None).await;
    let held = store.decider().unwrap();
    let who = held.holder.clone().expect("somebody is deciding");
    let term_before = held.term;

    // a second process against the same database, which decides nothing at
    // all while the first one is up
    let mut spare = served(free_port(), None).await;
    assert_eq!(
        store.decider().unwrap().holder.as_deref(),
        Some(who.as_str()),
        "the spare took the lease while the leader was still up"
    );

    let asked = Instant::now();
    term(&leader);
    let took = wait_for("the lease to move", || {
        store
            .decider()
            .unwrap()
            .holder
            .filter(|h| *h != who)
            .map(|_| asked.elapsed())
    })
    .await;
    println!("     the lease moved after {took:?}");

    // the lease is ten seconds and a kill costs whatever is left of it. this
    // is the other case: the row is free the moment the leader goes, and what
    // is left is how long the spare takes to look, which is one renewal
    assert!(
        took < Duration::from_secs(4),
        "the handover took {took:?}, which is an expiry rather than a handback"
    );
    assert!(
        store.decider().unwrap().term > term_before,
        "the handover did not move the term"
    );
    let status = exited(&mut leader, Duration::from_secs(20)).await;
    assert!(status.success(), "the leader exited with {status:?}");
    kill_child(&mut spare);
}

/// in-flight work is finished rather than dropped: a stop is not a cancel.
async fn work_in_flight_is_finished(db: &str) {
    let store = Store::open(db).unwrap();
    // well inside the deadline this child is given
    let id = enqueuer(db)
        .launch("sleep", json!({ "ms": 1_500 }), Trigger::Manual)
        .unwrap();
    let mut child = served(free_port(), Some(Duration::from_secs(6))).await;
    started(&store, &id).await;

    let asked = Instant::now();
    term(&child);
    let status = exited(&mut child, Duration::from_secs(30)).await;
    let took = asked.elapsed();
    println!("     it finished the run and stopped in {took:?}");

    assert!(status.success(), "the process exited with {status:?}");
    let run = store.run(&id).unwrap().unwrap();
    assert_eq!(
        run.status,
        RunStatus::Success,
        "a run that had time to finish did not: {:?}",
        run.error
    );
    assert!(
        took > Duration::from_millis(800),
        "the process was gone in {took:?}, so it did not wait for the run"
    );
    assert!(
        took < Duration::from_secs(6),
        "the process waited {took:?}, past the deadline it was given"
    );
}

/// and the other half: what a stopping process is still holding when its
/// deadline runs out goes back on the queue, and does not go there as a
/// failure.
async fn a_run_it_cannot_finish_is_released(db: &str) {
    let store = Store::open(db).unwrap();
    let id = enqueuer(db)
        .launch("sleep", json!({ "ms": 60_000 }), Trigger::Manual)
        .unwrap();
    let mut child = served(free_port(), Some(Duration::from_millis(400))).await;
    started(&store, &id).await;

    let asked = Instant::now();
    term(&child);
    let status = exited(&mut child, Duration::from_secs(30)).await;
    let took = asked.elapsed();
    println!("     it gave the run up after {took:?}");
    assert!(status.success(), "the process exited with {status:?}");

    let run = store.run(&id).unwrap().unwrap();
    assert_eq!(
        run.status,
        RunStatus::Queued,
        "the run did not go back on the queue: {:?}",
        run.error
    );
    assert_eq!(run.claimed_by, None, "a released run is still claimed");
    assert_eq!(run.lease_until, None, "a released run still has a lease");
    assert_eq!(run.started_at, None, "a released run still says it started");
    let op = store.op_run(&id, "work").unwrap().unwrap();
    assert_eq!(
        op.status,
        OpStatus::Pending,
        "the op did not go back to pending: {:?}",
        op.error
    );

    let kinds: Vec<String> = store
        .events(&id, 0)
        .unwrap()
        .into_iter()
        .map(|e| e.kind.as_str().to_string())
        .collect();
    assert!(
        kinds.iter().any(|k| k == "run_released"),
        "nothing in the log says the run was released: {kinds:?}"
    );
    assert!(
        !kinds.iter().any(|k| k == "run_failed"),
        "a released run was recorded as a failure: {kinds:?}"
    );
}

/// and it is there for the taking, without the run lease having to run out
/// first: sixty seconds is what a kill costs and this is not that.
async fn another_worker_takes_it(db: &str) {
    let store = Store::open(db).unwrap();
    let id = enqueuer(db)
        .launch("sleep", json!({ "ms": 60_000 }), Trigger::Manual)
        .unwrap();
    let mut first = served(free_port(), Some(Duration::from_millis(400))).await;
    started(&store, &id).await;
    let held_by = store.run(&id).unwrap().unwrap().claimed_by;
    term(&first);
    let _ = exited(&mut first, Duration::from_secs(30)).await;

    let handed = Instant::now();
    let mut next = spawn("worker", free_port(), None);
    let took = wait_for("another worker to take the run", || {
        store
            .run(&id)
            .unwrap()
            .filter(|r| r.claimed_by.is_some())
            .map(|r| (handed.elapsed(), r.claimed_by))
    })
    .await;
    println!("     another worker had it {:?} later", took.0);
    assert_ne!(
        took.1, held_by,
        "the same claimer took it back, so nothing was handed over"
    );
    // the run lease is sixty seconds. a released run is not waiting for it
    assert!(
        took.0 < Duration::from_secs(20),
        "it took {:?} for another worker to pick the run up",
        took.0
    );
    kill_child(&mut next);
}

/// the deadline is a number a deployment sets, and a stopping process waits
/// it: not longer, and not a fixed one of hestan's own.
async fn the_deadline_is_honoured(db: &str) {
    let store = Store::open(db).unwrap();
    for ms in [400u64, 2_500] {
        let id = enqueuer(db)
            .launch("sleep", json!({ "ms": 60_000 }), Trigger::Manual)
            .unwrap();
        let mut child = served(free_port(), Some(Duration::from_millis(ms))).await;
        started(&store, &id).await;

        let asked = Instant::now();
        term(&child);
        let status = exited(&mut child, Duration::from_secs(30)).await;
        let took = asked.elapsed();
        println!("     a {ms}ms deadline stopped in {took:?}");
        assert!(status.success(), "the process exited with {status:?}");
        assert!(
            took >= Duration::from_millis(ms),
            "a {ms}ms deadline was over in {took:?}"
        );
        assert!(
            took < Duration::from_millis(ms) + Duration::from_secs(4),
            "a {ms}ms deadline took {took:?}"
        );
        // and it went back on the queue rather than being carried off
        assert_eq!(store.run(&id).unwrap().unwrap().status, RunStatus::Queued);
        // taken off it again, so the next turn of this loop starts empty
        let _ = enqueuer(db).cancel(&id);
    }
}

/// what a stop does to an [`isolated`](hestan::Op::isolated) op's child, which
/// is the one process in this hestan deployment that hestan did not start and
/// cannot leave behind.
///
/// hestan never signals it: the op is being given time to finish, not asked to
/// stop. what ends it is the parent dropping it on the way out, and dropping
/// an op subprocess is a SIGKILL. the run is going back on the queue, so a
/// child still working after its parent left would be a second copy of the op
/// running against whoever claims the run next.
async fn the_child_goes_too(db: &str) {
    let store = Store::open(db).unwrap();
    let id = enqueuer(db)
        .launch("sleep_isolated", json!({ "ms": 60_000 }), Trigger::Manual)
        .unwrap();
    let mut child = served(free_port(), Some(Duration::from_millis(400))).await;
    let pid = wait_for("the op subprocess to be spawned", || {
        store.op_run(&id, "work").unwrap().and_then(|op| op.pid)
    })
    .await as libc::pid_t;
    assert!(alive(pid), "the op subprocess was gone before the stop");

    term(&child);
    let status = exited(&mut child, Duration::from_secs(30)).await;
    assert!(status.success(), "the process exited with {status:?}");

    let mut gone = false;
    for _ in 0..500 {
        if !alive(pid) {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        gone,
        "the op subprocess (pid {pid}) outlived the process that spawned it"
    );
    assert_eq!(
        store.run(&id).unwrap().unwrap().status,
        RunStatus::Queued,
        "the run did not go back on the queue"
    );
}

// ---------------------------------------------------------------- the harness

/// whether a pid still names a process, asked the way `tests/isolation.rs`
/// asks it.
fn alive(pid: libc::pid_t) -> bool {
    // SAFETY: signal 0 checks that the process exists and sends it nothing
    unsafe { libc::kill(pid, 0) == 0 }
}

fn spawn(role: &str, port: u16, within: Option<Duration>) -> Child {
    let exe = std::env::current_exe().expect("this binary");
    let mut command = Command::new(exe);
    command.env(ROLE, role).env(PORT, port.to_string());
    if let Some(within) = within {
        command.env(WITHIN_MS, within.as_millis().to_string());
    }
    command
        .spawn()
        .unwrap_or_else(|e| panic!("could not start a {role}: {e}"))
}

/// start a served child and wait until it is answering.
async fn served(port: u16, within: Option<Duration>) -> Child {
    let child = spawn("served", port, within);
    wait_for("the served child to answer", || {
        answering(port).then_some(())
    })
    .await;
    child
}

fn spawn_one_shot(ms: u64) -> Child {
    let exe = std::env::current_exe().expect("this binary");
    Command::new(exe)
        .env(ROLE, "one_shot")
        .env(ONE_SHOT_MS, ms.to_string())
        .spawn()
        .expect("could not start a one-shot")
}

fn port() -> u16 {
    std::env::var(PORT)
        .expect("a served child is told which port to bind")
        .parse()
        .unwrap()
}

/// a port nothing is listening on, found by binding one and letting it go.
///
/// there is a window between the two in which something else could take it,
/// and it is a test on a loopback interface: if that happens the child fails
/// to bind and the wait below says so, rather than anything passing quietly.
fn free_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("a loopback port")
        .local_addr()
        .unwrap()
        .port()
}

/// whether a served child is **answering**, which is the thing worth waiting
/// for and is not the same as the socket being bound.
///
/// `serve` binds the listener before it opens the store, and a bound socket
/// completes a connection out of the kernel's backlog whether or not anything
/// is accepting yet. so this asks for a response: a child that produced one is
/// inside `axum::serve`, and therefore past the point where it started
/// listening for a signal. `/api/whoami` because it is the one route outside
/// the auth guard, so this stays true of a deployment that has a token.
fn answering(port: u16) -> bool {
    use std::io::{Read, Write};
    let Ok(mut sock) = std::net::TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let brief = Duration::from_millis(500);
    let _ = sock.set_read_timeout(Some(brief));
    let _ = sock.set_write_timeout(Some(brief));
    let request = b"GET /api/whoami HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if sock.write_all(request).is_err() {
        return false;
    }
    let mut head = [0u8; 12];
    sock.read_exact(&mut head).is_ok() && &head == b"HTTP/1.1 200"
}

/// SIGTERM, to one child, by pid.
fn term(child: &Child) {
    // SAFETY: kill(2) with the pid of a child this process spawned and has not
    // reaped, so it names this child or nothing at all
    let sent = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(sent, 0, "could not signal {}", child.id());
}

/// stop a child the blunt way and reap it, so the next case does not race a
/// process that is still claiming.
fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// wait for a child to exit, and fail rather than hang if it does not.
async fn exited(child: &mut Child, within: Duration) -> ExitStatus {
    let until = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= until {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the process did not exit within {within:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// wait until somebody has claimed `id` and started its op, which is the
/// earliest moment there is work in flight to be stopped in the middle of.
async fn started(store: &Store, id: &str) {
    wait_for(&format!("run {id} to start"), || {
        store
            .op_run(id, "work")
            .unwrap()
            .filter(|op| op.started_at.is_some())
    })
    .await;
}

fn marks() -> Vec<String> {
    let path = PathBuf::from(std::env::var(MARKS).unwrap());
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(str::to_string)
        .collect()
}

/// whether the process `pid` got as far as finishing an op.
fn finished_in(pid: u32) -> bool {
    let ended = format!(" {pid}");
    marks()
        .iter()
        .any(|line| line.starts_with("finished ") && line.ends_with(&ended))
}

fn mark(line: &str) {
    let path = PathBuf::from(std::env::var(MARKS).expect("every process is told where to mark"));
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("the mark file");
    std::io::Write::write_all(&mut file, format!("{line}\n").as_bytes()).expect("marking");
}

/// take everything off the queue before the next case puts something on it.
///
/// **a released run is a queued run**, which is the point of half of these,
/// and a queued run is one the next case's child would happily claim and then
/// spend its whole deadline on. so each case starts from an empty queue.
fn tidy(db: &str) {
    let store = Store::open(db).unwrap();
    let runner = enqueuer(db);
    for run in store.runs(None, None, None, None, None, 500).unwrap() {
        if run.status == RunStatus::Queued {
            let _ = runner.cancel(&run.id);
        }
    }
}

async fn case(db: &str, name: &str, body: impl Future<Output = ()>) {
    tidy(db);
    let started = Instant::now();
    body.await;
    println!("test {name} ... ok ({:?})", started.elapsed());
}

async fn wait_for<T>(what: &str, mut ready: impl FnMut() -> Option<T>) -> T {
    for _ in 0..4_500 {
        if let Some(value) = ready() {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("waited 45s for {what}");
}
