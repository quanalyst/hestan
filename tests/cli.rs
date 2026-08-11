//! the command line, as a process.
//!
//! `harness = false` for the same reason [`queue`](queue.rs) needs it: every
//! case here is this same binary started again, and its `main` has to be able
//! to *be* the command line. that is not a detail of the test — an exit code is
//! not something a function call has, and the exit codes are the contract.
//!
//! so nothing below asserts that a command compiles. each case runs one, reads
//! what it printed on each stream, and reads what it exited with.

use std::future::Future;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use hestan::prelude::*;
use hestan::{EventQuery, Limits, Runner, Store, Trigger};

/// where the process under test finds its run log. absent means "run the
/// cases", which is how one binary is both halves of this.
const DB: &str = "HESTAN_CLI_DB";
/// what a no-argument invocation is to serve on, since that is the address a
/// host would have handed `cli::run`.
const ADDR: &str = "HESTAN_CLI_ADDR";

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    if let Ok(db) = std::env::var(DB) {
        let addr: SocketAddr = std::env::var(ADDR)
            .unwrap_or_else(|_| "127.0.0.1:0".into())
            .parse()
            .expect("ADDR is host:port");
        // the mount, exactly as a deployment writes it
        if let Err(e) = rt.block_on(hestan::cli::run(app(&db), addr)) {
            eprintln!("serving failed: {e}");
            std::process::exit(70);
        }
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    rt.block_on(cases(dir.path()));
    println!("cli: every case passed");
}

/// the registry the process under test is built from — a job that works, one
/// that does not, and one that takes longer than any case waits.
fn app(db: &str) -> Hestan {
    Hestan::new()
        .jobs(jobs())
        // one at a time, so a case can hold the only slot and watch another
        // run queue up behind it
        .max_concurrent_runs(1)
        .db(db)
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct Window {
    days: u32,
}

fn jobs() -> Vec<Job> {
    vec![
        Job::builder("quick")
            .description("finishes before anything can attach to it")
            .op(Op::new("greet", |ctx: OpCtx| async move {
                ctx.info("hello from quick");
                Ok(json!({ "ok": true }))
            }))
            .build()
            .unwrap(),
        Job::builder("boom")
            .op(Op::new("explode", |_| async {
                Err::<Value, _>("the warehouse said no".into())
            }))
            .build()
            .unwrap(),
        // a diamond, for the plan `explain` resolves: two ops that depend on
        // the first and not on each other
        Job::builder("diamond")
            .op(Op::new("fetch", |_| async { Ok(json!(null)) }))
            .op(Op::new("left", |_| async { Ok(json!(null)) }).after(["fetch"]))
            .op(Op::new("right", |_| async { Ok(json!(null)) }).after(["fetch"]))
            .op(Op::new("join", |_| async { Ok(json!(null)) }).after(["left", "right"]))
            .build()
            .unwrap(),
        // and a job with a params schema, for the dry run to reject against
        Job::builder("windowed")
            .op(Op::new("render", |_| async { Ok(json!(null)) }).params::<Window>())
            .build()
            .unwrap(),
        Job::builder("slow")
            .op(Op::new("linger", |_| async {
                tokio::time::sleep(Duration::from_secs(120)).await;
                Ok(json!(null))
            }))
            .build()
            .unwrap(),
    ]
}

async fn cases(dir: &Path) {
    case("a_run_that_succeeds_exits_zero", succeeds(dir)).await;
    case("a_run_that_fails_exits_one", fails(dir)).await;
    case("a_usage_mistake_exits_two", usage(dir)).await;
    case("a_launch_without_wait_stays_on_the_queue", launched(dir)).await;
    case("a_canceled_run_exits_three", canceled(dir)).await;
    case("a_wait_that_gives_up_exits_four", timed_out(dir)).await;
    case("a_store_that_will_not_open_exits_five", unreachable(dir)).await;
    case("json_output_parses_and_says_what_it_promised", machine(dir)).await;
    case("quiet_prints_the_id_and_nothing_else", quiet(dir)).await;
    case("nothing_is_styled_into_a_pipe", unstyled(dir)).await;
    case("no_arguments_serves", serves(dir)).await;
    case("each_mode_reaches_what_it_should", modes(dir)).await;
    case("a_follow_resumes_from_a_cursor", resumes(dir)).await;
    case("doctor_answers_why_nothing_is_running", diagnosed(dir)).await;
    case("a_dry_run_checks_the_params_and_creates_nothing", dry(dir)).await;
    case(
        "completion_comes_from_the_registry_in_this_binary",
        completing(dir),
    )
    .await;
}

// ------------------------------------------------------------------ the cases

/// the fast-run case, and it is the one worth having.
///
/// `quick` is over in milliseconds — very likely before the wait loop's first
/// poll — so a stream that attaches to a running run and reads forward from
/// there would print nothing at all and this would still exit 0. what is
/// asserted is that the line the op said is on stderr anyway.
async fn succeeds(dir: &Path) {
    let db = db(dir, "succeeds");
    let ran = cli(&db, &["run", "quick", "--wait"]);
    ran.assert(0);
    assert!(
        ran.stderr.contains("hello from quick"),
        "the run's own line never reached stderr: {:?}",
        ran.stderr
    );
    assert!(
        ran.stdout.contains("success"),
        "stdout said nothing about how it went: {:?}",
        ran.stdout
    );
}

async fn fails(dir: &Path) {
    let db = db(dir, "fails");
    let ran = cli(&db, &["run", "boom", "--wait"]);
    ran.assert(1);
    assert!(
        ran.stderr.contains("the warehouse said no"),
        "the failure never said why: {:?}",
        ran.stderr
    );
    // and the same job without --wait is a launch, which succeeded
    cli(&db, &["run", "boom"]).assert(0);
}

/// a launch is a launch: the run goes on the queue for whatever is serving this
/// database, and this process does not start executing something it is about to
/// exit out from under.
async fn launched(dir: &Path) {
    let db = db(dir, "launched");
    let ran = cli(&db, &["--quiet", "run", "quick"]);
    ran.assert(0);
    let id = ran.stdout.trim();
    let store = Store::open(db.to_str().unwrap()).unwrap();
    // after long enough that a process which had started it would have died
    // holding it
    std::thread::sleep(Duration::from_millis(300));
    let run = store.run(id).unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Queued, "{run:?}");
    assert_eq!(run.claimed_by, None);
}

async fn usage(dir: &Path) {
    let db = db(dir, "usage");
    // a job this binary does not define
    let unknown = cli(&db, &["run", "nope", "--wait"]);
    unknown.assert(2);
    assert!(unknown.stderr.contains("unknown job"), "{:?}", unknown);
    // a flag nothing defines, refused by the parser with the same code
    cli(&db, &["run", "quick", "--sideways"]).assert(2);
    // and a run id that is not one
    cli(&db, &["show", "nosuchrun"]).assert(2);
}

/// exit 3 needs a run that is canceled while something is waiting on it, and
/// only the process executing a run can stop it — so the run under test is one
/// that never got that far: the case holds the deployment's only slot, the
/// child's run queues behind it, and the case takes it off the queue.
async fn canceled(dir: &Path) {
    let db = db(dir, "canceled");
    let runner = Runner::new(jobs(), Store::open(db.to_str().unwrap()).unwrap())
        .with_limits(Limits::new().global(1), 0);
    let holding = runner.launch("slow", json!({}), Trigger::Manual).unwrap();
    wait_for("the case to hold the only slot", || {
        runner
            .store()
            .run(&holding)
            .unwrap()
            .filter(|r| r.status == RunStatus::Running)
    })
    .await;

    let waiting = spawn(&db, &["run", "quick", "--wait", "--timeout", "30"]);
    let queued = wait_for("the child's run to queue", || {
        runner
            .store()
            .runs(Some("quick"), None, None, None, None, 5)
            .unwrap()
            .pop()
    })
    .await;
    assert_eq!(queued.status, RunStatus::Queued);
    runner.cancel(&queued.id).unwrap();

    let ran = Ran::of(waiting.wait_with_output().unwrap());
    ran.assert(3);
    assert!(ran.stderr.contains("canceled"), "{ran:?}");
    runner.cancel(&holding).unwrap();
}

async fn timed_out(dir: &Path) {
    let db = db(dir, "timeout");
    let started = Instant::now();
    let ran = cli(&db, &["run", "slow", "--wait", "--timeout", "1"]);
    ran.assert(4);
    assert!(started.elapsed() < Duration::from_secs(20), "it waited out");
    assert!(ran.stderr.contains("stops with this process"), "{ran:?}");
    // the run is still going, which is what the message said
    let store = Store::open(db.to_str().unwrap()).unwrap();
    let run = store.runs(Some("slow"), None, None, None, None, 1).unwrap();
    assert_eq!(run[0].status, RunStatus::Running);
}

/// the database being out of reach is its own answer, because it is the one a
/// cron line should retry rather than page someone about.
async fn unreachable(dir: &Path) {
    let db = dir.join("no").join("such").join("directory.db");
    let ran = cli(&db, &["runs"]);
    ran.assert(5);
}

async fn machine(dir: &Path) {
    let db = db(dir, "json");
    let launched = cli(&db, &["--json", "run", "quick"]);
    launched.assert(0);
    let value: Value = serde_json::from_str(&launched.stdout)
        .unwrap_or_else(|e| panic!("stdout is not one json object: {e}: {:?}", launched.stdout));
    let id = value["run_id"].as_str().expect("run_id").to_string();
    assert_eq!(value["job"], "quick");
    assert_eq!(value["status"], "queued");

    let waited = cli(&db, &["--json", "run", "quick", "--wait"]);
    waited.assert(0);
    let run: Value = serde_json::from_str(&waited.stdout).expect("one json object");
    assert_eq!(run["status"], "success");
    assert_eq!(run["job"], "quick");
    assert!(run["id"].is_string());

    let listed = cli(&db, &["--json", "runs"]);
    listed.assert(0);
    let value: Value = serde_json::from_str(&listed.stdout).expect("one json object");
    let ids: Vec<&str> = value["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&id.as_str()), "the launch is not in the list");

    let shown = cli(&db, &["--json", "show", &id]);
    shown.assert(0);
    let value: Value = serde_json::from_str(&shown.stdout).expect("one json object");
    assert_eq!(value["run"]["id"], id.as_str());
    assert!(value["ops"].is_array());
}

async fn quiet(dir: &Path) {
    let db = db(dir, "quiet");
    let ran = cli(&db, &["--quiet", "run", "quick", "--wait"]);
    ran.assert(0);
    let id = ran.stdout.trim();
    assert_eq!(
        ran.stdout,
        format!("{id}\n"),
        "quiet printed more than the id"
    );
    assert!(
        ran.stderr.is_empty(),
        "quiet streamed the log anyway: {:?}",
        ran.stderr
    );
    let store = Store::open(db.to_str().unwrap()).unwrap();
    assert!(store.run(id).unwrap().is_some(), "{id} is not a run id");
}

/// every stream here is a pipe, which is exactly the case where an escape code
/// is not decoration but corruption.
async fn unstyled(dir: &Path) {
    let db = db(dir, "unstyled");
    let ran = cli(&db, &["run", "quick", "--wait"]);
    ran.assert(0);
    assert!(!ran.stdout.contains('\x1b'), "escapes on stdout: {ran:?}");
    assert!(!ran.stderr.contains('\x1b'), "escapes on stderr: {ran:?}");
    let ran = cli(&db, &["runs"]);
    assert!(!ran.stdout.contains('\x1b'), "escapes in a table: {ran:?}");
}

/// the compatibility promise: with no arguments the mount is `serve`, on the
/// address the host handed it, and the api it serves is the one it always was.
async fn serves(dir: &Path) {
    let db = db(dir, "serve");
    let addr = free_port();
    let mut child = Command::new(exe())
        .env(DB, &db)
        .env(ADDR, addr.to_string())
        .spawn()
        .expect("the binary starts with no arguments");
    let health = wait_for("the ui to come up", || get(addr, "/api/health")).await;
    assert!(health.contains("\"ok\":true"), "{health}");
    let _ = child.kill();
    let _ = child.wait();
}

/// the three ways to reach a deployment, against one database: this binary,
/// the standalone one over the same file, and the standalone one over a server
/// that is serving it.
async fn modes(dir: &Path) {
    let db = db(dir, "modes");
    let addr = free_port();
    let mut server = Command::new(exe())
        .env(DB, &db)
        .env(ADDR, addr.to_string())
        .spawn()
        .expect("the deployment serves");
    wait_for("the ui to come up", || get(addr, "/api/health")).await;

    // embedded: everything, because everything is here
    cli(&db, &["run", "quick", "--wait"]).assert(0);

    // a run log: the reads are there and the registry is not, and what it
    // cannot do it says in a sentence rather than an error code
    let listed = operator(&["--db", db.to_str().unwrap(), "--json", "runs"]);
    listed.assert(0);
    let value: Value = serde_json::from_str(&listed.stdout).expect("one json object");
    assert!(!value["runs"].as_array().unwrap().is_empty());

    for command in [vec!["run", "quick"], vec!["jobs"], vec!["serve"]] {
        let mut args = vec!["--db", db.to_str().unwrap()];
        args.extend(command.iter().copied());
        let refused = operator(&args);
        refused.assert(6);
        assert!(
            refused.stderr.contains("no job definitions"),
            "{command:?} was refused without saying why: {refused:?}"
        );
    }

    // a server: the same commands over the api it was already serving
    let url = format!("http://{addr}");
    let over = |args: &[&str]| {
        let mut all = vec!["--server", &url];
        all.extend(args.iter().copied());
        operator(&all)
    };
    over(&["runs"]).assert(0);
    let jobs = over(&["--quiet", "jobs"]);
    jobs.assert(0);
    assert!(jobs.stdout.contains("quick"), "{jobs:?}");
    let waited = over(&["--json", "run", "quick", "--wait"]);
    waited.assert(0);
    let run: Value = serde_json::from_str(&waited.stdout).expect("one json object");
    assert_eq!(run["status"], "success", "{waited:?}");
    // and a run that fails over the network still fails here
    over(&["run", "boom", "--wait"]).assert(1);
    // an unreachable server is its own answer, not a failure of the work
    operator(&["--server", "http://127.0.0.1:1", "runs"]).assert(5);

    let _ = server.kill();
    let _ = server.wait();
}

/// a follower given a cursor starts above it, which is what makes a dropped
/// connection something you can pick back up rather than a hole.
async fn resumes(dir: &Path) {
    let db = db(dir, "resume");
    cli(&db, &["run", "quick", "--wait"]).assert(0);
    let store = Store::open(db.to_str().unwrap()).unwrap();
    let all = store.event_log(&EventQuery::default(), 500).unwrap();
    // the log comes back newest first; the cursor is halfway down it
    let cursor = all[all.len() / 2].seq;
    let above = all.iter().filter(|e| e.seq > cursor).count();
    assert!(above > 1, "not enough log to resume through");

    let mut following = spawn(
        &db,
        &[
            "--json",
            "events",
            "--follow",
            "--after",
            &cursor.to_string(),
        ],
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    let _ = following.kill();
    let ran = Ran::of(following.wait_with_output().unwrap());
    let seqs: Vec<i64> = ran
        .stdout
        .lines()
        .map(|line| {
            let event: Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("a followed line is not json: {e}: {line}"));
            event["seq"].as_i64().expect("every event has a seq")
        })
        .collect();
    assert_eq!(seqs.len(), above, "a resumed follow read the wrong range");
    assert!(seqs.iter().all(|&seq| seq > cursor), "{seqs:?}");
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "out of order: {seqs:?}"
    );
}

/// doctor, end to end: a healthy deployment says so and exits 0, and one with
/// a run nobody is going to take says which and exits 7. the conditions
/// themselves are each constructed and asserted in `src/cli.rs`; what this adds
/// is that the exit code follows.
async fn diagnosed(dir: &Path) {
    let db = db(dir, "doctor");
    cli(&db, &["run", "quick", "--wait"]).assert(0);
    let healthy = cli(&db, &["doctor"]);
    healthy.assert(0);
    assert!(healthy.stdout.contains("ok    store"), "{healthy:?}");
    assert!(
        healthy.stdout.contains("disk"),
        "no disk check: {healthy:?}"
    );

    // a launch with nothing running to execute it, which is the question
    // doctor exists to answer
    cli(&db, &["run", "quick"]).assert(0);
    let stuck = cli(&db, &["doctor"]);
    stuck.assert(7);
    assert!(
        stuck.stdout.contains("nothing holding them back"),
        "{stuck:?}"
    );

    let machine = cli(&db, &["--json", "doctor"]);
    machine.assert(7);
    let value: Value = serde_json::from_str(&machine.stdout).expect("one json object");
    assert_eq!(value["ok"], false);
    let wrong: Vec<&Value> = value["findings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|f| f["level"] == "wrong")
        .collect();
    assert_eq!(wrong.len(), 1, "{:?}", value["findings"]);
    assert!(wrong[0]["fix"].is_string(), "a finding with no fix");
}

/// the params a dry run checks are the ones a launch would check, and nothing
/// it does reaches the run log.
async fn dry(dir: &Path) {
    let db = db(dir, "dry");
    let before = cli(&db, &["--quiet", "runs", "--limit", "500"])
        .stdout
        .lines()
        .count();

    let bad = cli(
        &db,
        &[
            "run",
            "windowed",
            "--params",
            "{\"days\":\"lots\"}",
            "--dry-run",
        ],
    );
    bad.assert(2);
    assert!(bad.stderr.contains("invalid params"), "{bad:?}");

    let good = cli(&db, &["--json", "run", "diamond", "--dry-run"]);
    good.assert(0);
    let plan: Value = serde_json::from_str(&good.stdout).expect("one json object");
    let stages = plan["stages"].as_array().expect("stages");
    assert_eq!(stages.len(), 3, "{stages:?}");
    let mut middle: Vec<&str> = stages[1]["ops"]
        .as_array()
        .unwrap()
        .iter()
        .map(|op| op["name"].as_str().unwrap())
        .collect();
    middle.sort_unstable();
    assert_eq!(middle, ["left", "right"], "the parallel pair is one stage");

    let after = cli(&db, &["--quiet", "runs", "--limit", "500"])
        .stdout
        .lines()
        .count();
    assert_eq!(before, after, "a dry run reached the run log");
}

/// the claim the mount makes, tested: the names a shell completes come out of
/// the registry compiled into this binary, at the moment they are asked for.
async fn completing(dir: &Path) {
    let db = db(dir, "completions");
    let script = cli(&db, &["completions", "bash"]);
    script.assert(0);
    assert!(script.stdout.contains("__complete"), "{script:?}");
    assert!(script.stdout.contains("what=jobs"), "{script:?}");

    let names = cli(&db, &["__complete", "jobs"]);
    names.assert(0);
    let listed: Vec<&str> = names.stdout.lines().collect();
    assert!(listed.contains(&"quick"), "{listed:?}");
    assert!(listed.contains(&"diamond"), "{listed:?}");

    // and the subcommands, which a shell has to be able to ask for before it
    // has been told where any deployment is
    let commands = operator(&["__complete", "commands"]);
    commands.assert(0);
    assert!(commands.stdout.contains("doctor"), "{commands:?}");
    assert!(!commands.stdout.contains("__complete"), "{commands:?}");
}

// ---------------------------------------------------------------- the harness

#[derive(Debug)]
struct Ran {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn of(out: Output) -> Ran {
        Ran {
            // a follower this suite killed has no code of its own, and -1 is
            // not one any case asserts on
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    fn assert(&self, code: i32) {
        assert_eq!(self.code, code, "wrong exit code for {self:?}");
    }
}

fn exe() -> PathBuf {
    std::env::current_exe().expect("this binary")
}

fn db(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!("{name}.db"))
}

/// one command, run to completion. both streams are pipes, which is what makes
/// the styling cases mean anything.
fn cli(db: &Path, args: &[&str]) -> Ran {
    Ran::of(
        Command::new(exe())
            .env(DB, db)
            .args(args)
            .output()
            .expect("the command starts"),
    )
}

/// the standalone `hestan`, which has no registry of its own: the binary an
/// operator installs, run against whatever this case points it at.
fn operator(args: &[&str]) -> Ran {
    Ran::of(
        Command::new(env!("CARGO_BIN_EXE_hestan"))
            .args(args)
            .output()
            .expect("the standalone binary starts"),
    )
}

/// the same, left running, for the cases that have to do something to it while
/// it waits.
fn spawn(db: &Path, args: &[&str]) -> std::process::Child {
    Command::new(exe())
        .env(DB, db)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("the command starts")
}

/// a port nothing is on, released a moment before the child binds it. the
/// alternative is asking the child what it bound, which is a channel this test
/// would have to invent.
fn free_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

/// one http get, spelled by hand: this test binary has no client and needs no
/// feature to have one.
fn get(addr: SocketAddr, path: &str) -> Option<String> {
    let mut socket = TcpStream::connect(addr).ok()?;
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: hestan\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .ok()?;
    let mut body = String::new();
    socket.read_to_string(&mut body).ok()?;
    body.starts_with("HTTP/1.1 200").then_some(body)
}

async fn case(name: &str, body: impl Future<Output = ()>) {
    let started = Instant::now();
    body.await;
    println!("test {name} ... ok ({:?})", started.elapsed());
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
