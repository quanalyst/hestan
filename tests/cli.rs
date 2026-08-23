//! the command line, as a process.
//!
//! `harness = false` for the same reason [`queue`](queue.rs) needs it: every
//! case here is this same binary started again, and its `main` has to be able
//! to *be* the command line. that is not a detail of the test: an exit code is
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
use hestan::{Auth, EventQuery, Limits, Runner, Store, Trigger};

/// where the process under test finds its run log. absent means "run the
/// cases", which is how one binary is both halves of this.
const DB: &str = "HESTAN_CLI_DB";
/// what a no-argument invocation is to serve on, since that is the address a
/// host would have handed `cli::run`.
const ADDR: &str = "HESTAN_CLI_ADDR";
/// the token the deployment under test is configured with, where a case wants
/// an authenticated one. absent means an open deployment, which is what every
/// other case here serves.
const TOKEN: &str = "HESTAN_CLI_TOKEN";
const SECRET: &str = "tk-cli-4d1f7a-not-in-any-output";

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

/// the registry the process under test is built from: a job that works, one
/// that does not, and one that takes longer than any case waits.
fn app(db: &str) -> Hestan {
    let app = Hestan::new()
        .jobs(jobs())
        .assets(assets())
        // one at a time, so a case can hold the only slot and watch another
        // run queue up behind it
        .max_concurrent_runs(1)
        .db(db);
    match std::env::var(TOKEN) {
        Ok(token) => app.auth(Auth::bearer(token)),
        Err(_) => app,
    }
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct Window {
    days: u32,
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct Deploy {
    token: String,
    env: u32,
}

/// two warehouse tables, a vendor feed and something built out of them: the
/// smallest registry that has a group, an origin and an asset in neither.
fn assets() -> Vec<Asset> {
    let orders = Asset::source("orders").group("warehouse");
    let returns = Asset::source("returns").group("warehouse");
    let fx = Asset::source("fx_rates");
    let margin = Asset::new("margin", |_| async { Ok(json!({ "margin": 1 })) })
        .from(&orders)
        .from(&fx)
        .group("finance");
    // and one whose group is the prefix in its name, with nothing declared
    let netted = Asset::new("finance/netted", |_| async { Ok(json!(null)) }).from(&returns);
    vec![orders, returns, fx, margin, netted]
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
        // an isolated op re-executes this binary with no arguments, and no
        // arguments is how the command line spells "serve"
        #[cfg(unix)]
        Job::builder("elsewhere")
            .op(Op::new("apart", |ctx: OpCtx| async move {
                ctx.info("ran in its own process");
                Ok(json!({ "ok": true }))
            })
            .isolated())
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
        // a job whose token is a credential, for the dry run to redact and the
        // run log not to hold
        Job::builder("deploy")
            .op(Op::new("push", |ctx: OpCtx| async move {
                ctx.info("pushing");
                Ok(ctx.params().clone())
            })
            .params::<Deploy>()
            .secret_params(["token"]))
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
    case(
        "an_authenticated_server_is_reached_with_a_token_or_not_at_all",
        authenticated(dir),
    )
    .await;
    case("a_follow_resumes_from_a_cursor", resumes(dir)).await;
    case(
        "replay_re_runs_what_a_run_did_or_says_why_not",
        replays(dir),
    )
    .await;
    case("doctor_answers_why_nothing_is_running", diagnosed(dir)).await;
    case("a_dry_run_checks_the_params_and_creates_nothing", dry(dir)).await;
    case(
        "completion_comes_from_the_registry_in_this_binary",
        completing(dir),
    )
    .await;
    case("a_hue_is_the_same_number_in_another_process", coloured(dir)).await;
    #[cfg(unix)]
    case(
        "an_isolated_op_is_served_its_op_and_not_a_socket",
        isolated_child(dir),
    )
    .await;
}

// ------------------------------------------------------------------ the cases

/// the fast-run case, and it is the one worth having.
///
/// `quick` is over in milliseconds (very likely before the wait loop's first
/// poll) so a stream that attaches to a running run and reads forward from
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

/// the guard `cli::run` takes before it looks at argv, asserted.
///
/// the child of an isolated op is this binary with no arguments, which is the
/// mount's spelling of "serve". without the guard the child binds a socket,
/// writes no terminal row, and the parent records an op that exited having
/// done nothing, so this case fails by timing out rather than by a wrong
/// answer, which is why it asks for a short wait.
#[cfg(unix)]
async fn isolated_child(dir: &Path) {
    let db = db(dir, "isolated_child");
    let ran = cli(&db, &["run", "elsewhere", "--wait", "--timeout", "30"]);
    ran.assert(0);
    assert!(
        ran.stderr.contains("ran in its own process"),
        "the isolated op's own line never came back, so the child did something \
         other than run it: {:?}",
        ran.stderr
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
/// only the process executing a run can stop it, so the run under test is one
/// that never got that far: the case holds the deployment's only slot, the
/// child's run queues behind it, and the case takes it off the queue.
async fn canceled(dir: &Path) {
    let db = db(dir, "canceled");
    let runner = Runner::new(jobs(), Store::open(db.to_str().unwrap()).unwrap())
        .unwrap()
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

/// replay through the command line, and the codes its refusals exit with.
///
/// a resume and a replay are one letter apart and mean opposite things, so
/// what is asserted is that the run this one leaves behind says which happened.
async fn replays(dir: &Path) {
    let db = db(dir, "replay");
    let store = Store::open(db.to_str().unwrap()).unwrap();

    // a run that failed, which is the case somebody actually has
    cli(&db, &["run", "boom", "--wait"]).assert(1);
    let failed = store.runs(Some("boom"), None, None, None, None, 1).unwrap();
    let failed = failed[0].id.clone();

    let ran = cli(&db, &["--quiet", "replay", &failed]);
    ran.assert(0);
    let replayed = store.run(ran.stdout.trim()).unwrap().unwrap();
    assert_eq!(replayed.trigger, Trigger::Replay);
    assert_eq!(replayed.replay_of.as_deref(), Some(failed.as_str()));
    assert_eq!(replayed.resumed_from, None);
    // the op that failed, and nothing else
    let ops: Vec<String> = store
        .op_runs(&replayed.id)
        .unwrap()
        .into_iter()
        .map(|o| o.op)
        .collect();
    assert_eq!(ops, ["explode"]);

    // a run that worked has no failed op to replay, and saying so is exit 2
    cli(&db, &["run", "quick", "--wait"]).assert(0);
    let won = store
        .runs(Some("quick"), None, None, None, None, 1)
        .unwrap();
    let ran = cli(&db, &["replay", &won[0].id]);
    ran.assert(2);
    assert!(ran.stderr.contains("nothing to replay"), "{ran:?}");

    // and a run id that is not one is the same answer `show` gives
    cli(&db, &["replay", "nosuchrun"]).assert(2);
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

/// an authenticated deployment, from the outside: the token goes in a flag or
/// in the environment, a command without one is refused with a code of its own,
/// and `doctor` can say which kind of deployment this is before you have a
/// credential for it.
async fn authenticated(dir: &Path) {
    let db = db(dir, "authenticated");
    let addr = free_port();
    let mut server = Command::new(exe())
        .env(DB, &db)
        .env(ADDR, addr.to_string())
        .env(TOKEN, SECRET)
        .spawn()
        .expect("the deployment serves");
    // whoami rather than health: health is a read, and reads need a viewer
    wait_for("the ui to come up", || get(addr, "/api/whoami")).await;
    let url = format!("http://{addr}");

    // nothing to present, and the message is what to do about it rather than
    // what happened
    let refused = operator(&["--server", &url, "runs"]);
    refused.assert(8);
    assert!(refused.stderr.contains("HESTAN_TOKEN"), "{refused:?}");
    assert!(refused.stderr.contains("--token"), "{refused:?}");

    // the flag, and the variable a cron line uses instead so the secret is not
    // in argv where `ps` shows it
    operator(&["--server", &url, "--token", SECRET, "runs"]).assert(0);
    operator_with(&[("HESTAN_TOKEN", SECRET)], &["--server", &url, "runs"]).assert(0);
    // and the flag wins, so a shell with a variable set can still be pointed
    // somewhere else
    operator_with(
        &[("HESTAN_TOKEN", "wrong")],
        &["--server", &url, "--token", SECRET, "runs"],
    )
    .assert(0);

    // a token it does not accept is the same refusal, and says nothing about
    // how close it was
    let wrong = operator(&["--server", &url, "--token", "wrong", "runs"]);
    wrong.assert(8);
    assert!(wrong.stderr.contains("refused this token"), "{wrong:?}");

    // launching over the network is what the token is for
    let launched = operator(&[
        "--server", &url, "--token", SECRET, "run", "quick", "--wait",
    ]);
    launched.assert(0);

    // doctor, pointed at a deployment it has no credential for, can still say
    // whether it is guarded, which is the question you ask before you know
    let blind = operator(&["--server", &url, "--json", "doctor"]);
    blind.assert(0);
    let value: Value = serde_json::from_str(&blind.stdout).expect("one json object");
    let finding = &value["findings"][0];
    assert_eq!(finding["check"], "auth");
    assert!(
        finding["says"].as_str().unwrap().contains("checks who"),
        "{finding}"
    );
    // and it says what it could not see rather than calling it healthy
    assert!(
        !value["unchecked"].as_array().unwrap().is_empty(),
        "{value}"
    );

    let known = operator(&["--server", &url, "--token", SECRET, "--json", "doctor"]);
    known.assert(0);
    let value: Value = serde_json::from_str(&known.stdout).expect("one json object");
    assert!(
        value["findings"][0]["says"]
            .as_str()
            .unwrap()
            .contains("bearer"),
        "{value}"
    );

    // an open deployment is a different answer, not a missing one
    let open = operator(&["--server", "http://127.0.0.1:1", "doctor"]);
    open.assert(5);

    // and nothing the command line printed carries the token
    for ran in [&refused, &wrong, &launched, &blind, &known] {
        assert!(!ran.stdout.contains(SECRET), "{ran:?}");
        assert!(!ran.stderr.contains(SECRET), "{ran:?}");
    }

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

/// the stability claim, made across a process boundary.
///
/// the numbers below are computed twice: once by the child, which built the
/// registry and answered `assets`, and once here out of the same names. a hue
/// seeded per process (which is what `std`'s hasher is) would agree with
/// itself and not with anybody else, and that is the failure this catches.
/// the group and origin columns and the `--group` filter are the same
/// invocation, so they are asserted here too.
async fn coloured(dir: &Path) {
    let db = db(dir, "colours");
    let listed = cli(&db, &["--json", "assets"]);
    listed.assert(0);
    let body: Value = serde_json::from_str(&listed.stdout).expect("one json object");
    let rows = body["assets"].as_array().expect("assets");
    assert_eq!(rows.len(), 5, "{rows:?}");

    let mut seen = 0;
    for row in rows {
        if let Some(group) = row["group"].as_str() {
            assert_eq!(
                row["group_hue"],
                json!(hestan::hue(group)),
                "the child painted {group} differently: {row}"
            );
            seen += 1;
        }
        for origin in row["provenance"].as_array().expect("provenance") {
            let name = origin["name"].as_str().expect("a name");
            assert_eq!(
                origin["hue"],
                json!(hestan::hue(name)),
                "the child painted {name} differently: {row}"
            );
            seen += 1;
        }
    }
    assert!(seen >= 8, "only {seen} hues came back to compare");

    // an origin is the group of the source it descends from, so the two
    // warehouse tables are one label and the vendor feed is another
    let margin = rows.iter().find(|a| a["name"] == json!("margin")).unwrap();
    assert_eq!(margin["group"], json!("finance"));
    assert_eq!(
        margin["provenance"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["fx_rates", "warehouse"]
    );
    // an ungrouped source stands for itself, and its group is null
    let fx = rows
        .iter()
        .find(|a| a["name"] == json!("fx_rates"))
        .unwrap();
    assert_eq!(fx["group"], json!(null));

    // the filter is on the resolved group, so it finds the one that declared
    // it and the one that only has it in its name
    let finance = cli(&db, &["--json", "assets", "--group", "finance"]);
    finance.assert(0);
    let body: Value = serde_json::from_str(&finance.stdout).expect("one json object");
    let names: Vec<&str> = body["assets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["margin", "finance/netted"]);

    // and the table says both, in words, since a colour is never the only
    // thing carrying an answer
    let table = cli(&db, &["assets"]);
    table.assert(0);
    assert!(table.stdout.contains("GROUP"), "{table:?}");
    assert!(table.stdout.contains("ORIGIN"), "{table:?}");
    assert!(table.stdout.contains("warehouse"), "{table:?}");
    assert!(table.stdout.contains("fx_rates, warehouse"), "{table:?}");
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

    // a dry run resolves a plan without touching the store, so it is the one
    // place params are rendered that the store never sees. it redacts off the
    // same declaration, because a token typed on a command line in ci ends up
    // in the same log the run page would have
    let planned = cli(
        &db,
        &[
            "--json",
            "run",
            "deploy",
            "--params",
            "{\"token\":\"a-deploy-token-nobody-should-see\",\"env\":1}",
            "--dry-run",
        ],
    );
    planned.assert(0);
    assert!(
        !planned.stdout.contains("a-deploy-token-nobody-should-see"),
        "{planned:?}"
    );
    assert!(planned.stdout.contains("[hestan:redacted]"), "{planned:?}");

    // and the classic leak: a refusal that prints what it was given
    let refused = cli(
        &db,
        &[
            "run",
            "deploy",
            "--params",
            "{\"token\":\"a-deploy-token-nobody-should-see\",\"env\":\"prod\"}",
            "--dry-run",
        ],
    );
    refused.assert(2);
    assert!(
        !refused.stderr.contains("a-deploy-token-nobody-should-see"),
        "{refused:?}"
    );

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
    operator_with(&[], args)
}

/// the same, with variables set for that one command, which is how a cron
/// line hands a secret to a process without putting it in argv.
fn operator_with(env: &[(&str, &str)], args: &[&str]) -> Ran {
    let mut command = Command::new(env!("CARGO_BIN_EXE_hestan"));
    for (name, value) in env {
        command.env(name, value);
    }
    Ran::of(
        command
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
