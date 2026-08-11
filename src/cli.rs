//! a command line that *is* the deployment.
//!
//! the jobs, assets, schedules and sensors are compiled into the binary this
//! ran from. there is no workspace file to point at, no module to import, no
//! registry to load: it is already here. so
//!
//! ```no_run
//! # async fn f(app: hestan::Hestan, addr: std::net::SocketAddr) -> Result<(), hestan::Error> {
//! hestan::cli::run(app, addr).await
//! # }
//! ```
//!
//! in place of `app.serve(addr).await` gives that binary a complete command
//! line that knows every name in the registry and starts in the time it takes
//! to open a database. everything distinctive here follows from that one fact.
//!
//! **with no arguments it serves**, on exactly the address it was handed. that
//! is the promise the whole mount rests on: a deployment that swaps the one
//! call for the other and changes nothing else behaves as it did.

use std::collections::BTreeMap;
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use serde_json::{Value, json};

use crate::app::Hestan;
use crate::error::Error;
use crate::executor::Runner;
use crate::model::{EventLevel, OpLog, OpStatus, Role, Run, RunStatus, RunTags, Trigger};
use crate::store::Store;

/// how often the wait loop looks for new lines and for a settled status.
///
/// a poll rather than a subscription because the run may be executing in
/// another process altogether — there is no notification that crosses a process
/// boundary, and the store is the only thing both of them can see.
const TAIL_POLL: Duration = Duration::from_millis(50);

/// how many captured lines or events one drain reads at a time. an op that
/// printed a million lines is paged through rather than read whole.
const TAIL_PAGE: u32 = 500;

/// how many runs `runs` shows unless `--limit` says otherwise.
const RUNS_PAGE: u32 = 20;

/// what the process exits with.
///
/// this is the whole of the interface a cron line or a ci step has to what
/// happened, so each number means one thing and keeps meaning it. the codes a
/// `--wait` run can produce are the first five.
///
/// | code | meaning |
/// | ---- | ------- |
/// | 0 | the command did what was asked; a `--wait` run succeeded |
/// | 1 | the run failed, or the command could not do what was asked |
/// | 2 | the command line was wrong: a bad flag, an unknown job, params the schema rejects |
/// | 3 | the run was canceled |
/// | 4 | `--timeout` ran out; the run is still going |
/// | 5 | the store or the server could not be reached |
/// | 6 | this mode cannot serve this command, and the message says why |
/// | 7 | `doctor` found something actionable |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Exit {
    Ok = 0,
    Failed = 1,
    Usage = 2,
    Canceled = 3,
    Timeout = 4,
    Unreachable = 5,
    Unsupported = 6,
    Actionable = 7,
}

/// why a command stopped: the line stderr gets, and the code the shell gets.
#[derive(Debug)]
struct Fail {
    code: Exit,
    message: String,
}

impl Fail {
    fn new(code: Exit, message: impl Into<String>) -> Fail {
        Fail {
            code,
            message: message.into(),
        }
    }

    fn usage(message: impl Into<String>) -> Fail {
        Fail::new(Exit::Usage, message)
    }
}

/// which failures are the caller's mistake, which are the database being out of
/// reach, and which are neither.
///
/// the split matters more than the exact wording: a cron line that retries on 5
/// and pages someone on 1 needs "nothing was reachable" and "the work went
/// wrong" to be different answers.
impl From<Error> for Fail {
    fn from(e: Error) -> Fail {
        let code = match &e {
            Error::UnknownJob(_)
            | Error::UnknownRun(_)
            | Error::UnknownAsset(_)
            | Error::UnknownBackfill(_)
            | Error::InvalidParams { .. }
            | Error::RunActive(_)
            | Error::RunNotFailed(_)
            | Error::NothingToResume(_) => Exit::Usage,
            Error::Sqlite(_) | Error::Io(_) | Error::UnsupportedDb(_) | Error::SchemaTooNew(_) => {
                Exit::Unreachable
            }
            #[cfg(feature = "postgres")]
            Error::Postgres(_) => Exit::Unreachable,
            _ => Exit::Failed,
        };
        Fail {
            code,
            message: e.to_string(),
        }
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "launch, inspect and diagnose this deployment",
    long_about = "launch, inspect and diagnose this deployment.\n\n\
                  the jobs, assets, schedules and sensors are compiled into this \
                  binary, so every name below is one it already knows and nothing \
                  has to be loaded to find them. with no command at all it serves, \
                  which is what it did before there was a command line.",
    disable_help_subcommand = true
)]
struct Cli {
    #[command(flatten)]
    global: Global,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Args, Clone)]
struct Global {
    /// one json object on stdout; anything that streams is one object per line
    #[arg(long, global = true)]
    json: bool,
    /// the id alone and nothing else, which is what `$(...)` wants
    #[arg(long, short, global = true, conflicts_with = "json")]
    quiet: bool,
}

#[derive(Subcommand)]
enum Command {
    /// launch a run of a job
    Run(RunArgs),
    /// recent runs, newest first
    Runs(RunsArgs),
    /// one run and every op of it
    Show(ShowArgs),
    /// what a run's ops printed
    Logs(LogsArgs),
    /// ask a queued or running run to stop
    Cancel(RunRef),
    /// launch the same job again with the same params
    Retry(RunRef),
    /// re-run what did not succeed, seeding what did
    Resume(ResumeArgs),
    /// the ui and whatever loops this process's role owns
    Serve(ServeArgs),
}

#[derive(Args)]
struct RunArgs {
    /// the job to run
    job: String,
    /// params as one json object
    #[arg(long, value_name = "JSON", conflicts_with = "preset")]
    params: Option<String>,
    /// launch with a stored preset's params instead
    #[arg(long, value_name = "NAME")]
    preset: Option<String>,
    /// tag the run; repeatable
    #[arg(long = "tag", value_name = "KEY=VALUE")]
    tags: Vec<String>,
    /// where in the queue it goes: higher starts first
    #[arg(long, allow_negative_numbers = true)]
    priority: Option<i64>,
    /// execute it here, stream the log to stderr, and exit with what it did
    #[arg(long, short)]
    wait: bool,
    /// stop waiting after this many seconds; the run carries on without you
    #[arg(long, value_name = "SECS", requires = "wait")]
    timeout: Option<u64>,
}

#[derive(Args)]
struct RunsArgs {
    /// only runs of this job
    #[arg(long, value_name = "NAME")]
    job: Option<String>,
    /// only runs carrying this tag
    #[arg(long, value_name = "KEY=VALUE")]
    tag: Option<String>,
    /// only runs created since then: `2h`, `30m`, `7d`, or an rfc3339 instant
    #[arg(long, value_name = "WHEN")]
    since: Option<String>,
    #[arg(long, default_value_t = RUNS_PAGE)]
    limit: u32,
}

#[derive(Args)]
struct ShowArgs {
    /// the run id
    run: String,
}

#[derive(Args)]
struct RunRef {
    /// the run id
    run: String,
}

#[derive(Args)]
struct LogsArgs {
    /// the run id
    run: String,
    /// only this op's output
    #[arg(long, value_name = "NAME")]
    op: Option<String>,
    /// keep printing as more arrives, until the run is over
    #[arg(long, short)]
    follow: bool,
    #[arg(long, default_value_t = 500)]
    limit: u32,
}

#[derive(Args)]
struct ResumeArgs {
    /// the run id
    run: String,
    /// re-run exactly these ops and everything downstream, whatever they did;
    /// repeatable. without it, every op that did not succeed
    #[arg(long = "from", value_name = "OP")]
    from: Vec<String>,
}

#[derive(Args)]
struct ServeArgs {
    /// bind here instead of wherever the host asked for
    #[arg(long, value_name = "HOST:PORT")]
    addr: Option<SocketAddr>,
}

/// parse argv and do what it says, in this binary, against this registry.
///
/// **with no arguments this is `app.serve(addr)`** — the same call, the same
/// address, the same error if the socket will not bind. a deployment swapping
/// one for the other gains a command line and changes nothing else, which is
/// the only way a mount like this is worth having.
///
/// anything that is not a success exits the process with the code its
/// [table](Exit) documents, after one line on stderr saying what happened.
/// that is why the `Ok` case here is only ever a success: a return value cannot
/// carry an exit code through a `main` that returns `Result`.
pub async fn run(app: Hestan, addr: impl Into<SocketAddr>) -> Result<(), Error> {
    // before argv, because this process may not have been started by a person:
    // an [isolated op](crate::Op::isolated) re-executes this same binary with
    // no arguments at all, and no arguments is how the command line spells
    // "serve". a mount that skipped this would answer a request to run one op
    // by binding a socket. `run_op_subprocess` never returns
    #[cfg(unix)]
    if let Some(req) = crate::isolate::requested() {
        app.run_op_subprocess(req).await
    }
    let cli = Cli::parse();
    let out = Out::new(&cli.global);
    let command = match cli.command {
        // the promise at the top of this file
        None => return app.serve(addr).await,
        Some(Command::Serve(args)) => return app.serve(args.addr.unwrap_or(addr.into())).await,
        Some(command) => command,
    };
    match embedded(app, command, &out).await {
        Ok(()) => Ok(()),
        Err(fail) => {
            eprintln!("{} {}", out.paint("error:", RED), fail.message);
            std::process::exit(fail.code as i32)
        }
    }
}

/// everything the registry in this process can answer.
async fn embedded(app: Hestan, command: Command, out: &Out) -> Result<(), Fail> {
    match command {
        Command::Run(args) => launch(app, args, out).await,
        Command::Runs(args) => list_runs(&app.open()?, args, out),
        Command::Show(args) => show(&app.open()?, &args.run, out),
        Command::Logs(args) => logs(&app.open()?, args, out).await,
        Command::Cancel(args) => cancel(&app.open()?, &args.run, out),
        Command::Retry(args) => {
            let built = app.role(launching_role(false)).build().await?;
            let run = run_row(built.runner.store(), &args.run)?;
            if matches!(run.status, RunStatus::Queued | RunStatus::Running) {
                return Err(Fail::usage(format!("run still active: {}", args.run)));
            }
            let id = built
                .runner
                .launch(&run.job, run.params, Trigger::Retry)
                .map_err(Fail::from)?;
            out.launched(&id, &run.job);
            Ok(())
        }
        Command::Resume(args) => {
            let built = app.role(launching_role(false)).build().await?;
            let from = (!args.from.is_empty()).then_some(args.from.as_slice());
            let id = built
                .runner
                .resume_from(&args.run, from)
                .map_err(Fail::from)?;
            let job = built
                .runner
                .store()
                .run(&id)?
                .map_or_else(String::new, |r| r.job);
            out.launched(&id, &job);
            Ok(())
        }
        // handled before the registry is built, since they never return
        Command::Serve(_) => unreachable!("serve is dispatched by `run`"),
    }
}

// ------------------------------------------------------------------- launching

/// what a process that is about to launch something should be.
///
/// **waiting**, it executes the run itself — the same thing
/// [`Hestan::run_once`](crate::Hestan::run_once) does, and for the same reason:
/// a one-shot has nobody else to hand the work to, and a `--wait` that only
/// enqueued would hang wherever nothing else was serving.
///
/// **not waiting**, it must not, and this is the whole reason there is a choice
/// here. an enqueue pokes the dispatcher, so a process that both decides and
/// executes would start the run and then exit out from under it a millisecond
/// later — a launch that reliably killed what it launched. a role that decides
/// and does not execute leaves the run on the queue, which is exactly what
/// "launch" has always meant everywhere else in hestan.
fn launching_role(wait: bool) -> Role {
    match wait {
        true => Role::All,
        false => Role::Scheduler,
    }
}

async fn launch(app: Hestan, args: RunArgs, out: &Out) -> Result<(), Fail> {
    let tags = parse_tags(&args.tags)?;
    let built = app.role(launching_role(args.wait)).build().await?;
    let runner = built.runner;
    let params = match (&args.params, &args.preset) {
        (Some(text), _) => serde_json::from_str(text)
            .map_err(|e| Fail::usage(format!("--params is one json object: {e}")))?,
        (None, Some(preset)) => {
            runner
                .store()
                .preset(&args.job, preset)?
                .ok_or_else(|| {
                    Fail::usage(format!("unknown preset: {preset} on job {}", args.job))
                })?
                .params
        }
        (None, None) => json!({}),
    };
    let id = runner
        .launch_prioritized(&args.job, params, Trigger::Manual, tags, args.priority)
        .map_err(Fail::from)?;
    if !args.wait {
        out.launched(&id, &args.job);
        return Ok(());
    }
    let timeout = args.timeout.map(Duration::from_secs);
    let run = wait(&runner, &id, timeout, out).await?;
    out.settled(&run);
    match run.status {
        RunStatus::Success => Ok(()),
        RunStatus::Canceled => Err(Fail::new(Exit::Canceled, format!("run {id} was canceled"))),
        _ => Err(Fail::new(
            Exit::Failed,
            run.error
                .clone()
                .unwrap_or_else(|| format!("run {id} failed")),
        )),
    }
}

/// stream what the run says while it runs, and hand back the row it settled at.
///
/// the ordering is the whole of this function. the status is read *before* the
/// drain that follows it, so a run that finished between two polls has its last
/// lines read after the status that ends the loop rather than before it — which
/// is the race a fast job loses: it can be over before the first poll, and
/// every line it wrote still has to come out. the executor writes a run's
/// terminal event before its terminal status for the same reason, so stopping
/// at the status leaves nothing behind.
async fn wait(
    runner: &Runner,
    id: &str,
    timeout: Option<Duration>,
    out: &Out,
) -> Result<Run, Fail> {
    let deadline = timeout.map(|t| Instant::now() + t);
    let mut tail = Tail::default();
    loop {
        let run = run_row(runner.store(), id)?;
        let settled = !matches!(run.status, RunStatus::Queued | RunStatus::Running);
        tail.drain(runner.store(), id, out)?;
        if settled {
            return Ok(run);
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            // "gave up waiting" and "stopped the run" are different things and
            // only one of them happened — except when the run is executing
            // right here, where exiting is the other one too, and saying so
            // beats leaving it to be discovered
            let mine = run.claimed_by.as_deref() == Some(runner.instance());
            let ours = match mine {
                true => ", which was executing here and stops with this process",
                false => "",
            };
            return Err(Fail::new(
                Exit::Timeout,
                format!("timed out waiting for run {id}{ours}"),
            ));
        }
        // whatever the queue will let this process start, now: without this a
        // limit that freed up elsewhere would be noticed a dispatch interval
        // late, and a run launched into a full queue would sit there
        runner.dispatch();
        tokio::time::sleep(TAIL_POLL).await;
    }
}

/// where a follower has read up to in each of the two tables a run talks
/// through: the events it raised and the lines its ops printed.
#[derive(Default)]
struct Tail {
    events: i64,
    logs: i64,
}

impl Tail {
    /// print everything new in both, oldest first.
    ///
    /// merged on the clock rather than shown one after the other, because they
    /// are two halves of one account of the run: the op that said it was
    /// starting and the line it printed a millisecond later belong next to each
    /// other.
    fn drain(&mut self, store: &Store, run: &str, out: &Out) -> Result<(), Error> {
        loop {
            let events = store.events(run, self.events)?;
            let logs = store.op_logs(run, None, self.logs, TAIL_PAGE)?;
            if events.is_empty() && logs.is_empty() {
                return Ok(());
            }
            let full = logs.len() as u32 == TAIL_PAGE;
            let mut lines: Vec<Line> = Vec::with_capacity(events.len() + logs.len());
            for e in events {
                self.events = self.events.max(e.seq);
                lines.push(Line {
                    at: e.ts,
                    op: e.op,
                    level: Some(e.level),
                    message: e.message,
                });
            }
            for l in logs {
                self.logs = self.logs.max(l.id);
                lines.push(Line {
                    at: l.at,
                    op: Some(l.op),
                    level: l.level,
                    message: l.message,
                });
            }
            lines.sort_by_key(|l| l.at);
            for line in lines {
                out.stream(&line);
            }
            // a page that came back full may have left more behind it
            if !full {
                return Ok(());
            }
        }
    }
}

/// one line of what a run is saying, from either of the two tables that say it.
struct Line {
    at: DateTime<Utc>,
    op: Option<String>,
    level: Option<EventLevel>,
    message: String,
}

// --------------------------------------------------------------------- reading

fn list_runs(store: &Store, args: RunsArgs, out: &Out) -> Result<(), Fail> {
    let tag = args.tag.as_deref().map(split_pair).transpose()?;
    let since = args.since.as_deref().map(instant).transpose()?;
    let runs = store.runs(
        args.job.as_deref(),
        since,
        None,
        None,
        tag.as_ref().map(|(k, v)| (k.as_str(), v.as_str())),
        args.limit.clamp(1, 2000),
    )?;
    if out.json {
        out.object(&json!({ "runs": runs }));
        return Ok(());
    }
    if out.quiet {
        for run in &runs {
            println!("{}", run.id);
        }
        return Ok(());
    }
    let mut table = Table::new(["RUN", "JOB", "STATUS", "TRIGGER", "STARTED", "TOOK"]);
    for run in &runs {
        table.row([
            Cell::plain(&run.id),
            Cell::plain(&run.job),
            Cell::styled(run.status.as_str(), status_color(run.status)),
            Cell::plain(run.trigger.as_str()),
            Cell::plain(stamp(run.started_at.unwrap_or(run.created_at))),
            Cell::plain(took(run)),
        ]);
    }
    table.print(out, "no runs");
    Ok(())
}

fn show(store: &Store, id: &str, out: &Out) -> Result<(), Fail> {
    let run = run_row(store, id)?;
    let mut ops = store.op_runs(id)?;
    // in the order they ran, not the order they are stored in: what a person
    // reading a run wants first is where it got to
    ops.sort_by(|a, b| match (a.started_at, b.started_at) {
        (Some(a), Some(b)) => a.cmp(&b),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
    if out.json {
        out.object(&json!({ "run": run, "ops": ops }));
        return Ok(());
    }
    if out.quiet {
        println!("{}", run.id);
        return Ok(());
    }
    println!(
        "{} {}  {}",
        out.paint(&run.job, BOLD),
        run.id,
        out.paint(run.status.as_str(), status_color(run.status))
    );
    println!("trigger  {}", run.trigger.as_str());
    println!("created  {}", run.created_at.to_rfc3339());
    if let Some(started) = run.started_at {
        println!("started  {}", started.to_rfc3339());
    }
    if let Some(finished) = run.finished_at {
        println!("ended    {}  ({})", finished.to_rfc3339(), took(&run));
    }
    if run.params != json!({}) {
        println!("params   {}", run.params);
    }
    if !run.tags.is_empty() {
        let tags: Vec<String> = run.tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
        println!("tags     {}", tags.join(" "));
    }
    if let Some(error) = &run.error {
        println!("error    {}", out.paint(error, RED));
    }
    println!();
    let mut table = Table::new(["OP", "STATUS", "ATTEMPTS", "TOOK", "ERROR"]);
    for op in &ops {
        let elapsed = match (op.started_at, op.finished_at) {
            (Some(a), Some(b)) => secs((b - a).num_milliseconds()),
            _ => "-".into(),
        };
        table.row([
            Cell::plain(&op.op),
            Cell::styled(op.status.as_str(), op_color(op.status)),
            Cell::plain(op.attempts.to_string()),
            Cell::plain(elapsed),
            Cell::plain(op.error.clone().unwrap_or_default()),
        ]);
    }
    table.print(out, "no ops recorded");
    Ok(())
}

async fn logs(store: &Store, args: LogsArgs, out: &Out) -> Result<(), Fail> {
    let run = run_row(store, &args.run)?;
    if !args.follow {
        let lines = store.op_logs(
            &args.run,
            args.op.as_deref(),
            0,
            args.limit.clamp(1, 100_000),
        )?;
        if out.json {
            out.object(&json!({ "logs": lines }));
            return Ok(());
        }
        for line in &lines {
            print_log(out, line);
        }
        return Ok(());
    }
    let mut after = 0;
    let mut settled = !matches!(run.status, RunStatus::Queued | RunStatus::Running);
    loop {
        // the same ordering `wait` documents: the status first, the drain
        // after it, so the last line of a run that ended mid-poll still prints
        let lines = store.op_logs(&args.run, args.op.as_deref(), after, TAIL_PAGE)?;
        for line in &lines {
            after = after.max(line.id);
            match out.json {
                true => out.line(&json!(line)),
                false => print_log(out, line),
            }
        }
        if settled && lines.is_empty() {
            return Ok(());
        }
        if !settled {
            let run = run_row(store, &args.run)?;
            settled = !matches!(run.status, RunStatus::Queued | RunStatus::Running);
        }
        if lines.is_empty() {
            tokio::time::sleep(TAIL_POLL).await;
        }
    }
}

fn print_log(out: &Out, line: &OpLog) {
    println!(
        "{} {} {}",
        out.paint(&stamp(line.at), DIM),
        out.paint(&line.op, CYAN),
        line.message
    );
}

// -------------------------------------------------------------------- stopping

/// stop a run, or say plainly why this process cannot.
///
/// a cancel is a signal to whichever process is executing the run, and there is
/// no signal in the database: a run already claimed can only be stopped by its
/// claimer, which a command line that started a moment ago is not. what this
/// *can* do is take a run off the queue before anyone claims it, which is the
/// case a cancel is usually reaching for anyway.
fn cancel(store: &Store, id: &str, out: &Out) -> Result<(), Fail> {
    let run = run_row(store, id)?;
    match run.status {
        RunStatus::Queued if run.claimed_by.is_none() => {
            if !store.cancel_queued(id)? {
                return Err(Fail::new(
                    Exit::Failed,
                    format!("run {id} was claimed while this was taking it off the queue"),
                ));
            }
            if out.json {
                out.object(&json!({ "run_id": id, "outcome": "canceled" }));
            } else if out.quiet {
                println!("{id}");
            } else {
                println!("canceled {id}, which had not started");
            }
            Ok(())
        }
        RunStatus::Queued | RunStatus::Running => Err(Fail::new(
            Exit::Unsupported,
            format!(
                "run {id} is being executed by instance {}, and only that process can stop it \
                 — reach it with --server, or use the ui it is serving",
                run.claimed_by.as_deref().unwrap_or("(unknown)")
            ),
        )),
        status => Err(Fail::new(
            Exit::Failed,
            format!("run {id} already finished ({status})"),
        )),
    }
}

// --------------------------------------------------------------------- helpers

fn run_row(store: &Store, id: &str) -> Result<Run, Fail> {
    store
        .run(id)?
        .ok_or_else(|| Fail::usage(format!("unknown run: {id}")))
}

/// `KEY=VALUE`, which is how a shell spells a pair without quoting anything.
fn split_pair(text: &str) -> Result<(String, String), Fail> {
    match text.split_once('=') {
        Some((k, v)) if !k.is_empty() => Ok((k.to_string(), v.to_string())),
        _ => Err(Fail::usage(format!("{text:?} is not KEY=VALUE"))),
    }
}

fn parse_tags(pairs: &[String]) -> Result<RunTags, Fail> {
    let mut tags = BTreeMap::new();
    for pair in pairs {
        let (k, v) = split_pair(pair)?;
        tags.insert(k, v);
    }
    Ok(tags)
}

/// `2h`, `30m`, `7d` — or an rfc3339 instant, for a script that has one.
///
/// the short form is what anybody types at a terminal and the long one is what
/// a program has, so both are accepted and neither is the "real" one.
fn instant(text: &str) -> Result<DateTime<Utc>, Fail> {
    if let Ok(t) = DateTime::parse_from_rfc3339(text) {
        return Ok(t.with_timezone(&Utc));
    }
    let (digits, unit) = text.split_at(text.len().saturating_sub(1));
    let n: i64 = digits.parse().map_err(|_| bad_when(text))?;
    let ago = match unit {
        "s" => chrono::Duration::seconds(n),
        "m" => chrono::Duration::minutes(n),
        "h" => chrono::Duration::hours(n),
        "d" => chrono::Duration::days(n),
        _ => return Err(bad_when(text)),
    };
    Ok(Utc::now() - ago)
}

fn bad_when(text: &str) -> Fail {
    Fail::usage(format!(
        "{text:?} is not a time: try 30m, 2h, 7d, or an rfc3339 instant"
    ))
}

fn stamp(at: DateTime<Utc>) -> String {
    at.format("%H:%M:%S").to_string()
}

fn took(run: &Run) -> String {
    match (run.started_at, run.finished_at) {
        (Some(a), Some(b)) => secs((b - a).num_milliseconds()),
        _ => "-".into(),
    }
}

fn secs(ms: i64) -> String {
    format!("{:.1}s", ms as f64 / 1000.0)
}

// ---------------------------------------------------------- the output contract

const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

fn status_color(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Success => GREEN,
        RunStatus::Failed => RED,
        RunStatus::Canceled => YELLOW,
        RunStatus::Running => CYAN,
        RunStatus::Queued => DIM,
    }
}

fn op_color(status: OpStatus) -> &'static str {
    match status {
        OpStatus::Success => GREEN,
        OpStatus::Failed => RED,
        OpStatus::Canceled | OpStatus::Skipped => YELLOW,
        OpStatus::Running => CYAN,
        OpStatus::Pending => DIM,
    }
}

/// how output is spelled: what the flags asked for, and what the terminal on
/// the other end can take.
///
/// the rule the rest of this module keeps is that **stdout belongs to the
/// answer**. under `--json` or `--quiet` nothing else may reach it — no
/// progress, no warning, no blank line — because something is parsing it. a
/// run's log is stderr for the same reason: it is company while you wait, not
/// the answer.
struct Out {
    json: bool,
    quiet: bool,
    /// whether stdout is a terminal that asked for colour.
    color: bool,
    /// and separately stderr, which is where a wait streams: one of the two
    /// being a pipe says nothing about the other.
    color_err: bool,
}

impl Out {
    fn new(global: &Global) -> Out {
        // a machine-readable mode is never styled, whatever is on the other end
        let wanted = !global.json && !global.quiet && !no_color();
        Out {
            json: global.json,
            quiet: global.quiet,
            color: wanted && std::io::stdout().is_terminal(),
            color_err: wanted && std::io::stderr().is_terminal(),
        }
    }

    fn paint(&self, text: &str, style: &str) -> String {
        match self.color {
            true => format!("{style}{text}{RESET}"),
            false => text.to_string(),
        }
    }

    /// one json object on stdout, which is what `--json` promises.
    fn object(&self, value: &Value) {
        println!("{value}");
    }

    /// one json object on its own line, which is what `--json` promises of
    /// anything that streams.
    fn line(&self, value: &Value) {
        println!("{value}");
    }

    /// a run was put on the queue.
    fn launched(&self, id: &str, job: &str) {
        if self.json {
            self.object(&json!({ "run_id": id, "job": job, "status": "queued" }));
        } else if self.quiet {
            println!("{id}");
        } else {
            println!("{id}  {job} queued");
        }
    }

    /// a `--wait` run reached a terminal status. the failure line is stderr's
    /// job, next to the exit code it goes with.
    fn settled(&self, run: &Run) {
        if self.json {
            self.object(&json!(run));
        } else if self.quiet {
            println!("{}", run.id);
        } else {
            println!(
                "{}  {} {} in {}",
                run.id,
                run.job,
                self.paint(run.status.as_str(), status_color(run.status)),
                took(run)
            );
        }
    }

    /// one line of a running run, on stderr.
    fn stream(&self, line: &Line) {
        if self.quiet {
            return;
        }
        let style = match line.level {
            Some(EventLevel::Error) => RED,
            Some(EventLevel::Warn) => YELLOW,
            _ => "",
        };
        let (at, op, message) = match self.color_err {
            true => (
                format!("{DIM}{}{RESET}", stamp(line.at)),
                line.op
                    .as_ref()
                    .map(|op| format!("{CYAN}{op}{RESET} "))
                    .unwrap_or_default(),
                match style.is_empty() {
                    true => line.message.clone(),
                    false => format!("{style}{}{RESET}", line.message),
                },
            ),
            false => (
                stamp(line.at),
                line.op
                    .as_ref()
                    .map(|op| format!("{op} "))
                    .unwrap_or_default(),
                line.message.clone(),
            ),
        };
        eprintln!("{at} {op}{message}");
    }
}

/// the `NO_COLOR` convention: any non-empty value means plain text, whatever
/// the terminal is.
fn no_color() -> bool {
    std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty())
}

struct Cell {
    text: String,
    style: &'static str,
}

impl Cell {
    fn plain(text: impl Into<String>) -> Cell {
        Cell {
            text: text.into(),
            style: "",
        }
    }

    fn styled(text: impl Into<String>, style: &'static str) -> Cell {
        Cell {
            text: text.into(),
            style,
        }
    }
}

/// a padded table on stdout.
///
/// the styling goes on after the padding, so a coloured cell is exactly as wide
/// as the same cell in a pipe — a column that moves when you turn colour on is
/// a column nothing can line up against.
struct Table {
    headers: Vec<&'static str>,
    rows: Vec<Vec<Cell>>,
}

impl Table {
    fn new<const N: usize>(headers: [&'static str; N]) -> Table {
        Table {
            headers: headers.to_vec(),
            rows: Vec::new(),
        }
    }

    fn row<const N: usize>(&mut self, cells: [Cell; N]) {
        self.rows.push(cells.into());
    }

    fn print(&self, out: &Out, empty: &str) {
        if self.rows.is_empty() {
            println!("{empty}");
            return;
        }
        let widths: Vec<usize> = self
            .headers
            .iter()
            .enumerate()
            .map(|(i, h)| {
                self.rows
                    .iter()
                    .map(|r| r[i].text.chars().count())
                    .chain(std::iter::once(h.chars().count()))
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let mut header = String::new();
        for (i, h) in self.headers.iter().enumerate() {
            pad(&mut header, h, widths[i], i + 1 == widths.len());
        }
        println!("{}", out.paint(header.trim_end(), BOLD));
        for row in &self.rows {
            let mut line = String::new();
            for (i, cell) in row.iter().enumerate() {
                let last = i + 1 == widths.len();
                match out.color && !cell.style.is_empty() {
                    true => {
                        let mut padded = String::new();
                        pad(&mut padded, &cell.text, widths[i], last);
                        line.push_str(&format!("{}{padded}{RESET}", cell.style));
                    }
                    false => pad(&mut line, &cell.text, widths[i], last),
                }
            }
            println!("{}", line.trim_end());
        }
    }
}

fn pad(into: &mut String, text: &str, width: usize, last: bool) {
    into.push_str(text);
    if !last {
        for _ in text.chars().count()..width + 2 {
            into.push(' ');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plain() -> Out {
        Out {
            json: false,
            quiet: false,
            color: false,
            color_err: false,
        }
    }

    #[test]
    fn a_tag_is_a_key_and_a_value_split_on_the_first_equals() {
        let tags = parse_tags(&["env=prod".into(), "note=a=b".into()]).unwrap();
        assert_eq!(tags["env"], "prod");
        assert_eq!(tags["note"], "a=b");
    }

    #[test]
    fn a_tag_with_no_equals_is_a_usage_error() {
        let fail = parse_tags(&["prod".into()]).unwrap_err();
        assert_eq!(fail.code, Exit::Usage);
        assert!(fail.message.contains("KEY=VALUE"), "{}", fail.message);
    }

    #[test]
    fn since_takes_a_duration_or_an_instant() {
        let two_hours = instant("2h").unwrap();
        let elapsed = Utc::now() - two_hours;
        assert!(elapsed.num_minutes() >= 119 && elapsed.num_minutes() <= 121);
        assert_eq!(
            instant("2024-03-01T00:00:00Z").unwrap().to_rfc3339(),
            "2024-03-01T00:00:00+00:00"
        );
        assert_eq!(instant("soon").unwrap_err().code, Exit::Usage);
    }

    // the store being out of reach and the work going wrong are different
    // answers to a cron line, and this is the mapping that keeps them apart
    #[test]
    fn an_unreachable_store_and_an_unknown_job_exit_differently() {
        let unreachable: Fail = Error::Io(std::io::Error::other("no such file")).into();
        assert_eq!(unreachable.code, Exit::Unreachable);
        let usage: Fail = Error::UnknownJob("nope".into()).into();
        assert_eq!(usage.code, Exit::Usage);
        let failed: Fail = Error::Graph("cycle".into()).into();
        assert_eq!(failed.code, Exit::Failed);
    }

    #[test]
    fn a_table_pads_every_column_to_its_widest_cell() {
        let mut table = Table::new(["RUN", "JOB"]);
        table.row([Cell::plain("a"), Cell::plain("orders")]);
        table.row([Cell::plain("longer-id"), Cell::plain("health")]);
        let widths: Vec<usize> = (0..2)
            .map(|i| {
                table
                    .rows
                    .iter()
                    .map(|r| r[i].text.chars().count())
                    .max()
                    .unwrap()
            })
            .collect();
        assert_eq!(widths, [9, 6]);
        let mut line = String::new();
        pad(&mut line, "a", 9, false);
        assert_eq!(line.len(), 11, "a column is its widest cell plus a gap");
    }

    // colour is a property of the terminal, not of the answer: a pipe and a
    // machine-readable mode both get plain text
    #[test]
    fn nothing_is_painted_without_a_terminal() {
        assert_eq!(plain().paint("success", GREEN), "success");
        let json = Out {
            json: true,
            ..plain()
        };
        assert_eq!(json.paint("success", GREEN), "success");
    }
}
