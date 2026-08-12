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
//!
//! # three ways to reach a deployment
//!
//! the same commands mean the same things whether the jobs are in this binary,
//! in a database on disk, or in a server across the network:
//!
//! - **embedded**, the default — everything works, because everything is here.
//! - **`--db <path|url>`** — a run log opened directly, with no server running.
//!   reads work; launching does not in a binary the jobs are not compiled into,
//!   and it says so in a sentence rather than an error code.
//! - **`--server <url>`** — a running instance, over the http api it already
//!   serves. no new endpoints: everything asked for over the network is
//!   something the ui already asks for.

use std::collections::{BTreeMap, HashMap};
use std::io::IsTerminal;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use clap::{Args, CommandFactory, Parser, Subcommand};
use serde_json::{Value, json};

use crate::app::{Built, Hestan, Inspected};
use crate::auth::Auth;
use crate::error::Error;
use crate::executor::Runner;
use crate::job::Job;
use crate::model::{EventLevel, Role, Run, RunStatus, RunTags, Trigger};
use crate::retention::Retention;
use crate::store::{EventQuery, Store};

/// how often the wait loop looks for new lines and for a settled status.
///
/// a poll rather than a subscription because the run may be executing in
/// another process altogether — there is no notification that crosses a process
/// boundary, and the store is the only thing both of them can see.
const TAIL_POLL: Duration = Duration::from_millis(50);

/// how often a followed event log is read. slower than a run's own tail on
/// purpose: this is the whole system's log rather than one run's, and a second
/// of lag on "what is happening" costs nothing.
const FOLLOW_POLL: Duration = Duration::from_secs(1);

/// how many captured lines or events one drain reads at a time. an op that
/// printed a million lines is paged through rather than read whole.
const TAIL_PAGE: u32 = 500;

/// how many runs `runs` shows unless `--limit` says otherwise.
const RUNS_PAGE: u32 = 20;

/// how much of the queue `queue` shows — the same page the api serves.
const QUEUE_PAGE: u32 = 200;

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
/// | 8 | the server refused this identity: no token, a token it does not accept, or a role that may not |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Exit {
    /// it did what was asked.
    Ok = 0,
    /// the work failed, or the command could not do what was asked. the code a
    /// shell reads as plain failure, and the one to retry.
    Failed = 1,
    /// the command line was wrong. no work was attempted, so retrying it does
    /// the same thing.
    Usage = 2,
    /// the run was canceled — somebody's decision, not a fault.
    Canceled = 3,
    /// `--timeout` ran out. the run is still going; this says nothing about
    /// how it ends.
    Timeout = 4,
    /// the store or the server could not be reached. the deployment, not the
    /// work.
    Unreachable = 5,
    /// this mode cannot serve this command — a launch against `--db`, say —
    /// and the message says which mode would.
    Unsupported = 6,
    /// `doctor` found something worth acting on. a code of its own so a check
    /// in ci can tell "something is wrong here" from "I could not look".
    Actionable = 7,
    /// a code of its own because a cron line does different things about it:
    /// work that failed is worth retrying and a credential that was refused is
    /// worth telling somebody about, and 1 for both is 1 for neither.
    Denied = 8,
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
    /// open this run log directly, with no server running
    #[arg(
        long,
        global = true,
        value_name = "PATH|URL",
        conflicts_with = "server"
    )]
    db: Option<String>,
    /// drive a running instance over its http api
    #[arg(long, global = true, value_name = "URL")]
    server: Option<String>,
    /// the token an authenticated `--server` wants; `HESTAN_TOKEN` is the
    /// other way, and the better one
    #[arg(long, global = true, value_name = "TOKEN")]
    token: Option<String>,
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
    /// every job this deployment defines
    Jobs,
    /// every asset, and whether it is stale
    Assets,
    /// materialize an asset and whatever upstream of it is stale
    Build(BuildArgs),
    /// launch a range of an asset's partitions, a chunk at a time
    Backfill(BackfillArgs),
    /// every schedule, when it fires next, and whether it is paused
    Schedules,
    /// stop a schedule firing or a sensor evaluating
    Pause {
        #[command(subcommand)]
        what: What,
    },
    /// start it again
    Unpause {
        #[command(subcommand)]
        what: What,
    },
    /// what is waiting to execute, in the order it will be taken
    Queue,
    /// move a queued run up or down the queue
    Priority(PriorityArgs),
    /// what happened, across every subsystem
    Events(EventsArgs),
    /// one command that answers "why is nothing running"
    Doctor,
    /// the plan a run would follow, without running it
    Explain(ExplainArgs),
    /// a completion script for your shell
    Completions(CompletionsArgs),
    /// the names this binary can complete, which is what those scripts ask for
    #[command(name = "__complete", hide = true)]
    Complete {
        #[arg(value_enum)]
        what: Names,
    },
    /// the ui and whatever loops this process's role owns
    Serve(ServeArgs),
}

#[derive(Args)]
struct ExplainArgs {
    /// the job whose plan to resolve
    job: String,
    /// validate these params against the schema while you are here
    #[arg(long, value_name = "JSON", conflicts_with = "preset")]
    params: Option<String>,
    /// validate a stored preset's params instead
    #[arg(long, value_name = "NAME")]
    preset: Option<String>,
}

#[derive(Args)]
struct CompletionsArgs {
    #[arg(value_enum)]
    shell: Shell,
    /// the command the script completes, if this binary is installed as
    /// something other than what it was invoked as
    #[arg(long, value_name = "NAME")]
    name: Option<String>,
}

/// the two things that can be paused. spelled as a subcommand rather than a
/// flag because a job and a sensor can share a name, and a command line that
/// guesses which one you meant is a command line that eventually guesses wrong.
#[derive(Subcommand)]
enum What {
    /// every schedule on this job, or the one `--expr` names
    Schedule {
        job: String,
        #[arg(long, value_name = "CRON")]
        expr: Option<String>,
    },
    Sensor {
        name: String,
    },
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
    /// check the params against the schema and print the plan, and launch
    /// nothing at all
    #[arg(long = "dry-run", conflicts_with = "wait")]
    dry_run: bool,
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
struct BuildArgs {
    /// the asset to materialize
    asset: String,
    /// rebuild exactly these partitions, whatever staleness says; repeatable
    #[arg(long = "partition", value_name = "KEY")]
    partitions: Vec<String>,
    /// wait for the build, streaming it, and exit with what it did
    #[arg(long, short)]
    wait: bool,
    #[arg(long, value_name = "SECS", requires = "wait")]
    timeout: Option<u64>,
}

#[derive(Args)]
struct BackfillArgs {
    /// the partitioned asset to fill in
    asset: String,
    /// the first partition key of the range
    #[arg(long, value_name = "KEY")]
    from: String,
    /// the last one, inclusive
    #[arg(long, value_name = "KEY")]
    to: String,
    /// build every key in the range, not only the missing and stale ones
    #[arg(long)]
    all: bool,
}

#[derive(Args)]
struct PriorityArgs {
    /// the run id, which must still be queued and unclaimed
    run: String,
    /// higher goes first
    #[arg(allow_negative_numbers = true)]
    priority: i64,
}

#[derive(Args)]
struct EventsArgs {
    /// only this kind of event
    #[arg(long, value_name = "KIND")]
    kind: Option<String>,
    /// only events about this run, job, asset, schedule or sensor
    #[arg(long, value_name = "NAME")]
    subject: Option<String>,
    /// this level exactly: info, warn or error
    #[arg(long, value_name = "LEVEL")]
    level: Option<String>,
    /// only events since then: `2h`, `30m`, `7d`, or an rfc3339 instant
    #[arg(long, value_name = "WHEN")]
    since: Option<String>,
    #[arg(long, default_value_t = 50)]
    limit: u32,
    /// keep printing as more arrives
    #[arg(long, short)]
    follow: bool,
    /// resume a follow from this seq, which is the last one you saw
    #[arg(long, value_name = "SEQ", requires = "follow")]
    after: Option<i64>,
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
/// ```no_run
/// # use hestan::{Hestan, Job};
/// #[tokio::main]
/// async fn main() -> Result<(), hestan::Error> {
/// #   let nightly = Job::builder("nightly").build()?;
///     let app = Hestan::new().job(nightly).schedule("nightly", "0 3 * * *");
///     hestan::cli::run(app, ([127, 0, 0, 1], 4000)).await
/// }
/// ```
///
/// that binary serves with no arguments, and answers `runs`, `logs`, `launch`,
/// `doctor` and the rest with the registry it already holds.
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
        // `serve` is still this binary serving, so it stays out of the
        // dispatch below — but `--db` moves which run log it serves, exactly
        // as it does for every other command
        Some(Command::Serve(args)) if cli.global.server.is_none() => {
            let app = match &cli.global.db {
                Some(db) => app.db(db),
                None => app,
            };
            return app.serve(args.addr.unwrap_or(addr.into())).await;
        }
        Some(command) => command,
    };
    finish(reach(Some(app), &cli.global), command, &out).await
}

/// the same command line in a binary that has no jobs of its own: the
/// [standalone `hestan`](../../hestan/index.html), which reaches a deployment
/// through its database or its server and says so plainly when that is not
/// enough.
///
/// there is no address to serve on and no registry to serve, so unlike
/// [`run`] this needs to be told where to look — `--db` or `--server`, and
/// nothing at all is a usage error rather than a default.
pub async fn standalone() -> Result<(), Error> {
    let cli = Cli::parse();
    let out = Out::new(&cli.global);
    let Some(command) = cli.command else {
        let _ = Cli::command().print_help();
        std::process::exit(Exit::Usage as i32)
    };
    finish(reach(None, &cli.global), command, &out).await
}

/// run the command and turn whatever it says into an exit code.
async fn finish(reach: Result<Reach, Fail>, command: Command, out: &Out) -> Result<(), Error> {
    let done = match command {
        // neither of these looks at a deployment: one writes a script and the
        // other lists this parser's own subcommands, and both have to work in a
        // shell that has not been told where anything is
        Command::Completions(args) => {
            completions(args.shell, &args.name.unwrap_or_else(invoked_as), out);
            Ok(())
        }
        Command::Complete { what } => complete(reach, what, out),
        command => match reach {
            Ok(reach) => dispatch(reach, command, out).await,
            Err(fail) => Err(fail),
        },
    };
    match done {
        Ok(()) => Ok(()),
        Err(fail) => {
            eprintln!("{} {}", out.paint("error:", RED), fail.message);
            std::process::exit(fail.code as i32)
        }
    }
}

// ----------------------------------------------------------------- the three ways

/// what a command is being run against.
///
/// the same commands mean the same things in all three, and where one of them
/// genuinely cannot answer, it says which one it is and what would — see
/// [`no_registry`]. what separates them is only what is in front of them:
/// definitions and a database, a database, or somebody else's process.
enum Reach {
    /// this binary: the jobs are compiled in, and every command works.
    Local(Box<Hestan>),
    /// a run log and nothing else. reads work; launching does not, because a
    /// database holds no job definitions.
    Store { store: Store, target: String },
    /// a running instance, over the http api it already serves.
    Server(Api),
}

fn reach(app: Option<Hestan>, global: &Global) -> Result<Reach, Fail> {
    if let Some(url) = &global.server {
        return Ok(Reach::Server(Api::new(url, token(global))));
    }
    match (app, &global.db) {
        // a binary with the jobs in it keeps them whichever database it is
        // pointed at: `--db` moves the run log, not the registry
        (Some(app), Some(db)) => Ok(Reach::Local(Box::new(app.db(db)))),
        (Some(app), None) => Ok(Reach::Local(Box::new(app))),
        (None, Some(db)) => Ok(Reach::Store {
            store: Store::at(db)?,
            target: db.clone(),
        }),
        (None, None) => Err(Fail::usage(
            "nothing to reach: --db <path|url> for a run log, or --server <url> for a \
             running instance. a binary with your jobs compiled into it needs neither",
        )),
    }
}

/// what to present to an authenticated server: `--token`, or `HESTAN_TOKEN`.
///
/// the environment is read here rather than by the argument parser, which can
/// do it — because a parser that knows about an environment variable prints
/// its **value** in `--help`, and a secret in a help screen is a secret in
/// whatever collected that help screen. an empty variable is not a token: an
/// unset one and one set to nothing are the same intention.
///
/// prefer the variable to the flag whatever this returns: an argument is
/// visible in `ps` to every account on the machine, for as long as the process
/// runs.
fn token(global: &Global) -> Option<String> {
    global.token.clone().or_else(|| {
        std::env::var("HESTAN_TOKEN")
            .ok()
            .filter(|token| !token.is_empty())
    })
}

impl Reach {
    /// the store in front of this, for the reads that need only rows.
    fn store(self) -> Result<Store, Fail> {
        match self {
            Reach::Local(app) => Ok(app.open()?),
            Reach::Store { store, .. } => Ok(store),
            Reach::Server(_) => unreachable!("a server-mode read goes through the api"),
        }
    }

    /// the registry beside the store, for the reads that need to know what is
    /// defined rather than only what has happened.
    fn inspect(self) -> Result<Inspected, Fail> {
        match self {
            Reach::Local(app) => Ok(app.inspect()?),
            Reach::Store { target, .. } => {
                Err(no_registry(&target, "listing what a deployment defines"))
            }
            Reach::Server(_) => unreachable!("a server-mode read goes through the api"),
        }
    }

    /// a runner over this binary's jobs, for the commands that launch.
    async fn built(self, wait: bool) -> Result<Built, Fail> {
        match self {
            Reach::Local(app) => Ok(app.role(launching_role(wait)).build().await?),
            Reach::Store { target, .. } => Err(no_registry(&target, "launching")),
            Reach::Server(_) => unreachable!("a server-mode launch goes through the api"),
        }
    }
}

/// the one clear line a database gets when it is asked for something only a
/// registry has.
///
/// worth spelling out rather than failing with an error code, because the
/// reason is not obvious from where you are standing: the run log looks like it
/// holds a deployment, and it holds everything about a deployment except the
/// part that is rust.
fn no_registry(target: &str, wanted: &str) -> Fail {
    Fail::new(
        Exit::Unsupported,
        format!(
            "--db {target} opens a run log, which records what ran but holds no job \
             definitions — {wanted} needs the binary they are compiled into, or --server \
             pointed at one that is running"
        ),
    )
}

// ------------------------------------------------------------------- dispatching

async fn dispatch(reach: Reach, command: Command, out: &Out) -> Result<(), Fail> {
    match command {
        Command::Run(args) => launch(reach, args, out).await,

        Command::Runs(args) => {
            let query = runs_query(&args)?;
            let answer = match reach {
                Reach::Server(api) => api.get(&format!("/api/runs?{query}")).await?,
                reach => {
                    let store = reach.store()?;
                    let tag = args.tag.as_deref().map(split_pair).transpose()?;
                    let since = args.since.as_deref().map(instant).transpose()?;
                    json!({
                        "runs": store.runs(
                            args.job.as_deref(),
                            since,
                            None,
                            None,
                            tag.as_ref().map(|(k, v)| (k.as_str(), v.as_str())),
                            args.limit.clamp(1, 2000),
                        )?,
                    })
                }
            };
            render_runs(&answer, out);
            Ok(())
        }

        Command::Show(args) => {
            let answer = match reach {
                Reach::Server(api) => api.get(&format!("/api/runs/{}", args.run)).await?,
                reach => {
                    let store = reach.store()?;
                    let run = run_row(&store, &args.run)?;
                    json!({ "run": run, "ops": store.op_runs(&args.run)? })
                }
            };
            render_show(&answer, out);
            Ok(())
        }

        Command::Logs(args) => logs(reach, args, out).await,

        Command::Cancel(args) => match reach {
            Reach::Server(api) => {
                let answer = api
                    .post(&format!("/api/runs/{}/cancel", args.run), json!({}))
                    .await?;
                out.said(
                    &answer,
                    &format!("cancel {}: {}", args.run, s(&answer, "outcome")),
                );
                Ok(())
            }
            reach => cancel(&reach.store()?, &args.run, out),
        },

        Command::Retry(args) => match reach {
            Reach::Server(api) => {
                let answer = api
                    .post(&format!("/api/runs/{}/retry", args.run), json!({}))
                    .await?;
                out.launched(&s(&answer, "run_id"), "");
                Ok(())
            }
            reach => {
                let built = reach.built(false).await?;
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
        },

        Command::Resume(args) => match reach {
            Reach::Server(api) => {
                let answer = api
                    .post(
                        &format!("/api/runs/{}/resume", args.run),
                        json!({ "from": args.from }),
                    )
                    .await?;
                out.launched(&s(&answer, "run_id"), "");
                Ok(())
            }
            reach => {
                let built = reach.built(false).await?;
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
        },

        Command::Jobs => {
            let answer = match reach {
                Reach::Server(api) => api.get("/api/jobs").await?,
                reach => {
                    let app = reach.inspect()?;
                    let pools: HashMap<&str, usize> =
                        app.pools.iter().map(|(n, l)| (n.as_str(), *l)).collect();
                    let mut jobs: Vec<&Job> = app.jobs.iter().collect();
                    jobs.sort_by_key(|j| j.name());
                    let jobs: Vec<Value> = jobs
                        .iter()
                        .map(|j| {
                            crate::server::job_summary(j, &app.store, |p| pools.get(p).copied())
                        })
                        .collect::<Result<_, _>>()?;
                    json!({ "jobs": jobs })
                }
            };
            render_jobs(&answer, out);
            Ok(())
        }

        Command::Assets => {
            let answer = match reach {
                Reach::Server(api) => api.get("/api/assets").await?,
                Reach::Local(app) => {
                    let app = app.inspect()?;
                    crate::server::assets_json(&app.registry, &app.store)?
                }
                // a database records what was built and cannot say what should
                // have been: the keys a registry fills in are absent rather
                // than guessed at
                Reach::Store { store, .. } => {
                    let built: Vec<Value> = store
                        .latest_materializations()?
                        .into_iter()
                        .map(|m| {
                            json!({
                                "name": m.asset,
                                "fingerprint": m.fingerprint,
                                "built_at": m.built_at,
                                "run_id": m.run_id,
                            })
                        })
                        .collect();
                    json!({ "assets": built })
                }
            };
            render_assets(&answer, out);
            Ok(())
        }

        Command::Build(args) => build(reach, args, out).await,

        Command::Backfill(args) => match reach {
            Reach::Server(api) => {
                let answer = api
                    .post(
                        &format!("/api/assets/{}/backfill", args.asset),
                        json!({ "from": args.from, "to": args.to, "only_missing": !args.all }),
                    )
                    .await?;
                out.said(&answer, &format!("backfill {} started", args.asset));
                Ok(())
            }
            reach => {
                let built = reach.built(false).await?;
                let backfill = crate::backfill::start(
                    &built.runner,
                    &built.registry,
                    &args.asset,
                    &args.from,
                    &args.to,
                    !args.all,
                )
                .map_err(Fail::from)?;
                let answer = json!(backfill);
                out.said(
                    &answer,
                    &format!(
                        "backfill {} of {}: {} partitions",
                        s(&answer, "id"),
                        args.asset,
                        answer["total"]
                    ),
                );
                Ok(())
            }
        },

        Command::Schedules => {
            let answer = match reach {
                Reach::Server(api) => api.get("/api/schedules").await?,
                reach => crate::server::schedules_json(&reach.store()?)?,
            };
            render_schedules(&answer, out);
            Ok(())
        }

        Command::Pause { what } => paused(reach, what, true, out).await,
        Command::Unpause { what } => paused(reach, what, false, out).await,

        Command::Queue => {
            let answer = match reach {
                Reach::Server(api) => api.get("/api/queue").await?,
                Reach::Local(app) => {
                    let app = app.inspect()?;
                    let defined = app.jobs.iter().map(|j| j.name().to_string()).collect();
                    crate::server::queue_json(&app.store, &app.limits, &defined)?
                }
                // the order is a fact about the queue; the blame is a fact
                // about the limits, and this mode has none — see
                // `Store::queue_rows`
                Reach::Store { store, .. } => {
                    let queued: Vec<Value> = store
                        .queue_rows(QUEUE_PAGE)?
                        .into_iter()
                        .enumerate()
                        .map(|(i, run)| json!({ "run": run, "position": i + 1 }))
                        .collect();
                    json!({ "depth": store.queue_depth()?, "queued": queued })
                }
            };
            render_queue(&answer, out);
            Ok(())
        }

        Command::Priority(args) => {
            let answer = match reach {
                Reach::Server(api) => {
                    api.post(
                        &format!("/api/runs/{}/priority", args.run),
                        json!({ "priority": args.priority }),
                    )
                    .await?
                }
                reach => {
                    let store = reach.store()?;
                    if !store.set_run_priority(&args.run, args.priority)? {
                        return Err(Fail::usage(format!("unknown run: {}", args.run)));
                    }
                    json!({ "run_id": args.run, "priority": args.priority })
                }
            };
            out.said(
                &answer,
                &format!("{} moved to priority {}", args.run, args.priority),
            );
            Ok(())
        }

        Command::Events(args) => events(reach, args, out).await,

        Command::Doctor => doctor(reach, out).await,

        Command::Explain(args) => explain(
            reach,
            &args.job,
            args.params.as_deref(),
            args.preset.as_deref(),
            out,
        ),

        // handled before a deployment is reached for
        Command::Completions(_) | Command::Complete { .. } => {
            unreachable!("dispatched by `finish`")
        }

        Command::Serve(args) => match reach {
            Reach::Local(app) => {
                let addr = args.addr.ok_or_else(|| {
                    Fail::usage("--addr is where to serve, since none was compiled in")
                })?;
                app.serve(addr).await.map_err(Fail::from)
            }
            Reach::Store { target, .. } => Err(no_registry(&target, "serving")),
            Reach::Server(_) => Err(Fail::new(
                Exit::Unsupported,
                "--server points at an instance that is already serving; \
                 to start another one, run its own binary",
            )),
        },
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

async fn launch(reach: Reach, args: RunArgs, out: &Out) -> Result<(), Fail> {
    let tags = parse_tags(&args.tags)?;
    // before anything is opened for writing: a dry run resolves exactly the
    // plan a launch would and validates exactly the params a launch would,
    // through the same two calls, and then stops
    if args.dry_run {
        return explain(
            reach,
            &args.job,
            args.params.as_deref(),
            args.preset.as_deref(),
            out,
        );
    }
    let timeout = args.timeout.map(Duration::from_secs);
    let (id, watched) = match reach {
        Reach::Server(api) => {
            let mut body = json!({ "tags": tags, "priority": args.priority });
            match (&args.params, &args.preset) {
                (Some(text), _) => body["params"] = json_arg(text)?,
                (None, Some(preset)) => body["preset"] = json!(preset),
                (None, None) => {}
            }
            let answer = api
                .post(&format!("/api/jobs/{}/runs", args.job), body)
                .await?;
            (s(&answer, "run_id"), Watched::There(api))
        }
        reach => {
            let built = reach.built(args.wait).await?;
            let runner = built.runner;
            let params = match (&args.params, &args.preset) {
                (Some(text), _) => json_arg(text)?,
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
            (id, Watched::Here(runner))
        }
    };
    if !args.wait {
        out.launched(&id, &args.job);
        return Ok(());
    }
    settle(&watched, &id, timeout, out).await
}

async fn build(reach: Reach, args: BuildArgs, out: &Out) -> Result<(), Fail> {
    let timeout = args.timeout.map(Duration::from_secs);
    let (answer, watched) = match reach {
        Reach::Server(api) => {
            let mut body = json!({});
            if !args.partitions.is_empty() {
                body["partitions"] = json!(args.partitions);
            }
            let answer = api
                .post(&format!("/api/assets/{}/build", args.asset), body)
                .await?;
            (answer, Watched::There(api))
        }
        reach => {
            let built = reach.built(args.wait).await?;
            let launched = crate::asset::build_one(
                &built.runner,
                &built.registry,
                &args.asset,
                &args.partitions,
            )
            .map_err(Fail::from)?;
            let answer = match launched {
                Some(id) => json!({ "run_id": id }),
                None => json!({ "up_to_date": true }),
            };
            (answer, Watched::Here(built.runner))
        }
    };
    if answer["up_to_date"] == json!(true) {
        out.said(&answer, &format!("{} is already up to date", args.asset));
        return Ok(());
    }
    let id = s(&answer, "run_id");
    if !args.wait {
        out.launched(&id, &args.asset);
        return Ok(());
    }
    settle(&watched, &id, timeout, out).await
}

/// wait for a run and answer with what it did, which includes the exit code.
async fn settle(
    watched: &Watched,
    id: &str,
    timeout: Option<Duration>,
    out: &Out,
) -> Result<(), Fail> {
    let run = wait(watched, id, timeout, out).await?;
    out.settled(&run);
    match terminal(&run) {
        Some(RunStatus::Success) => Ok(()),
        Some(RunStatus::Canceled) => {
            Err(Fail::new(Exit::Canceled, format!("run {id} was canceled")))
        }
        _ => Err(Fail::new(
            Exit::Failed,
            match run["error"].as_str() {
                Some(error) => error.to_string(),
                None => format!("run {id} failed"),
            },
        )),
    }
}

// --------------------------------------------------------------------- waiting

/// what a wait reads through.
///
/// a run may be executing in this process, in a process across the network, or
/// in one on the same machine that this one only shares a database with. the
/// wait is the same either way and this is the whole of the difference: where
/// the rows come from, and whether there is a dispatcher here to poke.
enum Watched {
    Here(Runner),
    There(Api),
}

impl Watched {
    async fn run(&self, id: &str) -> Result<Value, Fail> {
        match self {
            Watched::Here(runner) => Ok(json!(run_row(runner.store(), id)?)),
            Watched::There(api) => {
                let answer = api.get(&format!("/api/runs/{id}")).await?;
                Ok(answer["run"].clone())
            }
        }
    }

    async fn events(&self, id: &str, after: i64) -> Result<Vec<Value>, Fail> {
        match self {
            Watched::Here(runner) => Ok(runner
                .store()
                .events(id, after)?
                .into_iter()
                .map(|e| json!(e))
                .collect()),
            Watched::There(api) => {
                let answer = api
                    .get(&format!("/api/runs/{id}/events?after={after}"))
                    .await?;
                Ok(list(&answer, "events"))
            }
        }
    }

    async fn logs(&self, id: &str, after: i64) -> Result<Vec<Value>, Fail> {
        match self {
            Watched::Here(runner) => Ok(runner
                .store()
                .op_logs(id, None, after, TAIL_PAGE)?
                .into_iter()
                .map(|l| json!(l))
                .collect()),
            Watched::There(api) => {
                let answer = api
                    .get(&format!(
                        "/api/runs/{id}/logs?after={after}&limit={TAIL_PAGE}"
                    ))
                    .await?;
                Ok(list(&answer, "logs"))
            }
        }
    }

    /// start whatever the queue will let this process start, now. nothing to do
    /// where the run belongs to somebody else.
    fn poke(&self) {
        if let Watched::Here(runner) = self {
            runner.dispatch();
        }
    }

    /// whether the run is executing in this very process, which is the only
    /// case where giving up on it also ends it.
    fn ours(&self, run: &Value) -> bool {
        match self {
            Watched::Here(runner) => run["claimed_by"] == json!(runner.instance()),
            Watched::There(_) => false,
        }
    }
}

/// stream what the run says while it runs, and hand back the row it settled at.
///
/// the ordering is the whole of this function. the status is read *before* the
/// drain that follows it, so a run that finished between two polls has its last
/// lines read after the status that ends the loop rather than before it — which
/// is the race a fast job loses: it can be over before the first poll, and every
/// line it wrote still has to come out. the executor writes a run's terminal
/// event before its terminal status for the same reason, so stopping at the
/// status leaves nothing behind.
async fn wait(
    watched: &Watched,
    id: &str,
    timeout: Option<Duration>,
    out: &Out,
) -> Result<Value, Fail> {
    let deadline = timeout.map(|t| Instant::now() + t);
    let mut tail = Tail::default();
    loop {
        let run = watched.run(id).await?;
        let settled = terminal(&run).is_some();
        tail.drain(watched, id, out).await?;
        if settled {
            return Ok(run);
        }
        if deadline.is_some_and(|d| Instant::now() >= d) {
            // "gave up waiting" and "stopped the run" are different things and
            // only one of them happened — except when the run is executing
            // right here, where exiting is the other one too, and saying so
            // beats leaving it to be discovered
            let ours = match watched.ours(&run) {
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
        watched.poke();
        tokio::time::sleep(TAIL_POLL).await;
    }
}

/// the terminal status of a run, or `None` while it is still going.
fn terminal(run: &Value) -> Option<RunStatus> {
    match run["status"].as_str()?.parse().ok()? {
        RunStatus::Queued | RunStatus::Running => None,
        status => Some(status),
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
    async fn drain(&mut self, watched: &Watched, run: &str, out: &Out) -> Result<(), Fail> {
        loop {
            let events = watched.events(run, self.events).await?;
            let logs = watched.logs(run, self.logs).await?;
            if events.is_empty() && logs.is_empty() {
                return Ok(());
            }
            let full = logs.len() as u32 == TAIL_PAGE;
            let mut lines: Vec<Line> = Vec::with_capacity(events.len() + logs.len());
            for e in &events {
                self.events = self.events.max(e["seq"].as_i64().unwrap_or_default());
                lines.push(Line::event(e));
            }
            for l in &logs {
                self.logs = self.logs.max(l["id"].as_i64().unwrap_or_default());
                lines.push(Line::log(l));
            }
            lines.sort_by(|a, b| a.at.cmp(&b.at));
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
    at: String,
    op: Option<String>,
    level: Option<EventLevel>,
    message: String,
}

impl Line {
    fn event(e: &Value) -> Line {
        Line {
            at: s(e, "ts"),
            op: e["op"].as_str().map(str::to_string),
            level: e["level"].as_str().and_then(|l| l.parse().ok()),
            message: s(e, "message"),
        }
    }

    fn log(l: &Value) -> Line {
        Line {
            at: s(l, "at"),
            op: l["op"].as_str().map(str::to_string),
            level: l["level"].as_str().and_then(|l| l.parse().ok()),
            message: s(l, "message"),
        }
    }
}

// --------------------------------------------------------------------- reading

async fn logs(reach: Reach, args: LogsArgs, out: &Out) -> Result<(), Fail> {
    let limit = args.limit.clamp(1, 100_000);
    let op = args.op.clone().unwrap_or_default();
    let watched = match reach {
        Reach::Server(api) => Watched::There(api),
        reach => {
            let store = reach.store()?;
            run_row(&store, &args.run)?;
            // a runner over no jobs at all, because a log tail only ever
            // reads: `Watched` is the two places a row can come from, and this
            // is the local one
            Watched::Here(Runner::new([], store)?)
        }
    };
    if !args.follow {
        let lines = read_logs(&watched, &args.run, &op, 0, limit).await?;
        if out.json {
            out.object(&json!({ "logs": lines }));
            return Ok(());
        }
        // on stderr, so a pipe still gets exactly the lines and a person still
        // gets the answer: an op that ran in this process and printed is not
        // captured anywhere, and looking at an empty page is how people find
        // that out
        if lines.is_empty() && !out.quiet {
            eprintln!(
                "no captured output: only an isolated op's subprocess and the `capture` \
                 feature's layer write any (docs/logs.md)"
            );
        }
        for line in &lines {
            out.log(&Line::log(line));
        }
        return Ok(());
    }
    let mut after = 0;
    let mut settled = terminal(&watched.run(&args.run).await?).is_some();
    loop {
        // the same ordering `wait` documents: the status first, the drain after
        // it, so the last line of a run that ended mid-poll still prints
        let lines = read_logs(&watched, &args.run, &op, after, TAIL_PAGE).await?;
        for line in &lines {
            after = after.max(line["id"].as_i64().unwrap_or_default());
            match out.json {
                true => out.line(line),
                false => out.log(&Line::log(line)),
            }
        }
        if settled && lines.is_empty() {
            return Ok(());
        }
        if !settled {
            settled = terminal(&watched.run(&args.run).await?).is_some();
        }
        if lines.is_empty() {
            tokio::time::sleep(TAIL_POLL).await;
        }
    }
}

/// one page of captured output, narrowed to one op where one was named.
async fn read_logs(
    watched: &Watched,
    run: &str,
    op: &str,
    after: i64,
    limit: u32,
) -> Result<Vec<Value>, Fail> {
    match watched {
        Watched::Here(runner) => Ok(runner
            .store()
            .op_logs(run, (!op.is_empty()).then_some(op), after, limit)?
            .into_iter()
            .map(|l| json!(l))
            .collect()),
        Watched::There(api) => {
            let answer = api
                .get(&format!(
                    "/api/runs/{run}/logs?op={op}&after={after}&limit={limit}"
                ))
                .await?;
            Ok(list(&answer, "logs"))
        }
    }
}

async fn events(reach: Reach, args: EventsArgs, out: &Out) -> Result<(), Fail> {
    let query = events_query(&args)?;
    let filter = EventQuery {
        kind: args
            .kind
            .as_deref()
            .map(|k| k.parse().unwrap_or_else(|e| match e {})),
        subject_kind: None,
        subject: args.subject.clone(),
        level: match &args.level {
            Some(word) => Some(word.parse().map_err(Fail::usage)?),
            None => None,
        },
        since: args.since.as_deref().map(instant).transpose()?,
        until: None,
        before: None,
    };
    if !args.follow {
        let answer = match reach {
            Reach::Server(api) => api.get(&format!("/api/events?{query}")).await?,
            reach => {
                let events = reach
                    .store()?
                    .event_log(&filter, args.limit.clamp(1, 1000))?;
                json!({ "events": events })
            }
        };
        render_events(&answer, out);
        return Ok(());
    }
    match reach {
        Reach::Server(api) => {
            let from = match args.after {
                Some(seq) => format!("&after={seq}"),
                None => String::new(),
            };
            api.stream(&format!("/api/events/stream?{query}{from}"), |event| {
                show_event(&event, out)
            })
            .await
        }
        reach => follow_events(&reach.store()?, &filter, args.after, out).await,
    }
}

/// the log as it lands, from a cursor.
///
/// **the same rule the sse stream follows**, because it is the same call:
/// [`Store::readable`] decides what is safe to deliver, so a terminal tailing
/// the log and a browser watching it cannot disagree about what has settled.
async fn follow_events(
    store: &Store,
    filter: &EventQuery,
    after: Option<i64>,
    out: &Out,
) -> Result<(), Fail> {
    // where a follower with no cursor starts: now, not the beginning. "show me
    // what happens from here" is what opening a live feed means, and the whole
    // history is one command away
    let mut cursor = match after {
        Some(seq) => seq,
        None => store.event_watermark()?,
    };
    let mut waiting = None;
    loop {
        let step = store.readable(cursor, &mut waiting)?;
        while cursor < step.ceiling {
            let batch = store.event_tail(filter, cursor, Some(step.ceiling), TAIL_PAGE)?;
            if batch.is_empty() {
                cursor = step.ceiling;
                break;
            }
            for event in &batch {
                cursor = event.seq;
                show_event(&json!(event), out);
            }
        }
        if let Some(skip_to) = step.skip_to {
            cursor = cursor.max(skip_to);
        }
        tokio::time::sleep(FOLLOW_POLL).await;
    }
}

fn show_event(event: &Value, out: &Out) {
    if out.json {
        out.line(event);
        return;
    }
    if out.quiet {
        println!("{}", event["seq"]);
        return;
    }
    let level = event["level"].as_str().and_then(|l| l.parse().ok());
    println!(
        "{} {} {} {}",
        out.paint(&stamp(&s(event, "ts")), DIM),
        out.paint(&s(event, "kind"), level_color(level)),
        out.paint(
            event["subject"]
                .as_str()
                .or(event["run_id"].as_str())
                .unwrap_or("-"),
            CYAN
        ),
        s(event, "message"),
    );
}

// -------------------------------------------------------------------- changing

/// pause or unpause a schedule or a sensor, in whichever mode is in front of us.
///
/// a job may have several schedules, so naming one without an expression means
/// all of them: pausing "the nightly job" is what somebody means, and asking
/// them to type its cron back at it is not an improvement.
async fn paused(reach: Reach, what: What, paused: bool, out: &Out) -> Result<(), Fail> {
    let word = match paused {
        true => "paused",
        false => "unpaused",
    };
    match (what, reach) {
        (What::Sensor { name }, Reach::Server(api)) => {
            let answer = api
                .post(
                    "/api/sensors/state",
                    json!({ "name": name, "paused": paused }),
                )
                .await?;
            out.said(&answer, &format!("sensor {name} {word}"));
            Ok(())
        }
        (What::Sensor { name }, reach) => {
            let store = reach.store()?;
            // no actor: see the schedule arm below
            if !store.set_sensor_paused(&name, paused, None)? {
                return Err(Fail::usage(format!("unknown sensor: {name}")));
            }
            out.said(&json!({ "ok": true }), &format!("sensor {name} {word}"));
            Ok(())
        }
        (What::Schedule { job, expr }, reach) => {
            let matched = match &reach {
                Reach::Server(api) => list(&api.get("/api/schedules").await?, "schedules"),
                _ => list(
                    &crate::server::schedules_json(&stored(&reach)?)?,
                    "schedules",
                ),
            };
            let matched: Vec<Value> = matched
                .into_iter()
                .filter(|s| s["job"] == json!(job))
                .filter(|s| expr.as_ref().is_none_or(|e| s["expr"] == json!(e)))
                .collect();
            if matched.is_empty() {
                return Err(Fail::usage(match &expr {
                    Some(expr) => format!("no schedule {expr:?} on job {job}"),
                    None => format!("no schedules on job {job}"),
                }));
            }
            for row in &matched {
                let body = json!({ "job": job, "expr": s(row, "expr"), "paused": paused });
                match &reach {
                    Reach::Server(api) => {
                        api.post("/api/schedules/state", body).await?;
                    }
                    // no actor: a command line against a database has nobody
                    // to name. whoever ran it is a fact about a shell, not an
                    // identity anything checked — and a name nothing checked
                    // is worse in an audit trail than no name at all
                    _ => {
                        stored(&reach)?.set_schedule_paused(&job, &s(row, "expr"), paused, None)?;
                    }
                }
            }
            let exprs: Vec<String> = matched.iter().map(|r| s(r, "expr")).collect();
            out.said(
                &json!({ "ok": true, "job": job, "exprs": exprs }),
                &format!("{job} {} {word}", exprs.join(", ")),
            );
            Ok(())
        }
    }
}

/// the store behind a reach that is not a server, without consuming it.
fn stored(reach: &Reach) -> Result<Store, Fail> {
    match reach {
        Reach::Local(app) => Ok(app.open()?),
        Reach::Store { store, .. } => Ok(store.clone()),
        Reach::Server(_) => unreachable!("a server-mode write goes through the api"),
    }
}

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
            // no actor, for the reason `pause` gives above
            if !store.cancel_queued(id, None)? {
                return Err(Fail::new(
                    Exit::Failed,
                    format!("run {id} was claimed while this was taking it off the queue"),
                ));
            }
            out.said(
                &json!({ "run_id": id, "outcome": "canceled" }),
                &format!("canceled {id}, which had not started"),
            );
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

// ------------------------------------------------------------------ the api

/// a running instance, reached over the http api it already serves.
///
/// there is no second protocol here and no new endpoint: everything the command
/// line asks for over the network is something the ui already asks for, which
/// is why `--server` works against an instance that predates this.
struct Api {
    base: String,
    client: reqwest::Client,
    /// what this presents, if anything. never printed: it reaches the
    /// `Authorization` header and nothing else, and no error below quotes it.
    token: Option<String>,
}

impl Api {
    fn new(url: &str, token: Option<String>) -> Api {
        Api {
            base: url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
            token,
        }
    }

    async fn get(&self, path: &str) -> Result<Value, Fail> {
        self.answer(self.request(self.client.get(self.url(path))))
            .await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value, Fail> {
        self.answer(self.request(self.client.post(self.url(path)).json(&body)))
            .await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// the credential goes on every request rather than on the ones that were
    /// refused last time: a retry after a 401 is a second request in the log of
    /// whatever is in front of the deployment, and reads need it too.
    fn request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(token) => request.bearer_auth(token),
            None => request,
        }
    }

    async fn answer(&self, request: reqwest::RequestBuilder) -> Result<Value, Fail> {
        let response = request.send().await.map_err(|e| self.out_of_reach(&e))?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        match status.is_success() {
            true => Ok(body),
            false => Err(self.refused(status, &body)),
        }
    }

    /// what the server said no with, in this command line's vocabulary.
    fn refused(&self, status: reqwest::StatusCode, body: &Value) -> Fail {
        let message = match body["error"].as_str() {
            Some(error) => error.to_string(),
            None => format!("{} said {status}", self.base),
        };
        // the api's own vocabulary, kept: what it calls a bad request is what
        // this calls a usage error, and a script switching between `--server`
        // and the binary itself should not have to learn two tables
        let code = match status.as_u16() {
            400 | 404 | 422 => Exit::Usage,
            401 | 403 => Exit::Denied,
            502..=504 => Exit::Unreachable,
            _ => Exit::Failed,
        };
        // what to do about it, which the server cannot know: it has no idea
        // whether anything was sent or where it would have come from
        let message = match (status.as_u16(), self.token.is_some()) {
            (401, false) => format!(
                "{message} — {} is authenticated: pass --token, or set HESTAN_TOKEN, which \
                 keeps it out of ps",
                self.base
            ),
            (401, true) => format!("{message} — {} refused this token", self.base),
            _ => message,
        };
        Fail::new(code, message)
    }

    /// the server-sent event stream, one parsed `data:` payload at a time.
    ///
    /// hand-parsed rather than through a client library because the whole of
    /// the format that matters here is two field names — and a dependency for
    /// that would be a dependency in every build that turns this feature on.
    async fn stream(&self, path: &str, mut each: impl FnMut(Value)) -> Result<(), Fail> {
        let mut response = self
            .request(self.client.get(self.url(path)))
            .send()
            .await
            .map_err(|e| self.out_of_reach(&e))?;
        // a stream that never opened says why in the same words a request that
        // was refused does — a follow that was not authenticated is not a
        // network that was not there
        let status = response.status();
        if !status.is_success() {
            let body = response.json().await.unwrap_or(Value::Null);
            return Err(self.refused(status, &body));
        }
        let mut buffer = String::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| self.out_of_reach(&e))? {
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            // one message ends at a blank line; anything after the last one is
            // a message still arriving and stays in the buffer
            while let Some(end) = buffer.find("\n\n") {
                let message: String = buffer.drain(..end + 2).collect();
                for line in message.lines() {
                    if let Some(payload) = line.strip_prefix("data:")
                        && let Ok(value) = serde_json::from_str(payload.trim())
                    {
                        each(value);
                    }
                }
            }
        }
        Ok(())
    }

    fn out_of_reach(&self, e: &reqwest::Error) -> Fail {
        Fail::new(
            Exit::Unreachable,
            format!("could not reach {}: {e}", self.base),
        )
    }
}

// ------------------------------------------------------------------ rendering

// every command answers with one json object shaped exactly as the http api
// shapes it, whichever of the three modes produced it, and the tables below are
// renderings of that object rather than a second thing to keep in step with it.
// so `--json` means the same thing pointed at your own binary as it does
// pointed at a server, and a script does not have to care which it got.
//
// where a mode genuinely knows less — a run log has no registry — the keys it
// cannot fill are **absent** rather than null or invented, and the table drops
// the columns that would have shown them.

fn render_runs(answer: &Value, out: &Out) {
    let runs = list(answer, "runs");
    if out.json {
        out.object(answer);
        return;
    }
    if out.quiet {
        for run in &runs {
            println!("{}", s(run, "id"));
        }
        return;
    }
    let mut table = Table::new(["RUN", "JOB", "STATUS", "TRIGGER", "STARTED", "TOOK"]);
    for run in &runs {
        let status = status_of(run);
        table.row([
            Cell::plain(s(run, "id")),
            Cell::plain(s(run, "job")),
            Cell::styled(s(run, "status"), status_color(status)),
            Cell::plain(s(run, "trigger")),
            Cell::plain(stamp(
                run["started_at"].as_str().unwrap_or(&s(run, "created_at")),
            )),
            Cell::plain(took(run)),
        ]);
    }
    table.print(out, "no runs");
}

fn render_show(answer: &Value, out: &Out) {
    if out.json {
        out.object(answer);
        return;
    }
    let run = &answer["run"];
    if out.quiet {
        println!("{}", s(run, "id"));
        return;
    }
    println!(
        "{} {}  {}",
        out.paint(&s(run, "job"), BOLD),
        s(run, "id"),
        out.paint(&s(run, "status"), status_color(status_of(run)))
    );
    println!("trigger  {}", s(run, "trigger"));
    println!("created  {}", s(run, "created_at"));
    if let Some(started) = run["started_at"].as_str() {
        println!("started  {started}");
    }
    if let Some(finished) = run["finished_at"].as_str() {
        println!("ended    {finished}  ({})", took(run));
    }
    if run["params"] != json!({}) {
        println!("params   {}", run["params"]);
    }
    if let Some(tags) = run["tags"].as_object().filter(|t| !t.is_empty()) {
        let tags: Vec<String> = tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
        println!("tags     {}", tags.join(" "));
    }
    if let Some(error) = run["error"].as_str() {
        println!("error    {}", out.paint(error, RED));
    }
    println!();
    // in the order they ran, not the order they are stored in: what a person
    // reading a run wants first is where it got to
    let mut ops = list(answer, "ops");
    ops.sort_by_key(|op| op["started_at"].as_str().unwrap_or("~").to_string());
    let mut table = Table::new(["OP", "STATUS", "ATTEMPTS", "TOOK", "ERROR"]);
    for op in &ops {
        table.row([
            Cell::plain(s(op, "op")),
            Cell::styled(s(op, "status"), op_color(&s(op, "status"))),
            Cell::plain(op["attempts"].to_string()),
            Cell::plain(elapsed(
                op["started_at"].as_str(),
                op["finished_at"].as_str(),
            )),
            Cell::plain(op["error"].as_str().unwrap_or_default()),
        ]);
    }
    table.print(out, "no ops recorded");
}

fn render_jobs(answer: &Value, out: &Out) {
    let jobs = list(answer, "jobs");
    if out.json {
        out.object(answer);
        return;
    }
    if out.quiet {
        for job in &jobs {
            println!("{}", s(job, "name"));
        }
        return;
    }
    let mut table = Table::new(["JOB", "OPS", "SCHEDULE", "LAST RUN", "DESCRIPTION"]);
    for job in &jobs {
        let schedules: Vec<String> = list(job, "schedules")
            .iter()
            .map(|s_| match s_["paused"] == json!(true) {
                true => format!("{} (paused)", s(s_, "expr")),
                false => s(s_, "expr"),
            })
            .collect();
        let last = &job["last_run"];
        table.row([
            Cell::plain(s(job, "name")),
            Cell::plain(list(job, "ops").len().to_string()),
            Cell::plain(schedules.join(", ")),
            match last.is_object() {
                true => Cell::styled(s(last, "status"), status_color(status_of(last))),
                false => Cell::plain("-"),
            },
            Cell::plain(job["description"].as_str().unwrap_or_default()),
        ]);
    }
    table.print(out, "no jobs");
}

fn render_assets(answer: &Value, out: &Out) {
    let assets = list(answer, "assets");
    if out.json {
        out.object(answer);
        return;
    }
    if out.quiet {
        for asset in &assets {
            println!("{}", s(asset, "name"));
        }
        return;
    }
    // "stale" is a claim about the registry, so the column only exists where
    // one was there to make it
    let known = assets.iter().any(|a| a.get("stale").is_some());
    let mut table = Table::new(["ASSET", "STATE", "BUILT", "DEPS"]);
    for asset in &assets {
        let state = match (known, asset["stale"] == json!(true)) {
            (false, _) => Cell::plain("-"),
            (true, true) => Cell::styled("stale", YELLOW),
            (true, false) => Cell::styled("fresh", GREEN),
        };
        table.row([
            Cell::plain(s(asset, "name")),
            state,
            Cell::plain(match asset["built_at"].as_str() {
                Some(at) => when(at),
                None => "never".into(),
            }),
            Cell::plain(
                list(asset, "deps")
                    .iter()
                    .map(|d| d.as_str().unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        ]);
    }
    table.print(out, "no assets");
}

fn render_schedules(answer: &Value, out: &Out) {
    let schedules = list(answer, "schedules");
    if out.json {
        out.object(answer);
        return;
    }
    if out.quiet {
        for row in &schedules {
            println!("{}", s(row, "job"));
        }
        return;
    }
    let mut table = Table::new(["JOB", "CRON", "TZ", "STATE", "NEXT FIRE"]);
    for row in &schedules {
        let paused = row["paused"] == json!(true);
        table.row([
            Cell::plain(s(row, "job")),
            Cell::plain(s(row, "expr")),
            Cell::plain(s(row, "tz")),
            match paused {
                true => Cell::styled("paused", YELLOW),
                false => Cell::styled("active", GREEN),
            },
            Cell::plain(match (paused, row["next_fire"].as_str()) {
                (false, Some(at)) => when(at),
                _ => "-".into(),
            }),
        ]);
    }
    table.print(out, "no schedules");
}

fn render_queue(answer: &Value, out: &Out) {
    let queued = list(answer, "queued");
    if out.json {
        out.object(answer);
        return;
    }
    if out.quiet {
        for entry in &queued {
            println!("{}", s(&entry["run"], "id"));
        }
        return;
    }
    // the reason a run is waiting belongs to whoever owns the limits, so the
    // column is here only when the answer came from something that does
    let blamed = queued.iter().any(|q| q.get("blocked_by").is_some());
    let mut table = match blamed {
        true => Table::new(["#", "RUN", "JOB", "PRIORITY", "WAITING FOR"]),
        false => Table::new(["#", "RUN", "JOB", "PRIORITY", "QUEUED"]),
    };
    for entry in &queued {
        let run = &entry["run"];
        table.row([
            Cell::plain(entry["position"].to_string()),
            Cell::plain(s(run, "id")),
            Cell::plain(s(run, "job")),
            Cell::plain(run["priority"].to_string()),
            match blamed {
                true => Cell::plain(s(&entry["blocked_by"], "reason")),
                false => Cell::plain(stamp(&s(run, "created_at"))),
            },
        ]);
    }
    table.print(out, "the queue is empty");
    println!("{} waiting", answer["depth"]);
}

fn render_events(answer: &Value, out: &Out) {
    let events = list(answer, "events");
    if out.json {
        out.object(answer);
        return;
    }
    // newest first is how the log is read and oldest first is how it is
    // followed; a page printed newest-last would be a different order from the
    // same command with --follow
    for event in events.iter().rev() {
        show_event(event, out);
    }
}

// --------------------------------------------------------------------- helpers

fn run_row(store: &Store, id: &str) -> Result<Run, Fail> {
    store
        .run(id)?
        .ok_or_else(|| Fail::usage(format!("unknown run: {id}")))
}

/// a string field, or `""` where there is none. every renderer below reads its
/// answer this way: a missing key is a mode that does not know, and printing
/// nothing is what not knowing looks like.
fn s(value: &Value, key: &str) -> String {
    value[key].as_str().unwrap_or_default().to_string()
}

/// an array field, or an empty one.
fn list(value: &Value, key: &str) -> Vec<Value> {
    value[key].as_array().cloned().unwrap_or_default()
}

fn status_of(run: &Value) -> RunStatus {
    run["status"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(RunStatus::Queued)
}

/// one json object from the command line, with the parse error attached to the
/// flag that carried it.
fn json_arg(text: &str) -> Result<Value, Fail> {
    serde_json::from_str(text).map_err(|e| Fail::usage(format!("--params is one json object: {e}")))
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

/// the query string `GET /api/runs` takes, from the same flags the store read
/// uses — so the two modes are filtering on the same thing.
fn runs_query(args: &RunsArgs) -> Result<String, Fail> {
    let mut query = vec![format!("limit={}", args.limit)];
    if let Some(job) = &args.job {
        query.push(format!("job={}", escape(job)));
    }
    if let Some(tag) = &args.tag {
        split_pair(tag)?;
        query.push(format!("tag={}", escape(tag)));
    }
    if let Some(since) = &args.since {
        query.push(format!("since={}", escape(&instant(since)?.to_rfc3339())));
    }
    Ok(query.join("&"))
}

fn events_query(args: &EventsArgs) -> Result<String, Fail> {
    let mut query = vec![format!("limit={}", args.limit)];
    for (name, value) in [
        ("kind", &args.kind),
        ("subject", &args.subject),
        ("level", &args.level),
    ] {
        if let Some(value) = value {
            query.push(format!("{name}={}", escape(value)));
        }
    }
    if let Some(since) = &args.since {
        query.push(format!("since={}", escape(&instant(since)?.to_rfc3339())));
    }
    Ok(query.join("&"))
}

/// percent-encoding for the handful of characters a filter can carry that a
/// query string cannot: an rfc3339 `+`, a tag's `=`, a space in a name.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
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

/// the clock time out of an rfc3339 stamp, or the stamp itself if it is not one.
fn stamp(at: &str) -> String {
    match DateTime::parse_from_rfc3339(at) {
        Ok(t) => t.with_timezone(&Utc).format("%H:%M:%S").to_string(),
        Err(_) => at.to_string(),
    }
}

/// the date and the clock, for a column where "yesterday" and "an hour ago"
/// are different answers. [`stamp`] is the one for a run you are watching.
fn when(at: &str) -> String {
    match DateTime::parse_from_rfc3339(at) {
        Ok(t) => t.with_timezone(&Utc).format("%Y-%m-%d %H:%M").to_string(),
        Err(_) => at.to_string(),
    }
}

fn took(run: &Value) -> String {
    elapsed(run["started_at"].as_str(), run["finished_at"].as_str())
}

fn elapsed(from: Option<&str>, to: Option<&str>) -> String {
    let parse = |t: &str| DateTime::parse_from_rfc3339(t).ok();
    match (from.and_then(parse), to.and_then(parse)) {
        (Some(a), Some(b)) => secs((b - a).num_milliseconds()),
        _ => "-".into(),
    }
}

fn secs(ms: i64) -> String {
    format!("{:.1}s", ms as f64 / 1000.0)
}

// ---------------------------------------------------------------------- doctor

/// how much free space is little enough to say something about.
///
/// a ratio rather than a size, because "500mb left" means nothing without
/// knowing whether that is 90% of the disk or 0.4% of it — and because the
/// thing that fills a disk is a run log growing at whatever rate this
/// deployment writes.
const DISK_LOW: f64 = 0.10;

/// what one check found.
///
/// three levels rather than two, and the middle one earns its place: a paused
/// schedule is the answer to "why is nothing running" and is also something
/// somebody chose on purpose. reporting it as an error would make `doctor`
/// exit non-zero forever in a deployment that is exactly as it was meant to
/// be, and a check nobody can satisfy is a check everybody learns to ignore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Level {
    Ok,
    Note,
    Wrong,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Ok => "ok",
            Level::Note => "note",
            Level::Wrong => "wrong",
        }
    }

    fn color(self) -> &'static str {
        match self {
            Level::Ok => GREEN,
            Level::Note => YELLOW,
            Level::Wrong => RED,
        }
    }
}

struct Finding {
    level: Level,
    check: &'static str,
    says: String,
    /// what to do about it. only ever on something actionable — a fix beside
    /// an `ok` would be advice about nothing.
    fix: Option<String>,
}

impl Finding {
    fn ok(check: &'static str, says: impl Into<String>) -> Finding {
        Finding {
            level: Level::Ok,
            check,
            says: says.into(),
            fix: None,
        }
    }

    fn note(check: &'static str, says: impl Into<String>, fix: impl Into<String>) -> Finding {
        Finding {
            level: Level::Note,
            check,
            says: says.into(),
            fix: Some(fix.into()),
        }
    }

    fn wrong(check: &'static str, says: impl Into<String>, fix: impl Into<String>) -> Finding {
        Finding {
            level: Level::Wrong,
            check,
            says: says.into(),
            fix: Some(fix.into()),
        }
    }

    fn json(&self) -> Value {
        json!({
            "level": self.level.as_str(),
            "check": self.check,
            "says": self.says,
            "fix": self.fix,
        })
    }
}

/// one command that answers "why is nothing running".
///
/// **every check here looks at something.** a check that cannot see what it is
/// about does not report that everything is fine — it is not run at all, and
/// the checks a mode could not make are listed at the end under their own
/// heading. an `ok` line means something was read and was as it should be,
/// which is the only thing that makes the other lines worth believing.
async fn doctor(reach: Reach, out: &Out) -> Result<(), Fail> {
    let (app, store) = match reach {
        Reach::Local(app) => {
            let app = app.inspect()?;
            (Some(app), None)
        }
        Reach::Store { store, target } => (None, Some((store, target))),
        // over http there is exactly one question worth asking and exactly one
        // endpoint that answers it without credentials, so this reports that
        // and says plainly that it saw nothing else
        Reach::Server(api) => return remote_doctor(&api, out).await,
    };
    let (store, target) = match (&app, &store) {
        (Some(app), _) => (&app.store, app.db.clone()),
        (_, Some((store, target))) => (store, target.clone()),
        _ => unreachable!("one of the two is always there"),
    };

    let mut findings = Vec::new();
    let mut unchecked: Vec<&str> = Vec::new();
    findings.push(Finding::ok(
        "store",
        format!(
            "{} at {target}, schema v{}",
            store.backend(),
            store.schema_version()?
        ),
    ));
    findings.extend(check_schedules(store)?);
    findings.extend(check_sensors(store)?);
    findings.extend(check_leases(store, Utc::now())?);
    match &app {
        Some(app) => {
            findings.extend(check_queue(app)?);
            findings.extend(check_retention(app));
            findings.push(check_auth(app.auth.as_ref()));
        }
        None => unchecked.push(
            "the queue, the retention policy and whether anything checks who is asking, \
             which are read off limits, a role and an authenticator that only the \
             deployment's own binary carries",
        ),
    }
    match disk_free(&target) {
        Some((free, total)) => findings.push(check_disk(&target, free, total)),
        None => unchecked.push("free disk space, which is a question about a local file"),
    }

    let wrong = findings.iter().any(|f| f.level == Level::Wrong);
    if out.json {
        out.object(&json!({
            "ok": !wrong,
            "findings": findings.iter().map(Finding::json).collect::<Vec<_>>(),
            "unchecked": unchecked,
        }));
    } else if out.quiet {
        for finding in findings.iter().filter(|f| f.level != Level::Ok) {
            println!("{} {}", finding.level.as_str(), finding.says);
        }
    } else {
        for finding in &findings {
            println!(
                "{:<5} {:<10} {}",
                out.paint(finding.level.as_str(), finding.level.color()),
                finding.check,
                finding.says
            );
            if let Some(fix) = &finding.fix {
                println!("      {:<10} {}", "", out.paint(fix, DIM));
            }
        }
        for missed in &unchecked {
            println!("{:<5} {:<10} {missed}", "-", "not checked");
        }
    }
    match wrong {
        true => Err(Fail::new(Exit::Actionable, "something above is actionable")),
        false => Ok(()),
    }
}

/// every cron in the table, parsed the way the scheduler parses it.
///
/// the rows outlive the code that wrote them — a process syncs them at boot and
/// a database can hold rows from a deployment that has since changed — so an
/// expression or a timezone that no longer resolves is a schedule that silently
/// never fires again. that is what this looks for, by parsing every one.
fn check_schedules(store: &Store) -> Result<Vec<Finding>, Fail> {
    let rows = store.schedules()?;
    if rows.is_empty() {
        return Ok(vec![Finding::ok("schedules", "none defined")]);
    }
    let mut findings = Vec::new();
    let mut parsed = 0;
    for row in &rows {
        match crate::schedule::parse(&row.job, &row.expr, &row.tz) {
            Ok(_) => parsed += 1,
            Err(e) => findings.push(Finding::wrong(
                "schedules",
                format!("{} {:?} will never fire: {e}", row.job, row.expr),
                "fix the expression or the timezone where the schedule is declared, \
                 then restart so the table is synced",
            )),
        }
    }
    let paused: Vec<&str> = rows
        .iter()
        .filter(|r| r.paused)
        .map(|r| r.job.as_str())
        .collect();
    if parsed > 0 {
        findings.insert(
            0,
            Finding::ok("schedules", format!("{parsed} of {} parse", rows.len())),
        );
    }
    if !paused.is_empty() {
        findings.push(Finding::note(
            "schedules",
            format!("paused, so they will not fire: {}", paused.join(", ")),
            format!("unpause schedule {}", paused[0]),
        ));
    }
    Ok(findings)
}

fn check_sensors(store: &Store) -> Result<Vec<Finding>, Fail> {
    let rows = store.sensors()?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let paused: Vec<&str> = rows
        .iter()
        .filter(|r| r.paused)
        .map(|r| r.name.as_str())
        .collect();
    if paused.is_empty() {
        return Ok(vec![Finding::ok(
            "sensors",
            format!("{} defined, none paused", rows.len()),
        )]);
    }
    Ok(vec![Finding::note(
        "sensors",
        format!("paused, so they evaluate nothing: {}", paused.join(", ")),
        format!("unpause sensor {}", paused[0]),
    )])
}

/// runs held by a claimer that stopped renewing.
fn check_leases(store: &Store, now: DateTime<Utc>) -> Result<Vec<Finding>, Fail> {
    let stalled = store.stalled_claims(now)?;
    if stalled.is_empty() {
        return Ok(vec![Finding::ok("leases", "every claim is current")]);
    }
    let oldest = &stalled[0];
    Ok(vec![Finding::wrong(
        "leases",
        format!(
            "{} run(s) held past their lease by processes that stopped renewing it, \
             the oldest {} claimed by {}",
            stalled.len(),
            oldest.id,
            oldest.claimed_by.as_deref().unwrap_or("(unknown)")
        ),
        "any hestan process runs the lease loop that reclaims these — start one, \
         and they are failed or requeued as `reclaim` says",
    )])
}

/// what is waiting, and whether anything is going to take it.
///
/// the second half is the one worth having. a run that no limit is holding back
/// and that nobody has claimed is a run waiting for a process that executes,
/// and a deployment where every process was started as a scheduler has exactly
/// that and no other symptom.
fn check_queue(app: &Inspected) -> Result<Vec<Finding>, Fail> {
    let defined = app.jobs.iter().map(|j| j.name().to_string()).collect();
    let answer = crate::server::queue_json(&app.store, &app.limits, &defined)?;
    let queued = list(&answer, "queued");
    if queued.is_empty() {
        return Ok(vec![Finding::ok("queue", "nothing waiting")]);
    }
    let (blocked, free): (Vec<&Value>, Vec<&Value>) = queued
        .iter()
        .partition(|q| q["blocked_by"].is_object() || q.get("blocked_by").is_none());
    let mut findings = Vec::new();
    if !free.is_empty() {
        findings.push(Finding::wrong(
            "queue",
            format!(
                "{} run(s) are queued with nothing holding them back, so no process is \
                 taking them off the queue",
                free.len()
            ),
            "start a process whose role executes — `serve` with the default role, or \
             `work` — against this database",
        ));
    }
    if !blocked.is_empty() {
        let reason = s(&blocked[0]["blocked_by"], "reason");
        findings.push(Finding::note(
            "queue",
            format!("{} run(s) are waiting on a limit: {reason}", blocked.len()),
            "raise the limit, or wait for what is executing to finish",
        ));
    }
    Ok(findings)
}

/// a retention policy in a process that will never run it.
///
/// sweeping is a decision, so only a role that decides does it. a deployment
/// where the process carrying the policy is a worker has a policy that has
/// never deleted anything and never will, and the only symptom is a database
/// that keeps growing.
fn check_retention(app: &Inspected) -> Vec<Finding> {
    if app.retention == Retention::default() {
        return vec![Finding::ok("retention", "no policy: nothing is deleted")];
    }
    if app.role.decides() {
        return vec![Finding::ok(
            "retention",
            format!("a policy, and this role ({}) sweeps", app.role),
        )];
    }
    vec![Finding::wrong(
        "retention",
        format!(
            "a retention policy is configured but this process is a {}, and only a role \
             that decides sweeps — nothing here will ever delete anything",
            app.role
        ),
        "give the policy to the process that owns the schedules, which is the one \
         running under the scheduler or the default role",
    )]
}

/// whether anything checks who is asking.
///
/// not an error either way: a deployment on loopback is a deployment on one
/// machine, and the refusal in `serve` already makes that the only thing it can
/// be. what this is for is the deployment somebody is about to move — the
/// answer to "is the thing I am about to put an address on guarded" should not
/// be "read the source".
fn check_auth(auth: Option<&Auth>) -> Finding {
    match auth {
        Some(Auth::Bearer(_)) => Finding::ok("auth", "one bearer token, and it is an admin"),
        Some(Auth::Custom(_)) => Finding::ok("auth", "an authenticator of your own"),
        Some(Auth::None) => Finding::note(
            "auth",
            "Auth::None: nothing here checks who is asking, deliberately",
            "make sure what is in front of this still checks identity — that is what \
             Auth::None asserts",
        ),
        None => Finding::note(
            "auth",
            "nothing checks who is asking, so serve will only bind loopback",
            "Hestan::auth(Auth::bearer(…)) before giving this an address anyone can reach",
        ),
    }
}

/// what a deployment across the network can be asked without credentials.
///
/// one finding and a long list of things this could not see. that list is the
/// point: a doctor that answered "everything looks fine" having read one
/// endpoint would be worse than one that refused to run at all, which is what
/// this used to do.
async fn remote_doctor(api: &Api, out: &Out) -> Result<(), Fail> {
    let asked = api.get("/api/whoami").await?;
    let authenticated = asked["auth"].as_bool().unwrap_or(false);
    let who = asked["identity"]["name"].as_str();
    let finding = match (authenticated, who) {
        (true, Some(name)) => Finding::ok(
            "auth",
            format!(
                "it checks who is asking, and you are {name} ({})",
                asked["identity"]["role"].as_str().unwrap_or("?")
            ),
        ),
        (true, None) => Finding::note(
            "auth",
            "it checks who is asking, and does not know you",
            "pass --token, or set HESTAN_TOKEN",
        ),
        (false, _) => Finding::note(
            "auth",
            "it checks nobody: anyone who can reach this address can launch runs on it",
            "give it Hestan::auth(Auth::bearer(…)), or keep it on loopback",
        ),
    };
    let unchecked = [
        "the store, the schedules, the sensors, the leases, the queue, the retention \
         policy and the disk, which an http api exposes none of — point --db at the \
         database, or run doctor in the deployment's own binary",
    ];
    if out.json {
        out.object(&json!({
            "ok": true,
            "findings": [finding.json()],
            "unchecked": unchecked,
        }));
    } else if out.quiet {
        if finding.level != Level::Ok {
            println!("{} {}", finding.level.as_str(), finding.says);
        }
    } else {
        println!(
            "{:<5} {:<10} {}",
            out.paint(finding.level.as_str(), finding.level.color()),
            finding.check,
            finding.says
        );
        if let Some(fix) = &finding.fix {
            println!("      {:<10} {}", "", out.paint(fix, DIM));
        }
        for missed in unchecked {
            println!("{:<5} {:<10} {missed}", "-", "not checked");
        }
    }
    Ok(())
}

/// free space where the run log lives.
fn check_disk(target: &str, free: u64, total: u64) -> Finding {
    let left = free as f64 / total.max(1) as f64;
    let says = format!(
        "{} free of {} where {target} lives ({:.0}%)",
        bytes(free),
        bytes(total),
        left * 100.0
    );
    match left < DISK_LOW {
        true => Finding {
            level: Level::Wrong,
            check: "disk",
            says,
            fix: Some(
                "a run log that cannot be written to stops the deployment: free space, \
                 or set a retention policy so it stops growing"
                    .into(),
            ),
        },
        false => Finding::ok("disk", says),
    }
}

/// free and total bytes on the filesystem holding `target`, or `None` where the
/// question does not apply — a `postgres://` url is a server's disk and not
/// this machine's, and saying nothing beats reporting the wrong one.
#[cfg(unix)]
fn disk_free(target: &str) -> Option<(u64, u64)> {
    if target.contains("://") || target == ":memory:" {
        return None;
    }
    let dir = std::path::Path::new(target)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let path = std::ffi::CString::new(dir.as_os_str().as_encoded_bytes()).ok()?;
    // SAFETY: `path` is a valid nul-terminated string and `stats` is written
    // only on success, which is what the return value reports
    let stats = unsafe {
        let mut stats: libc::statvfs = std::mem::zeroed();
        (libc::statvfs(path.as_ptr(), &mut stats) == 0).then_some(stats)?
    };
    let unit = stats.f_frsize as u64;
    Some((stats.f_bavail as u64 * unit, stats.f_blocks as u64 * unit))
}

#[cfg(not(unix))]
fn disk_free(_target: &str) -> Option<(u64, u64)> {
    None
}

fn bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["b", "kb", "mb", "gb", "tb"];
    let mut size = n as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    match unit {
        0 => format!("{n}b"),
        _ => format!("{size:.1}{}", UNITS[unit]),
    }
}

// --------------------------------------------------------------------- explain

/// the plan, without running it.
///
/// this is the command the mount pays for. the dag, what is parallel, which
/// pools gate it and where isolation applies are all properties of the ops
/// themselves — so answering takes a registry, and the registry is compiled
/// into the binary this ran from. nothing is loaded and nothing is asked.
fn explain(
    reach: Reach,
    job: &str,
    params: Option<&str>,
    preset: Option<&str>,
    out: &Out,
) -> Result<(), Fail> {
    let app = match reach {
        Reach::Local(app) => app.inspect()?,
        Reach::Store { target, .. } => return Err(no_registry(&target, "explaining a plan")),
        Reach::Server(_) => {
            return Err(Fail::new(
                Exit::Unsupported,
                "a plan is a property of the job definitions, which live in the binary \
                 they were compiled into — run explain there",
            ));
        }
    };
    let job = app
        .jobs
        .iter()
        .find(|j| j.name() == job)
        .ok_or_else(|| Fail::usage(format!("unknown job: {job}")))?;

    // the params first, because a plan that could not launch is not a plan.
    // exactly the check a launch runs, so a dry run that passes here and a
    // launch that fails there cannot happen
    let params: Value = match (params, preset) {
        (Some(text), _) => json_arg(text)?,
        (None, Some(preset)) => {
            app.store
                .preset(job.name(), preset)?
                .ok_or_else(|| {
                    Fail::usage(format!("unknown preset: {preset} on job {}", job.name()))
                })?
                .params
        }
        (None, None) => json!({}),
    };
    if let Some((op, reason)) = job.params_error(&params) {
        return Err(Fail::from(Error::InvalidParams { op, reason }));
    }

    let stages = stages(job);
    let pools: Vec<Value> = job
        .ops()
        .iter()
        .filter_map(|op| op.pool_name())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|name| {
            let limit = app.pools.iter().find(|(p, _)| p == name).map(|(_, l)| *l);
            json!({ "name": name, "limit": limit })
        })
        .collect();
    let answer = json!({
        "job": job.name(),
        "description": job.description(),
        "params": params,
        "max_parallel": job.max_parallel(),
        "pools": pools,
        "stages": stages.iter().enumerate().map(|(i, stage)| json!({
            "stage": i + 1,
            "ops": stage.iter().map(|op| json!({
                "name": op.name(),
                "deps": op.deps(),
                "when": op.runs_when(),
                "pool": op.pool_name(),
                "isolated": op.is_isolated(),
                "retries": op.max_retries(),
                "timeout_secs": op.timeout_after().map(|d| d.as_secs_f64()),
                "mapped_over": op.mapped_over(),
            })).collect::<Vec<_>>(),
        })).collect::<Vec<_>>(),
    });
    if out.json {
        out.object(&answer);
        return Ok(());
    }
    if out.quiet {
        for stage in &stages {
            for op in stage {
                println!("{}", op.name());
            }
        }
        return Ok(());
    }
    println!(
        "{}{}",
        out.paint(job.name(), BOLD),
        match job.description() {
            Some(d) => format!(" — {d}"),
            None => String::new(),
        }
    );
    let parallel = match job.max_parallel() {
        Some(n) => format!(", at most {n} at once"),
        None => String::new(),
    };
    println!(
        "{} ops in {} stages{parallel}",
        job.ops().len(),
        stages.len()
    );
    if !pools.is_empty() {
        let gates: Vec<String> = pools
            .iter()
            .map(|p| match p["limit"].as_u64() {
                Some(limit) => format!("{} (limit {limit})", s(p, "name")),
                None => format!("{} (not declared)", s(p, "name")),
            })
            .collect();
        println!("pools    {}", gates.join(", "));
    }
    println!();
    for (i, stage) in stages.iter().enumerate() {
        // the stage is what runs together: every op in it has its dependencies
        // behind it and none on each other
        let together = match stage.len() > 1 {
            true => out.paint(&format!("  ({} in parallel)", stage.len()), DIM),
            false => String::new(),
        };
        println!("{}{together}", out.paint(&format!("stage {}", i + 1), BOLD));
        for op in stage {
            let mut notes: Vec<String> = Vec::new();
            if op.runs_when() != crate::model::When::AllSucceeded {
                notes.push(format!("runs {}", op.runs_when()));
            }
            if let Some(pool) = op.pool_name() {
                notes.push(format!("pool {pool}"));
            }
            if op.is_isolated() {
                notes.push("isolated".into());
            }
            if let Some(dep) = op.mapped_over() {
                notes.push(format!("one per item of {dep}"));
            }
            if op.max_retries() > 0 {
                notes.push(format!("retries {}", op.max_retries()));
            }
            if let Some(after) = op.timeout_after() {
                notes.push(format!("timeout {}s", after.as_secs()));
            }
            let line = format!("  {:<24} {}", op.name(), out.paint(&notes.join(", "), DIM));
            println!("{}", line.trim_end());
        }
    }
    Ok(())
}

/// the ops in dependency order, grouped into the stages a run goes through.
///
/// an op's stage is one past the deepest of its deps, so everything in a stage
/// has its dependencies behind it and none on each other — which is exactly the
/// set the executor is free to run at once, subject to `max_parallel` and
/// whatever pools they take from.
fn stages(job: &Job) -> Vec<Vec<&crate::op::Op>> {
    let mut depth: HashMap<&str, usize> = HashMap::new();
    // the job's own topological order, so a dep is always resolved before the
    // op that names it
    let ordered: Vec<&crate::op::Op> = job.order().iter().filter_map(|name| job.op(name)).collect();
    for op in &ordered {
        let deepest = op
            .deps()
            .iter()
            .filter_map(|dep| depth.get(dep.as_str()))
            .max()
            .map_or(0, |d| d + 1);
        depth.insert(op.name(), deepest);
    }
    let mut stages: Vec<Vec<&crate::op::Op>> = Vec::new();
    for op in ordered {
        let at = depth[op.name()];
        while stages.len() <= at {
            stages.push(Vec::new());
        }
        stages[at].push(op);
    }
    stages
}

// ----------------------------------------------------------------- completions

/// the shells `completions` writes a script for.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Shell {
    Bash,
    Zsh,
    Fish,
}

/// what `__complete` will answer with, which is what the scripts below ask for.
#[derive(Clone, Copy, clap::ValueEnum)]
enum Names {
    Commands,
    Jobs,
    Assets,
    Schedules,
    Sensors,
    Runs,
}

/// the name this process was invoked as, which is what a completion script
/// completes unless `--name` says otherwise.
fn invoked_as() -> String {
    std::env::args()
        .next()
        .and_then(|arg0| {
            std::path::Path::new(&arg0)
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .unwrap_or_else(|| "hestan".into())
}

/// the names this binary can complete, printed one per line.
///
/// **this is why the mount is worth having.** a completion script for an
/// orchestrator normally has to bake in a list at build time or ask a server
/// over the network, because nothing on the command line knows what your
/// pipelines are called. here the registry is in the process: answering is a
/// process start and a `Vec` walk, which is fast enough to sit under a tab key.
fn complete(reach: Result<Reach, Fail>, what: Names, out: &Out) -> Result<(), Fail> {
    let names: Vec<String> = match what {
        // the parser's own, which needs no deployment at all: a shell asking
        // what the subcommands are must get an answer before it has been told
        // where anything is
        Names::Commands => Cli::command()
            .get_subcommands()
            .filter(|c| !c.is_hide_set())
            .map(|c| c.get_name().to_string())
            .collect(),
        Names::Jobs => reach?
            .inspect()?
            .jobs
            .iter()
            .map(|j| j.name().to_string())
            .collect(),
        Names::Assets => reach?
            .inspect()?
            .registry
            .topo()
            .map(|meta| meta.name.clone())
            .collect(),
        // out of the table rather than the registry: a schedule can be paused
        // and a sensor can be one a probe added, and either way the row is the
        // thing the commands take
        Names::Schedules => {
            let mut jobs: Vec<String> = reach?
                .store()?
                .schedules()?
                .into_iter()
                .map(|s| s.job)
                .collect();
            jobs.dedup();
            jobs
        }
        Names::Sensors => reach?
            .store()?
            .sensors()?
            .into_iter()
            .map(|s| s.name)
            .collect(),
        Names::Runs => reach?
            .store()?
            .runs(None, None, None, None, None, 50)?
            .into_iter()
            .map(|r| r.id)
            .collect(),
    };
    if out.json {
        out.object(&json!({ "names": names }));
        return Ok(());
    }
    for name in names {
        println!("{name}");
    }
    Ok(())
}

/// a completion script for `shell`, naming this binary.
///
/// the scripts are written out rather than generated from the parser, because
/// the interesting half is not the flags: it is that every name comes from
/// `__complete`, run against this binary, at the moment you press tab. a job
/// added this morning completes this afternoon with nothing regenerated.
fn completions(shell: Shell, name: &str, out: &Out) {
    // stdout, whatever the flags say: a completion script is the answer here,
    // and `eval "$(myapp completions bash)"` is how it is used
    let _ = out;
    print!("{}", completion_script(shell, name));
}

fn completion_script(shell: Shell, name: &str) -> String {
    // a shell function's name has to be an identifier, and a binary's name does
    // not have to be one
    let ident: String = name
        .chars()
        .map(|c| match c.is_alphanumeric() {
            true => c,
            false => '_',
        })
        .collect();
    match shell {
        Shell::Bash => format!(
            r#"# {name} completion, from the binary itself: every name below is asked for
# at the moment you press tab, so nothing here goes stale.
_{ident}_complete() {{
    local cur prev what
    cur="${{COMP_WORDS[COMP_CWORD]}}"
    prev="${{COMP_WORDS[COMP_CWORD-1]}}"
    case "$prev" in
        run|explain) what=jobs ;;
        build|backfill) what=assets ;;
        show|logs|cancel|retry|resume|priority) what=runs ;;
        sensor) what=sensors ;;
        schedule) what=schedules ;;
        *) what=commands ;;
    esac
    COMPREPLY=( $(compgen -W "$("$1" __complete "$what" 2>/dev/null)" -- "$cur") )
}}
complete -F _{ident}_complete {name}
"#
        ),
        Shell::Zsh => format!(
            r#"#compdef {name}
# {name} completion, from the binary itself: every name below is asked for
# at the moment you press tab, so nothing here goes stale.
_{ident}() {{
    local what=commands
    case "${{words[CURRENT-1]}}" in
        run|explain) what=jobs ;;
        build|backfill) what=assets ;;
        show|logs|cancel|retry|resume|priority) what=runs ;;
        sensor) what=sensors ;;
        schedule) what=schedules ;;
    esac
    local -a names
    names=( ${{(f)"$(${{words[1]}} __complete $what 2>/dev/null)"}} )
    compadd -- $names
}}
compdef _{ident} {name}
"#
        ),
        Shell::Fish => format!(
            r#"# {name} completion, from the binary itself: every name below is asked for
# at the moment you press tab, so nothing here goes stale.
function __{ident}_complete
    set -l tokens (commandline -opc)
    set -l what commands
    if test (count $tokens) -gt 1
        switch $tokens[-1]
            case run explain
                set what jobs
            case build backfill
                set what assets
            case show logs cancel retry resume priority
                set what runs
            case sensor
                set what sensors
            case schedule
                set what schedules
        end
    end
    {name} __complete $what 2>/dev/null
end
complete -c {name} -f -a '(__{ident}_complete)'
"#
        ),
    }
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

fn op_color(status: &str) -> &'static str {
    match status {
        "success" => GREEN,
        "failed" => RED,
        "canceled" | "skipped" => YELLOW,
        "running" => CYAN,
        _ => DIM,
    }
}

fn level_color(level: Option<EventLevel>) -> &'static str {
    match level {
        Some(EventLevel::Error) => RED,
        Some(EventLevel::Warn) => YELLOW,
        _ => "",
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
        match self.color && !style.is_empty() {
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

    /// something happened and there is not much to say about it: the object
    /// under `--json`, the sentence otherwise, and nothing at all under
    /// `--quiet`, which asked for an id and did not get one.
    fn said(&self, answer: &Value, sentence: &str) {
        if self.json {
            self.object(answer);
        } else if !self.quiet {
            println!("{sentence}");
        }
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
    fn settled(&self, run: &Value) {
        if self.json {
            self.object(run);
        } else if self.quiet {
            println!("{}", s(run, "id"));
        } else {
            println!(
                "{}  {} {} in {}",
                s(run, "id"),
                s(run, "job"),
                self.paint(&s(run, "status"), status_color(status_of(run))),
                took(run)
            );
        }
    }

    /// one line of a running run, on stderr.
    fn stream(&self, line: &Line) {
        if self.quiet {
            return;
        }
        eprintln!("{}", self.rendered(line, self.color_err));
    }

    /// one line of captured output, on stdout, where it is the answer.
    fn log(&self, line: &Line) {
        println!("{}", self.rendered(line, self.color));
    }

    fn rendered(&self, line: &Line, color: bool) -> String {
        let paint = |text: &str, style: &str| match color && !style.is_empty() {
            true => format!("{style}{text}{RESET}"),
            false => text.to_string(),
        };
        let op = match &line.op {
            Some(op) => format!("{} ", paint(op, CYAN)),
            None => String::new(),
        };
        format!(
            "{} {op}{}",
            paint(&stamp(&line.at), DIM),
            paint(&line.message, level_color(line.level))
        )
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

    // a filter goes over the wire in a query string, and a `+` that arrived as
    // a space would be an hour of runs nobody asked about
    #[test]
    fn a_query_escapes_what_a_url_cannot_carry() {
        assert_eq!(
            escape("2026-01-01T00:00:00+00:00"),
            "2026-01-01T00%3A00%3A00%2B00%3A00"
        );
        assert_eq!(escape("env=prod"), "env%3Dprod");
        assert_eq!(escape("orders_etl"), "orders_etl");
    }

    // both modes filter on the same thing, so the query the api gets is built
    // from exactly the flags the store read uses
    #[test]
    fn the_runs_query_carries_every_filter() {
        let query = runs_query(&RunsArgs {
            job: Some("etl".into()),
            tag: Some("env=prod".into()),
            since: None,
            limit: 5,
        })
        .unwrap();
        assert!(query.contains("limit=5"), "{query}");
        assert!(query.contains("job=etl"), "{query}");
        assert!(query.contains("tag=env%3Dprod"), "{query}");

        let bad = runs_query(&RunsArgs {
            job: None,
            tag: Some("prod".into()),
            since: None,
            limit: 5,
        })
        .unwrap_err();
        assert_eq!(bad.code, Exit::Usage);
    }

    // a database holds no registry, and the sentence that says so is the whole
    // of what makes that mode usable rather than baffling
    #[test]
    fn a_run_log_says_why_it_cannot_launch() {
        let fail = no_registry("/tmp/hestan.db", "launching");
        assert_eq!(fail.code, Exit::Unsupported);
        assert!(fail.message.contains("no job definitions"), "{fail:?}");
        assert!(fail.message.contains("--server"), "{fail:?}");
    }

    // ------------------------------------------------------------ doctor

    // every case below constructs the condition and asserts the check finds
    // it, because a check that cannot see what it is about is worse than no
    // check: it reports that everything is fine, forever, about nothing.

    use crate::asset::AssetRegistry;
    use crate::executor::Limits;
    use crate::job::Job;
    use crate::op::Op;
    use crate::schedule::Schedule;
    use std::sync::Arc;

    fn job(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new("only", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap()
    }

    fn app(store: Store, jobs: Vec<Job>) -> Inspected {
        Inspected {
            jobs,
            registry: Arc::new(AssetRegistry::empty()),
            store,
            pools: Vec::new(),
            limits: Limits::new(),
            retention: Retention::default(),
            role: Role::All,
            auth: None,
            db: ":memory:".into(),
        }
    }

    fn levels(findings: &[Finding]) -> Vec<Level> {
        findings.iter().map(|f| f.level).collect()
    }

    #[test]
    fn doctor_finds_a_cron_or_a_timezone_the_scheduler_cannot_use() {
        let store = Store::open(":memory:").unwrap();
        store
            .sync_schedules(&[
                Schedule::new("good", "*/5 * * * *"),
                Schedule::new("lost", "*/5 * * * *").tz("Mars/Olympus"),
            ])
            .unwrap();
        let findings = check_schedules(&store).unwrap();
        assert_eq!(levels(&findings), [Level::Ok, Level::Wrong]);
        assert!(findings[1].says.contains("lost"), "{}", findings[1].says);
        assert!(
            findings[1].says.contains("never fire"),
            "{}",
            findings[1].says
        );
        assert!(findings[1].fix.is_some());
    }

    #[test]
    fn doctor_finds_a_paused_schedule_and_a_paused_sensor() {
        let store = Store::open(":memory:").unwrap();
        store
            .sync_schedules(&[Schedule::new("nightly", "0 2 * * *")])
            .unwrap();
        store.sync_sensors(&["inbox".to_string()]).unwrap();
        assert_eq!(levels(&check_schedules(&store).unwrap()), [Level::Ok]);
        assert_eq!(levels(&check_sensors(&store).unwrap()), [Level::Ok]);

        assert!(
            store
                .set_schedule_paused("nightly", "0 2 * * *", true, None)
                .unwrap()
        );
        assert!(store.set_sensor_paused("inbox", true, None).unwrap());
        let schedules = check_schedules(&store).unwrap();
        assert_eq!(levels(&schedules), [Level::Ok, Level::Note]);
        assert!(schedules[1].says.contains("nightly"));
        let sensors = check_sensors(&store).unwrap();
        assert_eq!(levels(&sensors), [Level::Note]);
        assert!(sensors[0].fix.as_deref() == Some("unpause sensor inbox"));
    }

    // a claimer that stopped renewing leaves rows nothing else will notice,
    // which is exactly the case where a lease loop is not running either
    #[test]
    fn doctor_finds_a_run_held_past_its_lease() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([job("etl")], store.clone())
            .unwrap()
            .with_role(Role::Scheduler, 1);
        runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        assert_eq!(
            levels(&check_leases(&store, Utc::now()).unwrap()),
            [Level::Ok]
        );

        // claimed under a lease with no time on it at all, which is what a
        // process that died the instant after claiming leaves behind
        let defined = std::collections::HashSet::from(["etl".to_string()]);
        store
            .claim_next("gone", Duration::from_secs(0), &Limits::new(), &defined)
            .unwrap()
            .expect("the run was there to claim");
        let findings = check_leases(&store, Utc::now() + chrono::Duration::seconds(1)).unwrap();
        assert_eq!(levels(&findings), [Level::Wrong]);
        assert!(findings[0].says.contains("gone"), "{}", findings[0].says);
    }

    // the finding worth having: a run nothing is holding back and nothing is
    // taking, which is a deployment where every process was started as a
    // scheduler and has no other symptom at all
    #[test]
    fn doctor_finds_a_queue_that_nothing_is_taking_and_one_a_limit_is_holding() {
        let store = Store::open(":memory:").unwrap();
        let runner = Runner::new([job("etl")], store.clone())
            .unwrap()
            .with_role(Role::Scheduler, 1);
        let mut app = app(store.clone(), vec![job("etl")]);
        assert_eq!(levels(&check_queue(&app).unwrap()), [Level::Ok]);

        runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        let findings = check_queue(&app).unwrap();
        assert_eq!(levels(&findings), [Level::Wrong]);
        assert!(
            findings[0].says.contains("nothing holding them back"),
            "{}",
            findings[0].says
        );

        // and a limit that really is holding one back is a note rather than a
        // fault: the limit is doing what it was set to do
        runner.launch("etl", json!({}), Trigger::Manual).unwrap();
        let defined = std::collections::HashSet::from(["etl".to_string()]);
        let one = Limits::new().global(1);
        store
            .claim_next("worker", Duration::from_secs(60), &one, &defined)
            .unwrap()
            .expect("one of the two was claimable");
        app.limits = one;
        let findings = check_queue(&app).unwrap();
        assert_eq!(levels(&findings), [Level::Note]);
        assert!(findings[0].says.contains("limit"), "{}", findings[0].says);
    }

    // a policy in a process that will never run it: the database grows and
    // nothing anywhere says why
    #[test]
    fn doctor_finds_a_retention_policy_a_worker_will_never_sweep() {
        let store = Store::open(":memory:").unwrap();
        let mut app = app(store, Vec::new());
        assert_eq!(levels(&check_retention(&app)), [Level::Ok]);

        app.retention = Retention::days(7);
        assert_eq!(
            levels(&check_retention(&app)),
            [Level::Ok],
            "a role that decides sweeps"
        );

        app.role = Role::Worker;
        let findings = check_retention(&app);
        assert_eq!(levels(&findings), [Level::Wrong]);
        assert!(findings[0].says.contains("worker"), "{}", findings[0].says);
    }

    // the question to ask before giving a deployment an address: is anything
    // going to check who arrives on it
    #[test]
    fn doctor_says_whether_anything_checks_who_is_asking() {
        let unguarded = check_auth(None);
        assert_eq!(unguarded.level, Level::Note);
        assert!(unguarded.says.contains("loopback"), "{}", unguarded.says);
        assert!(unguarded.fix.unwrap().contains("Hestan::auth"));

        let guarded = check_auth(Some(&Auth::bearer("s3cret")));
        assert_eq!(guarded.level, Level::Ok);
        assert!(guarded.says.contains("bearer"), "{}", guarded.says);
        // and never the token, in a line that is on somebody's terminal and
        // in whatever collected it
        assert!(!guarded.says.contains("s3cret"), "{}", guarded.says);

        // the opt-out is a claim about something else, so it is worth a line
        // rather than a tick
        let asserted = check_auth(Some(&Auth::None));
        assert_eq!(asserted.level, Level::Note);
        assert!(asserted.says.contains("Auth::None"), "{}", asserted.says);
    }

    // a full disk cannot be constructed in a test, so the two halves are
    // tested apart: the reading, against a directory that exists, and the
    // verdict, against numbers
    #[test]
    fn doctor_reads_the_disk_and_calls_a_nearly_full_one_wrong() {
        let dir = std::env::temp_dir().join("hestan-doctor-disk.db");
        let (free, total) = disk_free(&dir.display().to_string()).expect("a local path has a disk");
        assert!(total > 0 && free <= total, "free {free} of {total}");
        assert_eq!(
            disk_free("postgres://host/db"),
            None,
            "a server's disk is not ours"
        );
        assert_eq!(disk_free(":memory:"), None);

        assert_eq!(check_disk("/x.db", 500, 1000).level, Level::Ok);
        let nearly = check_disk("/x.db", 1, 1000);
        assert_eq!(nearly.level, Level::Wrong);
        assert!(nearly.says.contains("0%"), "{}", nearly.says);
    }

    #[test]
    fn bytes_are_readable_at_every_scale() {
        assert_eq!(bytes(512), "512b");
        assert_eq!(bytes(2048), "2.0kb");
        assert_eq!(bytes(5 * 1024 * 1024 * 1024), "5.0gb");
    }

    // ----------------------------------------------------------- explain

    // a diamond: one op, two that depend on it and not on each other, and one
    // that waits for both. the stage in the middle is the parallel pair, and
    // getting that wrong is the whole way an explain can lie
    #[test]
    fn explain_orders_a_diamond_and_puts_the_parallel_pair_in_one_stage() {
        let diamond = Job::builder("diamond")
            .op(Op::new("fetch", |_| async { Ok(json!(null)) }))
            .op(Op::new("left", |_| async { Ok(json!(null)) }).after(["fetch"]))
            .op(Op::new("right", |_| async { Ok(json!(null)) }).after(["fetch"]))
            .op(Op::new("join", |_| async { Ok(json!(null)) }).after(["left", "right"]))
            .build()
            .unwrap();
        let stages = stages(&diamond);
        let names: Vec<Vec<&str>> = stages
            .iter()
            .map(|stage| stage.iter().map(|op| op.name()).collect())
            .collect();
        assert_eq!(names.len(), 3, "{names:?}");
        assert_eq!(names[0], ["fetch"]);
        assert_eq!(names[2], ["join"]);
        let mut middle = names[1].clone();
        middle.sort_unstable();
        assert_eq!(middle, ["left", "right"], "the parallel pair is one stage");
    }

    // a chain is three stages of one, which is the same rule saying the
    // opposite thing
    #[test]
    fn explain_puts_a_chain_in_one_stage_each() {
        let chain = Job::builder("chain")
            .op(Op::new("a", |_| async { Ok(json!(null)) }))
            .op(Op::new("b", |_| async { Ok(json!(null)) }).after(["a"]))
            .op(Op::new("c", |_| async { Ok(json!(null)) }).after(["b"]))
            .build()
            .unwrap();
        assert_eq!(
            stages(&chain).iter().map(Vec::len).collect::<Vec<_>>(),
            [1, 1, 1]
        );
    }

    // ------------------------------------------------------- completions

    // the point of the mount, as a test: the script asks the binary, so the
    // names cannot be stale, and the subcommand it asks with is a real one
    #[test]
    fn a_completion_script_asks_this_binary_for_every_name() {
        for shell in [Shell::Bash, Shell::Zsh, Shell::Fish] {
            let script = completion_script(shell, "myapp");
            assert!(script.contains("__complete"), "{script}");
            assert!(script.contains("myapp"), "{script}");
            for what in ["jobs", "assets", "runs", "sensors", "schedules", "commands"] {
                assert!(script.contains(what), "{shell:?} never asks for {what}");
            }
        }
    }

    // and the hidden subcommand the scripts call is not in the list they offer
    #[test]
    fn the_completion_hook_is_not_itself_a_suggestion() {
        let names: Vec<String> = Cli::command()
            .get_subcommands()
            .filter(|c| !c.is_hide_set())
            .map(|c| c.get_name().to_string())
            .collect();
        assert!(names.contains(&"doctor".to_string()));
        assert!(names.contains(&"explain".to_string()));
        assert!(!names.contains(&"__complete".to_string()), "{names:?}");
    }
}
