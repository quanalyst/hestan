# Getting started

from nothing to a job running on a schedule, with a ui, in one pass. it needs
a rust toolchain (1.88 or newer) and nothing else: no server to install, no
sidecar, no daemon. hestan is a library, and what you end up with is your own
binary.

## The dependency

```
cargo new orders && cd orders
cargo add hestan
cargo add tokio --features full
```

tokio because everything hestan runs is async and something has to drive it,
and that is the whole of the setup. an op hands its result back as json, and
`json!` and `Value` come out of `hestan::prelude`, so nothing on this page
needs a `serde_json` of its own; add that, and `serde` with `derive`, at the
point your ops start taking [typed params](typed-io.md) or returning types you
declared. to track the repository instead of the release, with a tag
or a sibling checkout for hacking on both at once:

```toml
hestan = { git = "https://github.com/quanalyst/hestan", tag = "v0.2.0" }
hestan = { path = "../hestan" }
```

this is a 0.x: the api changes without a deprecation cycle, so read
[the changelog](../CHANGELOG.md) before bumping. everything on this page works
with no cargo features turned on; the optional ones are listed in
[the readme](../README.md#using-it-from-your-project).

## The smallest thing that runs

replace `src/main.rs` with this:

```rust
use hestan::prelude::*;

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    let etl = Job::builder("etl")
        .op(Op::new("extract", |ctx: OpCtx| async move {
            ctx.info("pulling rows");
            Ok(json!([1, 2, 3]))
        }))
        .op(Op::new("load", |ctx: OpCtx| async move {
            let rows = ctx.input("extract").cloned().unwrap_or_default();
            let n = rows.as_array().map_or(0, Vec::len);
            ctx.meta("rows", n as i64);
            Ok(json!({ "loaded": n }))
        })
        .after(["extract"])
        .retries(2))
        .build()?;

    Hestan::new()
        .job(etl)
        .schedule("etl", "*/10 * * * *")
        .serve(([127, 0, 0, 1], 4000))
        .await
}
```

`cargo run`, then open <http://127.0.0.1:4000>.

there is a job called `etl` on the jobs page, firing every ten minutes. press
its launch button and you do not have to wait for one.

## What each line is

an **op** is one unit of work: a name and an async closure. it is handed an
[`OpCtx`](concepts.md#opctx) and hands back json. whatever it returns is
recorded in the run log and passed to whatever depends on it.
`ctx.input("extract")` is how `load` reads what `extract` produced, and
`.after(["extract"])` is what makes `load` wait for it. there is no other
wiring: the edge and the data path are one declaration.

a **job** is a dag of ops, and `build()` is where it is checked: a cycle, a
dep on a name no op has, or two ops sharing a name is an error here, at
startup, rather than a run that gets halfway through and stops.

a **run** is one execution of a job. `.retries(2)` gives `load` two more
attempts if it fails, spaced by a backoff with jitter on it; each attempt is
recorded separately, so an op that worked on the third try says so rather than
looking like one that worked.

`ctx.info` writes a line into the run log: hestan's own structured record,
which is [not the same thing](logs.md) as a `println!`. `ctx.meta("rows", n)`
attaches a typed fact to what the op produced, and the ui renders it as a
number and tracks it across runs. neither is required; both are what make a run
readable three months later.

`Hestan::new()` collects everything, and `serve` is what starts: it opens the
database, recovers whatever a previous process left half-done, runs the
scheduler, and serves the ui and json api on the address given. it does not
return until the process is
[asked to stop](scaling.md#stopping-a-process-on-purpose). for one headless run
and no server, swap it for `run_once("etl", json!({})).await`; see
[embedding](embedding.md).

## Where the state lives

nothing above named a database, so the run log is `hestan.db` in the working
directory: a sqlite file, created on first run. `.db("var/orders.db")` puts
it somewhere else, `":memory:"` keeps nothing, and a `postgres://` url with the
`postgres` feature on is a run log several machines can share
([storage](storage.md)).

everything is in there: runs, op attempts, outputs, events, captured output.
delete the file and you have deleted the history, not the jobs. the jobs are
in your binary.

## The ui

the **jobs page** at `/` lists every registered job with its schedules, a
timeline of recent runs, and a duration sparkline each. click `etl` for its
dag, per-op statistics, schedule controls, and a launch button with a params
editor.

a **run page** shows the ops on a gantt, the event log live as it happens, and
whatever each op printed. `cmd-k` (or `ctrl-k`) opens a palette over jobs, runs
and pause actions. the full tour is in [web ui](web-ui.md).

nothing in the ui is a mock: an empty database says it is empty rather than
showing a sample of something.

## Then what

- **a command line over the same jobs.** add `features = ["cli"]` and call
  `hestan::cli::run(app, addr).await` in place of `serve`. with no arguments it
  serves exactly as before; with arguments that binary can launch, tail,
  cancel, explain and diagnose; see [the command line](cli.md).
- **[choosing](choosing.md)** answers the questions this page skipped: job or
  asset, sqlite or postgres, in-process or isolated, schedule or sensor.
- **[concepts](concepts.md)** is the execution model in full: how a run
  proceeds, what cancellation really does, trigger rules, reusable graphs,
  fan-out.
- **serving it to anybody else** means [authentication](auth.md): `serve`
  refuses an address that is not loopback until something checks who is asking.

## Running the examples

from a clone of the hestan repository rather than from your own project:

```
cargo run --example demo --features cli
```

two jobs on short schedules at <http://127.0.0.1:4000>, so history accumulates
on its own: the etl's `publish` op fails once per run and demonstrates a retry,
and `validate` drops malformed rows with warnings you can find in the run log.

`cargo run --example assets --features cli` serves a second instance on
<http://127.0.0.1:4002>, an asset pipeline over that repository's own `docs/`
directory, where touching a file has the probe notice and the totals rebuild
within ten seconds.
