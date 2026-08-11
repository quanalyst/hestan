# Getting started

## The dependency

hestan is consumed as a plain cargo dependency — there is no server to install
and no sidecar process.

```toml
[dependencies]
hestan = "0.1.0-alpha.2"
tokio = { version = "1", features = ["full"] }
```

this is alpha: the api changes without a deprecation cycle under 0.x, so read
the changelog before bumping. to track the repo instead — a tag:

```toml
[dependencies]
hestan = { git = "https://github.com/quanalyst/hestan", tag = "v0.1.0-alpha.2" }
```

or a sibling checkout, for hacking on both at once:

```toml
[dependencies]
hestan = { path = "../hestan" }
```

add `features = ["http"]` to either form if you want [`HttpSource`](http-sources.md);
everything else works without it.

## A first job

a complete `main.rs` — two ops, a dependency edge, retries, a schedule, and the
web ui:

```rust
use hestan::prelude::*;

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    let etl = Job::builder("etl")
        .op(Op::new("extract", |ctx| async move {
            ctx.info("pulling rows");
            Ok(json!([1, 2, 3]))
        }))
        .op(Op::new("load", |ctx| async move {
            let rows = ctx.input("extract").cloned().unwrap_or_default();
            Ok(json!({ "loaded": rows.as_array().map_or(0, Vec::len) }))
        })
        .after(["extract"])
        .retries(2))
        .build()?;

    Hestan::new()
        .job(etl)
        .schedule("etl", "*/10 * * * *")
        .db("hestan.db")
        .serve(([127, 0, 0, 1], 4000))
        .await
}
```

`build()` validates the dag up front: duplicate op names, deps on ops that
don't exist, and cycles are all errors at build time, not at run time. an op
body is an async closure from `OpCtx` to
`Result<Value, Box<dyn Error + Send + Sync>>`; `ctx.input("extract")` reads
the upstream op's output and `ctx.info` writes a log event into the run
history. `.after(["extract"])` declares the edge, `.retries(2)` allows two
extra attempts after a failure.

`Hestan::new()` collects jobs and schedules. `.db` names the sqlite file
(default `hestan.db`; `":memory:"` also works). `.serve` opens the database,
recovers any runs a previous process left behind, starts the in-process
scheduler, and serves the ui and json api on the address — then runs until
the process is killed. for a single headless run instead, swap it for
`run_once("etl", json!({})).await` — see [embedding](embedding.md).

## Running the demo

from the hestan repo itself:

```
cargo run --example demo --features cli
```

then open http://127.0.0.1:4000. the demo registers two jobs on short
schedules (`orders_etl` every 2 minutes, `warehouse_healthcheck` every 5), so
history accumulates on its own: the etl's `publish` op fails once per run and
demos a retry, and `validate` drops malformed rows with warnings you can find
in the run log.

## A first look at the ui

the jobs page shows every registered job with its schedules, a runs timeline,
and per-job duration sparklines. click a job for its dag, op stats, schedule
controls, and a launch button (with a json params editor); a run opens the
per-op gantt and the live event log. `cmd-k` (or `ctrl-k`) opens a
command palette over jobs, runs, and pause/resume actions. the full tour is in
[web ui](web-ui.md).
