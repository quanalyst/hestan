# Getting started

Create a scheduled job with a local run store and web UI. You need Rust 1.88
or newer; the default SQLite backend requires no separate database service.

## The dependency

```sh
cargo new example && cd example
cargo add hestan
cargo add tokio --features full
```

`hestan::prelude` exports `json!` and `Value`. Add `serde` with `derive` when
defining typed parameters or outputs. Optional features are listed in the
[README](../README.md#using-it-from-your-project).

For local development, use a path dependency; a Git dependency can be pinned
to a release tag. See [embedding](embedding.md#consuming-from-another-repo).

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

- `Op::new` defines a named async operation. Its JSON result is recorded and
  made available to downstream operations.
- `.after(["extract"])` declares a dependency. `ctx.input("extract")` reads
  its result.
- `Job::build()` rejects cycles, missing dependencies and duplicate names.
- `.retries(2)` permits two additional attempts after a failure, with backoff
  and jitter.
- `ctx.info` records an event; `ctx.meta` attaches structured output metadata.
- `serve` opens the store, starts the loops for the selected role, and serves
  the UI and API until shutdown.

For a headless execution, use `run_once("etl", json!({})).await` instead of
`serve`. See [embedding](embedding.md).

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

Open Jobs to inspect the graph and launch a run. Run details show timings,
operation results and logs. Cmd-K or Ctrl-K opens the command palette.
See [web UI](web-ui.md) for the full guide.

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
  requires an authenticator for non-loopback addresses unless `Auth::None`
  explicitly enables unauthenticated access.

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
