# hestan

[![ci](https://img.shields.io/github/actions/workflow/status/quanalyst/hestan/ci.yml?branch=main&label=ci)](https://github.com/quanalyst/hestan/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/hestan.svg)](https://crates.io/crates/hestan) [![docs.rs](https://img.shields.io/docsrs/hestan)](https://docs.rs/hestan)

dag-based job orchestration for rust — think dagster's core loop: ops, jobs,
schedules, a run log, and a small web ui. it is a library, not a service: the
jobs are async rust in your own binary and the run log is a sqlite file next
to it, so there is nothing to deploy alongside it.

## Alpha

this is `0.1.0-alpha.1`, the first public release. under 0.x the api changes
without a deprecation cycle, so read the changelog before bumping. it has not
been run in production, and the gaps that are known are listed under
[not here yet](#not-here-yet).

the ui and json api have no authentication. bind them to loopback — see
[SECURITY.md](SECURITY.md).

## Quickstart

```toml
[dependencies]
hestan = "0.1.0-alpha.1"
tokio = { version = "1", features = ["full"] }
```

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
            Ok(json!({"loaded": rows.as_array().map_or(0, Vec::len)}))
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

## Pulling from an http api

with the `http` feature, a scheduled REST pull is one block — no hand-written
ops:

```rust
use hestan::{HttpSource, Hestan};

Hestan::new()
    .source(
        HttpSource::get("https://api.coingecko.com/api/v3/simple/price")
            .name("btc_spot")
            .query("ids", "bitcoin")
            .query("vs_currencies", "usd")
            .cron("*/5 * * * *"),
    )
    .serve(([127, 0, 0, 1], 4000))
    .await
```

```toml
hestan = { version = "0.1.0-alpha.1", features = ["http"] }
```

transport errors, 429s, and 5xx responses are retried with capped exponential
backoff and full jitter (a numeric `Retry-After` is honored exactly, capped at
5 minutes); any other
non-2xx fails the op immediately. `query_each("ids", ["bitcoin", "ethereum"])`
fans the source out into one op per value, named `btc_spot_bitcoin`-style:
the value lowercased, with anything outside `[a-z0-9_]` replaced by `_`.

## Running the demo

```
cargo run --example demo
```

then open http://127.0.0.1:4000 — two jobs on short schedules, so runs
(including a retried op and some dropped-row warnings) accumulate on their own.

`cargo run --example assets` (from the repo root) serves a second instance
on http://127.0.0.1:4002: an asset pipeline over this repo's own `docs/`
directory, where touching a file has the probe rebuild the totals within ten
seconds, plus a sensor that launches an ingest job when `ingest.marker`
appears.

## How it works

- ops are async fns wired into a job dag; `Job::builder(..).op(..).build()`
  validates the graph (duplicate ops, unknown deps, cycles) up front
- ops can be typed: `Op::typed` deserializes upstream outputs into a struct
  (one field per dep, named after it) and serializes the return value back to
  json. a shape mismatch fails the attempt with a `type check failed` error
  that goes through the normal retry policy. untyped `Op::new` + `ctx.input`
  still work, and the two mix freely
- `.params::<P>()` declares an op's params type; a launch whose params don't
  deserialize into `P` is rejected before any run is recorded (http 400 from
  the launch endpoint)
- runs execute on tokio: every op whose deps are done runs concurrently
  (capped per job with `max_parallel`, and across jobs with named
  concurrency pools — `Hestan::pool("api", 3)` plus `Op::pool("api")` is one
  budget for one external resource, however many jobs overlap). a terminal op
  failure skips its downstream and fails the run, while independent branches
  keep going
- op outputs are pluggable: `Hestan::io(FileIo::new(dir))` moves them out of
  sqlite and leaves a `{"$io": ..}` handle in the run log, with `Op::io(name)`
  for per-op managers. the default `Inline` is exactly today's behaviour
- resources are built once at startup and shared by every op:
  `Hestan::resource("api", |_| async { Ok(ApiClient::new()?) })` plus
  `ctx.resource::<ApiClient>("api")?`, with `Op::requires(["api"])` turning a
  missing one into a build error. a constructor that fails aborts startup
  instead of leaving a half-live server
- a reusable `Graph` of ops drops into a job as many times as you like:
  `.graph("clean_a", &clean).after(["fetch"])`. it is flattened at build into
  ordinary ops named `{instance}.{inner}`, so nothing at run time — resume,
  fan-out, assets, the ui — has to know graphs exist
- an op's trigger rule says when it runs: `.when(When::Always)` or
  `.when(When::AnyFailed)` makes a summary, an alert or a cleanup after a
  failure expressible, where the default `AllSucceeded` skips it. skip
  propagation stops at such an op, and `ctx.dep_status(dep)` tells it what
  each dep actually did
- one op can fan out over a list only known at run time: `Op::mapped(f)
  .over("pages")` runs an instance per element of that dep's json array, each
  with its own op run row and its element as a typed argument, and hands
  downstream the collected outputs in element order. instances are ordinary
  tasks, so `max_parallel`, pools, retries and cancellation apply unchanged;
  it is all-or-nothing, and an empty array is a legal zero-instance fan-out
- failed ops retry up to `.retries(n)` extra attempts, backing off
  exponentially with full jitter by default so ops that fail together don't
  retry together; `.retry_delay(d)` keeps a fixed pause
- `.timeout(d)` fails a hung attempt instead of letting it hold a slot
  forever, and cancellation (or a timeout) is readable from the op itself via
  `ctx.is_cancelled()` — the only way blocking work ever stops early. an op
  that stops is recorded as stopped; one that never comes back is recorded as
  exactly that, with no invented finish time
- ops can carry persisted state: `ctx.set_state` stages a watermark that
  commits only when the attempt succeeds, so the next run picks up where the
  last successful one left off
- `ctx.meta("rows", 1_234)` attaches typed facts to what an op produced —
  ints, floats, text, urls, markdown, json — staged per attempt like state
  and committed with the op's terminal write, so a failed attempt's numbers
  never get recorded. an asset op's metadata lands on its materialization
  too, so the history says what each build reported
- every run, op attempt, output, and log event lands in a sqlite file (WAL,
  one connection behind a mutex — plenty at this scale) with no extra services
  and optional retention (`retention_days(n)` prunes terminal runs older than
  n days at startup; the default keeps everything). the schema migrates
  itself forward via `PRAGMA user_version`, and runs that a dead process left
  behind are marked failed on the next start
- events are structured: each carries a `kind` (`run_queued`, `op_retry`,
  `type_check_failed`, ...) and optional json data alongside the
  human-readable message; `ctx.info/warn/error` emit `kind=log`
- the scheduler keeps a **durable cursor** per schedule: the newest occurrence
  it has accounted for. everything between the cursor and now is what downtime
  swallowed, so `Catchup::{Skip, One, All { limit }}` can say what to do about
  it — and a queue-policy fire held for a busy job is durable for the same
  reason, since the pending queue is the tick log rather than a `HashMap`
- a caught-up run knows which logical time it stands for: `runs.scheduled_for`
  and `ctx.scheduled_for()`, so a pipeline can pull the data *for* the hour it
  missed rather than for now
- schedules are plain 5-field cron expressions (sunday is 0 or 7, as usual),
  evaluated in utc or in a named timezone via `schedule_tz`, by an in-process
  scheduler. they can be paused from the ui, the flag survives restarts, and
  every fire lands in a tick log with the run it launched
- assets add incremental-build semantics on top of ops: each keeps a
  persisted latest value and a fingerprint, declares explicit lineage, and
  staleness is provable from recorded fingerprints — a build materializes
  exactly the stale ancestors plus the target, seeding fresh values instead
  of recomputing them. source assets stand for external data and carry a
  cheap fingerprint probe; `.auto()` assets rebuild themselves when a probe
  upstream makes them stale
- materializations are append-only history, capped per asset: each build
  leaves an entry, and every entry says whether the fingerprint actually
  moved — so "when did this last change" is a question with an answer, not
  the same question as "when was this last built"
- asset checks are assertions bound to an asset, handed the value it just
  produced: `AssetCheck::new("rows_present", "orders", |_, v| ..)` plus
  `Hestan::check(..)`. they lower into ops of the same build, so retries,
  cancellation and the gantt apply unchanged; `Severity::Error` fails the run
  and `Severity::Warn` just records the failure. a check runs when its asset
  rebuilds — a memoized asset is not re-checked
- sensors poll the world on an interval and launch runs when something
  changed, with a store-backed cursor committed only on fully successful
  evaluations. probes run as internal sensors on the same loop, and both are
  pausable with tick history
- freshness policies are declarations, not guesses: `fresh_within(d)` on a job
  or an asset says how old its latest success may get, and past that it is
  `late` — per key on a partitioned asset, so one late partition carries the
  asset. a declared policy replaces the cron-derived `overdue` heuristic
- `on_failure` hooks fire for every failed run, `on_late` hooks fire when
  something crosses into late — once per crossing, not per poll, and the state
  survives restarts. ready-made webhook and slack helpers behind the `http`
  feature serve both; the failed run row carries the same
  `op {name} failed: {message}` summary for anything reading history instead
- the web ui is a prebuilt react bundle embedded in the binary; it polls the
  json api under `/api`

## Docs

the details live in [docs/](docs/README.md):
[getting started](docs/getting-started.md),
[concepts](docs/concepts.md) (execution semantics),
[typed io](docs/typed-io.md), [resources](docs/resources.md),
[io managers](docs/io-managers.md), [op state](docs/state.md),
[metadata](docs/metadata.md),
[assets](docs/assets.md), [freshness](docs/freshness.md),
[sensors](docs/sensors.md),
[scheduling](docs/scheduling.md), [http sources](docs/http-sources.md),
[notifications](docs/notifications.md), [the web ui](docs/web-ui.md),
[the http api](docs/http-api.md), [storage](docs/storage.md),
[embedding](docs/embedding.md), and [development](docs/development.md).
release notes are in [CHANGELOG.md](CHANGELOG.md).

## Using it from your project

```toml
[dependencies]
hestan = "0.1.0-alpha.1"
```

the binary is yours: define jobs, then `Hestan::new()...serve(addr)`, or
`run_once(job, params)` for headless one-off runs.

## Developing the ui

`cd ui && npm run dev` starts the vite dev server, which proxies `/api` to
`localhost:4000` — run the demo alongside it. `just ui-build` regenerates
`ui/dist`, which is committed so cargo users don't need node.

## Not here yet

- [ ] max concurrent runs per job (overlap policies gate scheduled fires
      only; concurrency pools cap ops across runs, not the runs themselves)
- [ ] postgres store
- [ ] post/body http sources
- [ ] incremental cursors for http sources
- [ ] paired-param fan-out
