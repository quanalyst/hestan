# hestan

[![ci](https://img.shields.io/github/actions/workflow/status/quanalyst/hestan/ci.yml?branch=main&label=ci)](https://github.com/quanalyst/hestan/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/hestan.svg)](https://crates.io/crates/hestan) [![docs.rs](https://img.shields.io/docsrs/hestan)](https://docs.rs/hestan)

dag-based job orchestration for rust: ops, jobs, schedules, a run log, and a
small web ui. it is a library, not a service: the jobs are async rust in your
own binary and the run log is a sqlite file next to it, so there is nothing to
deploy alongside it (or a postgres database, when the workers have to live on
more than one machine).

the ui and json api are unauthenticated by default, and `serve` refuses to bind
anything but loopback under that default. an address anyone can reach needs an
[authenticator](docs/auth.md): one token, or your own check. see
[SECURITY.md](SECURITY.md).

under 0.x the api changes without a deprecation cycle, and the minor number is
where a break lands.

## Quickstart

```toml
[dependencies]
hestan = "0.2.4"
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

with the `http` feature, a scheduled REST pull is one block, with no
hand-written ops:

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
hestan = { version = "0.2.4", features = ["http"] }
```

transport errors, 429s, and 5xx responses are retried with capped exponential
backoff and full jitter (a numeric `Retry-After` is honored exactly, capped at
5 minutes); any other
non-2xx fails the op immediately. `query_each("ids", ["bitcoin", "ethereum"])`
fans the source out into one op per value, named `btc_spot_bitcoin`-style:
the value lowercased, with anything outside `[a-z0-9_]` replaced by `_`.

## Running the demo

```
cargo run --example demo --features cli
```

then open http://127.0.0.1:4000. two jobs on short schedules, so runs
(including a retried op and some dropped-row warnings) accumulate on their own.

`cargo run --example assets --features cli` (from the repo root) serves a second instance
on http://127.0.0.1:4002: an asset pipeline over this repo's own `docs/`
directory, where touching a file has the probe rebuild the totals within ten
seconds, plus a sensor that launches an ingest job when `ingest.marker`
appears.

## A command line, in your binary

your jobs are compiled in, so a command line over them needs nothing loaded and
nothing configured to find them:

```rust
hestan::cli::run(app, ([127, 0, 0, 1], 4000)).await   // was: app.serve(addr).await
```

with no arguments that *is* `serve`, on the same address. with arguments it
knows every job, asset, schedule and sensor by name:

```
$ orders run orders_etl --wait
14:22:01 fetch_orders fetched 1,204 rows
14:22:02 validate dropping bad row: {"id":3}
14:22:03 run succeeded
019ff1b7-8df6-7732-8f54-70fa61013409  orders_etl success in 1.5s
$ echo $?      # 0 succeeded · 1 failed · 3 canceled · 4 timed out · 5 unreachable
0
```

`run --wait` streams the run to stderr and exits with what it did, which is
what a cron line needs. `--json` for one object, `--quiet` for the id alone.
`doctor` answers "why is nothing running" from the store, the registry and the
disk. `explain` resolves a real plan without running it, and shell completion
of your own job names comes out of the registry at the moment you press tab,
both only possible because the command line is the deployment. the same
commands reach a run log directly (`--db`) or a running instance
(`--server`), and `cargo install hestan --features cli` gives an operator a
standalone binary for those two. [docs/cli.md](docs/cli.md).

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
  concurrency pools: `Hestan::pool("api", 3)` plus `Op::pool("api")` is one
  budget for one external resource, however many jobs overlap). a terminal op
  failure skips its downstream and fails the run, while independent branches
  keep going
- the other shape an external limit comes in is a **rate**, and it is the one
  most apis publish: `Hestan::rate("api", 5, Duration::from_secs(1))` plus
  `Op::rate("api")` is five calls a second across every job in the process. a
  token bucket, so five may go at once and then one every 200ms, not a fixed
  window, which admits its whole allowance either side of a boundary. a token
  is spent rather than held, waiting for one is asynchronous and first-come
  first-served, and a canceled run's waiting op hands its token to the op
  behind it. the bucket is **this process's**: two workers each honouring five
  a second send ten, which [scaling](docs/scaling.md#a-rate-is-per-process) is
  blunt about
- op outputs are pluggable: `Hestan::io(FileIo::new(dir))` moves them out of
  the run log and leaves a `{"$io": ..}` handle in it, with `Op::io(name)`
  for per-op managers. the default `Inline` is exactly today's behaviour, and
  `ParquetIo` (behind `--features parquet`) stores an op's rows as one parquet
  file, recording how many rows and how many bytes without the op asking. an
  asset's value goes the same way with `Asset::io(name)`: the materialization
  records the handle and the next build seeds from the file. see
  [connecting to your data](docs/connecting.md) for the whole seam between an
  op and the system it reads: a client called from an op, a pool as a
  resource, secrets out of the environment, and why hestan wraps nobody's sdk
- resources are built once at startup and shared by every op:
  `Hestan::resource("api", |_| async { Ok(ApiClient::new()?) })` plus
  `ctx.resource::<ApiClient>("api")?`, with `Op::requires(["api"])` turning a
  missing one into a build error. a constructor that fails aborts startup
  instead of leaving a half-live server. `Hestan::run_resource` is the other
  scope (built when a run starts, dropped when it ends, off the runtime if
  dropping blocks) for a scratch directory or anything else one run must not
  share with the next
- a reusable `Graph` of ops drops into a job as many times as you like:
  `.graph("clean_a", &clean).after(["fetch"])`. it is flattened at build into
  ordinary ops named `{instance}.{inner}`, so nothing at run time (resume,
  fan-out, assets, the ui) has to know graphs exist
- an op's trigger rule says when it runs: `.when(When::Always)` or
  `.when(When::AnyFailed)` makes a summary, an alert or a cleanup after a
  failure expressible, where the default `AllSucceeded` skips it. skip
  propagation stops at such an op, and `ctx.dep_status(dep)` tells it what
  each dep actually did
- an op can also say for itself that there was nothing to do:
  `return Err(ctx.skip("no drop from the vendor yet"))` ends it `skipped`
  with the reason on its row and in the log, and neither succeeds (which would
  record a build that did not happen) nor fails the run (which would wake
  somebody for a non-event). it goes out through the error channel so the
  compiler makes the body stop, and it is recorded and propagated exactly as a
  rule skip is, so nothing downstream has a second notion of skip to learn
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
  `ctx.is_cancelled()`, the only way blocking work ever stops early. an op
  that stops is recorded as stopped; one that never comes back is recorded as
  exactly that, with no invented finish time
- `Op::isolated()` runs one op's body in a child process: this same binary,
  re-executed, so there is no runtime to load and no code to re-import. it
  contains what in-process cannot: an op that segfaults or aborts fails that
  op instead of taking hestan down, and the parent records the signal that
  killed it. it also makes stopping real: for an isolated op, cancellation
  and timeouts are SIGTERM, three seconds, then SIGKILL, rather than a request
  blocking work can ignore. per op rather than per job, so the one risky
  parser is contained while the other forty stay in-process and free.
  `.memory_limit(bytes)` and `.cpu_limit(d)` cap the child with `setrlimit`;
  the store is the only channel between the two processes, and unix-only is a
  build error rather than a silent fallback
- ops can carry persisted state: `ctx.set_state` stages a watermark that
  commits only when the attempt succeeds, so the next run picks up where the
  last successful one left off
- `ctx.meta("rows", Meta::count(1_240))` attaches typed facts to what an op
  produced (counts, sizes, durations, tables, paths, urls, markdown, json,
  and links to another run or asset of the same deployment), staged per
  attempt like state and committed with the op's terminal write, so a failed
  attempt's numbers never get recorded. the units are not decoration: the ui
  renders `1.2 GB` and `3.4s` rather than the integers. an asset op's metadata
  lands on its materialization too, so the history says what each build
  reported
- `ctx.saved("by_region", table)` is the same fact **marked as a sample of
  what the op wrote**, which is what collects every op's into one section on
  the run page rather than leaving each behind selecting an op.
  `Meta::series` is the timeseries shape, kept across its range rather than
  off its head, since the first hundred points of an hourly year are January
  drawn as a year. the op selects its own sample back, so hestan runs no query
  and holds no credentials, and what is stored is a snapshot of the moment the
  run wrote rather than a view of the table now, which the page says out loud
- launching is a request rather than a start: a launch writes a **queued** run
  and a dispatcher starts it as soon as no limit says otherwise; with no
  limits declared, the same instant. `Hestan::max_concurrent_runs(n)` caps the
  deployment, `JobBuilder::max_concurrent_runs(n)` one job, and
  `Hestan::tag_limit("env", "prod", 2)` whatever carries a tag, with
  `{"priority": n}` deciding what goes first. the queue is the `runs` table, so
  it survives a restart and something else can pull from it
- **that something else is `Hestan::work(addr)`**: a worker process that claims
  queued runs and executes them, and fires no schedule and evaluates no sensor.
  `Hestan::role(Role::Scheduler)` is the other half: one process owns the
  decisions, any number execute. claiming is a compare-and-set with a renewed
  lease, so exactly one claimer wins a run and a claimer that dies loses it
  rather than stranding it. `Dockerfile` and `docker-compose.yml` run postgres,
  one scheduler and three workers from one image against one database
  ([scaling](docs/scaling.md), [containers](docs/containers.md))
- every run, op attempt, output, and log event lands in a sqlite file (WAL,
  one connection behind a mutex; plenty at this scale) with no extra services,
  or in postgres with `--features postgres` and a url: same schema, same api,
  and the same test suite runs against both. with optional retention: `Retention::days(30).keep_last(20).failed_days(90)`,
  globally or per job, swept at startup and every hour after it rather than
  only at boot. a run goes only when **every** rule would take it, which is the
  conservative direction; the default keeps everything. the schema migrates
  itself forward via `PRAGMA user_version`, and runs whose claimer went away
  are marked failed on the next start, while a run another process is holding
  a live lease on is left exactly alone
- **an op that failed two months ago can be re-run on the input that broke
  it**: `runner.replay(&run)` launches a new run of the ops that failed,
  seeded with what that run recorded, so the fix is tested against the value
  rather than against one reconstructed by hand. exactly those ops and nothing
  downstream. a resume is the opposite operation, re-running what did *not*
  succeed, and both are still there and told apart in the run log. the
  original run is never written to. what a replay does not reproduce is
  written down rather than left to be discovered: today's code, today's
  resources, today's clock, and today's answer from anything the op fetches
  itself, and a run whose values retention has taken is refused, naming the
  op whose input is gone, rather than run on a hole ([replay](docs/replay.md))
- **one event log for the whole deployment**, not just for runs. each event
  carries a `kind`, a documented json payload, and a *subject* (an asset, a
  schedule, a sensor, a backfill, a job, a run, or hestan itself), so "what
  happened last night" is one query: `GET /api/events?since=…&level=error`.
  each is written by the subsystem that does the work **in the transaction
  that does it**, because an event written next to the row instead is a claim
  a crash can falsify; the three places with no transaction to join are named
  in [docs/events.md](docs/events.md) with the window each leaves.
  `GET /api/events/stream` follows it live from a cursor, and the ui has an
  Activity view over it
- **a run is a distributed trace**, with `--features otel`: the run is a span,
  each attempt is a span beneath it, a retry is its own span, and an
  [isolated op](docs/isolation.md)'s subprocess is handed the w3c trace context
  so its spans nest under the op that spawned it, across the fork, which is
  the part nothing else does. hestan installs no exporter and no subscriber:
  you compose `tracing-opentelemetry` into yours, exactly as with `capture`
- **what an op *printed* is captured too**, and the two cases differ on
  purpose. an isolated op is a subprocess, so its stdout and stderr are piped
  and stored whole (`println!`, a linked c library, all of it), with both
  pipes drained concurrently, because reading one while the other fills its
  buffer deadlocks the child. an in-process op emits `tracing` events instead,
  and the `capture` feature offers a **layer you compose into your own
  subscriber**: hestan installs no global subscriber and redirects no file
  descriptor, so an in-process `println!` is *not* captured: hijacking fd 1
  would take the host application's output with it, and
  [docs/logs.md](docs/logs.md) says so rather than hiding it. capped per
  attempt (1 MiB and 10,000 lines by default), because an op in a print loop
  must not fill the disk the run log lives on
- the scheduler keeps a **durable cursor** per schedule: the newest occurrence
  it has accounted for. everything between the cursor and now is what downtime
  swallowed, so `Catchup::{Skip, One, All { limit }}` can say what to do about
  it, and a queue-policy fire held for a busy job is durable for the same
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
  staleness is provable from recorded fingerprints: a build materializes
  exactly the stale ancestors plus the target, seeding fresh values instead
  of recomputing them. source assets stand for external data and carry a
  cheap fingerprint probe
- an asset can own a **cron**: `Schedule::asset("vendor_prices", "0 6 * * *")`
  builds it at 06:00 because that is when the vendor publishes, which is a
  different question from the staleness the policies below answer. it is a
  `Schedule` in the same list, the same table and the same tick loop as a job's,
  and it plans exactly what `hestan build <asset>` plans
- **builds only wait for a build they intersect.** two families of assets with
  nothing in common build at the same time, and a backfill of one asset's 2019
  partitions no longer blocks every unrelated build in the deployment. what a
  build claims is every `(asset, key)` its plan will materialize, taken in the
  transaction that writes the run row and released by that row reaching a
  terminal status. four execute at once by default;
  `Hestan::max_concurrent_builds(n)` is the knob
- an **automation policy** says when an asset rebuilds itself:
  `AutoPolicy::when_stale()` (which is what `.auto()` is), `when_missing()` for
  the fresh deployment and the newly declared asset, `after_cron("0 2 * * *")`
  for "nightly, but do not rebuild what has not moved", and
  `.and_upstream_ready()` on any of them so a daily rollup waits for its last
  hour rather than recording a partial day. a partitioned asset is evaluated per
  key, so a pass builds the keys that qualify and leaves the ones that do not
- a **partitioned** asset materializes once per key of a daily, hourly or
  static key set, and a dep between two of them declares *which* keys it reads:
  `Asset::reads(&hourly, PartitionMapping::covering())` is a daily rollup of
  hourly data, with `offset(-1)` for yesterday's key and `all` for every key at
  once. staleness follows the mapping (a build records a fingerprint per
  upstream key it consumed, so the day is stale when an hour under it moves and
  says which hour), and a pairing that could never resolve fails at build
  rather than at 3am
- materializations are append-only history, capped per asset: each build
  leaves an entry, and every entry says whether the fingerprint actually
  moved, so "when did this last change" is a question with an answer, not
  the same question as "when was this last built"
- asset checks are assertions bound to an asset, handed the value it just
  produced: `AssetCheck::new("rows_present", "orders", |_, v| ..)` plus
  `Hestan::check(..)`. they lower into ops of the same build, so retries,
  cancellation and the gantt apply unchanged; `Severity::Error` fails the run
  and `Severity::Warn` just records the failure. a check runs when its asset
  rebuilds: a memoized asset is not re-checked
- sensors poll the world on an interval and launch runs when something
  changed, with a store-backed cursor committed only on fully successful
  evaluations. probes and run-status chains
  (`RunStatusSensor::new("chain", f).on([RunStatus::Success]).for_job("etl")`,
  or "when job A succeeds, run job B") are two more sources on that same loop, not
  loops of their own, so all three share pausing, cursors and tick history
- a sensor request can carry a run key (`RunRequest::new("publish").key(day)`),
  which turns at-least-once into effectively-once per sensor. the key is
  claimed in the same transaction that creates the run, so it can never name a
  run that did not launch
- due sensors evaluate concurrently, bounded, each under a timeout, so one
  slow closure cannot delay the rest. two evaluations of the same sensor never
  overlap: the loop skips a sensor that is still busy rather than queueing
  behind it
- freshness policies are declarations, not guesses: `fresh_within(d)` on a job
  or an asset says how old its latest success may get, and past that it is
  `late`, per key on a partitioned asset, so one late partition carries the
  asset. a declared policy replaces the cron-derived `overdue` heuristic
- `on_run_finished` fires for every terminal run with the status on the event,
  `on_op_finished` once per op **attempt** with its number, and both scope to
  one job with `JobBuilder::on_run_finished`, so an alert can cover prod
  without covering every backfill. `on_failure` is that path with a filter on
  it and is unchanged. `on_late` fires when something crosses into late, once
  per crossing, not per poll, and the state survives restarts. ready-made
  webhook and slack helpers behind the `http` feature serve all of them, and a
  run that succeeded does not read like an alarm
- `durable_notifications()` writes each run's event **in the same transaction
  as the run's terminal row** and delivers it from a loop that retries on the
  same backoff op retries use and gives up loudly after eight attempts, leaving
  the row visible as `failed` with its error. delivery is at-least-once and the
  docs say so next to the api rather than in a footnote. off by default: a hook
  that bumps a metric wants a callback, not a table
- **an asset's page says why it is stale as a chain of things that provably
  moved**: the dep whose content changed, the recorded fingerprint against the
  one it holds now, the build that fingerprint arrived in, and then the same
  question asked of that build. inferring staleness from clocks cannot say any
  of it. the catalog beside it searches, filters by state and by group, and
  folds a group in the graph, because one flat table is fine at twelve assets
  and useless at three hundred
- **an asset declares the group it belongs to**, so regrouping is not
  renaming: the name is the key every materialization is recorded under, and
  moving `sales/orders` into `finance` by renaming it would leave a new asset
  with no past. the name prefix is still the fallback, so a graph that never
  declares one groups exactly as it always did. from a source's group, which
  names the external system it stands for, hestan computes what every
  downstream asset descends from, and the ui marks a row by group or by
  origin: a mark never means status, the legend names every one of them, and
  turning it off loses nothing
- **backfills you can start**: drag a range across the partition grid, see
  which keys it covers and what it will cost from what a build of one has
  actually taken, then start it. no timings on record means it says so rather
  than quoting a guess, and an empty or already-fresh range is a disabled
  button with a reason rather than an error after the click
- the web ui is a prebuilt react bundle embedded in the binary; it polls the
  json api under `/api`
- **a dbt project's models are assets**, behind `--features dbt`:
  `Dbt::from_manifest("target/manifest.json")` reads the dag dbt already
  compiled and produces one asset per model with dbt's own lineage, each
  building by invoking `dbt run --select <model>` with its output captured on
  the run page. your dbt and your profile, invoked; see
  [docs/dbt.md](docs/dbt.md)
- **`GET /metrics` is prometheus**, in every build and behind no feature:
  queue depth and the age of the oldest queued run, claim latency, schedule
  lateness, reclaims, retries, store errors and run outcomes. no metric carries
  a job name, an asset name or a partition key, and the type of a label is what
  enforces that rather than a note. it sits **inside** the auth guard, because
  prometheus can hold a token where a kubelet cannot; see
  [docs/metrics.md](docs/metrics.md), which also says what to alert on
- **an address anyone can reach needs an authenticator, and `serve` refuses to
  start without one**: `Auth::bearer(token)` for one shared secret, or
  `Auth::custom(|req| …)` to compose the identities you already have. three
  roles (viewer reads, operator drives runs, admin changes how the deployment
  behaves), and a control a role may not use is not rendered in the ui rather
  than rendered and answering 403. `Identity::operator("ci").scoped_to(
  Scope::jobs(["deploy"]))` narrows it further: a token that may launch one job
  and is a stranger everywhere else, enforced in one place so a route added
  later is covered by the rule rather than by a list. see
  [docs/auth.md](docs/auth.md)
- **a namespace divides one deployment between teams, and a job or an asset
  says who owns it**: `Job::builder("etl").namespace("finance").owner(
  Owner::team("data-platform").contact("#data-alerts"))`, and a token scoped to
  `finance` reaches every job and asset in it without naming any of them. the
  owner is on the event a failure hook is handed, so an alert says who to wake
  without the caller threading a recipient through. a namespace is not a group:
  a group labels a picture and hestan draws it, a namespace divides the
  deployment and hestan enforces it. `.group("weather")` on a job labels the run
  timeline the way it labels the asset graph, and the jobs overview folds a
  group's lanes into one row. see [docs/namespaces.md](docs/namespaces.md)
- **a launch key makes a retried request harmless**: `hestan run deploy --key
  ci-build-4182`, or `{"key": "ci-build-4182"}` on the launch endpoint. same
  key, same job, same params, and there is one run: the second call is answered
  with the first one's id rather than launching beside it. the uniqueness is a
  database constraint taken in the same transaction as the run row, so two api
  processes racing one key still produce one run, and the same key with a
  different request is refused by name. see
  [docs/launching.md](docs/launching.md#launching-once)
- **a copy of the run log says it is a copy**: `hestan backup out.db` takes a
  consistent one while runs are being recorded (sqlite's online backup, not a
  `cp` of a WAL database), and a deployment refuses to come up on a restored
  one until `hestan resettle` has handed back the claims and the deciding lease
  it carries, because every one of them names a process that is somewhere else.
  what a copy does not contain, and the hazard of restoring one while workers
  are still up, are written down rather than implied. see
  [docs/backup.md](docs/backup.md)
- **a param an op declares secret does not reach the store**:
  `Op::secret_params(["token"])` puts `[hestan:redacted]` in `runs.params`,
  `schedules.params` and `presets.params` while the ops still read the value,
  so it is not on the run page, not on the api and not in the database. the
  redaction is in the store rather than in any renderer, which is what makes it
  something a new reader cannot walk around. it costs the run its replay, on
  purpose: see [docs/secrets.md](docs/secrets.md)

## More than one process

the demo is one process because that is right until it is not. when it is not:

```
docker compose up -d --build   # postgres, one scheduler, three workers
open http://localhost:4000
```

`Dockerfile` and `docker-compose.yml` are at the repo root, and it is one
image: a scheduler and its workers must build the same registry, so they
differ only by `HESTAN_ROLE`. it is still five containers on one host, which
[docs/scaling.md](docs/scaling.md) is careful about: on one host it was run,
and on several it follows.

what containers do buy is a network that can be taken away.
`deploy/checks/partition.sh` cuts the process holding the deciding lease off
the database while it keeps running, and its next decision comes back refused
by the store on the term it named. [docs/containers.md](docs/containers.md)
has the image, the stack and what the fault injection measured.

**a process stops when it is told to.** SIGTERM or SIGINT stops `serve` and
`work` accepting, finishes what they are already doing up to
`Hestan::stop_within`, hands the deciding lease back so the next process takes
over without waiting out the expiry, and puts anything that did not finish back
on the queue rather than leaving it claimed. the cost is that a process now
takes longer to exit than one that simply died, because it is finishing the
work; `deploy/checks/stop.sh` has the stop and the kill side by side.

## Docs

the details live in [docs/](docs/README.md):
[getting started](docs/getting-started.md),
[choosing](docs/choosing.md) (which of these do you want),
[concepts](docs/concepts.md) (execution semantics),
[typed io](docs/typed-io.md), [resources](docs/resources.md),
[connecting to your data](docs/connecting.md),
[io managers](docs/io-managers.md), [op state](docs/state.md),
[isolation](docs/isolation.md),
[metadata](docs/metadata.md), [events](docs/events.md), [logs](docs/logs.md),
[assets](docs/assets.md), [dbt](docs/dbt.md), [freshness](docs/freshness.md),
[sensors](docs/sensors.md),
[scheduling](docs/scheduling.md), [http sources](docs/http-sources.md),
[notifications](docs/notifications.md), [replay](docs/replay.md),
[launching](docs/launching.md),
[the web ui](docs/web-ui.md),
[the command line](docs/cli.md),
[namespaces and owners](docs/namespaces.md),
[authentication](docs/auth.md),
[the http api](docs/http-api.md), [metrics](docs/metrics.md),
[storage](docs/storage.md), [backup and recovery](docs/backup.md),
[scaling](docs/scaling.md), [containers](docs/containers.md),
[deployment and build identity](docs/deployment.md),
[embedding](docs/embedding.md), [stability](docs/stability.md), and
[development](docs/development.md).
release notes are in [CHANGELOG.md](CHANGELOG.md).

## Using it from your project

```toml
[dependencies]
hestan = "0.2.4"
```

the binary is yours: define jobs, then `Hestan::new()...serve(addr)`, or
`run_once(job, params)` for headless one-off runs. `features = ["cli"]` and
`hestan::cli::run(app, addr)` in place of `serve` gives that binary a
[command line](docs/cli.md) over the same registry.

everything optional is off, because every one of them is a dependency somebody
should get to decline:

| feature | what it adds |
| --- | --- |
| `bundled` | **on by default**: compiles sqlite from source. turned off, it links the system library instead, which then has to be installed where the linker can find it |
| `postgres` | `Store::connect`, for a run log several processes share ([storage](docs/storage.md)) |
| `cli` | `hestan::cli::run`, and the standalone `hestan` binary ([the command line](docs/cli.md)) |
| `capture` | `hestan::capture_layer`, storing the `tracing` events ops emit ([logs](docs/logs.md)) |
| `otel` | `hestan::otel`, a run as a distributed trace |
| `http` | `HttpSource` and the [notification](docs/notifications.md) helpers ([http sources](docs/http-sources.md)) |
| `parquet` | `ParquetIo`, op outputs stored as parquet files ([io managers](docs/io-managers.md)) |
| `dbt` | `hestan::dbt`, a dbt project's models as assets ([dbt](docs/dbt.md)) |

it is a 0.x, so a break is possible and arrives announced: it lands on a new
minor version, which `hestan = "0.1"` does not take on its own, and the first
line of that changelog entry names every type that moved and what to write
instead. [stability](docs/stability.md) is which types are a closed set,
which will grow and want a `_` arm, and which of the traits here are
contracts because somebody implements them.
