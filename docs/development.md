# Development

## Repo layout

```
src/
  lib.rs        public exports and the prelude
  app.rs        the Hestan builder: collect jobs/schedules/assets/sensors, build, serve
  op.rs         Op, OpCtx, OpResult, typed io
  job.rs        Job and JobBuilder (dag validation), Graph and graph flattening
  graph.rs      topo order (kahn's) and transitive-downstream
  io.rs         IoManager, Inline, FileIo, the per-op manager table
  resource.rs   process-wide resources and their constructors
  executor.rs   Runner and the run loop: concurrency, retries, skips, subset runs
  asset.rs      Asset, the registry, staleness/planning, the materializing wrapper
  sensor.rs     Sensor, SensorCtx, the sensor loop (probes included)
  schedule.rs   cron parsing (5-field normalization, dow remap), scheduler loop
  store.rs      the store: schema, migrations, all reads and writes
  pg.rs         the postgres half of it (behind the postgres feature)
  server.rs     axum router, api handlers, embedded ui fallback
  model.rs      Run/OpRun/Event/Tick/Materialization rows and the status enums
  http.rs       HttpSource (behind the http feature)
  notify.rs     webhook/slack failure hooks (behind the http feature)
  logs.rs       what an op printed: the op_logs writers and their cap
  otel.rs       trace context across the isolated-op boundary (behind otel)
  isolate.rs    isolated ops: the parent, the child, the output capture
  capture.rs    the tracing layer (behind the capture feature)
  auth.rs       who may drive this: the refusal, the two authenticators, the roles
  cli.rs        the command line: argv, the three modes, doctor, explain (behind cli)
  bin/hestan.rs the standalone operator binary (behind cli)
  error.rs      the Error enum
ui/             react + vite app; ui/dist is committed and embedded
examples/       demo.rs and assets.rs (both mount the cli, so both need
                --features cli), http_source.rs (needs --features http)
tests/          pipeline.rs, assets.rs, isolation.rs, queue.rs, auth.rs,
                docs.rs; http_source.rs and notify.rs (need the http
                feature); capture.rs (needs capture); otel.rs (needs otel);
                cli.rs (needs cli)
```

## Gates

`just check` runs the gates ci runs. `http`, `capture`, `postgres`, `otel`
and `cli` each compile real extra code, and all but `postgres` gate a test
target of their own via `required-features` — postgres's extra coverage is the
second half of the store suite instead. the crate has to be clean without any
of them as well as with them, so seven configurations are checked rather than
one:

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features http -- -D warnings
cargo clippy --all-targets --features capture -- -D warnings
cargo clippy --all-targets --features postgres -- -D warnings
cargo clippy --all-targets --features otel -- -D warnings
cargo clippy --all-targets --features cli -- -D warnings
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo test --features http
cargo test --features capture
cargo test --features postgres
cargo test --features otel
cargo test --features cli
cargo test --all-features
```

ci additionally runs the ui's own gates — `npm run lint`, `npm test` and
`npm run build` — and fails on a `ui/dist` diff, so a stale committed bundle
can't ship. two things it does that `just check` cannot: it sets
`HESTAN_TEST_PG`, so the postgres half of the store suite actually runs
(`just check-pg` is the local equivalent), and it compiles the whole thing on
the msrv toolchain, so a newer language feature slipping in is caught here
rather than by whoever is pinned.

the other justfile recipes: `just check-pg` (the store suite with a real
postgres behind it), `just demo` (the demo on :4000), `just assets` (the
assets example on :4002), `just http-source`, `just ui-dev` (vite dev server),
`just ui-test` (the ui suites), `just ui-build` (rebuild `ui/dist` and touch
`src/server.rs`), `just build`.

## The ui loop

`cd ui && npm run dev` starts vite on its own port with `/api` proxied to
`localhost:4000` — run `just demo` alongside and edit with hot reload. the
production bundle is a separate step: `npm run build` (that's `tsc -b` then
`vite build`) regenerates `ui/dist`, which is committed so cargo users never
need node.

the bundle is embedded at compile time via `include_dir!` in `src/server.rs`,
and cargo does not track the embedded files — after rebuilding the ui,
`src/server.rs` has to be touched so the next `cargo build` re-embeds it.
`just ui-build` does both:

```
cd ui && npm run build
touch src/server.rs
```

skipping the touch serves the stale bundle. it looks exactly like your ui
change not working.

`ui/dist` is committed, so rebuild it in the same commit as any `ui/src`
change — ci fails on a `ui/dist` diff.

## The store suite runs twice

every case in `src/store.rs`'s suite runs against both backends: sqlite
always, and postgres when `HESTAN_TEST_PG` names a server.

```
HESTAN_TEST_PG=postgres://user:pw@localhost/hestan_test cargo test --features postgres
```

unset, the postgres half skips itself and the suite passes on a machine with
no postgres — which is what keeps it from being something only ci runs. each
case gets a schema of its own on that server, dropped with the fixture, so
they run in parallel and none of them can see another's rows.

it is one suite run twice rather than two suites, and that is the whole
verification strategy for the second backend. a second set of cases is exactly
how two backends come to disagree: the case nobody copied across is the one
nobody misses. so when you add a store method, put its case in `both(...)`
with the rest — the point at which a query has never been run against postgres
is the point at which nobody knows whether it works.

`tests/queue.rs` does the same at the other end of the scale: its three cases
run once against a sqlite file and once against a postgres schema, with real
worker processes racing one queue either way.

## Adding a migration

migrations live in `src/store.rs` and run forward from `PRAGMA user_version`
on every open. **this is the sqlite chain**; a postgres database is created
whole at the current version by `src/pg.rs`, so a new step means editing that
schema as well. to add the next one — call it vN, one past whatever
`SCHEMA_VERSION` says today:

1. write a `SCHEMA_VN` const with the DDL (`ALTER TABLE` / `CREATE TABLE`,
   the same style as the one below it).
2. in `migrate`, add `if version < N { conn.execute_batch(SCHEMA_VN)?; }`
   and bump the final `pragma_update` to N.
3. add the same columns or tables to `SCHEMA` in `src/pg.rs`, and bump
   `SCHEMA_VERSION`. a fresh postgres database is stamped with it; an existing
   one needs a forward step of its own, in `pg::migrate`.
4. add tests like the existing ones: build a fixture database at the old
   version (see `phase1_db`), open it, assert old rows survive and new
   tables/columns work, then reopen to prove the migration doesn't run twice.

keep the v0 quirk in mind: version 0 plus an existing `runs` table means a
pre-versioning database and is stamped v1 before migrating — don't reuse
version 0 for anything.

## Tests

unit tests sit at the bottom of the module they exercise — most modules have
some, and these are the ones worth knowing about: `graph.rs` (ordering,
cycles), `job.rs` (graph flattening: prefixes, wiring, nesting, rejected
shapes), `schedule.rs` (cron normalization, dow remap, windows), `op.rs` (the
metadata tags, which are a wire format, and the delta arithmetic),
`auth.rs` (every spelling of loopback, and what each role may), `store.rs`
(lifecycle roundtrips, migrations, sweep, claims under real contention —
[twice](#the-store-suite-runs-twice)), `server.rs` (handlers called directly
with axum extractors — no live server needed, and the two scrapers that hold
`docs/auth.md` and `docs/http-api.md` to the router).

`tests/pipeline.rs` is the end-to-end executor suite against in-memory
stores: output passing, diamond ordering, failure/skip, retries, panics,
typed io, params rejection, op state, cancellation, failure hooks.
`tests/http_source.rs` spins up real axum servers on port 0 to exercise the
http retry policy, fan-out, and `bearer_env`; `tests/notify.rs` does the
same for the webhook and slack hooks. both only exist under
`--features http`.

`tests/otel.rs` is the span tree a run opens, against a real subscriber, and it
is a binary of its own for the same reason `tests/capture.rs` is one — see
below. it asserts the shape (`hestan.run`, an `hestan.op` per attempt beneath
it, events on the span they belong to) and exports nothing: turning that tree
into otel spans is `tracing-opentelemetry`'s job.

`tests/auth.rs` is a binary of its own for a third reason: it has to be both
halves of what it tests. a child process serves an
[authenticated](auth.md) deployment with every tracing line on its stdout, and
the parent drives that deployment over http and then greps both of the child's
streams, every response it sent, every event and run row, and every byte of the
database file for the token. a credential that is checked correctly and then
written into a log line is a credential in a log aggregator, and hestan cannot
take it back out.

`tests/cli.rs` is the [command line](cli.md) as a process, and it is a binary
of its own for a reason of its own: an exit code is not something a function
call has. every case starts this same binary with argv, reads both of its
streams and reads what it exited with, so each documented exit code is asserted
against a real process rather than against a return value. the conditions
`doctor` reports are constructed and asserted in `src/cli.rs` instead, where a
bad timezone or a claim past its lease can be written straight into a store.

`tests/capture.rs` is the [capture layer](logs.md) against a real subscriber,
and it is a binary of its own for a reason worth knowing: `tracing` caches a
callsite's interest the first time that callsite is hit, using whatever
subscriber the thread that hit it had. in a binary where hundreds of other
tests run ops with no subscriber installed, the executor's op span would be
registered as "nobody is interested" by whichever thread got there first, and
the layer's cases would fail about one run in three.

the ui has suites of its own under `ui/test/`, run with `npm test` (or
`just ui-test`) and registered one import each in `ui/test/all.test.ts`: the
[markdown subset](metadata.md#the-markdown-subset) construct by construct with
the injection cases asserted against the exact string react renders, the
metadata row, the run page's log merge, the activity feed's merge and filters,
the backfill range arithmetic, the catalog's grouping and filters, the lineage
walk, and what each role may see. they
need no test framework and no browser — vite bundles them for node with the
same config the app is built with, and `node:test` runs them.
