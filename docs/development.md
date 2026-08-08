# Development

## Repo layout

```
src/
  lib.rs        public exports and the prelude
  app.rs        the Hestan builder: collect jobs/schedules/assets/sensors, build, serve
  op.rs         Op, OpCtx, OpResult, typed io
  job.rs        Job and JobBuilder (dag validation)
  graph.rs      topo order (kahn's) and transitive-downstream
  executor.rs   Runner and the run loop: concurrency, retries, skips, subset runs
  asset.rs      Asset, the registry, staleness/planning, the materializing wrapper
  sensor.rs     Sensor, SensorCtx, the sensor loop (probes included)
  schedule.rs   cron parsing (5-field normalization, dow remap), scheduler loop
  store.rs      sqlite: schema, migrations, all reads and writes
  server.rs     axum router, api handlers, embedded ui fallback
  model.rs      Run/OpRun/Event/Tick/Materialization rows and the status enums
  http.rs       HttpSource (behind the http feature)
  notify.rs     webhook/slack failure hooks (behind the http feature)
  error.rs      the Error enum
ui/             react + vite app; ui/dist is committed and embedded
examples/       demo.rs, assets.rs, http_source.rs (needs --features http)
tests/          pipeline.rs, assets.rs; http_source.rs and notify.rs (need the http feature)
```

## Gates

`just check` runs exactly what ci runs. the `http` feature compiles real extra
code (and gates a test target and an example via `required-features`), so both
configurations are checked:

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features http -- -D warnings
cargo test
cargo test --features http
```

ci additionally rebuilds `ui/dist` and fails on a diff, so a stale committed
bundle can't ship.

the other justfile recipes: `just demo` (the demo on :4000), `just assets`
(the assets example on :4002), `just http-source`, `just ui-dev` (vite dev
server), `just ui-build` (rebuild `ui/dist` and touch `src/server.rs`),
`just build`.

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

## Adding a migration

migrations live in `src/store.rs` and run forward from `PRAGMA user_version`
on every open. to add v3:

1. write a `SCHEMA_V3` const with the DDL (`ALTER TABLE` / `CREATE TABLE`,
   the same style as `SCHEMA_V2`).
2. in `migrate`, add `if version < 3 { conn.execute_batch(SCHEMA_V3)?; }`
   and bump the final `pragma_update` to 3.
3. add tests like the existing ones: build a fixture database at the old
   version (see `phase1_db`), open it, assert old rows survive and new
   tables/columns work, then reopen to prove the migration doesn't run twice.

keep the v0 quirk in mind: version 0 plus an existing `runs` table means a
pre-versioning database and is stamped v1 before migrating — don't reuse
version 0 for anything.

## Tests

unit tests sit at the bottom of the module they exercise: `graph.rs`
(ordering, cycles), `schedule.rs` (cron normalization, dow remap, windows),
`store.rs` (lifecycle roundtrips, migrations, sweep), `server.rs` (handlers
called directly with axum extractors — no live server needed).

`tests/pipeline.rs` is the end-to-end executor suite against in-memory
stores: output passing, diamond ordering, failure/skip, retries, panics,
typed io, params rejection, op state, cancellation, failure hooks.
`tests/http_source.rs` spins up real axum servers on port 0 to exercise the
http retry policy, fan-out, and `bearer_env`; `tests/notify.rs` does the
same for the webhook and slack hooks. both only exist under
`--features http`.
