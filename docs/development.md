# Development

## Repo layout

| Path | Contents |
| --- | --- |
| `src/` | Library, executor, storage backends, HTTP server and optional CLI. |
| `ui/src/`, `ui/test/` | React UI and Node test suites. |
| `ui/dist/` | Committed bundle embedded in the crate. |
| `examples/` | Runnable applications; required features are listed in `Cargo.toml`. |
| `tests/` | Integration tests and fixtures. |
| `deploy/` | Container checks and Kubernetes examples. |
| `docs/` | User guides and references. |

`Cargo.toml` defines the package allowlist; `tests/docs.rs` checks it.
See [RELEASING.md](../RELEASING.md) for release validation.

## Gates

`just check` runs formatting, Clippy and Rust tests across the supported feature
combinations. `--no-default-features` is linted separately because it requires
system SQLite when linked.

| Command | Purpose |
| --- | --- |
| `just check-pg` | Run the shared store suite against PostgreSQL. |
| `just docs` | Build the docs.rs configuration on nightly, with warnings denied. |
| `just ui-test` | Run UI tests. |
| `just ui-build` | Rebuild and re-embed the committed UI bundle. |
| `just release-check` | Package the crate and build it offline in isolation. |
| `just demo`, `just assets`, `just http-source` | Run examples. |

CI also checks the minimum supported Rust toolchain, UI lint/build/tests and
that rebuilding leaves `ui/dist` unchanged. PostgreSQL tests require
`HESTAN_TEST_PG`; without it, their PostgreSQL cases skip.

## The ui loop

Run `just demo` and `just ui-dev` together. Vite proxies `/api` to port 4000.

After editing the UI, run `just ui-build` and commit `ui/dist` with the source
changes. This rebuilds the bundle and touches `src/server.rs` so Cargo embeds
it again. CI rejects a stale bundle.

## The store suite runs twice

Store tests use `both(...)` to exercise SQLite and PostgreSQL with the same
cases. Set a test database to enable the PostgreSQL half:

```sh
HESTAN_TEST_PG=postgres://user:pw@localhost/hestan_test cargo test --features postgres
```

Each case creates and drops an isolated schema. Add new store behavior to this
shared suite. `tests/queue.rs` also checks real worker processes against both
backends.

## Adding a migration

SQLite migrations live in `src/store.rs`; PostgreSQL's current schema and
forward migrations live in `src/pg.rs`. Both backends must support fresh stores
and upgrades from existing stores.

- Add the migration SQL and advance `SCHEMA_VERSION` and the stored schema stamp.
- Update PostgreSQL's fresh-store schema and its forward migration chain.
- Test an older fixture (see `legacy_db`): preserve its rows, check new fields
  and tables, then reopen it to confirm migration is idempotent.
- Update every affected PostgreSQL rewind fixture so it removes all objects
  newer than the schema it represents.


a column added to `runs` also wants `RUN_COLS`, `run_from_row`, the insert and
`RUN_COL_COUNT`, which is what the one query selecting a column *beside* a run
reads that column by. get the count wrong and the run's last column is read as
the other one, quietly, because both are nullable text; there is a case
asserting the two agree.

A schema stamp of zero with an existing `runs` table identifies a legacy
unstamped database. The migration code recognizes it as the initial schema;
do not reuse zero for another format.

## Tests

Unit tests live beside their implementation; integration tests cover execution,
assets, queue workers, isolation, authentication, shutdown and optional features.
HTTP integrations use local test servers. dbt tests use a stub executable and
do not validate a live warehouse.

Tests requiring a global tracing subscriber (`capture` and `otel`) run in
separate binaries to avoid callsite-cache interference. CLI tests launch real
processes to check stdout, stderr and exit codes.

Documentation is checked against code: server tests compare route tables;
`src/app.rs` compares the [connection example](connecting.md) with its doctest;
`tests/stability.rs` checks public enums and exit codes. Preserve those examples
and tables when editing their surrounding prose.

UI tests use `node:test`, bundled by Vite. Register suites in `ui/test/all.test.ts`
and run them with `just ui-test`.
