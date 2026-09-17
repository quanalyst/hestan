# hestan

[![ci](https://img.shields.io/github/actions/workflow/status/quanalyst/hestan/ci.yml?branch=main&label=ci)](https://github.com/quanalyst/hestan/actions/workflows/ci.yml) [![crates.io](https://img.shields.io/crates/v/hestan.svg)](https://crates.io/crates/hestan) [![docs.rs](https://img.shields.io/docsrs/hestan)](https://docs.rs/hestan)

Hestan is a Rust library for running jobs with dependencies, schedules and
persistent history. Define work as async Rust functions and inspect it through
an embedded web UI, JSON API or optional CLI.

It runs inside your application. Start with one process and a SQLite file;
use PostgreSQL and worker processes when you need to distribute execution.

## Quickstart

Add these dependencies to your Rust project:

```toml
[dependencies]
hestan = "0.2.5"
tokio = { version = "1", features = ["full"] }
```

Put this in `src/main.rs`:

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

Run `cargo run` and open <http://127.0.0.1:4000>. Launch `etl` from the UI or
wait for its schedule, which fires every ten minutes in UTC. `load` runs after
`extract` succeeds and can retry twice. History is stored in `hestan.db`.

## How it works

- **Jobs** connect operations into a dependency graph validated at startup.
  Ready operations run concurrently, with retries, timeouts and configurable
  concurrency limits. [Execution semantics](docs/concepts.md).
- **Schedules and sensors** launch jobs on a cron schedule or in response to
  polled changes and run outcomes. [Scheduling](docs/scheduling.md) ·
  [Sensors](docs/sensors.md).
- **Assets** describe data and its dependencies. Recorded fingerprints track
  staleness; builds can reuse fresh upstream values. Assets also support
  partitions, backfills, checks and freshness policies. [Assets](docs/assets.md).
- **History and inspection** include run timelines, logs, output metadata,
  notifications and Prometheus metrics. [Web UI](docs/web-ui.md) ·
  [Notifications](docs/notifications.md) · [Metrics](docs/metrics.md).
- **Presentation metadata** gives jobs and assets readable display names,
  groups, optional subgroups and labels. Grouping views organize the same
  registrations without changing identity, dependencies or permissions.
  [Display names and grouping](docs/presentation.md).

Use your own clients inside operations. Share connections through
[resources](docs/resources.md), declare [typed inputs and outputs](docs/typed-io.md),
and choose an [I/O manager](docs/io-managers.md) for stored outputs.

## Using it from your project

`Hestan::serve` runs the scheduler, workers, API and UI in one process by
default. Use `run_once` for a headless invocation, or enable `cli` to give your
application commands over the same registry. See [embedding](docs/embedding.md).

SQLite is bundled by default. Disable default features to link a system SQLite
installation; all other features are opt-in.

| feature | what it adds |
| --- | --- |
| `bundled` | Compiles SQLite from source; enabled by default. |
| `postgres` | A PostgreSQL run store shared by multiple processes. [Storage](docs/storage.md). |
| `cli` | Commands in your application and a standalone operator binary. [CLI](docs/cli.md). |
| `capture` | A tracing layer that stores operation logs. [Logs](docs/logs.md). |
| `otel` | OpenTelemetry tracing for runs and operation attempts. |
| `http` | Scheduled HTTP sources and HTTP notification helpers. [HTTP sources](docs/http-sources.md). |
| `parquet` | `ParquetIo` for storing operation outputs as Parquet files. [I/O managers](docs/io-managers.md). |
| `dbt` | Registers dbt models as assets from a manifest. [dbt](docs/dbt.md). |

The UI and API are unauthenticated on loopback by default. Serving on another
address requires an authenticator or an explicit opt-out. See
[authentication](docs/auth.md), [security](SECURITY.md) and the
[stability policy](docs/stability.md).

## Running the demo

From a repository checkout:

```sh
cargo run --example demo --features cli
```

Open <http://127.0.0.1:4000> to watch scheduled jobs and retries.
For assets, run `cargo run --example assets --features cli` from the repository
root and open <http://127.0.0.1:4002>.

The [presentation example](examples/presentation.rs) demonstrates display names,
subgroups and labels: `cargo run --example presentation`.

## More than one process

Schedulers decide what to launch; workers claim queued runs from the shared
store. Processes must register the same jobs and assets. Use PostgreSQL when
workers run on multiple hosts. See [scaling](docs/scaling.md) for leases,
concurrency limits and per-process rate limits.

The repository includes [Docker and Compose examples](docs/containers.md):

```sh
docker compose up -d --build
```

## Docs

Start with [getting started](docs/getting-started.md) or
[choosing components](docs/choosing.md). The [documentation index](docs/README.md)
covers the full API and operating guides.

[Rust API](https://docs.rs/hestan) · [HTTP API](docs/http-api.md) ·
[Development](docs/development.md) · [Changes](CHANGELOG.md) · [MIT license](LICENSE)
