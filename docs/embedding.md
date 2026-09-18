# Embedding

Choose an entry point based on whether your application needs a server, a
command line or a single execution.

## cli::run

With the `cli` feature, `hestan::cli::run(app, addr)` adds commands over the
application's registry. With no arguments it calls `serve` on the supplied
address. It also handles isolated-operation subprocess startup before parsing
normal commands. See [CLI](cli.md).

## serve, run_once, Runner

| Entry point | Behavior |
| --- | --- |
| `Hestan::serve(addr)` | Opens and migrates the store, performs startup recovery and synchronization, starts role-specific loops, and serves the UI/API. |
| `Hestan::run_once(job, params)` | Performs startup work, executes one manual run and returns its final `Run`; no server or continuous scheduler. |
| `Hestan::build_asset(name)` | Performs one headless build of an asset and its stale ancestors. |
| `Runner::new(jobs, store)` | Creates an executor without startup recovery, synchronization or serving. Returns an error for duplicate jobs. |

`serve` binds and validates its address before starting background loops.
SIGTERM or SIGINT initiates [graceful shutdown](scaling.md#stopping-a-process-on-purpose).
`run_once` and `build_asset` install no signal handlers.

Configure [retention](storage.md#retention) to prune history; by default it is
kept. A serving process performs periodic sweeps in addition to startup work.

When constructing a `Runner` directly, declare pools with `Runner::with_pools`
and rates with `.with_rates(...)`. An operation using an undeclared pool or
rate fails at runtime. `Hestan` validates those declarations during build.

## Testing your jobs

`Store::open(":memory:")` makes job tests self-contained and fast. this is
exactly how hestan's own suite works:

```rust
use hestan::prelude::*;
use hestan::{OpStatus, RunStatus, Runner, Store, Trigger};

#[tokio::test]
async fn etl_handles_bad_rows() {
    let runner = Runner::new([my_etl_job()], Store::open(":memory:").unwrap()).unwrap();
    let run = runner.run("etl", json!({}), Trigger::Manual).await.unwrap();

    assert_eq!(run.status, RunStatus::Success);
    let ops = runner.store().op_runs(&run.id).unwrap();
    let load = ops.iter().find(|o| o.op == "load").unwrap();
    assert_eq!(load.status, OpStatus::Success);
    assert_eq!(load.output, Some(json!({"loaded": 3})));

    let events = runner.store().events(&run.id, 0).unwrap();
    assert!(events.iter().any(|e| e.message.contains("dropping bad row")));
}
```

`run` awaits completion, so there is nothing to poll; the store then answers
any question about what happened: op statuses, attempt counts, outputs,
events. for a test that needs the sweep or schedule sync, use
`Hestan::new()...run_once` with a `tempfile` path instead.

## Consuming from another repo

Use a registry dependency for releases (`cargo add hestan`), a path dependency
for local development, or a git dependency pinned to a revision. For example,
choose one of these entries in `[dependencies]`:

```toml
hestan = { path = "../hestan" }
# Or: hestan = { git = "https://github.com/quanalyst/hestan", rev = "<commit>" }
```

To override an existing git dependency locally, add a patch to the consumer's
`.cargo/config.toml`:

```toml
# .cargo/config.toml: local only, don't commit
[patch."https://github.com/quanalyst/hestan"]
hestan = { path = "../hestan" }
```

cargo resolves the git dependency to your working copy while the manifest
keeps the tag; delete the file and you're back on the pin.

See [stability](stability.md) for compatibility guarantees, enum matching and
extension-trait contracts.

## Where the database lives

`.db(target)` names the sqlite file, or with `--features postgres` a
`postgres://` url ([storage](storage.md#configuring-it)). the default is
`hestan.db` resolved against the process working directory, so a service
manager's `WorkingDirectory` decides where it lands. pass an absolute path if
that's ever ambiguous. WAL
mode means `-wal` and `-shm` sidecar files appear next to it while a process
has it open.

## Single-process assumptions

A store has one active decider and may have multiple workers. The
[deciding lease](scaling.md#the-deciding-lease) allows additional scheduler or
all-role processes to act as standbys. Workers claim runs individually, and
startup recovery preserves live claims.

[Isolated-operation subprocesses](isolation.md) execute one operation and do
not run scheduler or worker loops. Schedule overlap policies govern scheduled
launches; manual launches remain subject to concurrency limits.
