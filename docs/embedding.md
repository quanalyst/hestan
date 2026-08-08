# Embedding

hestan is a library; the binary is yours. there are three levels of entry,
each dropping a layer of machinery.

## serve, run_once, Runner

`Hestan::new()...serve(addr)` is the full deal: open and migrate the
database, sweep runs that a dead process left behind, sync the schedules
and sensors tables to the code, start the in-process scheduler and the
sensor loop, and serve the ui and api.
`.retention_days(n)` folds one more step into that startup work: terminal
runs older than `n` days are pruned before anything new launches (the
default keeps everything — see [storage](storage.md)).
it binds the listener *before* spawning the loops, so a bind failure
(port taken) can't leave a detached loop firing jobs into a server that never
started. when serve returns, both loop tasks are aborted with it.

`Hestan::new()...run_once(job, params)` builds the same way — including the
crash sweep and the schedule/sensor sync — but runs a single manual run to
completion and returns the final `Run`. no server, no scheduler, no sensor
loop. good for cron-driven containers or one-off backfills where hestan is
the executor and something else owns the clock.
`Hestan::new()...build_asset(name)` is the same shape for
[assets](assets.md): one headless build run of the named asset and its
stale ancestors.

`Runner::new(jobs, store)` is the bare executor: no sweep, no schedule sync,
no server. `launch` and `run` as described in [concepts](concepts.md), with
`runner.store()` for reading history back. one behavioral difference from
`Hestan`: registering two jobs with the same name in a `Runner` keeps the
last and logs a warning, while `Hestan` refuses to build
(`Error::DuplicateJob`).

## Testing your jobs

`Store::open(":memory:")` makes job tests self-contained and fast — this is
exactly how hestan's own suite works:

```rust
use hestan::prelude::*;
use hestan::{OpStatus, RunStatus, Runner, Store, Trigger};

#[tokio::test]
async fn etl_handles_bad_rows() {
    let runner = Runner::new([my_etl_job()], Store::open(":memory:").unwrap());
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
any question about what happened — op statuses, attempt counts, outputs,
events. for a test that needs the sweep or schedule sync, use
`Hestan::new()...run_once` with a `tempfile` path instead.

## Consuming from another repo

the path and git dependency forms are in
[getting started](getting-started.md). for local iteration against a pinned
git dep — hacking on hestan and the consumer at once — put a patch in the
consumer's `.cargo/config.toml` rather than editing `Cargo.toml`:

```toml
# .cargo/config.toml — local only, don't commit
[patch."https://github.com/quanalyst/hestan"]
hestan = { path = "../hestan" }
```

cargo resolves the git dependency to your working copy while the manifest
keeps the tag; delete the file and you're back on the pin.

## Where the database lives

`.db(path)` names the sqlite file; the default is `hestan.db` resolved
against the process working directory, so a service manager's `WorkingDirectory`
decides where it lands — pass an absolute path if that's ever ambiguous. WAL
mode means `-wal` and `-shm` sidecar files appear next to it while a process
has it open.

## Single-process assumptions

one hestan process per database. the store is a single connection behind a
mutex — one writer, by design — and the scheduler runs in-process with no
coordination: two processes sharing a file would each fire every schedule
(double runs), and each startup sweep would mark the *other* process's live
runs as interrupted. within one process, a job slower than its own cron
interval is handled by its overlap policy (skip by default — see
[scheduling](scheduling.md)); manual launches are never gated, so those can
still overlap a running job.
