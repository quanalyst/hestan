# Embedding

hestan is a library; the binary is yours. there are three levels of entry,
each dropping a layer of machinery, and one that wraps the first, so the
binary you already have gains a [command line](cli.md) over the registry inside
it.

## cli::run

`hestan::cli::run(app, addr)`, behind `features = ["cli"]`, is
[`serve`](#serve-run_once-runner) with argv in front of it. **with no arguments
it is exactly `app.serve(addr)`** (the same address, the same loops, the same
error if the socket will not bind), so swapping the call in a running
deployment changes nothing about how it behaves. with arguments it launches,
inspects and diagnoses against the jobs compiled into that binary. see
[the command line](cli.md).

it asks the [isolated-op](isolation.md) guard before it looks at argv, like
every other entry point does, because an op subprocess is this binary
re-executed with no arguments at all, and no arguments is how a command line
spells "serve".

## serve, run_once, Runner

`Hestan::new()...serve(addr)` is the full deal: open and migrate the
database, sweep runs that a dead process left behind, sync the schedules and
sensors tables to the code, start the loops this process's
[role](scaling.md#roles) owns (the scheduler, the sensors, the backfill
chunker, the freshness checker, the
[policy](assets.md#automation-policies) pass and the retention sweeper for a
role that decides, the queue dispatcher for one that executes, and the lease loop
whatever the role is), and serve the ui and api.
`.retention(Retention::days(n))` folds one more step into that startup work:
terminal runs older than `n` days are pruned before anything new launches, and
the sweeper keeps pruning every hour after that (the default keeps
everything; see [storage](storage.md#retention)).
it binds the listener *before* spawning the loops, so a bind failure
(port taken) can't leave a detached loop firing jobs into a server that never
started. it is the **bound** address that is checked against the
[authenticator](auth.md), so `serve` refuses `Error::Unguarded` on an address
anyone can reach with nothing configured. when serve returns, every loop is
aborted with it.

**and `serve` returns when the process is signalled.** SIGTERM or SIGINT stops
it accepting, finishes what it is holding up to `Hestan::stop_within`, hands
the deciding lease back and puts what it could not finish back on the queue;
[scaling](scaling.md#stopping-a-process-on-purpose) has the order. `run_once`
and `build_asset` install no handler at all, and a signal ends them where it
finds them: they exist to execute the run they were asked for.

`Hestan::new()...run_once(job, params)` builds the same way (including the
crash sweep, the schedule/sensor sync and one retention sweep) but runs a
single manual run to completion and returns the final `Run`. no server, no
scheduler, no sensor loop. good for cron-driven containers or one-off
backfills where hestan is the executor and something else owns the clock.
`Hestan::new()...build_asset(name)` is the same shape for
[assets](assets.md): one headless build run of the named asset and its
stale ancestors.

`Runner::new(jobs, store)` is the bare executor: no sweep, no schedule sync,
no server. `launch` and `run` as described in [concepts](concepts.md), with
`runner.store()` for reading history back. it returns
`Result<Runner, Error>`, and two jobs of one name is `Error::DuplicateJob`:
the same answer `Hestan` gives, since which one you would have got otherwise
depends on the order they were handed over.

`Runner::new` declares no [concurrency pools](concepts.md#concurrency-pools),
so an op with `.pool(name)` fails at run time with
`op takes from pool {name}, which is not declared`. running it unlimited
would quietly break the promise the pool exists to keep. use
`Runner::with_pools(jobs, store, hooks, [("api".into(), 3)])`, which validates
every op's pool up front and returns `Error::Graph` if one is missing;
`Hestan::pool(name, limit)` is the same check at build.

[rates](concepts.md#rates) work the same way and are declared on the runner
rather than in a constructor, because they compose with whichever one you
used: `Runner::new(jobs, store)?.with_rates([("api".into(), 5, Duration::from_secs(1))])?`.
an op with `.rate(name)` on a runner that declares none fails at run time for
the same reason an undeclared pool does.

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

the path and git dependency forms are in
[getting started](getting-started.md). for local iteration against a pinned
git dep (hacking on hestan and the consumer at once), put a patch in the
consumer's `.cargo/config.toml` rather than editing `Cargo.toml`:

```toml
# .cargo/config.toml: local only, don't commit
[patch."https://github.com/quanalyst/hestan"]
hestan = { path = "../hestan" }
```

cargo resolves the git dependency to your working copy while the manifest
keeps the tag; delete the file and you're back on the pin.

either way you are pinning a 0.x, where the minor number is the compatibility
number: `cargo update` moves you within `0.1.x` without asking and stops at
`0.2.3`. [stability](stability.md) is what that costs and what it does not:
which surfaces hold still, which types are a closed set, and which of the
traits you implement are contracts.

## Where the database lives

`.db(target)` names the sqlite file, or with `--features postgres` a
`postgres://` url ([storage](storage.md#configuring-it)). the default is
`hestan.db` resolved against the process working directory, so a service
manager's `WorkingDirectory` decides where it lands. pass an absolute path if
that's ever ambiguous. WAL
mode means `-wal` and `-shm` sidecar files appear next to it while a process
has it open.

## Single-process assumptions

one *decider* at a time per database, and as many executors as you like. that
split is what [scaling](scaling.md) is about: schedules, sensors, freshness
checks, automation policies and backfill chunking are decisions, and the
deployment makes each one once. which process makes them is settled by a
[lease in the store](scaling.md#the-deciding-lease), so starting a second
`Role::All` or `Role::Scheduler` gives you a warm spare rather than two of
every scheduled run. any number may be `Role::Worker`, because a run is claimed
by exactly one of them and the startup sweep respects a live claim rather than
assuming it is alone.

hestan's own extra process is a third thing again: an [isolated
op](isolation.md) runs in an op subprocess that opens the same file, takes
neither path, and runs one op. within one process, a job slower than its own
cron interval is handled by its overlap policy (skip by default; see
[scheduling](scheduling.md)); manual launches are never gated, so those can
still overlap a running job.
