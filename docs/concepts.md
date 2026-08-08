# Concepts

## Vocabulary

an *op* is a named async fn plus the upstream ops it waits on: `Op::new` or
`Op::typed`, then `.after([...])`, `.retries(n)`, `.retry_delay(d)`,
`.params::<P>()`. it receives an `OpCtx` and returns json output or an error.

a *job* is a validated dag of ops. `Job::builder(name)...build()` rejects
duplicate op names, deps on unknown ops, and cycles, and fixes a deterministic
topological order (ties broken by declaration order).

a *run* is one execution of a job: an id (uuid v7, so ids sort by creation
time), params (arbitrary json), a trigger, and a status that moves
`queued -> running -> success | failed | canceled`. each op within a run gets
an *op run* row: status
(`pending | running | success | failed | skipped | canceled`), attempt count,
start/finish times, and the output or error.

*events* are the append-only log attached to a run. each carries a level
(`info | warn | error`), a kind, a message, and optional structured json
`data`. the kinds: `run_queued`, `run_started`, `run_success`, `run_failed`,
`run_canceled`, `op_started`, `op_retry`, `op_success`, `op_failed`,
`op_skipped`, `op_canceled`, `type_check_failed`, and `log` — the last is
what `ctx.info/warn/error` emit.

the *trigger* records why a run exists: `manual` (launch endpoint,
`run_once`), `schedule` (the cron scheduler), `retry` (the re-run endpoint),
`build` (an [asset build](assets.md): the endpoints, `build_asset`, and
probe-driven auto builds), or `sensor` (a [sensor](sensors.md) evaluation
asked for it). retry is for finished runs — the api answers 409 for one
still queued or running, since a fresh copy of a live run would only double
it. manual launches stay ungated: that is the documented escape hatch when
an overlapping run is really wanted.

an *asset* is an op with identity: a persisted latest value, a fingerprint,
and explicit lineage on other assets, which makes staleness provable and
builds incremental — stale ancestors plus the target, fresh values seeded.
asset builds run as ordinary runs of an internal job named `assets`, so
everything on this page applies to them unchanged. the model has
[its own page](assets.md).

all of it lands in sqlite as it happens — see [storage](storage.md).

## OpCtx

each op invocation gets a context carrying the run id, the run params, and the
outputs of its declared deps:

```rust
Op::new("load", |ctx| async move {
    let rows = ctx.input("extract").cloned().unwrap_or_default(); // raw Value
    let rows: Vec<Order> = ctx.input_as("extract")?;              // or typed
    let p: MyParams = ctx.params_as()?;                           // run params
    ctx.info("loading");                                          // log event
    Ok(json!({ "ok": true }))
})
```

only declared deps are visible. `ctx.input` on an op you did not `.after`
returns `None` even if that op ran.

## How a run executes

every op whose deps have all produced output is spawned as its own tokio
task, so independent branches run concurrently — in a diamond
`a -> {b, c} -> d`, `b` and `c` run at the same time.
`Job::builder(..).max_parallel(n)` caps how many ops of one run are in
flight at once, and ready ops over the cap wait their turn in readiness
order (first ready, first spawned). without a cap, everything ready runs
together. an op's output is handed to its dependents directly in memory (and
persisted as a side effect); downstream ops never read the database.

when an op exhausts its attempts, its transitive downstream is marked
`skipped`, each with an `op_skipped` event naming the failed root, and the
run will be failed. branches that don't depend on the failed op keep running
to completion.

retries are extra attempts: `.retries(2)` means up to 3 total. a failed
attempt emits `op_retry` (`data: {"attempt": n}`), sleeps the fixed
`.retry_delay` (default 1s, no backoff), and tries again; the last failure
emits `op_failed` (`data: {"error": msg}`). the op run's `started_at` is kept
from the first attempt, so its recorded duration spans all of them. a panic
in the op body is caught, turned into an `op panicked: ...` error, and goes
through exactly the same retry policy as a returned `Err`.

a run is `failed` if any op finished `failed`, otherwise `success` — unless
it was canceled, which wins over both. the terminal event
(`run_failed` / `run_success` / `run_canceled`) is committed before the
terminal status, so anything that observes a finished run can also read its
closing event.

## Cancellation

`runner.cancel(run_id)` (or `POST /api/runs/{id}/cancel`) asks a queued or
running run to stop. in-flight ops are aborted, every op that isn't terminal
yet (running and pending alike) is marked `canceled` with error `"canceled"`
and an `op_canceled` event, then the run gets its `run_canceled` event and
finishes with status `canceled`. retry sleeps die with the abort, so a
canceled op mid-backoff doesn't linger.

the abort lands at the op's next await point. an op that is inside a
blocking section (a long computation, a synchronous db call) finishes that
section first and disappears at the next `.await`; an op that never awaits
runs to completion. an op that finishes in the instant between the cancel
request and the abort keeps its real result: its success (and any staged
[state](state.md)) is recorded, not overwritten with `canceled`.

`cancel` reports what it did: `Requested` (signal sent), `AlreadyFinished`
(terminal already, or a run left over from before a restart), `Unknown` (no
such run). canceled is terminal — there is no resume; the retry endpoint
launches a fresh run with the same params. a canceled run counts as inactive
for the scheduler's [overlap policy](scheduling.md), and
[failure hooks](notifications.md) do not fire for it.

## launch() vs run()

`Runner` is what `Hestan` drives internally, and is usable directly. it
exposes both:

```rust
let id = runner.launch("etl", json!({}), Trigger::Manual)?;      // fire and forget
let run = runner.run("etl", json!({}), Trigger::Manual).await?;  // await the result
```

`launch` creates the run row (status `queued`, with its `run_queued` event in
the same transaction), spawns execution, and returns the run id immediately.
`run` does the same and then awaits completion, returning the final `Run`.
execution is spawned onto the runtime rather than driven by the returned
future, so dropping that future (a timeout, a `select!` losing) detaches the
run: it finishes in the background instead of being aborted mid-write.

both validate params before the run row is written. if any op declared
`.params::<P>()` and the given params don't deserialize, the launch fails with
`Error::InvalidParams` and leaves no trace in the database. launching an
unregistered job is `Error::UnknownJob`.
