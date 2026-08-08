# Concepts

## Vocabulary

an *op* is a named async fn plus the upstream ops it waits on: `Op::new` or
`Op::typed`, then `.after([...])`, `.retries(n)`, `.retry_backoff(base, max)`
or `.retry_delay(d)`, `.timeout(d)`, `.pool(name)`, `.params::<P>()`. it
receives an `OpCtx` and returns json output or an error.

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
`resume` (the [resume endpoint](#resume), which continues an earlier run),
`build` (an [asset build](assets.md): the endpoints, `build_asset`, and
probe-driven auto builds), or `sensor` (a [sensor](sensors.md) evaluation
asked for it). retry and resume are for finished runs — the api answers 409
for one still queued or running, since a fresh copy of a live run would only
double it. manual launches stay ungated: that is the documented escape hatch
when an overlapping run is really wanted.

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
attempt emits `op_retry` (`data: {"attempt": n}`), sleeps, and tries again;
the last failure emits `op_failed` (`data: {"error": msg}`). the op run's
`started_at` is kept from the first attempt, so its recorded duration spans
all of them. a panic in the op body is caught, turned into an
`op panicked: ...` error, and goes through exactly the same retry policy as a
returned `Err`.

the pause between attempts is capped exponential backoff with full jitter by
default: the nth retry waits a uniformly random slice of `1s * 2^n`, never
more than 30s. `.retry_backoff(base, max)` sets the two numbers.
`.retry_delay(d)` is the fixed-pause alternative — the same wait every time,
no jitter. prefer the default: ops that fail together (one rate limit, one
dead dependency) also retry together under a fixed delay, and hammer whatever
knocked them over on the same second; jitter is what pulls the herd apart.

`.timeout(d)` fails an attempt that runs longer than `d` with a
`timed out after 30s` error, which then goes through the retry policy like
any other failure. without one, a hung op runs forever: it holds its
`max_parallel` slot, and its run stays active, so a schedule on
[`Overlap::Skip`](scheduling.md) never fires again. the clock starts when the
op starts running, so time spent waiting for a pool permit is not counted
against it. expiry also trips `ctx.is_cancelled()` — see
[cancellation](#cancellation) for what that does and does not stop.

a run is `failed` if any op finished `failed`, otherwise `success` — unless
it was canceled, which wins over both. a failed run carries an `error` of its
own: the first op that terminally failed, as `op {name} failed: {message}`,
which is the same pair an [`on_failure` hook](notifications.md) receives. the
terminal event (`run_failed` / `run_success` / `run_canceled`) is committed
before the terminal status, so anything that observes a finished run can also
read its closing event.

## Concurrency pools

`max_parallel` is a property of one job. the limit that usually matters is a
property of something outside every job — "at most 3 requests in flight to
this api, ever" — and two jobs that overlap give 3 + 3.

a *pool* is that budget, declared once and shared by every job in the
process:

```rust
Hestan::new()
    .pool("eia_api", 3)
    .job(Job::builder("hourly").op(Op::new("pull", ..).pool("eia_api")).build()?)
    .job(Job::builder("backfill").op(Op::new("pull", ..).pool("eia_api")).build()?)
```

an op with `.pool(name)` takes a permit before its body runs and gives it
back when the attempt ends — however it ends: success, failure, panic,
timeout, or cancel. the permit is per attempt, so an op backing off between
retries is not sitting on the resource it is backing off from. naming a pool
that was never declared is `Error::Graph` at build time, as is declaring the
same pool twice; a limit below 1 means 1.

pools compose with `max_parallel`: an op waits for both, in that order — a
slot in its own run first, then a permit. an op waiting for a permit does
hold its `max_parallel` slot, which can idle a job, but it cannot deadlock:
permits are only ever held by ops that are already running and are on their
way to releasing one, and nothing that holds a permit ever waits for a slot.
the wait order is the same everywhere, so there is no cycle to close. (the
permit is deliberately taken inside the op's own task rather than in the
run's scheduling loop; taking it in the loop would stop that loop from
reaping the very ops whose permits it is waiting for, and *that* would
deadlock.)

an op that finds the pool full logs `waiting for a {pool} pool permit`, so a
queued op reads as queued instead of as an op mysteriously stuck in
`running`. `GET /api/jobs/{name}` reports each op's `pool` and the job's
`pools` with their limits, and the op inspector shows both.

## Cancellation

`runner.cancel(run_id)` (or `POST /api/runs/{id}/cancel`) asks a queued or
running run to stop. in-flight ops are aborted, every op that isn't terminal
yet (running and pending alike) is marked `canceled` with error `"canceled"`
and an `op_canceled` event, then the run gets its `run_canceled` event and
finishes with status `canceled`. retry sleeps die with the abort, so a
canceled op mid-backoff doesn't linger.

what actually stops, and what hestan claims about it, depends on the op:

- an **async op** is dropped at its next await point. that is real
  cancellation, and it is what "canceled" means on its op run row. it can
  also `select!` on `ctx.cancelled()` to unwind on purpose.
- a **blocking op** — `spawn_blocking`, a long computation, a synchronous
  driver — cannot be dropped at all. tokio has nothing to interrupt: the
  closure owns its thread until it returns. the only thing that stops it is
  the closure itself, polling `ctx.is_cancelled()` and bailing out:

  ```rust
  Op::new("crunch", |ctx| async move {
      tokio::task::spawn_blocking(move || {
          for chunk in chunks {
              if ctx.is_cancelled() { return Err("canceled".into()); }
              crunch(chunk)?;
          }
          Ok(json!({"done": true}))
      }).await?
  })
  ```

  `is_cancelled()` is a watch-channel read: cheap enough for an inner loop,
  and it stays true after the run is over, so a closure that outlives its run
  still sees it. an `Op::timeout` expiring trips the same flag, so one
  polling loop handles both.
- anything that polls neither **runs to completion**, whatever hestan's
  records say the run did. hestan cannot stop it and does not pretend to.

so cancellation is honest about what it observed rather than about what it
asked for. after aborting, the run waits up to a three-second grace period
for its ops to actually come back:

- an op that comes back in time is recorded as whatever really happened. one
  that finished in the instant between the cancel request and the abort keeps
  its real result — its success (and any staged [state](state.md)) is
  recorded, not overwritten with `canceled`.
- an op that does not come back is recorded `canceled` with the error
  `cancellation requested; this op was not observed to stop within 3s and may
  still be running (...)`, and **no `finished_at`**. a finish time there
  would be hestan asserting that work stopped when all it knows is that it
  asked. the missing timestamp is the point: the op has no duration in the
  gantt or in op stats, because its duration is not a thing this process
  knows.

note that a blocking closure launched with `spawn_blocking` is invisible to
this: the op's own task is awaiting the join handle, so it aborts and comes
back promptly while the closure keeps going. that is exactly why polling
`is_cancelled()` is the contract rather than a suggestion — hestan can hand
the closure the signal, but it cannot see whether the closure heeded it.

`cancel` reports what it did: `Requested` (signal sent), `AlreadyFinished`
(terminal already, or a run left over from before a restart), `Unknown` (no
such run). canceled is terminal: the run itself never continues, but its ops
that did finish are reusable, so a canceled run is resumable exactly like a
failed one. a canceled run counts as inactive for the scheduler's
[overlap policy](scheduling.md), and [failure hooks](notifications.md) do
not fire for it.

## Resume

`runner.resume(run_id)` (or `POST /api/runs/{id}/resume`) launches a new run
that continues a finished one instead of redoing it. every op that did not
succeed runs again, together with everything downstream of it; every op that
did succeed is reused — its recorded output is seeded, and its body never
runs. the new run carries the original run's params, trigger `resume`, and a
`resumed_from` pointing at the run it continued.

```rust
let id = runner.resume(&failed)?;                        // from the failure
let id = runner.resume_from(&id, Some(&["clean".into()]))?;  // from a chosen op
```

`resume_from` with a selection re-runs exactly those ops and their
transitive downstream whatever their last status was — "re-run from here" —
which works on a successful run too. a plain resume of one is refused:
there is nothing to continue. re-run (`POST /api/runs/{id}/retry`) stays the
way to redo everything.

a resumed run's `op_runs` only holds the ops it actually ran, so resuming a
resume walks the `resumed_from` chain backwards: each op is seeded with the
most recent successful output recorded anywhere in the chain, which can be
several runs back. a run pruned by [retention](storage.md) breaks its
descendants' chains, and the resume says so rather than seeding a hole.

the ops recorded across that chain must still be exactly the job's ops, or
the resume is refused: resuming into a graph that has gained or lost an op
would record lineage that never happened. the same rule refuses resuming a
run that only ever covered part of the graph — an [asset build](assets.md)
records rows for its plan alone. a resume is also refused when nothing is
left to re-run, and when a chosen op's input was never produced by any run
in the chain.

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

a schedule's params are checked earlier still. `schedule_with` (and
`schedule_tz_with`) attach the params every cron fire launches with, and
`Hestan::build` runs them through those same validators, so a schedule whose
params no op accepts is a startup error rather than a fire that fails forever
at 3am. `Job::params_error` is that check on its own, without a store or a run,
and is what `POST /api/jobs/{name}/validate_params` answers with.
