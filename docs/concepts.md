# Concepts

## Vocabulary

| Term | Meaning |
| --- | --- |
| Op | A named async function with declared dependencies, retry policy and optional timeout. Receives `OpCtx`; returns JSON or an error. |
| Job | A validated DAG of ops. Build rejects duplicate names, unknown dependencies and cycles. Declaration order breaks topological ties. |
| Graph | A reusable set of ops, flattened into a job at build time. |
| Run | One execution: UUID v7 ID, JSON params, trigger and status. |
| Op run | One op's status, attempts, timing, output and error within a run. |
| Asset | A named value with materialization history, fingerprints and explicit lineage. Builds execute in the internal `assets` job. |
| Identity | An authenticated caller's name and role, recorded as `actor` on requested runs and events. Unauthenticated requests have no actor. |

Run statuses are `queued`, `running`, `success`, `failed` and `canceled`.
Op statuses are `pending`, `running`, `success`, `failed`, `skipped` and `canceled`.
Triggers are `manual`, `schedule`, `retry`, `resume`, `replay`, `build` and `sensor`.
Retry, resume and replay require a finished run; a live run returns HTTP 409.
Manual launches can overlap, subject to concurrency limits.

[Events](events.md) record deployment activity; [captured logs](logs.md) hold
operation output separately. The [store](storage.md) persists execution
history. [IO managers](io-managers.md) can move large outputs out of that store.
See [assets](assets.md) for incremental builds and lineage.

## Where a duplicate name is refused

a name in a declaration is claimed once. a second claim is a build error and
not a preference, because the alternative is a deployment that depends on the
order things were handed over: which of two jobs called `nightly` the
scheduler fires is not something a warning in a log can settle.

| name | claimed within | refused by, and what it says |
| --- | --- | --- |
| op | its job | `Job::builder(..).build()`: `invalid job graph: duplicate op extract` |
| job | the process | `serve`, `run_once` and `Runner::new`: `duplicate job: nightly` (`Error::DuplicateJob`) |
| asset | the process | asset registration: `invalid job graph: assets: duplicate op sales/orders` |
| multi-asset | the process | `invalid job graph: duplicate multi-asset split_orders` |
| check | its asset | `invalid job graph: duplicate check row_count on asset orders` |
| sensor | the process | `invalid job graph: duplicate sensor watch` |
| schedule | its `(job, expression)` pair | `invalid job graph: schedule 0 3 * * * on job nightly is declared twice` |
| pool | the process | `invalid job graph: pool eia_api is declared twice` |

every one of them is raised before a row is written, so a definition that
cannot be read one way does not get as far as running.

two schedules on one job are **not** a duplicate: that is a job with two
expressions, both of which fire, and two of them landing on the same minute
launch one run rather than two. the pair is the key because the run log keys a
schedule on it: a second declaration of the same pair was never a second
schedule, only that row carrying whichever timezone and params came last.

a job named `assets` collides with the internal job that
[asset builds](assets.md) run as, and reads as the duplicate it is.

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

Ready ops run in separate Tokio tasks once all dependencies are terminal and
[trigger rules](#trigger-rules) allow them. Independent branches run concurrently.
`Job::builder(..).max_parallel(n)` limits in-flight ops per run; excess ready
ops wait in readiness order. Without a limit, all ready ops may run together.

Outputs pass through the [IO manager](io-managers.md) before success is recorded.
A failed `put` fails the op. Inputs are resolved from stored handles when each
dependent starts. Terminal failure skips downstream ops unless their trigger
rules allow execution; independent branches continue.

`.retries(2)` permits three attempts. Returned errors and caught panics use the
same retry policy. Default backoff is exponential with full jitter, starting
at one second and capped at 30 seconds. Set `.retry_backoff(base, max)` or use
`.retry_delay(d)` for a fixed delay. Recorded duration includes all attempts.

`.timeout(d)` applies to each attempt after pool and rate waits. Timeout is a
retryable failure and trips `ctx.is_cancelled()`. Without a timeout, an op can
hold its concurrency slot indefinitely. See [cancellation](#cancellation) for
in-process limits and [isolation](isolation.md) for process termination.

A run fails if any op terminally fails; otherwise it succeeds. Cancellation
takes precedence. The run error names the first terminally failed op.
[Hooks](notifications.md) receive each terminal attempt and the final run outcome.
The terminal run event is written before the terminal status.

## What hestan promises about writes

Execution status is backed by persisted rows. Critical writes—status changes,
fan-out rows and committed [state](state.md)—receive four attempts with capped,
jittered backoff lasting less than a second in total. An already-terminal op
row cannot be overwritten by another terminal status.

[Events](events.md) and [captured output](logs.md) are best-effort. Failed writes
are counted in `GET /api/health`.

### When a write cannot land at all

If critical-write retries are exhausted, the run stops without inventing a
terminal outcome. Its row remains `running` until the unrenewed claim expires
(after 60 seconds). A process with a working store then applies the configured
[reclaim policy](scaling.md#claims-and-leases): fail it or requeue it.

The affected process stops claiming new work until a write succeeds. It aborts
the abandoned run's in-process ops and kills isolated children. Blocking work
remains subject to the [cancellation limits](#cancellation).

## Trigger rules

by default an op runs when its whole upstream worked. that makes the one op
you most want after a failure (a summary, an alert, a cleanup) exactly the
one that gets skipped. `.when(rule)` says otherwise:

```rust
use hestan::When;

Op::new("summary", |ctx: OpCtx| async move {
    let load = ctx.dep_status("load");          // Some(OpStatus::Failed)
    ctx.warn(format!("load ended {load:?}"));
    Ok(json!({ "reported": true }))
})
.after(["extract", "load"])
.when(When::Always)
```

- `When::AllSucceeded`: every dep succeeded. the default, and what an op
  without a rule has always meant.
- `When::AnyFailed`: at least one dep did **not** succeed, whether it failed,
  was skipped or was canceled. an op with no deps never qualifies, so it is always skipped.
- `When::Always`: whatever the deps did.

readiness is the same for all three: an op waits until every dep has reached
a *terminal* status, not until every dep has produced output. the rule then
decides run vs skip. an op the rule turns down is `skipped` with an
`op_skipped` event that names the rule
(`skipped by rule any_failed: every dep succeeded`, `data: {"when": ...}`),
deliberately different wording from the upstream-failure skip
(`skipped: upstream load failed`), so the log says which of the two happened.

inside such an op, `ctx.input(dep)` for a dep that produced nothing is `None`
(there is no output to hand over), and `ctx.dep_status(dep)` is how it finds
out what happened instead. a dep seeded from outside the run (a
[resume](#resume)'s reused output, an [asset build](assets.md)'s memoized
value, a source asset) reads as `success`, since that is what it stands in
for.

### What a rule does not change

the run's own outcome. any op that finished `failed` fails the run, however
many cleanup ops ran happily afterwards. there is no "recovered" state: a
cleanup that worked is not evidence that the thing it cleaned up after
worked.

### Propagation

skip propagation asks each candidate's rule rather than assuming. when an op
fails, the walk down its dependents stops at the first op that would still
run, and therefore at everything hanging off that op, which waits on what it
does rather than on what happened above it:

```
boom(failed) -> cut_off(skipped) -> deeper(skipped)
             -> cleanup(always, runs) -> after_cleanup(runs, if cleanup succeeded)
```

everything reached through plain `all_succeeded` ops is skipped as one group
naming the original root: one failure with one cause, not a chain of them.
if `cleanup` then fails, its own downstream is cut off naming `cleanup`.

### Rules and fan-out

a rule applies to a [mapped op](#dynamic-fan-out) as a whole; its instances
are all-or-nothing already, so there is nothing finer to apply it to. a
mapped op admitted by its rule when the array it maps over never arrived has
nothing to expand over, so it expands into **zero instances**: no bodies run,
no instance rows, an `op_expanded` event with `instances: 0`, and output `[]`
downstream, exactly the empty fan-out an empty array would have given.

### An op that had nothing to do

the rules above are what the *run* decides about an op. `ctx.skip(reason)` is
what the op decides about itself:

```rust
Op::new("load", |ctx: OpCtx| async move {
    if !vendor_file_is_there() {
        return Err(ctx.skip("no drop from the vendor yet"));
    }
    Ok(json!({"loaded": true}))
})
```

the two things an op could say before this both said something untrue.
`Ok(..)` records a success, so freshness, any materialization and every success
hook take a build that did not happen as one that did. `Err(..)` fails the run,
which wakes whoever owns it for a non-event.

**it returns the error rather than setting a flag, so the body stops.** a skip
that only marked something and let the body carry on would be wrong the first
time somebody wrote it inside an `if` and forgot the `return`, and wrong
silently. going out through the error channel is the one shape the compiler
enforces: the op either returns this or returns a value, because those are the
two arms of one `Result`, so skipping and also succeeding is not a state that
exists. `Op::typed` bodies return their own output type, which is why what you
get back is the boxed error rather than an `OpResult`.

**a wrapped one is a failure.** an op that catches this and re-raises it inside
its own error type has turned it into something else, and hestan reading
through a conversion somebody wrote on purpose would be a guess.

**downstream cannot tell it from a rule skip**, and that is deliberate: the row
is `skipped` either way, and the run propagates from it through the same
function, so an op on the default rule is skipped naming the one that skipped
itself, and an `always` op runs and reads `ctx.dep_status(dep)` as `skipped`.
there is nothing to seed such an op with, so `ctx.input(dep)` is `None`: a skip
produced no output, and a downstream op that needs one wants `all_succeeded`,
which is the default.

**the run's status is unchanged.** a run whose ops all skipped is `success`,
which is already what an all-rule-skipped run is: it did nothing and it failed
nothing, and two ways of doing nothing must not end in two statuses.

**it never retries.** a skip is a decision the body reached, not a failure it
might reach differently next time, so `.retries(n)` does not apply to it.

**an asset op that skips materializes nothing.** what an asset op stages is
written in the transaction that records the op as having *succeeded*, and this
is not that. so the asset is still stale and the next real build still happens;
a skip that wrote a materialization would suppress it.

**what it staged is kept, except the state.** `ctx.meta` and `ctx.saved`
survive, because a skip is a finished op rather than a discarded attempt: the
numbers the body read on the way to deciding there was nothing to do are the
evidence for the decision, and a run page saying "skipped: no drop from the
vendor yet" is better for having them. a failed attempt drops its metadata
because a retry is about to replace it, and a skip has no retry to be replaced
by. `ctx.set_state` is the exception and is dropped: a watermark is a promise
about work that was done, and moving it without doing the work is how the next
run skips real input. an [isolated](isolation.md) op is no different: the child
that ran the body is the process that records the skip, so what it staged is on
the row it wrote, and the run adds nothing on top of it.

the reason is not optional. it lands in the event log as an `op_skipped` event
at warn level, the way `ctx.warn` does, and on the op run's row, so the run
page says why without anybody opening the log. **that row now carries the
reason for every kind of skip**, including the two that already existed
(`skipped by rule any_failed: every dep succeeded`, `skipped: upstream load
failed`), which used to be computed and then written only to the log.

a hook sees an attempt that really happened, with `status: skipped` on it and
the reason on `error`, the way a failed attempt carries its message. it is
neither a success nor a failure, and a hook that treats "not failed" as
"worked" is the one place that has to be said out loud. an `on_failure` hook
hears nothing: no run failed.

## Resources

a *resource* is a value built once at startup and shared by every op that asks
(an http client, a connection pool, a parsed config) instead of each op
capturing its own in a closure:

```rust
Hestan::new().resource("api", |_| async { Ok(ApiClient::new()?) })

Op::new("query", |ctx| async move {
    let api = ctx.resource::<ApiClient>("api")?;   // Arc<ApiClient>
    ..
})
.requires(["api"])
```

constructors are async and fallible and run before the store opens, so one
that fails aborts startup with `Error::Resource { name, reason }` rather than
leaving a half-live server. `Op::requires` turns a name nobody registered into
a build error instead of a run that gets halfway.

a resource declared with `Hestan::run_resource` is built when a run starts and
dropped when it ends instead: a scratch directory, a per-tenant client, a
token that belongs to one execution. ops read either the same way, and the
constructor running per run is the cost: a pool built that way is a pool per
run, which is usually a mistake. the model has [its own page](resources.md),
and [connecting to your data](connecting.md) is the worked version of it: a
pool built once, the credential out of the environment, and the reason there
is no client of anybody's wrapped in here.

## Concurrency pools

`max_parallel` is a property of one job. the limit that usually matters is a
property of something outside every job ("at most 3 requests in flight to
this api, ever"), and two jobs that overlap give 3 + 3.

a *pool* is that budget, declared once and shared by every job in the
process:

```rust
Hestan::new()
    .pool("eia_api", 3)
    .job(Job::builder("hourly").op(Op::new("pull", ..).pool("eia_api")).build()?)
    .job(Job::builder("backfill").op(Op::new("pull", ..).pool("eia_api")).build()?)
```

an op with `.pool(name)` takes a permit before its body runs and gives it
back when the attempt ends, however it ends: success, failure, panic,
timeout, or cancel. the permit is per attempt, so an op backing off between
retries is not sitting on the resource it is backing off from. naming a pool
that was never declared is `Error::Graph` at build time, as is declaring the
same pool twice; a limit below 1 means 1.

"ends" means the work stopped, not that hestan stopped waiting for it. a
[cancelled](#cancellation) run abandons an op at its next await point, and
blocking work the body started carries on, so the permit is held by the
`OpCtx` the body was handed rather than by the task that ran it, and the slot
goes back when the last holder of that ctx lets go. blocking work already has
to keep its ctx to see a cancel at all, and keeping it is what keeps the
count true: the closure still calling the api still holds the slot that
admitted it. an op that never stops holds its permit until the process ends,
because the work genuinely has not stopped: the pool is a promise about that
api, and hestan would rather hold a slot than break it. the limit of this is
work that keeps nothing of hestan's: a thread the body spawned and handed
nothing is work hestan cannot see the end of, and the slot goes back without
it.

pools compose with `max_parallel`: an op waits for both, in that order (a
slot in its own run first, then a permit). an op waiting for a permit does
hold its `max_parallel` slot, which can idle a job, but it cannot deadlock:
permits are only ever held by work that is already running, and nothing that
holds a permit ever waits for a slot.
the wait order is the same everywhere, so there is no cycle to close. (the
permit is deliberately taken inside the op's own task rather than in the
run's scheduling loop; taking it in the loop would stop that loop from
reaping the very ops whose permits it is waiting for, and *that* would
deadlock.)

an op that finds the pool full logs `waiting for a {pool} pool permit`, so a
queued op reads as queued instead of as an op mysteriously stuck in
`running`. `GET /api/jobs/{name}` reports each op's `pool` and the job's
`pools` with their limits, and the op inspector shows both.

## Rates

a pool caps how many calls are in flight. the limit an api publishes is almost
never that (it is "5 requests a second", "1000 an hour"), and three at a time
is a rate only if you know how long each call takes. `max_parallel(3)` is how
that limit usually gets approximated, and the failure mode is a 429 at 06:00.

a *rate* is the limit itself, declared once and shared by every job in the
process:

```rust
Hestan::new()
    .rate("eia_api", 5, Duration::from_secs(1))
    .job(Job::builder("hourly").op(Op::new("pull", ..).rate("eia_api")).build()?)
    .job(Job::builder("backfill").op(Op::new("pull", ..).rate("eia_api")).build()?)
```

an op with `.rate(name)` takes one token before its body runs. naming a rate
that was never declared is `Error::Graph` at build time, as is declaring the
same name twice; a limit below 1 means 1. an op may hold a pool permit and a
rate token at once, and the two names are separate: a pool called `api` and a
rate called `api` are two different limits on the same thing.

**a token is spent, not returned.** that is the whole difference from a pool: a
permit comes back when the work ends, and a token does not come back at all.
there is nothing to release and nothing to hold: the op has had its call, and
the bucket refills on its own clock whatever the op does next. so none of the
"held until the work stops" that a permit needs applies here, in either
direction. the token is per attempt, because a retry is another call, and per
[fan-out](#dynamic-fan-out) instance, because each instance is another call
too.

### the burst, and the boundary

it is a **token bucket**: `limit` tokens accrue over `per`, up to `limit` may be
spent at once, and one more accrues every `per / limit` after that. so "5 a
second" lets five go at once and then one every 200ms, which is what an api
publishing a per-second limit generally tolerates, and metering them out one
every 200ms from the start would be slower than the thing being protected asked
for.

the alternative is a fixed window (count what this second has spent, reset on
the tick), and it is less code and it is wrong. five calls at 0.99s and five at
1.01s are two legal windows and ten calls in fifty milliseconds, and the api
sees ten. a bucket cannot do that, wherever the boundary falls, because there
is no boundary: the second five have to wait for tokens to accrue.

### waiting

an op that finds the bucket empty waits. the wait is asynchronous (nothing
holds a runtime thread) and it does not count against
[`Op::timeout`](#how-a-run-executes), whose clock starts when the body does.
the op logs `waiting for a {name} token`, so a throttled op reads as throttled
instead of as an op mysteriously stuck in `running`, which is the line a pool
writes for the same reason.

**first come, first served.** the token is reserved when the op asks rather
than handed out when it arrives, so the queue is in arrival order and stays
there: an op cannot be overtaken by one that asked later, and a long queue
cannot starve the op at the head of it.

a [cancelled](#cancellation) run's waiting op stops waiting, and **does not take
a token on its way out**: the token it was holding goes to the op behind it in
the queue, which is woken to find out rather than sleeping out the one it was
given. a token spent on an op that is already dying is a call nobody makes and
a call somebody else should have been making.

an op that takes both a pool permit and a rate token waits for the permit
first and then for the token, and holds only the permit while it does. the
token is taken as late as anything can be, so that "a token was spent" and "a
call was made" stay the same moment.

### one process

**the bucket lives in this process.** two workers each honouring five a second
send ten, and the system being protected sees ten. that is not a caveat worth
burying: a rate exists to protect something outside hestan, and the deployment
shape that makes hestan scale ([`Role::Worker`](scaling.md#roles) on several
hosts) is exactly the one that breaks the guarantee.

what to do about it is arithmetic: divide. three workers against an api that
allows six a second is `rate("api", 2, ..)` in the registry all three build.
[scaling](scaling.md#a-rate-is-per-process) has the whole of it, including why
there is no shared bucket in the store.

`GET /api/rates` reports every declared rate and how many ops are waiting on it
here; `GET /api/jobs/{name}` reports each op's `rate` and the job's `rates`;
the runs page shows what is piling up behind one, and `hestan doctor` says so.

## Cancellation

`runner.cancel(run_id)` (or `POST /api/runs/{id}/cancel`) asks a queued or
running run to stop. in-flight ops are aborted, every op that isn't terminal
yet (running and pending alike) is marked `canceled` with error `"canceled"`
and an `op_canceled` event, then the run gets its `run_canceled` event and
finishes with status `canceled`. retry sleeps die with the abort, so a
canceled op mid-backoff doesn't linger.

"isn't terminal yet" is enforced where it cannot be raced: **an op run row that
already holds `success`, `failed`, `skipped` or `canceled` is not moved to
another terminal status**, by a cancel or by anything else. the condition is on
the write itself rather than on a check the run ran a moment earlier, because
the two things that have to agree here are two processes: an
[isolated](isolation.md) op's child records what it did on its own row, and its
parent can still be draining that child's pipes when the cancel arrives. an op
the guard declines gets no `op_canceled` event either, since nothing happened
to it. the one way off a terminal status is a fresh attempt, which puts the row
back to `running` before it writes anything, and that is what makes a retry
`failed -> running -> success` rather than a contradiction.

what actually stops, and what hestan claims about it, depends on the op:

- an **[isolated op](isolation.md)** is stopped, full stop. its body runs in a
  child process, so cancelling sends that process SIGTERM, waits three
  seconds, and then SIGKILLs it. nothing in the op gets a say. this is the
  only kind of op hestan can make that promise about, and it is the reason
  `.isolated()` exists; everything below is what cancellation means when the
  work shares this process.
- an **async op** is dropped at its next await point. that is real
  cancellation, and it is what "canceled" means on its op run row. it can
  also `select!` on `ctx.cancelled()` to unwind on purpose.
- a **blocking op** (`spawn_blocking`, a long computation, a synchronous
  driver) cannot be dropped at all. tokio has nothing to interrupt: the
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
asked for. the run aborts every in-process op (isolated ops are left alone,
because each is busy killing its own child and a dropped task could not) and
then waits a three-second grace period for its ops to come back, doubled while
an isolated op is spending a grace of its own inside it:

- an op that comes back in time is recorded as whatever really happened. one
  that finished in the instant between the cancel request and the abort keeps
  its real result: its success (and any staged [state](state.md)) is
  recorded, not overwritten with `canceled`. that holds whether or not the run
  noticed in time, because it is the row that refuses the write and not the
  run that remembers to skip it: an op whose result was already on the row when
  the cancel arrived keeps its status, its output, its metadata, its error and
  the finish time of the attempt that really ended.
- an op that does not come back is recorded `canceled` with the error
  `cancellation requested; this op was not observed to stop within 3s and may
  still be running (...)`, and **no `finished_at`**. a finish time there
  would be hestan asserting that work stopped when all it knows is that it
  asked. the missing timestamp is the point: the op has no duration in the
  gantt or in op stats, because its duration is not a thing this process
  knows. an op that does not come back but had already written its own row
  (an isolated op's child records its outcome itself) keeps that row: not
  coming back is a fact about the parent's wait, not about the work.

note that a blocking closure launched with `spawn_blocking` is invisible to
this: the op's own task is awaiting the join handle, so it aborts and comes
back promptly while the closure keeps going. that is exactly why polling
`is_cancelled()` is the contract rather than a suggestion: hestan can hand
the closure the signal, but it cannot see whether the closure heeded it.

one thing does outlive the abort with it. a [pool](#concurrency-pools) permit
is held by the ctx rather than by the task, so a closure that carried its ctx
into `spawn_blocking` (which is how it reads the cancel signal at all)
holds the slot it was admitted into until it returns. the run is over and its
row says canceled; the pool still counts the call that is still in flight,
which is the only count worth having.

a [rate](#rates) token is the other way round, and it is the difference between
holding something and having spent it. an op still *waiting* for one when the
cancel lands takes none with it (the token goes to the op behind it in the
queue), and an op that already had one has already made its call, so there is
nothing for the cancel to reach.

### the isolated contrast

the same op, `.isolated()`, is a different story end to end, because there is
a process to point a signal at:

|                          | in-process                            | [isolated](isolation.md)          |
| ------------------------ | ------------------------------------- | --------------------------------- |
| cancel reaches the op as | a dropped future, or a polled flag    | SIGTERM, then SIGKILL             |
| an op that ignores it    | runs to completion, uncontained       | is killed after three seconds     |
| `Op::timeout` expiring   | the same request, with the same holes | the same kill                     |
| the row's `finished_at`  | absent when nothing was observed      | set, because the process is gone  |

the op run row is where the difference shows. an in-process op that never
came back is recorded canceled with **no finish time**, and an error saying
hestan asked and did not see it stop. an isolated op is recorded canceled
**with** one, and an error saying which of the two signals ended it:
`canceled: it stopped when asked` or `canceled: it ignored SIGTERM for 3s and
was killed`. the second row is a fact; the first is a request. that is worth
knowing before you write a blocking op that matters.

the timeout story is the same story: `Op::timeout` on an in-process op trips
`ctx.is_cancelled()` and hopes, while on an isolated op it kills the process
and reports `timed out after 30s: it ignored SIGTERM for 3s and was killed`.
the attempt then retries like any other failure: a timeout is a failed
attempt, not a canceled run.

`cancel` reports what it did: `Requested` (signal sent), `AlreadyFinished`
(terminal already, or a run left over from before a restart), `Unknown` (no
such run). canceled is terminal: the run itself never continues, but its ops
that did finish are reusable, so a canceled run is resumable exactly like a
failed one. a canceled run counts as inactive for the scheduler's
[overlap policy](scheduling.md), and [failure hooks](notifications.md) do
not fire for it, though `on_run_finished` does, with `status = canceled`, as
long as the run had started. cancel one still on the queue and nothing
reports on it: it never ran.

## Resume

`runner.resume(run_id)` (or `POST /api/runs/{id}/resume`) launches a new run
that continues a finished one instead of redoing it. every op that did not
succeed runs again, together with everything downstream of it; every op that
did succeed is reused: its recorded output is seeded, and its body never
runs. the new run carries the original run's params, trigger `resume`, and a
`resumed_from` pointing at the run it continued.

```rust
let id = runner.resume(&failed)?;                        // from the failure
let id = runner.resume_from(&id, Some(&["clean".into()]))?;  // from a chosen op
```

`resume_from` with a selection re-runs exactly those ops and their
transitive downstream whatever their last status was ("re-run from here"),
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
run that only ever covered part of the graph: an [asset build](assets.md)
records rows for its plan alone. a resume is also refused when nothing is
left to re-run, and when a chosen op's input was never produced by any run
in the chain.

## Replay

`runner.replay(run_id)` (or `POST /api/runs/{id}/replay`) launches a new run
that re-runs ops of a finished one **on the inputs that run gave them**: every
dep of the replayed ops is seeded from what the original recorded, so the op
reads byte for byte what it read then. it is the question "does my fix work on
the input that broke it".

```rust
let id = runner.replay(&broken)?;                              // the ops that failed
let id = runner.replay_ops(&broken, Some(&["load".into()]))?;  // or exactly these
```

it is the opposite of a resume, and the difference is worth holding onto: a
resume re-runs what did **not** succeed together with everything downstream,
and a replay re-runs what **did**, exactly the ops named and nothing below
them. the new run carries the original's params, trigger `replay`, and a
`replay_of`, a column of its own beside `resumed_from`, because a run log
that could not tell the two apart could not say which of two opposite things
happened. the original run is not written to.

what a replay does not reproduce (today's code, today's resources, today's
clock, today's answer from anything the op fetches itself) and the
[retention](storage.md#retention) horizon past which a run cannot be replayed
at all are [its own page](replay.md), and are the difference between a result
you can trust and one that misleads you.

## Reusable graphs

a *graph* is a unit of ops you can drop into a job more than once. it is a
build-time thing and nothing else: `JobBuilder::build` flattens every instance
into ordinary ops, so runs, resume, fan-out, assets, the gantt and the ui
never learn that a graph existed.

```rust
let clean = Graph::builder("clean")
    .op(Op::new("parse", ..))
    .op(Op::new("dedupe", ..).after(["parse"]))
    .input("parse")        // inner ops that receive the instance's deps
    .output("dedupe")      // the one inner op that supplies the instance output
    .build()?;

Job::builder("nightly")
    .op(Op::new("fetch", ..))
    .graph("clean_a", &clean)      // instance name
    .after(["fetch"])              // ...and what it waits on
    .op(Op::new("load", ..).after(["clean_a"]))
    .build()?
```

that job has four ops: `fetch`, `clean_a.parse`, `clean_a.dedupe`, `load`.

- inner ops are renamed `{instance}.{inner}`, and their deps on each other are
  rewritten to match. inner names may not contain a dot, since that is the
  separator.
- the ops named by `input` additionally wait on whatever the instance waits
  on: that is the only way into a graph, and an inner dep that names nothing
  inside the graph is a build error rather than a reach outward.
- anything depending on the instance name is rewired to the op named by
  `output`. `input` and `output` are both required, and an unknown or
  dot-containing name is a build error naming it.
- two instances of one graph must not share a name (that is exactly what the
  instance name is for), and an instance colliding with an op is `Error::Graph`.

### Reading inputs inside a graph

a graph's ops keep their own vocabulary. inside `clean`, `dedupe` reads
`ctx.input("parse")`, not `ctx.input("clean_a.parse")`; at job level, `load`
reads `ctx.input("clean_a")`: the name it wrote in `.after`, not the inner op
that happened to supply it. renaming is a wiring concern, so it stays out of
the bodies.

what a graph's *input* op cannot know is what the job called the dep it was
handed (`fetch` here, something else in the next job). `ctx.inputs()` is the
way out: every dep that produced output, name and value, sorted by name.

### Nesting

a graph may contain a graph (`GraphBuilder::graph` is the same call), and
`input`/`output` may name a nested instance, which resolves through it to a
real op. names compound: `s.inner.pages`. it is all one flattening, so a
recursive self-inclusion could not terminate; that is refused with a clear
error, though the immutable builder makes it unreachable in practice (a graph
can only contain graphs that were built before it).

### In the ui

the dag mutes an op's `{instance}.` prefix and draws the inner name at full
strength, so a graph instance's ops read as a group without a second layout.
they are ordinary nodes otherwise: clickable, statused, and gantt rows like
any other op.

## Dynamic fan-out

the static graph stays the unit of definition, but one node can become many
at run time. `Op::mapped` is `Op::typed`'s sibling: the closure takes the
deserialized *element* as its second argument, and `.over(dep)` names the one
upstream op whose output it expands.

```rust
Op::mapped("fetch_page", |ctx: OpCtx, page: u32| async move {
    Ok(fetch(page).await?)
})
.over("pages")        // exactly one mapped dep, required
.after(["config"])    // ordinary deps too, read whole as usual
```

`.over` adds the dep if it wasn't declared, so `.over("pages")` alone is
enough. the mapped op is **one node** in the static graph with its declared
deps, so topo order, cycle checks, `max_parallel` and the `assets` job are
untouched by any of this.

### What a run does with it

when every dep of a mapped op is satisfied the executor reads the mapped
dep's output. it must be a json array, or the op fails with
`mapped over pages, which produced a string rather than an array`, an
ordinary op failure that skips downstream. for an array of n elements it
creates n **instances** named `fetch_page[0]`, `fetch_page[1]`, … each one:

- gets its own `op_runs` row, inserted the moment the instances are created,
  so the ui, the gantt and the run detail see it like any other op;
- receives its element as the typed argument, and reads every other dep whole
  with `ctx.input`, including the mapped dep itself, whose entry is the
  entire array;
- is an ordinary spawned task, so `max_parallel`, [pools](#concurrency-pools),
  retries, [timeouts](#cancellation) and cancellation apply to it exactly as
  they do to a static op, with no special cases.

the mapped op's own output, which downstream ops see under its plain name, is
the json array of instance outputs **in element order**, never in completion
order, however the instances interleave. the mapped op itself gets **no
`op_runs` row**: the instances are the record. its expansion is visible as an
`op_expanded` event (`data: {"instances": n, "over": dep}`) against the
parent's name, which is also the only trace left when n is 0.

### All or nothing

a mapped op counts as succeeded only if **every** instance succeeded. one
instance failing fails the mapped op for skip propagation: its downstream is
skipped, and the run fails naming the instance
(`op fetch_page[3] failed: 429`). there is no partial array and no partial
success. sibling instances already in flight run to the end, exactly as an
op's siblings do when it fails: hestan skips downstream, it never cancels
peers.

n = 0 is legal and load-bearing: no instances, output `[]`, downstream runs
normally on an empty array. that is the difference between "nothing to do"
and "something went wrong", and a fan-out over a filtered list needs it.

### A fan-out inside a fan-out

`.over` may name a mapped op. each of that op's instances then produces an
array of its own, and this op runs once per element of each:

```rust
Op::new("regions", |_| async { Ok(json!(["north", "south"])) })
Op::mapped("sites", |_ctx: OpCtx, region: String| async move {
    Ok(json!(sites_in(&region)))       // one array per region
})
.over("regions")
Op::mapped("probe", |_ctx: OpCtx, site: String| async move {
    Ok(json!(probe(&site)))            // one instance per site of each region
})
.over("sites")
```

instances carry one `[label]` per level of fan-out they sit inside, outermost
first: `sites[0]` and `sites[1]`, then `probe[0][0]`, `probe[0][1]`,
`probe[1][0]`, … so an inner instance names the outer one it belongs to, and
`op_runs` still has a unique row per instance. that is the whole naming rule,
and it goes as deep as the nesting does. a label never holds a bracket (one
that would is refused at the expansion), so the name reads back as an op and
its coordinates without ambiguity. an op named like one of those instances,
`probe[0]` in a job that also has a mapped `probe`, fails the build rather
than spending the deployment's life being read as an instance.

the collected output keeps the **shape**: `probe`'s value downstream is one
array per outer element (`[["north-a", "north-b"], ["south-a"]]`), not one
flat list. flattening would lose which region a reading came from, and that is
the only reason to nest a fan-out rather than flatten in the outer op. an outer
element yielding `[]` contributes an empty array in its place, not a gap.

everything an instance already inherits works at every level: pools, rates,
limits, retries, [rules](#rules-and-fan-out), timeouts and cancellation apply
per instance with no special cases. failure works the same way one level down:
`probe[1][1]` failing fails `probe`, skips its downstream, and leaves the
instances under `probe[0]` running to the end.

**flattening in the outer op is usually the better design.** nesting
multiplies: forty regions each yielding forty sites is sixteen hundred op runs
from two lines that each looked small, and every one of them is a row, a span
in the gantt and a value held in memory until the fan-out collects. reach for
it when the shape genuinely matters downstream, and reach for one `Vec` built
in the outer op when it does not.

### The ceiling

`Hestan::max_instances(n)` is the most op runs one run may expand its fan-outs
into, across every level of them; the default is 1000. a run about to go past
it fails at the expansion:

```
op probe expands over sites into 1600 instances, one for each of its 1600
elements; with the 40 this run has already made that is past the ceiling of
1000 op runs one run may expand to.
```

the check is made **before** any of the rows are written, which is the point:
a runaway found by counting op rows in the ui is a runaway that already
happened. the budget belongs to the run rather than to any one op, since what
a nesting multiplies is the run: ten fan-outs of a hundred cost the same
thousand rows as one of a thousand.

a thousand is thirty times what a [partitioned build](assets.md) launches by
default and far more than any hand-written fan-out, so a job that means it
never meets this. raise it for a deployment that genuinely fans out wider (a
build naming thousands of partitions by hand is the usual one), and prefer
flattening to raising it much.

### Limits

a mapped op without `.over` fails the build, and so does `.over` on an op that
isn't mapped.

instance names are op names everywhere else in the system, which is what makes
them free: `ctx.set_state` from an instance writes state keyed
`(job, "fetch_page[3]")`, an io manager writes its value to a file of that
name, and op stats aggregate per static op (at every level, so
`probe[1][1]`'s history is `probe`'s), which is why a mapped op shows no
history of its own.

### Resume across a mapped op

a [resume](#resume) reuses instance outputs by their instance names, rebuilt
into the collected array. because the array a mapped op expands over can
differ on a re-run, a mapped op is reusable **only if it fully succeeded**:
every instance present, covering `0..n`, every one of them successful with a
recorded output. anything less and the whole mapped op re-expands from its
dep, instances and all, with its downstream. a mapped op that expanded over an
empty array leaves no rows at all, and so is indistinguishable from one that
never ran: it re-expands too, which costs nothing.

nesting reads the same way one level in. the reassembled value keeps its shape,
so a resume past `probe` seeds the array-per-region it collected; and the rule
applies at every level, so an outer element whose inner fan-out was empty is
the same gap as a mapped op with no rows, and the whole of `probe` expands
again. an inner fan-out re-expanding over an outer one that *was* reused works
from the seeded value: the shape is in the value, and the labels of an op whose
output can be reused are its indices.

## launch() vs run()

`Runner` is what `Hestan` drives internally, and is usable directly. it
exposes both:

```rust
let id = runner.launch("etl", json!({}), Trigger::Manual)?;      // fire and forget
let run = runner.run("etl", json!({}), Trigger::Manual).await?;  // await the result
```

`launch` creates the run row (status `queued`, with its `run_queued` event in
the same transaction), pokes the [dispatcher](../docs/scaling.md), and returns
the run id immediately. `run` does the same and then awaits completion,
returning the final `Run`. execution is spawned onto the runtime rather than
driven by the returned future, so dropping that future (a timeout, a `select!`
losing) detaches the run: it finishes in the background instead of being
aborted mid-write.

**`queued` is a real state now, not a millisecond on the way to `running`.**
launching is a request to run rather than a start: the dispatcher decides when
it starts, and it starts as soon as no [limit](scaling.md#limits) says
otherwise, which, with no limits declared, is the same instant, and is why
nothing above reads any differently than it did. with limits declared the run
waits on the queue, and `run` waits with it. the queue is the `runs` table, so
a run enqueued by one process can be executed by [another](scaling.md#roles).

both validate params before the run row is written. if any op declared
`.params::<P>()` and the given params don't deserialize, the launch fails with
`Error::InvalidParams` and leaves no trace in the database. launching an
unregistered job is `Error::UnknownJob`.

the [command line](cli.md) spells the same two: `run <job>` enqueues and
returns the id, and `run <job> --wait` executes it here and exits with what it
did. the difference between them is a role (a process that enqueues and then
exits must not be one that also executes, or the launch would kill what it
launched), and `run --dry-run` runs the params check above and stops there.

a schedule's params are checked earlier still. `schedule_with` (and
`schedule_tz_with`) attach the params every cron fire launches with, and
`Hestan::build` runs them through those same validators, so a schedule whose
params no op accepts is a startup error rather than a fire that fails forever
at 3am. `Job::params_error` is that check on its own, without a store or a run,
and is what `POST /api/jobs/{name}/validate_params` answers with.
