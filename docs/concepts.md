# Concepts

## Vocabulary

an *op* is a named async fn plus the upstream ops it waits on: `Op::new` or
`Op::typed`, then `.after([...])`, `.when(rule)`, `.retries(n)`,
`.retry_backoff(base, max)` or `.retry_delay(d)`, `.timeout(d)`,
`.pool(name)`, `.params::<P>()`. it receives an `OpCtx` and returns json
output or an error.

a *job* is a validated dag of ops. `Job::builder(name)...build()` rejects
duplicate op names, deps on unknown ops, and cycles, and fixes a deterministic
topological order (ties broken by declaration order). a *graph* is a reusable
bundle of ops instantiated into a job by name; it is
[flattened away at build](#reusable-graphs), so nothing past this line ever
sees one.

a *run* is one execution of a job: an id (uuid v7, so ids sort by creation
time), params (arbitrary json), a trigger, and a status that moves
`queued -> running -> success | failed | canceled`. each op within a run gets
an *op run* row: status
(`pending | running | success | failed | skipped | canceled`), attempt count,
start/finish times, and the output or error.

*events* are the append-only log of the whole deployment. each carries a level
(`info | warn | error`), a kind, a message, optional structured json `data`,
and — this is the part that stopped being about runs in v17 — a **subject**:
`subject_kind` is one of `run`, `job`, `asset`, `schedule`, `sensor`,
`backfill` or `system`, and `subject` names which one. so an asset
materialized, a schedule that fired or was skipped, a sensor tick, a backfill's
chunks, an alert nobody received and a lease reclaimed from a dead worker are
all in the same log the run kinds are, and "what happened last night" is one
query.

a run's own kinds are `run_queued`, `run_started`, `run_success`, `run_failed`,
`run_canceled`, `run_reclaimed`, `op_started`, `op_expanded`, `op_retry`,
`op_success`, `op_failed`, `op_skipped`, `op_canceled`, `type_check_failed`,
and `log` — the last is what `ctx.info/warn/error` emit. [events](events.md)
has every kind, what each payload carries, where each one is written and which
of them cannot be atomic, how to query and follow the log, and how a run maps
onto a distributed trace.

*captured output* is the other log, and a different thing: what the op itself
printed rather than what hestan said about it. an [isolated op](isolation.md)'s
stdout and stderr are piped and stored whole; an in-process op's `tracing`
events are stored if you compose hestan's [capture layer](logs.md) into your
subscriber. it lives in its own table for the good reason that a chatty op
would otherwise bury the eight events that describe what the run did. the
[logs page](logs.md) has the rest, including the one thing that is *not*
captured and why.

the *trigger* records why a run exists: `manual` (launch endpoint,
`run_once`), `schedule` (the cron scheduler), `retry` (the re-run endpoint),
`resume` (the [resume endpoint](#resume), which continues an earlier run),
`replay` (the [replay endpoint](replay.md), which re-runs ops of an earlier
run on the inputs it gave them), `build` (an [asset build](assets.md): the
endpoints, `build_asset`, and probe-driven auto builds), or `sensor` (a
[sensor](sensors.md) evaluation asked for it). retry, resume and replay are
for finished runs — the api answers 409 for one still queued or running, since
a fresh copy of a live run would only double it. manual launches stay ungated: that is the documented escape hatch
when an overlapping run is really wanted.

an *identity* is who asked, where a person did and something
[checked](auth.md): a name and a role — viewer, operator or admin. it lands on
the run as `actor` and on every event the request caused, so `manual` becomes
"manual, by ada". a deployment with no authenticator records no actor rather
than a fabricated one, which is exactly what it knows: a person asked, and
nothing was checking who.

an *asset* is an op with identity: a persisted latest value, a fingerprint,
and explicit lineage on other assets, which makes staleness provable and
builds incremental — stale ancestors plus the target, fresh values seeded.
asset builds run as ordinary runs of an internal job named `assets`, so
everything on this page applies to them unchanged. the model has
[its own page](assets.md).

all of it lands in the store as it happens — sqlite by default, postgres if
you point it at one — see [storage](storage.md). op
outputs land there too by default, which is wrong for anything bulky;
[io managers](io-managers.md) move them somewhere else and keep a handle in
the run log.

## Where a duplicate name is refused

a name in a declaration is claimed once. a second claim is a build error and
not a preference, because the alternative is a deployment that depends on the
order things were handed over — which of two jobs called `nightly` the
scheduler fires is not something a warning in a log can settle.

| name | claimed within | refused by, and what it says |
| --- | --- | --- |
| op | its job | `Job::builder(..).build()` — `invalid job graph: duplicate op extract` |
| job | the process | `serve`, `run_once` and `Runner::new` — `duplicate job: nightly` (`Error::DuplicateJob`) |
| asset | the process | asset registration — `invalid job graph: assets: duplicate op sales/orders` |
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
schedule on it — a second declaration of the same pair was never a second
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

every op whose deps have all reached a terminal status — and whose
[trigger rule](#trigger-rules) admits it — is spawned as its own tokio
task, so independent branches run concurrently — in a diamond
`a -> {b, c} -> d`, `b` and `c` run at the same time.
`Job::builder(..).max_parallel(n)` caps how many ops of one run are in
flight at once, and ready ops over the cap wait their turn in readiness
order (first ready, first spawned). without a cap, everything ready runs
together. an op's output is persisted through its
[io manager](io-managers.md) before its success is recorded — a `put` that
fails fails the op — and its dependents are handed the resulting handle,
resolved back to the value as each is spawned. under the default `Inline`
manager a handle *is* the value, so this is the same in-memory handoff it has
always been.

when an op exhausts its attempts, its transitive downstream is marked
`skipped`, each with an `op_skipped` event naming the failed root, and the
run will be failed. branches that don't depend on the failed op keep running
to completion. propagation stops at any op whose [trigger rule](#trigger-rules)
would still run it.

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
[cancellation](#cancellation) for what that does and does not stop, and
[isolation](isolation.md) for the op that it stops for real.

a run is `failed` if any op finished `failed`, otherwise `success` — unless
it was canceled, which wins over both. a failed run carries an `error` of its
own: the first op that terminally failed, as `op {name} failed: {message}`,
which is the same pair an [`on_failure` hook](notifications.md) receives.
whichever status it reached, an [`on_run_finished`](notifications.md) hook
gets one event carrying it, and each terminal **attempt** of each op gets an
[`on_op_finished`](notifications.md) event of its own. the
terminal event (`run_failed` / `run_success` / `run_canceled`) is committed
before the terminal status, so anything that observes a finished run can also
read its closing event.

## What hestan promises about writes

**a run never reports an outcome the run log did not take.** every status you
read — an op that succeeded, a run that failed, an asset that was built — is a
row that landed, not a thing the process believed at the time.

there are two kinds of write behind that, and the difference is deliberate:

- **what a run did** is critical: an op's terminal row, a run's terminal row,
  an op starting, a fan-out instance's row, a committed
  [watermark](state.md). one that fails is retried — four attempts, capped
  exponential with full jitter, under a second in the worst case — because a
  busy database is the ordinary reason a write does not land and it is over in
  milliseconds.
- **what a run said** is best-effort: the [event log](events.md) and captured
  [op output](logs.md). losing a line of narration is survivable where losing
  a run's outcome is not, so these are let go rather than retried. they are
  not let go *silently*: the count is on `GET /api/health`, and a store
  dropping them says so there.

what makes this a guarantee rather than a convention is that the two are
different types in the source. an event write returns a value the "let it go"
path accepts and a critical write does not, so dropping a run's outcome is a
compile error rather than a thing somebody has to remember not to write.

### When a write cannot land at all

after its retries, a critical write can still fail: a disk that is full, a
database that is gone, a postgres connection that died. **the run stops
there.** it does not report success, and it does not report failure either,
because at that point this process does not know what is true — the work may
well have finished, and the row that would say so is the write that did not
land. reporting either way is picking one of two guesses.

what it leaves behind is worth stating plainly, because it is not tidy:

> the run sits `running`, claimed by the process that gave up on it, with a
> lease nobody is renewing. it stays that way until the lease runs out — 60
> seconds — and some process with a working store reclaims it, at which point
> [`Reclaim`](scaling.md#claims-and-leases) decides: `Fail` marks it failed
> with `claimer went away` and fires the failure hooks, `Requeue` puts it back
> on the queue.

that is worse than a clean failure. a run hangs around for a minute looking
active when it is not, and anything waiting on it waits. it is far better than
a false success, which is the only other thing hestan could do: a `success`
the store never heard about is a lie that outlives the incident, gets read by
the next resume, and marks an asset current that was never built.

the process also stops taking new work while its store is refusing writes.
claiming a run is promising to record what it does, and a queue draining into
a process that cannot keep that promise turns one lost run into a shift's
worth. it starts again on its own as soon as a write lands.

the ops of an abandoned run are stopped with it: in-process ops are aborted
and an [isolated](isolation.md) op's child process is killed, so nothing
carries on working for a run nobody is going to record.

## Trigger rules

by default an op runs when its whole upstream worked. that makes the one op
you most want after a failure — a summary, an alert, a cleanup — exactly the
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

- `When::AllSucceeded` — every dep succeeded. the default, and what an op
  without a rule has always meant.
- `When::AnyFailed` — at least one dep did **not** succeed: failed, skipped
  or canceled. an op with no deps never qualifies, so it is always skipped.
- `When::Always` — whatever the deps did.

readiness is the same for all three: an op waits until every dep has reached
a *terminal* status, not until every dep has produced output. the rule then
decides run vs skip. an op the rule turns down is `skipped` with an
`op_skipped` event that names the rule
(`skipped by rule any_failed: every dep succeeded`, `data: {"when": ...}`) —
deliberately different wording from the upstream-failure skip
(`skipped: upstream load failed`), so the log says which of the two happened.

inside such an op, `ctx.input(dep)` for a dep that produced nothing is `None`
— there is no output to hand over — and `ctx.dep_status(dep)` is how it finds
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
run — and therefore at everything hanging off that op, which waits on what it
does rather than on what happened above it:

```
boom(failed) -> cut_off(skipped) -> deeper(skipped)
             -> cleanup(always, runs) -> after_cleanup(runs, if cleanup succeeded)
```

everything reached through plain `all_succeeded` ops is skipped as one group
naming the original root — one failure with one cause, not a chain of them.
if `cleanup` then fails, its own downstream is cut off naming `cleanup`.

### Rules and fan-out

a rule applies to a [mapped op](#dynamic-fan-out) as a whole; its instances
are all-or-nothing already, so there is nothing finer to apply it to. a
mapped op admitted by its rule when the array it maps over never arrived has
nothing to expand over, so it expands into **zero instances**: no bodies run,
no instance rows, an `op_expanded` event with `instances: 0`, and output `[]`
downstream — exactly the empty fan-out an empty array would have given.

## Resources

a *resource* is a value built once at startup and shared by every op that asks
— an http client, a connection pool, a parsed config — instead of each op
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
a build error instead of a run that gets halfway. resources live for the
process — no per-run scoping, no teardown hooks. the model has
[its own page](resources.md), and [connecting to your data](connecting.md) is
the worked version of it: a pool built once, the credential out of the
environment, and the reason there is no client of anybody's wrapped in here.

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

"ends" means the work stopped, not that hestan stopped waiting for it. a
[cancelled](#cancellation) run abandons an op at its next await point, and
blocking work the body started carries on — so the permit is held by the
`OpCtx` the body was handed rather than by the task that ran it, and the slot
goes back when the last holder of that ctx lets go. blocking work already has
to keep its ctx to see a cancel at all, and keeping it is what keeps the
count true: the closure still calling the api still holds the slot that
admitted it. an op that never stops holds its permit until the process ends,
because the work genuinely has not stopped — the pool is a promise about that
api, and hestan would rather hold a slot than break it. the limit of this is
work that keeps nothing of hestan's: a thread the body spawned and handed
nothing is work hestan cannot see the end of, and the slot goes back without
it.

pools compose with `max_parallel`: an op waits for both, in that order — a
slot in its own run first, then a permit. an op waiting for a permit does
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

## Cancellation

`runner.cancel(run_id)` (or `POST /api/runs/{id}/cancel`) asks a queued or
running run to stop. in-flight ops are aborted, every op that isn't terminal
yet (running and pending alike) is marked `canceled` with error `"canceled"`
and an `op_canceled` event, then the run gets its `run_canceled` event and
finishes with status `canceled`. retry sleeps die with the abort, so a
canceled op mid-backoff doesn't linger.

what actually stops, and what hestan claims about it, depends on the op:

- an **[isolated op](isolation.md)** is stopped, full stop. its body runs in a
  child process, so cancelling sends that process SIGTERM, waits three
  seconds, and then SIGKILLs it. nothing in the op gets a say. this is the
  only kind of op hestan can make that promise about, and it is the reason
  `.isolated()` exists — everything below is what cancellation means when the
  work shares this process.
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
asked for. the run aborts every in-process op — isolated ops are left alone,
because each is busy killing its own child and a dropped task could not — and
then waits a three-second grace period for its ops to come back, doubled while
an isolated op is spending a grace of its own inside it:

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

one thing does outlive the abort with it. a [pool](#concurrency-pools) permit
is held by the ctx rather than by the task, so a closure that carried its ctx
into `spawn_blocking` — which is how it reads the cancel signal at all —
holds the slot it was admitted into until it returns. the run is over and its
row says canceled; the pool still counts the call that is still in flight,
which is the only count worth having.

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
**with** one, and an error saying which of the two signals ended it —
`canceled: it stopped when asked` or `canceled: it ignored SIGTERM for 3s and
was killed`. the second row is a fact; the first is a request. that is worth
knowing before you write a blocking op that matters.

the timeout story is the same story: `Op::timeout` on an in-process op trips
`ctx.is_cancelled()` and hopes, while on an isolated op it kills the process
and reports `timed out after 30s: it ignored SIGTERM for 3s and was killed`.
the attempt then retries like any other failure — a timeout is a failed
attempt, not a canceled run.

`cancel` reports what it did: `Requested` (signal sent), `AlreadyFinished`
(terminal already, or a run left over from before a restart), `Unknown` (no
such run). canceled is terminal: the run itself never continues, but its ops
that did finish are reusable, so a canceled run is resumable exactly like a
failed one. a canceled run counts as inactive for the scheduler's
[overlap policy](scheduling.md), and [failure hooks](notifications.md) do
not fire for it — though `on_run_finished` does, with `status = canceled`, as
long as the run had started. cancel one still on the queue and nothing
reports on it: it never ran.

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
`replay_of` — a column of its own beside `resumed_from`, because a run log
that could not tell the two apart could not say which of two opposite things
happened. the original run is not written to.

what a replay does not reproduce — today's code, today's resources, today's
clock, today's answer from anything the op fetches itself — and the
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
  on — that is the only way into a graph, and an inner dep that names nothing
  inside the graph is a build error rather than a reach outward.
- anything depending on the instance name is rewired to the op named by
  `output`. `input` and `output` are both required, and an unknown or
  dot-containing name is a build error naming it.
- two instances of one graph must not share a name — that is exactly what the
  instance name is for — and an instance colliding with an op is `Error::Graph`.

### Reading inputs inside a graph

a graph's ops keep their own vocabulary. inside `clean`, `dedupe` reads
`ctx.input("parse")`, not `ctx.input("clean_a.parse")`; at job level, `load`
reads `ctx.input("clean_a")` — the name it wrote in `.after`, not the inner op
that happened to supply it. renaming is a wiring concern, so it stays out of
the bodies.

what a graph's *input* op cannot know is what the job called the dep it was
handed (`fetch` here, something else in the next job). `ctx.inputs()` is the
way out: every dep that produced output, name and value, sorted by name.

### Nesting

a graph may contain a graph — `GraphBuilder::graph` is the same call — and
`input`/`output` may name a nested instance, which resolves through it to a
real op. names compound: `s.inner.pages`. it is all one flattening, so a
recursive self-inclusion could not terminate; that is refused with a clear
error, though the immutable builder makes it unreachable in practice (a graph
can only contain graphs that were built before it).

### In the ui

the dag mutes an op's `{instance}.` prefix and draws the inner name at full
strength, so a graph instance's ops read as a group without a second layout.
they are ordinary nodes otherwise — clickable, statused, and gantt rows like
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
`mapped over pages, which produced a string rather than an array` — an
ordinary op failure that skips downstream. for an array of n elements it
creates n **instances** named `fetch_page[0]`, `fetch_page[1]`, … Each one:

- gets its own `op_runs` row, inserted the moment the instances are created,
  so the ui, the gantt and the run detail see it like any other op;
- receives its element as the typed argument, and reads every other dep whole
  with `ctx.input` — including the mapped dep itself, whose entry is the
  entire array;
- is an ordinary spawned task, so `max_parallel`, [pools](#concurrency-pools),
  retries, [timeouts](#cancellation) and cancellation apply to it exactly as
  they do to a static op, with no special cases.

the mapped op's own output, which downstream ops see under its plain name, is
the json array of instance outputs **in element order** — never in completion
order, however the instances interleave. the mapped op itself gets **no
`op_runs` row**: the instances are the record. its expansion is visible as an
`op_expanded` event (`data: {"instances": n, "over": dep}`) against the
parent's name, which is also the only trace left when n is 0.

### All or nothing

a mapped op counts as succeeded only if **every** instance succeeded. one
instance failing fails the mapped op for skip propagation — its downstream is
skipped, and the run fails naming the instance
(`op fetch_page[3] failed: 429`). there is no partial array and no partial
success. sibling instances already in flight run to the end, exactly as an
op's siblings do when it fails: hestan skips downstream, it never cancels
peers.

n = 0 is legal and load-bearing: no instances, output `[]`, downstream runs
normally on an empty array. that is the difference between "nothing to do"
and "something went wrong", and a fan-out over a filtered list needs it.

### Limits

fan-out does not nest: a mapped op may not be `.over` another mapped op, and
saying so fails the build (`fan-out does not nest`). so does a mapped op
without `.over`, and `.over` on an op that isn't mapped. instances are not
themselves mappable for the same reason — one level, deliberately, because
the second level has no honest name for its rows.

instance names are op names everywhere else in the system, which is what makes
them free: `ctx.set_state` from an instance writes state keyed
`(job, "fetch_page[3]")`, and op stats aggregate per static op, so a mapped op
shows no history of its own.

### Resume across a mapped op

a [resume](#resume) reuses instance outputs by their instance names, rebuilt
into the collected array. because the array a mapped op expands over can
differ on a re-run, a mapped op is reusable **only if it fully succeeded** —
every instance present, covering `0..n`, every one of them successful with a
recorded output. anything less and the whole mapped op re-expands from its
dep, instances and all, with its downstream. a mapped op that expanded over an
empty array leaves no rows at all, and so is indistinguishable from one that
never ran: it re-expands too, which costs nothing.

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
otherwise — which, with no limits declared, is the same instant, and is why
nothing above reads any differently than it did. with limits declared the run
waits on the queue, and `run` waits with it. the queue is the `runs` table, so
a run enqueued by one process can be executed by [another](scaling.md#roles).

both validate params before the run row is written. if any op declared
`.params::<P>()` and the given params don't deserialize, the launch fails with
`Error::InvalidParams` and leaves no trace in the database. launching an
unregistered job is `Error::UnknownJob`.

the [command line](cli.md) spells the same two: `run <job>` enqueues and
returns the id, and `run <job> --wait` executes it here and exits with what it
did. the difference between them is a role — a process that enqueues and then
exits must not be one that also executes, or the launch would kill what it
launched — and `run --dry-run` runs the params check above and stops there.

a schedule's params are checked earlier still. `schedule_with` (and
`schedule_tz_with`) attach the params every cron fire launches with, and
`Hestan::build` runs them through those same validators, so a schedule whose
params no op accepts is a startup error rather than a fire that fails forever
at 3am. `Job::params_error` is that check on its own, without a store or a run,
and is what `POST /api/jobs/{name}/validate_params` answers with.
