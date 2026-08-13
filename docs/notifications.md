# Notifications

`on_run_finished` registers a hook that runs whenever a run reaches a terminal
status — succeeded, failed or canceled alike:

```rust
Hestan::new()
    .job(etl)
    .on_run_finished(|e: RunEvent| println!("{} {}", e.job, e.status.as_str()))
    .serve(([127, 0, 0, 1], 4000))
    .await
```

call it as many times as you like — every registered hook fires, each on
tokio's blocking pool, so a hook may block outright (sleep, sync http, a
database write) without stalling the executor or other runs, and a panicking
one is caught and logged as a warning without touching the others. driving
`Runner` directly, the same hooks go in through
`Runner::new(jobs, store)?.with_hooks(run_hooks, op_hooks)`, and
`Runner::with_pools(jobs, store, hooks, pools)` adds
[concurrency pools](concepts.md#concurrency-pools).

## RunEvent

| field | what it holds |
| --- | --- |
| `run_id` | the run |
| `job` | its job name |
| `status` | `success`, `failed` or `canceled` |
| `trigger` | why the run existed: `manual`, `schedule`, `retry`, `resume`, `replay`, `build`, or `sensor` |
| `failed_op` | the first op that exhausted its attempts; `None` unless one did |
| `error` | that op's final error message |
| `started_at` | when it began executing; `None` for a run that never got that far |
| `finished_at` | when it went terminal |
| `duration` | how long it **executed** for, which is not how long it existed for — a run held on the queue by a limit was not running while it waited |

`failed_op` is the first terminal failure; with parallel branches other ops
may have failed after it, and their errors are in the run's op runs and
events, queryable by `run_id`.

the run row itself carries the same thing: `run.error` is
`op {failed_op} failed: {error}`, so an alert that only ever sees a run —
from `GET /api/runs/{id}`, or straight out of the store — is not left
guessing why it failed.

## OpEvent

`on_op_finished` fires once per **attempt** of one op:

```rust
Hestan::new().on_op_finished(|e: OpEvent| {
    if e.status == OpStatus::Failed {
        metrics::count("op_attempt_failed", &e.job, &e.op)
    }
})
```

| field | what it holds |
| --- | --- |
| `run_id` `job` `op` | which attempt of what |
| `attempt` | which attempt this was, from 1 |
| `status` | `success`, `failed` or `canceled` |
| `error` | what this attempt said, if it failed |
| `started_at` `finished_at` `duration` | this attempt's own, not the op's |

per attempt rather than per op, because an op that failed twice and worked on
the third try is three facts and only the hook knows which of them it wanted.
a hook that only cares about the end filters on `status`; one watching for
flakiness wants exactly the ones a per-op event would have hidden. the timing
is the attempt's own — `op_runs.started_at` keeps the *first* attempt's, since
that is what "when did this op start" means on a page.

an op skipped by its [trigger rule](concepts.md), or canceled before it was
ever spawned, produces no event at all: there was no attempt to report.

## Per job

`JobBuilder::on_run_finished` and `JobBuilder::on_op_finished` are the same
hooks scoped to one job, and they fire alongside anything registered on
`Hestan` for every job:

```rust
Job::builder("orders_etl")
    .on_run_finished(hestan::notify::slack(prod_channel))
    .op(load)
    .build()?
```

scoping is the point. an alert can cover the nightly production job without
covering every backfill and every ad-hoc re-run beside it, and a hook that had
to filter on the job name would have to be kept in step with the job list by
hand.

## on_failure

`on_failure` is `on_run_finished` with `status == failed` applied for you, and
is not going anywhere:

```rust
Hestan::new().on_failure(|f: RunFailure| eprintln!("{} failed at {:?}", f.job, f.failed_op))
```

it receives a `RunFailure` — `run_id`, `job`, `trigger`, `failed_op`, `error`,
`finished_at` — which is what it always received. it is the same dispatch with
a filter on it rather than a mechanism beside it, so there is one place an
event can go missing from rather than two.

## When nothing fires

two deliberate gaps. the startup sweep — the boot-time pass that marks runs a
dead process left behind as failed — writes straight to the database without
touching the executor, so a restart after a crash does not replay a morning of
old failures into your alert channel. and a run canceled before it started
never executed, so nothing reports on it; cancel a *running* run and its hooks
fire with `status = canceled`, because that run did things.

a run whose claimer went away *does* fire, with `status = failed` and no
`failed_op`: no op failed, the process holding the run stopped saying it was
there. a stall is exactly what an on-call hook exists to hear about.

## Late alerts

`on_late` is the third hook, and it works the same way: it fires when a job or
asset with a declared [freshness policy](freshness.md) crosses from fresh to
late.

```rust
Hestan::new()
    .job(Job::builder("etl").fresh_within(Duration::from_secs(86_400)).op(pull).build()?)
    .on_late(|e: LateEvent| eprintln!("{} {} is {:?} late", e.kind.as_str(), e.name, e.late_by))
```

| field | what it holds |
| --- | --- |
| `kind` | `job` or `asset` |
| `name` | the job or asset name |
| `late_by` | how far past the policy's deadline, at the crossing |
| `last_success` | the success the deadline was measured from |

the dispatch is the same one the others use — one blocking task per hook,
panics caught and logged — and the difference that matters is *when*: a run
finishing is an event, so every one of them fires, while lateness is a state,
so only the **crossing** fires. something late for a week alerts once, across
restarts, and going fresh again re-arms the next one. [freshness](freshness.md)
has the rest.

## Http helpers

with the `http` feature, `hestan::notify` ships two ready-made hooks, and both
serve every kind of event:

```rust
Hestan::new()
    .on_run_finished(hestan::notify::webhook("https://ops.example/hestan"))
    .on_failure(hestan::notify::slack(slack_webhook_url))
    .on_late(hestan::notify::slack(slack_webhook_url))
```

`webhook(url)` POSTs the whole event as json. `slack(url)` posts the
incoming-webhook shape `{"text": <one-line summary>}`:

| event | the line |
| --- | --- |
| a failed run | `job {job} failed at {failed_op}: {error} in {n}s ({run_id})` |
| a successful run | `job {job} succeeded in {n}s ({run_id})` |
| a canceled run | `job {job} was canceled in {n}s ({run_id})` |
| an op attempt | `op {op} of job {job} failed on attempt {n}: {error} in {n}s ({run_id})` |
| something late | `{kind} {name} is {n}m late (last success {t})` |

a run that succeeded does not read like an alarm, which is deliberate: a
channel where the good news looks like the bad news is a channel people stop
reading.

which event a helper is built for is inferred from the hook it is handed to;
the trait behind that is `notify::Alert`, and implementing it on your own type
is not a thing this crate needs you to do. they share one reqwest client with
a 10s timeout that does not follow redirects — following one would replay the
POST as a bodyless GET at whatever the `Location` header said.

delivery is best-effort unless you ask for otherwise: a non-2xx response (3xx
included) or a network error is logged via `tracing` and never retried. that
is the next section.

## Durable delivery

a hook is a `spawn_blocking` call. if the post fails the alert is gone; if the
process dies between the run finishing and the hook running, the alert was
never sent and nothing anywhere records that it should have been. for a hook
whose job is to tell a human, that is the failure mode that matters — the
outage that kills the process is exactly the one you wanted to hear about.

```rust
Hestan::new()
    .durable_notifications()
    .on_run_finished(hestan::notify::slack(url))
```

**off by default, and meant to stay off for most people.** an embedder whose
hook bumps a counter or writes a line wants a callback, not a table and a
delivery loop; the ordinary dispatch costs nothing and loses nothing that
matters.

with it on, each run's terminal event is written to the `notifications` table
**in the same transaction as the run's terminal row**. that is the whole of
what this buys: written afterwards, a crash in the gap leaves a failed run
nothing ever alerted about and no record that anything was owed. a run failed
by the [lease reclaimer](scaling.md) is written the same way, in the
transaction that fails it.

a delivery loop then takes what is due, hands it to the hooks, and marks it
delivered. it belongs to the process that [decides](scaling.md) — `Role::All`
or `Role::Scheduler` — so register the hooks there; two processes delivering
would send every alert twice. `run_once` and `build_asset` deliver once before
they return, since nothing else in that process will.

### At-least-once

**a hook can see the same event twice, and must tolerate it.** a crash between
a hook returning and the row being marked delivered re-delivers on the next
pass, because the alternative is marking first and losing the delivery
instead — and of those two, a receiver seeing an alert twice is the one you
can do something about. key on `run_id` if it matters. exactly-once needs the
receiver's cooperation and hestan will not pretend otherwise.

one hook failing fails the row, so hooks that already succeeded will see the
event again on the retry. that is the same rule, stated for the case that
surprises people.

### Retry and giving up

a hook that panics is a failed delivery. it retries on the same capped
exponential backoff with full jitter that op retries use — 10s, doubling —
for **eight attempts**, which is seven gaps and so at most about twenty
minutes, and nearer ten once the jitter is counted: long enough to cover a
restart of whatever is on the other end, not long enough to keep trying a url
that was wrong when it was typed. (the pacing carries a 30-minute ceiling for
the same reason op retries do; eight attempts never grow far enough to meet
it.)

past that hestan stops, and stops **loudly**. the row stays, `failed`, with
the error that stopped it, and appears in `GET /api/notifications?state=failed`
and in a section of the runs page. an alert nobody received should be visible
in the ui the alert was about, rather than in a log line from Tuesday.

| state | what it is |
| --- | --- |
| `pending` | undelivered, due again |
| `failed` | given up on; nothing will retry it |
| `delivered` | done |

[retention](storage.md#retention) takes delivered notifications on its age
cutoff and leaves undelivered ones at any age: one that never got through is
not history, it is something outstanding.

### What it covers

run events. op hooks and `on_late` stay in-process and best-effort: they fire
per attempt and per poll, and writing a row for every one of those is a
different bargain than the one this makes. if you need an op-level event to be
durable, hand it to your own queue from the hook.
