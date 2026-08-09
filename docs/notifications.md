# Notifications

`on_failure` registers a hook that runs whenever a run finishes failed:

```rust
Hestan::new()
    .job(etl)
    .on_failure(|f| eprintln!("{} failed at {:?}: {:?}", f.job, f.failed_op, f.error))
    .serve(([127, 0, 0, 1], 4000))
    .await
```

call it as many times as you like — every registered hook fires, each on
tokio's blocking pool, so a hook may block outright (sleep, sync http, a
database write) without stalling the executor or other runs, and a panicking
one is caught and logged as a warning without touching the others. driving
`Runner` directly, the same hooks go in through
`Runner::with_failure_hooks(jobs, store, hooks)`; `Runner::new` is that with
no hooks, and `Runner::with_pools(jobs, store, hooks, pools)` adds
[concurrency pools](concepts.md#concurrency-pools).

## RunFailure

the hook receives one `RunFailure` per failed run:

| field | what it holds |
| --- | --- |
| `run_id` | the failed run |
| `job` | its job name |
| `trigger` | why the run existed: `manual`, `schedule`, `retry`, `build`, or `sensor` |
| `failed_op` | the first op that exhausted its attempts |
| `error` | that op's final error message |
| `finished_at` | when the run went terminal |

`failed_op` is the first terminal failure; with parallel branches other ops
may have failed after it, and their errors are in the run's op runs and
events, queryable by `run_id`.

the run row itself carries the same thing: `run.error` is
`op {failed_op} failed: {error}`, so an alert that only ever sees a run —
from `GET /api/runs/{id}`, or straight out of the store — is not left
guessing why it failed.

## When nothing fires

two deliberate gaps. a canceled run notifies nobody: someone asked it to
stop, and paging on that trains people to ignore the page. and the startup
sweep (the boot-time pass that marks runs a dead process left behind as
failed) writes straight to the database without touching the executor, so a
restart after a crash does not replay old failures into your alert channel.
every other failed run fires, whatever its trigger — a failed asset build
reaches the hooks exactly like a failed etl.

## Late alerts

`on_late` is the other hook, and it works the same way: it fires when a job or
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

the dispatch is the same one `on_failure` uses — one blocking task per hook,
panics caught and logged — and the difference that matters is *when*: a
failure is an event, so every one of them fires, while lateness is a state, so
only the **crossing** fires. something late for a week alerts once, across
restarts, and going fresh again re-arms the next one. [freshness](freshness.md)
has the rest.

## Http helpers

with the `http` feature, `hestan::notify` ships two ready-made hooks, and both
serve either kind of alert:

```rust
Hestan::new()
    .on_failure(hestan::notify::webhook("https://ops.example/hestan"))
    .on_failure(hestan::notify::slack(slack_webhook_url))
    .on_late(hestan::notify::slack(slack_webhook_url))
```

`webhook(url)` POSTs the whole event as json. `slack(url)` posts the
incoming-webhook shape `{"text": <one-line summary>}` —
`job {job} failed at {failed_op}: {error} ({run_id})` for a failure,
`{kind} {name} is {n}m late (last success {t})` for a late one. which event a
helper is built for is inferred from the hook it is handed to; the trait
behind that is `notify::Alert`, and implementing it on your own type is not a
thing this crate needs you to do. they
share one reqwest client with a 10s timeout that does not follow redirects —
following one would replay the POST as a bodyless GET at whatever the
`Location` header said — and delivery is best-effort: a non-2xx response
(3xx included) or a network error is logged via `tracing` and never retried.
if the channel must not drop messages, write a hook that hands the event to
your own queue instead.
