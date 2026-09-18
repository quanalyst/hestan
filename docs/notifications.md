# Notifications

## Named destinations

Register a named destination for durable delivery that awaits the sender's
acknowledgement. HTTP adapters require the `http` feature:

```rust
# #[cfg(feature = "http")]
# fn example(url: String) -> Result<(), hestan::Error> {
use hestan::{Hestan, NotificationDestination, notify};

let alerts = NotificationDestination::builder("run-alerts")
    .on_failure()
    .sender(notify::Slack::new(url))
    .build()?;

Hestan::new().notification(alerts);
# Ok(())
# }
```

Use `.on_run_finished()` for success, failure, and cancellation. `.jobs(["a",
"b"])` restricts the subscription to persistent job names; no restriction
includes asset-build runs. Registration automatically enables durable delivery
for that destination. It does not change ordinary callbacks or require
`.durable_notifications()`.

Destination IDs are explicit, unique within the application, and at most 256
bytes. Missing selectors, senders, unknown selected jobs, invalid policies,
invalid HTTP endpoints, and duplicate IDs are rejected before execution.
Groups, labels, namespaces, and owner contact strings do not select recipients.

| Sender | Submission contract |
| --- | --- |
| `notify::Webhook::new(url)` | JSON event envelope; 2xx acknowledges submission. `.header(name, value)` adds application-provided authentication. |
| `notify::Slack::new(url)` | Slack incoming webhook; a successful `ok` acknowledgement is required. Default spacing is one second. |
| `notify::TeamsWorkflow::new(url)` | Adaptive Card for a configured Teams Workflows webhook. Acceptance does not prove the workflow's later actions succeeded. |
| Async closure or `NotificationSender` | Await the application client's submission; return success, retryable failure, or permanent failure. |

Use `.public_url("https://hestan.example")` for explicit run links. Credentials
and endpoint URLs stay in process configuration and are excluded from delivery
history. HTTP redirects are refused. Built-in errors contain status codes and
safe transport descriptions, not response bodies or credential-bearing URLs.

A custom sender receives `NotificationDeliveryCtx`: the immutable event,
destination, lifetime attempt number, timeout, and a cancellation signal. The
event includes the original run outcome and readable name, persistent job/run
IDs, and stable event and delivery IDs. Parameters and output values are not
included. Error excerpts are bounded and marked when truncated.

```rust
use hestan::{NotificationDeliveryCtx, NotificationDeliveryError, NotificationDestination};

let destination = NotificationDestination::builder("receiver")
    .on_run_finished()
    .sender(|ctx: NotificationDeliveryCtx| async move {
        // Await your client's send here. Do not detach delivery work.
        let _stable_id = &ctx.event.delivery_id;
        Ok::<(), NotificationDeliveryError>(())
    })
    .build()?;
# Ok::<(), hestan::Error>(())
```

[The local receiver example](../examples/notifications.rs) runs without an
external service. [The email example](../examples/notification_email.rs) adapts
an application-owned HTTP mail service, with one recipient per destination and
a stable Message-ID. Hestan does not bundle SMTP, provision Teams workflows,
or manage OAuth credentials. A mail server accepting a message does not prove
inbox delivery; Message-ID alone does not guarantee deduplication.

## Delivery and recovery

Subscriptions, policy, and presentation context are captured when a run is
queued. Terminal recording atomically creates one delivery per matching
destination. This includes startup interruption, lease recovery as failure,
resource failure, and cancellation while queued. Enabling a destination does
not notify historical runs. A failed notification never changes run status.

Each destination retries independently. Database claims prevent an expired
worker from overwriting a recovered delivery. Attempts are bounded; a crash
can leave an unknown outcome. Delivery is at least once, subject to the retry
budget: the receiver can accept a request before the acknowledgement is stored.
Use the stable delivery ID to deduplicate. Generic webhooks send it in the
JSON envelope and `Idempotency-Key`; the changing attempt number is in
`X-Hestan-Attempt`. Providers do not necessarily deduplicate these headers.

`NotificationPolicy::default()` allows eight total attempts, ten seconds per
attempt, and exponential full-jitter backoff from ten seconds to thirty
minutes. `.attempts(n)`, `.timeout(duration)`, `.backoff(base, cap)`, and
`.minimum_interval(duration)` override these values. A supplied policy replaces
the adapter's default spacing. Four sends may be active in one process, with
one per destination across processes. Strict event ordering is not guaranteed.

Network errors, timeouts, HTTP 408/429, and 5xx responses retry. Other 4xx and
redirect responses fail permanently. Valid `Retry-After` delays are respected;
429 and 503-with-delay responses pause the destination, including later
messages. Custom senders return `NotificationDeliveryError::retryable(...)`,
`::permanent(...)`, or `.retry_after(duration)`. A sender panic counts as a
retryable failure. Claimed attempts count even when a crash makes their
external outcome unknown. Custom senders must be asynchronous, cancellation-safe,
and must keep secrets out of error strings.

Removing or disabling a destination leaves its deliveries blocked. Restoring
its compatible registration resumes them. Changing policy or filters affects
new runs only. Endpoint credentials are resolved at send time: rotating them
works for pending deliveries, and changing an endpoint under the same ID also
redirects pending deliveries. Use a new ID for a different logical recipient.
A different sender protocol under the same ID does not reinterpret old events.

## Inspecting and operating deliveries

The Runs page links to named deliveries; each run also links to its filtered
history. List filters are stored in the URL. Pending timestamps include any
destination cooldown. The detail page shows each attempt
and its outcome. The old callback panel is explicitly identified as legacy.

| State | Meaning |
| --- | --- |
| `pending` | Waiting for an initial attempt or retry. |
| `in_flight` | A dispatcher holds its claim. |
| `blocked` | A compatible destination is missing, disabled, or otherwise unavailable. |
| `delivered` | Submission acknowledged. |
| `failed` | Permanently rejected or attempts exhausted. |
| `dismissed` | An administrator deliberately stopped delivery. |

Unscoped admins can retry a failed delivery or dismiss inactive owed work.
Retry keeps the delivery ID and full history, starts another attempt budget,
and respects provider cooldowns. Actions use the displayed generation; stale
requests return 409 and cannot reset a budget repeatedly. Delivered or active
requests cannot be resent or dismissed. Reads follow Hestan's existing Viewer
access; namespace scopes do not provide read isolation.

```sh
hestan --server http://localhost:4000 notifications list --state failed
hestan --server http://localhost:4000 notifications show DELIVERY_ID
hestan --server http://localhost:4000 notifications retry DELIVERY_ID --generation 4
hestan --server http://localhost:4000 notifications dismiss DELIVERY_ID --generation 7
```

Direct database CLI mode can inspect; mutations need the application registry
or an authenticated server. See the [HTTP API](http-api.md) for list filters
and action payloads. Health, doctor, and metrics report backlog and expired
claims independently of execution health.

Pending, blocked, active, and failed deliveries survive retention and run
history pruning. Delivered and dismissed records and their attempts follow the
configured retention cutoff. Delivery history can remain after its run page
has been pruned.

Server and scheduler roles dispatch; worker-only processes record owed work.
Headless `run_once` and `build_asset` attempt their own notifications within a
ten-second budget, configurable with `.notification_flush_within(duration)`.
Unfinished sends remain recoverable after claim expiry. With `Runner`, call
`flush_notifications(budget)` explicitly; it reports attempts started and
remaining owed work. A later process must run to retry persisted work after
all current processes exit. Shutdown stops new claims and gives active sends
the existing shutdown grace period.

Upgrades add storage tables on both backends. Stop old binaries before the
schema upgrade; rolling execution across incompatible schema releases is not
supported. Existing callbacks, legacy notification rows, and their API remain
unchanged. Registering the same recipient through both APIs creates two
independent subscriptions. Named delivery covers terminal runs; op-attempt and lateness hooks
remain best-effort.

## Ordinary callbacks

`on_run_finished` registers a hook that runs whenever a run reaches a terminal
status (succeeded, failed or canceled alike):

```rust
Hestan::new()
    .job(etl)
    .on_run_finished(|e: RunEvent| println!("{} {}", e.job, e.status.as_str()))
    .serve(([127, 0, 0, 1], 4000))
    .await
```

call it as many times as you like: every registered hook fires, each on
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
| `owner` | who to wake about that job, from `JobBuilder::owner`; `None` for a job nobody claimed |
| `status` | `success`, `failed` or `canceled` |
| `trigger` | why the run existed: `manual`, `schedule`, `retry`, `resume`, `replay`, `build`, or `sensor` |
| `failed_op` | the first op that exhausted its attempts; `None` unless one did |
| `error` | that op's final error message |
| `started_at` | when it began executing; `None` for a run that never got that far |
| `finished_at` | when it went terminal |
| `duration` | how long it **executed** for, which is not how long it existed for; a run held on the queue by a limit was not running while it waited |

`failed_op` is the first terminal failure; with parallel branches other ops
may have failed after it, and their errors are in the run's op runs and
events, queryable by `run_id`.

the run row itself carries the same thing: `run.error` is
`op {failed_op} failed: {error}`, so an alert that only ever sees a run
(from `GET /api/runs/{id}`, or straight out of the store) is not left
guessing why it failed.

### The owner reaches the hook

`owner` is the field that answers "who do I wake", and it is on the event
rather than something the caller passes in:

```rust
Hestan::new()
    .job(Job::builder("orders_etl")
        .owner(Owner::team("data-platform").contact("#data-alerts"))
        .op(pull)
        .build()?)
    // nothing threaded an owner through this closure: it is on what it is handed
    .on_failure(|f: RunFailure| page(f.owner.as_ref().and_then(Owner::contact_at)))
```

the executor reads it off the job's declaration at the one place a terminal
event is built, so every hook, the durable delivery loop and the built-in
notifiers get the same answer without any of them looking anything up. a job
nobody claimed is `None`, and `None` renders as an absence rather than as an
empty name.

durable delivery writes the owner into the notification row with the rest of
the event, so the process that finally delivers does not need to be holding the
registry the run was launched from. a row written by an older hestan has no
`owner` key and reads back as an event with no owner.

**the limit, plainly**: an asset build runs under one internal `assets` job, so
its run event carries that job's owner and not the asset's. one run can build
several assets with several owners and picking one would be a guess. an asset's
own owner reaches a hook through `on_late`, below.

what an `Owner` is, and what escalation deliberately is not, is on
[namespaces and owners](namespaces.md#an-owner).

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
is the attempt's own; `op_runs.started_at` keeps the *first* attempt's, since
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

it receives a `RunFailure` (`run_id`, `job`, `owner`, `trigger`, `failed_op`,
`error`, `finished_at`), which is what it always received plus the owner. it is the same dispatch with
a filter on it rather than a mechanism beside it, so there is one place an
event can go missing from rather than two.

## When nothing fires

two deliberate gaps. the startup sweep (the boot-time pass that marks runs a
dead process left behind as failed) writes straight to the database without
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
| `owner` | who to wake about it, from `JobBuilder::owner` or `Asset::owner`; `None` for one nobody claimed |
| `late_by` | how far past the policy's deadline, at the crossing |
| `last_success` | the success the deadline was measured from |

**this is the path an asset's owner reaches a hook by.** a run event carries
the owner of the job the run was of, and an asset build runs under the internal
`assets` job; a crossing into late is about the asset itself, so it carries the
asset's own owner.

the dispatch is the same one the others use (one blocking task per hook,
panics caught and logged), and the difference that matters is *when*: a run
finishing is an event, so every one of them fires, while lateness is a state,
so only the **crossing** fires. something late for a week alerts once, across
restarts, and going fresh again re-arms the next one. [freshness](freshness.md)
has the rest.

## Http helpers

With the `http` feature, `hestan::notify` retains two legacy best-effort hooks.
Both serve every kind of event; use the named adapters above for awaited delivery:

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

a run line or a late line about something with a declared owner ends
`, owned by ada of data-platform (#data-alerts)`; one about something nobody
claimed ends where it always did, with nothing appended. the `webhook` body
gains an `owner` object, and omits the key entirely when there is none.

which event a helper is built for is inferred from the hook it is handed to;
the trait behind that is `notify::Alert`, and implementing it on your own type
is not a thing this crate needs you to do. they share one reqwest client with
a 10s timeout that does not follow redirects: following one would replay the
POST as a bodyless GET at whatever the `Location` header said.

delivery is best-effort unless you ask for otherwise: a non-2xx response (3xx
included) or a network error is logged via `tracing` and never retried. that
is the next section.

## Durable delivery

This section describes legacy callback durability. Named destinations above
already enable their own durable delivery. To persist ordinary callback invocations:

```rust
Hestan::new()
    .durable_notifications()
    .on_run_finished(hestan::notify::slack(url))
```

The notification row commits with the terminal run row, including runs failed
by lease recovery. The active decider delivers due rows; register hooks on
scheduler/all-role processes. `run_once` and `build_asset` attempt delivery
before returning.

Durability covers callback invocation. The built-in HTTP helpers have additional
limits described under [retry](#retry-and-giving-up).

### At-least-once

A crash after a hook returns but before delivery is marked can invoke it again.
Use `run_id` to deduplicate when needed. If one hook fails, the whole row retries,
including hooks that already succeeded.

### Retry and giving up

The built-in `notify::slack` and `notify::webhook` helpers return after spawning
an HTTP request. They log request failures without reporting them to the delivery
loop; enabling durable delivery does not make those HTTP requests retryable.

a hook that panics is a failed delivery. it retries on the same capped
exponential backoff with full jitter that op retries use (10s, doubling)
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

Durable delivery covers run events only. Op hooks and `on_late` remain
best-effort; forward them to your own durable queue if required.
