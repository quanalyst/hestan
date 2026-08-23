# Events

the event log is hestan's answer to "what happened last night".

until v17 it could not answer that, and the reason was structural rather than
cosmetic: `events.run_id` was `NOT NULL`, so **an event could only ever be
about a run**. an asset materialized, a schedule that fired, a sensor tick, a
backfill's progress, an alert that never got through, a lease reclaimed from a
dead worker: each of those happened in a table of its own and reached no
stream at all. you could ask a run what it did. you could not ask the
deployment.

now every subsystem writes into one log, and each event says what it is about.
`hestan events --follow` is that log in a terminal, and the Activity view is
the same one in the ui.

## The shape of an event

```json
{
  "seq": 4182,
  "run_id": null,
  "subject_kind": "asset",
  "subject": "sales/orders",
  "op": null,
  "level": "info",
  "kind": "asset_materialized",
  "message": "sales/orders[2026-01-01] materialized",
  "data": { "partition": "2026-01-01", "fingerprint": "9f2c…", "run_id": "018f…", "meta": { "rows": { "count": 1240 } } },
  "ts": "2026-01-01T03:14:07Z",
  "actor": null
}
```

- **`seq`** orders the log and is the cursor. it only goes up. what it does
  and does not promise a follower is [below](#following-the-log).
- **`subject_kind`** and **`subject`** are what the event is about:
  `run`, `job`, `asset`, `schedule`, `sensor`, `backfill`, `system`.
- **`run_id`** is set on a run event and null on everything else. a schedule
  fire that launched a run puts that run in its *payload* rather than in this
  column, because the event is about the schedule. this column is what makes
  a run's page exactly the run's own log and nothing else.
- **`subject` is null on a run event.** the run is `run_id`, which was already
  there and already indexed; v17 deliberately did not copy it into `subject`,
  because doing so is a full rewrite of the largest table in the database to
  store a second copy of a column. `Event::about()` in rust (and
  `ev.subject ?? ev.run_id` in the ui) is where the two become one answer, and
  the api's `subject=` filter matches either.
- **`op`** is set on the run events that belong to one op.
- **`level`** is `info`, `warn` or `error`, and it is not the same claim as the
  kind: a check that failed at severity `warn` is a `check_failed` at level
  `warn`, because the run it belongs to succeeded.
- **`actor`** is who caused it, on the events a person caused and something
  [checked who they were](auth.md): a launch, a cancel, a pause, a backfill.
  it is the identity's **name** and never a credential, and it is null on
  everything a loop did on its own, and on everything at all in a deployment
  with no authenticator, which has nobody to name. an empty name is not
  "system".

## Where an event is written, and why that is the whole design

an event is a claim that something happened. if it is written *next to* the
thing rather than *with* it, then a crash in the gap produces one of two lies:
a log that says a thing happened which did not, or a thing that happened and
left no record. so every event added in v17 is written by the subsystem that
does the work, in the same transaction as the row that is the work, the same
rule phase 21 applied to a run's terminal notification.

| what | transaction it joins | atomic |
| --- | --- | --- |
| `run_queued` | the `runs` insert | yes |
| `run_success` / `run_failed` / `run_canceled` | none | **no**, see below |
| `run_reclaimed` | the reclaim's status change | yes |
| `run_released` | the release's status change | yes |
| `op_*`, `type_check_failed`, `log` | none | **no**, see below |
| `asset_materialized` | the `asset_materializations` insert, which for a build is the op's terminal write | yes |
| `policy_launched` | none | **no**, see below |
| `check_passed` / `check_failed` | the `asset_checks` insert | yes |
| `schedule_*` | the `schedule_ticks` insert | yes |
| `sensor_tick` | the `sensor_ticks` insert | yes |
| `backfill_started` | the `backfills` insert | yes |
| `backfill_chunk` | the launched-count update | yes |
| `backfill_finished` / `backfill_canceled` | the status update | yes |
| `notification_delivered` / `notification_failed` | the `notifications` update | yes |
| `retention_pruned` | the deletes it counts | yes |

### The three windows, stated rather than hidden

**an op's progress has no row to be atomic with.** `op_started`, `op_retry`,
`op_success`, `op_failed`, `op_skipped`, `op_canceled` and `type_check_failed`
are each a separate statement, written immediately before or after the
`op_runs` update they describe. a crash in that gap loses the event and keeps
the status, or vice versa. this is not new in v17 and it is not fixable by
moving the write: an op *starting* is not a row anywhere, so there is nothing
to join. what the gap costs is one line of narration; the op run row is the
record of record, and the ui reads both.

**a run's terminal event is written just before the terminal status.** in that
order deliberately: anyone who can see a run marked `failed` can also see the
line that says why. the reverse order would let a status exist with no
explanation, which is the worse of the two.

**a fired schedule's run and its tick are two transactions.** the launch
commits first, then the tick and its `schedule_fired` event. a crash between
them leaves a run that is queued and will execute, with no tick and no event,
recoverable and visible as a run whose trigger is `schedule`. the other
direction, an event claiming a run that was never created, cannot happen. the
same applies to `backfill_chunk`.

**a policy's launch and its event are two writes.** the run is enqueued, then
one `policy_launched` per asset in the plan. a crash between them leaves a build
that will execute, tagged `policy`, with nothing saying which rule wanted it;
the other direction, an event about a run that was never created, cannot happen.
it is the same trade the fired schedule makes above, and for the same reason:
the launch is the thing, and the narration is about it.

**a delivered notification's event is about the mark, not about the hook.**
delivery is at-least-once: the hook returns, then the row is marked and the
event written in one transaction. a crash between the hook returning and that
transaction re-delivers on the next pass, which is what at-least-once means.

## Every kind, and what its payload carries

the payload is the `data` column: json, documented per kind, and **stable**
under the rules in [schema version](#schema-version). a key marked optional may
be `null` or absent.

### Runs

| kind | level | payload |
| --- | --- | --- |
| `run_queued` | info | `job`, `trigger`, `priority`, `tags` |
| `run_started` | info | `job`, `trigger` |
| `run_success` | info | `job`, `status`, `error` (null), `failed_op` (null), `duration_secs` |
| `run_failed` | error | `job`, `status`, `error`, `failed_op`, `duration_secs` |
| `run_canceled` | warn | `job`, `status`, `error` (optional), `failed_op` (optional), `duration_secs` |
| `run_reclaimed` | warn | `claimer` (the instance that stopped renewing) and `policy`, `fail` or `requeue` |
| `run_released` | warn | `claimer` (the instance that was stopping) |

a reclaimed run under `fail` gets `run_reclaimed` **and then** `run_failed`:
the first says why, the second says what the run did. under `requeue` it gets
only the first, because the run has not ended.

`run_released` is the same shape as a requeue and a different cause: the
process holding the run was [asked to stop](scaling.md#stopping-a-process-on-purpose)
and handed it back rather than leaving it claimed until the lease ran out. it
is warn rather than info because the run starts over, so whatever its ops
already did they do again. unlike a `fail` reclaim it is not followed by a
terminal status: the run did not end, and it is queued again in the same
transaction that wrote this line. what it does after that it does under
whichever process claims it next.

### Ops

all carry `run_id` and `op`.

| kind | level | payload |
| --- | --- | --- |
| `op_started` | info | `attempt` |
| `op_expanded` | info | `instances`, `over` (the dep the fan-out mapped) |
| `op_success` | info | `attempt`, `output_type` (optional), `meta` (optional) |
| `op_retry` | error | `attempt`, `error` |
| `op_failed` | error | `attempt`, `error` |
| `op_skipped` | warn | `reason`, and `when` or `upstream` depending on which rule skipped it |
| `op_canceled` | warn | `reason`, `stopped` |
| `type_check_failed` | error | `error` |
| `log` | as emitted | whatever `ctx.info`/`warn`/`error` attached, usually null |

`meta` on `op_success` is [the tagged map](metadata.md) the attempt reported
with `ctx.meta`, exactly as the op run carries it, so a consumer following the
log alone sees the row counts and the byte sizes without fetching the op run.

`stopped` on `op_canceled` is the one that matters: `true` means the work
provably stopped (an [isolated](isolation.md) op's process was signalled,
killed and reaped), `false` means cancellation was *requested* and the op was
never observed to stop.

### Assets

| kind | level | payload |
| --- | --- | --- |
| `asset_materialized` | info | `partition` (optional), `fingerprint`, `run_id` (optional), `meta` (optional) |
| `policy_launched` | info | `rule`, `partitions` (empty on an unpartitioned asset), `run_id` |
| `check_passed` | info | `check`, `partition` (optional), `status`, `severity`, `message` (optional), `run_id`, `meta` (optional) |
| `check_failed` | warn or error | as above; the level follows the *severity*, not the verdict |

`subject` is the asset name. the partition is in the payload rather than in the
subject, so a filter on one asset finds every key of it.

`run_id` in the payload is where the build happened; it is null for a
materialization a [probe](assets.md) recorded outside any run.

`policy_launched` says an [automation policy](assets.md#automation-policies)
asked for a build: `rule` is the one that fired (`stale`, `missing` or `cron`)
and `partitions` is the keys it asked for, newest first. one per asset rather
than one per key, because a pass that wants a month of a daily set made one
decision, and a pass that wants nothing writes nothing at all: a rule waiting on
something that will never arrive is silent rather than hourly.

### Schedules

`subject` is the job the schedule fires. all carry `job`, `expr`,
`scheduled_for`, `outcome`, `run_id` (optional) and `error` (optional).

| kind | level | what it means |
| --- | --- | --- |
| `schedule_fired` | info | an occurrence came due and launched |
| `schedule_caught_up` | info | an occurrence that came due while nothing was running to fire it, fired now |
| `schedule_skipped` | info | accounted for without firing: an overlap policy, a [catch-up cap](scheduling.md), or a declaration that has gone |
| `schedule_deferred` | info | held back until the job is free, and still waiting |
| `schedule_error` | error | came due, and the launch failed |

`scheduled_for` is the logical occurrence and `ts` is when the scheduler got to
it. on a `schedule_caught_up` those are far apart, and the distance is the
downtime.

### Sensors

| kind | level | payload |
| --- | --- | --- |
| `sensor_tick` | info, or error on a failed evaluation | `outcome`, `launched`, `skipped`, `duration_ms`, `runs`, `error` (optional) |

`runs` is the ids of what it launched, in order. `skipped` counts requests
whose [run key](sensors.md) was already claimed, which is a different fact from
launching nothing.

**a tick that did nothing gets no event.** every evaluation is still a row in
`sensor_ticks` and the [sensors page](web-ui.md) still reads all of them: that
is the sensor's health record. but a sensor polling every five seconds is
seventeen thousand evaluations a day, and an activity log in which those are
99% of the rows is one you cannot read anything else out of. so the log gets
the ticks that *did* something: launched a run, declined a keyed request, or
failed.

### Schedules and sensors, paused

| kind | level | payload |
| --- | --- | --- |
| `schedule_paused` | info | `expr`, `paused` |
| `sensor_paused` | info | `paused` |

one kind for both directions: `paused: false` is a resume. `subject` is the job
for a schedule and the sensor's name for a sensor, and `actor` is whoever
asked: a paused schedule outlives whoever paused it, which is exactly why the
log says who.

### Backfills

`subject` is the backfill id, as a decimal string.

| kind | level | payload |
| --- | --- | --- |
| `backfill_started` | info | `asset`, `from_key`, `to_key`, `total` |
| `backfill_chunk` | info | `asset`, `run_id`, `launched`, `total` |
| `backfill_finished` | info, or error when it failed | `asset`, `status`, `launched`, `total` |
| `backfill_canceled` | warn | as above |

`backfill_chunk` says a chunk **went out**, not that it came back: what it did
is the run it names. a range that resolves to no keys at all gets
`backfill_started` and `backfill_finished` at the same instant, because both are
true.

### System and jobs

| kind | subject | level | payload |
| --- | --- | --- | --- |
| `notification_delivered` | `system`, the notification id | info | `notification_id` |
| `notification_failed` | `system`, the notification id | error | `notification_id`, `attempts`, `error` |
| `retention_pruned` | `job`, the job name | info | `job`, `runs` |

`notification_failed` is written **only when hestan gives up**. the seven
retries before that are the mechanism working, and one event apiece would bury
the one that matters.

`retention_pruned` is written only when a sweep actually deleted something, in
the transaction that deleted it. the other caps (the two tick logs,
materialization history, delivered notifications, and the event log's own) are
size limits rather than policy and write no event.

## Schema version

`GET /api/events` reports `"schema": 1` beside every page, and
`hestan::EVENT_SCHEMA` is the same number.

while hestan is 0.x, that number promises this:

- a key documented above keeps its **name, its type and its meaning** for as
  long as the number does not move.
- what may happen without the number moving: a payload gains a key, a kind is
  added, a key documented as optional is absent.
- what may not: a key changing type, a key changing meaning, or a kind changing
  what it is about.

so: **read the keys you know and ignore the rest.** a consumer written that way
survives the whole of 0.x. one that matches exhaustively on `kind` does not,
which is why hestan's own reader does not either: an unrecognised kind reads
as `EventKind::Unknown("…")` carrying the stored word, rather than failing the
query and taking the rest of the page with it. the same is true of
`subject_kind`.

## Asking

`GET /api/events` is the whole log, newest first.

```
GET /api/events?since=2026-01-01T22:00:00Z&level=error&limit=100
GET /api/events?subject_kind=asset&subject=sales/orders
GET /api/events?kind=schedule_fired&subject=nightly
GET /api/events?before=4182
```

every filter composes, and every one is optional:

| parameter | what it narrows to |
| --- | --- |
| `kind` | one kind, exactly |
| `subject_kind` | one of `run`, `job`, `asset`, `schedule`, `sensor`, `backfill`, `system` |
| `subject` | one subject; on a run event this matches the run id |
| `level` | that level exactly; three levels, and "show me the errors" is what anyone types |
| `since`, `until` | rfc3339; `since` is inclusive, `until` exclusive |
| `before` | seq, exclusive: the cursor for the next page back |
| `limit` | default 100, max 1000 |

pages go backwards: take the `seq` of the last row you got and pass it as
`before`. an unfiltered first page plus `before` walks the whole log without
skipping or repeating, because nothing is ever inserted below a seq that has
already committed; see the next section for the one exception, at the very top
of the log.

a run's own log is still `GET /api/runs/{id}/events`, oldest first from a
cursor, which is what the run page follows.

## Following the log

`GET /api/events/stream` is the same log as [server-sent
events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events),
live, taking the same filters plus `after=<seq>`:

```
GET /api/events/stream?after=4182&subject_kind=asset
```

each message is one event, with the event's `seq` as the SSE `id`, so a
reconnecting consumer that sends `Last-Event-ID` (or passes `after=`) picks up
exactly where it stopped and the gap is delivered before the live tail.

**a stream ends when the process serving it is
[asked to stop](scaling.md#stopping-a-process-on-purpose).** it is a response
with no natural end, so a stream that went on polling would be the one
connection keeping a stopping process alive. reconnect with the
`Last-Event-ID` you already have and nothing is missed.

### `seq` is allocated on insert, not on commit

this is the one thing worth understanding before you write a consumer.

both backends allocate `seq` when the row is inserted, not when its transaction
commits. so a writer that has taken seq 5 and not yet committed is invisible
while a writer that took 6 and did commit is not, and a follower that takes
everything it can see and moves its cursor to 6 will **never come back for 5**.
that is a real bug, it is silent, and it is the classic one for anything that
tails an autoincrementing column.

hestan does not skip, and how it avoids it differs by backend:

- **sqlite**: it cannot happen. writers take the database's write lock and hold
  it until they commit, so no transaction can commit below one that already
  has. seq order *is* commit order, and the stream delivers up to the newest
  seq immediately.
- **postgres**: several processes write at once and seq order is not commit
  order, so the stream delivers only the **unbroken run** above its cursor. a
  missing seq stops it: that seq is either a transaction still committing or
  one that aborted, and nothing outside the database can tell those apart. so
  it waits on the hole for **two seconds** and steps over it after that, which
  is the one assumption in the whole mechanism, and here it is: *a transaction
  that appends an event and takes longer than two seconds to commit may be
  skipped*. hestan's are a handful of statements each. a hole left by a
  retention sweep is a whole range, and one wait covers the range rather than
  each row in it.

`GET /api/events` has the same exposure at the very top of the log and does
*not* apply the rule: a page of the past is exact, and the newest page on
postgres may be missing a row that is committing as you read it. it will be
there on the next call, at a seq below the one you already have, which is why
the stream exists, and why paging forward on `before` is not how you follow a
log.

### A consumer that falls behind is dropped, loudly

the stream never buffers without bound. a slow consumer would otherwise turn
into unbounded memory in the server, which is the failure mode that takes the
orchestrator down along with the consumer.

the queue between the reader and the socket holds 256 events. when it is full:

- further events are **dropped**, and the cursor moves past them anyway.
- how many were dropped is counted, and sent as a `dropped` SSE event as soon
  as there is room:

```
event: dropped
data: {"count": 412, "through": 51233}
```

- `through` is the seq the drop ran up to. a consumer that cares about what it
  missed can fetch exactly that range from `GET /api/events` with `before` and
  `since`, which is why the marker carries a seq rather than only a count.

a gap that says it is a gap is worth something. a gap that does not is worse
than nothing, which is the same reasoning the [capture layer](logs.md) drops
under.

## In the ui

**Activity** is the whole log, one row per event, newest first: what it was
about, what happened, and when. the filters are the api's (subject kind,
level, and a find box over the message and the subject), and the feed follows
the stream, so a run that starts while you are looking at it appears at the
top.

it is the one page that is not about a single thing. every other page answers
"what is the state of this job / asset / run"; this one answers "what has this
deployment been doing", which is the question you have at 3am and the one
hestan could not previously answer at all.

## The same run, as a trace

a run is already a causal tree: a run, its ops, an attempt each, and for an
[isolated op](isolation.md) a subprocess under that. that is what a distributed
trace is, and the optional `otel` feature emits it as one, so a pipeline shows
up in Grafana or Jaeger beside the services it calls, rather than in a tab of
its own.

```toml
hestan = { version = "0.1", features = ["otel"] }
```

**hestan installs nothing.** no subscriber, no tracer provider, no exporter,
no environment variable of its own. it opens `tracing` spans with the right
shape and the right fields; the host composes
[`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) into the
subscriber it was going to build anyway, the same arrangement the
[capture layer](logs.md) uses, and for the same reason.

```rust
use tracing_subscriber::prelude::*;

tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(tracing_opentelemetry::layer().with_tracer(your_tracer))
    .init();
```

### What maps to what

| hestan | span |
| --- | --- |
| a run | `hestan.run`, the root, with `run_id`, `job`, `trigger` |
| one attempt of an op | `hestan.op` beneath it, with `run_id`, `op`, `attempt` |
| a retry | another `hestan.op` with the next `attempt`: its own span, not an annotation on the first |
| an event | a span event: on the attempt's span for anything the op body said, on the run's for hestan's own narration |

the span fields are exactly the ones the capture layer reads, because they are
the same spans. a build with both features composes both layers and each takes
what it wants; the capture layer ignores the `hestan::events` target the run
log is mirrored under, so nothing is stored twice.

### Across the process boundary

an isolated op runs in a subprocess, and a subprocess is where every other
orchestrator's trace stops. hestan hands the child its parent attempt's
[w3c trace context](https://www.w3.org/TR/trace-context/) in the environment
(`traceparent`, and `tracestate` if there is one), and the child parents its own
`hestan.op` span to it. so spans the child's code opens nest under the op that
spawned them, in the same trace, across the fork.

that is the part nothing else does, and here is exactly how far it goes.

### What it does not do

**a child's spans are exported only if the child's binary exports.** an
isolated op re-executes *your* binary; the tracer provider in that process is
the one your `main` built. hestan can hand the child a parent, and does. it
cannot give it an exporter.

- child composes an otel layer → its spans nest correctly under the parent's
  `hestan.op`. this is the case worth having.
- child composes nothing → no child spans at all. the parent's `hestan.op`
  still covers the whole of the child's execution, because hestan times the
  subprocess, so the trace is complete at op granularity and missing only what
  happened inside.

**hestan will not flush the child's exporter.** a batch exporter that has not
shipped when the child exits loses those spans, and short-lived processes are
exactly where that bites. the provider belongs to the host, so the host's child
path has to flush before `main` returns. hestan reaching into a provider it
does not own to do it would be a library taking over an application's
telemetry, which is the thing this whole design refuses.

**a context is only carried when there is one.** with the feature on and no
layer composed, `carry` produces nothing and the child is handed nothing,
rather than a synthesised trace id that leads nowhere.

**nothing outside a run is traced.** a schedule firing, a sensor evaluating, a
retention sweep: those are events in the log and they are not spans. a trace is
a run and the work under it; the rest of the log is the log.

**the versions have to match.** `opentelemetry` and `tracing-opentelemetry` do
not promise compatibility across releases, and a host on a different major of
either will not compile against this. hestan tracks the current pair
(`opentelemetry` 0.32, `tracing-opentelemetry` 0.33) and says so here rather
than pretending the constraint is not there.

## Retention

a run's events are deleted with the run, by [retention](storage.md), and always
were.

everything v17 added belongs to no run and so belongs to no run's retention
either. those events are capped instead at the newest **50,000**, swept by the
same loop, unconditionally: an asset built every five minutes writes a row
here forever otherwise. the cap is not configurable today; it is a size limit
on a table that grows with time rather than with the history anybody asked to
keep.
