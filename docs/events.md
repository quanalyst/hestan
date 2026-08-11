# Events

the event log is hestan's answer to "what happened last night".

until v17 it could not answer that, and the reason was structural rather than
cosmetic: `events.run_id` was `NOT NULL`, so **an event could only ever be
about a run**. an asset materialized, a schedule that fired, a sensor tick, a
backfill's progress, an alert that never got through, a lease reclaimed from a
dead worker — each of those happened in a table of its own and reached no
stream at all. you could ask a run what it did. you could not ask the
deployment.

now every subsystem writes into one log, and each event says what it is about.

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
  "ts": "2026-01-01T03:14:07Z"
}
```

- **`seq`** orders the log and is the cursor. it only goes up. what it does
  and does not promise a follower is [below](#following-the-log).
- **`subject_kind`** and **`subject`** are what the event is about:
  `run`, `job`, `asset`, `schedule`, `sensor`, `backfill`, `system`.
- **`run_id`** is set on a run event and null on everything else. a schedule
  fire that launched a run puts that run in its *payload* rather than in this
  column, because the event is about the schedule — this column is what makes
  a run's page exactly the run's own log and nothing else.
- **`subject` is null on a run event.** the run is `run_id`, which was already
  there and already indexed; v17 deliberately did not copy it into `subject`,
  because doing so is a full rewrite of the largest table in the database to
  store a second copy of a column. `Event::about()` in rust — and
  `ev.subject ?? ev.run_id` in the ui — is where the two become one answer, and
  the api's `subject=` filter matches either.
- **`op`** is set on the run events that belong to one op.
- **`level`** is `info`, `warn` or `error`, and it is not the same claim as the
  kind: a check that failed at severity `warn` is a `check_failed` at level
  `warn`, because the run it belongs to succeeded.

## Where an event is written, and why that is the whole design

an event is a claim that something happened. if it is written *next to* the
thing rather than *with* it, then a crash in the gap produces one of two lies:
a log that says a thing happened which did not, or a thing that happened and
left no record. so every event added in v17 is written by the subsystem that
does the work, in the same transaction as the row that is the work — the same
rule phase 21 applied to a run's terminal notification.

| what | transaction it joins | atomic |
| --- | --- | --- |
| `run_queued` | the `runs` insert | yes |
| `run_success` / `run_failed` / `run_canceled` | — | **no**, see below |
| `run_reclaimed` | the reclaim's status change | yes |
| `op_*`, `type_check_failed`, `log` | — | **no**, see below |
| `asset_materialized` | the `asset_materializations` insert | yes |
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
them leaves a run that is queued and will execute, with no tick and no event —
recoverable, and visible as a run whose trigger is `schedule`. the other
direction, an event claiming a run that was never created, cannot happen. the
same applies to `backfill_chunk`.

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
| `run_reclaimed` | warn | `claimer` — the instance that stopped renewing — and `policy`, `fail` or `requeue` |

a reclaimed run under `fail` gets `run_reclaimed` **and then** `run_failed`:
the first says why, the second says what the run did. under `requeue` it gets
only the first, because the run has not ended.

### Ops

all carry `run_id` and `op`.

| kind | level | payload |
| --- | --- | --- |
| `op_started` | info | `attempt` |
| `op_expanded` | info | `instances`, `over` — the dep the fan-out mapped |
| `op_success` | info | `attempt`, `output_type` (optional), `meta` (optional) |
| `op_retry` | error | `attempt`, `error` |
| `op_failed` | error | `attempt`, `error` |
| `op_skipped` | warn | `reason`, and `when` or `upstream` depending on which rule skipped it |
| `op_canceled` | warn | `reason`, `stopped` |
| `type_check_failed` | error | `error` |
| `log` | as emitted | whatever `ctx.info`/`warn`/`error` attached, usually null |

`meta` on `op_success` is [the tagged map](metadata.md) the attempt reported
with `ctx.meta`, exactly as the op run carries it — so a consumer following the
log alone sees the row counts and the byte sizes without fetching the op run.

`stopped` on `op_canceled` is the one that matters: `true` means the work
provably stopped (an [isolated](isolation.md) op's process was signalled,
killed and reaped), `false` means cancellation was *requested* and the op was
never observed to stop.

### Assets

| kind | level | payload |
| --- | --- | --- |
| `asset_materialized` | info | `partition` (optional), `fingerprint`, `run_id` (optional), `meta` (optional) |
| `check_passed` | info | `check`, `partition` (optional), `status`, `severity`, `message` (optional), `run_id`, `meta` (optional) |
| `check_failed` | warn or error | as above; the level follows the *severity*, not the verdict |

`subject` is the asset name. the partition is in the payload rather than in the
subject, so a filter on one asset finds every key of it.

`run_id` in the payload is where the build happened; it is null for a
materialization a [probe](assets.md) recorded outside any run.

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
the transaction that deleted it. the other caps — the two tick logs,
materialization history, delivered notifications, and the event log's own — are
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
which is why hestan's own reader does not either — an unrecognised kind reads
as `EventKind::Unknown("…")` carrying the stored word, rather than failing the
query and taking the rest of the page with it. the same is true of
`subject_kind`.

## Retention

a run's events are deleted with the run, by [retention](storage.md), and always
were.

everything v17 added belongs to no run and so belongs to no run's retention
either. those events are capped instead at the newest **50,000**, swept by the
same loop, unconditionally — an asset built every five minutes writes a row
here forever otherwise. the cap is not configurable today; it is a size limit
on a table that grows with time rather than with the history anybody asked to
keep.
