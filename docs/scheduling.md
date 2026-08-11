# Scheduling

## Attaching schedules

```rust
Hestan::new()
    .job(etl)
    .schedule("etl", "*/10 * * * *")                       // utc
    .schedule_tz("report", "0 9 * * 1-5", "Europe/London") // iana tz
    .serve(([127, 0, 0, 1], 4000))
    .await
```

a job can carry several schedules; a schedule on an unregistered job, a bad
expression, or an unknown timezone is an error from `serve`/`run_once` — at
startup, not at fire time. scheduled runs carry the trigger `schedule`.

the surface above is the short way to say the common thing. once a schedule
wants more than a job and an expression there is a builder, which is the same
declaration without four positional arguments:

```rust
Hestan::new()
    .job(etl)
    .add_schedule(
        Schedule::new("etl", "0 * * * *")
            .tz("Europe/London")
            .params(json!({"region": "eu"}))
            .catchup(Catchup::All { limit: 24 }),
    )
```

`schedule`, `schedule_tz`, `schedule_with` and `schedule_tz_with` all build one
of these with the defaults filled in — utc, `{}`, `Catchup::Skip` — so nothing
that already works changes.

## Params

`schedule` and `schedule_tz` fire with params `{}`. `schedule_with` and
`schedule_tz_with` take what every fire should launch with instead:

```rust
Hestan::new()
    .job(backfill)
    .schedule_with("backfill", "0 3 * * *", json!({"days": 1}))
    .schedule_tz_with("report", "0 9 * * 1-5", "Europe/London", json!({"full": true}))
```

they are validated **at build**, not at fire time: `serve`/`run_once` run each
schedule's params through the same op validators a launch runs, so a schedule
a job's ops could never accept is an `Error::InvalidParams` naming the op, the
expression and the job — instead of a tick that fails every night at 3am. that
also means a job whose ops declare required params needs `schedule_with`; the
plain form's `{}` is refused at startup.

the params live in the schedules table and sync from the code like the
timezone: changing them updates the row in place, and the paused flag survives
(as ever, changing the *expression* is a new row). a queue-policy fire that
waits launches with the params it was held with, not with whatever the
declaration says by the time its turn comes.

## Cron syntax

expressions are standard 5-field crontab: minute, hour, day-of-month, month,
day-of-week. internally the `cron` crate wants a seconds field, so a 5-field
expression is normalized by prepending `0` — fires always land on second zero.
expressions with six or more fields are passed through untouched, so you *can*
write seconds-resolution schedules, but the day-of-week remap below only
applies to the 5-field form.

### Day-of-week numbering

posix crontab numbers sunday as 0 (or 7); the `cron` crate numbers the week 1
(sunday) through 7 (saturday). hestan remaps so **crontab numbering is what
you write**: bare numbers and range endpoints `0..=7` are converted
(`(n % 7) + 1`), names (`MON`) and step divisors (`*/2`) are left alone.

- `0 9 * * 1` — 9:00 monday
- `0 9 * * 0` and `0 9 * * 7` — 9:00 sunday
- `0 9 * * 1-5` — weekdays

one caveat: a numeric range **ending in 7**, like `5-7` (friday–sunday), remaps
to an inverted range and fails to parse — loudly, at startup, which beats
firing on the wrong day. write `5-6,0` or `FRI-SUN` instead.

## Timezones

`schedule` evaluates in utc; `schedule_tz` takes an iana zone name
(`America/New_York`) and evaluates the expression in that zone, so `0 9 * * *`
tracks local 9:00 across dst transitions. everything is converted to utc for
storage and display; the ui shows the zone next to the expression when it
isn't utc.

at the transitions themselves: a local time that occurs twice on fall-back
fires at both offsets (an hourly new york job runs 01:00 EDT *and* 01:00
EST), while a local time that doesn't exist on spring-forward is skipped for
that day (a daily 02:30 new york job next fires the day after).

## The cursor

every `(job, expression)` pair carries a **cursor**: the newest occurrence the
scheduler has accounted for — fired, skipped, held, or deliberately dropped.
it lives in the `schedules` table and is written after every one of those, so
it survives a restart.

it exists because of a question the scheduler could not previously answer.
computing fires relative to *now* means downtime is invisible: a process that
was dead from 08:00 to 10:30 comes back, asks "when is the next fire", and the
occurrences at 08:00, 09:00 and 10:00 are simply gone. with a cursor at 07:00,
**everything strictly after the cursor and strictly before now is the missed
set** — knowable, enumerable, and something a policy can be applied to.

the first time a process sees a schedule the cursor is `null`, and it is set to
now rather than to the beginning of the expression's history: a schedule
declared today has no downtime behind it, and treating its whole cron past as
missed would fire the epoch.

a schedule that is **paused** advances its cursor over the gap without firing.
pause means stop, including the catch-up: resuming a schedule paused for a week
must not fire a week of backlog.

## Missed-fire catch-up

what to do with the missed set is a per-schedule policy:

```rust
Schedule::new("hourly_rollup", "0 * * * *").catchup(Catchup::All { limit: 24 })
```

- `Catchup::Skip` (the default): advance the cursor over them and fire nothing.
  exactly what the scheduler did before it had a cursor. no ticks either — a
  process down for a week would otherwise write a week of skipped ticks on
  boot.
- `Catchup::One`: fire the most recent missed occurrence only. for a job that
  computes current state, where the last one subsumes the rest.
- `Catchup::All { limit }`: fire every missed occurrence, oldest first, at most
  `limit` of them (below 1 means 1). past the cap the **oldest are dropped**,
  and the drop is recorded — a `skipped` tick at the oldest dropped occurrence
  whose error reads `catch-up cap 24: dropped 9 missed occurrences up to
  2026-03-04T01:00:00Z`, plus a warning in the log. a backlog quietly losing
  its head is the failure mode this policy exists to avoid.

**caught-up fires queue; they never overlap.** the first launches immediately
if the job is free, and the rest are held and drained one at a time as it
frees up. that is deliberate: the overlap policy governs a live fire landing on
a busy job, while catch-up governs occurrences the process was never there for,
and firing 24 hours of backlog concurrently is not what anyone means by
"catch up".

## Which hour is this run for

a caught-up run is useless to a data pipeline that cannot tell which logical
time it stands for. `runs.scheduled_for` is that time — the cron occurrence,
not the wall clock the run started at — and ops read it back:

```rust
Op::new("pull", |ctx: OpCtx| async move {
    let hour = ctx.scheduled_for().expect("scheduled");
    Ok(json!({ "rows": pull_orders_for(hour).await? }))
})
```

it is set on scheduled fires, caught-up fires and held fires that later drain,
and it is `None` on a manual launch, a retry, a resume, an asset build and a
sensor fire — all of which stand for nothing but themselves. it is on the run
json, and the ui shows it next to the trigger.

## The scheduler loop

`serve` runs one in-process scheduler task. each iteration reconciles every
schedule's cursor (above), drains anything waiting, computes every schedule's
next fire, sleeps until the earliest (capped at 60s so clock drift
self-corrects within a minute, and at 2s while a
[deferred](#overlap-policy) fire is waiting to drain), then fires everything
that has come due.
pause state is read from the database at fire time, not at startup. two
schedules on the same job that share a fire instant launch one run, not two —
the runner-up records a `skipped` tick, so the dedupe is visible in the fire
history rather than silent. each fire is recorded as a tick (below) whether
the launch succeeded or not.

expressions with no future fires left (a specific date now in the past)
are dropped with a warning; once every schedule is exhausted **and nothing is
still held**, the scheduler task exits — a held fire keeps it alive so that
fire can still drain. that's per-process state: an exhausted schedule comes
back on restart if it has fires again.

## Pause and resume

each `(job, expression)` pair has a persisted paused flag, toggled from the ui
(job page, or the command palette), from a terminal with
`hestan pause schedule <job> [--expr CRON]` and `hestan unpause schedule`
([the command line](cli.md)), or via
`POST /api/schedules/state {"job": ..., "expr": ..., "paused": true}`. a
paused schedule is skipped at fire time and records **no tick**. the flag
lives in the database and survives restarts; on startup hestan syncs the
schedules table to the code (new pairs inserted, removed pairs deleted,
timezone refreshed) while pause state on surviving pairs is kept.

one consequence of the row identity being `(job, expression)`: editing a
schedule's cron expression in code creates a *new* pair on the next startup —
the old row (and its paused flag) is deleted, so the edited schedule comes
back unpaused. editing only the timezone updates the existing row in place
and preserves the flag.

**the flag fails closed.** if the read that determines pause state fails, the
pass fires nothing and moves no cursor, rather than treating an unreadable flag
as unpaused. that is the only direction it can fail in: a missed occurrence is
recoverable — the next pass's catch-up sees it, and honours the flag it can
read by then — where a launch nobody asked for is not. the pass logs a warning
and retries a second later. sensors do the same thing with theirs
([sensors](sensors.md#pausing-ticks-sync)); launching itself is deliberately
still at-least-once, and this is about the administrative switch only.

## Ticks

every actual fire lands in a tick log: the `(job, expr)` pair, the instant
the fire was scheduled for, when it actually fired, and the outcome —
`fired` (with the launched run's id), `error` (with the failure message),
`skipped` (a fire dropped by the overlap policy or the same-instant dedupe),
or `deferred` (a queue-policy fire waiting its turn, below) — queryable via
`GET /api/schedules/ticks` and shown on the job page. ticks answer "did the
schedule do its job at 09:00" separately from "did the run succeed".

## Upcoming projection

`GET /api/schedules/upcoming?window=<secs>` projects future fires inside the
window for every unpaused schedule (capped at 100 fires per schedule). the ui
draws these as ghost marks on the future side of the timeline.

## Overlap policy

a scheduled fire that lands while the job still has an active run consults the
job's overlap policy: `Job::builder(..).overlap(Overlap::...)`.

- `Skip` (the default): the fire is dropped and a `skipped` tick is recorded.
- `Queue`: the fire waits until the active run finishes, then launches with
  its original `scheduled_for`. a deferral shows in the tick log as a pair: a
  `deferred` tick (no run id) recorded the moment the fire is held, then a
  `fired` tick when it launches, both carrying the same `scheduled_for` (the
  `fired_at` gap shows the delay). while one fire is waiting, further fires
  of the same job are recorded `skipped` — a job that missed three ticks
  catches up once, not three times.
- `Allow`: pre-policy behavior; concurrent runs of the same job are fine.

the policy gates scheduled fires only. manual launches, the retry endpoint,
and `run_once` are never held back.

"active" means the job has a run **outstanding** — queued or running, claimed
or not — and not "a run is executing this second". the distinction only came
up once the [queue](scaling.md) made a queued run something that can sit there
for a while, and outstanding is the answer overlap wants: a job held back by a
concurrency limit would otherwise collect a fire every minute behind the run
it is waiting on, which is the pile-up `Skip` exists to prevent. a
[concurrency limit](scaling.md#limits-are-not-overlap-policies) is the other
question and counts the other set.

**that pair is the queue, not a record of it.** a fire is waiting exactly when
it has a `deferred` tick with no later tick for the same occurrence, which is a
question the database answers — so a fire held when the process died is still
held when it comes back, and drains then. nothing about the wait lives in
memory. one consequence: a fire reconstructed after a restart launches with the
schedule's *current* params, since the process that held the old ones is gone;
within a process it still launches with the params it was held with, which are
the same thing unless the declaration changed across the restart.

pausing a schedule drops its waiting fire (recorded as a `skipped` tick), and
so does deleting the schedule from the code — nothing else knows what params it
should have launched with. the tick log is pruned to the newest 5000 rows by
the [retention sweep](storage.md#retention) — at startup and every hour after
it — which matters under skip (a schedule firing into a long run writes one
skipped tick per fire) and is also the one way a held fire can be forgotten: a
`deferred` tick pruned away is a fire nobody is waiting for any more.

## Overdue and interval_secs

the jobs api derives a freshness signal per job. `interval_secs` is the gap
between the schedule's next two fires, minimized across the job's unpaused
schedules — `null` when the job has no active schedule. `overdue` anchors on
the *previous* scheduled fire (the latest across the job's unpaused
schedules): true when that fire is more than half an interval in the past and
no successful run has finished since it — including the case where the job
has never succeeded at all. a job with no active schedule is never overdue.
the ui tags overdue jobs in the jobs table. anchoring on the previous fire
rather than on interval-sized windows keeps clustered schedules honest: a
weekday-only job that succeeded friday morning is not overdue on sunday,
because sunday's silence was on the calendar.

a job that declares [`fresh_within`](freshness.md) is not asked the heuristic
at all: `overdue` is always false there and `freshness` is the answer. the
heuristic guesses at what a policy states outright, and reporting both would
be two answers to one question.
