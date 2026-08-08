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

## The scheduler loop

`serve` runs one in-process scheduler task. each iteration computes every
schedule's next fire, sleeps until the earliest (capped at 60s so clock drift
self-corrects within a minute), then fires everything that has come due.
pause state is read from the database at fire time, not at startup. two
schedules on the same job that share a fire instant launch one run, not two —
the runner-up records a `skipped` tick, so the dedupe is visible in the fire
history rather than silent. each fire is recorded as a tick (below) whether
the launch succeeded or not.

expressions with no future fires left (a specific date now in the past)
are dropped with a warning; once every schedule is exhausted the scheduler
task exits. that's per-process state: an exhausted schedule comes back on
restart if it has fires again.

## Pause and resume

each `(job, expression)` pair has a persisted paused flag, toggled from the ui
(job page, or the command palette) or via
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

pausing a schedule also drops its waiting deferred fire (recorded as a
`skipped` tick): pause means stop, including the catch-up. the wait itself
lives in scheduler memory, so a process restart drops a waiting fire — but
its `deferred` tick is already on disk, so the audit trail survives as a
`deferred` tick with no `fired` twin, and the next cron fire proceeds
normally. the tick log is pruned to the newest 5000 rows at startup, which
matters under skip: a schedule that keeps firing into a long run writes one
skipped tick per fire.

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
