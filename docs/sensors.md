# Sensors

a sensor is a polling closure with a cursor. every `every` it looks at the
outside world and returns the runs it wants launched, usually none: a
directory that may have a new file in it, a queue with something on it, an
api with a changed-since feed. schedules answer "it is 09:00"; sensors
answer "a file showed up".

```rust
let marker_watch = Sensor::new("marker_file", Duration::from_secs(5), |ctx| async move {
    let Ok(meta) = fs::metadata("ingest.marker") else {
        return Ok(Vec::new());
    };
    let mtime = meta.modified()?.duration_since(UNIX_EPOCH)?.as_millis() as u64;
    if ctx.cursor_as::<u64>()? == Some(mtime) {
        return Ok(Vec::new()); // seen it already
    }
    ctx.set_cursor(json!(mtime));
    Ok(vec![RunRequest::new("ingest_marker").key(mtime.to_string())])
});

Hestan::new().sensor(marker_watch) // stackable
```

`serve` runs one loop for every registered sensor (probes included, below):
each entry evaluates once at startup, then on its own interval, and the loop
always sleeps until the earliest due entry. `every` is the gap between
evaluations, counted from the end of the last one, so a slow closure pushes its
own next due time back rather than stacking. `run_once` and `build_asset` are
headless and run no sensor loop, same as schedules.

## Evaluation, exactly

one evaluation: load the committed cursor → run the closure → launch every
returned `RunRequest` with trigger `sensor` → commit the staged cursor only
if all of that succeeded → record a tick.

- the closure returns `Err` (or panics, caught like ops): tick `error`,
  cursor untouched, nothing launched.
- a launch fails (unknown job included): tick `error` with the message.
  runs launched before the failure stay launched, and the tick's `launched`
  count says how many; the cursor stays put, so the next evaluation sees the
  same world and may re-request. sensor-launched work should tolerate a
  duplicate request, the usual at-least-once posture.
- full success: the cursor staged via `set_cursor` commits (no call, no
  change), tick `fired` with the launch count. `launched: 0` is the common,
  boring case.

the cursor is one json value per sensor, `store`-backed like op state:
`cursor()` / `cursor_as::<T>()` read what the last fully-successful
evaluation committed. commit-on-success is what makes the marker example
correct: if the launch fails, the mtime is not recorded, so the file is
retried instead of silently dropped.

## Run keys

at-least-once is defensible, but it puts deduplication on every caller. a run
key moves it into hestan:

```rust
RunRequest::new("publish").params(json!({"day": day})).key(day)
```

a keyed request launches **at most once per key, ever, for that sensor**. the
loop reads the key before launching and skips a claimed one (the tick counts
it under `skipped` rather than `launched`), and the key itself is written in
the same transaction that creates the run. that is the part that matters: a key
recorded for a run that never launched would drop that work forever and nobody
would ever notice, which is strictly worse than the duplicate a key exists to
prevent. insert-then-delete-on-failure has that window; one transaction does
not. a launch that fails leaves no key, so the next evaluation asks again.

so a key turns at-least-once into **effectively-once per sensor**. per sensor:
keys are scoped to the name that used them, two sensors may use the same string
for different things, and nothing looks across them. a keyless request is
unchanged (at-least-once, exactly as before), which is still the right default
for work that is naturally idempotent.

pick a key that names the *work*, not the moment: the partition, the day, the
upstream run id, the file's mtime. `"2026-08-09"` is a key; `Utc::now()` is not.

**keys are never collected on their own.** a daily-keyed sensor writes a row a
day for as long as the database exists.
[retention](storage.md#retention) prunes them on the same age cutoff it prunes
runs on, on every sweep rather than only at boot; without a policy they
accumulate. that is deliberate: a key deleted early is a duplicate launch, so
nothing throws one away by default.

## Timeouts and concurrency

due sensors evaluate **on tasks of their own**, at most 8 at a time. in
sequence, one closure blocking on a dead endpoint delayed every sensor and
every probe behind it, which is the failure mode where a fifteen-second
sensor quietly becomes a fifteen-minute one. the bound is there because the
alternative (every due entry at once) is how a hundred sensors become a
hundred concurrent api calls.

**two evaluations of the same sensor never overlap.** a sensor whose previous
evaluation is still going is skipped for that turn, not queued behind it: a
queued second evaluation could commit a cursor over a newer one, and a backlog
of them is not what `every` asked for. the skip lands as a `skipped` tick,
once per stall, not once per turn, so a sensor wedged for an hour cannot bury
every other sensor's history under its own.

each evaluation has a **timeout**, 60s unless `Sensor::timeout(d)` (or
`RunStatusSensor::timeout(d)`) says otherwise; probes get the same 60s and no
way to change it, because a fingerprint that takes a minute is a broken probe.
on expiry the evaluation is abandoned, the tick records the timeout as an
error, and the staged cursor is not committed.

abandoning is not stopping, and this is exactly the limit ops have. an `.await`
inside the closure is where an abandoned evaluation actually goes away; a
closure doing blocking work between await points cannot be dropped at all, so
it keeps its thread until that work returns, and if it does return, late, what
it returns still counts. nothing else can have run in the meantime, because the
sensor was held. `ctx.is_cancelled()` is the cooperative half, true once the
deadline has passed:

```rust
Sensor::new("crunch", Duration::from_secs(60), |ctx| async move {
    for chunk in chunks {
        if ctx.is_cancelled() {
            return Err("evaluation timed out".into());
        }
        crunch(chunk);            // blocking, so nothing can drop this future
    }
    Ok(Vec::new())
})
```

## Failure backoff

a sensor that errors is evaluated less and less often until it stops erroring:
the gap doubles from its own interval to a 15 minute cap, with jitter, and the
**first success collapses it straight back**. an endpoint that has been down
for an hour does not need polling every five seconds, and hammering it is how
one broken sensor becomes a log flood and a rate-limit ban.

the floor doubles along with the ceiling (a gap after three failures is
somewhere in `[4×every, 8×every]`, never below), so the wait genuinely
lengthens rather than merely lengthening on average, and the jitter is there so
a fleet of sensors watching the same dead endpoint does not come back in
lockstep. a sensor whose interval is already past the cap is left alone: there
is nothing to back off to, and speeding it up would be the opposite of the
point.

backoff is per sensor and lives in memory, so a restart starts everything
fresh. `GET /api/sensors` reports `next_eval` and `consecutive_failures`, and
the ui tags a backing-off sensor rather than leaving it looking merely slow.

## Probes are sensors

every source asset with a [probe](assets.md) becomes an internal sensor
named `probe:<asset>` on the same loop: same pausing, same tick history,
listed by the same endpoint. its evaluation compares the probe's fingerprint
against the stored one. a changed fingerprint rewrites the source
materialization; then, changed or not, every descendant whose
[automation policy](assets.md#automation-policies) wants a build is launched as
one combined build run (trigger `build`), so
`launched` is 1 when a run went out and 0 when nothing was owed.

re-deriving what is owed on every tick is the probe's self-heal. the
fingerprint commits before the launch, so once it is written nothing in the
data will ask for that build again: the source would have to change a
second time. a launch that failed, or that was skipped because an assets
build was already active (builds are serialized; see [assets](assets.md)),
is therefore picked up by the next tick instead of stranding the descendant
stale. the usual unchanged tick stays the cheap, boring `fired` /
`launched: 0`.

sensor names share one namespace: two sensors named alike, or a user sensor
colliding with a probe or a `run:` name, fail startup with a graph error.

## Run-status sensors

"when job A succeeds, run job B" is the same shape again, with the run log as
the outside world:

```rust
Hestan::new().run_sensor(
    RunStatusSensor::new("chain", |_ctx, run: RunSummary| async move {
        Ok(vec![RunRequest::new("publish").params(json!({"from": run.id}))])
    })
    .on([RunStatus::Success])      // which terminal statuses; success by default
    .for_job("orders_etl")         // optional; every job when absent
    .every(Duration::from_secs(15)),
)
```

it registers as `run:{name}` and is a **third source on the one sensor loop**,
exactly as probes are a second: same interval handling, same pausing, same
tick history, same cursor column, same endpoint. there is no second loop and
no second set of concepts.

the closure is handed a `RunSummary` (`id`, `job`, `status`, `trigger`,
`started_at`, `finished_at`, `error`) and not the internal `Run`. what a chain
needs to decide is which run it was and how it went, not the params blob or the
resume chain, and a small public struct is a promise that can be kept.

### Its cursor

the cursor is the last terminal run it read, as `{"finished_at", "id"}`. each
evaluation reads terminal runs after that pair (ordered by finish time then
id, so two runs finishing in the same instant can neither be skipped nor seen
twice), calls the closure once per run that matches the status filter, launches
what comes back, and commits the cursor **only after every launch succeeded**.

that is the same at-least-once contract a user sensor has, and it has the same
consequence: a launch that fails halfway leaves the cursor where it was, so the
runs already handled are handed over again on the next tick. downstream work
should tolerate a duplicate request, or the chain should give each request a
[run key](#run-keys), and `.key(run.id)` is the obvious one here.

the cursor covers every run *read*, not every run matched, so a filtered-out
failure is consumed rather than re-read forever. a page is capped at 200 runs
per evaluation, so a sensor resumed after a long pause drains its backlog over
a few ticks instead of launching thousands of runs at once.

a **new** run sensor seeds its cursor from the newest terminal run and chains
nothing on its first evaluation: it is there for what happens next, not for the
run log it was added to. adding one to a busy process does not replay history.

### A chain can feed itself

a closure that launches the job whose run triggered it is legal, and it will
run forever: the launched run finishes, the sensor sees it, and round it goes.
nothing stops you, because "re-run myself until a condition holds" is a real
thing to want.

what makes it safe is the filter. `for_job` restricts what it watches, the
status list restricts which outcomes count, and the closure can look at what it
was handed (`run.trigger != Trigger::Manual`, say) and return no requests.
one of those has to break the cycle.

## Pausing, ticks, sync

`POST /api/sensors/state {"name", "paused"}` flips the flag (404 for an
unknown name), and so do `hestan pause sensor <name>` and
`hestan unpause sensor <name>` ([the command line](cli.md)). a paused sensor is not evaluated: no tick, no cursor
movement. its schedule keeps ticking over regardless, so resuming picks up
at the next interval rather than with a burst of catch-ups.

**the flag fails closed.** a read of it that fails counts as paused, and the
turn is held. it used to count as running, which is an administrative stop
failing open, the one direction it must not fail in. a turn not taken is
recoverable; a launch nobody asked for is not. schedules do the same with
theirs ([scheduling](scheduling.md#pause-and-resume)). launching itself is
deliberately still at-least-once: this is about the switch, not about making
firing stricter.

every evaluation of an unpaused sensor lands in `sensor_ticks`: outcome
(`fired | error | skipped`), how many runs launched, how many keyed requests
were skipped, how long it took in milliseconds, the error message if any.
those four are what answer "is this sensor healthy" without reading the log:
a sensor whose duration is climbing is a sensor about to hit its timeout, and
`launched: 0, skipped: 3` is a different fact from launching nothing. the
sensors table shows them all against each sensor's last tick.
`GET /api/sensors/ticks?sensor=&limit=` reads it newest-first; the
[retention sweep](storage.md#retention) prunes the table to the newest 5000 on
every pass, the same policy as schedule ticks.

at startup the sensors table is synced to the code like schedules are: new
names inserted, undeclared rows dropped, surviving rows keeping their paused
flag and cursor across restarts. renaming a sensor is not a rename to the
store: it is a new row, and the old cursor goes with the old name.

`GET /api/sensors` lists each sensor with its interval, paused flag, cursor,
filter and last tick; shapes in [http api](http-api.md), tables in
[storage](storage.md). the ui's sensors table shows all three kinds together,
which is the point of them being one thing.
