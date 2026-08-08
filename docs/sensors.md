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
    Ok(vec![RunRequest { job: "ingest_marker".into(), params: json!({}) }])
});

Hestan::new().sensor(marker_watch) // stackable
```

`serve` runs one loop for every registered sensor (probes included, below):
each entry evaluates once at startup, then on its own interval, and the loop
always sleeps until the earliest due entry. `every` is the gap between
evaluations, so a slow closure pushes its own next due time back rather than
stacking. `run_once` and `build_asset` are headless and run no sensor loop,
same as schedules.

## Evaluation, exactly

one evaluation: load the committed cursor → run the closure → launch every
returned `RunRequest` with trigger `sensor` → commit the staged cursor only
if all of that succeeded → record a tick.

- the closure returns `Err` (or panics — caught, like ops): tick `error`,
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

## Probes are sensors

every source asset with a [probe](assets.md) becomes an internal sensor
named `probe:<asset>` on the same loop: same pausing, same tick history,
listed by the same endpoint. its evaluation compares the probe's fingerprint
against the stored one. a changed fingerprint rewrites the source
materialization; then, changed or not, every `.auto()` descendant that
staleness proves stale is launched as one combined build run (trigger
`build`), so
`launched` is 1 when a run went out and 0 when nothing was owed.

re-deriving from staleness on every tick is the probe's self-heal. the
fingerprint commits before the launch, so once it is written nothing in the
data will ask for that build again — the source would have to change a
second time. a launch that failed, or that was skipped because an assets
build was already active (builds are serialized; see [assets](assets.md)),
is therefore picked up by the next tick instead of stranding the descendant
stale. the usual unchanged tick stays the cheap, boring `fired` /
`launched: 0`.

sensor names share one namespace: two sensors named alike, or a user sensor
colliding with a probe name, fail startup with a graph error.

## Pausing, ticks, sync

`POST /api/sensors/state {"name", "paused"}` flips the flag (404 for an
unknown name). a paused sensor is not evaluated: no tick, no cursor
movement. its schedule keeps ticking over regardless, so resuming picks up
at the next interval rather than with a burst of catch-ups.

every evaluation of an unpaused sensor lands in `sensor_ticks`: outcome
(`fired | error`), how many runs launched, the error message if any.
`GET /api/sensors/ticks?sensor=&limit=` reads it newest-first; at boot the
table is pruned to the newest 5000, the same policy as schedule ticks.

at startup the sensors table is synced to the code like schedules are: new
names inserted, undeclared rows dropped, surviving rows keeping their paused
flag and cursor across restarts. renaming a sensor is not a rename to the
store — it is a new row, and the old cursor goes with the old name.

`GET /api/sensors` lists each sensor with its interval, paused flag, cursor,
and last tick — shapes in [http api](http-api.md), tables in
[storage](storage.md).
