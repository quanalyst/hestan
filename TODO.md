# todo

things worth building, with enough of the shape worked out that picking one up
does not start from a blank page. not a roadmap and not a promise.

this file is not in the published crate; the `include` allowlist in
`Cargo.toml` keeps it out.

## a hook that can say what the run produced

**the gap.** `on_run_finished` already fires on every terminal status,
success included, so a success hook works today. what it is handed does not.
`RunEvent` (`src/hooks.rs`) carries `run_id`, `job`, `owner`, `trigger`,
`status`, `failed_op` and `error`, and nothing about **what the run made**. A
hook that wants to report yesterday's volumes has to open the store itself,
by run id, and know the schema to do it.

phase 42 built the other half and the two have never met: `ctx.saved()` marks a
sample of what an op wrote, and `Meta::series` holds a timeseries sampled
across its range. **a plot is a series.** the pieces for "post the chart when
the run succeeds" all exist and there is no path between them.

what to work out:

- does `RunEvent` gain the saved samples, or a cheap handle that fetches them?
  carrying them means a hook payload that can be megabytes on a fan-out; a
  handle means a hook that can fail at read time, after the run is over.
- a hook fires per run. a report usually wants one asset's history, not one
  run's output. decide whether that is the same feature or a different one.
- the durable delivery path (phase 33) writes the notification into the same
  transaction as the terminal row. a payload that has to be read back later
  cannot be written there. say which guarantee a report hook gets.

## teams, and email

`notify::slack` and `notify::webhook` are the only built-ins.

- **teams** is the small one: another webhook with a different body. it wants
  an adaptive card rather than slack's blocks, and the owner line and the run
  link belong in it the way they do in the slack helper.
- **email** is the one that looks small and is not. smtp means a dependency,
  credentials hestan would then be holding, and a delivery failure mode that is
  not http's. **look at whether it belongs in hestan at all**, or whether the
  honest answer is a documented example that hands a `RunEvent` to whatever the
  deployment already uses to send mail. `docs/notifications.md` says a hook is
  your own code; email may be the case that proves it.
- whichever ships, it inherits phase 33's known limit: the shipped helpers
  spawn their request and return, so a failed post is logged rather than
  retried. the retry path is durable delivery, and it covers your own hooks.

## success as a trigger, not just a notification

the note that started this: "hook plots and reports on success, other pipelines
maybe". there are two different things in that sentence.

- **report on success** is the hook above.
- **another pipeline on success** is `RunStatusSensor`, which already exists
  (`src/sensor.rs`) and already watches for a run reaching a status. if that is
  what somebody wants, the answer is documentation rather than code, and the
  fact that it was not obvious is itself the bug. check whether
  `docs/sensors.md` makes it findable from the word "success".

## smaller, and already written down elsewhere

- `Store::runs` takes seven positional filters and wants a `RunQuery` struct.
  `EventQuery` is already that shape. the next filter should force it; noted in
  the method's own docs.
- 31 public structs have public fields, so a new field breaks a literal.
  `docs/stability.md` records the decision to leave them and why constructors
  come first.
- the `ui/dist` bundles accumulate: every `npm run build` leaves the old hashed
  files behind and nothing prunes them.
