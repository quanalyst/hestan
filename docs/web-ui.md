# Web ui

the ui is a react bundle compiled into the binary; at deploy time the
executable is all there is. it polls the [json api](http-api.md) and works
entirely from real state. five pages: jobs (the overview at `/`), a job page,
assets, runs, and a run page.

## The status language

the ui is monochrome, so shape carries state — grey level alone never does:

- solid disc: success
- ×: failed
- arc with a center dot, spinning: running
- hollow circle: queued / pending
- dashed hollow circle: skipped
- hollow diamond: canceled

bars follow the same code: solid fill for success, 45° hatch for failed,
animated fill for running, a hollow outline for queued, and a dimmed muted
fill for canceled. dimming is the one place grey level does the work, so
tooltips and legends pair it with the diamond. every legend in the ui is
built from these swatches, and legends only appear once two or more states
are actually present.

asset freshness borrows the same shapes: the solid disc marks a fresh asset,
the hollow ring a stale one, always with the word printed beside it.

## Jobs (overview)

the page opens with a statline for the selected window: run count, success
rate over finished runs, p95 duration, and how many are running right now.
with no runs it says so ("no runs in the last 6 hours") rather than showing
zeros.

under it, the timeline: one lane per registered job (runs of a job that has
since left the code still get a lane), a 1h/6h/24h window switch, and about
15% of the axis reserved right of the "now" line for the future. runs draw as
bars; overlapping runs within a job pack greedily into stacked sub-lanes
rather than drawing over each other. projected schedule fires render as
hollow ghost slivers in the future zone — hover names the schedule and time,
click goes to the job. failed runs additionally get an × in a strip under
the plot; hovering one fetches the run and names the first op that failed,
so you can often diagnose without leaving the overview.

drag horizontally on the plot (more than a few pixels; a plain click still
opens the run under the cursor) to brush a time range: a selection panel
lists every run that overlaps it. escape, clicking elsewhere, or "clear"
dismisses it.

the jobs table shows each job's description, op count, schedule expressions
(paused ones muted and tagged), a duration sparkline of its recent finished
runs in the window (hatched bars are failures), and the last run's status
glyph with a relative time. a job whose previous scheduled fire is more than
half an interval in the past, with no successful run finishing since,
carries an `overdue` tag; [scheduling](scheduling.md) has the exact rule.
rows click through to the job page. the page polls jobs every 5s, window
runs every 10s, and upcoming fires every 30s.

## Job page

the header holds the launch button and a `params` toggle that opens a json
editor. the editor validates as you type (invalid json disables launch), an
empty editor launches with `{}`, and the text persists per job in
localStorage, so the params you used last time are still there next visit. when
any op declares `.params::<P>()` the type name is shown above the editor; a
launch the server rejects (a 400 from params validation) surfaces the
server's message inline, and nothing is recorded.

the graph section draws the dag, columns laid out by longest dependency path.
clicking a node opens the op inspector: deps and dependents, the retry
budget, declared params/input/output types, and history over the last 50
runs: average and p95 duration, failure count, a duration trend, and the most
recent error verbatim. when the op has committed watermark state via
`ctx.set_state` ([op state](state.md)), a state line shows the value as json
(truncated past ~120 characters, hover for the whole thing) and when it was
committed; ops that never committed show nothing. escape closes it. when ops
declare output types, an ops list also shows each op as
`aggregate -> demo::Summary`.

schedules appear with their expression, timezone (when not utc), and a
countdown to the next fire, each with a pause/resume button. the toggle flips
optimistically and reverts with a message if the server refuses. under the
schedules, the last five ticks: a glyph for the outcome (solid for `fired`,
× for `error`, dashed for `skipped`, the pending ring for a `deferred` fire
still waiting), when the fire was scheduled for, a link to the launched run,
and the error message when the launch failed.

then a single-lane timeline (same windows, ghosts, and brushing as the
overview), a bar chart of the last finished runs' durations, and a table of
the ten most recent runs, with an "all runs" link into the runs page
pre-filtered to this job.

## Assets

the assets page shows everything registered through `Hestan::assets`,
polling every 5s. it opens with the asset dag in the same layout as job
graphs: a fresh asset wears the solid disc, a stale one the hollow ring,
with the word under the name; source assets (external data with a probe
instead of a body) additionally carry a muted `source` marker. nodes here
aren't clickable; the table below holds the detail.

the table lists assets in dependency order: name, kind, deps, the current
fingerprint as a 12-character prefix (hover for the full hash; an em dash
for an asset never built), when it was last built, and its state. a stale
asset says why: "dep X changed" or "N deps changed", with the recorded ->
current short hashes per dep on hover, or "never built" when no
materialization exists. assets declared `.auto()` carry an `auto` tag. every
derived row has a `build` action; sources have none, since the endpoint 400s
on them. a launched build (202) navigates straight to the new run,
while a build that finds nothing to do reports "up to date" inline. when any
asset is stale the header shows a "build stale" button that materializes
every stale asset as a single run.

asset builds are ordinary runs of the internal `assets` job, so the run
page, gantt, cancel, and re-run all apply unchanged; the `assets` job
appears on the jobs overview like any other job, and asset build runs carry
the `build` trigger on the runs page.

### Sensors

when sensors are registered, a sensors table sits at the bottom of the
assets page (with none, the section is absent entirely). the columns: name,
tagged `paused` when paused; the evaluation interval; the cursor as short
json, hover for the whole value; the last tick as an outcome glyph (solid
for `fired`, × for `error`, hover carries the evaluation error) with a
relative time; how many runs that tick launched; and a pause/resume toggle.
the toggle flips optimistically and reverts with a message if the server
refuses, exactly like schedule pausing. source probes appear here as sensors
named `probe:<asset>`; a probe that found no change still ticks `fired` with
zero launches. runs a sensor launches carry the `sensor` trigger.

## Runs page

if anything is queued or running, a "running now" section lists it with a
live elapsed clock (ticking every second; the ticker only runs while
something is active). below that, filters: status, trigger (manual,
schedule, retry, build, sensor), a time window (all/1h/6h/24h), and a quick
find box matching substrings of the job name or run id (escape clears it).
filters apply client-side to the loaded set.

the table is the newest 100 runs, polled every 5s; "load more" pages
backwards through history using the oldest loaded run's `created_at` plus its
`id` as a composite cursor (the id breaks created_at ties, so simultaneous
runs never vanish between pages), 100 at a time, until a short page marks
the history exhausted. terminal runs — success, failed, or canceled — have a
re-run button that launches a fresh run with the original params and
navigates to it; a refusal (409 when the run turns out to still be active or
the job has left the code, 400 when the old params no longer validate) shows
inline. each duration cell carries a bar scaled to the longest visible run;
canceled bars draw dim and muted, and canceled never appears in "running
now" — it is over, just not finished.

## Run page

the header names the job (linked), the short run id, trigger, creation time,
and duration, next to the status and one action: cancel while the run is
queued or running, re-run once it is terminal. cancel posts and disables
itself; cancellation is asynchronous, so the page keeps polling until the
status flips to canceled. if the run finished in the race, the server's 409
is swallowed — the next poll says the same thing better. canceled is
terminal: polling stops, and re-run works from a canceled run exactly as
from a failed one.

the dag reappears with each node showing that op's live status glyph and
label. clicking a node filters the log below to that op.

the gantt chart plots each op run against elapsed time from the first op's
start, with duration labels at the bar ends and a glyph in place of a bar for
ops that never started (pending or skipped). the critical path — the heaviest
chain of actual durations through the dep graph, walked back from the
last-finishing op — is drawn solid; off-path ops are muted; failed ops are
hatched; canceled ops draw dim and muted whatever their path position, with
the diamond for any that never got to start. it answers "what would I speed
up to make this run faster".

the log streams events while the run is live (polling every 1.5s on a
sequence cursor, stopping once the run is terminal). it auto-follows the tail
unless you've scrolled up. filters sit in the header: `all` vs `logs`
(`kind=log` only — just your `ctx.info/warn/error` lines), a level filter
(all/info/warn/error), and the op chip when one is selected in the dag.
system events print their kind (`op_retry`, `run_canceled`,
`type_check_failed`, ...) before the message.

## Command palette

`cmd-k` / `ctrl-k` anywhere. it searches jobs (name and description), the 50
most recent runs (id, job, status, trigger), and schedule actions — pause and
resume for every schedule. the query is tokenized; every token must match.
arrows move, enter performs, escape closes, tab is trapped so focus stays in
the input. a failed action (say, resuming a schedule that no longer exists)
prints the server's error at the foot of the palette.

## Empty states

nothing in the ui fakes data. an empty database says "no runs yet — launch
one to get started"; a timeline with no runs in the window says so; a job
with no schedules has no schedules section; sparklines and gantt
render nothing rather than placeholder marks. the assets page with nothing
registered says so; a never-built asset shows an em dash where a fingerprint
would be invented; and the sensors table only exists when sensors do.
