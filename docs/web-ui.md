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
carries an `overdue` tag; [scheduling](scheduling.md) has the exact rule. a
job or asset past its declared [freshness policy](freshness.md) carries a
`late` tag instead — a different claim, and the one that wins where both could
apply — and the statline counts everything currently late.
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

a job with stored [presets](launching.md#presets) shows a dropdown beside the
launch button. picking one fills the editor rather than launching — the point
of a stored parameter set is that it is a starting point you can still edit.
inside the editor block, a name field with **save** stores whatever is in the
editor under that name, and **delete** appears once the name matches a preset
that exists. a preset the server refuses (it would not launch) reports inline
and is not stored. under the editor, a tags line takes `env:prod, kind:smoke` —
the same `key:value` the runs page filters on — and a line that is not tags
disables the launch rather than dropping the part it could not read.

when the job declares a [params schema](launching.md#params-schemas) the block
also lists its fields under the editor — name, type, `required`, and the
description the schema carries — and calls out keys the editor holds that the
schema does not know. that is a legend, not a form builder: the json is still
what launches, and the schema never refuses anything.

the graph section draws the dag, columns laid out by longest dependency path.
clicking a node offers **launch from here** — that op and everything downstream
of it, as one run, with the op count beside it and the params from the editor —
which is the mirror of the run page's *re-run from here*. an op whose upstreams
are not in that set cannot run without them, and the server's refusal appears
beside the button rather than being second-guessed locally
([subset launches](launching.md#launching-a-subset-of-ops)). clicking a node
also opens the op inspector: deps and dependents, the retry
budget, any per-attempt `timeout`, the [concurrency pool](concepts.md#concurrency-pools)
the op draws from with that pool's process-wide limit, an `isolated` line for
an op that runs in [its own process](isolation.md) with whatever memory and
cpu limits that process carries, declared
params/input/output types, and history over the last 50
runs: average and p95 duration, failure count, a duration trend, and the most
recent error verbatim. when the op has committed watermark state via
`ctx.set_state` ([op state](state.md)), a state line shows the value as json
(truncated past ~120 characters, hover for the whole thing) and when it was
committed; ops that never committed show nothing. escape closes it. when ops
declare output types, an ops list also shows each op as
`aggregate -> demo::Summary`.

schedules appear with their expression, timezone (when not utc), the
[catch-up policy](scheduling.md#missed-fire-catch-up) when it is not the
default (hovering shows the cursor), and a countdown to the next fire, each
with a pause/resume button. the toggle flips
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
instead of a body) additionally carry a muted `source` marker. clicking a
node — or a table row — opens the detail panel below.

the table lists assets in dependency order: name, kind, deps, the current
fingerprint as a 12-character prefix (hover for the full hash; an em dash
for an asset never built), when it was last built, its state, and its
checks. a [partitioned](assets.md#partitioned-assets) asset has no single
fingerprint, so that cell reads `built/total` keys instead and the built
column is an em dash; its state says how many keys are stale and how many
are missing. a stale asset says why: "dep X changed" or "N deps changed", with
the recorded -> current short hashes per dep on hover, or "never built" when
no materialization exists. the checks cell uses the same shapes as
everything else — a solid disc with "n passed" when every check passed, an ×
with "n failed" when any did not, and nothing at all when no check has ever
recorded anything for that asset. assets declared `.auto()` carry an `auto`
tag. every
derived row has a `build` action; sources have none, since the endpoint 400s
on them. a launched build (202) navigates straight to the new run,
while a build that finds nothing to do reports "up to date" inline. when any
asset is stale the header shows a "build stale" button that materializes
every stale asset as a single run.

a **backfills** section appears under the table once any exist: the asset, the
range, how many partitions have been launched against the total, the status in
the usual shapes, a link to the chunk running now, and a cancel action while
one is running. the partition grid re-polls with the panel, so a backfill lands
cell by cell while you watch it.

asset builds are ordinary runs of the internal `assets` job, so the run
page, gantt, cancel, and re-run all apply unchanged; the `assets` job
appears on the jobs overview like any other job, and asset build runs carry
the `build` trigger on the runs page. checks are ops of that same job, so
they appear in its dag and gantt as `check:{asset}:{check}` nodes.

### The asset panel

clicking an asset opens a drawer on the right, the same one job pages use
for ops (escape or × closes it). it carries the asset's kind, the op that
materializes it when that is not simply its own name (a
[multi-asset](assets.md#one-op-several-assets)), its deps and its state.

a partitioned asset then gets the **partition grid**: one cell per key,
newest first, in the same shape vocabulary as everything else — solid for a
materialized key, hatched for a stale one, hollow for one never built.
hovering a cell names the key, its state, its short fingerprint and when it
was built; clicking one builds exactly that key and follows the run. the grid
shows the newest 120 keys and counts the rest, and it re-polls with the panel,
so a backfill lands cell by cell while you watch.

after that comes each check's latest result — status shape, the check name, a
`warn` marker when a failure there would not fail the run, its message and
its metadata — then the recent materializations: a `•` in the left gutter
for the entries whose fingerprint actually moved, the short fingerprint,
when it happened, and a link to the run that built it (or `probe` for a
source row, which no run wrote). anything a build reported with `ctx.meta`
sits under its entry, rendered by type.

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
schedule, retry, resume, build, sensor), a time window (all/1h/6h/24h), and
a quick find box matching substrings of the job name or run id (escape
clears it).
filters apply client-side to the loaded set.

one exception: the tag box is **served**. it takes `key:value` and applies on
enter or blur, refetching from `/api/runs?tag=`, because a
[tag](launching.md#run-tags) on a run the page never loaded cannot be filtered
for locally; escape clears it, and a pair the server refuses says so rather
than leaving the old list looking like an answer. each row shows its run's tags
as muted `key:value` chips beside the job name.

the table is the newest 100 runs, polled every 5s; "load more" pages
backwards through history using the oldest loaded run's `created_at` plus its
`id` as a composite cursor (the id breaks created_at ties, so simultaneous
runs never vanish between pages), 100 at a time, until a short page marks
the history exhausted. terminal runs — success, failed, or canceled — have a
re-run button that launches a fresh run with the original params and
navigates to it; failed and canceled ones carry a resume button beside it,
which continues the run instead of redoing it
([resume](concepts.md#resume)). a refusal (409 when the run turns out to
still be active or the job has left the code, 400 when the old params no
longer validate or the job's ops have changed) shows inline. each duration
cell carries a bar scaled to the longest visible run; canceled bars draw dim and muted, and canceled never appears in "running
now" — it is over, just not finished.

## Run page

the header names the job (linked), the short run id, trigger, creation time,
and duration — plus, on a resumed run, a link back to the run it continued.
next to the status sit the actions: clone at any point, cancel while the run is
queued or running, re-run once it is terminal, and resume beside re-run when
the run failed or was canceled. clone launches nothing — it opens the job's
launchpad prefilled with this run's params and tags
([cloning](launching.md#cloning-a-past-run)), because editing one field is the
point; a clone that launched straight away would be re-run, which is right
beside it. cancel posts and disables itself; cancellation is
asynchronous, so the page keeps polling until the status flips to canceled.
if the run finished in the race, the server's 409 is swallowed — the next
poll says the same thing better. canceled is terminal: polling stops, and
both re-run and resume work from a canceled run exactly as from a failed one.

a resumable run also shows what resume would do before it is clicked — "3 to
re-run · 2 reused", from `resume_preview` — or, when the resume is refused
(the job's ops changed since), the reason instead.

the dag reappears with each node showing that op's live status glyph and
label. clicking a node filters the log below to that op; on a terminal run
the selection also offers "re-run from here", with the same counts, which
re-runs that op and everything downstream whatever their last status was.
ops a subset run never contained read "not in run" and carry no glyph, which
is how a resumed run shows what it reused. an [isolated op](isolation.md)
carries an `isolated` marker on its node, and while it is running the selected
op shows the process id it is running in — which is what to reach for when the
question is what to look at in `top`. under the graph, the selected op's
output shows on one line, and whatever it reported with `ctx.meta`
([metadata](metadata.md)) below it — rendered by type rather than as raw
json: numbers right-aligned and tabular, urls as links, text inline,
markdown and json in a muted preformatted block. markdown is shown as
source; hestan carries no markdown parser.

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
