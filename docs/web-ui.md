# Web ui

the ui is a react bundle compiled into the binary; at deploy time the
executable is all there is. it polls the [json api](http-api.md) and works
entirely from real state. eight pages: jobs (the overview at `/`), a job page,
the asset catalog, an asset page, runs, a run page, a backfill page, and the
activity feed at `/activity`.

## Who is driving

on a deployment with no [authentication](auth.md) — which is the loopback
default — the ui is what it always was: it asks nobody who you are and offers
everything.

on one that checks, the ui asks `/api/whoami` before anything else and shows a
token prompt if it holds nothing the deployment recognizes. the token lives in
`sessionStorage`, scoped to the tab and dropped when it closes; the prompt says
what that does not protect against, and
[auth.md](auth.md#where-the-token-lives-and-what-that-does-not-protect-against)
says it at length. who you are sits at the right of the header —
`ada · admin` — with a way to forget the token where the tab is holding one.

**a control your role may not use is not rendered.** a viewer's job page says
`launching needs an operator` where the launch controls would be; cancel,
re-run, resume, build, backfill, pause, presets and the queue's `bump` are
absent the same way, and the palette does not offer the actions it would
refuse. a button that answers 403 teaches people that the ui lies about what
they can do.

one thing changes shape rather than disappearing: the activity feed **polls**
instead of following the live stream when the tab holds a token, because an
`EventSource` cannot carry a header and the alternative is a credential in a
url — and so in a log, and in the browser's history.

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
params/input/output types, the newest facts the op reported with `ctx.meta`
(with the same sparklines the asset panel draws), and history over the last 50
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

## The catalog

the assets page shows everything registered through `Hestan::assets`,
polling every 5s. every control on it lives in the url — `q`, `state`,
`sort`, `dir`, `closed`, `graph`, `depth`, `asset` — so a filtered, folded,
sorted view is a link somebody else can open.

### The graph

the asset dag draws in the same layout as job graphs: a fresh asset wears the
solid disc, a stale one the hollow ring, with the word under the name; source
assets (external data with a probe instead of a body) additionally carry a
muted `source` marker. clicking a node — or a table row — opens the detail
panel.

drawing every node stops working somewhere past a hundred, so there are three
ways of drawing fewer:

- **focus.** `focus` narrows to one node and its neighbourhood — deps in both
  directions, out to 1, 2 or 3 hops. capped at 40 nodes, because a source with
  sixty dependents has a neighbourhood the size of the graph; past the cap the
  caption says to fold a group instead.
- **fold.** the `fold` chips collapse a prefix group to a single node carrying
  its count, with the edges that crossed the group's boundary rewired to it
  and the ones inside it gone. the same chips fold the table's groups: one set
  of folded groups, two views of it.
- **find.** what the search box below matches is marked in the graph with a
  heavy outline and everything else recedes. a folded group is findable by
  what it swallowed, and a search nothing matches marks nothing rather than
  dimming the whole graph, which would read as a fault.

**past 60 nodes the graph opens focused rather than whole** — on the selected
asset, or on the first stale one, which is what anyone opening a graph of
three hundred assets came to look at. 60 is about where the tallest column in
a realistic graph stops fitting on a screen. `whole` is always one click away
and the choice is remembered in the url.

### The table

a search box filters by name substring as you type, and a state filter
separates four questions the engine answers with one word: `fresh`, `stale`,
`never built` — which is the same verdict as stale, and a different thing to
look at — and `failed check`, which cuts across the other three.

names carrying a `/` are grouped under the part before the first one:
`sales/orders` and `sales/returns` are one collapsible `sales` group, and the
prefix is dropped from the rows underneath since the heading already says it.
**with no separator anywhere there is no grouping**, since a common substring
is not a namespace; assets that carry none sort last under their own heading,
or the first of them reads as the last row of the group above.

the columns are state (with the reason beside it), when it was last built, the
run that built it, freshness where a policy is declared, and partition
coverage where the asset is [partitioned](assets.md#partitioned-assets) — the
last two only when something fills them. all of them sort, and clicking the
column already sorted turns it around. deps and the current fingerprint are
not columns: both live on the asset's own page, and neither was ever read
across three hundred rows.

a stale asset says why in a phrase — "dep X changed", "N deps changed",
"never built", or the key counts on a partitioned one — with the recorded ->
current short hashes on hover; the whole story is on
[the asset's page](#the-asset-page). the checks cell uses the same shapes as
everything else: a solid disc with "n passed" when every check passed, an ×
with "n failed" when any did not, and nothing at all when no check has ever
recorded anything. `source` and `auto` are tags beside the name, as `late` is.

every derived row has a `build` action; sources have none, since the endpoint
400s on them. a launched build (202) navigates straight to the new run, while
a build that finds nothing to do reports "up to date" inline. when any asset
is stale the header shows a "build stale" button that materializes every stale
asset as a single run.

a **backfills** section appears under the table once any exist: the id
(linking to [its page](#backfills)), the asset, the range, how many partitions
have been launched against the total, the status in the usual shapes, a link
to the chunk running now, and a cancel action while one is running.

asset builds are ordinary runs of the internal `assets` job, so the run
page, gantt, cancel, and re-run all apply unchanged; the `assets` job
appears on the jobs overview like any other job, and asset build runs carry
the `build` trigger on the runs page. checks are ops of that same job, so
they appear in its dag and gantt as `check:{asset}:{check}` nodes.

## The asset page

`/assets/{name}` is an asset's permanent address — deep-linkable, and what a
`Meta::asset_ref` points at. the name is a path, so `sales/orders` reads as
`/assets/sales/orders` rather than as an escape sequence. the header carries
the kind, when it was last built (or how many of its partitions are fresh),
the run that produced the current value as a link, the state glyph, and a
build button.

### Why it is stale

**this is the thing hestan can say that a clock-based orchestrator cannot.**
staleness here is not a policy verdict about elapsed time: every build records
its own content fingerprint and the fingerprints of the inputs it consumed, so
what is on the page is a chain of facts on disk.

each row names an upstream and what it did. three claims, kept apart, because
collapsing them would be the easy lie:

- **changed** — the dep's content moved. the row carries the fingerprint this
  asset consumed, the one the dep holds now, the build the second arrived in
  as a link to its run, and when. the build named is the *oldest* consecutive
  one holding that fingerprint: a rebuild that produced the same bytes is not
  when it changed.
- **is stale itself** — the dep has not been rebuilt yet, so nothing has moved
  here; rebuilding it is what would move it.
- **has never been built** — there is no fingerprint to compare against.

under a `changed` row sits the same question asked of that build: which of
*its* inputs held a different fingerprint than they had for the build before
it. that recursion goes four levels, so "customers is stale because orders
changed, in run 3f2a1b8c four hours ago, because the events source moved
under it" is one glance rather than four page loads. a fingerprint the
recorded history does not reach names no build at all rather than the nearest
plausible one.

a partitioned asset's staleness is per key, so it says how many keys were
built against inputs that have since moved and how many have never been built,
and the grid below says which.

### Freshness, lineage, and the rest

where a [freshness policy](freshness.md) is declared, the page draws how far
into its window the asset is — as a length and in words, since "fresh" with
four of six hours gone is a different fact from "fresh". `within_secs` on the
api is what makes that sayable: it cannot be derived from `late_by_secs`,
which is null exactly while the asset is inside its window.

lineage is links in both directions. downstream is the reverse edges, computed
from the deps every asset already carries rather than from an endpoint of its
own; a hub asset's sixty dependents wrap as a list with the rest behind a
count, since sixty names down the page is not lineage.

then the same body the drawer draws, described below — one implementation, so
the quick look and the permanent page can never say different things.

### Launching a backfill

on a partitioned asset the grid is the control: **drag across it** and the
cells you crossed are the range. two dates typed into boxes would be the ui
guessing at a partition scheme, and the key set is already on the screen.

under the grid, what the range covers: the first and last key, how many of
them a launch would actually build once the already-fresh ones are dropped
(`skip the ones already fresh` is the api's `only_missing`, on by default),
and **what it will cost**. the estimate is the median of what a successful
build of one of this asset's partitions has actually taken, from
`op_stats` — a failure's duration is how long it took to break, which is not
how long the work takes. with no history it says *"no build of this asset has
been timed yet, so no estimate"*: a number with nothing behind it is worse
than no number. it is quoted as work rather than as wall clock, because chunks
go out one after another.

nothing obviously wrong is a click. an empty range, a range holding no
partitions, a range that is entirely fresh already, and an asset whose
previous backfill is still running are each a **disabled button with the
reason beside it**, rather than a 400 after the click.

## Backfills

`/backfills/{id}`: the range, how many partitions it holds, the status in the
usual shapes, and cancel while it is running. the grid draws one cell per
partition in the same vocabulary as everywhere else — solid built, hatched for
one whose run failed, hollow for one not launched yet — with the built/failed/
left counts under it, and the legend appears only once there is more than one
state in the grid to tell apart.

under that, the chunks: a backfill launches its keys in chunks of the asset's
build limit, one run each, so each row is a run link, its status, the keys it
covered and when it started. which run built which key is arithmetic rather
than a stored fact — the chunk size is `launched` over the number of runs it
took to launch them. a failed chunk stops the rest going out, and the page
says so; starting the same range again picks up what is missing.

### The asset panel

clicking an asset in the catalog opens a drawer on the right, the same one job
pages use for ops (escape or × closes it), showing the same body as the asset
page. its title links through to the page. it carries the asset's kind, the op
that materializes it when that is not simply its own name (a
[multi-asset](assets.md#one-op-several-assets)), its deps as links and its
state.

a partitioned asset then gets the **partition grid**: one cell per key,
newest first, in the same shape vocabulary as everything else — solid for a
materialized key, hatched for a stale one, hollow for one never built.
hovering a cell names the key, its state, its short fingerprint and when it
was built; clicking one builds exactly that key and follows the run — except
on the asset page, where dragging across the grid picks a
[backfill range](#launching-a-backfill) instead. the grid shows the newest 120
keys and counts the rest, and it re-polls with the panel, so a backfill lands
cell by cell while you watch.

after that comes each check's latest result — status shape, the check name, a
`warn` marker when a failure there would not fail the run, its message and
its metadata — then the recent materializations: a `•` in the left gutter
for the entries whose fingerprint actually moved, the short fingerprint,
when it happened, and a link to the run that built it (or `probe` for a
source row, which no run wrote). anything a build reported with `ctx.meta`
sits under its entry, rendered by type exactly as the run page renders an
op's — deltas included, against the previous build of that same partition.
under the newest entry's numbers, each numeric key gets a
[sparkline](metadata.md#trends) of its recent builds, oldest on the left, once
there are three or more points; two points are a delta, which the row already
says, and one is the value itself.

the panel's selection lives in the url — `/assets?asset=orders` opens the
catalog with that asset's panel already open, which is what a graph click
records. what a `Meta::asset_ref` links to is the asset's own page, since that
is the permanent address.

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

an "undelivered notifications" section leads the page whenever a
[durable notification](notifications.md#durable-delivery) has not got through:
its state (`pending`, or `failed` for one hestan has given up retrying), the
run and job it is about, how many attempts it has had, and the error that
stopped the last one. clicking a row goes to the run. an alert nobody received
should be visible in the ui the alert was about, rather than in a log line
from Tuesday — so it sits above the filters and outside them, and delivered
ones never appear at all. the section is absent entirely unless a process
asked for `durable_notifications()`, which is off by default.

when the [queue](scaling.md) has anything on it, a "queued" section leads the
page: how many are waiting, and then each waiting run in the order a
dispatcher will take them — position, run, job, priority, and **what is
holding it back** in words (`2 runs tagged env:prod are already executing,
which is the limit`). a run the next pass will start says "starting now"
instead. each row has a bump button, which puts that run one above whatever is
currently at the head — at 3am what somebody wants is *this run next*, not a
number to type. it polls every 5s alongside the run list, and the section is
absent entirely when nothing is queued, which is the normal state of a
deployment with no limits declared.

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
json: numbers right-aligned and tabular in whatever unit they were reported
in (`1,240`, `1.2 GB`, `3.4s`), urls as links, a reported run id or asset
name as a link into this ui, paths monospace with the basename emphasised,
tables as small scrolling tables that say so when they were truncated at the
source, text inline, json in a muted preformatted block, and markdown
rendered — headings, bold, italic, code, fenced blocks, lists, links and
rules, with everything outside that
[documented subset](metadata.md#the-markdown-subset) left as the literal text
it was written as. the renderer parses to react elements and never to html, so
markup in a metadata value is text on the page rather than markup in the dom,
and a link is only ever made for an `http(s)` target.

every number carries what it did since the last run of that op, muted and
after the value: `1,240 +37` for a count, `1.2 GB −4%` for a size or a
duration, with the other form on the hover and `±0` for something that was
measured and did not move. the sign does the work rather than colour — the ui
is monochrome — and a key with nothing to compare against shows nothing at
all, which is a different claim from having not moved. the
[delta rule](metadata.md#deltas) is computed by the api, so the page never
fetches history to draw a row.

the gantt chart plots each op run against elapsed time from the first op's
start, with duration labels at the bar ends and a glyph in place of a bar for
ops that never started (pending or skipped). the critical path — the heaviest
chain of actual durations through the dep graph, walked back from the
last-finishing op — is drawn solid; off-path ops are muted; failed ops are
hatched; canceled ops draw dim and muted whatever their path position, with
the diamond for any that never got to start. it answers "what would I speed
up to make this run faster".

the log streams while the run is live (polling every 1.5s on a cursor,
stopping once the run is terminal). while it does, a **follow** toggle pins
the pane to the newest line, and **scrolling up releases it** — scrolling back
down does not re-arm it, because a pane that yanks you back where you were
reading is worse than one that does not follow at all. the toggle is absent on
a finished run, which has no newest line to pin to.

a **find** box searches what was printed, over both sources at once. matches
are marked where they are and the count says how many lines hold one; `only`
narrows the pane to those lines, which is a second decision, since the line
above a match is often the point. the marks are pieces of text rather than
html, like every other rendered value in this ui.

filters sit in the header, starting with the source: `events`
(hestan narrating the run), `output` ([what the ops printed](logs.md)), or
both interleaved by time, which is the default. then `all` vs `logs`
(`kind=log` only — just your `ctx.info/warn/error` lines, and only relevant
while events are shown), a level filter (all/info/warn/error) that applies to
both sources, and the op chip when one is selected in the dag.

system events print their kind (`op_retry`, `run_canceled`,
`type_check_failed`, ...) before the message; a captured line prints its op,
its attempt once there has been more than one, and its stream or level. a
line hestan wrote about the capture itself — "capture stopped: this attempt
reached its cap" — is set apart by a rule down its side, because that is
hestan speaking and not the op. a `download output` link beside the filters
fetches the whole thing as text, narrowed to the selected op if there is one.

a line off a pipe has no level, so a level filter hides it rather than
inventing one for it.

## Activity

the whole [event log](events.md) as one table at `/activity`, newest first —
every run queued, asset materialized, check failed, schedule fired or skipped,
sensor tick, backfill chunk, notification given up on and lease taken back.
this is the page that answers "what happened last night" without knowing which
run to open first.

one page of history is fetched, then the feed **follows what happens next**,
merged into the same list so there is no seam between the two. the header says
`live` or `not following`, because a feed that quietly stopped updating is
worse than one that says it has. `load older` walks back a page at a time.

the filters — `about` (run, job, asset, schedule, sensor, backfill, system),
`level`, and a text `find` over the message, kind and subject — narrow **what
has been loaded**, not the query, which is why the load-older button says so
while a filter is on. a gap the stream reports (a consumer that fell behind)
is a row of its own rather than a silence.

## Command palette

`cmd-k` / `ctrl-k` anywhere. it searches jobs (name and description), the 50
most recent runs (id, job, status, trigger), and — for an admin — schedule
actions, pause and resume for every schedule. the query is tokenized; every token must match.
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

the ones this ui adds to that list: an asset never built says so and offers
the build button rather than showing an empty history; an asset with no check
shows nothing where a table of no rows would go; a backfill estimate with no
timings behind it says there are none; a graph search that matches nothing
marks nothing; and a url naming an asset or a backfill that does not exist
says exactly that, as a missing run does.
