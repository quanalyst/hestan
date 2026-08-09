# Http api

everything lives under `/api`, speaks json, and is what the ui itself runs
on — there is no privileged path. errors are always
`{"error": "<message>"}` with an appropriate status: 400 for bad input
(malformed query parameters included), 404 for unknown names, 409 for a
request that conflicts with reality (a retry or resume of a run still active
or whose job has left the code, a resume of a run that succeeded, a cancel of
a finished run, an asset build while one is already running), 500 for storage
failures. timestamps are rfc3339 strings in utc.

| method | path | purpose |
| --- | --- | --- |
| GET | `/api/health` | liveness, this process's instance id, and what it is holding |
| GET | `/api/resources` | registered resources: names and types |
| GET | `/api/jobs` | all job summaries, sorted by name |
| GET | `/api/jobs/{name}` | one job summary |
| POST | `/api/jobs/{name}/runs` | launch a run |
| GET | `/api/jobs/{name}/presets` | the job's stored parameter sets |
| PUT | `/api/jobs/{name}/presets/{preset}` | store one, validated first |
| DELETE | `/api/jobs/{name}/presets/{preset}` | drop one |
| POST | `/api/jobs/{name}/validate_params` | check params without launching |
| GET | `/api/jobs/{name}/op_stats` | per-op aggregates over recent runs |
| GET | `/api/jobs/{name}/ops/{op}/metadata/{key}` | one numeric metadata key over recent runs |
| GET | `/api/jobs/{name}/state` | the job's committed op state |
| GET | `/api/runs` | run list with filters and paging |
| GET | `/api/runs/{id}` | one run plus its op runs |
| GET | `/api/runs/{id}/events` | the run's event log, cursored |
| GET | `/api/runs/{id}/logs` | what the run's ops printed, cursored |
| GET | `/api/runs/{id}/logs/download` | the same as `text/plain`, to grep |
| POST | `/api/runs/{id}/retry` | launch a fresh run with the same params |
| POST | `/api/runs/{id}/resume` | continue a run from where it broke |
| GET | `/api/runs/{id}/resume_preview` | what a resume would do |
| GET | `/api/runs/{id}/clone` | a past run's params and tags, to launch again |
| POST | `/api/runs/{id}/cancel` | stop a queued or running run |
| GET | `/api/queue` | what is waiting, in order, and what blocks each |
| POST | `/api/runs/{id}/priority` | move a queued run up or down the queue |
| GET | `/api/assets` | every asset with lineage and staleness |
| POST | `/api/assets/{name}/build` | build one asset (and stale ancestors) |
| GET | `/api/assets/{name}/history` | one asset's recent materializations |
| GET | `/api/assets/{name}/metadata/{key}` | one numeric metadata key over recent builds |
| GET | `/api/assets/{name}/checks` | one asset's recent check results |
| POST | `/api/assets/build` | build everything stale as one run |
| GET | `/api/sensors` | every sensor with cursor and last tick |
| POST | `/api/sensors/state` | pause or resume a sensor |
| GET | `/api/sensors/ticks` | sensor evaluation history |
| GET | `/api/schedules` | all schedules |
| POST | `/api/schedules/state` | pause or resume a schedule |
| GET | `/api/schedules/ticks` | fire history |
| GET | `/api/schedules/upcoming` | projected future fires |

## Job summaries

`GET /api/jobs` returns `{"jobs": [...]}`; `GET /api/jobs/{name}` returns one
summary or a 404. the shape:

```json
{
  "name": "orders_etl",
  "description": "pull orders, clean them, publish aggregates",
  "ops": [
    {
      "name": "fetch_orders",
      "deps": [],
      "when": "all_succeeded",
      "requires": [],
      "retries": 0,
      "timeout_secs": 30.0,
      "pool": "orders_api",
      "io": null,
      "isolated": false,
      "memory_limit_bytes": null,
      "cpu_limit_secs": null,
      "mapped_over": null,
      "input_type": null,
      "output_type": null,
      "params_type": "demo::FetchParams",
      "params_schema": { "type": "object",
                         "properties": { "days": {"type": "integer"} } }
    }
  ],
  "params_schema": { "type": "object",
                     "properties": { "days": {"type": "integer"} },
                     "required": ["days"] },
  "schedules": [
    { "expr": "*/2 * * * *", "tz": "UTC", "paused": false,
      "params": {}, "next_fire": "2026-08-07T12:34:00+00:00" }
  ],
  "last_run": { "...": "a run object, or null" },
  "max_parallel": 4,
  "pools": [ { "name": "orders_api", "limit": 3 } ],
  "overlap": "skip",
  "interval_secs": 120,
  "overdue": false,
  "freshness": { "status": "fresh", "late_by_secs": null,
                 "last_success": "2026-08-07T12:30:02+00:00" }
}
```

`max_parallel` caps ops of this job (null for uncapped). `pools` lists the
[concurrency pools](concepts.md#concurrency-pools) this job's ops draw from,
in first-use order, with the limit each carries — that limit is shared with
every other job in the process, not per job. an op's `pool` is the pool it
takes a permit from (null for most ops) and `timeout_secs` its per-attempt
time limit (null for none). `io` is the named
[io manager](io-managers.md) the op's output is persisted through, null for
the process default. `when` is the op's
[trigger rule](concepts.md#trigger-rules) — `all_succeeded` (the default),
`any_failed` or `always`. `requires` lists the
[resources](concepts.md#resources) the op declared with `Op::requires`.
`isolated` says whether the op's body runs in a child process of its own
([isolation](isolation.md)), with `memory_limit_bytes` and `cpu_limit_secs`
the caps that child applies to itself — both null unless declared, and
declaring either without `isolated` is a build error rather than a limit
nothing enforces. `mapped_over` is the dep an
[`Op::mapped`](concepts.md#dynamic-fan-out) fans out over, null for every
ordinary op. an op's `params_schema` is whatever it declared with
[`Op::params_schema`](launching.md#params-schemas), verbatim, and the job's is
every op's merged into one — both null when nothing declared one. it is a
legend for the launchpad and never a validator: what a launch is judged
against is the ops' declared params types, so a schema that disagrees with one
cannot widen what launches. `interval_secs` is the gap between the next two fires, minimized across the
job's unpaused schedules (`null` without one); `overdue` is true when the
previous scheduled fire is more than half an interval past and no successful
run has finished since it (see [scheduling](scheduling.md)). `freshness` is
the job's declared [policy](freshness.md)'s verdict — `status` is `fresh`,
`late` or `never`, `late_by_secs` is non-null only when late — and is `null`
when nothing was declared. a job that declares one always reports `overdue`
false: the policy is the answer then. the type fields
are `std::any::type_name` strings from [typed io](typed-io.md), `null` for
untyped ops.

## Health and the queue

`GET /api/health` says who this process is. `instance` is the eight-hex-digit
id it claims runs under — the value a run row's `claimed_by` carries — and
`holding` lists the runs it is executing right now. Pointed at each process in
a [split deployment](scaling.md#roles) in turn, that answers "which worker has
my run" and "which one has gone quiet".

```json
{"ok": true, "instance": "3f2a91cc", "holding": ["0192...", "0192..."]}
```

`GET /api/queue` is the queue itself: runs nobody has claimed, in the order a
dispatcher will take them.

```json
{
  "depth": 12,
  "queued": [
    {
      "run": {"id": "0192...", "job": "orders_etl", "priority": 5, "claimed_by": null, "...": "..."},
      "position": 1,
      "blocked_by": {"scope": "tag", "reason": "2 runs tagged env:prod are already executing, which is the limit"}
    }
  ],
  "limits": {
    "global": 8,
    "jobs": [{"job": "orders_etl", "limit": 2}],
    "tags": [{"key": "env", "value": "prod", "limit": 2}]
  }
}
```

`depth` counts every unclaimed queued run; `queued` is capped at 200, because
a queue ten thousand deep is a fact about the deployment rather than a list
anybody reads to the end. `blocked_by` is `null` on a run the next dispatch
pass will start, and its `scope` is `global`, `job`, `tag` or `undefined` —
the last meaning no process here defines that run's job.

The walk is the dispatcher's own, dry: a run that would start counts against
the limits for everything behind it, so this is what the next pass will
actually do rather than a per-run guess that would call the whole queue
unblocked.

`POST /api/runs/{id}/priority` with `{"priority": 5}` moves a queued run.
Higher goes first, ties by creation time. 404 for an unknown run, and **409
once something has claimed it** — by then the priority has been spent, and
saying so beats a 200 that changed nothing.

Note that priority is a preference and not an order: the dispatcher skips a
run a limit blocks and starts the next one that fits. See
[scaling](scaling.md#priority).

## Resources

`GET /api/resources` lists the [resources](resources.md) this process built,
sorted by name:

```json
{ "resources": [ { "name": "api", "type": "demo::ApiClient" } ] }
```

names and declared types only — never values. a resource is usually a client
holding credentials, so there is nothing here to leak.

## Launching runs

`POST /api/jobs/{name}/runs` with body `{"params": {...}}`. the body is
optional — empty body, or a body without `params`, launches with `{}`. on
success: `202 {"run_id": "..."}`; the run is [queued](scaling.md) and the id is
immediately queryable. 202 has always meant accepted rather than started, and
now it means it precisely: with no limits declared the run starts in the same
instant, and with limits declared it starts when there is room. a body that isn't `{"params": ...}`-shaped is a
400 (`invalid body: ...`), params rejected by an op's `.params::<P>()` are a
400 (`invalid params for op fetch: ...`) with nothing written, and an unknown
job is a 404.

`{"preset": "nightly"}` launches with a stored [preset](#presets)'s params
instead, which is an alternative to `params` rather than a base for it:
naming both is a 400 (`params and preset are alternatives; name one`) and a
name nothing is stored under is a 404, in both cases with nothing launched.

`{"tags": {"kind": "smoke"}}` [tags](launching.md#run-tags) the run: a flat
string-to-string map that lands on the run row and comes back on every run
object. `Hestan::run_tags` defaults merge underneath it, per-launch winning.

`{"priority": 5}` puts the run higher up the [queue](scaling.md#priority) than
the process default; higher goes first, ties by creation time, negatives are
legal. it combines with everything else here.

`{"ops": ["clean", "publish"]}` runs only those ops and everything downstream
of them, [seeding nothing](launching.md#launching-a-subset-of-ops). their own
upstreams must therefore be in the set, or the request is a 400 naming what is
missing — the same refusal, from the same check, that an asset build or a
resume gets. an empty list is a 400 (`no ops named`), an op the job does not
have is a 400, and leaving `ops` out is what runs the whole job. it combines
with `params`, `preset` and `tags`.

## Presets

`GET /api/jobs/{name}/presets` returns the job's stored
[parameter sets](launching.md#presets), sorted by name; 404 for an unknown
job, `{"presets": []}` when it has none:

```json
{ "presets": [
  { "job": "orders_etl", "name": "nightly", "params": {"days": 1},
    "created_at": "2026-08-07T12:00:00+00:00" }
] }
```

`PUT /api/jobs/{name}/presets/{preset}` with `{"params": {...}}` stores one,
replacing whatever was under that name — `200 {"ok": true}`. the params run
the same check `validate_params` does *before* anything is written, so a
preset that could never launch is a 400 (`invalid params for op fetch: ...`)
and no row appears. an empty body means `{}`, which is what a launch would
use. `created_at` survives a rewrite.

`DELETE /api/jobs/{name}/presets/{preset}` returns `200 {"deleted": true}`, or
404 when there is no such preset. both take a 404 for an unknown job.

## Validating params

`POST /api/jobs/{name}/validate_params` with the same `{"params": {...}}` body
runs a launch's params check and stops there — nothing is written and no run
is created. `200 {"ok": true}` when every op that declared `.params::<P>()`
accepts them, `400 {"error": "invalid params for op fetch: ..."}` when one
doesn't, `404` for an unknown job. an empty body means `{}`, which is what a
launch would use. the ui's params editor calls this on blur so a typo shows up
before the launch rather than as a failed run.

## Op stats

`GET /api/jobs/{name}/op_stats?runs=50` aggregates op runs over the job's
most recent `runs` runs (default 50, clamped to 1..=200). every declared op
appears, in declared order, even with no history:

```json
{
  "ops": [
    {
      "op": "fetch_orders",
      "runs": 42,
      "failures": 3,
      "avg_ms": 412.5,
      "p95_ms": 890.0,
      "last_error": "timeout",
      "recent": [ { "run_id": "...", "status": "success", "ms": 401.0 } ],
      "metadata": { "rows": {"count": 1240} }
    }
  ]
}
```

durations come from op runs with both timestamps; `avg_ms` is null with no
samples and `p95_ms` (nearest-rank) is null under two. `last_error` is the
newest non-null error in the window. `recent` holds at most the 20 newest
samples, newest first; `ms` is null for ops that never started (skipped).
`metadata` is the newest facts the op reported inside the window, null if it
reported none — no deltas here, since what one build did against the one
before it belongs on that run's page.

`GET /api/jobs/{name}/ops/{op}/metadata/{key}?limit=` is one numeric metadata
key of one op across that job's recent runs, oldest first:

```json
{ "job": "orders_etl", "op": "aggregate", "key": "rows",
  "points": [ {"at": "2026-08-08T10:01:36Z", "value": 1203, "run_id": "019fe0b2-…"},
              {"at": "2026-08-08T11:01:36Z", "value": 1240, "run_id": "019fe109-…"} ] }
```

`limit` (default 20, clamped to 1..=200) is how many **runs** are read, not
how many points come back: a run that did not report the key, or reported it
as something that is not a number, contributes nothing rather than a gap or a
zero. an unknown job or an unknown op is a 404; a key nobody ever reported is
an empty `points`, since "never reported" is a fact about the data and not a
bad request. `GET /api/assets/{name}/metadata/{key}` is the same over one
asset's builds and additionally takes `partition=`. see
[trends](metadata.md#trends).

## Op state

`GET /api/jobs/{name}/state` lists what the job's ops have committed via
`ctx.set_state` ([op state](state.md)), ordered by op name; 404 for an
unknown job, an empty list when nothing has ever been committed:

```json
{ "states": [
  { "op": "pull_orders", "value": 81234,
    "updated_at": "2026-08-07T12:00:03Z" }
] }
```

`value` is arbitrary json — whatever the op staged. `updated_at` is the last
successful commit, which can be far older than the last run when the op
skips `set_state` on empty pulls.

## Runs

`GET /api/runs?job=&since=&before=&before_id=&tag=&limit=` returns
`{"runs": [...]}`, newest first — ordered by `created_at` then `id`, both
descending (ids are uuid v7, so the tiebreak follows creation order). `job`
filters exactly; `since` (inclusive) and `before` (exclusive) are rfc3339
bounds on `created_at` — an empty value counts as absent, a malformed one is
a 400. `limit` defaults to 50 and clamps to 1..=500, or 1..=2000 when
`since` is present (windowed fetches page through whole days). paging walks
backwards by passing the oldest loaded run's `created_at` as `before` and
its `id` as `before_id`: the composite cursor keeps runs sharing a
`created_at` from being dropped or repeated across pages. `before` alone
stays a plain exclusive timestamp compare (back-compat), and `before_id`
without `before` is ignored. `tag=key:value` keeps runs carrying that exact
[tag](launching.md#run-tags) — exact, not a prefix — split at the first colon
so a value may hold one; anything that is not a `key:value` pair is a 400
(`invalid tag: ...`) rather than a filter that quietly does nothing. a run:

```json
{
  "id": "0198f2a4-...",
  "job": "orders_etl",
  "status": "success",
  "trigger": "schedule",
  "params": {},
  "created_at": "2026-08-07T12:00:00Z",
  "started_at": "2026-08-07T12:00:00Z",
  "finished_at": "2026-08-07T12:00:03Z",
  "error": null,
  "resumed_from": null,
  "scheduled_for": "2026-08-07T12:00:00Z",
  "tags": { "kind": "backfill", "backfill": "41" },
  "priority": 0,
  "claimed_by": "3f2a91cc",
  "claimed_at": "2026-08-07T12:00:00Z",
  "lease_until": null
}
```

a failed run's `error` names the first op that terminally failed, as
`"op publish failed: warehouse connection reset"` — the same pair an
`on_failure` hook receives. it is null on runs that did not fail.

`status` is `queued | running | success | failed | canceled`; `trigger` is
`manual | schedule | retry | resume | build | sensor`. an op run's status is
`pending | running | success | failed | skipped | canceled`. `resumed_from`
is the id of the run this one continued, null for every run that isn't a
[resume](#resume). `scheduled_for` is the cron occurrence the run stands for —
not the clock it started at, once a schedule is
[catching up](scheduling.md#missed-fire-catch-up) or a held fire drains — and
is null on a manual launch, a retry, a resume, a build or a sensor fire.
`tags` is the run's [tag map](launching.md#run-tags), `{}` when it carries
none — set at launch, defaulted with `Hestan::run_tags`, and set automatically
on sensor, backfill and single-asset build runs.

the last four are the [queue's](scaling.md). `priority` is where the run sits
while it waits, higher first. `claimed_by` is the instance id of the process
executing it — **null on a queued run nobody has taken yet, which is what
being on the queue is** — with `claimed_at` beside it. `lease_until` is how
long that claim is believed for, renewed on a heartbeat while the run is going
and null once it is over. runs written before the queue existed read back as
priority 0 and unclaimed.

`GET /api/runs/{id}/clone` returns what a run was launched with, for the
launchpad to open prefilled ([cloning](launching.md#cloning-a-past-run)):

```json
{ "job": "orders_etl", "params": {"days": 1}, "tags": {"kind": "smoke"} }
```

it launches nothing. an unknown run id is a 404; a run whose job has left the
code is a `409 {"error": "job no longer defined: orders_etl"}` — the same
refusal a retry of that run gets, since a launchpad prefilled for a job that
cannot launch would be a lie.

`GET /api/runs/{id}` returns `{"run": ..., "ops": [...]}` (404 for an unknown
id), the op runs sorted by op name. a [mapped op](concepts.md#dynamic-fan-out)
appears here as its instances — `fetch_page[0]`, `fetch_page[1]`, … rows
created during the run — and never under its own name, so no extra endpoint is
needed to see a fan-out:

```json
{
  "run_id": "0198f2a4-...",
  "op": "publish",
  "status": "success",
  "attempts": 2,
  "started_at": "2026-08-07T12:00:01Z",
  "finished_at": "2026-08-07T12:00:03Z",
  "output": { "orders": 4, "revenue": 171.65, "enriched": 6 },
  "metadata": { "rows": {"int": 1234}, "source": {"url": "https://example.test/orders"} },
  "deltas": { "rows": {"delta": 37, "delta_pct": 3.08} },
  "error": null,
  "pid": null
}
```

`metadata` is what the op reported with `ctx.meta`, one tagged value per
name, and `null` when it reported nothing, which is not the same as `{}`. the
tags are `int`, `float`, `text`, `url`, `markdown`, `json`, `table`, `bytes`,
`duration_secs` (seconds, as a float), `count`, `path`, `run` (a run id) and
`asset` (an asset name); a reader that does not know a tag should show the
value as it is rather than guess, and no tag ever changes meaning. only the
attempt that succeeded contributes: a failed attempt's metadata is discarded.
see [metadata](metadata.md).

`deltas` is what each **numeric** metadata value did since the newest earlier
run of the same op of the same job, keyed by the same names: `delta` always,
`delta_pct` only when the previous value was 100 or more in absolute value. a
key that is new, that the previous run did not report, or that was not a
number then is **absent** rather than carrying a zero, and the object is `{}`
when nothing has a delta. it is computed here rather than in the client, so
rendering a row costs no second request. see
[deltas](metadata.md#deltas).

a `table` carries its own shape, capped at 100 rows where it was built:

```json
{ "by_region": { "table": {
  "columns": [ {"name": "region", "type": "text"},
               {"name": "orders", "type": "int"} ],
  "rows": [ ["emea", 812], ["amer", 428] ],
  "truncated": false } } }
```

`pid` is the child process an [isolated op](isolation.md) is running in right
now. it is null for every op that runs in the orchestrator itself, and null
again the moment an isolated one finishes: the field says what is running
where, and a pid outliving its process would answer that wrongly.

## Events

`GET /api/runs/{id}/events?after=0` returns `{"events": [...]}` — every event
with `seq` greater than `after`, ascending. poll with the last seen `seq` to
tail a live run; an unknown run id is a 404 even when `after` skips past
everything.

```json
{
  "seq": 17,
  "run_id": "0198f2a4-...",
  "op": "publish",
  "level": "error",
  "kind": "op_retry",
  "message": "attempt 1 failed: warehouse connection reset",
  "data": { "attempt": 1 },
  "ts": "2026-08-07T12:00:02Z"
}
```

`op` is null for run-level events (`run_queued`, `run_started`, ...). `kind`
and `data` are catalogued in [concepts](concepts.md); `op_expanded`
(`data: {"instances": n, "over": dep}`) is how many instances a mapped op made,
and the only record it leaves when that number is zero.

## Captured output

`GET /api/runs/{id}/logs?op=&after=0&limit=500` returns `{"logs": [...]}` —
what the run's ops *printed*, as opposed to what hestan said about them.
cursored on `id` exactly as events are cursored on `seq`, ascending, `limit`
clamped to 1..=2000. `op` narrows to one op. an unknown run id is a 404.

```json
{
  "id": 412,
  "run_id": "0198f2a4-...",
  "op": "publish",
  "attempt": 2,
  "at": "2026-08-07T12:00:02.481Z",
  "stream": "stderr",
  "level": null,
  "target": null,
  "message": "warehouse connection reset"
}
```

exactly one half of `stream` and `level`/`target` is filled and which half
says where the line came from: `stream` is `stdout`/`stderr` for an
[isolated op](isolation.md)'s subprocess capture, and `level`/`target` belong
to a `tracing` event captured by the [`capture` layer](logs.md). a row with no
`stream` and the target `hestan` is hestan speaking about the capture itself —
the line that says a cap was reached. [logs](logs.md) is the whole story,
including the caps and what is deliberately not captured.

`GET /api/runs/{id}/logs/download?op=` is the same rows as `text/plain`, one
line per line, because at some point everyone wants to grep it:

```
2026-08-07T12:00:01.412Z publish #1 stdout connecting to the warehouse
2026-08-07T12:00:02.481Z publish #2 stderr warehouse connection reset
```

the columns are the timestamp, the op, `#attempt`, and the stream or level.
the download stops at 100,000 lines and says so on the last line if it hit
that; what is stored is capped per attempt long before then.

## Retry

`POST /api/runs/{id}/retry` launches a fresh run of the same job with the
original params and trigger `retry` — it redoes everything, where
[resume](#resume) continues, and it is for finished runs only. 202 with the
new `run_id`; 404 when the run id is unknown; 409 (`run still active: ...`)
when the run is still queued or running — retrying a live run would only
double it, and a manual launch is the ungated escape hatch when an
overlapping run is really wanted; 409
(`job no longer defined: ...`) when the run exists but its job is no longer
registered — a 404 would lie, the run is right there; 400 when the recorded
params no longer pass a `.params::<P>()` check the job has since grown. the
checks apply in that order.

## Resume

`POST /api/runs/{id}/resume` launches a run that continues a finished one:
the ops that did not succeed run again with their downstream, the ops that
did are seeded from their recorded outputs. the new run keeps the original
params, gets trigger `resume`, and records `resumed_from`. the semantics —
what is reused, the chain walk, the changed-graph refusal — are in
[concepts](concepts.md).

the body is optional: empty, `{}`, or `{"from": []}` all mean "from the
failure". `{"from": ["clean", "publish"]}` re-runs exactly those ops and
their transitive downstream whatever their last status was, which is how
"re-run from here" is expressed and the one form that also applies to a run
that succeeded.

202 with the new `run_id`; 404 when the run id is unknown; 409
(`run still active: ...`) when the run is still queued or running; 409
(`run did not fail: ...`) for a plain resume of a successful run — a
targeted `from` on the same run is fine; 409 (`job no longer defined: ...`)
when the run exists but its job is no longer registered — a 404 would lie,
the run is right there; 400 for a body that isn't `{"from": [...]}`-shaped,
for `from` naming ops the job does not have, when the ops recorded across the
resume chain are no longer exactly the job's ops, when an ancestor run has
been pruned out of the history, when nothing is left to re-run, when a
re-run op's input was never produced, and when the recorded params no longer
pass a `.params::<P>()` check the job has since grown. the checks apply in
that order.

`GET /api/runs/{id}/resume_preview?from=clean,publish` answers what that
resume would do, without launching anything:

```json
{ "reuse": ["fetch_orders"], "rerun": ["clean", "publish"] }
```

both lists are in the job's topological order. `from` is optional and comma
separated (blank means "from the failure"), and every refusal above applies
here identically — a preview never promises a run the launch would reject.
ops that neither re-run nor have an output worth reusing appear in neither
list.

## Cancel

`POST /api/runs/{id}/cancel` asks a queued or running run to stop — the
semantics (which ops end up `canceled`, what a blocking op has to do to stop
at all, and why some canceled op runs have no `finished_at`) are in
[concepts](concepts.md#cancellation). 202 `{"ok": true}` when the signal was
sent: the cancel is asynchronous, and the run holds a short grace period open
for its ops to land, so poll the run until its status flips. 404 for an
unknown id. 409 (`run already finished: ...`) when the run is terminal —
including a run recorded as active by a process that died, which the next
startup's sweep marks failed.

## Assets

`GET /api/assets` returns every registered asset in topological order:

```json
{ "assets": [
  { "name": "docs_dir", "kind": "source", "deps": [], "auto": false, "op": null,
    "fingerprint": "14a61f3c...", "built_at": "2026-08-08T11:01:36Z",
    "run_id": null, "stale": false, "reasons": [] },
  { "name": "doc_stats", "kind": "derived", "deps": ["docs_dir"], "auto": false,
    "op": "doc_stats", "partitions": null,
    "fingerprint": "3bffef12...", "built_at": "2026-08-08T11:01:36Z",
    "run_id": "019fe109-...", "stale": true,
    "reasons": [ { "dep": "docs_dir", "had": "14a61f3c...", "now": "9c01d2aa..." } ],
    "checks": { "passed": 1, "failed": 0, "last_run_at": "2026-08-08T11:01:36Z" },
    "freshness": { "status": "late", "late_by_secs": 1800,
                   "last_success": "2026-08-08T10:01:36Z" } }
] }
```

`op` is the op that materializes the asset — its own name, unless a
[multi-asset](assets.md#one-op-several-assets) produces it alongside others,
and null on a source, which has no op. `fingerprint`, `built_at`, and
`run_id` come from the current materialization: all null before the first one, and `run_id` is always null
on sources (probes write their rows outside any run). `reasons` carries the
staleness evidence per dep, the fingerprint consumed (`had`) against the
dep's current one (`now`); equal values mean the dep is itself stale and
this asset is stale transitively. `checks` counts the latest result per
[check](assets.md#asset-checks) name; both zero with a null timestamp means
nothing has ever been recorded for this asset, which reads the same whether
no check is declared or none has run yet. `freshness` is the asset's declared
[policy](freshness.md)'s verdict, in the same shape jobs report, and `null`
when nothing was declared — stale and late are separate questions and both
are answered here. the semantics are in [assets](assets.md).

`partitions` is null on an unpartitioned asset and, on a
[partitioned](assets.md#partitioned-assets) one, replaces the single
fingerprint (which is then null) with the shape of its key set:
`{"total": 220, "materialized": 190, "stale": 12, "missing": 18}` — three
disjoint states summing to `total`.

`POST /api/assets/{name}/build` builds a stale asset: its stale ancestors
plus the target as one run, 202 `{"run_id": "..."}`. a fresh target answers
200 `{"up_to_date": true}` and launches nothing. an unknown name is a 404;
a source is a 400 (`sources are probed, never built`) — a request that can
never do anything, not an up-to-date one.

the body is optional and, for a partitioned asset, may name the keys to build:

```json
{ "partitions": ["2026-01-05", "2026-01-06"] }
```

those keys are built whatever staleness says. a key the asset's set does not
hold, an empty list, `partitions` on an asset that is not partitioned, and a
body that does not parse are all 400s. with no body (or no `partitions`) a
partitioned asset builds its default target set: missing or stale, newest
first, capped by `Partitions::build_limit`.

`GET /api/assets/{name}/partitions?limit=` returns one row per key, newest
first (default 90, clamped 1..=1000), with the total so a capped list can say
how much it left out:

```json
{ "total": 220, "shown": 90, "partitions": [
  { "key": "2026-08-09", "state": "missing",
    "fingerprint": null, "built_at": null, "run_id": null },
  { "key": "2026-08-08", "state": "materialized",
    "fingerprint": "3bffef12...", "built_at": "2026-08-08T11:01:36Z",
    "run_id": "019fe109-..." }
] }
```

`state` is `materialized`, `stale` or `missing`. 404 for an unknown asset and
400 for one that is not partitioned.

while any run of the `assets` job is active, both build endpoints are a 409
(`asset build already running`): builds are serialized so overlapping runs
cannot record each other's half-written lineage ([assets](assets.md)). the
checks apply in that order: unknown, source, build-active, freshness. the
gate is on these two endpoints only, so a manual
`POST /api/jobs/assets/runs` and a retry of an earlier assets run both stay
ungated, and both rebuild every derived asset.

`POST /api/assets/build` builds everything stale in one plan and one run:
202 `{"run_ids": ["..."]}`, 200 `{"up_to_date": true}` when the whole
graph is fresh, and the same 409 while a build is active.

`GET /api/assets/{name}/checks?limit=` returns that asset's recent check
results, newest first, every check on the asset mixed together — the first
row for a name is that check's latest (same `limit` clamps, same 404):

```json
{ "checks": [
  { "id": 88, "asset": "doc_stats", "check": "no_empty_docs",
    "run_id": "019fe109-...", "status": "passed", "severity": "error",
    "message": null, "metadata": { "checked": {"int": 18} },
    "checked_at": "2026-08-08T11:01:36Z" }
] }
```

`status` is `passed | failed` and `severity` is `warn | error`. a `failed`
row with severity `warn` belongs to a run that succeeded: the check failed,
the op did not. `run_id` always names the run whose build was checked — a
check only ever runs inside one.

`GET /api/assets/{name}/history?limit=` returns that asset's
materializations, newest first (`limit` default 20, clamped to 1..=200):

```json
{ "materializations": [
  { "id": 412, "fingerprint": "9c01d2aa...", "changed": true,
    "inputs": { "docs_dir": "14a61f3c..." },
    "run_id": "019fe109-...", "built_at": "2026-08-08T11:01:36Z",
    "metadata": { "files": {"int": 18} },
    "deltas": { "files": {"delta": 2, "delta_pct": null} } },
  { "id": 407, "fingerprint": "3bffef12...", "changed": false,
    "inputs": { "docs_dir": "14a61f3c..." },
    "run_id": "019fe0b2-...", "built_at": "2026-08-08T10:01:36Z",
    "metadata": null, "deltas": {} }
] }
```

`changed` is true when the entry's fingerprint differs from the entry before
it in time, which is the difference between a rebuild and a change; the
oldest entry of all is `true`, and a page's oldest entry is compared against
the entry just off the page rather than reported as a change. `run_id` is
null on source rows, which probes write outside any run. no `value` here, as
on `GET /api/assets`: these are the facts about a build, not its payload.
`metadata` is what the op that built it reported, the same map its op run
carries ([metadata](metadata.md)), and `deltas` is what each of its numbers
did since the previous build **of that same partition** — the same shape and
the same rule as on a run's op rows. 404 for an unknown asset.

`history` and `checks` both take `partition=` to narrow to one key of a
partitioned asset; without it they interleave every key by time, and each row
carries the `partition` it belongs to (null on an unpartitioned asset).

## Backfills

`POST /api/assets/{name}/backfill` records a request to materialize a range of
one asset's partitions and launches its first chunk:

```json
{ "from": "2026-01-01", "to": "2026-01-31", "only_missing": true }
```

`only_missing` defaults to true and drops the keys that are already
materialized and fresh. the answer is 202 with the record:

```json
{ "id": 3, "asset": "daily_orders",
  "from_key": "2026-01-01", "to_key": "2026-01-31",
  "partitions": ["2026-01-04", "2026-01-05"],
  "run_ids": ["019fe109-..."], "total": 2, "launched": 2,
  "created_at": "2026-08-09T09:12:00Z", "finished_at": null,
  "status": "running" }
```

404 for an unknown asset, 400 for one that is not partitioned or a range whose
ends are not keys of it, and **409 while another backfill of that asset is
running** — one at a time per asset. a range that resolves to no keys comes
back `complete` with `total` 0.

`GET /api/backfills?limit=` lists them newest first (default 20, clamped
1..=200). `GET /api/backfills/{id}` returns `{"backfill": {..}, "runs": [..]}`
— the record plus the full run rows of every chunk it launched, oldest first,
which is where you go to see which chunk broke. 404 for an unknown id.

`POST /api/backfills/{id}/cancel` asks the run in flight to stop and sends no
further chunk: 200 `{"canceled": true}`, 409 for a backfill that already
finished, 404 for an unknown id.

`status` is `running`, `complete`, `failed` or `canceled`, derived from the
runs — see [backfills](assets.md#backfills).

## Sensors

`GET /api/sensors` returns every sensor, probes included (named
`probe:<asset>`) and [run-status chains](sensors.md#run-status-sensors) too
(named `run:<name>`), in registration order — user sensors first, then run
sensors, then probes in asset topo order:

```json
{ "sensors": [
  { "name": "marker_file", "every_secs": 5, "paused": false,
    "cursor": 1786186914014, "filter": null,
    "next_eval": "2026-08-08T11:02:11Z", "consecutive_failures": 0,
    "last_tick": { "id": 7, "sensor": "marker_file",
      "evaluated_at": "2026-08-08T11:02:06Z", "outcome": "fired",
      "launched": 1, "skipped": 0, "duration_ms": 34, "error": null } },
  { "name": "run:chain", "every_secs": 15, "paused": false,
    "cursor": { "finished_at": "2026-08-08T11:02:04Z", "id": "0198f2a4-..." },
    "filter": { "job": "orders_etl", "statuses": ["success"] },
    "next_eval": "2026-08-08T11:02:19Z", "consecutive_failures": 0,
    "last_tick": null }
] }
```

`cursor` is whatever the sensor last committed (null before the first
commit); for a run sensor it is the last terminal run it read, as
`{finished_at, id}`. `filter` is what a run sensor watches — the job (null for
every job) and the terminal statuses that fire it — and null for a user sensor
or a probe, which watch whatever their closure looks at. `last_tick` is null
until the sensor has evaluated once.

`next_eval` is when the loop will evaluate it next, and it is further out than
`every_secs` while the sensor is [backing off](sensors.md#failure-backoff) —
`consecutive_failures` is what explains the gap. both live in memory, so a
restart reports a fresh sensor with no failures behind it.

`POST /api/sensors/state` with `{"name": ..., "paused": true}` flips the
flag and returns `{"ok": true}`; an unknown name is a 404. paused sensors
are skipped without ticks and resume on their normal interval.

`GET /api/sensors/ticks?sensor=&limit=` returns evaluation history, newest
first (`limit` defaults to 20, clamps to 1..=200; empty `sensor` means
all). `outcome` is `fired` (the evaluation completed; `launched` says how
many runs it asked for, usually 0), `error` (with the message in
`error`; the cursor was not moved), or `skipped` (the turn the loop did not
take because the previous evaluation was still running — one per stall, not
one per turn). `skipped` counts the requests whose
[run key](sensors.md#run-keys) was already claimed, so they were not launched
a second time — a keyed sensor in its steady state reports
`launched: 0, skipped: n`, which is not the same fact as launching nothing.
`duration_ms` is how long the evaluation took, and is 0 on a `skipped` tick,
which records a turn that was never taken. between them, outcome, duration and
the two counts are what answer "is this sensor healthy".

## Schedules

`GET /api/schedules` returns every registered schedule, ordered by job then
expression:

```json
{ "schedules": [
  { "job": "orders_etl", "expr": "*/2 * * * *", "tz": "UTC",
    "paused": false, "params": {"region": "eu"},
    "catchup": "all:24", "cursor": "2026-08-07T12:32:00Z",
    "next_fire": "2026-08-07T12:34:00+00:00" }
] }
```

`params` is what every fire of that schedule launches with — `{}` unless the
declaration set it with `schedule_with` / `schedule_tz_with`, and validated
against the job's ops at startup (see [scheduling](scheduling.md)). `catchup`
is `skip` (the default), `one` or `all:<limit>`, and `cursor` is the newest
occurrence the scheduler has accounted for — `null` until this process has seen
the schedule once. everything strictly between the cursor and now is what
downtime swallowed, which is what the policy applies to
([catch-up](scheduling.md#missed-fire-catch-up)). the same two fields appear on
each entry of a job summary's `schedules`.

`POST /api/schedules/state` with `{"job": ..., "expr": ..., "paused": true}`
flips the flag and returns `{"ok": true}`; an unregistered `(job, expr)` pair
is a 404.

`GET /api/schedules/ticks?job=&limit=` returns fire history, newest first
(`limit` defaults to 20, clamps to 1..=200; empty `job` means all):

```json
{ "ticks": [
  { "id": 812, "job": "orders_etl", "expr": "*/2 * * * *",
    "scheduled_for": "2026-08-07T12:32:00Z", "fired_at": "2026-08-07T12:32:00Z",
    "outcome": "fired", "run_id": "0198f2a4-...", "error": null }
] }
```

`outcome` is `fired` (with the launched run's id), `error` (with the launch
failure in `error`), `skipped` (a fire dropped by the overlap policy, the
same-instant dedupe, or a catch-up cap — which puts its reason in `error`), or
`deferred` (a fire held for an active run; it later records a separate `fired`
tick with the same `scheduled_for` — see [scheduling](scheduling.md)). a
caught-up fire is an ordinary `fired` tick whose `scheduled_for` is well before
its `fired_at`, which is exactly what it is.

`GET /api/schedules/upcoming?window=86400` projects fires for every unpaused
schedule within the next `window` seconds (default one day, clamped to
60..=604800), at most 100 per schedule:

```json
{ "upcoming": [
  { "job": "orders_etl", "expr": "*/2 * * * *",
    "times": ["2026-08-07T12:34:00+00:00", "2026-08-07T12:36:00+00:00"] }
] }
```

## Late

`GET /api/late` returns everything a declared [freshness
policy](freshness.md) currently calls late, jobs first and then assets, each
group by name:

```json
{ "late": [
  { "kind": "job", "name": "orders_etl", "late_by_secs": 1800,
    "last_success": "2026-08-07T11:00:04Z" },
  { "kind": "asset", "name": "report", "late_by_secs": 7200,
    "last_success": "2026-08-07T09:31:00Z" }
] }
```

this is the same shape an `on_late` hook receives, computed the same way at
the same moment, so the alert and the list cannot disagree. something that has
never succeeded is `never`, not late, and does not appear here. an empty
`late` is the normal answer.

## Everything else

a GET outside `/api` serves the embedded ui: the file from the bundled
`ui/dist` if it exists, otherwise `index.html`, so client-side routes like
`/runs/0198...` deep-link correctly. an unmatched path *under* `/api` — or
`/api` itself — gets a json 404 (`{"error": "no such endpoint: /api/nope"}`)
instead of a confusing html page, and a non-GET request to a ui path gets a
json 405 rather than a 200 full of html.
