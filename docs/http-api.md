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
| GET | `/api/health` | liveness; `{"ok": true}` |
| GET | `/api/resources` | registered resources: names and types |
| GET | `/api/jobs` | all job summaries, sorted by name |
| GET | `/api/jobs/{name}` | one job summary |
| POST | `/api/jobs/{name}/runs` | launch a run |
| POST | `/api/jobs/{name}/validate_params` | check params without launching |
| GET | `/api/jobs/{name}/op_stats` | per-op aggregates over recent runs |
| GET | `/api/jobs/{name}/state` | the job's committed op state |
| GET | `/api/runs` | run list with filters and paging |
| GET | `/api/runs/{id}` | one run plus its op runs |
| GET | `/api/runs/{id}/events` | the run's event log, cursored |
| POST | `/api/runs/{id}/retry` | launch a fresh run with the same params |
| POST | `/api/runs/{id}/resume` | continue a run from where it broke |
| GET | `/api/runs/{id}/resume_preview` | what a resume would do |
| POST | `/api/runs/{id}/cancel` | stop a queued or running run |
| GET | `/api/assets` | every asset with lineage and staleness |
| POST | `/api/assets/{name}/build` | build one asset (and stale ancestors) |
| GET | `/api/assets/{name}/history` | one asset's recent materializations |
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
      "mapped_over": null,
      "input_type": null,
      "output_type": null,
      "params_type": "demo::FetchParams"
    }
  ],
  "schedules": [
    { "expr": "*/2 * * * *", "tz": "UTC", "paused": false,
      "params": {}, "next_fire": "2026-08-07T12:34:00+00:00" }
  ],
  "last_run": { "...": "a run object, or null" },
  "max_parallel": 4,
  "pools": [ { "name": "orders_api", "limit": 3 } ],
  "overlap": "skip",
  "interval_secs": 120,
  "overdue": false
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
`mapped_over` is the dep an
[`Op::mapped`](concepts.md#dynamic-fan-out) fans out over, null for every
ordinary op. `interval_secs` is the gap between the next two fires, minimized across the
job's unpaused schedules (`null` without one); `overdue` is true when the
previous scheduled fire is more than half an interval past and no successful
run has finished since it (see [scheduling](scheduling.md)). the type fields
are `std::any::type_name` strings from [typed io](typed-io.md), `null` for
untyped ops.

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
success: `202 {"run_id": "..."}`; the run executes in the background and the
id is immediately queryable. a body that isn't `{"params": ...}`-shaped is a
400 (`invalid body: ...`), params rejected by an op's `.params::<P>()` are a
400 (`invalid params for op fetch: ...`) with nothing written, and an unknown
job is a 404.

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
      "recent": [ { "run_id": "...", "status": "success", "ms": 401.0 } ]
    }
  ]
}
```

durations come from op runs with both timestamps; `avg_ms` is null with no
samples and `p95_ms` (nearest-rank) is null under two. `last_error` is the
newest non-null error in the window. `recent` holds at most the 20 newest
samples, newest first; `ms` is null for ops that never started (skipped).

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

`GET /api/runs?job=&since=&before=&before_id=&limit=` returns
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
without `before` is ignored. a run:

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
  "resumed_from": null
}
```

a failed run's `error` names the first op that terminally failed, as
`"op publish failed: warehouse connection reset"` — the same pair an
`on_failure` hook receives. it is null on runs that did not fail.

`status` is `queued | running | success | failed | canceled`; `trigger` is
`manual | schedule | retry | resume | build | sensor`. an op run's status is
`pending | running | success | failed | skipped | canceled`. `resumed_from`
is the id of the run this one continued, null for every run that isn't a
[resume](#resume).

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
  "error": null
}
```

`metadata` is what the op reported with `ctx.meta`, one tagged value per
name — `int`, `float`, `text`, `url`, `markdown`, `json` — and `null` when
it reported nothing, which is not the same as `{}`. only the attempt that
succeeded contributes: a failed attempt's metadata is discarded. see
[metadata](metadata.md).

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
    "checks": { "passed": 1, "failed": 0, "last_run_at": "2026-08-08T11:01:36Z" } }
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
no check is declared or none has run yet. the semantics are in
[assets](assets.md).

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
    "metadata": { "files": {"int": 18} } },
  { "id": 407, "fingerprint": "3bffef12...", "changed": false,
    "inputs": { "docs_dir": "14a61f3c..." },
    "run_id": "019fe0b2-...", "built_at": "2026-08-08T10:01:36Z",
    "metadata": null }
] }
```

`changed` is true when the entry's fingerprint differs from the entry before
it in time, which is the difference between a rebuild and a change; the
oldest entry of all is `true`, and a page's oldest entry is compared against
the entry just off the page rather than reported as a change. `run_id` is
null on source rows, which probes write outside any run. no `value` here, as
on `GET /api/assets`: these are the facts about a build, not its payload.
`metadata` is what the op that built it reported, the same map its op run
carries ([metadata](metadata.md)). 404 for an unknown asset.

## Sensors

`GET /api/sensors` returns every sensor, probes included (named
`probe:<asset>`), in registration order — user sensors first, then probes
in asset topo order:

```json
{ "sensors": [
  { "name": "marker_file", "every_secs": 5, "paused": false,
    "cursor": 1786186914014,
    "last_tick": { "id": 7, "sensor": "marker_file",
      "evaluated_at": "2026-08-08T11:02:06Z", "outcome": "fired",
      "launched": 1, "error": null } }
] }
```

`cursor` is whatever the sensor last committed (null before the first
commit); `last_tick` is null until the sensor has evaluated once.

`POST /api/sensors/state` with `{"name": ..., "paused": true}` flips the
flag and returns `{"ok": true}`; an unknown name is a 404. paused sensors
are skipped without ticks and resume on their normal interval.

`GET /api/sensors/ticks?sensor=&limit=` returns evaluation history, newest
first (`limit` defaults to 20, clamps to 1..=200; empty `sensor` means
all). `outcome` is `fired` (the evaluation completed; `launched` says how
many runs it asked for, usually 0) or `error` (with the message in
`error`; the cursor was not moved).

## Schedules

`GET /api/schedules` returns every registered schedule, ordered by job then
expression:

```json
{ "schedules": [
  { "job": "orders_etl", "expr": "*/2 * * * *", "tz": "UTC",
    "paused": false, "params": {"region": "eu"},
    "next_fire": "2026-08-07T12:34:00+00:00" }
] }
```

`params` is what every fire of that schedule launches with — `{}` unless the
declaration set it with `schedule_with` / `schedule_tz_with`, and validated
against the job's ops at startup (see [scheduling](scheduling.md)).

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
failure in `error`), `skipped` (a fire dropped by the overlap policy or the
same-instant dedupe), or `deferred` (a queue-policy fire held for an active
run; its catch-up later records a separate `fired` tick with the same
`scheduled_for` — see [scheduling](scheduling.md)).

`GET /api/schedules/upcoming?window=86400` projects fires for every unpaused
schedule within the next `window` seconds (default one day, clamped to
60..=604800), at most 100 per schedule:

```json
{ "upcoming": [
  { "job": "orders_etl", "expr": "*/2 * * * *",
    "times": ["2026-08-07T12:34:00+00:00", "2026-08-07T12:36:00+00:00"] }
] }
```

## Everything else

a GET outside `/api` serves the embedded ui: the file from the bundled
`ui/dist` if it exists, otherwise `index.html`, so client-side routes like
`/runs/0198...` deep-link correctly. an unmatched path *under* `/api` — or
`/api` itself — gets a json 404 (`{"error": "no such endpoint: /api/nope"}`)
instead of a confusing html page, and a non-GET request to a ui path gets a
json 405 rather than a 200 full of html.
