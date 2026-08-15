# Http api

everything lives under `/api`, speaks json, and is what the ui itself runs
on: there is no privileged path. errors are always
`{"error": "<message>"}` with an appropriate status: 400 for bad input
(malformed query parameters included), 404 for unknown names, 409 for a
request that conflicts with reality (a retry or resume of a run still active
or whose job has left the code, a resume of a run that succeeded, a cancel of
a finished run, an asset build while one is already running), 500 for storage
failures. timestamps are rfc3339 strings in utc.

nothing below has to be reached with `curl`: `hestan --server <url> <command>`
speaks this api and prints the same objects ([the command line](cli.md)). this
page is for writing something that is not hestan.

| method | path | purpose |
| --- | --- | --- |
| GET | `/api/health` | liveness, this process's instance id, what it is holding, and whether its store is taking writes |
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
| GET | `/api/events` | the whole log, newest first, filtered and cursored |
| GET | `/api/events/stream` | the whole log as server-sent events, live, from a cursor |
| GET | `/api/runs/{id}/logs` | what the run's ops printed, cursored |
| GET | `/api/runs/{id}/logs/download` | the same as `text/plain`, to grep |
| POST | `/api/runs/{id}/retry` | launch a fresh run with the same params |
| POST | `/api/runs/{id}/resume` | continue a run from where it broke |
| GET | `/api/runs/{id}/resume_preview` | what a resume would do |
| POST | `/api/runs/{id}/replay` | re-run ops on the inputs they had |
| GET | `/api/runs/{id}/replay_preview` | what a replay would do |
| GET | `/api/runs/{id}/clone` | a past run's params and tags, to launch again |
| POST | `/api/runs/{id}/cancel` | stop a queued or running run |
| GET | `/api/queue` | what is waiting, in order, and what blocks each |
| GET | `/api/rates` | every declared rate, and how many ops wait on it here |
| POST | `/api/runs/{id}/priority` | move a queued run up or down the queue |
| GET | `/api/assets` | every asset with lineage and staleness |
| POST | `/api/assets/{name}/build` | build one asset (and stale ancestors) |
| GET | `/api/assets/{name}/history` | one asset's recent materializations |
| GET | `/api/assets/{name}/metadata/{key}` | one numeric metadata key over recent builds |
| GET | `/api/assets/{name}/checks` | one asset's recent check results |
| GET | `/api/assets/{name}/partitions` | one partitioned asset's keys, newest first |
| POST | `/api/assets/build` | build everything stale as one run |
| POST | `/api/assets/{name}/backfill` | materialize a range of one asset's partitions |
| GET | `/api/backfills` | recorded backfills, newest first |
| GET | `/api/backfills/{id}` | one backfill and the runs its chunks launched |
| POST | `/api/backfills/{id}/cancel` | stop one partway |
| GET | `/api/sensors` | every sensor with cursor and last tick |
| POST | `/api/sensors/state` | pause or resume a sensor |
| GET | `/api/sensors/ticks` | sensor evaluation history |
| GET | `/api/schedules` | all schedules |
| POST | `/api/schedules/state` | pause or resume a schedule |
| GET | `/api/schedules/ticks` | fire history |
| GET | `/api/schedules/upcoming` | projected future fires |
| GET | `/api/late` | everything past its declared freshness policy |
| GET | `/api/notifications` | the durable notification queue |
| GET | `/api/whoami` | whether this deployment checks who is asking, and who you are |

## Who may call it

a deployment with no [authenticator](auth.md) configured answers everybody, and
`serve` will only bind loopback under it. one with an authenticator wants a
credential on every call but two:

```
$ curl -H 'Authorization: Bearer '"$HESTAN_TOKEN" https://hestan.internal/api/runs
```

- **401**: no credential, or one this deployment does not recognize. it says
  nothing about what was wrong with it. an `Auth::bearer` deployment sends
  `WWW-Authenticate: Bearer` with it.
- **403**: an identity that may not do this. the message says what it
  would have taken: `{"error": "this needs operator, and vic is a viewer"}`.

what each role may is the [roles table](auth.md#the-roles): every `GET` is a
viewer's, launching and cancelling and building are an operator's, and pausing,
priority and presets are an admin's. anything not in that table needs an
operator if it is not a `GET`.

`GET /api/whoami` needs nothing, because it is what the ui and `hestan doctor`
ask *before* they hold anything to present:

```json
{ "auth": true, "identity": { "name": "ada", "role": "admin" } }
```

`auth` is whether this deployment checks at all; `identity` is `null` when it
does and does not recognize you, a 200, not a 401. the ui's own files
(`/`, `/assets/…`) need no credential either, or the page that asks for one
could not load.

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
in first-use order, with the limit each carries: that limit is shared with
every other job in the process, not per job. an op's `pool` is the pool it
takes a permit from (null for most ops) and `timeout_secs` its per-attempt
time limit (null for none). `io` is the named
[io manager](io-managers.md) the op's output is persisted through, null for
the process default. `when` is the op's
[trigger rule](concepts.md#trigger-rules): `all_succeeded` (the default),
`any_failed` or `always`. `requires` lists the
[resources](concepts.md#resources) the op declared with `Op::requires`.
`isolated` says whether the op's body runs in a child process of its own
([isolation](isolation.md)), with `memory_limit_bytes` and `cpu_limit_secs`
the caps that child applies to itself. both are null unless declared, and
declaring either without `isolated` is a build error rather than a limit
nothing enforces. `mapped_over` is the dep an
[`Op::mapped`](concepts.md#dynamic-fan-out) fans out over, null for every
ordinary op. an op's `params_schema` is whatever it declared with
[`Op::params_schema`](launching.md#params-schemas), verbatim, and the job's is
every op's merged into one, both null when nothing declared one. it is a
legend for the launchpad and never a validator: what a launch is judged
against is the ops' declared params types, so a schema that disagrees with one
cannot widen what launches. `interval_secs` is the gap between the next two fires, minimized across the
job's unpaused schedules (`null` without one); `overdue` is true when the
previous scheduled fire is more than half an interval past and no successful
run has finished since it (see [scheduling](scheduling.md)). `freshness` is
the job's declared [policy](freshness.md)'s verdict (`status` is `fresh`,
`late` or `never`, and `late_by_secs` is non-null only when late), and is
`null` when nothing was declared. a job that declares one always reports `overdue`
false: the policy is the answer then. the type fields
are `std::any::type_name` strings from [typed io](typed-io.md), `null` for
untyped ops.

## Health and the queue

`GET /api/health` says who this process is. `instance` is the eight-hex-digit
id it claims runs under (the value a run row's `claimed_by` carries), and
`holding` lists the runs it is executing right now. pointed at each process in
a [split deployment](scaling.md#roles) in turn, that answers "which worker has
my run" and "which one has gone quiet".

```json
{
  "ok": true,
  "instance": "3f2a91cc",
  "holding": ["0192...", "0192..."],
  "store": {
    "writing": true,
    "dropped_writes": 0,
    "unrecorded_writes": 0,
    "given_up": []
  }
}
```

`store` is what this process has seen its run log do. **`ok` is `false`
whenever `writing` is**: a process whose store is refusing writes has also
stopped claiming runs, and a control plane that reported it healthy while run
outcomes went missing is the thing this field exists to prevent. `writing` is
about the last write attempted, so it goes back to `true` on its own as soon
as one lands; the lease loop makes one every 15 seconds.

the two counts do not go down. `dropped_writes` is best-effort writes lost:
events and captured log lines, which a run survives. `unrecorded_writes` is
the other kind: something a run *did* that could not be written down, after
retries. `given_up` lists the runs this process stopped executing for that
reason; each is waiting for its lease to lapse so a reclaimer can settle it.
see [what hestan promises about writes](concepts.md#what-hestan-promises-about-writes).

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
pass will start, and its `scope` is `global`, `job`, `tag` or `undefined`,
the last meaning no process here defines that run's job.

the walk is the dispatcher's own, dry: a run that would start counts against
the limits for everything behind it, so this is what the next pass will
actually do rather than a per-run guess that would call the whole queue
unblocked.

`POST /api/runs/{id}/priority` with `{"priority": 5}` moves a queued run.
higher goes first, ties by creation time. 404 for an unknown run, and **409
once something has claimed it**: by then the priority has been spent, and
saying so beats a 200 that changed nothing.

note that priority is a preference and not an order: the dispatcher skips a
run a limit blocks and starts the next one that fits. see
[scaling](scaling.md#priority).

`GET /api/rates` is every [rate](concepts.md#rates) this registry declares and
what each one is doing, sorted by name:

```json
{
  "rates": [
    { "name": "eia_api", "limit": 5, "per_secs": 1.0, "waiting": 3 }
  ]
}
```

`limit` and `per_secs` are what was declared; `waiting` is how many ops are
queued for a token **in this process**, which is the only place the bucket
exists. point this at each worker in turn and add them up: the far side is
seeing the sum. see [a rate is per process](scaling.md#a-rate-is-per-process).

## Resources

`GET /api/resources` lists the [resources](resources.md) this process built,
sorted by name:

```json
{ "resources": [ { "name": "api", "type": "demo::ApiClient" } ] }
```

names and declared types only, never values. a resource is usually a client
holding credentials, so there is nothing here to leak.

## Launching runs

`POST /api/jobs/{name}/runs` with body `{"params": {...}}`. the body is
optional: empty body, or a body without `params`, launches with `{}`. on
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
missing, the same refusal, from the same check, that an asset build or a
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
replacing whatever was under that name: `200 {"ok": true}`. the params run
the same check `validate_params` does *before* anything is written, so a
preset that could never launch is a 400 (`invalid params for op fetch: ...`)
and no row appears. an empty body means `{}`, which is what a launch would
use. `created_at` survives a rewrite.

`DELETE /api/jobs/{name}/presets/{preset}` returns `200 {"deleted": true}`, or
404 when there is no such preset. both take a 404 for an unknown job.

## Validating params

`POST /api/jobs/{name}/validate_params` with the same `{"params": {...}}` body
runs a launch's params check and stops there: nothing is written and no run
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
reported none. no deltas here, since what one build did against the one
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

`value` is arbitrary json: whatever the op staged. `updated_at` is the last
successful commit, which can be far older than the last run when the op
skips `set_state` on empty pulls.

## Runs

`GET /api/runs?job=&since=&before=&before_id=&tag=&limit=` returns
`{"runs": [...]}`, newest first, ordered by `created_at` then `id`, both
descending (ids are uuid v7, so the tiebreak follows creation order). `job`
filters exactly; `since` (inclusive) and `before` (exclusive) are rfc3339
bounds on `created_at`; an empty value counts as absent, a malformed one is
a 400. `limit` defaults to 50 and clamps to 1..=500, or 1..=2000 when
`since` is present (windowed fetches page through whole days). paging walks
backwards by passing the oldest loaded run's `created_at` as `before` and
its `id` as `before_id`: the composite cursor keeps runs sharing a
`created_at` from being dropped or repeated across pages. `before` alone
stays a plain exclusive timestamp compare (back-compat), and `before_id`
without `before` is ignored. `tag=key:value` keeps runs carrying that exact
[tag](launching.md#run-tags) (exact, not a prefix), split at the first colon
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
  "replay_of": null,
  "scheduled_for": "2026-08-07T12:00:00Z",
  "tags": { "kind": "backfill", "backfill": "41" },
  "priority": 0,
  "claimed_by": "3f2a91cc",
  "claimed_at": "2026-08-07T12:00:00Z",
  "lease_until": null,
  "actor": "ada"
}
```

`actor` is who asked for this run, where a person did and the deployment
checked who they were: the name of the [identity](auth.md), never a
credential. it is null on everything a schedule, a sensor, a backfill or a
freshness policy launched, and on every launch through a deployment with no
authenticator: `"trigger": "manual"` with no actor means a person asked and
nothing was checking who.

a failed run's `error` names the first op that terminally failed, as
`"op publish failed: warehouse connection reset"`, the same pair an
`on_failure` hook receives. it is null on runs that did not fail.

`status` is `queued | running | success | failed | canceled`; `trigger` is
`manual | schedule | retry | resume | replay | build | sensor`. an op run's
status is `pending | running | success | failed | skipped | canceled`.
`resumed_from` is the id of the run this one continued, null for every run
that isn't a [resume](#resume); `replay_of` is the id of the run this one
[replayed](#replay), null for every run that isn't one. they are separate
columns and a run carries at most one of them, because a resume re-runs what
did not succeed and a replay re-runs what did. `scheduled_for` is the cron
occurrence the run stands for (not the clock it started at, once a schedule
is [catching up](scheduling.md#missed-fire-catch-up) or a held fire drains),
and is null on a manual launch, a retry, a resume, a replay, a build or a
sensor fire.
`tags` is the run's [tag map](launching.md#run-tags), `{}` when it carries
none: set at launch, defaulted with `Hestan::run_tags`, and set automatically
on sensor, backfill and single-asset build runs.

the last four are the [queue's](scaling.md). `priority` is where the run sits
while it waits, higher first. `claimed_by` is the instance id of the process
executing it (**null on a queued run nobody has taken yet, which is what
being on the queue is**), with `claimed_at` beside it. `lease_until` is how
long that claim is believed for, renewed on a heartbeat while the run is going
and null once it is over. runs written before the queue existed read back as
priority 0 and unclaimed.

`GET /api/runs/{id}/clone` returns what a run was launched with, for the
launchpad to open prefilled ([cloning](launching.md#cloning-a-past-run)):

```json
{ "job": "orders_etl", "params": {"days": 1}, "tags": {"kind": "smoke"} }
```

it launches nothing. an unknown run id is a 404; a run whose job has left the
code is a `409 {"error": "job no longer defined: orders_etl"}`, the same
refusal a retry of that run gets, since a launchpad prefilled for a job that
cannot launch would be a lie.

`GET /api/runs/{id}` returns `{"run": ..., "ops": [...]}` (404 for an unknown
id), the op runs sorted by op name. a [mapped op](concepts.md#dynamic-fan-out)
appears here as its instances (`fetch_page[0]`, `fetch_page[1]`, … rows
created during the run) and never under its own name, so no extra endpoint is
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

`GET /api/runs/{id}/events?after=0` returns `{"events": [...]}`: every event
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
  "ts": "2026-08-07T12:00:02Z",
  "actor": null
}
```

`op` is null for run-level events (`run_queued`, `run_started`, ...). `kind`
and `data` are catalogued per kind in [events](events.md); `op_expanded`
(`data: {"instances": n, "over": dep}`) is how many instances a mapped op made,
and the only record it leaves when that number is zero.

### The whole log

`GET /api/events` returns `{"events": [...], "schema": 1}`: every event in the
deployment, not only the ones about runs, **newest first**. this is the "what
happened last night" query.

| parameter | | |
| --- | --- | --- |
| `kind` | one kind exactly | an unknown word matches nothing rather than 400ing: a newer writer may write kinds this build has never heard of |
| `subject_kind` | `run`, `job`, `asset`, `schedule`, `sensor`, `backfill`, `system` | same |
| `subject` | one subject | on a run event this matches the run id, which is where a run event's subject lives |
| `level` | `info`, `warn` or `error` | that level exactly; anything else is a 400 |
| `since`, `until` | rfc3339 | `since` inclusive, `until` exclusive |
| `before` | seq | exclusive: the cursor for the page below |
| `limit` | default 100 | clamped to 1..=1000 |

filters compose. an event carries `subject_kind` and `subject` beside the
`run_id` a run event has, and `data` is [documented per kind](events.md).
`schema` is the payload schema version and what it promises is written down
there.

pages go backwards: take the `seq` of the last row and pass it as `before`.

### Following it live

`GET /api/events/stream` is the same log and the same filters, as [server-sent
events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events),
plus `after=<seq>`. each message is one event with its `seq` as the SSE `id`,
so a reconnecting `EventSource` resumes exactly where it stopped; `after` and
the `Last-Event-ID` header both work, and `after` wins.

with no cursor at all it starts from *now* rather than from the beginning of
the log: opening a live feed means "from here", and the history is one call to
`GET /api/events` away.

two things about it are worth reading [events.md](events.md#following-the-log)
for, because both are the kind of thing that is silent when it goes wrong:

- `seq` is allocated on insert rather than on commit, so a naive follower would
  skip events. this one does not, and what that costs on each backend is
  written down there.
- a consumer that stops reading is **dropped** rather than buffered, and told:

```
event: dropped
data: {"count": 412, "through": 51233}
```

## Captured output

`GET /api/runs/{id}/logs?op=&after=0&limit=500` returns `{"logs": [...]}`:
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
`stream` and the target `hestan` is hestan speaking about the capture itself:
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
original params and trigger `retry`: it redoes everything, where
[resume](#resume) continues, and it is for finished runs only. 202 with the
new `run_id`; 404 when the run id is unknown; 409 (`run still active: ...`)
when the run is still queued or running, since retrying a live run would only
double it, and a manual launch is the ungated escape hatch when an
overlapping run is really wanted; 409
(`job no longer defined: ...`) when the run exists but its job is no longer
registered (a 404 would lie, the run is right there); 400 when the recorded
params no longer pass a `.params::<P>()` check the job has since grown. the
checks apply in that order.

## Resume

`POST /api/runs/{id}/resume` launches a run that continues a finished one:
the ops that did not succeed run again with their downstream, the ops that
did are seeded from their recorded outputs. the new run keeps the original
params, gets trigger `resume`, and records `resumed_from`. the semantics
(what is reused, the chain walk, the changed-graph refusal) are in
[concepts](concepts.md).

the body is optional: empty, `{}`, or `{"from": []}` all mean "from the
failure". `{"from": ["clean", "publish"]}` re-runs exactly those ops and
their transitive downstream whatever their last status was, which is how
"re-run from here" is expressed and the one form that also applies to a run
that succeeded.

202 with the new `run_id`; 404 when the run id is unknown; 409
(`run still active: ...`) when the run is still queued or running; 409
(`run did not fail: ...`) for a plain resume of a successful run (a
targeted `from` on the same run is fine); 409 (`job no longer defined: ...`)
when the run exists but its job is no longer registered (a 404 would lie,
the run is right there); 400 for a body that isn't `{"from": [...]}`-shaped,
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
here identically: a preview never promises a run the launch would reject.
ops that neither re-run nor have an output worth reusing appear in neither
list.

## Replay

`POST /api/runs/{id}/replay` launches a run that re-runs ops of a finished
one **on the inputs that run gave them**: every dep of the replayed ops is
seeded from what the original recorded, so the op reads byte for byte what it
read then. it is for the question "does this fix work on the input that broke
it". the new run keeps the original params, gets trigger `replay`, and records
`replay_of`. the original run is not written to.

the body is optional: empty, `{}`, or `{"ops": []}` all mean "every op this
run recorded as failed". `{"ops": ["clean"]}` replays exactly those ops and
nothing downstream of them, which is the difference from
[resume](#resume), and the reason both exist.

what a replay does **not** reproduce (today's code, today's resources,
today's clock, and today's answer from anything the op fetches itself) is
[replay](replay.md), and worth reading before trusting one that succeeded.

202 with the new `run_id`; 404 when the run id is unknown; 409
(`run still active: ...`) when the run is still queued or running; 409
(`job no longer defined: ...`) when the run exists but its job is no longer
registered; 400 for a body that isn't `{"ops": [...]}`-shaped, for `ops`
naming ops the job does not have or ops this run never ran, when nothing the
run recorded failed and no `ops` were named
(`nothing to replay: no op of run ... failed`), when a replayed op's dep
recorded no output to read back, and when an input cannot be read back at all
(`cannot replay load of run ...: its input extract cannot be read back: ...`)
, which is what a run whose files [retention](storage.md#retention) has taken
answers, rather than running on a hole.

`GET /api/runs/{id}/replay_preview?ops=clean` answers what that replay would
do, without launching anything:

```json
{ "ops": ["clean"], "inputs": ["fetch_orders"] }
```

both lists are in the job's topological order: `ops` is what would execute and
`inputs` is what would be seeded from the original run. `ops` is optional and
comma separated (blank means "the ops that failed"), and every refusal above
applies here identically, including the missing-input one, so a run that
cannot be replayed says so before anybody clicks.

## Cancel

`POST /api/runs/{id}/cancel` asks a queued or running run to stop. the
semantics (which ops end up `canceled`, what a blocking op has to do to stop
at all, and why some canceled op runs have no `finished_at`) are in
[concepts](concepts.md#cancellation). 202 `{"ok": true}` when the signal was
sent: the cancel is asynchronous, and the run holds a short grace period open
for its ops to land, so poll the run until its status flips. 404 for an
unknown id. 409 (`run already finished: ...`) when the run is terminal,
including a run recorded as active by a process that died, which the next
startup's sweep marks failed.

## Assets

`GET /api/assets` returns every registered asset in topological order:

```json
{ "assets": [
  { "name": "docs_dir", "kind": "source", "deps": [], "auto": false,
    "policy": null, "op": null,
    "fingerprint": "14a61f3c...", "built_at": "2026-08-08T11:01:36Z",
    "run_id": null, "stale": false, "reasons": [] },
  { "name": "doc_stats", "kind": "derived", "deps": ["docs_dir"], "auto": true,
    "policy": { "rule": "stale", "cron": null, "tz": null,
                "upstream_ready": true, "says": "when stale, once upstream is ready",
                "waiting": { "key": "2026-08-08", "for": "hourly_traffic[2026-08-08T23]",
                             "keys": 1 } },
    "op": "doc_stats", "partitions": null, "mappings": [],
    "fingerprint": "3bffef12...", "built_at": "2026-08-08T11:01:36Z",
    "run_id": "019fe109-...", "stale": true,
    "reasons": [ { "dep": "docs_dir", "partition": null,
                   "had": "14a61f3c...", "now": "9c01d2aa..." } ],
    "checks": { "passed": 1, "failed": 0, "last_run_at": "2026-08-08T11:01:36Z" },
    "freshness": { "status": "late", "late_by_secs": 1800,
                   "last_success": "2026-08-08T10:01:36Z" } }
] }
```

`op` is the op that materializes the asset: its own name, unless a
[multi-asset](assets.md#one-op-several-assets) produces it alongside others,
and null on a source, which has no op. `fingerprint`, `built_at`, and
`run_id` come from the current materialization: all null before the first one, and `run_id` is always null
on sources (probes write their rows outside any run). `reasons` carries the
staleness evidence per dep, the fingerprint consumed (`had`) against the
dep's current one (`now`); equal values mean the dep is itself stale and
this asset is stale transitively. `partition` on a reason is which key of the
dep it is about, and is null except where the dep is read through a
[mapping](assets.md#what-a-partition-reads-of-its-dep) that reads a key other
than this asset's own, the hour under a daily rollup rather than the day.
`mappings` lists those deps and how each is read
(`[{"dep": "hourly_traffic", "mapping": "covering"}]`), and is empty on every
asset whose deps are all read at the same key. `checks` counts the latest result per
[check](assets.md#asset-checks) name; both zero with a null timestamp means
nothing has ever been recorded for this asset, which reads the same whether
no check is declared or none has run yet. `freshness` is the asset's declared
[policy](freshness.md)'s verdict, in the same shape jobs report, and `null`
when nothing was declared: stale and late are separate questions and both
are answered here. the semantics are in [assets](assets.md).

`auto` says hestan rebuilds this one itself, and `policy` says on what terms:
`rule` is `stale`, `missing` or `cron`, `cron` and `tz` are set on that rule
alone, `upstream_ready` is whether the build is held until everything it reads
is there, and `says` is the whole of it in one line. both are null and false on
an asset that declared no [policy](assets.md#automation-policies). `waiting` is
what it wants and cannot have yet: the newest key in that position (null on an
unpartitioned asset), what it is waiting `for` as `dep[key]`, and how many of
its keys are waiting. null means nothing is waiting, which is also what an asset
with nothing to build reports.

`partitions` is null on an unpartitioned asset and, on a
[partitioned](assets.md#partitioned-assets) one, replaces the single
fingerprint (which is then null) with the shape of its key set:
`{"total": 220, "materialized": 190, "stale": 12, "missing": 18}`, three
disjoint states summing to `total`.

`POST /api/assets/{name}/build` builds a stale asset: its stale ancestors
plus the target as one run, 202 `{"run_id": "..."}`. a fresh target answers
200 `{"up_to_date": true}` and launches nothing. an unknown name is a 404;
a source is a 400 (`sources are probed, never built`), a request that can
never do anything, not an up-to-date one.

the body is optional and, for a partitioned asset, may name the keys to build:

```json
{ "partitions": ["2026-01-05", "2026-01-06"] }
```

those keys are built whatever staleness says. a key the asset's set does not
hold, an empty list, `partitions` on an asset that is not partitioned, and a
body that does not parse are all 400s. so is a key whose
[window](assets.md#what-a-partition-reads-of-its-dep) reaches a key its dep
does not hold, and the message says which one: a day whose hours only half
exist is a wrong number rather than a partial build. with no body (or no `partitions`) a
partitioned asset builds its default target set: missing or stale, newest
first, capped by `Partitions::build_limit`.

`GET /api/assets/{name}/partitions?limit=` returns one row per key, newest
first (default 90, clamped 1..=1000), with the total so a capped list can say
how much it left out:

```json
{ "total": 220, "shown": 90, "partitions": [
  { "key": "2026-08-09", "state": "missing",
    "fingerprint": null, "built_at": null, "run_id": null,
    "reads": [], "reasons": [], "waiting": "hourly_traffic[2026-08-09T13]" },
  { "key": "2026-08-08", "state": "stale",
    "fingerprint": "3bffef12...", "built_at": "2026-08-08T11:01:36Z",
    "run_id": "019fe109-...",
    "reads": [ { "dep": "hourly_traffic", "mapping": "covering", "count": 24,
                 "first": "2026-08-08T00", "last": "2026-08-08T23", "missing": 0 } ],
    "reasons": [ { "dep": "hourly_traffic", "partition": "2026-08-08T07",
                   "had": "aa01...", "now": "bb02..." } ],
    "waiting": null }
] }
```

`state` is `materialized`, `stale` or `missing`. `reads` is what this key
reads of each dep it [maps](assets.md#what-a-partition-reads-of-its-dep):
the keys it resolves to, and how many it wants that the dep does not hold,
which is what makes a key unbuildable rather than merely unbuilt. it is empty
where every dep is read at the same key, since the key itself already says
that. `reasons` is this key's own staleness evidence, in the shape
`GET /api/assets` uses. `waiting` is what this key's
[policy](assets.md#automation-policies) wants and cannot have yet, as
`dep[key]`, and null on every key that is not waiting and on every asset that
declared no policy. 404 for an unknown asset and 400 for one that is not
partitioned.

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
results, newest first, every check on the asset mixed together, so the first
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
the op did not. `run_id` always names the run whose build was checked: a
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
did since the previous build **of that same partition**, the same shape and
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
running**: one at a time per asset. a range that resolves to no keys comes
back `complete` with `total` 0.

`GET /api/backfills?limit=` lists them newest first (default 20, clamped
1..=200). `GET /api/backfills/{id}` returns `{"backfill": {..}, "runs": [..]}`:
the record plus the full run rows of every chunk it launched, oldest first,
which is where you go to see which chunk broke. 404 for an unknown id.

`POST /api/backfills/{id}/cancel` asks the run in flight to stop and sends no
further chunk: 200 `{"canceled": true}`, 409 for a backfill that already
finished, 404 for an unknown id.

`status` is `running`, `complete`, `failed` or `canceled`, derived from the
runs; see [backfills](assets.md#backfills).

## Sensors

`GET /api/sensors` returns every sensor, probes included (named
`probe:<asset>`) and [run-status chains](sensors.md#run-status-sensors) too
(named `run:<name>`), in registration order: user sensors first, then run
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
`{finished_at, id}`. `filter` is what a run sensor watches: the job (null for
every job) and the terminal statuses that fire it. it is null for a user
sensor or a probe, which watch whatever their closure looks at. `last_tick` is null
until the sensor has evaluated once.

`next_eval` is when the loop will evaluate it next, and it is further out than
`every_secs` while the sensor is [backing off](sensors.md#failure-backoff);
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
take because the previous evaluation was still running, one per stall, not
one per turn). `skipped` counts the requests whose
[run key](sensors.md#run-keys) was already claimed, so they were not launched
a second time: a keyed sensor in its steady state reports
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

`params` is what every fire of that schedule launches with: `{}` unless the
declaration set it with `schedule_with` / `schedule_tz_with`, and validated
against the job's ops at startup (see [scheduling](scheduling.md)). `catchup`
is `skip` (the default), `one` or `all:<limit>`, and `cursor` is the newest
occurrence the scheduler has accounted for, `null` until this process has seen
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
same-instant dedupe, or a catch-up cap, which puts its reason in `error`), or
`deferred` (a fire held for an active run, which later records a separate
`fired` tick with the same `scheduled_for`; see [scheduling](scheduling.md)). a
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

## Notifications

`GET /api/notifications?state=&limit=` lists
[durable notifications](notifications.md#durable-delivery), newest first:

```json
{ "notifications": [
  { "id": 41, "kind": "run",
    "payload": { "run_id": "0198...", "job": "orders_etl", "status": "failed",
                 "failed_op": "load", "error": "connection refused",
                 "started_at": "2026-08-08T02:00:01Z",
                 "finished_at": "2026-08-08T02:00:09Z", "duration_secs": 8.1 },
    "created_at": "2026-08-08T02:00:09Z", "attempts": 3,
    "next_attempt_at": "2026-08-08T02:04:41Z", "delivered_at": null,
    "last_error": "hook panicked: 503", "state": "pending" }
] }
```

`state` is `pending` (undelivered and due again), `failed` (given up on after
its attempts ran out: nothing will retry it) or `delivered`; anything else is
a 400. omit it for all three. `limit` defaults to 50 and is clamped to 500.
`payload` is the event exactly as the hook receives it, so an operator reading
this and a hook receiving it are looking at the same thing.

the list is empty unless a process asked for `durable_notifications()`, which
is off by default. the runs page shows the undelivered and given-up rows,
because an alert nobody received should be visible in the ui the alert was
about.

## Everything else

a GET outside `/api` serves the embedded ui: the file from the bundled
`ui/dist` if it exists, otherwise `index.html`, so client-side routes like
`/runs/0198...` deep-link correctly. an unmatched path *under* `/api` (or
`/api` itself) gets a json 404 (`{"error": "no such endpoint: /api/nope"}`)
instead of a confusing html page, and a non-GET request to a ui path gets a
json 405 rather than a 200 full of html.
