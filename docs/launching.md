# Launching

a launch is a job name and a json params value, and for a long time the ui
offered exactly that: a textarea. this page covers what the launchpad grew
around it: stored parameter sets, a schema the editor can read, tags on the
run, and launching part of a job.

everything here is optional. a job with no presets, no schema and no tags
launches exactly as it always has.

## Presets

a preset is a named parameter set stored against one job. it is the answer to
"launch the one that works" at 2am, which is not a question a textarea answers.

declare one in code:

```rust
Hestan::new()
    .job(orders_etl())
    .preset("orders_etl", "nightly", json!({"region": "eu", "days": 1}))
    .preset("orders_etl", "backfill", json!({"region": "eu", "days": 90}))
```

or save one from the launchpad: open **params**, write the json, type a name
and hit **save**. both write the same row, and the ui lists both the same way.

a declared preset is **seeded at build with an upsert**. the code that declares
one owns its params, so a redeployed change lands on the next start; a preset
somebody saved in the ui under another name is left alone. the flip side is
that deleting the declaration leaves the stored preset behind: presets are
runtime data and nothing sweeps them, unlike schedules, which mirror the code
exactly. delete it from the ui, or with `Store::delete_preset`.

declared presets are **validated at build**, exactly as a schedule's params
are: a preset that no op would accept is `Error::InvalidParams` at startup
rather than a 400 the night you reach for it. presets written through the api
are validated before they are stored, for the same reason: a preset that
cannot launch is not worth keeping.

launching by name is an alternative to inline params, never a merge:

```
POST /api/jobs/orders_etl/runs  {"preset": "nightly"}
```

naming both `preset` and `params` is a 400. two answers to "what params" is one
too many, and which one won would only ever be learned by accident. an unknown
preset name is a 404 and launches nothing.

the endpoints are `GET /api/jobs/{name}/presets`,
`PUT /api/jobs/{name}/presets/{preset}` with `{"params": {...}}`, and
`DELETE /api/jobs/{name}/presets/{preset}`; see
[the http api](http-api.md#presets).

## Params schemas

the editor knows a params *type name* and nothing else, which helps nobody at
the moment of typing. `Op::params_schema` records a json schema beside the
`.params::<P>()` validator, and the launchpad lists the fields under the
textarea:

```rust
Op::new("fetch", body)
    .params::<Fetch>()
    .params_schema(serde_json::to_value(schemars::schema_for!(Fetch))?)
```

hestan takes **no schemars dependency**. the argument is a plain
`serde_json::Value` and hestan never asks where it came from;
`schemars::schema_for!(T)` produces exactly this value in one line, and a
hand-written object works identically.

### it is a ui aid, not a second validator

the authority is and remains the serde round-trip: every launch deserializes
the params into `P` before a run row exists. a schema that disagrees with `P`
therefore **cannot admit anything `P` refuses**: it can only describe it
wrongly, which makes a bad legend rather than a hole. params matching a lying
schema and not the type are still a 400 at the launch, at
`validate_params`, and at a preset write; params matching the type and not the
schema still launch. nothing here is ever checked against a params value.

that also means a schema without a `.params::<P>()` beside it describes params
nobody validates, which is exactly as unvalidated as it was before the schema
existed. the schema does not make it stricter.

### merging

a launch's params go to every op that runs, so the fields to list are the union
of what the job's ops describe. the merge happens once at build:

- `properties` are unioned. a name two ops declare identically is one field.
- `required` is unioned: a field one op requires is required of the launch.
- `$defs` and `definitions` are unioned too, each under its own key, so a
  `$ref` a derived schema emits still resolves in the merged result.
- **a name two ops give different shapes is a build error** naming both ops.
  picking a winner would describe a field in terms half the job disagrees
  with, and a legend nobody can trust is worse than none.

`Job::params_schema()` is the merged result and `GET /api/jobs/{name}` carries
it as `params_schema`, `null` for a job where nothing declared one. each op
object carries its own `params_schema` verbatim beside its `params_type`.

### in the ui

with a schema, the params block grows a field list: name, type, `required`,
and the description if the schema carries one. keys in the editor that the
schema does not know are called out under it (`not in the schema: reigon`),
pointed at, never refused, since the schema does not decide what launches.

it is deliberately a legend and not a form builder. json is what the api takes
and what a preset stores, so the editor stays the thing you edit.

## Secret params

`Op::secret_params(["token"])` names params that are credentials. the ops still
read them; the store writes `"[hestan:redacted]"` in their place, so they are
not on the run page, not in `GET /api/runs`, not in a preset and not in the
database. the cost is that a run launched with one cannot be replayed, resumed
or retried, because the value was never written down.
[secrets in params](secrets.md) is where that decision, its limits and what it
does not cover are set out; read it before declaring one.

## Run tags

a tag is a flat `{"k": "v"}` mark on a run. `trigger` already says *what kind
of thing* launched it (schedule, sensor, build, resume), and tags say the rest:
this is a backfill, this was a manual smoke test, this one belongs to backfill
41.

set them at launch:

```
POST /api/jobs/orders_etl/runs  {"params": {...}, "tags": {"kind": "smoke"}}
```

or process-wide, for facts about the deployment rather than the run:

```rust
Hestan::new().run_tags([("env", "prod"), ("cluster", "eu-1")])
```

defaults are **defaults**: a launch naming the same key wins, since a default
describes the deployment and the launch is closer to the truth about the run.

### automatic tags

machine-made runs are tagged with what `trigger` cannot say, and nothing more:

| run | tags | why |
| --- | --- | --- |
| sensor launch | `sensor: {name}` | `sensor` says a sensor did it, not which |
| probe auto build | `sensor: {source}` | a probe is a sensor named after its source |
| policy build | `policy: {rules}`, `asset: {name}` when it is the only one | which rule wanted it, since `build` says only that something built |
| backfill chunk | `asset: {name}`, `backfill: {id}` | a chunk you cannot trace back to its backfill is a run adrift |
| build of one asset | `asset: {name}` | `build` does not say what was asked for |

a build of *everything* stale carries no `asset` tag: there is no single asset
to name, and inventing one would be worse than the silence. nothing tags a
manual launch, a schedule fire or a retry: `trigger` already says all there is
to say about those.

### filtering

`GET /api/runs?tag=key:value` matches exactly (not a prefix, not a substring)
and composes with `job`, `since`, `before` and paging. the split is at the
*first* colon, so a value may contain one (`at:12:30` is `at` = `12:30`). a
`tag` that is not a `key:value` pair is a 400 rather than a filter quietly
doing nothing.

runs carry `tags` in their json, `{}` when untagged. the runs page has a tag
box beside the other filters (served, unlike the rest, since a tag the page
never fetched cannot be filtered for client-side), and shows each run's tags as
muted `key:value` chips. from a terminal it is the same filter:
`hestan runs --tag kind=smoke`, and `hestan run <job> --preset nightly
--tag kind=smoke` is this whole page as one line ([the command
line](cli.md)).

## Launching once

a launch is a request, and a request can be made twice. an http client whose
call timed out retries it; a ci step re-runs; a queue consumer with
at-least-once delivery hands you the same message again. none of those callers
wants two runs, and until now every one of them got two.

a **launch key** is the caller's name for one request:

```
POST /api/jobs/deploy/runs  {"params": {"env": "prod"}, "key": "ci-build-4182"}
```

```
hestan run deploy --params '{"env":"prod"}' --key ci-build-4182
```

```rust
let launch = runner.launch_once(
    "deploy", params, Trigger::Manual, "ci-build-4182", RunTags::new(), None,
)?;
```

same key, same job, same params: **one run**, and the second call is answered
with the first one's run id rather than with a conflict, because what the
caller asked for is "make sure this ran once" and it has. the api answers `202`
for the call that launched and `200 {"repeat": true}` for one that did not; the
command line prints the same id either way and says which happened;
[`Launch::repeat`] is the rust half.

### the store refuses it, not a check

the key is the primary key of a table, and the insert that takes it is in the
same transaction as the run row. so two callers arriving at the same instant on
two connections produce one run because the **database** said no to the second,
not because either of them looked first. that is the same mechanism a keyed
[sensor](sensors.md#run-keys) and a [cron
occurrence](scheduling.md#one-fire-per-occurrence) already rest on, and it is
deliberate: a read-then-write is two callers away from a race, and no amount of
locking in front of it changes that.

### the same key with a different request

is a caller bug, and hestan says so rather than handing back a run that did
something else:

```
$ hestan run deploy --key ci-build-4182 --params '{"env":"staging"}'
error: launch key ci-build-4182 already launched run 019ff1b7-8df6 of job
deploy, and this call names job deploy with different params. a key stands for
one launch: send what the first call sent, or use a key of its own
```

that is a `409` over http and exit 2 on the command line, with nothing
launched. the same key naming a **different job** is refused the same way: a
key is the caller's name for a request, so it is unique across the deployment
rather than per job, and one key meaning two things on two jobs would hand
somebody a second run without either caller seeing it.

what "the same params" means is worth one sentence: the comparison is over the
params **as stored**, so key order in an object does not matter, and two
launches differing only in a param the job declared [secret](secrets.md) are
the same launch as far as this is concerned, because the stored params are
identical. that is the price of hashing what is in the database rather than
what was passed, and it is the right way round.

### how long a key is honoured

until [retention](storage.md#retention)'s age cutoff passes it. keys ride the
same knob and the same cutoff as a sensor's run keys, on purpose: two lifetimes
for two kinds of key would be two things to reason about. **with no retention
policy configured nothing prunes either**, and a key is honoured for as long as
the database lives.

a key is also dropped with the run it names, so it can never hand back an id
nothing can be looked up under.

### what it does not cover

a key and a [subset launch](#launching-a-subset-of-ops) are alternatives:
`{"key": ..., "ops": [...]}` is a `400`. a subset launch has no keyed form, and
honouring one of the two silently would be worse than saying so.

## Launching a subset of ops

hestan has always been able to run part of a job: it is how an asset build and
a [resume](concepts.md#resume) work. this exposes it:

```
POST /api/jobs/orders_etl/runs  {"ops": ["clean", "publish"]}
```

runs exactly those ops **and everything downstream of them**, seeding nothing.
listing the downstream by hand would be tedious and easy to get wrong, so the
request names the starting points and the closure is worked out for you.

seeding nothing is what separates this from a resume. a resume has a finished
run behind it, so an op it skips still has a recorded output to hand its
dependents; a fresh subset launch has none. an upstream left out therefore has
nothing to stand in for it, and the request is a **400 naming what is
missing**:

```
{"error": "invalid job graph: job orders_etl: subset op publish depends on
           clean, which is neither in the subset nor seeded"}
```

that refusal is not a check of its own: it is `Runner::launch_subset`'s, the
same one an asset build and a resume go through, reported with the same words.
there is exactly one implementation of "can this subset run".

`{"ops": []}` is a 400 (`no ops named`) rather than a launch of everything: an
empty selection names nothing, and the way to launch the whole job is to leave
`ops` out. an op the job does not have is a 400 from the same check. `params`,
`preset` and `tags` all work alongside `ops`, and the run is an ordinary
`manual` one: its `op_runs` rows are the ops it covered, and the run page draws
the rest of the dag as `not in run`.

in the ui, selecting a node on the job page's dag offers **launch from here**
with the number of ops it covers, next to the op inspector. it is the mirror
of the run page's *re-run from here*. whether the selection is launchable is the
server's answer, and a refusal appears beside the button.

## Cloning a past run

the commonest real launch is "that run again, with one field changed". the run
page's **clone** does exactly that and nothing more: it opens the job's
launchpad prefilled with that run's params and tags, and launches nothing.
editing is the point: a clone that launched immediately would be `re-run`,
which is already there.

it works through a query parameter the job page reads,
`/jobs/{name}?from={run_id}`, and fetches the values rather than carrying them
in the url: a run's params do not belong in a query string, and a url long
enough to hold them is a url that gets truncated.

`GET /api/runs/{id}/clone` is what it fetches: `{"job", "params", "tags"}`,
404 for a run that does not exist, and **409 `job no longer defined: {job}`**
for a run whose job has left the code. that is the same refusal, in the same
words, that a retry of such a run gets; a launchpad prefilled for a job that
cannot launch would be a lie, and a 404 would blame the run, which is still
right there.

the launchpad's params block carries a tags line (`env:prod, kind:smoke`, the
same `key:value` spelling the runs page filters on), so a cloned run's tags
arrive editable rather than dropped. a line that is not tags disables the
launch instead of quietly dropping the part that could not be read.
