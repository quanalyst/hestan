# Launching

a launch is a job name and a json params value, and for a long time the ui
offered exactly that: a textarea. this page covers what the launchpad grew
around it — stored parameter sets, a schema the editor can read, tags on the
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
that deleting the declaration leaves the stored preset behind — presets are
runtime data and nothing sweeps them, unlike schedules, which mirror the code
exactly. delete it from the ui, or with `Store::delete_preset`.

declared presets are **validated at build**, exactly as a schedule's params
are: a preset that no op would accept is `Error::InvalidParams` at startup
rather than a 400 the night you reach for it. presets written through the api
are validated before they are stored, for the same reason — a preset that
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
`DELETE /api/jobs/{name}/presets/{preset}` — see
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
therefore **cannot admit anything `P` refuses** — it can only describe it
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
schema does not know are called out under it (`not in the schema: reigon`) —
pointed at, never refused, since the schema does not decide what launches.

it is deliberately a legend and not a form builder. json is what the api takes
and what a preset stores, so the editor stays the thing you edit.
