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
