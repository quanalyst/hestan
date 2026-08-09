# Assets

an asset is an op with identity: alongside running, it keeps a persisted
latest value and a *fingerprint* of that value. derived assets declare
explicit lineage on other assets; source assets stand for external data (a
directory, a table, an api) and carry no fn body at all, only a cheap probe
that fingerprints it. from fingerprints, staleness is provable rather than
guessed: an asset is stale exactly when the recorded facts say its inputs
moved. builds then do the minimum, stale ancestors plus the target and
nothing else, with everything fresh seeded from stored values.

```rust
let docs_dir = Asset::source("docs_dir")
    .probe(|| async { fingerprint_of_the_directory() })
    .probe_every(Duration::from_secs(10));

let doc_stats = Asset::new("doc_stats", |ctx| async move {
    // reads the real directory; the source dep is lineage, not data
    Ok(measure_docs()?)
})
.from(&docs_dir);

let doc_totals = Asset::typed("doc_totals", |_ctx: OpCtx, input: TotalsIn| async move {
    Ok(totals_over(input.doc_stats))
})
.from(&doc_stats)
.auto();

Hestan::new()
    .assets([docs_dir, doc_stats, doc_totals])
    .serve(([127, 0, 0, 1], 4000))
    .await
```

`examples/assets.rs` is this pipeline in full, over this repo's own `docs/`
directory.

## Derived assets are ops

`Asset::new` and `Asset::typed` take exactly the fns `Op::new` and
`Op::typed` take; `.retries(n)` and `.retry_delay(d)` forward to the op.
dep values arrive like op inputs: `ctx.input("<dep asset name>")`, or one
typed struct field per dep with `Asset::typed`. `.from(&dep)` is repeatable
and is the only wiring — there is no `.after` for assets.

builds run as ordinary runs of an internal job named `"assets"`, registered
only when assets exist (a user job also named `assets` is then
`Error::DuplicateJob`). every existing feature (retries, cancellation, the
gantt, events, failure hooks) applies to asset builds untouched, and the job
appears in the jobs table like any other. one consequence: launching the
`assets` job directly (`POST /api/jobs/assets/runs`) is a full rebuild of
every derived asset, ignoring staleness; the build endpoints below are the
incremental path.

a source contributes no op to that job. its "value" is null everywhere — a
derived fn whose dep is a source reads the external data itself, and the dep
exists so staleness can flow, not to carry bytes.

## Fingerprints

when a derived asset materializes, its fingerprint is the sha256 hex of its
output's json text (`serde_json::to_string` of the value). this build does
not enable serde_json's `preserve_order`, so maps serialize with sorted keys
and the same value always hashes the same. the caveat: the hash is of the
*json text*, so anything json cannot see (float formatting quirks, a type
whose serialization changes between versions) changes the fingerprint. when
the default is wrong for you — an output embedding a timestamp, say — call
`ctx.set_fingerprint(s)` inside the fn. it overrides the content hash for
that materialization and is buffered like `set_state`: last call wins,
discarded if the attempt fails.

source fingerprints come from probes and are whatever string the probe
returns. cheap identity beats content: names, lengths and mtimes hashed
together, an etag, a `MAX(updated_at)` — anything that moves when the data
moves will do.

## Provable staleness

each materialization records, per dep, the dep's fingerprint at the moment
it was consumed (the `inputs` map). an asset is stale iff:

- it has never materialized, or
- some dep's *current* fingerprint differs from the recorded one, or
- some dep has no materialization at all (a source that has never probed
  keeps its descendants stale — give sources probes), or
- some dep is itself stale. that last one is computed in topo order, so
  staleness reaches descendants before anything rebuilds, and it is
  deliberately pessimistic: if a rebuild of the dep would come out
  fingerprint-identical, the descendant rebuilds anyway (no early cutoff).

`GET /api/assets` shows the verdict with its evidence: each stale asset
lists `{dep, had, now}` reasons — the fingerprint it consumed against the
dep's current one. equal `had`/`now` on a reason means the dep itself is
stale and the asset is stale transitively. an asset that has never
materialized is stale with an empty reasons list — nothing was recorded to
compare against.

## Memoized builds

building a target materializes exactly the stale ancestors plus the target,
as one run. fresh ancestors do not re-run: their stored values are seeded
into the run as if their ops had just produced them, and their ops are
absent from the run's op list entirely. build-all is the same idea across
the whole graph, one plan covering every stale asset and one run, so a
morning's catch-up reads as a single coherent run in the ui.

`Hestan::build_asset(name)` is the headless form, like `run_once`: it always
materializes the target itself (plus stale ancestors), so check staleness
first if you only want conditional builds. that is what the http endpoint
does.

memoization is a property of the *plan*, not of the run: retrying an assets
run (`POST /api/runs/{id}/retry`, or re-run in the ui) relaunches the whole
job with the original params, not the recorded subset — a full rebuild of
every derived asset, staleness ignored. cheap enough for small graphs; for
an expensive one, prefer the build endpoints, which re-plan from current
staleness.

## One build at a time

when an asset materializes, it records its deps' fingerprints by re-reading
the store at that moment. two overlapping builds interleaving those reads
and writes could record lineage that never happened: an asset claiming it
consumed a fingerprint its actual input value never had. so builds are
serialized. while any run of the `assets` job is active, the incremental
build endpoints answer 409 (`asset build already running`) and the probe
auto-build path skips launching, leaving the next tick to self-heal (below).

the gate covers exactly those two paths. anything that reaches the executor
by the ordinary run path launches regardless: a manual
`POST /api/jobs/assets/runs`, a retry of an earlier assets run, and
`build_asset` in a headless process. those are the documented escape
hatches, and they cost what they cost — concurrent rebuilds can interleave,
so their recorded lineage is only as coherent as the interleaving.

## Probes and auto

a probe is a generated internal sensor named `probe:<asset>`, evaluated on
the [sensor loop](sensors.md) every `probe_every` (default 60s). a changed
fingerprint rewrites the source materialization (value null, no run); then,
changed or not, every `.auto()` descendant that staleness proves stale is
gathered into one combined plan and launched as a single build run (trigger
`build`), the same one-coherent-run shape as build-all. the tick's
`launched` records that run, so 0 or 1.

launching only what staleness says is owed, on every tick, is the self-heal.
a build that failed to launch, or was skipped because an assets run was
already active, is retried on the next tick without waiting for the data to
move again — the fingerprint commits before the launch, so nothing else
would ever re-trigger it. probes are pausable and tick-logged like any
sensor.

`.auto()` marks a derived asset to rebuild whenever a probe upstream makes
it stale. auto without a probed source somewhere upstream never fires —
nothing else re-evaluates staleness spontaneously. non-auto assets just show
stale in the ui until someone builds them.

## At-least-once materialization

the materialization row is written inside the asset's op, after the fn
returns its output and immediately before the op reports success; the op
result row then commits through the executor's normal path. a crash in the
gap leaves a written materialization for an op with no recorded success, so
the next build re-runs it and rewrites the same row: materialization is
at-least-once, the same policy (and the same reasoning) as
[op state](state.md). a failure writing the row fails the attempt, which
goes through the op's ordinary retry policy.

## Materialization history

`asset_materializations` is append-only: every build adds an entry, and the
newest entry for an asset is its current state — what staleness compares
against, what a memoized build seeds, what `GET /api/assets` reports. nothing
overwrites anything.

that separates two facts the keyed table used to conflate. an asset that gets
rebuilt hourly has an entry per hour; the ones where the *fingerprint moved*
are the ones where the data actually changed. `GET /api/assets/{name}/history`
carries `changed` on each entry for exactly that: true when its fingerprint
differs from the entry before it in time, so a list of rebuilds reads as a
list of changes. the oldest entry of all counts as changed — nothing to
something — and a page's oldest entry is compared against the entry just off
the page, not reported as a change the window invented.

source assets append only when their probe sees a new fingerprint (the probe
path skips the write otherwise), so a source's history is already nothing but
changes. derived assets append on every build, and a run of identical
fingerprints is the record of work that found nothing new.

history grows without bound, so it is capped rather than left to. at startup
every asset is trimmed to its newest 200 entries; `Hestan::asset_history(n)`
sets the number. the newest entry is never trimmed whatever `n` says — it is
current state, and losing it would read as an asset that has never been
built. unlike `retention_days` this happens whether you ask or not.

that cap is the only thing that ever removes a materialization. run retention
still does not: an asset keeps its latest value and fingerprint long after
the run that built it is deleted, exactly as op state keeps a watermark, so a
materialization's `run_id` can point at a run that is gone.

## The http api

`GET /api/assets` returns every asset in topo order with its kind, deps,
auto flag, current fingerprint/built_at/run_id, and the staleness verdict
with reasons.

`POST /api/assets/{name}/build` answers 202 `{"run_id"}` for a stale target
and 200 `{"up_to_date": true}` for a fresh one; 404 for an unknown name, 400
for a source (sources are probed, never built), and 409 while an assets run
is active.

`POST /api/assets/build` builds everything stale as one run: 202
`{"run_ids": [..]}`, 200 `{"up_to_date": true}` when nothing is stale, and
the same 409 while a build is active.

`GET /api/assets/{name}/history?limit=` returns that asset's recent
materializations newest first (default 20, clamped to 1..=200), each with the
`changed` flag above and a link back to the run that built it. 404 for an
unknown name.

shapes and details in [http api](http-api.md); the `asset_materializations`
table in [storage](storage.md).
