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

## One op, several assets

some computations produce more than one thing. a query that splits into a
clean table and a rejected one, an api pull that yields two resources — you
do not want to run it twice to materialize both. `MultiAsset` is one op that
produces several assets:

```rust
let split = MultiAsset::new("split_orders", |ctx: OpCtx| async move {
    let rows = pull_orders().await?;
    Ok(json!({
        "orders_clean":    clean(&rows),
        "orders_rejected": rejected(&rows),
    }))
})
.produces(["orders_clean", "orders_rejected"])
.from(&raw_orders);

let report = Asset::new("report", |ctx| async move {
    let rows = ctx.input("orders_clean").unwrap();   // named as the asset
    Ok(summarize(rows))
})
.from_named("orders_clean");

Hestan::new().assets([raw_orders, report]).multi_assets([split])
```

the body returns a json **object** whose keys are exactly the produced names.
a key it did not return, or one nothing declared, fails the op and says which:
the alternative is materializing a `null` nobody asked for.

`MultiAsset::new` names the *op*, not an asset — nothing is ever materialized
under `split_orders`. each produced asset gets its own materialization row, its
own fingerprint (the content hash of that key's value) and its own history,
and behaves like any other asset from there: deps, staleness, checks, builds,
the api, the ui. `.from(&dep)` declares lineage once and every produced asset
gets it, since one op reads its inputs once.

the registry is asset -> op **N:1**, and the consequences are worth stating:

- **staleness is per asset**, as before. the *op* is stale when any asset it
  produces is.
- **a plan holds the op once**, however many of its outputs are stale. asking
  to build `orders_clean` and `orders_rejected` in one build is one run of one
  computation, and a build of either materializes both — there is no way to
  produce half of one op.
- **downstream depends on the asset**, not the op: `.from_named("orders_clean")`
  and `ctx.input("orders_clean")`. the wiring to the op that produced it is
  hestan's problem, so an asset moving into or out of a multi-asset does not
  change anything reading it.
- **a fresh multi-asset is seeded whole**: a build that memoizes it seeds the
  object its op returns, so whichever key a consumer reads is there.
- **checks bind to a produced asset** and are handed that key's value.

per-output overrides, for when the outputs do not all want the same rule:

```rust
ctx.set_fingerprint_of("orders_clean", etag);   // instead of the content hash
ctx.meta_of("orders_clean", "rows", 1_234);     // on that materialization
ctx.meta("pulled", 5_000);                      // on the op run: the work as a whole
```

`ctx.set_fingerprint` on a multi-asset op covers every output that did not
stage one of its own. plain `ctx.meta` describes the computation, so it lands
on the op run; for an op producing one asset it lands on the materialization
too, exactly as it always has.

names live in one namespace inside the lowered job, so a multi-asset called
after an existing asset is a build error, as are two multi-assets with one
name, one that produces nothing, and two claiming the same output.

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

## Freshness policies

staleness answers "did a dep move". it does not answer "when did this last get
built at all", which is the question that matters for anything on a clock:

```rust
Asset::new("report", build).fresh_within(Duration::from_secs(3600))
```

past that window the asset is **late**, `GET /api/assets` says so in its
`freshness` field, the ui tags it, and `Hestan::on_late` alerts on the
crossing. on a partitioned asset the policy applies per key and the asset is
late as soon as any one key is. stale and late are independent: an asset can
be fresh and stale (a dep changed a minute ago) or late and not stale (nothing
moved upstream, and nothing rebuilt it either). the whole of it is in
[freshness](freshness.md).

## Partitioned assets

an asset can be materialized once per key instead of once:

```rust
let daily_orders = Asset::new("daily_orders", |ctx: OpCtx| async move {
    let day = ctx.partition().expect("partitioned");   // "2026-01-05"
    Ok(pull_orders_for(day).await?)
})
.partitioned(Partitions::daily("2026-01-01"));
// also: Partitions::hourly("2026-01-01T00"), Partitions::keys(["emea", "amer"])
```

`daily` keys are `YYYY-MM-DD` and `hourly` keys are `YYYY-MM-DDTHH`, both utc,
running from the start to now — so the set grows with the clock.
`Partitions::keys` is a fixed set of whatever strings you like. keys may not
contain brackets, for a reason the next paragraph makes obvious.
`ctx.partition()` is `Some(key)` inside a partitioned asset and `None`
everywhere else, exactly as it has always effectively been.

everything an asset has is per key: its materialization, its fingerprint, its
history, its checks and its metadata. `GET /api/assets` reports an
unpartitioned asset exactly as before, and reports a partitioned one as
`partitions: {total, materialized, stale, missing}` — three disjoint states
summing to `total` — instead of a single fingerprint, because there isn't one.

### Building is fan-out

a build of a partitioned asset expands into one instance per target key,
named `{asset}[{key}]`, through the **same machinery a mapped op uses**. that
is the whole implementation, and it is why `max_parallel`, pools, retries,
cancellation, per-instance `op_runs` rows, the gantt and the event log all
apply to partitions without any of them knowing partitions exist.

concretely: the lowered `assets` job gains an external name
`partitions:{asset}` per partitioned asset, the asset's op fans out over it,
and the build plan seeds it with the keys that build targets. one consequence
worth knowing: launching the `assets` job directly
(`POST /api/jobs/assets/runs`) computes no plan, so that external seeds `[]`
and partitioned assets expand into nothing. a full launch is a full rebuild of
the *unpartitioned* graph; the build endpoints and backfills are how
partitions get built.

with nothing named, a build targets the keys that are missing or stale, newest
first, capped by `Partitions::build_limit` (default 31) — so an unbounded daily
range cannot start a thousand instances by accident. `Hestan::build_asset` and
`POST /api/assets/{name}/build` both work this way, which means the "a build
always rebuilds its target" rule of unpartitioned assets reads slightly
differently here: a partitioned asset with nothing stale has no keys to target
and builds nothing. name keys outright to rebuild regardless:

```
POST /api/assets/daily_orders/build  {"partitions": ["2026-01-05"]}
```

a key the asset's set does not hold is a 400, as is naming partitions on an
asset that has none.

### Identity mapping only

dependencies between partitioned assets take **the same key**, and nothing
else:

- **partitioned on partitioned** — `daily_report` reading `daily_orders` at
  its own key. the value comes from the store at `(dep, key)` rather than out
  of the run, which is what makes "the same key" mean one thing whether the
  upstream partition was rebuilt by this run or was already fresh. a build
  pulls the upstream keys its targets need along with them.
- **partitioned on unpartitioned** — fine, and the whole value arrives.
- **unpartitioned on partitioned** — **rejected at build.** reading every
  partition of something at once is an aggregation, and hestan does not define
  one yet. partition the consumer too, or aggregate inside the body from a
  source.

two partitioned assets in a dep relationship must also use the same *kind* of
key set (daily/hourly/static): "the same key" is not a thing two different
kinds can agree on, so it is a build error rather than a shape that quietly
never matches. there are no partition mapping functions and no partition sets
added at runtime in this phase.

a key the upstream's set does not contain keeps the downstream partition
stale forever — identity mapping has nothing to read there. that is the honest
reading of a range that starts later upstream than downstream.

### Backfills

a backfill is a recorded, watchable request to materialize a range of one
asset's partitions:

```
POST /api/assets/daily_orders/backfill
     {"from": "2026-01-01", "to": "2026-01-31", "only_missing": true}
```

the range resolves against the asset's key set at the moment it is made and is
then **fixed** — a daily set grows, and a backfill should build what it was
asked for rather than whatever that range means tomorrow. `only_missing`
(default true) drops the keys that are already materialized and fresh, which
is what makes re-running a backfill after a partial failure cheap. a range
that resolves to nothing is recorded `complete` on arrival rather than
refused: "there was nothing to do" is a better record than a 400.

it then launches **in chunks of `Partitions::build_limit`**, one run at a time:
the first goes out immediately, and each next one starts as the previous
finishes. that is the whole point — a 400-day range fired as a single run
would be 400 instances at somebody's api at once. each chunk is an ordinary
build run of the `assets` job, so the run page, the gantt, cancel and the
event log all work on it.

the record lives in the `backfills` table and its status derives from its runs:
`running` while a chunk is in flight or between chunks, `complete` when the
last one succeeded, `failed` when one failed (chunking stops there), and
`canceled` when one was canceled or the backfill itself was. `launched` counts
the keys handed to a run so far, against `total`.

limits: **one backfill per asset at a time** — a second is a 409 — and no
cross-asset backfills; back one asset at a time. a backfill also respects the
one-build-at-a-time gate: while any assets run is active the next chunk simply
waits for the following tick, the same self-heal the probe path uses.

### Probes mark everything stale

a probe fingerprint change marks **all** partitions of a descendant stale.
that is not a special rule; it falls out of the ordinary one. each partition
records the source's fingerprint at the moment it was built, and an
unpartitioned dep is read whole, so when the source moves every key's
recorded input disagrees at once. crude but honest — hestan cannot know which
days of your data an external change touched.

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
built. unlike a [retention policy](storage.md#retention) this happens whether
you ask or not.

that cap is the only thing that ever removes a materialization. run retention
still does not: an asset keeps its latest value and fingerprint long after
the run that built it is deleted, exactly as op state keeps a watermark, so a
materialization's `run_id` can point at a run that is gone.

## Asset checks

a check is an assertion bound to an asset, run right after it materializes
and handed the value it just produced.

```rust
let rows_present = AssetCheck::new("rows_present", "orders_clean", |_ctx, value: Value| async move {
    let n = value.get("rows").and_then(Value::as_u64).unwrap_or(0);
    if n > 0 {
        Ok(CheckResult::pass().meta("rows", n as i64))
    } else {
        Ok(CheckResult::fail("no rows"))
    }
})
.severity(Severity::Error);   // the default; Warn records and continues

Hestan::new().assets(..).check(rows_present).serve(..).await
```

the fn is handed an owned `Value` — the asset's freshly materialized output —
and returns a `CheckResult`: `pass()` or `fail(message)`, either with
`.meta(name, value)` facts attached, the same
[typed values](metadata.md) an op reports. naming an asset that is not
registered, naming a source (a check runs on what a build *produced*, and
sources are probed), or declaring the same check name twice on one asset are
all build errors.

### Checks are ops

a check lowers into an op of the same internal `assets` job, named
`check:{asset}:{check}`, depending on the asset's own op. it is not a second
execution path: retries, cancellation, the gantt, the event log, `max_parallel`
and the run status all apply to it because it is an ordinary op. it appears in
the dag, in the run's op list, and in the gantt like anything else.

that also decides what a failure costs, which is the whole of the severity
distinction:

- **`Severity::Error`** (the default) — the check op fails, so the run fails.
- **`Severity::Warn`** — the check op *succeeds* while the recorded result is
  `failed`. the run carries on and the failure is a fact in the check log
  rather than in the run status.

either way the result is recorded before the verdict is acted on, so a failing
error check leaves its message and metadata behind rather than only a failed
op.

state this one plainly, because it is the consequence people expect to go the
other way: **a failing error check does not un-materialize the asset.** the
materialization was written inside the asset's op, which succeeded; the check
hangs off that op rather than feeding it, so downstream assets still see the
value and still build. what a failing error check does is fail the run that
produced it — loudly, in the run list, through the failure hooks. if you need
bad data to not reach downstream, that belongs in the asset's own fn, where
returning an error stops everything below it.

### Checks and memoization

a check is in a build plan exactly when the asset it checks is. an asset that
was fresh and got seeded rather than rebuilt does **not** get re-checked: it
produced no new value this run, and its last recorded result still describes
the value that is still current. that follows from checks being ops in the
plan rather than a separate pass, and it means a build costs nothing for the
parts it skipped — which is the entire point of memoized builds.

the consequence to know: a check that was added, or fixed, after an asset last
built does not run until that asset builds again. `POST /api/assets/{name}/build`
always rebuilds its target, so that is the way to force one.

### Results

results land in `asset_checks`, capped per check by the same
`Hestan::asset_history(n)` that caps materializations, and never trimmed below
the latest one. `GET /api/assets/{name}/checks` lists them newest first, and
each asset in `GET /api/assets` carries
`{"passed": n, "failed": n, "last_run_at": ts}` counted from the latest result
per check name — zero and zero when nothing has ever recorded a result, which
reads the same whether no check is declared or none has run yet.

a check whose *body* returns an error (rather than a `CheckResult`) records
nothing: it produced no verdict, so the failed op is the whole of the record.

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

`GET /api/assets/{name}/checks?limit=` returns that asset's recent check
results, same clamps and same 404.

metadata an asset op reports lands on its materialization too, so history
carries what each build said — with [deltas](metadata.md#deltas) against the
build before it, and `GET /api/assets/{name}/metadata/{key}` for one numeric
key across recent builds. see [metadata](metadata.md).

shapes and details in [http api](http-api.md); the `asset_materializations`
table in [storage](storage.md).
