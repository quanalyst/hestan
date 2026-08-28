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

`.auto()` there is an [automation policy](#automation-policies): rebuild when
what it is made of moves. `examples/assets.rs` is this pipeline in full, over
this repo's own `docs/` directory.

## Derived assets are ops

`Asset::new` and `Asset::typed` take exactly the fns `Op::new` and
`Op::typed` take; `.retries(n)` and `.retry_delay(d)` forward to the op.
dep values arrive like op inputs: `ctx.input("<dep asset name>")`, or one
typed struct field per dep with `Asset::typed`. `.from(&dep)` is repeatable
and is the only wiring: there is no `.after` for assets.

builds run as ordinary runs of an internal job named `"assets"`, registered
only when assets exist (a user job also named `assets` is then
`Error::DuplicateJob`). every existing feature (retries, cancellation, the
gantt, events, failure hooks) applies to asset builds untouched, and the job
appears in the jobs table like any other. one consequence: launching the
`assets` job directly (`POST /api/jobs/assets/runs`) is a full rebuild of
every derived asset, ignoring staleness; the build endpoints below are the
incremental path.

a source contributes no op to that job. its "value" is null everywhere: a
derived fn whose dep is a source reads the external data itself, and the dep
exists so staleness can flow, not to carry bytes.

## One op, several assets

some computations produce more than one thing. a query that splits into a
clean table and a rejected one, an api pull that yields two resources. you
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

`MultiAsset::new` names the *op*, not an asset: nothing is ever materialized
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
  computation, and a build of either materializes both: there is no way to
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

ops and assets share one set of names inside the lowered job, so a multi-asset
called after an existing asset is a build error, as are two multi-assets with
one name, one that produces nothing, and two claiming the same output.

## Group, origin and namespace

three questions about one asset, and they are three questions rather than one
worded three ways. **group** is what it is labeled with on the graph.
**origin** is where its data came from. **namespace** is whose slice of the
deployment it is in.

**a group labels a picture and hestan draws it; a namespace divides the
deployment and hestan enforces it, and neither is derived from the other.**
that is the whole of the relationship: if you are dividing a picture, reach for
a group; if you are dividing a deployment, reach for a namespace.
[namespaces and owners](namespaces.md) is the page for the second one, and
everything below is the first.

the picture here is this graph. a [job declares the same
label](namespaces.md#a-job-has-one-too) and its picture is the run timeline:
one sentence and one concept rather than two, and a group of one name on both
is one label drawn in one colour.

### Group

one flat name, one per asset, declared on the asset:

```rust
let orders  = Asset::source("orders").group("warehouse");
let returns = Asset::source("returns").group("warehouse");
let daily   = Asset::new("daily_revenue", ..).from(&orders).group("finance");
```

**the resolution order is the declared group, else the part of the name before
the first `/`, else no group at all.** a graph that never calls `.group`
groups exactly as it always did.

declaring it rather than spelling it into the name is the whole point. the
name is the key in `asset_materializations`, in every recorded lineage ref and
in every op run, so moving `sales/orders` into `finance` by renaming it is not
a reorganisation: it is a new asset with no past. moving it with `.group`
leaves the name, and therefore the history, exactly where it was.

a group is flat. there is no nesting inside one, and nothing is parsed out of
the name you give it. three groups are refused at build, each naming both the
asset and the group:

- an empty or whitespace-only name, since a group with no name is no group;
- a name containing `/`, since `/` is the character a name uses to say which
  group it is in, so `a/b` reads as nesting that is not there;
- a name that is also the name of an ungrouped source, since an origin label
  is a group name falling back to a bare source name and one legend entry
  would then point at two things.

the first two come from the one function a [job's
group](namespaces.md#a-job-has-one-too) is refused by, so a name one of them
may carry is a name the other may carry. the third is about origins and is an
asset's alone.

a source's group names the **external system** the data stands for, which is
what makes `orders` and `returns` above one thing downstream rather than two.

that is also the clearest reason a group is not a tenancy boundary: `vendor` is
a feed, not a team, and two teams reading one vendor is ordinary. neither is
the fallback: an asset called `finance/orders` is in group `finance` without
anybody declaring anything, which is right for a colour and wrong for anything
that decides who may touch what. a [namespace](namespaces.md) is declared, has
no fallback, and is the thing an api filter and a token's scope read.

### Origin

the set of source groups an asset descends from, transitively, computed rather
than declared. a source with no group contributes its own name; a source's own
origin is itself.

so `daily_revenue` above descends from `warehouse`, and so does anything built
out of it, however many hops down. an asset with **no source anywhere
upstream** has an empty set, which is a real state and reads as "no source"
rather than as a blank.

it is one forward pass over the topological order the build already walks,
made once when the registry is built, so `GET /api/assets`, `hestan assets`
and `hestan doctor` all read the same answer. the set is ordered by name
everywhere it is exposed, because a set that reorders between two requests
makes a swatch flicker.

a partition [mapping](#what-a-partition-reads-of-its-dep) changes nothing
here: a mapping says which keys a read takes, and where the data came from is
the same answer at every key.

### Colour

a group and an origin each have a **hue**: an integer 0..=359 degrees around
the colour wheel, from `hestan::hue(name)`.

it is a pure function of the name and nothing else, so a group keeps its
colour across restarts, across processes, across machines, and across however
many other groups appear beside it. it is deliberately **not** an index into a
palette: an index renumbers every group after the one you added, and a graph
that repaints itself when somebody declares an asset is a graph nobody trusts
the colours of.

the number is a hue and not a colour. what lightness is legible depends on the
ground it is drawn on, so the reader picks saturation and lightness and hestan
picks the angle: the web ui does that in css per theme, and anything painting
a terminal gets the same angle to work from.

**a job's group is in the same space.** `hue` reads a name and nothing else, so
a job in group `weather` and the assets in group `weather` land on the same
angle without either end being told about the other, and a pin moves both. one
name, one colour, on the graph and on the [timeline](web-ui.md#jobs-overview).

**the limit, stated plainly**: two names can hash close enough together to be
hard to tell apart, and no pure function of a single name can prevent that,
because preventing it needs the whole set of names and a function of the whole
set is exactly the unstable thing above. so `hestan doctor` reports the pairs
it finds, naming both and how far apart they are, and `Asset::hue(n)` pins one
of the two. a hue belongs to the label rather than to one asset, so two assets
in one group may not pin two different angles, and a hue outside 0..=359 fails
the build.

**colour never means status.** the palette everywhere else in the ui is grey
and shape carries state, which is exactly what leaves colour free; the moment
a hue meant "failed" the channel would be carrying two things. and colour is
never the only carrier: every group and origin name is written on the same
screen as the hue that stands for it. `docs/web-ui.md` is where that is drawn.

### Namespace, and who owns it

```rust
Asset::source("orders")
    .group("warehouse")
    .namespace("finance")
    .owner(Owner::team("finance-data").contact("#fin-alerts"))
```

an asset that came from the warehouse, belongs to finance, and wakes
`#fin-alerts`. the namespace is what an api filter and a token's
[scope](auth.md#a-namespace-is-the-coarse-half) narrow by, and the owner is on
`GET /api/assets`, on the asset's page, on `hestan owner <name>`, and on the
[`LateEvent`](freshness.md) a declared `fresh_within` fires. a
[`MultiAsset`](#one-op-several-assets) declares both once for everything it
produces, since it produces names rather than `Asset` values.

**neither is a group, and no group is either of them.** the whole of that
decision, why they were not merged, how an owner reaches a hook and where the
line is drawn on escalation is [namespaces and owners](namespaces.md).

## Fingerprints

when a derived asset materializes, its fingerprint is the sha256 hex of its
output's json text (`serde_json::to_string` of the value). this build does
not enable serde_json's `preserve_order`, so maps serialize with sorted keys
and the same value always hashes the same. the caveat: the hash is of the
*json text*, so anything json cannot see (float formatting quirks, a type
whose serialization changes between versions) changes the fingerprint. when
the default is wrong for you (an output embedding a timestamp, say), call
`ctx.set_fingerprint(s)` inside the fn. it overrides the content hash for
that materialization and is buffered like `set_state`: last call wins,
discarded if the attempt fails.

source fingerprints come from probes and are whatever string the probe
returns. cheap identity beats content: names, lengths and mtimes hashed
together, an etag, a `MAX(updated_at)`. anything that moves when the data
moves will do.

## Provable staleness

each materialization records, per dep, the dep's fingerprint at the moment
it was consumed (the `inputs` map). an asset is stale iff:

- it has never materialized, or
- some dep's *current* fingerprint differs from the recorded one, or
- some dep has no materialization at all (a source that has never probed
  keeps its descendants stale; give sources probes), or
- some dep is itself stale. that last one is computed in topo order, so
  staleness reaches descendants before anything rebuilds, and it is
  deliberately pessimistic: if a rebuild of the dep would come out
  fingerprint-identical, the descendant rebuilds anyway (no early cutoff).

`GET /api/assets` shows the verdict with its evidence: each stale asset
lists `{dep, partition, had, now}` reasons (the fingerprint it consumed
against the dep's current one, and which key of the dep that was where a
[mapping](#what-a-partition-reads-of-its-dep) reads one other than its own).
equal `had`/`now` on a reason means the dep itself is stale and the asset is
stale transitively. an asset that has never
materialized is stale with an empty reasons list: nothing was recorded to
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

a freshness policy says when something is *late*; an
[automation policy](#automation-policies) says when hestan *rebuilds* it. one
alerts and the other acts, and an asset can carry both.

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
running from the start to now, so the set grows with the clock.
`Partitions::keys` is a fixed set of whatever strings you like. keys may not
contain brackets, for a reason the next paragraph makes obvious.
`ctx.partition()` is `Some(key)` inside a partitioned asset and `None`
everywhere else, exactly as it has always effectively been.

everything an asset has is per key: its materialization, its fingerprint, its
history, its checks and its metadata. `GET /api/assets` reports an
unpartitioned asset exactly as before, and reports a partitioned one as
`partitions: {total, materialized, stale, missing}` (three disjoint states
summing to `total`) instead of a single fingerprint, because there isn't one.

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
first, capped by `Partitions::build_limit` (default 31), so an unbounded daily
range cannot start a thousand instances by accident. `Hestan::build_asset` and
`POST /api/assets/{name}/build` both work this way, which means
`build_asset`'s "a build always rebuilds its target" rule reads slightly
differently here: a partitioned asset with nothing stale has no keys to target
and builds nothing. name keys outright to rebuild regardless:

```
POST /api/assets/daily_orders/build  {"partitions": ["2026-01-05"]}
```

a key the asset's set does not hold is a 400, as is naming partitions on an
asset that has none.

naming keys outright goes past `build_limit` deliberately, but not past
[`Hestan::max_instances`](concepts.md#the-ceiling), the ceiling on what one
run may expand to, 1000 by default. a build naming more keys than that fails at
the expansion saying so, before it writes a row; chunk it into backfills, or
raise the ceiling if the deployment means it. partitioned assets are one level
of fan-out and always have been: a partitioned asset expands over its own key
set rather than over an upstream asset, so nothing here nests.

### What a partition reads of its dep

a dep declares **which of its keys** this asset's partitions read. that is a
property of the edge and not of either asset, so it is declared where the dep
is:

```rust
let hourly_traffic = Asset::new("hourly_traffic", …)
    .partitioned(Partitions::hourly("2026-01-01T00"));

let daily_traffic = Asset::new("daily_traffic", |ctx: OpCtx| async move {
    // one entry per hour of the day this key is for
    let hours = ctx.input("hourly_traffic").cloned().unwrap_or(json!({}));
    Ok(json!({ "hits": hours.as_object().map_or(0, |h| h.len()) }))
})
.reads(&hourly_traffic, PartitionMapping::covering())
.partitioned(Partitions::daily("2026-01-01"));
```

there are four shapes, and `Asset::from` is the first of them:

| mapping | what one key reads | pairs with |
| --- | --- | --- |
| `identity` (the default, and what `from` declares) | the same key | two sets of the same kind, or an unpartitioned dep, whose whole value arrives |
| `covering` | the dep keys inside this one (a day and its 24 hours) | two time sets, the reader's keys no finer than the dep's |
| `offset(n)` | the key `n` steps along the dep's order | two sets of the same kind, with an order to step along |
| `all` | every key the dep has | anything, including an **unpartitioned** reader |

a mapping that reads one key hands the body that key's value, exactly as
identity always has. one that reads a set hands it an object keyed by
partition (`{"2026-01-05T00": …, "2026-01-05T01": …}`), so the body knows
which key each value came from.

either way the value comes from the store at `(dep, key)` rather than out of
the run, which is what makes a mapping mean one thing whether the upstream
partition was rebuilt by this run or was already fresh. a build pulls the
upstream keys its targets read along with them: materializing one daily key
materializes the hours under it that are missing or stale.

**a pairing the mapping could never resolve is a build error**, named at
registration with both partitionings in the message: a window over a static
key set (which spans no time to cover), an hourly asset trying to cover a
daily one (an hour sits inside a day, not the other way round), an offset
along a static set (declaration order is not an order to step along), any
mapping but identity on a dep with no keys at all. the alternative is a
dependency that resolves to nothing at 3am and calls itself fresh.

two boundaries, and they are deliberately different:

- an **offset off the end** of a set reads nothing. the first key has no key
  before it, and that is a fact about the edge of history rather than a broken
  dependency.
- a **window that its dep cannot fill** (a daily key whose hourly asset starts
  at 06:00 that day, so six of its hours are not keys of anything) is refused.
  a window promises its whole range, and 18 hours reported as a day is a wrong
  number rather than a missing one. naming that key at a build, or covering it
  with a backfill range, says which hour is missing; a build that names no keys
  leaves it out of its target set rather than refusing everything else with it,
  and the grid shows it as never built.

the *end* of a generated set is the one exception, and it is the ordinary case:
the hours left in today have not happened, so they are not missing but not yet
due. today's key rolls up the hours it has and goes stale as each next one
lands, which is what makes an hourly rollup an hourly rollup rather than
something that appears at midnight. an hour *before* the dep's first key is the
one that never arrives, and that is what a refusal is about.

**unpartitioned on partitioned** is the `all` mapping and only that: reading
every partition at once is an aggregation, and it has to say so. `identity`
there is still the error it was, because "the same key" needs a key.

a key the upstream's set does not contain keeps the downstream partition stale
forever under identity: there is nothing there to read. that is the honest
reading of a range that starts later upstream than downstream.

### Staleness follows the mapping

a rollup that reported fresh because it only ever checked its own key would be
worse than no mapping at all. so a build records the fingerprint of **every
upstream key it consumed**: one string per dep as ever, except for a dep read
through a mapping that names a set, which records an object of one fingerprint
per key. no new column: `inputs` is json, and both shapes live in the one it
has always had.

staleness then asks the same question of every key the mapping resolves to. a
daily rollup is stale when any hour it covers has moved, has gone missing, or
is itself stale, and the reason it reports names **that hour** rather than the
day: `{dep, partition, had, now}`, with `partition` null for the identity reads
that are every dep that declared nothing. the grid's tooltip and the asset
page's staleness chain both say `hours[2026-01-05T07]` where they used to be
able to say only `hours`.

two consequences worth knowing:

- reading `all` of a set that grows leaves the reader stale every time it
  grows. that is what "every key" means, and it is why `all` belongs on
  fixed key sets and short ones rather than on an hourly range since 2020.
- an hour rebuilt to the same bytes leaves the day fresh. staleness is
  fingerprints, not clocks, all the way through.

### The build limit counts keys, not instances

`Partitions::build_limit` (default 31) caps the keys **of the asset being
built**. under identity that was also roughly the size of the run: 31 keys of
a target pulled at most 31 keys of each upstream. under a mapping it is not.
one daily key covering 24 hourly ones is 25 op instances, so a default build of
that rollup is up to 775, and a backfill chunk (which chunks by the same
limit) is the same multiple. `all` is the extreme: one key of the reader can
pull the dep's whole set.

so on a mapped asset, `build_limit` is the number to set deliberately, and
[`Hestan::max_instances`](concepts.md#the-ceiling) (1000 by default) is the
ceiling that actually holds. a run that would expand past it fails at the
expansion saying so, before it writes a row.

### Backfills

a backfill is a recorded, watchable request to materialize a range of one
asset's partitions:

```
POST /api/assets/daily_orders/backfill
     {"from": "2026-01-01", "to": "2026-01-31", "only_missing": true}
```

a range covering a key whose window its dep cannot fill is a 400 naming the
missing upstream key, for the same reason naming that key at a build is: a
chunk that silently skipped it would report a backfill complete that was not.

the range resolves against the asset's key set at the moment it is made and is
then **fixed**: a daily set grows, and a backfill should build what it was
asked for rather than whatever that range means tomorrow. `only_missing`
(default true) drops the keys that are already materialized and fresh, which
is what makes re-running a backfill after a partial failure cheap. a range
that resolves to nothing is recorded `complete` on arrival rather than
refused: "there was nothing to do" is a better record than a 400.

it then launches **in chunks of `Partitions::build_limit`**, one run at a time:
the first goes out immediately, and each next one starts as the previous
finishes. that is the whole point: a 400-day range fired as a single run
would be 400 instances at somebody's api at once. each chunk is an ordinary
build run of the `assets` job, so the run page, the gantt, cancel and the
event log all work on it.

the record lives in the `backfills` table and its status derives from its runs:
`running` while a chunk is in flight or between chunks, `complete` when the
last one succeeded, `failed` when one failed (chunking stops there), and
`canceled` when one was canceled or the backfill itself was. `launched` counts
the keys handed to a run so far, against `total`.

limits: **one backfill per asset at a time** (a second is a 409) and no
cross-asset backfills; back one asset at a time. a chunk that
[meets a build already running](#builds-that-do-not-intersect-run-at-once)
waits for the following tick, the same self-heal the probe path uses. a chunk
of one asset's january no longer waits behind a build of something else: what
it claims is the keys it is filling.

### Probes mark everything stale

a probe fingerprint change marks **all** partitions of a descendant stale.
that is not a special rule; it falls out of the ordinary one. each partition
records the source's fingerprint at the moment it was built, and an
unpartitioned dep is read whole, so when the source moves every key's
recorded input disagrees at once. crude but honest: hestan cannot know which
days of your data an external change touched.

## Memoized builds

building a target materializes exactly the stale ancestors plus the target,
as one run. fresh ancestors do not re-run: their stored values are seeded
into the run as if their ops had just produced them, and their ops are
absent from the run's op list entirely. build-all is the same idea across
the whole graph, one plan covering every stale asset and one run, so a
morning's catch-up reads as a single coherent run in the ui.

### Where a seeded value comes from

a materialization records **what the io manager returned** for the value, so
what a memoized build seeds is what the row holds and the run reads it back the
way it reads any other input. under the default `Inline` that is the value
itself, which is what hestan has always done. under a manager it is a handle,
and the op downstream reads the file:

```rust
let orders = Asset::new("orders", ..).io("parquet");
```

so an asset of rows is stored once (as parquet, where the op run's handle
already pointed) rather than as a file plus a json copy in the run log.
`docs/io-managers.md` has the whole of it, including the one thing this changes
about [retention](io-managers.md#the-run-an-assets-value-is-inside): a run that
an asset's current value is inside is held back from the policy until something
rebuilds the asset.

a row written before any of this holds the value itself and still seeds: a
manager hands back what it did not write, so nothing has to tell an old row
from a new one and there is no migration.

`Hestan::build_asset(name)` is the headless form, like `run_once`: it always
materializes the target itself (plus stale ancestors), so check staleness
first if you only want conditional builds. **the http endpoint and
`hestan build <asset>` do not**: both go through the same conditional path
and answer `up_to_date` on a target that is already fresh.

memoization is a property of the *plan*, not of the run: retrying an assets
run (`POST /api/runs/{id}/retry`, or re-run in the ui) relaunches the whole
job with the original params, not the recorded subset: a full rebuild of
every derived asset, staleness ignored. cheap enough for small graphs; for
an expensive one, prefer the build endpoints, which re-plan from current
staleness.

## Builds that do not intersect run at once

when an asset materializes, it records its deps' fingerprints by re-reading
the store at that moment. two builds of the *same* asset interleaving those
reads and writes could record lineage that never happened: an asset claiming
it consumed a fingerprint its actual input value never had. two builds with
nothing in common cannot: they read and write different rows.

so a build is refused when, and only when, it intersects one already running.
the endpoints answer 409 with the asset and the run that holds it
(`sales is already being built by run 01a0...`), and the
[policy](#automation-policies) pass and each chunk a [backfill](#backfills)
sends hold quietly and ask again on their next tick.

### What is claimed

**`(asset, partition key)`**, with a null key for an unpartitioned asset. key
level is what lets a backfill of january run beside a build of february, which
is the case worth having: a backfill is the long one, and it is the one whose
blocking hurt.

**the whole plan, not the asset you named.** a build of `forecast` drags its
stale upstream in, so two builds whose plans share an upstream conflict even
though the two names typed at them do not, and the refusal names the shared
asset rather than either target.

### How it is decided, and how it is released

the claim is taken **in the transaction that writes the run row**, not by a
read the caller does first. a read followed by a launch is one process away
from two runs materializing one asset from two plans, and narrowing the rule
from "any build" to "an intersecting build" is exactly what makes concurrent
callers ordinary rather than rare.

**it is decided by what the run's plan says it builds**, and by nothing about
which way the build was asked for. the api, the command line, a policy pass, a
backfill chunk and a [cron on the asset](#a-cron-on-an-asset) all write a run
row carrying the same list, so all of them are refused by the same scan and
all of them refuse the next one. a build launched down a path that wrote the
list and did not read it would be a run everything else stood aside for while
it stood aside for nothing.

it is also **derived from the run rows** rather than kept in a table of its
own: what a build claims is exactly what its own recorded plan says it will
build. so a run reaching a terminal status releases it, there is nothing to
leak, and nothing can disagree with the run log. a process that dies mid-build
leaves a non-terminal run, and the sweeps that already recover those settle it:
`fail_interrupted` at the next boot, and the reclaimer every heartbeat once the
[lease](scaling.md) lapses, which is about a minute and a quarter after the
process stopped.

**a run that will not say what it builds claims everything.** a full manual
`POST /api/jobs/assets/runs` records no plan; a resume, a replay and
`build_asset` in a headless process record the ops they will run without the
assets those ops produce. hestan cannot bound their reach, so while one of them
is outstanding every build is refused, exactly as every build was refused
before. those are still the documented escape hatches, and they still cost what
they cost: nothing checks them on the way in, so their own recorded lineage is
only as coherent as the interleaving.

### How many at once

unbounded is not the goal. **four builds execute at once by default**, and the
rest wait on the queue in the ordinary way; `Hestan::max_concurrent_builds(n)`
is the knob. it is the [per-job limit](scaling.md#limits) on the `assets` job
rather than a mechanism of its own, so it counts *executing* runs and a queued
one costs nothing.

it is not the knob for an api that rate limits you. `Hestan::rate` caps calls
per second whatever is running, and a pool caps how many ops hold a connection
at once; both hold however the builds around them are arranged, and this does
not.

## A cron on an asset

an asset can own a schedule, which builds it when the expression comes round:

```rust
Hestan::new()
    .assets([vendor_prices, forecast])
    .add_schedule(Schedule::asset("vendor_prices", "0 6 * * *"))
```

this is the clock the policies below are not. a policy reacts to staleness,
which answers "rebuild when what it is made of moved"; a cron answers "build at
06:00, because that is when the vendor publishes". `AutoPolicy::after_cron` is
the two together, and is still what you want when the answer is "nightly, but
do not rebuild what has not moved".

it plans through the same function the endpoint and the command line plan
through, so it builds that asset plus whatever upstream of it is stale, and a
partitioned one takes the same default target set a build that names no keys
takes. the run is `trigger: schedule` and is tagged with the asset, so it
appears here like any other build.

it takes the same [claim](#builds-that-do-not-intersect-run-at-once) too. a
06:00 cron firing while `POST /api/assets/{name}/build` is still running is
refused by it, and refused without taking the occurrence: the tick is
`skipped`, saying which asset the two shared and which run holds it, and the
asset is still stale for the next pass to pick up. `docs/scheduling.md` has
the rest: what is refused at startup, and what a fire that found nothing to
build records.

## Automation policies

`.policy(..)` says when hestan may rebuild an asset without being asked. four
shapes, and no more, because each of the four is a thing people were writing
sensors for:

```rust
Asset::new("report", build).policy(AutoPolicy::when_stale());
Asset::new("report", build).policy(AutoPolicy::when_missing());
Asset::new("report", build).policy(AutoPolicy::after_cron("0 2 * * *"));
Asset::new("rollup", build).policy(AutoPolicy::when_stale().and_upstream_ready());
```

- **`when_stale`** rebuilds whatever staleness says is owed. `.auto()` is this
  and always was: the two spellings are the same policy and behave identically.
- **`when_missing`** builds what has never been built and nothing else. a fresh
  deployment and a newly declared asset both land here, and neither is a dep
  that moved; once it exists this rule has nothing more to say, however stale it
  goes.
- **`after_cron`** builds after the expression comes round *and only if it is
  stale by then*: "nightly, but do not rebuild what has not moved". the same
  5-field crontab a [schedule](scheduling.md) takes, read in utc until `.tz()`
  says otherwise, and an expression that does not parse is a boot error. a key
  builds when the last occurrence at or before now is newer than that key's last
  build, so an occurrence that passed while something else was building is
  picked up by the next pass rather than lost.
- **`and_upstream_ready`** holds any of them until every upstream key the build
  would read is there. a daily rollup [covering](#what-a-partition-reads-of-its-dep)
  hourly data waits for all 24 hours of its day rather than recording a partial
  one; without it, the rollup of the hours that happen to be there is what you
  get, and it goes stale as each of the rest lands. it waits for what its
  mapping says it reads, which is the same question staleness asks, so the two
  can never disagree about a key. it needs something to build upstream: an hourly
  asset with no policy of its own is one nobody is filling.

**a policy is evaluated one key at a time.** on a partitioned asset each key
gets its own verdict, so a pass builds the keys that qualify and leaves the ones
that do not: the two stale days of a daily rollup, not the four hundred fresh
ones. the keys it takes are newest first and capped by the same
[build limit](#the-build-limit-counts-keys-not-instances) a build that names no
keys respects, so a rule declared over two years of days fills them a chunk per
pass rather than all at once.

### The pass that acts on them

the process that [decides](scaling.md#roles) evaluates every policy once a
minute,
beside the [freshness](freshness.md) checker that reads the same staleness to
say what is late, and launches everything it wants as one plan and one run
(trigger `build`, tagged `policy` with the rule and `asset` when it is the only
one). one process decides, so one process launches.

it holds rather than stampeding, and only against what it actually meets: the
launch takes its [claim](#builds-that-do-not-intersect-run-at-once) like any
other build, and a pass whose plan intersects one already running launches
nothing, says so at debug and asks again next minute of fresher data. the build
endpoints answer 409 there instead, because a person is reading that refusal,
and a loop that logged one every minute would be a log nobody can read. nothing
queues in between: "is this stale" is not a question that expires. a build of
something unrelated no longer holds the pass at all.

a rule that cannot be satisfied sits quietly. the pass writes when it launches
and at no other time, so a rollup waiting for an hour that will never arrive
produces no run and no event, however many passes go by. what it is waiting for
is a fact `GET /api/assets` reports and `hestan doctor` reads, rather than
something it announces once a minute.

every launch says so: one `policy_launched` [event](events.md) per asset in the
plan, carrying the rule that fired and the keys it asked for.

### Probes and auto

a probe is a generated internal sensor named `probe:<asset>`, evaluated on
the [sensor loop](sensors.md) every `probe_every` (default 60s). a changed
fingerprint rewrites the source materialization (value null, no run); then,
changed or not, the policies of everything under that source are evaluated and
whatever they want is gathered into one combined plan and launched as a single
build run, the same one-coherent-run shape as build-all. the tick's `launched`
records that run, so 0 or 1. it is the same evaluation the pass makes, over the
part of the graph the probe just answered a question about, so the two cannot
disagree; a cron rule is left to the pass, since a fingerprint moving says
nothing about what time it is.

launching only what a policy says is owed, on every tick, is the self-heal.
a build that failed to launch, or was skipped because an assets run was
already active, is retried on the next tick without waiting for the data to
move again: the fingerprint commits before the launch, so nothing else
would ever re-trigger it. probes are pausable and tick-logged like any
sensor.

**a policy under a source nothing has observed does not fire.** a source's
fingerprint is what everything below it is compared against, and until a probe
writes one there is nothing to compare: a build would consume null and leave the
asset exactly as stale as it found it, and be owed again on the next pass
forever. so those keys wait for the probe instead, which is what `.auto()`
without a probed source upstream has always done, and `doctor` reports the ones
whose source has no probe at all. an asset with no policy just shows stale in
the ui until someone builds it.

## When a build is recorded

an asset is built when the op that built it succeeded, so that is when the
row is written, in the same transaction as the op run's terminal row, after
the output has been handed to the [io manager](io-managers.md) and stored.
the body computes what only the body can know (the fingerprint, the deps it
consumed, the value, whatever `ctx.meta` staged) and stages it; the executor
commits it with the row that says the op worked.

so an attempt that does not get all the way through records **nothing**: an
op that returns an error, panics, times out, is cancelled, or whose output
the manager refuses leaves no materialization, and the asset stays stale and
gets built again. a retry that succeeds records one entry, not one per
attempt. that matters most for the manager: an op whose output was never
stored used to leave a row saying the asset was current, and the next build
believed it and skipped: the asset was missing and nothing was stale.

an op that produces [several assets](#one-op-several-assets) writes all of
its materializations or none of them. they are one fact about one op run, and
they go into that one transaction together, so a history never holds the
first half of a multi-asset build.

what a probe writes is the exception, and it is a different kind of row: a
[source](#probes-and-auto) materialization records what a probe observed
outside any op, so it is written when the probe sees it and carries no run.

materialization stays at-least-once, the same policy (and the same reasoning)
as [op state](state.md): a crash between the transaction committing and
anything downstream reading it re-runs the build, and the rebuild appends a
second entry rather than editing the first. the direction of the uncertainty
is the part worth knowing: a build hestan lost the record of is rebuilt, and
a build hestan recorded is one whose op succeeded and whose value is stored.

## Materialization history

`asset_materializations` is append-only: every build adds an entry, and the
newest entry for an asset is its current state: what staleness compares
against, what a memoized build seeds, what `GET /api/assets` reports. nothing
overwrites anything.

that separates two facts the keyed table used to conflate. an asset that gets
rebuilt hourly has an entry per hour; the ones where the *fingerprint moved*
are the ones where the data actually changed. `GET /api/assets/{name}/history`
carries `changed` on each entry for exactly that: true when its fingerprint
differs from the entry before it in time, so a list of rebuilds reads as a
list of changes. the oldest entry of all counts as changed (nothing to
something), and a page's oldest entry is compared against the entry just off
the page, not reported as a change the window invented.

source assets append only when their probe sees a new fingerprint (the probe
path skips the write otherwise), so a source's history is already nothing but
changes. derived assets append on every build, and a run of identical
fingerprints is the record of work that found nothing new.

history grows without bound, so it is capped rather than left to grow. at
startup every asset is trimmed to its newest 200 entries;
`Hestan::asset_history(n)` sets the number. the newest entry is never trimmed
whatever `n` says: it is current state, and losing it would read as an asset
that has never been built. unlike a [retention policy](storage.md#retention)
this happens whether you ask or not.

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

the fn is handed an owned `Value` (the asset's freshly materialized output)
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

- **`Severity::Error`** (the default): the check op fails, so the run fails.
- **`Severity::Warn`**: the check op *succeeds* while the recorded result is
  `failed`. the run carries on and the failure is a fact in the check log
  rather than in the run status.

either way the result is recorded before the verdict is acted on, so a failing
error check leaves its message and metadata behind rather than only a failed
op.

state this one plainly, because it is the consequence people expect to go the
other way: **a failing error check does not un-materialize the asset.** the
materialization was written with the asset's op, which succeeded; the check
hangs off that op rather than feeding it, so downstream assets still see the
value and still build. what a failing error check does is fail the run that
produced it, loudly, in the run list, through the failure hooks. if you need
bad data to not reach downstream, that belongs in the asset's own fn, where
returning an error stops everything below it.

### Checks and memoization

a check is in a build plan exactly when the asset it checks is. an asset that
was fresh and got seeded rather than rebuilt does **not** get re-checked: it
produced no new value this run, and its last recorded result still describes
the value that is still current. that follows from checks being ops in the
plan rather than a separate pass, and it means a build costs nothing for the
parts it skipped, which is the entire point of memoized builds.

the consequence to know: a check that was added, or fixed, after an asset last
built does not run until that asset builds again, and the build endpoint will
not do it, since it answers `up_to_date` on an asset nothing made stale. what
forces one is `Hestan::build_asset(name)`, which always materializes its
target; naming the keys outright on a partitioned asset, which skips the
staleness gate; or `POST /api/jobs/assets/runs`, which rebuilds everything.

### Results

results land in `asset_checks`, capped per check by the same
`Hestan::asset_history(n)` that caps materializations, and never trimmed below
the latest one. `GET /api/assets/{name}/checks` lists them newest first, and
each asset in `GET /api/assets` carries
`{"passed": n, "failed": n, "last_run_at": ts}` counted from the latest result
per check name: zero and zero when nothing has ever recorded a result, which
reads the same whether no check is declared or none has run yet.

a check whose *body* returns an error (rather than a `CheckResult`) records
nothing: it produced no verdict, so the failed op is the whole of the record.

## The http api

`GET /api/assets` returns every asset in topo order with its kind, deps,
auto flag, its [policy](#automation-policies) (the rule, the cron where there is
one, and what it is waiting for when it wants a build it cannot have yet),
current fingerprint/built_at/run_id, and the staleness verdict with reasons.

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
carries what each build said, with [deltas](metadata.md#deltas) against the
build before it, and `GET /api/assets/{name}/metadata/{key}` for one numeric
key across recent builds. see [metadata](metadata.md).

shapes and details in [http api](http-api.md); the `asset_materializations`
table in [storage](storage.md).
