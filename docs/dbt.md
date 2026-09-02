# dbt

a dbt project already has a dag. it is compiled, it is correct, and it is
written down in `target/manifest.json`. this reads that file and produces one
hestan [asset](assets.md) per dbt model, wired from the manifest's own
`depends_on`.

```toml
hestan = { version = "0.2.1", features = ["dbt"] }
```

```rust
use hestan::dbt::Dbt;

let dbt = Dbt::from_manifest("analytics/target/manifest.json")?;

Hestan::new()
    .assets(dbt.assets())
    .schedule("assets", "0 4 * * *")
    .serve(([127, 0, 0, 1], 4000))
    .await
```

no dependency comes with the feature. a manifest is json and `serde_json` was
already here.

this is the one part of [connecting to your data](connecting.md) that is a
capability rather than a convenience. calling a client from an op was always
possible; bringing another tool's dag *into* hestan's is the thing a wrapper
around a client cannot do.

## What you get

- **one asset per model**, named after the model: `stg_orders`,
  `orders_daily`.
- **one source asset per dbt source a model reads**, named
  `{source_name}.{table}`: `raw.orders`. a source dbt parsed but nothing reads
  is not in the graph: it is a line in a yml file, not a thing anything is
  made of.
- **dbt's lineage**, taken from each node's `depends_on.nodes`. nobody retypes
  it, so nobody forgets to update it.
- the [catalog](web-ui.md), the [lineage view](assets.md), staleness,
  [freshness policies](freshness.md), checks, and everything else an asset
  has. a dbt model is an asset like any other.

what that buys over `dbt run` on a cron: a model is a node in the same graph
as everything else you orchestrate, so an asset of your own can depend on
`orders_daily` and be built when it is; the run page says which model failed
rather than giving one exit code for the lot; and each model's output is
stored under its own op.

## What building one does

`dbt run --select <model>`, in the project directory, with the environment
hestan was started with:

```
dbt run --select stg_orders
```

**your dbt and your profile, invoked.** nothing here reimplements a jinja
renderer, an adapter or a profile lookup, and nothing here reads your sql.
hestan decides when a model is built and records what happened when it was.

- **stdout and stderr are captured**, line by line, under the op and the
  attempt. it is the same [subprocess capture](logs.md#subprocess-capture) an
  isolated op's child gets, and is subject to the same caps. dbt is chatty and
  the interesting line is always in the middle of it.
- **a non-zero exit fails the asset**, with the exit code in the op's error
  and no materialization recorded. `Asset::retries` works as it does anywhere
  else.
- **a cancelled run kills dbt.** the child is spawned with `kill_on_drop`, so
  a run that is cancelled does not leave a `dbt run` writing to your
  warehouse.
- **stdin is `/dev/null`**, so nothing can block waiting for a terminal
  nobody is at.
- a dbt that is not installed fails the asset naming what could not be
  started, rather than "No such file or directory".

`Dbt::command("...")` names a different executable: a virtualenv's dbt, or a
wrapper of your own. one program and no arguments: the arguments are hestan's.
`Dbt::project_dir("...")` moves where it runs; by default that is two levels
up from the manifest, since dbt writes `<project>/target/manifest.json`.

## Manifest versions

**v9 through v12**, which is dbt 1.5 through 1.10.

hestan reads four things out of a manifest: a node's `name`, its
`resource_type`, its `depends_on.nodes`, and a source's `source_name`. those
have meant the same thing across all four versions, and everything else in the
file (and there is a great deal of it) is ignored, so a version that only
adds fields keeps working.

anything else is refused by version, naming the file:

```
dbt manifest analytics/target/manifest.json: it is manifest schema v14, and this
build of hestan reads v9 to v12 (dbt 1.5 to 1.10)
```

that is a startup error, before any run exists. the alternative (parsing
hopefully and taking what matches) produces an *empty asset graph*, which
looks exactly like a project nobody has compiled yet, and that is a failure
somebody debugs for an afternoon.

the other refusals are the same variant and name the file the same way: it
could not be read, it is not json a manifest could be, or two of its nodes
would become one asset (two models of the same name in two packages; keeping
the second quietly would drop the first's lineage).

## Freshness, and what hestan cannot see

hestan does not query your warehouse. it cannot know whether a table's
contents changed, and it will not pretend to:

- a model that hestan rebuilt gets a **new fingerprint**, so everything
  downstream of it is stale. that is the honest reading of "dbt ran": the
  table may well be different now.
- a **source arrives with no [probe](assets.md)**, so nothing marks a model
  stale on its own. every plan that reaches a source treats what is under it
  as stale, which is what `dbt run --select` does anyway.

give a source a probe and the graph becomes incremental. dbt runs for the
models a change actually reaches:

```rust
let assets = dbt.assets().into_iter().map(|asset| {
    if asset.name() != "raw.orders" {
        return asset;
    }
    // whatever cheaply fingerprints the source: a max(updated_at), an etag,
    // the load-time watermark whatever fills that table already writes
    asset.probe(|| async { Ok(latest_load_time().await?) })
});

Hestan::new().assets(assets)
```

`Asset::name()` is how you find the one you want in a vec you did not write.

building a model always runs dbt for **that** model, whatever its freshness:
asking for it is what asking means. freshness decides what upstream of it runs
too.

## What is not covered

written down rather than discovered:

- **models only.** seeds, snapshots, data tests, analyses and hooks are not
  assets. a model that depends on a seed keeps that dependency in dbt and
  loses it in hestan's graph: hestan has no node to point the edge at. run
  `dbt seed` and `dbt snapshot` as ops of your own if you need them.
- **`dbt test` is not run.** an [asset check](assets.md) of your own can shell
  out to `dbt test --select <model>` if you want the results in hestan; a
  future phase may read them from `run_results.json` rather than guessing at
  them.
- **`run_results.json` is not read**, so rows affected, per-model timing and
  dbt's own status words are not in hestan's metadata. what is there is what
  dbt printed and what it exited with.
- **no `--target`, `--vars`, `--profiles-dir` or `--full-refresh`.** the
  environment hestan was started with is the environment dbt gets
  (`DBT_TARGET`, `DBT_PROFILES_DIR` and the rest included), and that is the
  whole of the configuration surface. hestan does not build a second way to
  configure dbt.
- **the manifest is read once, at startup.** a model added by a later
  `dbt compile` appears when the process restarts. the manifest is a build
  artifact of your project, and hestan's registry is built from your code at
  startup like everything else.
- **selection is per model.** hestan builds the graph node by node, so there
  is one `dbt run` per model rather than one for the lot. that is the price of
  a model being a node you can see, retry and depend on, and it is a real
  price: `dbt run` starts a process and connects to the warehouse each time.
  a project with three hundred models is a project to think about this in.

## What the tests cover, and what they cannot

**dbt is not installed in hestan's test suite and must not need to be.** the
fixture manifest in `tests/fixtures/dbt/` (a diamond over a source, with a
seed, a data test, a hook and a disabled model beside it) is committed, and
the parse and the graph are asserted against it.

the shell-out is asserted against a script standing in for dbt: that each
model is invoked with `run --select <model>`, in the project directory, that
what it printed on either stream lands under that op with the right stream,
that a non-zero exit fails the asset and materializes nothing, and that a
missing executable is an error naming it.

what no test here can assert is the other side of the boundary: that
`dbt run --select orders_daily` builds `orders_daily` in your warehouse. that
is dbt's, and a test of it in this repo would be a test of whether dbt
happened to be installed on the machine that ran it, which is the kind of
test that passes by not running.
