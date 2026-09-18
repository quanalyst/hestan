# dbt

With the `dbt` feature, `Dbt::from_manifest` registers models and their source
lineage from a compiled manifest as Hestan assets. Install and configure dbt
separately.

```toml
hestan = { version = "0.2.5", features = ["dbt"] }
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

## What you get

- One asset per model, named after the model.
- Source assets for referenced dbt sources, named `{source_name}.{table}`.
- Dependencies from the manifest's `depends_on.nodes`.
- Normal asset inspection, materialization history, retries and downstream
  dependencies, including dependencies from application-defined assets.

Unreferenced sources are omitted. Duplicate model names are rejected.

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

The parser accepts manifest schemas **v9 through v12** (dbt 1.5 through 1.10).
It reads node names, resource types, dependencies and source names, ignoring
unused fields. Unsupported schemas, unreadable or invalid manifests, and names
that would collide produce startup errors identifying the file.

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

- Only models and referenced sources become assets. Seeds, snapshots, tests,
  analyses and hooks are not registered. Dependencies on omitted node types
  are absent from Hestan's graph.
- Hestan does not run `dbt test` or read `run_results.json`. Metadata is limited
  to captured output and process results; run additional commands in your own
  operations when needed.
- The wrapper accepts an executable and project directory, not arbitrary dbt
  flags. Configure dbt through its environment or an executable wrapper.
- The manifest is read at startup. Restart after changes to register new models.
- Each model build starts a separate `dbt run --select <model>` process and
  connection, which can add significant overhead for large projects.

## What the tests cover, and what they cannot

Repository tests validate manifest parsing, graph construction and command
invocation using a substitute executable. They do not validate dbt adapters or
connect to a warehouse; test those in your application environment.
