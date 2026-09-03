# IO managers

op outputs are json in the run log, whichever backend it is on. that is right
for `{"loaded": 4210}` and wrong for a dataframe, and the run log is the worst
place to find out which one you have. an *io manager* makes persistence pluggable: the value goes wherever you
say, and `op_runs.output` keeps a handle to it.

```rust
Hestan::new()
    .io(FileIo::new("/var/lib/hestan/io"))
```

the default is `Inline`, which is exactly what hestan has always done: the
output is its own handle and lands in the run log as json. changing nothing
changes nothing.

## The trait

```rust
pub trait IoManager: Send + Sync + 'static {
    /// persist a value, returning the handle stored in op_runs.output
    fn put(&self, key: &IoKey, value: Value) -> IoResult;
    /// resolve a handle back to the value
    fn get(&self, key: &IoKey, handle: &Value) -> IoResult;
    /// drop everything stored for one run; retention calls this
    fn drop_run(&self, run_id: &str, job: &str) -> IoDropped;
}

pub struct IoKey { pub run_id: String, pub job: String, pub op: String }
```

`IoKey::op` is `{op}[{i}]` for one fan-out instance, since each instance
persists its own output.

three rules:

- **round-trip.** `get(key, put(key, v))` must be `v`.
- **`get` must be total.** it is called on every value a run hands an op, and
  not all of them came from this manager's `put`: a source asset is seeded
  `null`, a fan-out's collected array is assembled from its instances, a
  resume seeds a value an earlier run recorded, and a job can mix managers op
  by op. anything a manager did not produce, it returns unchanged. `Inline`
  does that with everything; `FileIo` does it with anything that is not one of
  its own `$io` handles.
- **`drop_run` is idempotent.** a run that wrote nothing and a run an earlier
  sweep already collected are both `Ok(())`; [what retention
  takes](#what-retention-takes) is the whole of why.

resolve from the **handle**, not from the key. the key is context (useful for
laying out storage in `put`, and for logging), but a handle read back on a
resume or a [replay](replay.md) carries the run id of the run that wrote it,
not the one reading it.

`drop_run` has **no default**, so adding a manager of your own is a decision
you make rather than one that gets made by omission: a no-op default would
compile and leak every file the manager has ever written. `Inline` returns
`Ok(())` because an inline output is the run log row retention is deleting.

all three calls are synchronous, and hestan makes them on tokio's blocking
pool rather than on the task driving the run. write them as ordinary blocking
code: a manager may take as long as the storage behind it takes, and the ops
beside it keep running. the exceptions are `Runner::resume_plan` and
`Runner::replay_plan`, which resolve an earlier run's outputs on the thread
that called them, exactly as they do their store reads: that whole api is
synchronous, and nothing is executing while a resume or a replay is being
planned.

## FileIo

bundled, because it is the obvious first real one:

```rust
Hestan::new().io(FileIo::new("/var/lib/hestan/io"))
```

it writes `{dir}/{run_id}/{op}.json` and records

```json
{ "$io": "file", "path": "/var/lib/hestan/io/019.../extract.json" }
```

the handle is an object rather than a bare path so anything reading
`op_runs.output` (including the ui) can tell a reference from a value at a
glance.

[retention](storage.md#retention) takes the files with the rows: pruning a
run removes `{dir}/{run_id}` whole. what is never collected is a run no
policy deletes; see [what retention takes](#what-retention-takes).

## What a name may be

`{run_id}` and `{op}` go into a path, and the rule is that the file lands
under the directory the manager was given. that is the whole rule. it is not
"no separators", because an [asset](assets.md) name is already a path:
`sales/orders` is a directory and a file here, exactly as the catalog groups
it on the same prefix. one fan-out instance keeps its `fetch[0]` too.

what is refused is a name that would leave the directory. every part of both
halves of the key has to be an ordinary path component, so `..` anywhere, a
name that starts at the root, a name that is only dots, and an empty name all
fail the op the way any other failed `put` does:

```
could not persist the output: op "../escape" does not name a file under the io directory
```

refused rather than quietly rewritten, because a name silently spelled
differently is a file nobody looking for it finds. both bundled managers
answer this the same way, since a key that is a file for one and an error for
the other would be two answers to one question.

two things this does not do. a `get` resolves the path **in the handle**
rather than recomputing one, so outputs written before the directory was
moved still read back: the handle is the record of where the value actually
went. and a symlink already inside the directory pointing out of it is
outside what a name check can see: an io directory somebody else can write to
is a problem no orchestrator can spell its way out of.

## ParquetIo

behind `--features parquet`, for the case json is wrong about: an op that
returns **rows**.

```toml
hestan = { version = "0.2.4", features = ["parquet"] }
```

```rust
Hestan::new().io_named("parquet", ParquetIo::new("/var/lib/hestan/parquet"))
```

it writes `{dir}/{run_id}/{op}.parquet` and records

```json
{ "$io": "parquet", "path": "/var/lib/hestan/parquet/019.../extract.parquet",
  "rows": 41233, "bytes": 918444 }
```

what it stores is **a table**: a json array whose elements are objects, one
per row, which is what an op returns when it returns rows, including a
[typed](typed-io.md) op returning a `Vec<T>`. the column types come from the
values: whole numbers as `int64`, fractions as `float64`, then `utf8`, `bool`,
lists, structs, and a column that is null the whole way down as parquet's null
type. the op downstream reads the same rows back.

`null` passes straight through: an op that produced nothing has no table to
write, and null is already its own handle. **anything else is an error**, not
a quiet fallback to json:

```
could not persist the output: parquet stores a table: an array of row objects, not an object
```

which fails the op, exactly as any other failed `put` does. an op whose output
went somewhere it did not ask for is a value nobody finds again. this is
usually a *named* manager selected by the ops that produce tables, rather than
the default for every op in a deployment.

two things do not survive the round trip, and neither can:

- a column mixing whole numbers and fractions is one `float64` column, so `1`
  reads back as `1.0`. a parquet column has one type; json does not.
- a key missing from one row reads back as an explicit `null`, because a table
  has the same columns in every row.

**what it is not**: a directory of files, exactly as `FileIo` is one. no
partitioned datasets, no compaction, no manifest, no object store. one op
writes one file and the op downstream reads that file. anything more is a
table format, which is a different thing to be. names land where `FileIo`'s
do, are refused on the same terms, and are collected on the same terms.

reading and writing happen on the blocking pool rather than on the run's own
task, as every manager call does, so a file worth minutes of io costs the op
that wrote it and not the ops beside it.

## What a handle says about what it stored

a manager knows two things the op does not, because the op returned a value
rather than a file: how many rows were stored and how big the thing holding
them is. a handle carrying `rows` or `bytes` gets them recorded as
[`Meta::Count` and `Meta::Bytes`](metadata.md) on the op run, so they show on
the run page and in the trend beside every previous build:

```json
{ "rows": { "count": 41233 }, "bytes": { "bytes": 918444 } }
```

it is a rule about handles rather than one about parquet, so a manager of your
own gets it by putting either key in what `put` returns. anything the op
staged under the same name wins: an op that said `rows` meant its own rows.

## Per-op managers

`Hestan::io_named(name, manager)` registers one under a name, and
`Op::io(name)` selects it:

```rust
Hestan::new()
    .io_named("archive", FileIo::new("/mnt/archive"))
    .job(Job::builder("etl")
        .op(Op::new("extract", ..).io("archive"))
        .op(Op::new("load", ..).after(["extract"]))     // the default
        .build()?)
```

naming a manager that was never registered fails the build:

```
invalid job graph: job etl: op extract persists through io manager archive, which is not registered
```

quietly falling back to the run log would put the value in the one place the
op said not to.

## Where handles are resolved

everything an op reads goes through `get`, so a manager is exercised on every
path, not just the happy one:

- **downstream inputs.** an op's dependents are handed handles, which are
  resolved as each op is spawned. an input that cannot be fetched fails that
  op, with `could not read the output of extract: ...`, not an op that runs
  believing its dep produced nothing.
- **fan-out.** the array a mapped op expands over is an op output, so it is
  fetched back before it can be counted. each instance persists its own
  output, and the collected array downstream sees is what those handles
  resolve to. a mapped op has no row and never puts anything of its own.
- **resume.** seeds come from an earlier run's `op_runs.output`, which are
  handles; resuming across a `FileIo`-backed op reads the file the first run
  wrote.
- **[replay](replay.md).** the same seeds, read twice: once when the replay is
  planned, to refuse a run whose values are gone rather than launch one that
  cannot reproduce anything, and once by the op that reads the input. a
  manager whose `get` is expensive pays for that, and the alternative is a run
  that fails halfway through claiming to be a reproduction.
- **asset builds.** a memoized build seeds a fresh dep from its
  materialization, which holds what `put` returned for it. so seeding a
  parquet-backed asset hands the next build a handle, and the op that reads it
  reads the file; see [an asset's value](#an-assets-value).

## An asset's value

an [asset](assets.md) is an op with a value somebody keeps, so its value goes
where every other output goes:

```rust
let orders = Asset::new("orders", ..).io("parquet");

Hestan::new()
    .io_named("parquet", ParquetIo::new("/var/lib/hestan/parquet"))
    .assets([orders])
```

`asset_materializations.value` records **what the manager returned**: the
handle under `ParquetIo`, the value itself under `Inline`. the same handle the
op run keeps, for the same file: an asset of rows used to be stored twice, once
as a handle in `op_runs.output` and once inline in the materialization, and the
inline copy was the one a later build read.

seeding reads it back through the manager the asset stores through, so the
build that memoizes `orders` hands the op downstream the rows out of the
parquet file rather than json out of the run log. nothing about staleness
changes: the fingerprint is of the value the op returned, so where the value
ended up cannot make anything stale.

three things worth knowing:

- **a source has no value of its own**, so `.io(..)` on one is a build error. a
  source's materialization is a fingerprint of something outside hestan.
- **a multi-asset keeps each slice inline.** one op returns one object holding
  every asset it produces, the manager has one handle for the whole of it, and
  no part of it has a handle of its own, so each produced asset records the
  value it was, as it always has. `MultiAsset::io(..)` still says where the
  op's own output goes.
- **an existing row keeps working**, and there is no migration. a row written
  before any of this holds the value itself, which is not one of any manager's
  handles, so `get`'s pass-through rule hands it straight back. nothing has to
  tell the two apart, and no column says which kind a row is. that is the
  same arrangement `op_runs.output` has always had, with the same residual
  ambiguity: a value that happens to look like a live manager's handle is read
  as one.

**changing where an asset's value goes** leaves a row the new manager did not
write, and a manager hands back what it did not write: a `ParquetIo` handle is
not a value to `Inline`, so a build that memoizes that asset would seed the
handle itself. a build of the asset writes a row the new manager wrote and
settles it, so the window is one memoized build in between. rebuild each asset
you move, rather than waiting to find out.

what else changes is [what retention takes](#what-retention-takes).

## When put fails

on success the output is persisted **before** the success is recorded. a
`put` that fails fails the op instead:

```
could not persist the output: nowhere to put it
```

the op run is `failed` with no output, its downstream is skipped, and the run
fails. recording success for a value that was never stored would strand the
next resume, which would seed a handle to nothing.

## What a backup does not contain

the counterpart to the section below, and the one nobody expects. a manager
puts op outputs **outside** the store, so a copy of the store holds the handles
and none of what they point at. restore a run log without the directory beside
it and you have materializations that say an asset is built, with a fingerprint
and a row count, pointing at parquet that is not on this machine. nothing in
the store can tell you, and nothing checks until a build, a resume or a replay
tries to read one.

so the directory is part of the backup, from the same instant.
[backup and recovery](backup.md#what-a-copy-does-not-contain) is the whole of
it.

## What retention takes

a run's rows are the only record that the run existed. so when
[retention](storage.md#retention) prunes a run it asks **every registered
manager** to drop what that run stored, and it does that **before** deleting
the rows:

1. read the ids this job's policy may no longer keep
2. `drop_run(run_id, job)` on every manager, for each of them
3. delete the rows

that order is not arbitrary. rows first, and a crash in between loses the
ids: nothing is left that knows which files to collect, and the leak is
permanent. files first, and a crash leaves rows pointing at outputs that are
gone, for runs already past retention, which the next sweep deletes anyway.
which is also why `drop_run` has to be idempotent: it will be asked twice.

every manager rather than the one each op selected, because which manager
wrote a given run's outputs is a question about a job the sweeping process
may no longer define. a manager that stored nothing for that run does
nothing, which is what makes asking all of them cheap.

a manager that **cannot** drop something is logged and the sweep carries on,
to the rest of that job's rows and to the next job. a file left behind is one
run's worth of waste that whoever owns the directory can still find; a sweep
that stopped there would grow the database forever behind one unwritable
directory, and go on doing it every hour.

### The run an asset's value is inside

a value in a manager is inside the run that wrote it, and the sweep takes what
a run wrote when it takes the run. so a run that an asset's **current**
materialization still reads is held back from every policy, rows and files
together, until something rebuilds the asset, at which point it is history
like any other run and the next sweep takes it.

the alternative is worse in both directions: prune it and the row points at
nothing, so the next build either fails on a hole or silently redoes work
somebody paid for. but this is a real change to what a policy promises, and it
is stated rather than buried: `Retention::days(30)` no longer means nothing
older than thirty days is here. an asset built a year ago and never rebuilt
keeps its run for as long as it stays current. `hestan doctor` counts them:

```
note  values     3 run(s) are held back from retention: an asset's current value is what they wrote, and a later build reads it
```

nothing is held back under `Inline`, whose values are in the materialization
itself and go nowhere when a run is pruned: a deployment that never configured
a manager prunes exactly as it did.

three more things follow, and all three are worth knowing before you point a
manager at a directory:

- **only what a policy deletes is collected.** with no `Retention`
  configured nothing is pruned and nothing is dropped, so the directory grows
  exactly as the run log does.
- **the process that decides is the process that deletes**; see
  [roles](scaling.md#roles). the directory has to be on *its* filesystem: a
  scheduler that cannot see the disk the workers wrote to cannot collect it.
- **the whole run goes at once.** `{run_id}/{op}` means one directory per
  run for both bundled managers, and the sweep removes it whole, after
  checking that it is under the manager's own directory, by the same rule a
  `put` is checked by. a path computed from a run id and removed without that
  check would be a much worse bug than the leak it was fixing.

## In the ui

the run page shows the selected op's output on one line. an `$io` object reads
as the reference it is (`file · /var/lib/hestan/io/019.../extract.json`)
rather than as pretty-printed json, because the json is not the value.
`GET /api/jobs/{name}` reports each op's `io`: the named manager it selected,
`null` for the default.
