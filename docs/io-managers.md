# IO managers

op outputs are json in the run log, whichever backend it is on. that is right
for `{"loaded": 4210}` and wrong for a dataframe, and the run log is the worst
place to find out which one you have. an *io manager* makes persistence pluggable: the value goes wherever you
say, and `op_runs.output` keeps a handle to it.

```rust
Hestan::new()
    .io(FileIo::new("/var/lib/hestan/io"))
```

the default is `Inline`, which is exactly what hestan has always done — the
output is its own handle and lands in the run log as json. changing nothing
changes nothing.

## The trait

```rust
pub trait IoManager: Send + Sync + 'static {
    /// persist a value, returning the handle stored in op_runs.output
    fn put(&self, key: &IoKey, value: Value) -> IoResult;
    /// resolve a handle back to the value
    fn get(&self, key: &IoKey, handle: &Value) -> IoResult;
    /// drop everything stored for one run — retention calls this
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
  sweep already collected are both `Ok(())` — [what retention
  takes](#what-retention-takes) is the whole of why.

resolve from the **handle**, not from the key. the key is context — useful for
laying out storage in `put`, and for logging — but a handle read back on a
resume carries the run id of the run that wrote it, not the one reading it.

`drop_run` has **no default**, so adding a manager of your own is a decision
you make rather than one that gets made by omission: a no-op default would
compile and leak every file the manager has ever written. `Inline` returns
`Ok(())` because an inline output is the run log row retention is deleting.

all three calls are synchronous, and hestan makes them on tokio's blocking
pool rather than on the task driving the run. write them as ordinary blocking
code: a manager may take as long as the storage behind it takes, and the ops
beside it keep running. the one exception is `Runner::resume_plan`, which
resolves an earlier run's outputs on the thread that called it, exactly as it
does its store reads — that whole api is synchronous, and nothing is
executing while a resume is being planned.

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
`op_runs.output` — including the ui — can tell a reference from a value at a
glance.

[retention](storage.md#retention) takes the files with the rows: pruning a
run removes `{dir}/{run_id}` whole. what is never collected is a run no
policy deletes — see [what retention takes](#what-retention-takes).

## What a name may be

`{run_id}` and `{op}` go into a path, and the rule is that the file lands
under the directory the manager was given. that is the whole rule — it is not
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
moved still read back — the handle is the record of where the value actually
went. and a symlink already inside the directory pointing out of it is
outside what a name check can see: an io directory somebody else can write to
is a problem no orchestrator can spell its way out of.

## ParquetIo

behind `--features parquet`, for the case json is wrong about: an op that
returns **rows**.

```toml
hestan = { version = "0.1.0-alpha.2", features = ["parquet"] }
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
per row — which is what an op returns when it returns rows, including a
[typed](typed-io.md) op returning a `Vec<T>`. the column types come from the
values: whole numbers as `int64`, fractions as `float64`, then `utf8`, `bool`,
lists, structs, and a column that is null the whole way down as parquet's null
type. the op downstream reads the same rows back.

`null` passes straight through — an op that produced nothing has no table to
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
partitioned datasets, no compaction, no manifest, no object store — one op
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
staged under the same name wins — an op that said `rows` meant its own rows.

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
  op, with `could not read the output of extract: ...` — not an op that runs
  believing its dep produced nothing.
- **fan-out.** the array a mapped op expands over is an op output, so it is
  fetched back before it can be counted. each instance persists its own
  output, and the collected array downstream sees is what those handles
  resolve to — a mapped op has no row and never puts anything of its own.
- **resume.** seeds come from an earlier run's `op_runs.output`, which are
  handles; resuming across a `FileIo`-backed op reads the file the first run
  wrote.
- **asset builds.** a memoized build seeds a fresh dep from its
  materialization. that value never went through `put` — asset
  materializations record the asset's value, not an op handle — so it arrives
  through `get`'s pass-through rule.

## When put fails

on success the output is persisted **before** the success is recorded. a
`put` that fails fails the op instead:

```
could not persist the output: nowhere to put it
```

the op run is `failed` with no output, its downstream is skipped, and the run
fails. recording success for a value that was never stored would strand the
next resume, which would seed a handle to nothing.

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
gone — for runs already past retention, which the next sweep deletes anyway.
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

three things follow, and all three are worth knowing before you point a
manager at a directory:

- **only what a policy deletes is collected.** with no `Retention`
  configured nothing is pruned and nothing is dropped, so the directory grows
  exactly as the run log does.
- **the process that decides is the process that deletes** — see
  [roles](scaling.md#roles). the directory has to be on *its* filesystem: a
  scheduler that cannot see the disk the workers wrote to cannot collect it.
- **the whole run goes at once.** `{run_id}/{op}` means one directory per
  run for both bundled managers, and the sweep removes it whole — after
  checking that it is under the manager's own directory, by the same rule a
  `put` is checked by. a path computed from a run id and removed without that
  check would be a much worse bug than the leak it was fixing.

## In the ui

the run page shows the selected op's output on one line. an `$io` object reads
as the reference it is (`file · /var/lib/hestan/io/019.../extract.json`)
rather than as pretty-printed json, because the json is not the value.
`GET /api/jobs/{name}` reports each op's `io` — the named manager it selected,
`null` for the default.
