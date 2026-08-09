# IO managers

op outputs are json in sqlite. that is right for `{"loaded": 4210}` and wrong
for a dataframe, and the run log is the worst place to find out which one you
have. an *io manager* makes persistence pluggable: the value goes wherever you
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
}

pub struct IoKey { pub run_id: String, pub job: String, pub op: String }
```

`IoKey::op` is `{op}[{i}]` for one fan-out instance, since each instance
persists its own output.

two rules:

- **round-trip.** `get(key, put(key, v))` must be `v`.
- **`get` must be total.** it is called on every value a run hands an op, and
  not all of them came from this manager's `put`: a source asset is seeded
  `null`, a fan-out's collected array is assembled from its instances, a
  resume seeds a value an earlier run recorded, and a job can mix managers op
  by op. anything a manager did not produce, it returns unchanged. `Inline`
  does that with everything; `FileIo` does it with anything that is not one of
  its own `$io` handles.

resolve from the **handle**, not from the key. the key is context — useful for
laying out storage in `put`, and for logging — but a handle read back on a
resume carries the run id of the run that wrote it, not the one reading it.

both calls are synchronous and run on the run's own task, so a manager that
talks to something slow should say so.

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

**nothing is ever cleaned up.** [retention](storage.md) prunes run rows, not
files. point `FileIo` at a directory you are willing to sweep.

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

## In the ui

the run page shows the selected op's output on one line. an `$io` object reads
as the reference it is (`file · /var/lib/hestan/io/019.../extract.json`)
rather than as pretty-printed json, because the json is not the value.
`GET /api/jobs/{name}` reports each op's `io` — the named manager it selected,
`null` for the default.
