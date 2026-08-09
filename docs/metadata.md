# Metadata

an op's output is for the ops downstream. metadata is for the people
upstream — how many rows it wrote, where it read them from, what it noticed
along the way. facts *about* the work rather than the work's result.

```rust
Op::new("load", |ctx| async move {
    let rows = copy_into_warehouse()?;
    ctx.meta("rows", rows as i64);
    ctx.meta("source", Meta::Url(endpoint.clone()));
    ctx.meta("note", "backfilled from the archive");
    Ok(json!({ "loaded": true }))
})
```

it lands on the op run, which means the run page, the api, and — for an
asset op — that build's entry in the [materialization
history](assets.md#materialization-history).

## Typed, not stringly

the type is not decoration. it is what lets a row count line up as a number,
a size read as `1.2 GB`, a source render as a link, and a blob stay a blob,
without anything reading the value having to guess.

| variant | stored as | rendered as |
| --- | --- | --- |
| `Int(i64)` | `{"int": 1234}` | `1,234`, tabular |
| `Float(f64)` | `{"float": 0.5}` | `0.5`, tabular |
| `Text(String)` | `{"text": "…"}` | inline text |
| `Url(String)` | `{"url": "…"}` | a link, new tab |
| `Markdown(String)` | `{"markdown": "…"}` | source |
| `Json(Value)` | `{"json": …}` | a preformatted block |
| `Table(MetaTable)` | `{"table": {…}}` | a small table |
| `Bytes(u64)` | `{"bytes": 1288490188}` | `1.2 GB` |
| `Duration(Duration)` | `{"duration_secs": 3.4}` | `3.4s` |
| `Count(u64)` | `{"count": 1240}` | `1,240`, tabular |
| `Path(String)` | `{"path": "/tmp/x.parquet"}` | monospace, basename emphasised |
| `RunRef(String)` | `{"run": "019fe109-…"}` | a link to that run |
| `AssetRef(String)` | `{"asset": "orders"}` | a link to that asset |

the obvious rust types convert on their own — `i64`, `i32`, `u32`, `f64`,
`String`, `&str`, `std::time::Duration`, and `serde_json::Value` (which
becomes `Meta::Json`) — and the rest are named outright, either as a variant
or through a constructor that reads better at a call site:

```rust
ctx.meta("rows", Meta::count(1_240));
ctx.meta("size", Meta::bytes(1_288_490_188));
ctx.meta("output", Meta::path("/warehouse/orders.parquet"));
ctx.meta("derived_from", Meta::asset_ref("raw_orders"));
```

`u64` and `usize` deliberately do not convert: narrowing them is a lie waiting
to happen, so cast them yourself — or say which kind of number you meant, with
`Meta::count` or `Meta::bytes`, and get the units for free.

### Units are display types over one number

`Bytes`, `Duration` and `Count` hold the same numbers `Int` and `Float` do.
The difference is that the ui knows what they are in, so `1288490188` renders
as `1.2 GB` and `3.4` as `3.4s` — and anything computing over metadata reads
all five as one number, so a count that grew by 37 and a size that shrank by
4% are the same arithmetic over different units. Byte sizes are printed in decimal
units (kB, MB, GB, TB), the way storage and warehouses quote them.

### Links hestan can follow

`RunRef` and `AssetRef` take an id and a name, not a url. Dagster needs a url
here because it cannot know where its own pages are; hestan is the ui, so an
op that says `Meta::run_ref(id)` gets a working link to that run and
`Meta::asset_ref(name)` opens that asset's panel. Neither is validated — a
reference to a run that has since been swept is a link to a 404, which is
better than no link and better than a broken guess at a hostname.

### Tables

```rust
ctx.meta("by_region", Meta::table(
    [("region", "text"), ("orders", "int")],
    rows.iter().map(|r| vec![json!(r.region), json!(r.orders)]),
));
```

columns are names, or `(name, type)` pairs; the type is a label printed beside
the column and never anything hestan parses. two invariants hold at
construction, so nothing downstream has to check them:

- **at most `META_TABLE_ROWS` (100) rows**, with `truncated: true` recorded
  when rows were dropped. a metadata table is a sample you read at a glance,
  not a result set: it lands in every run page, every history entry and every
  api response that carries the row.
- **rectangular**: every row is padded with `null` and trimmed to the column
  count, so a short row is a visible gap rather than a shape question.

### Reading it back

`Meta::tagged` produces the stored value and `Meta::from_tagged` reads one,
returning `None` for anything that is not a one-key object with a tag this
version knows. `Meta::as_f64` is the number a numeric variant carries and
`None` for every other variant — the two of them are how anything computes
over rows that were written long before.

**tags are never renumbered.** rows written by every hestan since phase 12 are
on disk, so `int`, `float`, `text`, `url`, `markdown` and `json` mean today
exactly what they meant then; this phase added tags beside them rather than
changing any. there is a test with a phase-12 row in it that fails if that
ever stops being true.

## Staged like state

`ctx.meta` buffers per attempt, exactly like
[`set_state`](state.md) and `set_fingerprint`:

- the last call for a name wins,
- a failed attempt's metadata is discarded entirely, so the retry starts from
  nothing and what gets stored is what the attempt that *worked* reported,
- everything staged commits in the op's terminal write, in the same statement
  as the success. an op run never carries facts about work that did not
  finish.

an op that said nothing stores `null`, not `{}` — "reported no metadata" and
"reported an empty map" are different, and only the first one ever happens by
accident.

## How it is stored

one json object per op run in `op_runs.metadata`, keyed by name with a tagged
value:

```json
{ "rows": {"int": 1234},
  "source": {"url": "https://example.test/orders"},
  "note": {"text": "backfilled from the archive"} }
```

the tag is the wire format the api and the ui both read, which is why it is
written out rather than inferred. keys come out sorted.

## Assets carry it twice

when an asset op reports metadata, the same map is written to that
materialization as well as to the op run. the op run is what that *run* did;
the materialization is what that *build of the asset* reported, and it
outlives the run — history keeps it long after retention has deleted the run
row. `GET /api/assets/{name}/history` carries it per entry, so "the day the
row count halved" is a thing you can go and look at.

nothing else in the pipeline reads metadata. it does not affect fingerprints,
staleness, or what a build decides to run — an asset that reports different
metadata for an identical value is still fresh.

## Where it shows up

- `GET /api/runs/{id}` — each op row has `metadata`, null when the op
  reported none.
- `GET /api/assets/{name}/history` — each entry has `metadata`.
- the run page renders the selected op's metadata by type: numbers
  right-aligned and tabular in their unit, urls as links, runs and assets as
  links into the ui, paths monospace, tables as tables, text inline, markdown
  and json in a muted preformatted block.
- the asset detail panel renders each build's metadata under its history
  entry, the same way.
