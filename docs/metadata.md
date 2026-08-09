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

```rust
pub enum Meta { Int(i64), Float(f64), Text(String), Url(String), Markdown(String), Json(Value) }
```

the type is not decoration. it is what lets a row count line up as a number,
a source render as a link, and a blob stay a blob, without anything reading
the value having to guess. the obvious rust types convert on their own —
`i64`, `i32`, `u32`, `f64`, `String`, `&str`, and `serde_json::Value` (which
becomes `Meta::Json`) — and the rest are named outright:
`ctx.meta("source", Meta::Url(url))`. `u64` and `usize` deliberately do not
convert: narrowing them is a lie waiting to happen, so cast them yourself.

`Meta::Markdown` is stored and shown as **source**. hestan does not render
markdown and does not carry a parser to do it; the variant exists so a tool
that does render it knows which strings to try.

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
  right-aligned and tabular, urls as links, text inline, markdown and json in
  a muted preformatted block.
- the asset detail panel renders each build's metadata under its history
  entry, the same way.
