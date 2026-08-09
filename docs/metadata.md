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
| `Markdown(String)` | `{"markdown": "…"}` | [a rendered subset](#the-markdown-subset) |
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

## The markdown subset

`Meta::Markdown` is rendered, by a parser in the ui that is about 180 lines
long and takes no dependency. It renders **exactly** the following and
nothing else:

| construct | written | notes |
| --- | --- | --- |
| heading | `# h` … `###### h` | `#` becomes an `h3` and the rest shift with it, clamped at `h6`; the page already owns `h1` and `h2`. a hash with no space after it is not a heading |
| paragraph | any other run of lines | soft-wrapped lines are joined with a space, blank lines separate paragraphs |
| bold | `**b**` | nests |
| italic | `*i*` | `_i_` is **not** italic — it is text |
| code span | `` `c` `` | nothing inside one is a construct |
| code block | ```` ```lang ```` … ```` ``` ```` | the info string is shown above the block; an unclosed fence runs to the end of the source |
| unordered list | `- a`, `* a`, `+ a` | one run of items is one list |
| ordered list | `1. a`, `2) a` | the numbers are not renumbered or read; switching between bullets and numbers starts a second list |
| link | `[text](https://…)` | see below |
| horizontal rule | `---`, `***`, `___` | three or more, alone on the line |

**everything else is the literal text it was written as.** no tables, no
blockquotes, no images, no reference links, no footnotes, no setext headings,
no html, no nested lists, no line-break-on-two-spaces. `> quoted` renders as
`> quoted`; `**unclosed` renders as `**unclosed`; `<img src=x onerror=...>`
renders as those characters. The subset is small because the point of it is
the paragraph after next.

### Links

a link is made **only** for an `http://` or `https://` target, and it opens
in a new tab with `rel="noreferrer"`. anything else — `javascript:`, `data:`,
a protocol-relative `//host`, a path, an empty target — is not a link at all,
and the construct stays the text it was written as, so what it pointed at is
visible rather than silently dropped.

### Why it cannot inject

the parser produces **data** — a tree of `{kind: "text" | "code" | "strong" |
"em" | "link" | …}` nodes — and the renderer turns that data into react
elements. no html string is built anywhere in the path, and nothing in the ui
uses `dangerouslySetInnerHTML`, which the test suite asserts by scanning the
source. so markup in a metadata value is a text child that react escapes, and
the only href that can exist is one the http check above approved. this is
injection being impossible by construction rather than by remembering to
escape — the version worth shipping.

`npm test` in `ui/` runs it: every construct above, a nesting case, and the
two attacks (`<img src=x onerror=...>`, `[x](javascript:alert(1))`) asserted
against the exact string react renders.

## Deltas

what a build reported is worth less than what *changed*. hestan has
materialization history and op-run history, so the api computes it: beside
every numeric metadata value, what it did since the last time.

```json
{ "metadata": { "rows": {"count": 1240}, "size": {"bytes": 1152000000} },
  "deltas":   { "rows": {"delta": 37, "delta_pct": 3.08},
                "size": {"delta": -48000000, "delta_pct": -4} } }
```

**what it is compared against.** for an op run, the same op of the newest
earlier run of that job; for a materialization, the previous build of that
same `(asset, partition)`. an op run that reported no metadata at all is
skipped rather than ending the search — a failed op records none, and one bad
run between two good ones should not erase the comparison between them.

**the rule.** `delta` is always the absolute change. `delta_pct` is reported
only when the previous value was **100 or more in absolute value**: under a
hundred, one unit is more than one percent, so the percentage says less than
the number it came from and rounds to noise. that also disposes of a previous
value of zero, which is the division that would otherwise have gone wrong.
percentages are rounded to two decimals; whole numbers stay whole in the json.

**what has no delta.** a key that is new, a key the previous build did not
report, and a key that was something other than a number last time. those are
absent from `deltas` entirely rather than carrying a zero: "did not move" and
"nothing to compare against" are different facts, and only one of them is
information. the [numeric types](#units-are-display-types-over-one-number) all
compare against each other, so an op that starts reporting a size as
`Meta::bytes` instead of `Meta::Int` keeps its history.

**it is computed server-side**, in the run and history endpoints, so
rendering a row never costs the ui a second request — and the number the ui
prints is the number the api computed, not one the ui derived.

**the ui shows one of the two.** a size or a duration reads as the percentage
(`1.2 GB −4%`), a count or a plain number as itself (`1,240 +37`), and
whichever is not shown is on the hover. the delta is muted, sits after the
value, and always carries a sign — `+`, `−`, or `±` for a value that was
measured and did not move. no colour: the ui is monochrome, and colour alone
would be the wrong way to say it anyway.

## Trends

a delta is one step back. the endpoints below are the rest of the line:

```
GET /api/assets/{name}/metadata/{key}?limit=&partition=
GET /api/jobs/{name}/ops/{op}/metadata/{key}?limit=
```

```json
{ "asset": "doc_stats", "key": "files",
  "points": [ {"at": "2026-08-08T10:01:36Z", "value": 16, "run_id": "019fe0b2-…"},
              {"at": "2026-08-08T11:01:36Z", "value": 18, "run_id": "019fe109-…"} ] }
```

**oldest first**, so the last point is the current value and the series reads
left to right the way it is drawn. `limit` (default 20, clamped to 1..=200) is
how many **builds or runs** are read, not how many points come back: a build
that did not report the key, or reported it as something that is not a number,
contributes nothing rather than a gap or a zero. `partition` narrows an asset
to one key; without it every key's builds interleave by time, which is a trend
of the asset rather than of any one partition.

the ui draws it as a sparkline under the value, in the asset panel and the op
inspector, **only once there are three or more points**. two points are a
delta, which the row already says, and one is the value itself.

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
  reported none, and `deltas` beside it.
- `GET /api/assets/{name}/history` — each entry has `metadata` and `deltas`.
- `GET /api/assets/{name}/metadata/{key}` and
  `GET /api/jobs/{name}/ops/{op}/metadata/{key}` — one numeric key over
  recent history.
- the run page renders the selected op's metadata by type: numbers
  right-aligned and tabular in their unit, urls as links, runs and assets as
  links into the ui, paths monospace, tables as tables, text inline, markdown
  rendered as [the subset above](#the-markdown-subset), json in a muted
  preformatted block.
- the asset detail panel renders each build's metadata under its history
  entry, the same way, with a sparkline under the newest one's numbers.
- the op inspector on the job page shows the newest facts that op reported
  across the window it is already summarising, with the same sparklines.
