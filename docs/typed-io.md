# Typed io

ops speak json at the boundaries: every output is a `serde_json::Value` in the
run history. `Op::typed` puts serde structs over that boundary so the compiler
checks your wiring — and a shape mismatch at run time fails the attempt with a
`type check failed` error in the run log.

## Op::typed

```rust
use serde::{Deserialize, Serialize};
use hestan::prelude::*;

#[derive(Serialize, Deserialize)]
struct Extract { rows: Vec<u32> }

#[derive(Deserialize)]
struct TotalIn { extract: Extract }   // one field per dep, named after it

#[derive(Serialize)]
struct Total { total: u32 }

let job = Job::builder("typed")
    .op(Op::new("extract", |_| async { Ok(json!({"rows": [1, 2, 3]})) }))
    .op(Op::typed("total", |_ctx: OpCtx, input: TotalIn| async move {
        Ok(Total { total: input.extract.rows.iter().sum() })
    })
    .after(["extract"]))
    .build()?;
```

the input type `I` is deserialized from a json object with **one entry per
declared dep, keyed by the dep's op name** — so the field names of `TotalIn`
must match the upstream op names (or map to them via `#[serde(rename)]`). the
return type `O` is serialized back to json and recorded as the op's output.

serde's defaults give you slack in both directions: a dep you declared purely
for ordering can be left out of the struct (unknown fields are ignored), and
a dep whose output may be json null can map to an `Option<T>` field.

## OpResult and the untyped accessors

untyped ops return `OpResult`:

```rust
pub type OpResult = Result<Value, Box<dyn std::error::Error + Send + Sync>>;
```

a typed op's closure returns `Result<O, Box<dyn Error + Send + Sync>>` and
hestan does the serialization. inside any op, `OpCtx` offers both raw and
typed access:

```rust
ctx.input("extract")                       // Option<&Value>
ctx.input_as::<Vec<Order>>("extract")?     // Result<T, InputError>
ctx.params()                               // &Value
ctx.params_as::<MyParams>()?               // Result<P, InputError>
```

`InputError::Missing` means no such declared dep produced output;
`InputError::Mismatch` wraps the serde error. both convert into an op failure
via `?`.

## Gradual typing

typed and untyped ops mix freely in one job. a common shape: raw `Op::new`
ops at the edges where payloads are still fluid, `Op::typed` at the joins
where several branches meet and shape bugs hurt most —
`examples/demo.rs` does exactly this (`fetch_orders`/`enrich` untyped,
`aggregate`/`publish` typed). there is no migration step; tighten one op at a
time.

## Params validation

`.params::<P>()` declares the params type an op expects:

```rust
#[derive(Deserialize)]
struct FetchParams { limit: Option<usize> }

Op::new("fetch", |ctx| async move {
    let limit = ctx.params_as::<FetchParams>()?.limit.unwrap_or(6);
    ...
})
.params::<FetchParams>()
```

the check runs **before the run exists**: a launch whose params don't
deserialize into `P` is rejected with `Error::InvalidParams` (http 400 from
the launch and retry endpoints) and writes nothing to the database — no run
row, no events, zero traces. every op that declared a params type is checked
against the run's params, and the error names the op that rejected them.

## Type names in the api and ui

hestan records `std::any::type_name` for each declared type — module-qualified
strings like `pipeline::Total`. they appear as `input_type`, `output_type`,
and `params_type` on each op in the jobs api, in the ui's op inspector and
ops list (`aggregate -> demo::Summary`), and in the params editor's
`params_type` hint. a typed op's `op_success` event carries
`data: {"output_type": "..."}`.

## When the shape doesn't match

if upstream output fails to deserialize into `I`, the attempt fails with an
error starting `type check failed:` and emits a `type_check_failed` event
(level `error`, `data: {"error": "<serde message>"}`). the failure then goes
through the **normal retry policy** — with `.retries(n)` it will be retried
like any other error. inputs don't change between attempts, so retrying a
genuine shape mismatch just burns the attempts; the retry behavior exists so a
type failure isn't a special case in the executor. (the http source's
`expect_json` reuses the same event but fails fast — see
[http sources](http-sources.md).)
