# Op state

per-op persisted state is the incremental-pull primitive: an op reads the
value its last successful execution committed, does its work, and stages a
new value that is written only if the attempt succeeds. the usual shape is a
watermark ("i have everything through id 81234"), so each run fetches only
what is new instead of re-pulling history.

state is one json value per `(job, op)` pair, keyed by name rather than by
run, so it survives restarts and outlives any particular run. it is not an
inter-op channel: an op sees its own state only, never another op's. pass
data between ops through outputs and `.after`.

## Reading and staging

```rust
Op::new("pull", |ctx| async move {
    let since = ctx.state_as::<i64>()?.unwrap_or(0); // typed
    let raw = ctx.state();                           // or the raw Option<&Value>
    // ... fetch rows after `since` ...
    ctx.set_state(json!(new_high_water));            // staged, not yet written
    Ok(json!({ "rows": count }))
})
```

`state()` is loaded once per op execution, before the first attempt: retries
within one run all see the same starting value. `state_as::<T>()`
deserializes it: `Ok(None)` when the op has never committed state,
`InputError::Mismatch` when the stored value no longer fits `T` (say, after
you changed the type; clear the row or handle both shapes).

`set_state` stages the value in a buffer that lives for one attempt; the
last call wins. the executor commits it only when the attempt succeeds. a
failed attempt's staged value is dropped entirely: attempt 2, and the next
run, still read the old watermark. succeeding without calling `set_state`
leaves existing state untouched.

## At-least-once, by construction

on success the executor writes the op's result row first and the state
second, in that order deliberately. a crash between the two leaves a
recorded success with the *old* watermark, so the next run re-fetches that
window. the reverse order would advance the watermark past work whose
success was never recorded: rows silently skipped. hestan picks re-do over
skip: a state-driven op sees each window at least once, so whatever it
writes downstream should be idempotent within a window (upserts keyed on id,
not blind appends).

## A fetch-since-cursor op

```rust
Op::new("pull_orders", |ctx| async move {
    let since = ctx.state_as::<i64>()?.unwrap_or(0);
    let orders = api::orders_after(since).await?;
    ctx.info(format!("{} orders after id {since}", orders.len()));
    // no new rows: skip set_state and keep the old cursor
    if let Some(newest) = orders.iter().map(|o| o.id).max() {
        ctx.set_state(json!(newest));
    }
    Ok(serde_json::to_value(orders)?)
})
```

the first run sees no state and pulls from 0; every later run pulls from the
highest id it has successfully processed. if the op fails mid-run, nothing
was committed and the next run repeats the same window.

## Reading it back

`runner.store().op_state(job, op)` returns one committed value, and
`GET /api/jobs/{name}/state` lists everything the job carries:

```json
{ "states": [
  { "op": "pull_orders", "value": 81234, "updated_at": "2026-08-07T12:00:03Z" }
] }
```

404 for an unknown job; a known job whose ops never committed anything gets
an empty list. rows live in the `op_state` table. see
[storage](storage.md).
