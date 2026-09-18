# Op state

Op state is one persisted JSON value per `(job, op)` pair. Use it for a
watermark or cursor that survives runs and restarts. Operations read only their
own state; pass data between operations through outputs and dependencies.

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

The executor writes the successful operation result before its new state.
A crash between these writes leaves the old cursor, so the next run may repeat
the same window. Make external writes idempotent, for example by upserting on
a stable key.

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
