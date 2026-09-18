# Connecting to your data

Call Rust client libraries from operations and share expensive clients through
[resources](resources.md). Hestan handles execution, dependencies and history;
your application controls external connections and credentials.

## hestan does not wrap database clients, and will not

Use the client library for the system you need. Hestan does not wrap database
or object-storage SDKs. [HTTP sources](http-sources.md) and [dbt](dbt.md) provide
specific scheduling and asset-registration conveniences.

## Connecting from an op

the direct version, for a client that is cheap to build or used once:

```rust
Op::new("pull", |_| async {
    let client = reqwest::Client::new();
    let orders: Vec<Order> = client
        .get("https://api.example.test/orders")
        .send()
        .await?
        .json()
        .await?;
    Ok(json!({ "orders": orders.len() }))
})
```

`?` works on anything that is an error: an op returns
`Result<Value, Box<dyn Error + Send + Sync>>`, so a `reqwest::Error`, a
`tokio_postgres::Error` and a `std::io::Error` all convert on their own, and
the message on the op run row is that error's own.

## A pool as a resource

Build a shared client or pool once as a [resource](resources.md):

<!-- worked-example -->
```rust
use hestan::prelude::*;
use tokio_postgres::{Client, NoTls};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let nightly = Job::builder("nightly")
        .op(Op::new("count_orders", |ctx: OpCtx| async move {
            let db = ctx.resource::<Client>("warehouse")?;
            let row = db.query_one("select count(*) from orders", &[]).await?;
            let rows: i64 = row.get(0);
            ctx.meta("rows", Meta::count(rows as u64));
            Ok(json!({ "rows": rows }))
        })
        .requires(["warehouse"])
        .retries(3))
        .build()?;

    let run = Hestan::new()
        // connected once, shared by every op, and never a param: params
        // are stored on the run and served over the api
        .resource("warehouse", |_| async {
            let url = std::env::var("WAREHOUSE_URL")?;
            let (client, driver) = tokio_postgres::connect(&url, NoTls).await?;
            // the driver owns the socket and has to be polled by
            // somebody; the client is the handle the ops share
            tokio::spawn(driver);
            Ok(client)
        })
        .job(nightly)
        .db(":memory:")
        .run_once("nightly", json!({}))
        .await?;

    assert_eq!(run.status, RunStatus::Success);
    Ok(())
}
```

The example is also tested in
[`Hestan::resource`](https://docs.rs/hestan/latest/hestan/struct.Hestan.html#method.resource).
Set `HESTAN_TEST_PG` to exercise it against PostgreSQL.

The example shares one client, validates its declaration with `requires`,
records a row count, and retries failed attempts. The client type and connection
configuration belong to your application.

## Secrets come from the environment

Load credentials in resource constructors from the environment or your secret
manager. Ordinary run parameters are persisted and exposed through the API.

For launch-specific credentials, [secret parameters](secrets.md) prevent the
specified values from being stored. Such runs cannot be retried, resumed or
replayed from their stored parameters. Resources avoid this restriction and are
constructed in the process executing the work.

A failing process-resource constructor aborts startup with `Error::Resource`
before the store opens.

## Retries, timeouts, and a flaky endpoint

three separate knobs, and it is worth being deliberate about which one a given
failure wants:

```rust
Op::new("pull", ..)
    .retries(3)                                          // extra attempts
    .retry_backoff(Duration::from_secs(2), Duration::from_secs(60))
    .timeout(Duration::from_secs(30))                     // per attempt
```

- **`retries`** counts attempts *after* the first. each one is a fresh call of
  your fn (a new client borrow, a new query), and each is recorded on the run
  page as its own attempt, with what it failed with.
- **`retry_backoff(base, max)`** doubles the pause between attempts up to
  `max`. an endpoint that is down because everyone is retrying at once is not
  helped by retrying at once.
- **`timeout`** is per attempt, and it is the one people forget. a tcp
  connection to a host that stopped answering does not fail; it waits. without
  a timeout an op like that occupies its slot until the process is restarted,
  and the run sits in `running` looking like work.

a client that is slow rather than broken should also be kept off everything
else: `Hestan::pool("warehouse", 4)` plus `Op::pool("warehouse")` is one budget
of concurrent work against one system, however many jobs happen to overlap;
see [concurrency pools](concepts.md#concurrency-pools).

**what is not retried:** anything that is not an `Err` from your fn. an op that
catches the failure and returns `Ok` succeeded, as far as hestan can tell.

## Where the io managers fit

an op returns a `Value`, and by default that value lands in the run log as
json. that is right for `{"rows": 12}` and wrong for the rows themselves.

- [`FileIo`](io-managers.md#fileio) writes each op's output as one json file
  and keeps a handle in `op_runs.output`.
- [`ParquetIo`](io-managers.md#parquetio) writes a table as one parquet file,
  which is the format this kind of work already uses. it records the row
  count and the file size as metadata without the op asking.

both are a directory of files, and neither is a data lake.
[retention](storage.md#retention) takes a pruned run's files with its rows,
so what grows there is the history your policy keeps, and with no policy
configured, all of it.

the other half of the answer is that the value does not have to travel through
hestan at all. an op that loads a table into your warehouse can return
`{"table": "analytics.orders_daily", "rows": 41_233}` and let the data stay
where it was written. the run log is for what happened, and a handle to the
result is usually more useful than the result.

## Another tool's dag

[dbt](dbt.md) imports model lineage from `target/manifest.json`, so models can
participate in the same asset graph as application-defined work.

## Where to go next

- [resources](resources.md): the two scopes, the ordering, and what
  `GET /api/resources` will and will not show. a pool belongs in the
  process-wide one: `Hestan::run_resource` builds per run, which for a pool
  means a pool per run.
- [io managers](io-managers.md): the trait, the handle, and both bundled
  managers.
- [isolation](isolation.md): an op that segfaults a native driver, in a
  process of its own.
- [http sources](http-sources.md): a scheduled rest pull with no op at all,
  for the case where the api *is* the pipeline.
