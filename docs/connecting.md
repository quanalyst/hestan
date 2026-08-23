# Connecting to your data

an op is an async fn in your binary. connecting to postgres, snowflake, s3 or
somebody's rest api is `cargo add` and then whatever that crate's client does:
there is no hestan adapter to find, no plugin to install, and no configuration
file that has to name your warehouse.

that is the whole shape of it, and this page is the details: where the client
should live, where the credential should come from, what to do about a flaky
endpoint, and where the [io managers](io-managers.md) fit.

## hestan does not wrap database clients, and will not

there is no `SnowflakeResource`, no `PostgresOp`, no `S3Io`. that is a
decision, not a gap.

a wrapper around somebody else's client is a layer that can only lose: it
carries a subset of the features, a version behind, with its own bugs and its
own docs, and the day you need the one option it does not expose you are
reading two apis instead of one. rust's ecosystem is crates, and an op is a
place to call one.

what hestan owns is the part a client cannot do for you: **when** the work
runs, what happened when it did, what it produced, what to do when it fails,
and what is downstream of it. so this page is about the seam, not about sql.

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

a connection pool built inside an op is a pool per op invocation, which is a
pool per five minutes, forever. build it once as a
[resource](resources.md) instead:

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

**that example is a doctest.** it lives on
[`Hestan::resource`](https://docs.rs/hestan/latest/hestan/struct.Hestan.html#method.resource),
it connects to a real postgres when the test suite is given one in
`HESTAN_TEST_PG`, and a test holds this page to it character for character. a
docs example that has never been compiled is a guess, and a page nobody runs is
the page that tells you to call a method that was renamed two releases ago.

four things in it are the point:

- `ctx.resource::<Client>("warehouse")` hands every op the **same** client. no
  reconnection per run, no credential read twice, and `Arc::ptr_eq` on two
  ops' handles holds.
- `requires(["warehouse"])` makes a missing resource a **startup** error
  naming the op, rather than an op that fails at 3am.
- `ctx.meta("rows", ..)` puts the row count on the run page and in the
  [trend](metadata.md) beside every other build of it. the query already knew
  the number; this is what costs you nothing to record.
- `retries(3)` is the flaky-endpoint policy, and the next section is about
  what it does and does not cover.

`tokio_postgres` is that crate's, not hestan's. swap it for `sqlx`, `mysql`,
`rusoto`, `aws-sdk-s3`, `duckdb` or a client you wrote this morning: the seam
is the same, and hestan has an opinion about none of them.

## Secrets come from the environment

read credentials in the resource constructor, from the process environment or
from whatever secret manager your deployment has. **not from run params.**

params are stored on the run row and served by the api and the ui, which is
right for `{"day": "2026-08-11"}` and wrong for a password: it would be in the
run log, in every launch that copied that run, and in whatever your log
aggregator keeps. a run's params are a thing anyone who can read the run can
read.

**this is still the advice**, and it is not softened by
[`Op::secret_params`](secrets.md), which is for the case a resource cannot
cover: a credential that belongs to *one launch* rather than to the process, a
deploy token a ci pipeline hands over per run. that keeps the value out of the
store and costs the run its replay: a run launched with one cannot be replayed,
resumed or retried, because the value was never written down. a resource costs
nothing and is rebuilt on every replay, so it is the answer whenever the
credential is the deployment's rather than the launch's.

a constructor that returns `Err` aborts startup with `Error::Resource` before
the store is opened, so a deployment with a missing `WAREHOUSE_URL` fails at
boot with the name of the resource rather than serving a ui over a database it
cannot reach.

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

if the thing you are connecting to is dbt, it has a dag of its own and hestan
can read it rather than being told about it: [dbt](dbt.md) turns
`target/manifest.json` into one asset per model with dbt's own lineage. that
is the one case a wrapper genuinely buys something a client call cannot.

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
