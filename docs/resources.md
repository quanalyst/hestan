# Resources

Resources are named values supplied to operations: clients, connection pools,
configuration or temporary directories.

| Declaration | Created | Released |
| --- | --- | --- |
| `Hestan::resource` | At startup | With the process's resource registry |
| `Hestan::run_resource` | When a run starts | When the run ends, subject to retained `Arc` handles |

Read either scope with `ctx.resource::<T>(name)` and declare requirements with
`Op::requires([name])`.

```rust
Hestan::new()
    .resource("api", |_| async { Ok(ApiClient::new()?) })
    .job(Job::builder("pull")
        .op(Op::new("query", |ctx| async move {
            let api = ctx.resource::<ApiClient>("api")?;   // Arc<ApiClient>
            Ok(json!(api.get("/orders").await?))
        })
        .requires(["api"]))
        .build()?)
```

## Why not a closure

Closures can capture clients directly. Named resources additionally provide
shared construction, startup validation and an inspectable list of types and
scopes. Use a process resource for connections shared across runs.

## Building them

Constructors are async and fallible:

```rust
.resource("db", |_| async {
    let url = std::env::var("DATABASE_URL")?;
    Ok(Pool::connect(&url).await?)
})
```

resources are built during `Hestan::build`, **before the store is opened**. a
constructor that returns `Err` aborts startup with
`Error::Resource { name, reason }`.

they are built in declaration order, and each constructor is handed a
`ResourceCtx` holding the ones declared before it. that is how a client leans
on the config it reads:

```rust
Hestan::new()
    .resource("config", |_| async { Ok(Config::from_env()?) })
    .resource("api", |ctx| async move {
        let config = ctx.resource::<Config>("config")?;
        Ok(ApiClient::new(&config.base_url)?)
    })
```

asking for a resource declared *later* is `no resource named ...`, not a
deadlock. declaring one name twice is `Error::Resource`.

## Reading them

`ctx.resource::<T>(name)` returns `Arc<T>`: the same `Arc` for every op in
every job, so `Arc::ptr_eq` on two ops' handles holds. the error says which of
the two things went wrong:

```
no resource named api
resource api is a demo::Config, not a demo::ApiClient
```

`T` must be exactly the type the constructor returned. a resource stored as
`Config` cannot be read as `Arc<Config>` or as a trait object; if you want a
trait object, build one: `.resource("store", |_| async { Ok(Box::new(S3) as Box<dyn Blob>) })`.

## Declaring what you need

`Op::requires(["api"])` declares the dependency. the build then refuses a job
whose op names a resource nobody registered:

```
invalid job graph: job pull: op query requires resource api, which is not registered
```

ops may also just ask without declaring, and that works: `ctx.resource` is
the same call either way. declaring is how you find out at startup instead of
at 3am, which is the same bargain [`Op::pool`](concepts.md#concurrency-pools)
offers.

## Run-scoped resources

`Hestan::run_resource` builds a value per run and drops it when the run ends:

```rust
Hestan::new()
    .run_resource("scratch", |ctx| async move {
        // the run it belongs to, so what it leaves behind can be traced
        Ok(Scratch::under(ctx.run_id().unwrap())?)
    })
```

`ResourceCtx::run_id` is `Some` here and `None` in a process-wide constructor,
which is built before any run exists. run-scoped constructors run in
declaration order after the process-wide ones, so one can lean on either.

**what it costs.** the constructor runs for every run. a connection pool built
this way is a pool per run (a hundred pools on a busy afternoon, each with its
own connections, each dropped an hour later), which is almost always a mistake
and is the reason the two scopes have different names. build the pool with
`resource` and put the run's own short-lived thing in `run_resource`.

a constructor that fails fails the run before any op of it runs:

```
resource scratch: no space left on device
```

on the run row, with every op of the run recorded `skipped` and saying so.
nothing else could have been true of an op that needed it.

a name used by both scopes is `Error::Resource` at build, so
`ctx.resource("x")` never means two things. asking for a run-scoped name
outside a run (from a sensor, say) is `no resource named x`: nothing built
it, because there was no run for it to belong to.

### When it is dropped

Run resources are released when execution ends, including failure and
cancellation. Hestan schedules cleanup on the blocking pool; if the runtime is
already shutting down, normal value destruction applies.

An operation retaining an `Arc` beyond the run extends the resource lifetime;
its final drop occurs wherever that last handle is released. Isolated
operations construct their own resources in the child process.

## Lifetime

a process-wide resource lives for the process: nothing is rebuilt between runs,
and nothing is closed when the process exits beyond whatever `Drop` the value
itself does.

## Seeing what exists

`GET /api/resources` lists them (names, declared types and scopes, never
values):

```json
{ "resources": [
  { "name": "api",     "type": "demo::ApiClient", "scope": "process" },
  { "name": "scratch", "type": "demo::Scratch",   "scope": "run" }
] }
```

a run-scoped one is reported as *declared* rather than as built: nothing of it
exists between runs, and its type is the one its constructor was written to
return.

a resource is usually a client holding credentials, so the api has no business
showing what is inside one. `GET /api/jobs/{name}` reports each op's
`requires`, and the op inspector shows it beside the pool and timeout.

Cancellation also interrupts pending run-resource construction and releases the
resources already constructed for that run. A constructor panic fails the run.
