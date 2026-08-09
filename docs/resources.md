# Resources

a *resource* is a value built once at startup and shared by every op that asks
for it: an http client, a connection pool, a parsed config. it is what
replaces capturing a client in a closure.

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

capturing works right up until it doesn't. two ops that each capture their own
client are two clients, and two connection pools, and two sets of credentials
read from the environment at slightly different moments. a client that is
expensive to build gets built per op, or gets built once and then threaded
through every closure by hand. and nothing anywhere can say what the process
actually holds.

a resource is one value, named, built once, and reportable.

## Building them

the constructor is **async and fallible**, which is the point — most real
clients need a handshake, a file read, or an environment variable that might
not be there:

```rust
.resource("db", |_| async {
    let url = std::env::var("DATABASE_URL")?;
    Ok(Pool::connect(&url).await?)
})
```

resources are built during `Hestan::build`, **before the store is opened**. a
constructor that returns `Err` aborts startup with
`Error::Resource { name, reason }`, and no database file is created: a process
whose api client could not be built has nothing useful to serve, and should
not leave a run log behind implying otherwise.

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

`ctx.resource::<T>(name)` returns `Arc<T>` — the same `Arc` for every op in
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

ops may also just ask without declaring, and that works — `ctx.resource` is
the same call either way. declaring is how you find out at startup instead of
at 3am, which is the same bargain [`Op::pool`](concepts.md#concurrency-pools)
offers.

## Lifetime

resources live for the process. there is **no per-run scoping and no teardown
hook** in this phase: nothing is rebuilt between runs, and nothing is closed
when the process exits beyond whatever `Drop` the value itself does. anything
that needs a fresh value per run, or a deliberate shutdown, should own it
inside the op instead.

## Seeing what exists

`GET /api/resources` lists them — names and declared types, never values:

```json
{ "resources": [ { "name": "api", "type": "demo::ApiClient" } ] }
```

a resource is usually a client holding credentials, so the api has no business
showing what is inside one. `GET /api/jobs/{name}` reports each op's
`requires`, and the op inspector shows it beside the pool and timeout.
