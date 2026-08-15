use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hestan::prelude::*;
use hestan::{Auth, Role};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Order {
    id: u64,
    total: f64,
}

#[derive(Deserialize)]
struct FetchParams {
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct AggregateIn {
    validate: Vec<Order>,
    enrich: Vec<Value>,
}

#[derive(Serialize, Deserialize)]
struct Summary {
    orders: usize,
    revenue: f64,
    enriched: usize,
}

#[derive(Deserialize)]
struct PublishIn {
    aggregate: Summary,
}

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    // stderr, not stdout: this binary has a command line, and a command line's
    // stdout belongs to the answer. a log line landing in the middle of
    // `--json` output is a parse error in whatever was reading it.
    //
    // and filtered from RUST_LOG, because hestan traces the same events that
    // `run --wait` streams: serving, you want them, and waiting on one run you
    // already have them once. `RUST_LOG=warn` is the second copy turned off
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let flaky = Arc::new(AtomicU32::new(0));

    let orders = Job::builder("orders_etl")
        .description("pull orders, clean them, publish aggregates")
        // the one op here that stands in for a call to somebody else's api,
        // and the limit that api would publish: five a second, whichever job
        // is running. see docs/concepts.md#rates, and scaling.md before you
        // run two of these
        .op(Op::new("fetch_orders", |ctx| async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let limit = ctx.params_as::<FetchParams>()?.limit.unwrap_or(6);
            // two malformed rows for validate to drop
            let rows = vec![
                json!({"id": 1, "total": 42.5}),
                json!({"id": 2, "total": 17.0}),
                json!({"id": 3}),
                json!({"id": 4, "total": 99.9}),
                json!({"total": 5.0}),
                json!({"id": 5, "total": 12.25}),
            ];
            let rows: Vec<Value> = rows.into_iter().take(limit).collect();
            ctx.info(format!("fetched {} rows", rows.len()));
            ctx.meta("source", Meta::Url("https://example.test/orders".into()));
            ctx.meta("took", Duration::from_millis(400));
            Ok(json!(rows))
        })
        .timeout(Duration::from_secs(5))
        .rate("orders_api")
        .params::<FetchParams>())
        .op(Op::new("validate", |ctx| async move {
            let rows = ctx.input("fetch_orders").cloned().unwrap_or_default();
            let mut good: Vec<Order> = Vec::new();
            for row in rows.as_array().into_iter().flatten() {
                match serde_json::from_value(row.clone()) {
                    Ok(order) => good.push(order),
                    Err(_) => ctx.warn(format!("dropping bad row: {row}")),
                }
            }
            Ok(json!(good))
        })
        .after(["fetch_orders"]))
        // still raw Value: typed and untyped ops mix freely
        .op(Op::new("enrich", |ctx| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let rows = ctx.input("fetch_orders").cloned().unwrap_or_default();
            let rows: Vec<Value> = rows
                .as_array()
                .into_iter()
                .flatten()
                .map(|r| {
                    let mut r = r.clone();
                    r["region"] = json!("emea");
                    r
                })
                .collect();
            Ok(json!(rows))
        })
        .after(["fetch_orders"]))
        .op(
            Op::typed("aggregate", |ctx: OpCtx, input: AggregateIn| async move {
                let revenue: f64 = input.validate.iter().map(|o| o.total).sum();
                ctx.info(format!("aggregated {} orders", input.validate.len()));
                ctx.meta("orders", Meta::count(input.validate.len() as u64));
                ctx.meta("revenue", revenue);
                ctx.meta(
                    "dropped",
                    Meta::count(input.enrich.len().saturating_sub(input.validate.len()) as u64),
                );
                ctx.meta(
                    "sample",
                    Meta::table(
                        [("id", "int"), ("total", "float")],
                        input
                            .validate
                            .iter()
                            .take(5)
                            .map(|o| vec![json!(o.id), json!(o.total)]),
                    ),
                );
                ctx.meta(
                    "notes",
                    Meta::Markdown(
                        "rows are dropped when `id` or `total` is missing; see\n\
                         [metadata](https://github.com/quanalyst/hestan) for what\n\
                         *this* block is."
                            .into(),
                    ),
                );
                Ok(Summary {
                    orders: input.validate.len(),
                    revenue,
                    enriched: input.enrich.len(),
                })
            })
            .after(["validate", "enrich"]),
        )
        .op(Op::typed("publish", move |ctx: OpCtx, input: PublishIn| {
            let flaky = flaky.clone();
            async move {
                // even on the first attempt of every run, so each run retries once
                if flaky.fetch_add(1, Ordering::SeqCst).is_multiple_of(2) {
                    return Err("warehouse connection reset".into());
                }
                ctx.info("published");
                Ok(input.aggregate)
            }
        })
        .after(["aggregate"])
        // one warehouse, shared with the healthcheck job: the pool is the
        // budget for it, whichever job is running
        .pool("warehouse")
        .retries(2))
        .build()?;

    let health = Job::builder("warehouse_healthcheck")
        .description("ping the warehouse and report")
        .op(Op::new("ping", |_ctx| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(json!({"latency_ms": 50}))
        })
        .pool("warehouse"))
        .op(Op::new("report", |ctx| async move {
            let ping = ctx.input("ping").cloned().unwrap_or_default();
            ctx.info(format!("warehouse ok, ping {}ms", ping["latency_ms"]));
            Ok(json!({"ok": true}))
        })
        .after(["ping"]))
        .build()?;

    // the same registry whatever this process is here to do, which is the
    // constraint a split deployment is under, so the demo is under it too. see
    // docs/scaling.md and the compose file beside it.
    let app = Hestan::new()
        .job(orders)
        .job(health)
        .pool("warehouse", 1)
        .rate("orders_api", 5, Duration::from_secs(1))
        .schedule("orders_etl", "*/2 * * * *")
        .schedule("warehouse_healthcheck", "*/5 * * * *")
        .max_concurrent_runs(env_num("HESTAN_MAX_CONCURRENT_RUNS").unwrap_or(4))
        .slots(env_num("HESTAN_SLOTS").unwrap_or(2))
        .db(env("HESTAN_DB").unwrap_or_else(|| "demo.db".into()));

    // the compose file binds 0.0.0.0 and publishes the port, which `serve`
    // refuses to do unguarded, so it sets a token, and this is where a
    // deployment picks one up. from the environment rather than a literal: a
    // token in argv is a token in `ps` and a token in source is a token in git
    let app = match env("HESTAN_TOKEN") {
        Some(token) => app.auth(Auth::bearer(token)),
        None => app,
    };

    let addr: SocketAddr = env("HESTAN_ADDR")
        .unwrap_or_else(|| "127.0.0.1:4000".into())
        .parse()
        .expect("HESTAN_ADDR is host:port");
    match env("HESTAN_ROLE").as_deref() {
        // a worker still serves the ui here, because /api/health is where it
        // says which runs it is holding
        Some("worker") => app.work(Some(addr)).await,
        Some("scheduler") => app.role(Role::Scheduler).serve(addr).await,
        Some(other) => panic!("HESTAN_ROLE is scheduler, worker or unset, not {other}"),
        // the whole mount: with no arguments this is `app.serve(addr)` and the
        // demo is what it always was, and with any it is a command line over
        // the registry two hundred lines above
        None => hestan::cli::run(app, addr).await,
    }
}

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

fn env_num(key: &str) -> Option<usize> {
    env(key).map(|v| v.parse().unwrap_or_else(|_| panic!("{key} is a number")))
}
