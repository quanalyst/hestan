use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use hestan::prelude::*;
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
    tracing_subscriber::fmt().init();

    let flaky = Arc::new(AtomicU32::new(0));

    let orders = Job::builder("orders_etl")
        .description("pull orders, clean them, publish aggregates")
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
            Ok(json!(rows))
        })
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
        .retries(2))
        .build()?;

    let health = Job::builder("warehouse_healthcheck")
        .description("ping the warehouse and report")
        .op(Op::new("ping", |_ctx| async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(json!({"latency_ms": 50}))
        }))
        .op(Op::new("report", |ctx| async move {
            let ping = ctx.input("ping").cloned().unwrap_or_default();
            ctx.info(format!("warehouse ok, ping {}ms", ping["latency_ms"]));
            Ok(json!({"ok": true}))
        })
        .after(["ping"]))
        .build()?;

    println!("hestan demo ui: http://127.0.0.1:4000");
    Hestan::new()
        .job(orders)
        .job(health)
        .schedule("orders_etl", "*/2 * * * *")
        .schedule("warehouse_healthcheck", "*/5 * * * *")
        .db("demo.db")
        .serve(([127, 0, 0, 1], 4000))
        .await
}
