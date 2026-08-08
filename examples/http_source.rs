use std::collections::HashMap;

use hestan::HttpSource;
use hestan::prelude::*;
use serde::Deserialize;

// each fan-out op pulls one id, so its output is {coin: {currency: price}}
type Prices = HashMap<String, HashMap<String, f64>>;

#[derive(Deserialize)]
struct SummarizeIn {
    price_bitcoin: Prices,
    price_ethereum: Prices,
    price_solana: Prices,
}

#[tokio::main]
async fn main() -> Result<(), hestan::Error> {
    tracing_subscriber::fmt().init();

    let prices = HttpSource::get("https://api.coingecko.com/api/v3/simple/price")
        .name("price")
        .query("vs_currencies", "usd")
        .query_each("ids", ["bitcoin", "ethereum", "solana"])
        .expect_json::<Prices>();

    let mut builder =
        Job::builder("crypto_prices").description("coingecko spot prices, summarized");
    for op in prices.into_ops() {
        builder = builder.op(op);
    }
    let crypto = builder
        .op(
            Op::typed("summarize", |ctx: OpCtx, input: SummarizeIn| async move {
                let mut usd: HashMap<String, f64> = HashMap::new();
                for pulled in [
                    input.price_bitcoin,
                    input.price_ethereum,
                    input.price_solana,
                ] {
                    for (coin, quote) in pulled {
                        usd.insert(coin, quote.get("usd").copied().unwrap_or_default());
                    }
                }
                let mut parts: Vec<String> = usd
                    .iter()
                    .map(|(coin, p)| format!("{coin} ${p:.2}"))
                    .collect();
                parts.sort();
                ctx.info(parts.join(", "));
                Ok(usd)
            })
            .after(["price_bitcoin", "price_ethereum", "price_solana"]),
        )
        .build()?;

    println!("http source demo ui: http://127.0.0.1:4001");
    Hestan::new()
        .job(crypto)
        .schedule("crypto_prices", "*/5 * * * *")
        .db("http_source.db")
        .serve(([127, 0, 0, 1], 4001))
        .await
}
