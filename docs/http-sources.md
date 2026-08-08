# Http sources

`HttpSource` is a declarative GET pull that lowers into ordinary ops — the
scheduled REST poll you'd otherwise hand-write, as one block. it lives behind
the `http` cargo feature:

```toml
hestan = { version = "0.1.0-alpha.1", features = ["http"] }
```

the one-block form registers a job named after the source, plus a schedule
when `cron` is set:

```rust
use hestan::{Hestan, HttpSource};

Hestan::new()
    .source(
        HttpSource::get("https://api.coingecko.com/api/v3/simple/price")
            .name("btc_spot")
            .query("ids", "bitcoin")
            .query("vs_currencies", "usd")
            .cron("*/5 * * * *"),
    )
    .serve(([127, 0, 0, 1], 4000))
    .await
```

## Builder reference

`HttpSource::get(url)` is the only constructor; GET is the only method in v1.

| method | default | effect |
| --- | --- | --- |
| `.name(n)` | — | required. names the op, and the job in the `source` form |
| `.header(k, v)` | — | adds a request header (`k` is `&'static str`); repeatable |
| `.bearer_env(var)` | — | sends `authorization: Bearer <token>`, token read from the env var at request time |
| `.query(k, v)` | — | adds a query parameter; repeatable |
| `.query_each(k, vals)` | — | fans out into one op per value, each sending its own `k=value`; a second call replaces the first |
| `.expect_json::<T>()` | raw json | declares the response shape; output becomes `T` reserialized |
| `.cron(expr)` | none | 5-field cron in utc (see [scheduling](scheduling.md)) |
| `.cron_tz(expr, tz)` | none | same, evaluated in a named iana timezone |
| `.retries(n)` | 2 | extra attempts inside the request loop |
| `.retry_delay(d)` | 1s | backoff base: the nth retry waits `d * 2^n`, capped at 30s |
| `.max_parallel(n)` | unlimited | cap concurrent requests when fanning out; below 1 means 1 |
| `.overlap(o)` | skip | overlap policy for the generated job (see [scheduling](scheduling.md)) |
| `.timeout(d)` | 30s | per-request timeout |

## Fan-out naming

`query_each("ids", ["bitcoin", "ethereum"])` produces one op per value, named
`{name}_{value}` with the value sanitized: ascii-lowercased, every character
outside `[a-z0-9_]` replaced by `_`. so a source named `region` with
`query_each("r", ["EMEA", "us-east"])` yields ops `region_emea` and
`region_us_east`. plain `.query` pairs are sent by every fan-out op alongside
its own value.

sanitization can collide: `us-east` and `us_east` both become `us_east`.
`into_job` (and therefore `Hestan::source`) rejects that with an error naming
both raw values and the op name they fought over. an empty values list is
rejected too — it would lower into a job with no ops that always ran green.

## Retry policy

the request loop owns retrying, because it knows which failures are worth
another attempt:

- transport errors (connect failures, timeouts, body-read errors), 429, and
  any 5xx are retried, up to `.retries(n)` extra attempts.
- any other non-2xx fails the op immediately — a 404 or 403 never improves.
- a body that isn't valid json fails immediately, as does a missing or empty
  `bearer_env` variable, or a request that can't be built at all (a header
  value with a newline, say) — deterministic failures that no retry fixes.

the nth retry waits `retry_delay * 2^n`, capped at 30 seconds. on 429 and 503
a numeric `Retry-After` header (seconds; http-date form is ignored) is
honored when it's longer than the computed backoff, capped at 5 minutes —
servers ask for absurd waits often enough. each retry logs a warn event
(`503 Service Unavailable, retrying in 2s`); a successful pull logs
`200 OK, 1024 bytes`. the failure error is
`{status} from {url}: {first 200 chars of the body}`; transport and build
errors carry their full cause chain.

because the loop retries internally, the lowered op itself has `retries` 0 —
the op run's `attempts` column reads 1 even when the request was retried. the
retry history is in the warn events.

## expect_json

```rust
#[derive(Serialize, Deserialize)]
struct Payload { ok: bool, n: u32 }

HttpSource::get(url).name("pull").expect_json::<Payload>()
```

the body must deserialize into `T`; the recorded output is `T` serialized
back, so fields `T` doesn't declare are dropped from the stored output. a
mismatch emits the same `type_check_failed` event as `Op::typed` — but here
the failure is fatal to the op, not retried: the server said 200 and the
shape is wrong, so another request won't fix it. `expect_json` also sets the
op's `output_type` (e.g. `myapp::Payload`), which shows up in the api and ui.

## bearer_env

the token is read from the environment at request time, not at build time:
rotate the variable and the next pull uses the new value, no restart. a
missing or empty variable fails the op (fatal, not retried) — an empty
string would otherwise sail through as a blank `Bearer ` header.

## Lowering into jobs

three ways to use a source, from most to least packaged:

`Hestan::source(src)` registers a job named after the source (build fails
with `invalid job graph: http source needs a name` if `.name` was never
called) and attaches the `cron` schedule if one was set.

`src.into_job("name")` gives you the same single job to register yourself,
useful with `run_once` or a `Runner`.

`.cron` is consumed by the `source` form only: `into_job` and `into_ops`
lower the request alone, and any cron on the source is dropped. that's
deliberate — at those levels you own registration, so attach the schedule
yourself with `Hestan::schedule` (or don't). `into_job` logs a warning when
it drops one, so a schedule that silently never fires can't slip through.

`src.into_ops()` returns the raw `Vec<Op>` to mix with hand-written ops in
your own builder. `examples/http_source.rs` does this: three fan-out pulls
plus a typed join, where the input struct's fields are named after the
fan-out ops —

```rust
let prices = HttpSource::get("https://api.coingecko.com/api/v3/simple/price")
    .name("price")
    .query("vs_currencies", "usd")
    .query_each("ids", ["bitcoin", "ethereum", "solana"])
    .expect_json::<Prices>();

let mut builder = Job::builder("crypto_prices");
for op in prices.into_ops() {
    builder = builder.op(op);
}
let job = builder
    .op(Op::typed("summarize", |ctx: OpCtx, input: SummarizeIn| async move {
        // input.price_bitcoin, input.price_ethereum, input.price_solana
        ...
    })
    .after(["price_bitcoin", "price_ethereum", "price_solana"]))
    .build()?;
```
