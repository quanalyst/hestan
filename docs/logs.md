# Logs

Run pages show [structured events](events.md) and captured operation output.
Use events to follow execution state and captured logs to diagnose operation
behavior.

| Operation | Captured output | Setup |
| --- | --- | --- |
| [Isolated operation](isolation.md) | stdout and stderr | Automatic |
| In-process operation | `tracing` events | Enable `capture` and install its layer |
| Subprocess started by Hestan, including dbt | stdout and stderr | Automatic |

All capture is subject to the limits below.

## Why an in-process `println!` is not captured

In-process stdout belongs to the whole application and cannot be attributed
reliably to concurrent operations. `println!` therefore goes to the application's
stdout. Use `tracing` with the capture layer, or `ctx.info`, `ctx.warn` and
`ctx.error` for structured events.

## Subprocess capture

Hestan drains a child's stdout and stderr concurrently and stores lines under
the operation attempt. Order within each stream is preserved; interleaving
reflects arrival order. Partial final lines survive process exit or termination.
A retry gets a separate attempt and log budget. A silent child creates no log
rows.

## The tracing layer

opt in, behind the `capture` feature:

```toml
hestan = { version = "0.2.5", features = ["capture"] }
```

hestan does not install a subscriber (that is yours), so what it offers is a
layer you compose into the one you were going to build anyway:

```rust
use tracing_subscriber::prelude::*;

let store = hestan::Store::open("hestan.db")?;
tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(hestan::capture_layer(&store))
    .init();
```

your own logging is untouched. the layer stores an event only when the span it
was emitted inside carries hestan's `run_id`, `op` and `attempt`: a span only
the executor opens, around an op body, entered across every await. an event
from your http handler, your startup, or a background task of your own reaches
the layer and is ignored.

the level, the target and the message are stored, with any other fields after
the message: `tracing::info!(rows = 12, "loaded")` is stored as
`loaded rows=12`. hestan's run log has three levels, so `TRACE` and `DEBUG`
arrive as `info`; the target says the rest. filtering is yours as it is for
any layer: `.with_filter(LevelFilter::INFO)` and hestan stores what survives.

**an event from a task the op spawned is not captured.** `tokio::spawn` does
not carry the current span into the new task, so an event emitted there has no
op to belong to. this is a real edge and worth knowing before you go looking
for a line that never arrives:

```rust
// captured
tracing::info!("what the op is doing");

// not captured: no span went with it
tokio::spawn(async { tracing::info!("from somewhere else entirely") });

// captured: the span went with it
tokio::spawn(async { tracing::info!("in the op's span") }.instrument(tracing::Span::current()));
```

spans the op opens *itself* are fine: the layer walks outward from the event
to the first attempt it finds, so an op's own `info_span!` nests inside its
attempt rather than hiding it.

## Caps

| Limit | Default |
| --- | --- |
| `Hestan::log_limit(bytes)` | 1 MiB per attempt |
| `Hestan::log_lines(n)` | 10,000 lines per attempt |
| Single line | 8 KiB, clipped with `… [truncated]` |

stdout and stderr share an attempt's budget. Once a cap is reached, capture
records a truncation notice and stops storing further output. The operation
continues and pipes are still drained. Each retry starts with a fresh budget.

The tracing layer uses a bounded writer buffer so emitting threads do not wait
on database writes. Overflow is dropped and counted in a notice. Capture limits
apply to both subprocess output and the tracing layer.

## Reading it

the run page's log pane has a source filter: `events`, `output`, or both
interleaved by time, which is the default. the level and op filters work
across both. a captured line shows its op, its attempt once there has been
more than one, and its stream or level. a line hestan wrote about the capture
itself ("capture stopped: this attempt reached its cap") is set apart, since
that is hestan speaking and not the op.

a line off a pipe has no level, so a level filter hides it rather than
inventing one. plenty of ordinary programs write their progress to stderr, and
"stderr means error" would be a guess.

over http:

```
GET /api/runs/{id}/logs?op=&after=&limit=
GET /api/runs/{id}/logs/download?op=
```

the first is cursored on `id` exactly as the events endpoint is cursored on
`seq`: oldest first, `after` is the last id you saw, default 500 lines and at
most 2000. the second is `text/plain`, one line per line, because at some
point everyone wants to grep it, which is also what
[`hestan logs <run>`](cli.md) reads, with `--follow` to stay on it:

```
2026-08-08T10:00:01.412Z load #1 stdout connecting to the warehouse
2026-08-08T10:00:02.008Z load #1 stderr timed out, retrying
2026-08-08T10:00:04.114Z load #2 warn retrying with a longer deadline
```

## Where it lives

Captured lines live in `op_logs`. Pipe output has a `stream`; tracing output has
`level` and `target`. [Retention](storage.md#retention) removes logs with their
run. Reclaiming a run preserves logs from its previous execution attempts.
