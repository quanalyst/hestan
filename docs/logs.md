# Logs

there are two logs on a run page and they are not the same thing.

the **run log** is hestan narrating: `run_started`, `op_retry`,
`type_check_failed`, and whatever `ctx.info/warn/error` said. it is
[events](events.md), it is structured, and it is what the run page
has always shown.

**captured output** is what the op itself produced — the `println!` of a
subprocess, the `tracing` events a library it called emitted. hestan used to
drop all of it, which meant every op that calls a real library was half
invisible: the run log said "attempt 1 failed", and the reason was on a
terminal nobody was watching.

the obvious answer is to scrape a step's stdout and stderr into a text blob.
hestan does better than that in one case and refuses to do worse in the other,
and the split is the whole design:

| the op                                  | what is captured                              | how                                     |
| --------------------------------------- | --------------------------------------------- | --------------------------------------- |
| [isolated](isolation.md) (a subprocess)  | **everything**, verbatim, both pipes           | always on, no configuration             |
| in process                               | its `tracing` events, with level and target    | the `capture` feature's layer, opt in    |
| a process hestan itself spawned          | **everything**, verbatim, both pipes           | always on — a [dbt](dbt.md) model's `dbt run` is one |

## Why an in-process `println!` is not captured

fd 1 belongs to the process, not to the op. hestan is a library inside your
binary: redirecting stdout process-wide to catch an op's `println!` would take
*your* application's output with it — your startup banner, your web server's
access log, anything else running in that process — and hand it to whichever
op happened to be running at the time. a library has no business doing that,
so hestan does not.

that is an honest limit and it is stated here rather than discovered later:
**`println!` inside an in-process op goes to your stdout and nowhere else.**

what an in-process op does emit that hestan can capture is `tracing` events,
and those are the better half of the trade anyway. an event carries a level, a
target, fields and a message — structured records rather than a scraped blob,
so the pane can filter them by level and say which module they came from.

an isolated op is a subprocess whose stdout and stderr belong to hestan alone,
so there the answer is simply everything.

## Subprocess capture

nothing to switch on. an [isolated op](isolation.md)'s parent pipes the
child's stdout and stderr, reads both, tags each line with its stream, and
stores it under that attempt. so does anything else hestan starts a process
for — a [dbt](dbt.md) model's `dbt run --select` goes through the same reader,
under the same caps.

both pipes are drained **concurrently**, by a task each. this is not a
detail: reading stdout to its end and stderr afterwards leaves stderr's pipe
buffer to fill, and a child blocked writing into a full pipe never exits — the
parent then waits forever for a process waiting for the parent. it looks like
a slow op under load rather than like a bug.

- per-stream order is exact. the two streams interleave in the order the lines
  arrived, which is all a pipe can honestly tell you — hestan does not try to
  merge them by timestamp beyond that.
- a child that dies mid-line keeps what it wrote, and so does one that is
  killed or aborts without recording a result. the pipes are drained *after*
  the kill, because a pipe ends when the process holding the other side of it
  is gone. for a segfault, what the op printed is usually the only evidence
  there is.
- a child that printed nothing stores no rows at all, rather than a marker
  saying it was quiet.
- a retry is a fresh child and its output is stored under its own attempt, so
  what the attempt that failed printed is still there beside what the attempt
  that worked printed.

## The tracing layer

opt in, behind the `capture` feature:

```toml
hestan = { version = "0.1", features = ["capture"] }
```

hestan does not install a subscriber — that is yours — so what it offers is a
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
was emitted inside carries hestan's `run_id`, `op` and `attempt` — a span only
the executor opens, around an op body, entered across every await. an event
from your http handler, your startup, or a background task of your own reaches
the layer and is ignored.

the level, the target and the message are stored, with any other fields after
the message: `tracing::info!(rows = 12, "loaded")` is stored as
`loaded rows=12`. hestan's run log has three levels, so `TRACE` and `DEBUG`
arrive as `info` — the target says the rest. filtering is yours as it is for
any layer: `.with_filter(LevelFilter::INFO)` and hestan stores what survives.

**an event from a task the op spawned is not captured.** `tokio::spawn` does
not carry the current span into the new task, so an event emitted there has no
op to belong to. this is a real edge and worth knowing before you go looking
for a line that never arrives:

```rust
// captured
tracing::info!("what the op is doing");

// not captured — no span went with it
tokio::spawn(async { tracing::info!("from somewhere else entirely") });

// captured: the span went with it
tokio::spawn(async { tracing::info!("in the op's span") }.instrument(tracing::Span::current()));
```

spans the op opens *itself* are fine: the layer walks outward from the event
to the first attempt it finds, so an op's own `info_span!` nests inside its
attempt rather than hiding it.

## Caps

**capping is a correctness property, not a nicety.** an op in a `println!`
loop would otherwise fill the disk the run log lives on, and a run log that
ran out of room records nothing at all — including the failure you were trying
to read about.

| cap                        | default | what it is                                 |
| -------------------------- | ------- | ------------------------------------------ |
| `Hestan::log_limit(bytes)` | 1 MiB   | how much one **attempt** may store         |
| `Hestan::log_lines(n)`     | 10,000  | how many lines one attempt may store       |
| a single line              | 8 KiB   | clipped, with `… [truncated]` on the end   |

per attempt, not per op or per run: a retry starts from a full budget, since
the attempt that failed is usually the one worth reading. an isolated op's two
pipes share one attempt's budget, because the limit is on what the attempt
produced and not on which pipe it came out of.

past either cap, capture stops for that attempt and **one line** says what was
dropped and why. the op carries on running — capture stopping is not the op
failing, and the parent keeps reading the pipes so a chatty child never blocks
on one nobody is draining.

the caps apply to every capture in the process, the layer included. that
matters because you hand `capture_layer` a `Store` you opened yourself: a cap
that only covered the writers hestan happened to build would be a cap that
quietly does not hold.

the layer never makes the emitting thread wait on the store. events go into a
bounded buffer and a writer thread stores them; if an op fills the buffer
faster than it drains, the excess is **dropped and counted**, and one line
says how many. a gap that says it is a gap is worth something; one that does
not is worse than nothing.

## Reading it

the run page's log pane has a source filter — `events`, `output`, or both
interleaved by time, which is the default. the level and op filters work
across both. a captured line shows its op, its attempt once there has been
more than one, and its stream or level. a line hestan wrote about the capture
itself — "capture stopped: this attempt reached its cap" — is set apart, since
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
`seq` — oldest first, `after` is the last id you saw, default 500 lines and at
most 2000. the second is `text/plain`, one line per line, because at some
point everyone wants to grep it — which is also what
[`hestan logs <run>`](cli.md) reads, with `--follow` to stay on it:

```
2026-08-08T10:00:01.412Z load #1 stdout connecting to the warehouse
2026-08-08T10:00:02.008Z load #1 stderr timed out, retrying
2026-08-08T10:00:04.114Z load #2 warn retrying with a longer deadline
```

## Where it lives

one table, `op_logs`, with a row per line — see [storage](storage.md). the
`stream` column is `stdout`/`stderr` for subprocess capture and null for a
captured event; `level` and `target` are the other way round. exactly one half
is filled, and which half says where the line came from.

it is not `events`. a chatty op would otherwise bury the eight lines that
describe what the run actually did, and those two things deserve to be
readable apart.

[retention](storage.md#retention) takes captured output with its run. a
[reclaimed](scaling.md) run goes back to the queue rather than to a terminal
state, so what its first claimer captured is still there when the second one
finishes it.
