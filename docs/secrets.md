# Secrets in params

a launch's params are run data. they go into `runs.params`, onto the run page,
into `GET /api/runs`, out of `hestan runs show`, and they stay there until
[retention](storage.md) prunes them. that is right for a date, a region or a
row limit and wrong for a deploy token.

so an op may say which of its params are credentials:

```rust
Op::new("push", |ctx: OpCtx| async move {
    let token = ctx.params()["token"].as_str().ok_or("no token")?;
    deploy(token).await
})
.secret_params(["token"])
```

the op still reads the value. what changes is that nothing writes it down.

```
$ curl -s localhost:4000/api/runs/019.../ | jq .params
{ "token": "[hestan:redacted]", "env": "prod", "wait": 30 }
```

## Where the redaction is

**in the store.** `Store` holds what each job declared and applies it to every
params column it writes, before the insert:

| column | written by |
| --- | --- |
| `runs.params` | every launch, retry, resume, replay, schedule fire, sensor fire and asset build |
| `schedules.params` | the sync that mirrors declared schedules into the store on start |
| `presets.params` | `Hestan::preset`, and the launchpad's save |

that is the whole of it, and it is deliberately not in the ui, the api or the
cli. a value scrubbed in a renderer is still in the database and still on
every other renderer, and the next reader somebody adds gets it for free. a
value the database never held cannot be read by a reader that does not exist
yet, by a route added next month, or by whoever has a `psql` prompt.

the ops get it because the process that took the launch keeps it in memory,
keyed by run id, and puts it back into the params of the run it is about to
execute. it goes to no disk on the way.

## What a secret means for a replay

**this is the sharp edge, and it is worth reading before you declare one.**

a [replay](replay.md), a resume and a retry all read a finished run's stored
params back and launch with them. the store holds `[hestan:redacted]` where
the token was. re-launching from that row would run the deploy with the
literal string `[hestan:redacted]` as its credential: a run that fails
confusingly at best, and one that authenticates as something unintended at
worst.

**so they are refused, and the refusal names the param:**

```
$ hestan replay 019...
error: job deploy: param token is declared secret and not stored, so what came
back is the marker and not the value. a retry, a resume or a replay cannot
re-read one: launch again and pass it
```

the api answers `409` with the same sentence; a resume or replay *preview*
refuses identically, so the ui never offers a button that cannot work.

the refusal is not four checks in four handlers. the marker is refused as a
param value at `Runner::enqueue`, the one funnel every run in hestan goes
through, so a launch path added later lands on it without anybody remembering
to add it.

**a run that carried a secret is therefore not re-runnable.** launching again
and passing the value is the way to re-run it, and the reason to accept that
is the same reason to declare the param at all: hestan is not a secret store,
and the credential is somewhere that can hand it over again.

if a job's re-runnability matters more than this, do not declare the param. put
the credential in a [resource](resources.md) instead: a resource is process
configuration, it is built where the op runs, it is never run data, and a
replay rebuilds it like any other.

## What is covered

| | |
| --- | --- |
| run params | replaced with the marker before the insert, on every launch path |
| schedule params | same, on the sync that writes them |
| preset params | same, so a preset cannot become a credential store |
| the event log | the queued, started, finished and op events read the row or carry no params at all |
| the ui and `GET /api/runs` | read the row, so there is nothing to redact |
| `hestan runs show`, `hestan runs`, the log tail | read the row |
| `hestan doctor` | reports no params, of any job, at all |
| `hestan run --dry-run` | never touches the store, so it applies the declaration itself: it prints the marker |
| the refusal a params check gives | scrubbed of this job's secret values before it is a message |

that last row is the classic leak and is worth spelling out. an op declaring
`.params::<Deploy>()` refuses a launch through serde, and serde quotes back
what it was given:

```
invalid type: string "hunter2", expected u64
```

`Job::params_error` is the one function that produces that sentence, for the
launch, for `POST /api/jobs/{name}/validate_params` and for `--dry-run`, and it
replaces the job's declared secret values before returning. a caller that finds
that function later gets the same treatment without having to know to ask.

## The second line, and that it is second

an op holds the value: `ctx.params()["token"]` is a `&str` like any other, and
an op is free to log it, put it in metadata, or fail with it in the message.
the declaration cannot stop that, so there is a second pass that catches the
common shape of it.

**while a run holding secret values is executing in this process, every string
bound to every statement the store issues is scanned for those values, and any
that appears is replaced with the marker.** it sits at the store's parameter
binding, so it covers op output, metadata, log lines, op errors, the run's own
error and every event, including the ones a table added next year will carry.

what it is not:

- **not a name matcher.** hestan does not guess at `token|secret|password`.
  a pattern misses the credential somebody called `key2` and redacts the
  innocent column named `password_column`, and a redaction that is sometimes
  wrong is one nobody can reason about. **a param nobody declared is stored.**
- **not exhaustive.** it finds a copy, not a transformation. a token
  base64-encoded, hashed, or spliced into a signature is a different string and
  is not found.
- **not applied to short values.** a value under 16 characters is kept out of
  the params column by its declaration like any other, and is not hunted
  through every write: a six-character needle matches inside run ids and
  timestamps, and rewriting those would corrupt the run log to protect
  something the declaration already covered.
- **not applied to reads.** a `WHERE` clause rewritten under a query would
  answer wrongly. what a secret must not do is get *into* the database.

## The limits, plainly

- **top-level keys only.** `{"token": "…"}` is redacted;
  `{"db": {"password": "…"}}` is not. the declaration names a param, and a
  param is a key of the object a launch was given.
- **one process.** the value lives in the memory of whatever took the launch.
  a worker in another process that claims the run finds the marker, refuses to
  execute on it, and fails the run:

  ```
  params token were declared secret, so they are not in the run log, and this
  process is not the one that was given them. relaunch from here, or move the
  credential to a resource, which is built where the op runs
  ```

  so secret params work on a single-process deployment, and on a multi-process
  one only when whatever launched also executes. a `Role::Scheduler` process
  enqueuing for `Role::Worker` processes is exactly the shape they do not work
  in. **for that shape, use a resource.**
- **not a lifetime.** the value is dropped when the run finishes in this
  process, or when 4096 later runs with secret params have pushed it out,
  whichever comes first. nothing persists it and nothing recovers it.
- **it does not encrypt anything.** the marker is a marker. a param nobody
  declared is stored in plain text exactly as it always was, and so is
  everything else in the run log.

## What an existing deployment sees

nothing, until an op declares a param. no schema version, no migration, no
column, no response shape, and a job that declares nothing writes params byte
for byte as it always did.

## Where each piece lives

| | |
| --- | --- |
| the declaration | `Op::secret_params`, merged per job by `Job::secret_params` |
| the marker, the vault, the second line | `src/secret.rs` |
| the choke point | `Store::params_col`, and the scrub at `Exec::execute` |
| holding and putting back the value | `src/executor.rs` (`enqueue`, `execute_in_span`) |
| the replay refusal | `Error::RedactedParams`, raised by `refuse_marked` |
