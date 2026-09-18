# Secrets in params

Ordinary run parameters are persisted and exposed in the UI, API and CLI.
Declare sensitive top-level parameter keys with `Op::secret_params`:

```rust
Op::new("push", |ctx: OpCtx| async move {
    let token = ctx.params()["token"].as_str().ok_or("no token")?;
    deploy(token).await
})
.secret_params(["token"])
```

The operation receives the original value. Stored parameters contain
`[hestan:redacted]` in its place. Prefer [resources](resources.md) for
credentials shared by a deployment.

## Where the redaction is

The store redacts declared keys before writing `runs.params`,
`schedules.params` or `presets.params`. The launching process keeps the original
values in memory and restores them for local execution; it does not persist
them for another worker.

## What a secret means for a replay

Retry, resume and replay use stored parameters, so they cannot recover a
redacted value. These requests and their previews return a conflict when a
required secret value is the marker.

Launch again with the credential, or move it into a resource. Ordinary retries
within the original operation execution still have its in-memory parameters.

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

Redaction is opt-in. Undeclared parameters continue to be stored normally;
adding a declaration does not remove secrets from previously stored history.

## Where each piece lives

See `Op::secret_params` and `Job::secret_params` in the Rust API. The marker and
in-memory values are handled in `src/secret.rs`; storage redaction is applied
by `Store`, and launch validation rejects stored markers.

Declared schedules and presets are redacted before startup writes them. This does
not remove credentials from older database rows or backups. Isolated ops cannot
restore secret parameters from the parent’s memory: they fail before the op body
runs. Use a resource constructed in the child for those credentials.
