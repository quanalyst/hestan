# Isolated ops

an op runs in the orchestrator's own process. that is cheap, and it means one
op can end everything: a segfault in a c library, an `abort()`, an allocation
the machine cannot serve, and hestan goes down with the run it was executing
and every other run beside it. it also means cancellation is a request:
hestan can drop a future or set a flag, and blocking work that reads neither
carries on regardless.

`isolated()` moves one op's body into a child process:

```rust
Op::new("parse_untrusted", |ctx| async move {
    Ok(json!(risky_parse(ctx.input("fetch").unwrap())?))
})
.isolated()
```

two things change, and they are the two reasons to reach for it:

- **containment.** the op's process dies; hestan's does not. the failure is
  recorded against that op, with what killed it, and the other forty ops in
  the run carry on.
- **real stopping.** there is a process to signal, so cancelling a run or
  expiring an `Op::timeout` sends SIGTERM, waits, and then SIGKILLs. nothing in
  the op gets a vote.

everything else is unchanged. an isolated op is an ordinary unit whose body
happens to be a subprocess: `max_parallel`, [pools](concepts.md#concurrency-pools),
retries, [`When`](concepts.md#trigger-rules) rules, `Op::timeout` and the run's
cancellation all mean what they meant.

## Per op, not per job

the usual shape is an executor chosen for a whole job: every step pays a fresh
process and a reload of your code, whether or not it was ever going to crash.

isolation here is a property of the op. one risky parser is contained while
the other forty ops in the same job stay in-process and free. that is possible
because the child is not a runtime being loaded: it is this binary, again.

## An op subprocess is not a queue worker

both spawn processes, so it is worth saying which is which before anything
else. what this page is about is an **op subprocess**: started by
`Op::isolated()`, per attempt, it runs one op of one run and exits, and it
claims nothing and owns nothing. a [queue worker](scaling.md#roles) is the
other thing: a long-lived process you start, which claims whole runs off the
queue and executes them, and which spawns op subprocesses itself like any
other hestan process. containment is the point of one; throughput is the point
of the other.

## The subprocess is your binary

the parent spawns `std::env::current_exe()` with two environment variables
set:

```
HESTAN_ISOLATED_RUN=019...    the run
HESTAN_ISOLATED_OP=parse      the one op of it to run
```

`serve`, `work`, `run_once` and `build_asset` check for those **before
anything else they do**, and take the op-subprocess path rather than starting
up. so the child rebuilds the same jobs, resources and io managers as its
parent for one reason: it runs the same `main`. nothing describes the registry
to it.

that is a real constraint, and it is worth stating plainly:

- **the binary must build the same registry on re-exec.** any ordinary `main`
  does. a `main` that registers different jobs depending on argv, or reads a
  different database out of a flag the parent did not pass on, cannot host an
  op subprocess: the child will not find its op, and the parent will record
  that the op exited with a status and no result.
- **the database must be reachable from another process.** the child opens
  the same target your builder named: a sqlite path or a `postgres://` url.
  `":memory:"` is private to one connection, so an isolated op against an
  in-memory store is a build error rather than a mystery at run time.
- **unix only.** `isolated()` on another platform is a build error naming the
  platform. an isolation guarantee that quietly is not one is the worst option
  available, so there is no in-process fallback.

what the child does *not* do is as important. it runs no boot recovery, no
schedule sync, no tick prune, no retention sweep, no scheduler, sensor,
freshness or backfill loop, and it binds no listener. all of that assumes the
process owns the database: `fail_interrupted` in particular marks every
queued and running run as interrupted on the assumption that the last process
died, which in a child would mean marking its own parent's in-flight runs.

## The store is the channel

there is no protocol, no serialization format, and nothing to keep in sync
between the two processes, because everything an op invocation needs is
already a row:

| the op needs      | it reads                                    |
| ----------------- | ------------------------------------------- |
| params            | `runs.params`                               |
| `scheduled_for`   | `runs.scheduled_for`                        |
| its inputs        | `op_runs.inputs`, resolved through io       |
| committed state   | `op_state`                                  |
| what its deps did | `op_runs.inputs`                            |

and everything it produces goes back the same way: the output through its
[io manager](io-managers.md), the terminal status, output handle and
[metadata](metadata.md) onto its own `op_runs` row, `ctx.set_state` into
`op_state`, and every `ctx.info`/`warn`/`error` line straight into the run's
events, so an isolated op's logs appear in the run page's log exactly where
they would have anyway.

`op_runs.inputs` is the one thing the parent writes for the child, and it is
there for a case the rest of the store cannot cover: a **seeded** input. on a
[resume](concepts.md#resume) or an asset build, a dep's value belongs to an
earlier run, so it is not on any row of this one. the parent records what it
holds for each dep (`{"held": {dep: handle}, "deps": {dep: status}}`), and
the child reads its inputs from one place whether they were produced here or
seeded from elsewhere. they are **handles**, not payloads, so an op reading a
gigabyte through `FileIo` reads it once, in the process that wants it.

the run's cancellation is the only signal that does not travel through the
store, for the good reason that a process being asked to stop should not have
to poll a database to find out.

## What the child printed

the one thing that does travel down a pipe. stdout and stderr are the child's,
whole (nobody else in that process can claim them), so the parent pipes both
and stores every line under this attempt, tagged with the stream it came out
of. `println!`, a python subprocess, a linked c library writing to fd 2: all
of it, verbatim, with nothing to switch on.

the parent reads **both pipes concurrently**, and that is load-bearing rather
than tidy: draining stdout to its end and stderr afterwards leaves stderr's
pipe buffer to fill, and a child blocked writing into a full pipe never exits.

a child that is killed, aborts or dies mid-line keeps what it wrote: the
pipes are drained after the kill, since a pipe ends when the process holding
the other side of it is gone. for the segfault case above, what the op printed
before it went is usually the only evidence there is. an in-process op gets no
such thing, for a reason [docs/logs.md](logs.md) states plainly: redirecting
fd 1 process-wide would hijack the host application's output. capture is
capped per attempt (1 MiB and 10,000 lines by default) and reading carries on
after the cap, so a chatty child never blocks on a pipe nobody is draining.

## A child that dies without recording anything

this is the case the whole feature exists for. a child that is killed, aborts,
segfaults or is refused memory never writes a terminal row, so **the parent
writes it**, naming what happened to the process:

```
op exited with signal 6 (aborted) without recording a result
op exited with signal 9 (killed) without recording a result
op exited with status 101 without recording a result
```

the attempt then goes through the ordinary retry policy (`retries(2)` on an
isolated op means up to three child processes), and a terminal failure fails
the op, skips its downstream and fails the run, exactly as an in-process
failure does.

a child that *did* record a result is believed, whatever its exit status says
afterwards: it is the process that ran the body, and if it got as far as
writing an output that output is what happened.

## Stopping it for real

cancelling a run, or an `Op::timeout` expiring, does this to an isolated op:

1. **SIGTERM.** inside the child this arrives as ordinary cancellation
   (`ctx.is_cancelled()` turns true and `ctx.cancelled()` resolves), so an op
   written to wind down gets to.
2. **three seconds.**
3. **SIGKILL**, and the process is reaped.

the op run row then carries a real `finished_at` and an error saying which of
the two ended it:

```
canceled: it stopped when asked
canceled: it ignored SIGTERM for 3s and was killed
timed out after 30s: it ignored SIGTERM for 3s and was killed
```

compare the in-process row for work that polls nothing: status `canceled`,
**no finish time**, and an error saying hestan asked and never saw it stop,
because that is all it knows. the [cancellation
section](concepts.md#cancellation) has the full contrast. this is the one
place hestan can promise that work stopped, and it is the strongest argument
for `isolated()` on anything blocking.

a timeout is still a failed attempt rather than a canceled run, so it retries
like any other failure.

## Limits

a child can be capped, which an in-process op cannot be:

```rust
Op::new("parse", body)
    .isolated()
    .memory_limit(512 * 1024 * 1024)
    .cpu_limit(Duration::from_secs(30))
```

the child applies both to itself with `setrlimit` just before the body runs:
`RLIMIT_AS` for memory, `RLIMIT_CPU` for cpu time. a limit without
`isolated()` is a build error: a limit applies to a process, and in-process
that process is the orchestrator.

exceeding memory fails an allocation, which in rust aborts; exceeding cpu
arrives as SIGXCPU. either way the parent names the limit rather than the
signal:

```
op exited with signal 6 (aborted) without recording a result; it was running
under a memory limit of 512 MiB, which an allocation past the limit aborts on

op exited with signal 24 (cpu limit exceeded) without recording a result; it
exceeded its cpu limit of 30s
```

three things to know about them:

- `memory_limit` is **address space**, not resident memory. large reservations
  count even untouched, which is what makes the failure deterministic instead
  of a visit from the oom killer at a moment of the kernel's choosing.
- `cpu_limit` is **cpu time, not wall clock**. an op waiting an hour on a
  socket has spent no cpu and is untouched by it. that is the difference from
  `timeout`, and the reason the two compose. one-second granularity, the
  kernel's.
- both cover the **whole child**: hestan, sqlite and process startup are
  inside the limit, not just your body. leave headroom.

## What it costs

a process spawn per attempt. it is milliseconds, not the seconds an
interpreter start costs, because there is no runtime to load and no code to
re-import. it is not free, though, and it is per *attempt*, so a retried
isolated op spawns again.

the child also rebuilds what the parent built: it opens the store and
constructs every [resource](resources.md) your builder declares. a resource
whose constructor takes two seconds makes every isolated op cost two seconds.

and both processes write the same run log. on sqlite that is one file two
writers share, which is what the busy timeout on every connection is for; on
postgres they are two ordinary clients of the same server and there is nothing
to arrange. writes here are small and rare either way (a row and a few events
per op), so this is not a throughput concern at the scale hestan is built for,
but on sqlite it is why isolation wants a real database file rather than a
tmpfs afterthought.

reach for `isolated()` on the op that parses untrusted input, calls into a c
library, or blocks in a way you cannot interrupt. leave the other forty alone.

## Limits of the feature

- **not a sandbox.** the child is a full copy of your binary with your
  environment, your filesystem access and your credentials. it contains
  crashes and enforces resource caps; it does not contain intent. for that,
  reach for a container or a seccomp profile around the whole process.
- **not for mapped ops.** an isolated op may not be an
  [`Op::mapped`](concepts.md#dynamic-fan-out), and a fan-out's instances may not be
  isolated either; both are refused at build. an instance's element is the one
  input that is nowhere a child could read: it is a slice of the parent's
  collected array, not a row of its own. fan out first and isolate a downstream
  op if the risky work is per element.
- **not for assets.** the same reasoning: an asset op is built by the asset
  lowering, and a [partitioned asset](assets.md) expands through the same
  fan-out machinery.
- **one op per subprocess.** it runs one attempt of one op and exits. there is
  no pool and no reuse, which is what keeps the failure model simple: the
  process that ran the op is the process that died. a
  [queue worker](scaling.md) is the long-lived process, and it is a different
  mechanism for a different problem.
