# Replay

an op failed in production two months ago. you have a fix. the question worth
answering is whether the fix works **on the input that broke it** — not on one
you reconstructed by hand.

```rust
let id = runner.replay(&broken)?;                       // the ops that failed
let id = runner.replay_ops(&broken, Some(&["load".into()]))?;  // or these
```

that launches a **new** run of the same job which executes those ops and
nothing else, with every dep of them seeded from what the original run
recorded. the op reads byte for byte what it read then. the original run is
never written to — no status, no event, no materialization: it is history and
stays history, and the new run records `replay_of` pointing at it.

## What it is not

a [resume](concepts.md#resume) re-runs what did **not** succeed, together with
everything downstream of it, and is how you finish a run that broke. a replay
re-runs what **did**, exactly the ops named and nothing below them, and is how
you find out whether something would go differently now. they are one letter
apart in a run log and opposite in meaning, so they are separate triggers
(`resume`, `replay`) and separate columns (`resumed_from`, `replay_of`), and a
run carries at most one of them.

a [retry](http-api.md#retry) is the third of these: the whole job again from
the beginning, on nothing the old run produced.

## What it does not reproduce

**a replay reproduces the inputs, not the world.** four things move on
regardless, and a replay that succeeded is only evidence about the ones that
did not matter to it:

- **the code is today's.** that is the point — you are testing a fix — but it
  means a replay is not a bit-for-bit re-execution of what happened. an op
  whose body changed in twelve ways since is running all twelve.
- **resources are rebuilt.** a connection, a client, a temp directory: the op
  gets [today's](resources.md), not the original's. an op that read something
  through a resource — a table, a config row, a file the client points at —
  read a world that has moved on, and hestan captured none of it.
- **the clock, randomness, and anything the op fetches itself** are not
  captured and cannot be. `Utc::now()` answers today. an op that calls an api
  gets today's answer, not the one the api gave in June. only what arrived
  through `ctx.input` is reproduced.
- **the params are the original's, and the job is today's.** the run's params
  come back with it; the graph around the op does not. a job that has since
  gained a dep on the op being replayed fails the replay rather than seeding
  it, because there is no recorded output for a dep that did not exist.

so a replay that succeeds says "this code, today, on that input, worked". it
does not say the original run would have worked with this fix, and it is not a
reconstruction of an incident. if the difference matters for what you are
about to conclude, the honest reading is the narrow one.

## The retention horizon

[retention](storage.md#retention) prunes old runs, and pruning a run asks
every registered [io manager](io-managers.md) to drop what that run wrote — so
an old run's values go when its rows do. **a pruned run cannot be replayed**,
and neither can one whose files a manager can no longer produce.

that is refused rather than run:

```
cannot replay load of run 019ff1b7-...: its input extract cannot be read back:
No such file or directory (os error 2)
```

every seed is read back through its manager before anything is launched, so
the refusal comes instead of a run rather than halfway through one. a run that
executed with a silently defaulted input would be a "reproduction" that
reproduces nothing.

if replay is why you keep history, keep it for longer where it matters:

```rust
Hestan::new()
    // failures age slower than successes, and a failure is what you replay
    .retention(Retention::days(30).failed_days(180))
    // or per job, for the one whose inputs you will want in a year
    .job(Job::builder("orders_etl").retention(Retention::days(365)).op(load).build()?)
```

with no policy configured nothing is ever pruned, which is the default and
means every run stays replayable — and that every `FileIo` directory grows
forever. those are the same fact from two directions.

## The three ways in

**in code**, `Runner::replay(run_id)` replays the ops the run recorded as
failed, and `Runner::replay_ops(run_id, Some(&ops))` replays exactly the ops
named — whatever they did, as long as the run ran them.
`Runner::replay_plan` answers what either would do without launching it, and
raises every refusal the launch would.

**over http**, `POST /api/runs/{id}/replay` with an optional `{"ops": [..]}`,
and `GET /api/runs/{id}/replay_preview?ops=..` for the plan
([http api](http-api.md#replay)). it is an operator action, like every other
control that drives a run.

**on the command line**:

```
hestan replay <run> [--op OP]...
```

exit 2 covers what cannot be replayed: a run with nothing that failed, an op
the run never ran, an input that cannot be read back
([the command line](cli.md)).

**in the ui**, the run page carries a replay control on the run and one on the
selected op, each showing what it would do — "1 to replay · 1 input seeded" —
and a replayed run's header links back to the run it replayed
([web ui](web-ui.md#run-page)).

## What a replay of a subset run reads

a run that was itself a subset of its job — a resume, a replay, an
[asset build](assets.md) — did not produce everything its ops read. what it
was handed is recorded on the run as its plan, and that is what a replay of it
seeds from: the ops it ran are rows, and the values it was given are the plan.
so replaying an op of a resumed run reads what that op read, whichever run
originally produced it, without walking a chain or guessing.

an op the run never ran is refused rather than launched on its own. it has no
inputs of its own to reproduce, and running it anyway would be a partial
launch wearing a replay's name.
