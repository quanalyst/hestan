# Replay

Replay selected operations using their recorded dependency inputs and the
original run parameters. It creates a new run with `replay_of` pointing to the
original; it does not modify the original run.

```rust
let id = runner.replay(&broken)?;                       // the ops that failed
let id = runner.replay_ops(&broken, Some(&["load".into()]))?;  // or these
```

By default, replay selects failed operations. Explicit selection can include
other operations the original run executed.

## What it is not

| Action | What executes | Reused data |
| --- | --- | --- |
| [Retry](http-api.md#retry) | The whole job | Original parameters |
| [Resume](concepts.md#resume) | Operations that did not succeed and their downstream work | Successful outputs |
| Replay | Exactly the selected operations | Their recorded dependency inputs |

Resume records `resumed_from`; replay records `replay_of`. A run carries at
most one of these references.

## What it does not reproduce

Replay uses the current code, graph and resources. It does not reproduce the
clock, randomness, external services or data fetched directly by an operation.
Only recorded dependency inputs and parameters are reused.

If a selected operation now requires an input that the original run did not
record, replay is refused. Success demonstrates that the current code ran on
those saved inputs; it does not reconstruct the original environment.

## A run that carried a secret param cannot be replayed at all

Stored [secret parameters](secrets.md) contain a redaction marker rather than
the credential. Retry, resume, replay and their previews reject that marker.
Launch again with the required values, or supply credentials through
[resources](resources.md).

## The retention horizon

A replay requires the original run and all selected dependency inputs. Hestan
resolves every input through its I/O manager before launching and refuses the
request if a row or stored value is unavailable.

[Retention](storage.md#retention) removes run history and managed output files.
Keep failures longer if they are needed for diagnosis:

```rust
Hestan::new().retention(Retention::days(30).failed_days(180))
```

Without a retention policy, Hestan keeps history. External files may still
become unavailable independently of retention.

## The three ways in

**in code**, `Runner::replay(run_id)` replays the ops the run recorded as
failed, and `Runner::replay_ops(run_id, Some(&ops))` replays exactly the ops
named (whatever they did, as long as the run ran them).
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
selected op, each showing what it would do ("1 to replay · 1 input seeded"),
and a replayed run's header links back to the run it replayed
([web ui](web-ui.md#run-page)).

## What a replay of a subset run reads

Subset runs record their seeded inputs in their execution plan. Replaying one
uses that plan together with the operation rows to reproduce the inputs it
read, including values originally produced by another run. Selecting an
operation that the original run never executed is refused.
