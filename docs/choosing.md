# Choosing between the pieces

Choose the execution and storage model that fits your application. These
features can be combined within one registry.

## Job or asset

| | Job | Asset |
| --- | --- | --- |
| Unit | An execution of a dependency graph | A stored value and its lineage |
| Use for | Tasks and side effects | Data that downstream work consumes |
| Reuse | Executes the requested operations | Can seed fresh upstream values from stored materializations |
| Tracking | Run status and history | Fingerprints, staleness, materializations and checks |

Jobs run when launched by a person, schedule or sensor. Assets build when
requested or when a declared schedule or automation policy launches them;
changing an input alone does not enable automatic builds. Both use the same
executor, retries and cancellation.

See [concepts](concepts.md), [assets](assets.md) and
[automation policies](assets.md#automation-policies).

## sqlite or postgres

| | SQLite | PostgreSQL |
| --- | --- | --- |
| Storage | Local file | Database server |
| Shared by | Processes on one host | Processes on one or more hosts |
| Feature | Available by default | `postgres` |

Use SQLite for a local run store. Use PostgreSQL for workers on multiple hosts;
do not share SQLite over a network filesystem. Both expose the same store API.
See [storage](storage.md) and [scaling](scaling.md).

## In-process or isolated

Operations run as async tasks by default. Use `.isolated()` for an operation
that needs process-level crash containment or termination.

| | In process | Isolated |
| --- | --- | --- |
| Each attempt | Async task | Child process that rebuilds the registry and resources |
| Abort or segfault | Can terminate the application | Fails the operation |
| Cancellation | Cooperative for blocking work | SIGTERM, then SIGKILL |
| stdout/stderr | Application output | Captured per attempt |
| Platform | Supported Rust targets | Unix only |

Isolation is not a security sandbox. See [isolated ops](isolation.md).

## Schedule or sensor

Use a **schedule** for work tied to a time: cron, timezone, durable cursor and
catch-up policy. `ctx.scheduled_for()` identifies the occurrence being handled.

Use a **sensor** to poll for changes or chain jobs from run outcomes. A sensor
cursor records progress, and run keys deduplicate repeated requests. The next
poll sees the current source state; recovering events missed during downtime
depends on the source and the cursor your sensor uses.

See [scheduling](scheduling.md) and [sensors](sensors.md).

## The ones that are not choices

- [Overlap policies](scheduling.md#overlap-policy) decide whether a scheduled
  run should be launched or deferred. [Concurrency limits](scaling.md#limits)
  bound how many queued runs execute at once.
- [Freshness](freshness.md) measures the age of successful work;
  [staleness](assets.md#provable-staleness) compares recorded fingerprints.
- Scheduler roles make decisions; worker roles execute queued runs. Multiple
  schedulers share a deciding lease, so only one decides at a time.
- [Events](events.md) record structured activity. [Captured logs](logs.md)
  contain operation output. Both appear on the run page.
