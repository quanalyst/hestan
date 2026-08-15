# Choosing between the pieces

hestan has four pairs where both halves are documented, both are correct, and
nothing says which one you want. this page is only about that. each section
ends with the page that explains the winner properly.

## Job or asset

both describe work, and they disagree about what the unit is.

a **[job](concepts.md)** is a dag of ops and the unit is *the running*. you say
what happens in what order; hestan executes it when something asks. what it
leaves behind is a run.

an **[asset](assets.md)** is a thing that exists (a table, a file, a model),
declared by what it is made of. the unit is *the value*. what it leaves behind
is a materialization: the value, a fingerprint of it, and which fingerprint of
each input it was computed from. from those, staleness is a fact rather than a
guess, and a build does the minimum: stale ancestors and the target, with
everything already fresh seeded from what is stored.

| | job | asset |
| --- | --- | --- |
| the unit | a run | a value, with a fingerprint |
| you declare | what happens, in order | what a thing is made of |
| it runs when | a schedule, a sensor, or somebody asks | something it is made of changed, or somebody asks |
| "is this up to date?" | not a question it can answer | a fact the recorded fingerprints settle |
| re-running | does the work again | does the work only where the inputs moved |

**take a job** when the work is a sequence of effects: send the invoices, roll
the logs, poke the third-party api, run the healthcheck. nothing persists that
"is" a thing, so there is nothing for a fingerprint to describe, and a dag of
ops is exactly the shape of the problem.

**take an asset** when the work produces something that other work consumes,
and you would like to be asked less often whether it is current. a table built
from two other tables, a model trained on a dataset, a report derived from a
warehouse. the moment you find yourself writing "only rebuild this if the
source changed", the asset is the feature that already did it.

they are not exclusive and they are not layered on separate machinery: assets
lower into ops of one internal job, so retries, cancellation, the gantt, the
event log and the hooks are the same in both. a deployment that has both is
ordinary.

→ [concepts](concepts.md) · [assets](assets.md)

## sqlite or postgres

the run log is a database and there are two of them. the schema is identical,
the api is identical, and the same test suite runs against both, so this is a
question about deployment, not about features.

| | sqlite | postgres |
| --- | --- | --- |
| services to operate | none | one |
| processes that can share it | any number, on **one host** | any number, on any number of hosts |
| feature flag | on by default | `--features postgres` |

**take sqlite** (which is to say, do nothing) for one process, and for
several processes on one host. that covers a container, a compose file and a
great many real deployments. it is not the lesser option: one file, no service,
and writers serialized by the file lock rather than by anything you have to
configure.

**take postgres** when the processes have to live on more than one machine. one
host is the whole of the boundary: sqlite's guarantee comes from a file lock,
and a file lock over a network filesystem is not a guarantee. that is the only
reason to move, and moving is one string.

→ [storage](storage.md) · [scaling](scaling.md)

## In-process or isolated

every op runs in the orchestrator's process unless it says otherwise.
`.isolated()` puts one op's body in a child process instead.

| | in process | isolated |
| --- | --- | --- |
| cost per attempt | a task | a process start, plus rebuilding the registry and the resources |
| an `abort()` or a segfault in it | takes the whole deployment down | fails that op |
| cancelling it | asks, and blocking work may not answer | SIGTERM, then SIGKILL, and it is watched dying |
| `println!` from it | goes to your stdout | captured, verbatim, both pipes |
| platform | anywhere | unix, and it is a build error elsewhere |

**stay in process** for everything. this is the default because it is right
nearly always: an op is a task, it costs nothing to start, and rust code you
wrote does not usually abort a process.

**go isolated** for the specific op that parses untrusted input, calls into a c
library, or blocks in a way nothing can interrupt. the deciding question is
whether you need the *guarantee*: containment, or a stop that is provably a
stop. if the answer is "it would be nice", it is not worth a process per
attempt.

→ [isolation](isolation.md)

## Schedule or sensor

both launch runs without a person. they differ in what they are watching.

a **[schedule](scheduling.md)** watches the clock: a cron expression, a
timezone, and a durable cursor so that occurrences missed while nothing was
running are knowable rather than gone.

a **[sensor](sensors.md)** watches everything else: a closure evaluated every
`every`, which looks at a directory, a queue or an api and returns the runs it
wants, usually none.

| | schedule | sensor |
| --- | --- | --- |
| fires on | a cron occurrence | whatever the closure decides |
| downtime | occurrences are missed knowably, and a [catch-up policy](scheduling.md#missed-fire-catch-up) says what to do about them | there is no backlog, only whatever the world looks like at the next poll |
| "which hour is this run for" | `ctx.scheduled_for()` | nothing: a sensor fire stands for itself |
| launching once per thing | the occurrence is the thing | a [run key](sensors.md#run-keys) makes it once per key, ever |

**take a schedule** when the work is *for a time*: yesterday's orders, the
09:00 report, the hourly rollup. a schedule is the only one of the two that
knows a run stands for an occurrence, and that is what makes a backfill or a
catch-up mean anything.

**take a sensor** when the work is *for an event*: a file appeared, a queue is
not empty, an upstream job succeeded. polling on an interval is what a sensor
is, so the interval is a cost you are paying for latency. a run key is
what stops "I saw it again" turning into a second run.

a sensor whose closure only checks the clock is a schedule with worse
resolution. a schedule whose job's first op returns early when there is nothing
to do is a sensor with a fixed poll interval and no cursor. both work; neither
is the shape of what you meant.

→ [scheduling](scheduling.md) · [sensors](sensors.md)

## The ones that are not choices

worth saying, because they read like pairs and are not:

- **[`Overlap`](scheduling.md#overlap-policy) and a
  [concurrency limit](scaling.md#limits)** answer different questions. overlap
  decides whether a scheduled fire should exist at all while its job still has
  a run outstanding, a policy about the work. a limit decides how many runs
  execute at once, a policy about the machine. you may well want both.
- **[freshness](freshness.md) and [staleness](assets.md#provable-staleness)**
  are not the same claim. late means "the last success is older than the policy
  allows", which is about time. stale means "an input moved since this was
  built", which is about lineage. an asset can be fresh and stale, or late and
  up to date.
- **`Role::Scheduler` and `Role::Worker`** are not a scale-up ladder. one
  process deciding is a *requirement* (two schedulers is two of every
  scheduled run), while any number may execute. splitting them is how you get
  more executors, not how you get more of everything.
- **the [run log](events.md) and [captured output](logs.md)** are two logs on
  the same page on purpose. one is hestan narrating what happened; the other is
  what your op printed. merging them would bury eight structured lines under a
  library's debug output.
