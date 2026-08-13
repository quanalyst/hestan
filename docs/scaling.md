# Scaling

hestan has **one** mechanism for moving work off the process that asked for
it: a durable, claimable queue. deployment shape decides where the claimers
live. a container and a pod are packaging around that one mechanism, not new
execution paths — which is why there is no kubernetes executor here, and no
docker executor, and no celery integration.

read [what this does not do](#what-this-does-not-do) first if you are sizing
hestan for a deployment. the limits are real and stated rather than papered
over.

## The queue

a launch is a request, not a start. `launch()` writes a `queued` run and
returns its id exactly as it always did; a **dispatcher** decides when it
starts. with no limits declared that is the same instant and nothing about
hestan looks different. with limits declared, the run waits.

the queue is the `runs` table — a queued run is one with `claimed_by IS NULL`
— so it survives a restart, and anything that can reach the database can pull
from it.

```rust
Hestan::new()
    .job(orders_etl)
    .max_concurrent_runs(8)          // across the whole deployment
    .tag_limit("env", "prod", 2)     // whatever the jobs are
    .priority(0)                     // the default position in the queue
    // a reachable address needs an authenticator, or `serve` refuses it
    .auth(Auth::bearer(std::env::var("HESTAN_TOKEN")?))
    .serve(([0, 0, 0, 0], 4000))
    .await
```

`GET /api/queue` reports the depth, each waiting run's position, and what is
holding it back. the runs page shows the same thing with a bump button.

### Limits

| limit | scope | where |
| --- | --- | --- |
| `Hestan::max_concurrent_runs(n)` | the whole deployment | stored, shared |
| `JobBuilder::max_concurrent_runs(n)` | one job | stored, shared |
| `Hestan::tag_limit(k, v, n)` | every run carrying that [tag](launching.md#run-tags) | stored, shared |
| `Hestan::slots(n)` | **this process** | in memory, per process |

every one of them counts runs that are **executing** — claimed and not
finished. a run sitting on the queue costs nothing and counts as nothing.

`slots` is the odd one out and the one people forget. the others say how much
work the deployment does at once; `slots` says how much of it lands in this
container. without it the first worker to look at the queue claims all of it
and the worker beside it has nothing to do — a worker should take what it can
run, not what it can see.

limits are read at the top of every dispatch pass, so raising one drains the
queue it was holding back without a restart.

### Limits are not overlap policies

they answer different questions and it is worth being clear which you want.

[`Overlap`](scheduling.md#overlap-policy) decides whether a scheduled fire
should **exist at all** while its job still has a run outstanding — a policy
about the work. a concurrency limit decides how many runs **execute at once**
— a policy about the machine.

concretely: a queued run counts as outstanding for overlap, so a job with
`Overlap::Skip` whose runs are being held back by a limit does not pile up a
fire per minute behind them. that is deliberate. `Overlap::Skip` exists to
prevent exactly that pile-up, and a limit is not a reason to start ignoring
it. the same reasoning gates backfill chunking and the asset build endpoints:
"is there a run of this outstanding" is the question, and a queued run is
outstanding.

### Priority

`Hestan::priority(n)` sets the default and `{"priority": n}` overrides it per
launch. higher goes first, ties broken by creation time. negatives are legal
and are how a deployment says "these are the background ones".

**Priority is a preference, not an order.** The dispatcher skips a run a limit
would block and starts the next one that fits. so a high-priority `env:prod`
run waiting on its tag limit does not hold up an unrelated low-priority run
behind it. the alternative is head-of-line blocking, where one blocked run
stops a queue that has capacity sitting idle, and that is the worse trade —
but it does mean the start order is not the priority order, and you should not
build anything on the assumption that it is.

## Claims and leases

a queued run must be startable by exactly one claimer, and a claimer that dies
must not strand it forever.

claiming is a compare-and-set:

```sql
UPDATE runs SET claimed_by = ?, claimed_at = ?, lease_until = ?
WHERE id = ? AND claimed_by IS NULL AND status = 'queued'
```

one winner by construction, whoever else is looking at the same row. that
holds on both backends and is what makes a race here a thing that resolves
rather than a thing to avoid.

how the two get there differs, and it is the reason to run this on
[postgres](storage.md#postgres). sqlite serializes writers for us: an
immediate transaction is the only one there is, which is a complete guarantee
on one host and none at all across several. postgres reserves the one run a
dispatcher decided on with `SELECT ... FOR UPDATE SKIP LOCKED`, so several
dispatchers walk the same queue at the same moment and come away with
different runs, none of them waiting on any other.

counting capacity and spending it have to be one decision either way. one
transaction is enough for that on sqlite; on postgres it is not, because two
transactions each read the same last free slot from their own snapshot and
both fill it. so **when a limit is in force — and only then** — postgres
claimers take turns on an advisory lock for the length of one claim. with no
limits declared, which is the default, dispatchers never meet.

`claimed_by` is the claimer's **instance id**: eight hex digits, made once per
process, reported by `GET /api/health` along with the runs that process is
currently holding. three workers and one run is a question you can answer.

a claimer renews `lease_until` every 15 seconds and the lease is good for 60,
so four beats have to be missed before anything is taken — a slow store is not
a dead process. a run whose lease has expired is reclaimed by whichever
process notices: its ops are marked with `claimer went away`, naming the
claimer, and then

- **`Reclaim::Fail`** (the default) fails the run and fires the failure hooks.
  A run that got halfway may have done half its side effects; doing them again
  quietly is worse than a stall somebody has to look at.
- **`Reclaim::Requeue`** puts it back on the queue for another claimer. right
  when the work is idempotent and available beats exact.

```rust
Hestan::new().reclaim(Reclaim::Requeue)
```

a lease is also how a process says something it cannot write down. if a run's
critical write will not land after its retries, the process **stops executing
that run and stops renewing its lease** — it reports no status at all, because
at that point it does not know what the status is. one lease later the run is
reclaimed exactly as if the process had died, and `Reclaim` decides. that is
deliberate: a process that kept renewing a claim it had given up on would hold
that run out of every reclaimer's reach for as long as it stayed alive.
[what hestan promises about writes](concepts.md#what-hestan-promises-about-writes)
is the whole of it, including why a run left `running` for a minute beats a
run reported `success` that nothing recorded.

such a process also stops claiming until a write of its lands — the lease
renewal itself is what tells it the store is back — so a worker with a broken
store empties no queue. `GET /api/health` says `ok: false` while that is true,
and lists the runs it gave up on.

### Boot recovery respects a live claim

the startup sweep used to mark every `queued` or `running` run failed, on the
assumption that the process starting up was the only one there had ever been.
with a claimable queue that assumption is destructive: a second process
starting would fail a live one's in-flight runs, mid-run.

it is now a mechanism rather than an assumption. a run is swept only if its
lease has expired, or if it is `running` with no claim at all (which can only
be a row written before the queue existed). a **queued run nobody has claimed
is left where it is** — that row is not a casualty, it is the queue.

## Roles

```rust
Hestan::new().role(Role::Scheduler).serve(addr).await   // decides
Hestan::new().work(None).await                          // executes
Hestan::new().serve(addr).await                         // both; the default
```

| role | schedules, sensors, freshness, backfill chunking, retention, notification delivery | claims and executes |
| --- | --- | --- |
| `Role::All` (default) | yes | yes |
| `Role::Scheduler` | yes | no |
| `Role::Worker` | no | yes |

**Exactly one process may be `All` or `Scheduler`.** Schedules, sensors,
freshness checks and backfill chunking are decisions, and two processes making
them independently is two of every scheduled run — there is no lock that would
stop it. **Any number of processes may be `Worker`**; that is the entire point
of a claimable queue.

the [retention sweep](storage.md#retention) is on the same side of that line
and for a sharper reason: a worker owns none of the history, and one pruning
the scheduler's runs is data loss nothing reports. so is
[notification delivery](notifications.md#durable-delivery) — two processes
delivering would send every alert twice — which means the hooks want
registering on the process that decides.

`Hestan::work(addr)` is `role(Role::Worker)` with the address made optional,
because a worker may want no socket at all. give it one and you get the same
ui, which is worth having for `/api/health`.

every role runs the lease loop, including a scheduler holding nothing:
noticing a dead claimer cannot be the dead claimer's job.

every process must build the **same registry**. a worker executes runs a
scheduler wrote, and the two have to agree about what a job is; a run whose
job this process does not define is left on the queue and reported as blocked
rather than claimed and failed. in practice this means one binary started with
different roles, which is what the compose file below does.

### A queue worker is not an op subprocess

two mechanisms in hestan spawn processes and they are not the same thing.

| | [op subprocess](isolation.md) | queue worker |
| --- | --- | --- |
| started by | `Op::isolated()`, per attempt | you, per deployment |
| lives for | one op of one run | as long as you leave it up |
| claims | nothing | whole runs, off the queue |
| purpose | containment: a segfault costs one op | throughput: more hands |

a queue worker spawns op subprocesses like any other hestan process does. the
environment variables that mark an op subprocess are `HESTAN_ISOLATED_RUN` and
`HESTAN_ISOLATED_OP`, and every entry point checks for them first.

## The compose example

`Dockerfile` and `docker-compose.yml` at the repo root run the demo as one
scheduler and two workers against a shared volume:

```
docker compose up --build
open http://localhost:4000
```

the ui asks for a token the first time, because the compose file binds
`0.0.0.0` and publishes the port: `serve` refuses a reachable address with
nothing checking who is asking, so the file sets `HESTAN_TOKEN` and the demo
picks it up. it is `demo-token-change-me`, which is what a token in a compose
file deserves to be called — [auth.md](auth.md) has where a real one comes
from.

watch the queued section on the runs page fill and drain, and `claimed_by` on
a run say which worker took it. both workers have `HESTAN_SLOTS=2` and the
deployment has `HESTAN_MAX_CONCURRENT_RUNS=4`.

it is one image. the scheduler and the workers differ only by `HESTAN_ROLE`,
because they must build the same registry.

## Several hosts

everything above works on one host with the default sqlite file: several
containers, one volume, and the compose example just above. past one host you
need
a store every host can reach, and that is what the
[postgres backend](storage.md#postgres) is:

```rust
Hestan::new().db("postgres://user:pw@db.internal/hestan").work(None).await
```

built with `--features postgres`. nothing else about a deployment changes. the
queue, the claims, the leases and the roles were always backend-agnostic and
they still are — one scheduler, any number of workers, the same registry in
every process.

**What is proven and what merely follows.** hestan's suite runs the queue
cases twice: worker processes racing one sqlite file, and worker processes
racing one postgres schema, asserting in both that no run executed twice. both
of those are several *processes* against one database. nobody has run hestan's
workers on several *hosts*, because the machine the suite runs on is one
machine — and a process on another host differs from a process on this one
only in which socket it opens. it follows, and the difference between "it
follows" and "it was run" is the difference this paragraph exists to keep.

two things do still hold whatever the backend. **Exactly one process may be
`All` or `Scheduler`** — postgres does not change that, because two processes
independently deciding to fire a schedule is two runs and there is no lock
that would stop it. and **sqlite is still right** for one host: it needs no
server, and reaching for postgres to run one container is a database to
operate for nothing.

## What this does not do

**There is no kubernetes executor and no celery integration, and this page is
not going to imply otherwise.** hestan ships one mechanism for moving work
off-box rather than several, and a pod running
`HESTAN_ROLE=worker` against a shared postgres is what "the kubernetes
executor" would have been — that is the mechanism, and there is no operator, no
pod template and no autoscaler around it. celery has no rust analogue worth
porting; the queue and the workers above are the equivalent capability, and
calling them that is more useful than an integration page for something that is
not there.

## See also

- [scheduling](scheduling.md) — overlap policies, and why a queued run counts
  as outstanding.
- [isolation](isolation.md) — the other mechanism that spawns processes.
- [storage](storage.md) — the two backends, the queue columns, and what boot
  recovery sweeps.
- [http api](http-api.md) — `/api/queue`, `/api/runs/{id}/priority`,
  `/api/health`.
