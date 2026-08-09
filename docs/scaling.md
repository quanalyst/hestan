# Scaling

hestan has **one** mechanism for moving work off the process that asked for
it: a durable, claimable queue. deployment shape decides where the claimers
live. a container and a pod are packaging around that one mechanism, not new
execution paths — which is why there is no kubernetes executor here, and no
docker executor, and no celery integration.

Read [what this does not do](#what-this-does-not-do) first if you are sizing
hestan against dagster. the limits are real and stated rather than papered
over.

## The queue

A launch is a request, not a start. `launch()` writes a `queued` run and
returns its id exactly as it always did; a **dispatcher** decides when it
starts. With no limits declared that is the same instant and nothing about
hestan looks different. With limits declared, the run waits.

The queue is the `runs` table — a queued run is one with `claimed_by IS NULL`
— so it survives a restart, and anything that can reach the database can pull
from it.

```rust
Hestan::new()
    .job(orders_etl)
    .max_concurrent_runs(8)          // across the whole deployment
    .tag_limit("env", "prod", 2)     // whatever the jobs are
    .priority(0)                     // the default position in the queue
    .serve(([0, 0, 0, 0], 4000))
    .await
```

`GET /api/queue` reports the depth, each waiting run's position, and what is
holding it back. The runs page shows the same thing with a bump button.

### Limits

| limit | scope | where |
| --- | --- | --- |
| `Hestan::max_concurrent_runs(n)` | the whole deployment | stored, shared |
| `JobBuilder::max_concurrent_runs(n)` | one job | stored, shared |
| `Hestan::tag_limit(k, v, n)` | every run carrying that [tag](launching.md#run-tags) | stored, shared |
| `Hestan::slots(n)` | **this process** | in memory, per process |

Every one of them counts runs that are **executing** — claimed and not
finished. A run sitting on the queue costs nothing and counts as nothing.

`slots` is the odd one out and the one people forget. The others say how much
work the deployment does at once; `slots` says how much of it lands in this
container. Without it the first worker to look at the queue claims all of it
and the worker beside it has nothing to do — a worker should take what it can
run, not what it can see.

Limits are read at the top of every dispatch pass, so raising one drains the
queue it was holding back without a restart.

### Limits are not overlap policies

They answer different questions and it is worth being clear which you want.

[`Overlap`](scheduling.md#overlap-policies) decides whether a scheduled fire
should **exist at all** while its job still has a run outstanding — a policy
about the work. A concurrency limit decides how many runs **execute at once**
— a policy about the machine.

Concretely: a queued run counts as outstanding for overlap, so a job with
`Overlap::Skip` whose runs are being held back by a limit does not pile up a
fire per minute behind them. That is deliberate. `Overlap::Skip` exists to
prevent exactly that pile-up, and a limit is not a reason to start ignoring
it. The same reasoning gates backfill chunking and the asset build endpoints:
"is there a run of this outstanding" is the question, and a queued run is
outstanding.

### Priority

`Hestan::priority(n)` sets the default and `{"priority": n}` overrides it per
launch. Higher goes first, ties broken by creation time. Negatives are legal
and are how a deployment says "these are the background ones".

**Priority is a preference, not an order.** The dispatcher skips a run a limit
would block and starts the next one that fits. So a high-priority `env:prod`
run waiting on its tag limit does not hold up an unrelated low-priority run
behind it. The alternative is head-of-line blocking, where one blocked run
stops a queue that has capacity sitting idle, and that is the worse trade —
but it does mean the start order is not the priority order, and you should not
build anything on the assumption that it is.

## Claims and leases

A queued run must be startable by exactly one claimer, and a claimer that dies
must not strand it forever.

Claiming is a compare-and-set:

```sql
UPDATE runs SET claimed_by = ?, claimed_at = ?, lease_until = ?
WHERE id = ? AND claimed_by IS NULL AND status = 'queued'
```

One winner by construction, whoever else is looking at the same row. The
counting of capacity and the claim share one immediate transaction, so two
dispatchers cannot both read the last free slot and both fill it. On postgres
this becomes `SELECT ... FOR UPDATE SKIP LOCKED` and holds no global write
lock; sqlite serializes writers for us, which is the same guarantee on one
host and no guarantee at all across several.

`claimed_by` is the claimer's **instance id**: eight hex digits, made once per
process, reported by `GET /api/health` along with the runs that process is
currently holding. Three workers and one run is a question you can answer.

A claimer renews `lease_until` every 15 seconds and the lease is good for 60,
so four beats have to be missed before anything is taken — a slow store is not
a dead process. A run whose lease has expired is reclaimed by whichever
process notices: its ops are marked with `claimer went away`, naming the
claimer, and then

- **`Reclaim::Fail`** (the default) fails the run and fires the failure hooks.
  A run that got halfway may have done half its side effects; doing them again
  quietly is worse than a stall somebody has to look at.
- **`Reclaim::Requeue`** puts it back on the queue for another claimer. Right
  when the work is idempotent and available beats exact.

```rust
Hestan::new().reclaim(Reclaim::Requeue)
```

### Boot recovery respects a live claim

The startup sweep used to mark every `queued` or `running` run failed, on the
assumption that the process starting up was the only one there had ever been.
With a claimable queue that assumption is destructive: a second process
starting would fail a live one's in-flight runs, mid-run.

It is now a mechanism rather than an assumption. A run is swept only if its
lease has expired, or if it is `running` with no claim at all (which can only
be a row written before the queue existed). A **queued run nobody has claimed
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

The [retention sweep](storage.md#retention) is on the same side of that line
and for a sharper reason: a worker owns none of the history, and one pruning
the scheduler's runs is data loss nothing reports. So is
[notification delivery](notifications.md#durable-delivery) — two processes
delivering would send every alert twice — which means the hooks want
registering on the process that decides.

`Hestan::work(addr)` is `role(Role::Worker)` with the address made optional,
because a worker may want no socket at all. Give it one and you get the same
ui, which is worth having for `/api/health`.

Every role runs the lease loop, including a scheduler holding nothing:
noticing a dead claimer cannot be the dead claimer's job.

Every process must build the **same registry**. A worker executes runs a
scheduler wrote, and the two have to agree about what a job is; a run whose
job this process does not define is left on the queue and reported as blocked
rather than claimed and failed. In practice this means one binary started with
different roles, which is what the compose file below does.

### A queue worker is not an op subprocess

Two mechanisms in hestan spawn processes and they are not the same thing.

| | [op subprocess](isolation.md) | queue worker |
| --- | --- | --- |
| started by | `Op::isolated()`, per attempt | you, per deployment |
| lives for | one op of one run | as long as you leave it up |
| claims | nothing | whole runs, off the queue |
| purpose | containment: a segfault costs one op | throughput: more hands |

A queue worker spawns op subprocesses like any other hestan process does. The
environment variables that mark an op subprocess are `HESTAN_ISOLATED_RUN` and
`HESTAN_ISOLATED_OP`, and every entry point checks for them first.

## The compose example

`Dockerfile` and `docker-compose.yml` at the repo root run the demo as one
scheduler and two workers against a shared volume:

```
docker compose up --build
open http://localhost:4000
```

Watch the queued section on the runs page fill and drain, and `claimed_by` on
a run say which worker took it. Both workers have `HESTAN_SLOTS=2` and the
deployment has `HESTAN_MAX_CONCURRENT_RUNS=4`.

It is one image. The scheduler and the workers differ only by `HESTAN_ROLE`,
because they must build the same registry.

## What this does not do

Two limits worth stating plainly rather than discovering.

**Multi-node needs a store every host can reach, and sqlite is not one.**
Everything above is multi-**process** on one host, which is real and useful and
is exactly the compose case: three containers, one volume, one file. It is not
multi-node. sqlite over a network filesystem does not lock correctly, and
hestan will not pretend otherwise by shipping a config for it. **A postgres
backend is the next piece of work**, and it is the only thing standing between
the compose example and several machines — the queue, the claims, the leases
and the roles are all already backend-agnostic; `claim_next` is one statement
that becomes `SKIP LOCKED`.

**There is no kubernetes executor and no celery integration, and this page is
not going to imply otherwise.** Dagster ships three executors because it grew
three ways to move work off-box. Hestan ships one, and a pod running
`HESTAN_ROLE=worker` against a shared postgres is what "the kubernetes
executor" would have been — once that postgres exists. Celery has no Rust
analogue worth porting; the queue and the workers above are the equivalent
capability, and calling them that is more useful than an integration page for
something that is not there.

## See also

- [scheduling](scheduling.md) — overlap policies, and why a queued run counts
  as outstanding.
- [isolation](isolation.md) — the other mechanism that spawns processes.
- [storage](storage.md) — the queue columns, and what boot recovery sweeps.
- [http api](http-api.md) — `/api/queue`, `/api/runs/{id}/priority`,
  `/api/health`.
