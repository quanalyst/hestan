# Scaling

hestan has **one** mechanism for moving work off the process that asked for
it: a durable, claimable queue. deployment shape decides where the claimers
live. a container and a pod are packaging around that one mechanism, not new
execution paths, which is why there is no kubernetes executor here, and no
docker executor, and no celery integration.

read [what this does not do](#what-this-does-not-do) first if you are sizing
hestan for a deployment. the limits are real and stated rather than papered
over.

## The queue

a launch is a request, not a start. `launch()` writes a `queued` run and
returns its id exactly as it always did; a **dispatcher** decides when it
starts. with no limits declared that is the same instant and nothing about
hestan looks different. with limits declared, the run waits.

the queue is the `runs` table (a queued run is one with `claimed_by IS NULL`),
so it survives a restart, and anything that can reach the database can pull
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
| `Hestan::rate(name, n, per)` | **this process**, and ops rather than runs | in memory, per process |

every one of them but the last counts runs that are **executing**, claimed and
not finished. a run sitting on the queue costs nothing and counts as nothing.
a [rate](#a-rate-is-per-process) is the odd one out twice over: it counts calls
rather than runs, and it is the other limit that lives in memory.

`slots` is the odd one out and the one people forget. the others say how much
work the deployment does at once; `slots` says how much of it lands in this
container. without it the first worker to look at the queue claims all of it
and the worker beside it has nothing to do: a worker should take what it can
run, not what it can see.

limits are read at the top of every dispatch pass, so raising one drains the
queue it was holding back without a restart.

### Limits are not overlap policies

they answer different questions and it is worth being clear which you want.

[`Overlap`](scheduling.md#overlap-policy) decides whether a scheduled fire
should **exist at all** while its job still has a run outstanding, a policy
about the work. a concurrency limit decides how many runs **execute at once**,
a policy about the machine.

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

**priority is a preference, not an order.** the dispatcher skips a run a limit
would block and starts the next one that fits. so a high-priority `env:prod`
run waiting on its tag limit does not hold up an unrelated low-priority run
behind it. the alternative is head-of-line blocking, where one blocked run
stops a queue that has capacity sitting idle, and that is the worse trade. but
it does mean the start order is not the priority order, and you should not
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
both fill it. so **when a limit is in force, and only then**, postgres
claimers take turns on an advisory lock for the length of one claim. with no
limits declared, which is the default, dispatchers never meet.

`claimed_by` is the claimer's **instance id**: eight hex digits, made once per
process, reported by `GET /api/health` along with the runs that process is
currently holding. three workers and one run is a question you can answer.

a claimer renews `lease_until` every 15 seconds and the lease is good for 60,
so four beats have to be missed before anything is taken: a slow store is not
a dead process. a run whose lease has expired is reclaimed by whichever
process notices: its ops are marked with `claimer went away`, naming the
claimer, and then

- **`Reclaim::Fail`** (the default) fails the run and fires the failure hooks.
  a run that got halfway may have done half its side effects; doing them again
  quietly is worse than a stall somebody has to look at.
- **`Reclaim::Requeue`** puts it back on the queue for another claimer. right
  when the work is idempotent and available beats exact.

```rust
Hestan::new().reclaim(Reclaim::Requeue)
```

a lease is also how a process says something it cannot write down. if a run's
critical write will not land after its retries, the process **stops executing
that run and stops renewing its lease**: it reports no status at all, because
at that point it does not know what the status is. one lease later the run is
reclaimed exactly as if the process had died, and `Reclaim` decides. that is
deliberate: a process that kept renewing a claim it had given up on would hold
that run out of every reclaimer's reach for as long as it stayed alive.
[what hestan promises about writes](concepts.md#what-hestan-promises-about-writes)
is the whole of it, including why a run left `running` for a minute beats a
run reported `success` that nothing recorded.

such a process also stops claiming until a write of its lands (the lease
renewal itself is what tells it the store is back), so a worker with a broken
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
is left where it is**: that row is not a casualty, it is the queue.

### Stopping a process on purpose

`serve` and `work` listen for SIGTERM and SIGINT. a headless one-shot
(`run_once`, `build_asset`) does not, and dies on a signal exactly as any
program with no handler does: it exists to execute the run it was asked for,
and a handler that made it wait would be a worse bug than no handler at all.

what a stop does, in order:

1. the http server stops accepting and finishes the requests it has. the
   [event stream](events.md#following-the-log) ends here rather than holding a
   connection open, because it is the one response with no natural end.
2. this process claims nothing more. a run claimed now is a run it would hand
   straight back.
3. the loops that decide stop, and the
   [deciding lease](#the-deciding-lease) goes back, so the next process takes
   the term without waiting out the expiry.
4. what is already in flight gets until `Hestan::stop_within` to finish.
   **eight seconds by default**, which fits inside the ten `docker stop` gives
   with room for the rest of this.
5. whatever did not finish is **released**: back on the queue, claim cleared,
   ops back to `pending`, and a `run_released` line in the log saying so.

```rust
Hestan::new().stop_within(Duration::from_secs(25)).work(None).await
```

a **second signal** skips step 4. ctrl-c twice is somebody saying something,
and the second one is not swallowed.

three things follow, and the first of them is a cost:

- **a process takes longer to exit than it used to**, because it is finishing
  work instead of dying. an idle one is gone in milliseconds; one holding a run
  waits for the run, up to the deadline. a rolling restart timed around a
  process that died instantly is timed around a different number now.
- **released is not resumed.** whoever claims the run next runs it from the
  beginning, so an op that already ran runs again. that is the same trade
  [`Reclaim::Requeue`](#claims-and-leases) makes and it wants the same thought:
  side effects happen twice.
- **a released run is not a failed one.** nothing about it went wrong and
  nothing records an outcome for it, so no failure hook fires and no alert is
  sent. it is a queued run again.

what a stop deliberately does not do is cancel anything. an
[isolated](isolation.md#when-the-orchestrator-itself-is-stopped) op's child is
not signalled by hestan: it is being given time to finish. if the deadline runs
out first the parent drops it, and dropping it kills it, so a released run is
never being executed by a process that has left.

## Roles

```rust
Hestan::new().role(Role::Scheduler).serve(addr).await   // decides
Hestan::new().work(None).await                          // executes
Hestan::new().serve(addr).await                         // both; the default
```

| role | schedules, sensors, freshness, automation policies, backfill chunking, retention, notification delivery | claims and executes |
| --- | --- | --- |
| `Role::All` (default) | yes | yes |
| `Role::Scheduler` | yes | no |
| `Role::Worker` | no | yes |

**one process at a time is `All` or `Scheduler`**, and which one is now settled
by [a lease in the store](#running-more-than-one-scheduler) rather than by you
counting containers. schedules, sensors, freshness checks,
[automation policies](assets.md#automation-policies) and backfill chunking are
decisions, and the deployment makes each one once. **any number of processes may
be `Worker`**; that is the entire point of a claimable queue.

the [retention sweep](storage.md#retention) is on the same side of that line
and for a sharper reason: a worker owns none of the history, and one pruning
the scheduler's runs is data loss nothing reports. so is
[notification delivery](notifications.md#durable-delivery) (two processes
delivering would send every alert twice), which means the hooks want
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

## Running more than one scheduler

start two, or ten. one of them decides and the rest wait, and nothing about
that is your job to arrange.

### The constraint is the guarantee

**this is not built on the lease, and the order matters.** what makes a
duplicate decision impossible is the store:

- one `fired` tick per `(job, expr, scheduled_for)`, on a
  [unique index](storage.md#one-fire-per-occurrence), with the tick and the run
  written in one transaction so a refused tick launches nothing.
- one run per `(sensor, run_key)`, on the `sensor_run_keys` primary key, the
  same way and since [run keys](sensors.md#run-keys) existed.

a distributed lock fails exactly when a process pauses, a disk stalls or a
network splits, which is to say it fails at the moment its holder is most
certain it still holds it. a unique index does not have moments. so the
constraint went in first and the lease went in on top of it: **correctness comes
from the constraint, and the lease is what stops the duplicate being
attempted.**

### The deciding lease

one row in the `decider` table, in the same vocabulary the
[run lease](#claims-and-leases) uses: `claimed_by`, `claimed_at`,
`lease_until`. a process that decides takes it at boot, renews it every two
seconds, and loses it by failing to renew for ten. anybody may take an expired
one.

what the run lease has no need of is the **term**, a counter that goes up on
every acquisition and never on a renewal. a decision is written under the term
its process believes it holds, and the store checks that term in the same
transaction as the write. that is what a lease alone cannot do: a leader that
stops the world past its own expiry and resumes agrees with every check it makes
in its own memory, and disagrees with the row.

`Role::All` pays nothing for this in the ordinary case. one process on one
database finds the row free, takes it before `serve` binds its socket, and never
contends with anybody. the exception is worth stating: a process **killed**
without handing the lease back leaves it held until it expires, so a restart
inside that window waits up to ten seconds before it decides anything. ten
seconds of nobody deciding is ten more seconds of downtime, and
[catch-up](scheduling.md#missed-fire-catch-up) already has an answer for
downtime.

**a stop is the other case, and a deploy is a stop.** a process
[asked to stop](#stopping-a-process-on-purpose) hands the lease back, so the
row is free before it has finished leaving and what is left is how long the
next process takes to look: one two-second renewal rather than the rest of a
ten-second lease. measured in containers, with hestan as pid 1 and no init shim
in front of it:
[containers](containers.md#signals-and-what-a-stop-is-worth) has the stop and
the kill side by side.

### What the term fences, and what it does not

**fenced**: every run a deciding loop launches. a cron fire, a sensor request
(keyed or not), an [automation policy](assets.md#automation-policies) build and
a [backfill](assets.md#backfills) chunk are all refused by the store if the term
they name has moved on, in the transaction that would have written them. nothing
is written and no run exists.

**not fenced**, and here is what each one costs if a leader pauses past its
lease and wakes up:

| decision | what a stale decider can still do | what it costs |
| --- | --- | --- |
| the [retention sweep](storage.md#retention) | delete rows and files its policy says are past their policy | nothing the live decider would not also have deleted; the sweep is the same sweep whoever runs it |
| a [freshness](freshness.md) crossing | write `freshness_state` and call a late hook | one duplicate page for one asset going late |
| [durable delivery](notifications.md#durable-delivery) | send a notification and mark it delivered | one duplicate alert, which durable delivery is [already at-least-once](notifications.md#durable-delivery) about |
| a sensor cursor | commit a cursor over a newer one | a sensor re-reading a window, or skipping one |
| a schedule cursor | move it forward | nothing: the column only ever moves forward, so a stale writer cannot un-account for anything |
| a runless tick | record an occurrence as `skipped` or `deferred`, which is the [overlap policy](scheduling.md#overlap-policy) declining to make a run | a tick log row no live decider wrote. the unique index is over `fired` alone, so it neither blocks a real fire nor becomes one |
| the one boot sweep | it is lease-gated too, so nothing | nothing |

none of these launches a run, which is why none of them is on the fenced list:
the term rides on the run insert, and a decision with no run to insert has
nowhere to put it. the runless tick is the one that is easy to miss, because it
comes out of the same pass as a fire: a stale decider that wakes to find a run
of the job already active records a skip rather than attempting the fire the
store would have refused. `deploy/checks/partition.sh` saw that happen and
cycles until it gets a fire, because a partition where nothing was fenced
proves nothing about the fence. if any of them mattered enough to fence it would want a
constraint of its own rather than a second lock, for the reason at the top of
this section.

### Handover: fired late, or missed?

**an occurrence that comes due while nobody is deciding is an occurrence that
came due during downtime, and the schedule's own
[catch-up policy](scheduling.md#missed-fire-catch-up) decides what happens to
it.** the `schedules.cursor` column is how far the *deployment* has accounted
for, not how far a process has, so the process that takes over reads the dead
one's cursor and sees the gap.

- `Catchup::Skip`, the default: skipped. the cursor jumps the gap and no tick is
  written for it, exactly as after any other downtime.
- `Catchup::One`: the most recent missed occurrence fires, late, marked as a
  catch-up.
- `Catchup::All { limit }`: every missed occurrence up to the cap is accounted
  for, oldest first, subject to the job's [overlap policy](scheduling.md#overlap-policy).

a fire that was already **queued** when the decider went away survives the
handover whatever the policy says, because the tick log *is* the queue: a
`deferred` tick with no later tick for the same occurrence is on disk, and the
new decider drains it as its own.

### What two schedulers cost you

a second scheduler is a warm spare, not more throughput. it holds a connection,
builds the same registry and evaluates nothing. what it buys is that a killed
decider is a ten-second gap rather than an outage until somebody notices.

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

## A rate is per process

`Hestan::rate("api", 5, Duration::from_secs(1))` is five calls a second **from
this process**. two workers each honouring it send ten, and the system on the
other side sees ten and has no idea it was talking to two of anything. with
`HESTAN_ROLE=worker` on two hosts, a declared rate is per host and the external
system sees the sum.

that is not a footnote to bury. a rate exists to protect something outside
hestan, and the deployment shape that makes hestan scale is exactly the one
that breaks the promise. the bucket is memory: like `slots`, and unlike every
other limit on this page, it is not a row anybody else can read.

**what to do about it is arithmetic.** the number of workers is a number
somebody chose, so divide by it: three workers against an api that allows six
calls a second is `rate("api", 2, ..)` in the registry all three of them build.
that costs nothing, needs no database, and is exactly right for as long as the
count is known. where it genuinely is not (an autoscaler), the honest choices
are to size for the maximum you will ever run, or to keep the throttled work
where only one process does it.

this is asserted rather than described. `tests/queue.rs` starts two real worker
processes against one queue, each honouring "one call every two seconds", and
checks both halves of the truth: each process spaces its own calls a period
apart, and the two of them together put two calls inside one period.

### Why the bucket is not in the store

a bucket in the run log would hold across processes (a row per name, refilled
by elapsed time, decremented in a transaction), and it is deliberately not
here. the reasons are worth writing down, because they are also the reasons it
would have to be built differently than it sounds.

- **a round trip per call, on the path of every op that declares a rate.** the
  latency is not the problem; a millisecond either side of an http request is
  nothing. the write is. a token is an `UPDATE`, and at the rates people
  actually declare (5 a second, 100 a minute), that is a write transaction per
  call forever, against the same database the run log is going into. sqlite
  serializes writers, so those queue behind every `op_started` and every event;
  postgres does not, but the row is a single hot row and every worker wants it.
- **waiting would become polling.** the local bucket wakes a waiter at the
  instant its token arrives, because the waiter and the bucket are in one
  process. across processes there is nobody to wake: a worker that finds the
  bucket empty has to come back and ask, which is another round trip per waiter
  per interval, and the fairness goes with it, because then the worker that
  asks at the right moment wins rather than the op that has been waiting
  longest.
- **a token taken by a process that dies is gone.** locally a canceled op hands
  its token to the op behind it, because dropping a future runs code. a host
  that loses power runs nothing, so a shared bucket needs a lease per token (a
  row, an expiry, a sweeper), which is the claim machinery the queue already
  has, and a lot of it to protect a budget of five.
- **it would work on postgres only**, since two hosts cannot share a sqlite
  file, and the deployments that need a shared bucket are the ones on several
  hosts. "the limit you declared is kept across your deployment, if you run a
  database server" is a worse promise than one that is small and true
  everywhere.

so dividing is the answer for a known number of workers, which is most
deployments, and a shared bucket is a later phase's problem, one that would
start with the lease rather than with the row, and would be opt-in by name with
the local bucket still the default.

## The compose example

`Dockerfile` and `docker-compose.yml` at the repo root run the demo as
postgres, one scheduler and three workers, all from one image against one
database:

```
docker compose up -d --build
open http://localhost:4000
```

the ui asks for a token the first time, because the compose file binds
`0.0.0.0` and publishes the port: `serve` refuses a reachable address with
nothing checking who is asking, so the file sets `HESTAN_TOKEN` and the demo
picks it up. it is `demo-token-change-me`, which is what a token in a compose
file deserves to be called; [auth.md](auth.md) has where a real one comes
from.

watch the queued section on the runs page fill and drain, and `claimed_by` on
a run say which worker took it. each worker has `HESTAN_SLOTS=2` and the
deployment has `HESTAN_MAX_CONCURRENT_RUNS=4`.

it is one image. the scheduler and the workers differ only by `HESTAN_ROLE`,
because they must build the same registry.

[containers](containers.md) is the whole of it: what is in the image and what
is not, the role split as five containers, the second scheduler, and what
happened when the deciding process was cut off the network while it was still
running.

## Several hosts

everything above works on one host with the default sqlite file: several
processes, one file, and no server to operate. past one host you need a store
every host can reach, and that is what the
[postgres backend](storage.md#postgres) is:

```rust
Hestan::new().db("postgres://user:pw@db.internal/hestan").work(None).await
```

built with `--features postgres`. nothing else about a deployment changes. the
queue, the claims, the leases and the roles were always backend-agnostic and
they still are: one scheduler deciding at a time, any number of workers, the
same registry in every process.

**what is proven and what merely follows.** hestan's suite runs the queue
cases twice: worker processes racing one sqlite file, and worker processes
racing one postgres schema, asserting in both that no run executed twice. it
runs the deciding cases the same way twice: two scheduler processes against
one database, asserting that no occurrence is fired twice, that one of them
decides and the other fires nothing at all, and that killing the one that
decides hands the next occurrence to the other. both
of those are several *processes* against one database.

[containers](containers.md) adds a third shape and one fault the others cannot
produce. the compose stack is four hestan *containers* against one postgres,
so every process has a pid and network namespace of its own, and a network is then a
thing that can be taken away: `deploy/checks/partition.sh` cuts the process
holding the deciding lease off the database while it goes on running, and finds
its next decision refused by the store on the term it named. that is the first
time the [fence](#what-the-term-fences-and-what-it-does-not) has been tested
rather than reasoned about. it is still one host.

nobody has run hestan's workers on several *hosts*, because the machine the
suite runs on is one machine, and a process on another host differs from a
process on this one only in which socket it opens. it follows, and the
difference between "it follows" and "it was run" is the difference this
paragraph exists to keep.

two things do still hold whatever the backend. **one process at a time
decides**, and both backends enforce it the same way: the unique index over the
occurrence and the [deciding lease](#running-more-than-one-scheduler) are the
same schema on either. and **sqlite is still right** for one host: it needs no
server, and reaching for postgres to run one container is a database to
operate for nothing. two deciding processes on one sqlite file work, and are a
strange thing to want: they have to share a filesystem to share the file, so
they are on one host, and a spare decider on the host that just died is not a
spare.

## What this does not do

**there is no kubernetes executor and no celery integration, and this page is
not going to imply otherwise.** hestan ships one mechanism for moving work
off-box rather than several, and a pod running
`HESTAN_ROLE=worker` against a shared postgres is what "the kubernetes
executor" would have been. that is the mechanism, and there is no operator, no
pod template and no autoscaler around it. celery has no rust analogue worth
porting; the queue and the workers above are the equivalent capability, and
calling them that is more useful than an integration page for something that is
not there.

## See also

- [containers](containers.md): the image, the compose stack, and what a
  partitioned decider actually did.
- [scheduling](scheduling.md): overlap policies, and why a queued run counts
  as outstanding.
- [isolation](isolation.md): the other mechanism that spawns processes.
- [storage](storage.md): the two backends, the queue columns, and what boot
  recovery sweeps.
- [concepts](concepts.md#rates): what a rate is, the bucket behind it, and
  what waiting for a token does.
- [http api](http-api.md): `/api/queue`, `/api/runs/{id}/priority`,
  `/api/rates`, `/api/health`.
