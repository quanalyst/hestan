# Metrics

`GET /metrics` is prometheus text exposition, served by every hestan process
that serves anything. it is on in every build: it is not behind a feature,
because unlike `otel` it adds no dependency, and every feature hestan has is a
dependency somebody should get to decline.

it exists because "is orchestration healthy" was a question you could only
answer by opening the ui or querying the run log yourself. the numbers were
already there; a surface a scrape can read was not.

```
$ curl -s -H "Authorization: Bearer $HESTAN_TOKEN" localhost:4000/metrics | head
# HELP hestan_store_up 1 while this process can read the run log. …
# TYPE hestan_store_up gauge
hestan_store_up 1
```

## Which side of the auth guard, and why

**inside it.** `/metrics` needs a viewer, the same as every other read, and an
[authenticated deployment](auth.md) refuses an unauthenticated scrape with a
401.

the argument for putting it outside is real: a scrape often cannot carry a
credential, which is exactly why `/api/whoami` is outside the guard and why the
kubelet probes in `deploy/k8s` point at it. that argument does not carry here,
for two reasons.

- **prometheus can hold a token and a kubelet cannot.** a kubelet's `httpGet`
  probe has nowhere to put a bearer token, so an endpoint it must reach has to
  be open or it cannot be probed at all. prometheus has `authorization` in
  every scrape config, and `ServiceMonitor` has `authorization.credentials`
  pointing at a secret. the constraint that forced `/api/whoami` open simply is
  not present.
- **it publishes the shape of a deployment.** no metric here carries a job
  name, and the section below is why, but how much work there is, how much of
  it fails, how far behind the scheduler is and whether anything is deciding
  are all on this page. that is the same class of thing `/api/jobs` and
  `/api/runs` return, and those are behind the guard.

`/api/whoami` gives one bit to a probe that has no other way to ask. `/metrics`
gives a deployment's shape to a scraper that has somewhere to put a credential.

a deployment with no authenticator serves `/metrics` to anyone who can reach
the port, exactly as it serves everything else. `serve` already
[refuses to bind a reachable address](auth.md) without one, so that is either
loopback or a deliberate `Auth::None`.

there is no switch for the endpoint itself. one would be a second way to
configure what the guard already covers; a deployment that does not want it
blocks the path at whatever is in front of it.

## What may be a label

**a job name, an asset name, an op name, a partition key and a run id are
never labels here, and cannot become ones by accident.** a metric with a label
per run id grows a series per run forever and kills the scrape that reads it;
one with a label per partition key does the same a day at a time.

that rule is a type rather than a note. a label value in `src/metrics.rs` is a
`&'static str`, and a name read out of a database row borrows from the row, so
it does not typecheck as one. what is left is the words hestan spells in its
own source, which is why every label below comes from a **closed** enum:
`RunStatus`, `Reclaim` and `TickOutcome` each have an `as_str` returning
`&'static str`, while an open enum's borrows from `self`. the one label that is
not a word is a histogram's `le`, and it is a number off a const list.

so the number of series this endpoint can emit is fixed at compile time.
`neither_a_job_name_nor_a_partition_key_can_add_a_series` fills a store with
two hundred distinct job names and two hundred partition keys and asserts the
series count is the one an empty store produces.

the cost is real and worth saying: **you cannot ask this endpoint which job is
failing.** it will tell you that runs are failing, and `/api/runs?job=…`, the
run log and the ui are where you find out which. that is a deliberate trade of
one dashboard drill-down against a metric that stays the same size as the
deployment grows.

## Deployment-wide or per process

the two halves of this page do not mean the same thing, and aggregating them
the same way is the mistake to avoid.

**gauges are read off the run log when the scrape arrives.** the run log is
shared, so every process answers with the same figure. aggregate them with
`max`, never with `sum`: three workers reporting a queue of four are not a
queue of twelve.

**counters and histograms belong to the process that was scraped**, and read
**zero after a restart**. `sum` and `rate` across targets are exactly right for
them.

that split is not a compromise, it is the only honest shape. every table hestan
keeps is prunable by [retention](replay.md#the-retention-horizon), so a
`_total` read off a `COUNT(*)` would fall the first time a prune ran, and
prometheus reads a counter that fell as a process that restarted and invents
the rate to match. a counter that resets at a restart is a shape prometheus
already knows how to handle. one that drops halfway through a Tuesday is not.

each counter is incremented at the call that has just written the same fact to
the run log, after the transaction committed. so a write that was refused and
retried moves it once, and a write that never landed does not move it at all:
the counter and the run log agree or neither of them says anything.

## The metrics

### Gauges, read off the run log (deployment-wide, take `max`)

| metric | what it is |
| --- | --- |
| `hestan_queue_depth` | runs written down and claimed by nobody |
| `hestan_queue_oldest_seconds` | how long the oldest of those has waited; 0 when the queue is empty |
| `hestan_runs_active` | runs claimed by some process and not yet terminal |
| `hestan_runs_stalled` | claimed runs past the lease they were claimed under: work nothing has reclaimed |
| `hestan_schedules_paused` | declared schedules that are paused |
| `hestan_sensors_paused` | declared sensors that are paused |
| `hestan_decider_held` | 1 while **this** process holds the decision lease |
| `hestan_decider_lease_seconds` | how long the lease has left, whoever holds it; 0 when nobody does |

`hestan_decider_held` is the odd one: it is read off the store like the rest,
and it is about this process, so it is how you find *which* target is deciding.
`hestan_decider_lease_seconds` is the same for every target, and is how you
find out whether anybody is.

### Gauges about the process (per target)

| metric | what it is |
| --- | --- |
| `hestan_store_up` | 1 while this process can read the run log. the eight gauges above are **missing** while it is 0 |
| `hestan_store_writing` | 1 while the last write this process attempted landed. a 0 is a process that has stopped claiming |
| `hestan_runs_given_up` | runs this process claimed and stopped executing without recording an outcome, waiting on a reclaimer |

### Counters (per target, zero after a restart)

| metric | labels | what it counts |
| --- | --- | --- |
| `hestan_runs_total` | `status`: `success`, `failed`, `canceled` | runs this process took to a terminal status |
| `hestan_run_claims_total` | | runs this process claimed off the queue |
| `hestan_run_reclaims_total` | `outcome`: `failed`, `requeued` | claims taken back from a holder that stopped renewing |
| `hestan_op_retries_total` | | op attempts that failed with another attempt still to come |
| `hestan_schedule_fires_total` | `outcome`: `fired`, `caught_up`, `skipped`, `deferred`, `error` | occurrences this process accounted for |
| `hestan_store_write_retries_total` | | writes the store refused that hestan tried again |
| `hestan_store_unrecorded_writes_total` | | writes recording what a run did that never landed |
| `hestan_store_dropped_writes_total` | | best-effort writes let go: an event, a captured line, a pid |

the last two are the numbers `/api/health` has always carried, on a surface a
scrape can read. the first of the three is new and is the leading indicator:
retries climbing while the other two stay flat is a database stumbling and
recovering.

**a process counts what it did and nothing else.** a run is executed by the one
process that claimed it, so `sum()` across every target is the deployment's run
count, and a [scheduler](scaling.md#roles) that executes nothing reports zeros
for `hestan_runs_total` and `hestan_run_claims_total` forever. that is correct
rather than broken: it is the answer to "is this process doing any work".

### Histograms (per target, zero after a restart)

| metric | buckets (seconds) | what it observes |
| --- | --- | --- |
| `hestan_run_claim_delay_seconds` | 0.5, 1, 2, 5, 15, 60, 300, 1800 | queued to claimed |
| `hestan_schedule_lateness_seconds` | 1, 5, 15, 60, 300, 900, 3600 | due to fired |

buckets rather than quantiles, because a quantile computed per process cannot
be merged with another process's: three workers each reporting a p99 say
nothing about the deployment. buckets add.

two things they measure that are easy to misread. claim delay is measured from
when a run was **written down**, so a run that was requeued after a reclaim
reports its whole wait rather than the wait since the requeue. and lateness
includes catch-up fires, which are legitimately hours late after downtime: that
is the `caught_up` value on `hestan_schedule_fires_total`, and it is what tells
a slow scheduler apart from a deployment that restarted.

## What to alert on

a metric nobody alerts on is a row in a time series database forever. these are
the ones worth a rule, roughly in the order they matter.

**the store is losing what runs did.** page immediately.

```promql
increase(hestan_store_unrecorded_writes_total[15m]) > 0
min(hestan_store_writing) == 0            # for: 2m
min(hestan_store_up) == 0                 # for: 2m
```

the first is run outcomes that were never written down; every one is a run page
missing what happened. the second is a process that has stopped claiming and is
therefore doing nothing while still answering http, and the third is the same
process on the read side.

**`min`, not `max`, and this is the trap.** these three are per process, so one
worker whose database went away is one target at 0 and the rest at 1: `max`
would report the deployment healthy for exactly as long as one process still
was. the deployment-wide gauges are the other way round, which is why they say
`max` below. leave the target labels on if you want the rule to name the pod.

a process that is not answering at all renders nothing, so none of these fire
for it: that one is prometheus's own `up == 0`, and no metric hestan writes can
report its own absence.

**nothing is deciding.** page after a couple of minutes: no schedule fires, no
sensor evaluates and no policy builds while this holds.

```promql
max(hestan_decider_lease_seconds) == 0    # for: 2m
```

a gap of one lease is a handover, which is why the `for` is not seconds. see
[scaling](scaling.md#the-deciding-lease).

**the queue is stuck rather than deep.** depth alone is not an alert: a deep
queue that is draining is a busy deployment. age is.

```promql
max(hestan_queue_oldest_seconds) > 600    # for: 10m
```

pair it with `max(hestan_runs_active) == 0` on the same window if you want to
tell "no worker is claiming" apart from "the limits are holding everything
back".

**a process went away and its work is sitting there.**

```promql
max(hestan_runs_stalled) > 0              # for: 5m
sum(increase(hestan_run_reclaims_total[1h])) > 0
```

the first is the one to page on: a stalled claim that nothing reclaims is work
nobody is doing and nobody is being told about. the second is a warning rather
than a page, because a reclaim is the system working; a *rate* of them is a
deployment losing processes.

**runs are failing more than they were.**

```promql
sum(rate(hestan_runs_total{status="failed"}[30m]))
  / sum(rate(hestan_runs_total[30m])) > 0.1
```

pick the ratio your deployment actually holds; the shape is what matters. note
`canceled` is in the denominator and not the numerator, deliberately: a cancel
is somebody deciding, not a failure.

**the scheduler is behind.**

```promql
histogram_quantile(0.9,
  sum by (le) (rate(hestan_schedule_lateness_seconds_bucket[1h]))) > 300
```

fires that are minutes late are a deciding process that cannot keep up or a
store that is slow. exclude a deploy window if your restarts produce catch-up
fires.

**worth a graph, not a page.**

- `rate(hestan_op_retries_total[30m])` climbing is a dependency degrading
  before it fails.
- `increase(hestan_store_dropped_writes_total[1h])` is run pages losing events.
  survivable, and not silent.
- `max(hestan_runs_given_up) > 0` is the store having failed a process
  mid-run; the reclaimer picks those up, and if it does not,
  `hestan_runs_stalled` is the one that pages.
- `hestan_schedules_paused` and `hestan_sensors_paused` are **deliberate**, so
  they are a dashboard line and not a rule. they are here because "somebody
  paused it three weeks ago" is the most common answer to "why is nothing
  running".

## The overlap with `hestan doctor`

six of the gauges are facts [`doctor`](cli.md#doctor) already reports, off the
same rows: `hestan_schedules_paused` and `hestan_sensors_paused` are its
`schedules`/`sensors` check, `hestan_runs_stalled` is its `leases` check,
`hestan_decider_lease_seconds` is its `deciding` check, `hestan_queue_depth` is
half of its `queue` check, and `hestan_store_writing` is its `writes` check.
neither is computed from the other; both read the store.

they are not duplicates of each other because they answer at different times
and in different words. doctor is a person asking once and getting a sentence
and a fix (*"paused, so they will not fire: warehouse_healthcheck / unpause
schedule warehouse_healthcheck"*). a metric is a scrape asking every fifteen
seconds and getting a number, which is the only one of the two you can put a
threshold on.

what doctor has that this page does not, and deliberately: the reason and the
fix, and the checks that need the registry rather than the store. an automation
policy that can never fire, a rate that is per process, two asset colours that
collide, a retention policy in a role that never sweeps: each of those is
per-asset or per-declaration, and a metric carrying them would need exactly the
labels the section above rules out.

what this page has that doctor does not: `hestan_queue_oldest_seconds`, which
is what tells a deep queue that is draining apart from a shallow one that is
stuck, and the two histograms, which are about a trend rather than a moment.

## What is deliberately not here

- **op outcomes by status.** an op's terminal row is written by whichever
  process ran the op, and an [isolated op](isolation.md) writes its own from a
  child process that exits immediately afterwards, taking any counter with it.
  a per-process op counter would therefore undercount exactly the deployments
  that use isolation, silently. run outcomes have no such split, because a run
  is executed by the one process that claimed it. op detail stays in the run
  log and on `/api/runs/{id}`.
- **anything per job, per asset or per partition.** see the labels section
  above. this is the trade, not an oversight.
- **run duration.** the useful cut of it is per job, which is barred, and the
  aggregate across every job in a deployment is a number that means nothing.
  `/api/jobs/{name}/op_stats` is where duration lives.
- **sensor tick counts.** `/api/sensors` carries the two counts that answer "is
  this sensor healthy", per sensor. a metric could not carry them per sensor
  without a label per sensor, and summed across every sensor they are not a
  number anybody pages on.
- **asset freshness and staleness.** per asset, so barred, and
  [`on_late`](notifications.md) already alerts on the one that matters without
  going through prometheus at all.
- **a build info metric.** phase 46 left it out because nobody pages on a
  version string, and phase 50, which gave hestan a build identity to publish,
  agrees and left it out again. two reasons, and the second is the one that
  settles it.

  a `hestan_build_info{build="9f2c1ab"} 1` gauge is the standard prometheus
  shape for this, and its cardinality is defensible: one series per process per
  build, and the old series go stale at the next deploy rather than
  accumulating without bound. so the cardinality argument alone would not
  refuse it.

  what refuses it is the rule at the top of this page: **every label hestan
  emits is a `&'static str`, and that is both the rule and the whole of its
  enforcement.** a build identity is a string read out of the environment at
  start, which does not typecheck as one. publishing it would mean either
  giving that rule up, or leaking the string to get a `'static` out of it,
  which is the rule kept in letter and abandoned in spirit. neither is worth a
  series nothing alerts on.

  and there is somewhere better. `/api/health` carries the whole deployment
  identity, including hestan's own version, the schema version and the compiled
  features, which no metric was ever going to carry; `hestan doctor` says it in
  a sentence; and **the run rows carry the build that launched each of them**,
  which is the join a version string on a metric could not make. see
  [deployment and build identity](deployment.md).

## Scraping it

### A container

the compose stack's scheduler publishes 4000, so a prometheus on the host
scrapes `localhost:4000`. the workers publish no host port and are reached over
the compose network, by container name:

```yaml
scrape_configs:
  - job_name: hestan
    metrics_path: /metrics
    # /metrics is inside the auth guard, so a scrape carries the same token
    # the ui does. a file rather than an inline string keeps it out of the
    # config that gets pasted into a ticket
    authorization:
      credentials_file: /etc/prometheus/hestan-token
    static_configs:
      - targets:
          - hestan-scheduler-1:4000
          - hestan-worker-1:4000
          - hestan-worker-2:4000
          - hestan-worker-3:4000
```

**by container name and not by service name.** `worker:4000` on a compose
network resolves to whichever of the three replicas dns hands back, and a
target that reaches a different process each scrape reports counters that jump
around, which is the one way to make these numbers lie. the gauges would be
fine, because they are read off the shared run log; the counters would not.

[containers](containers.md#scraping-the-stack) has the same thing against the
stack that is running, with the numbers one occurrence produced on a scheduler
and on the worker that took the run it made.

### Kubernetes

[`deploy/k8s/podmonitor.yaml`](../deploy/k8s/podmonitor.yaml) selects every
hestan pod and points at `/metrics`, with `authorization.credentials` reading
the same key the pods read their token from. **like everything else in that
directory it has never been applied to a cluster**, and it assumes a prometheus
operator on top of that, which is a second thing nobody here has run: without
one, `kind: PodMonitor` is a resource the api server does not know.

a PodMonitor rather than a ServiceMonitor, because the counters are per
process. there is no service in front of every hestan pod (`service.yaml`
selects only the schedulers, so the ui and `/api/health` answer for a known
small set), so a ServiceMonitor would need a second service that exists only
to be scraped. and it selects **both** deployments: a worker publishes no
service and is the process that executes runs, so scraping only the schedulers
would give you no claim latency, no run outcomes and no reclaims at all.

if you scrape by annotation instead, both pod templates carry
`prometheus.io/scrape`, `prometheus.io/port` and `prometheus.io/path`. those
are inert on their own: something has to relabel on them, and that something is
also where the credential goes, because an annotation cannot carry one.

## See also

- [http api](http-api.md): every endpoint, including this one.
- [authentication](auth.md): the guard this sits behind, and the roles.
- [scaling](scaling.md): the queue, the claims, the leases and the deciding
  lease that half of these gauges are about.
- [the command line](cli.md): `hestan doctor`, which answers "why is nothing
  running" by asking the store the same questions.
- [containers](containers.md): scraping the compose stack.
