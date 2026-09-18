# Metrics

`GET /metrics` exposes Prometheus text metrics in every serving build, without
an optional feature. On an authenticated deployment, include a bearer token:

```sh
curl -H "Authorization: Bearer $HESTAN_TOKEN" localhost:4000/metrics
```

## Which side of the auth guard, and why

`/metrics` requires viewer access, like other read endpoints. Configure scrape
authorization on authenticated deployments; an unauthenticated request returns
401. With no authenticator, anyone who can reach the port can scrape it.

There is no endpoint-specific disable switch. Restrict the path at a proxy if
needed. `/api/whoami` remains available for unauthenticated health probes.

## What may be a label

Labels use fixed values from closed enums. Job, asset and op names, partition
keys, run IDs and build identifiers are excluded, keeping the number of series
bounded independently of deployment size.

Use `/api/runs`, the event log and the UI to identify individual failing jobs;
metrics report aggregate health.

## Deployment-wide or per process

Store-backed gauges are repeated by each process sharing the store. Aggregate
with `max`, not `sum`: three workers reporting four queued runs still mean four
queued runs. Process gauges are identified separately below.

Counters and histograms belong to each process and reset on restart. Use `sum`
and `rate` across targets. Counters increment after successful commits; retention
does not reduce them.

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

Metrics and [`doctor`](cli.md#doctor) share store facts about paused schedules
and sensors, expired leases, deciding, queue depth and write health. Doctor adds
explanations, remedies and registry checks such as unsatisfiable policies and
shade collisions. Metrics add continuous measurements, queue age and histograms.

## What is deliberately not here

Per-op counters would omit short-lived isolated children, so op outcomes remain
in the run log. Per-job durations, sensor health and asset freshness are exposed
through their API resources and [late hooks](notifications.md), without adding
unbounded metric labels. Build identity is in health, doctor and run rows; see
[deployment](deployment.md).

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

See [containers](containers.md#scraping-the-stack) for a manual scrape command.

### Kubernetes

[`deploy/k8s/podmonitor.yaml`](../deploy/k8s/podmonitor.yaml) selects every
hestan pod and points at `/metrics`, with `authorization.credentials` reading
the same key the pods read their token from. It requires the Prometheus
Operator. The manifests have not been validated on a cluster.

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

Named notification metrics include `hestan_notification_deliveries{state}`,
`hestan_notification_expired_claims`, and `hestan_notification_oldest_pending_seconds`
as deployment-wide gauges. `hestan_notification_attempts_total{outcome}` and
`hestan_notification_attempt_seconds` describe outcomes recorded by this process.
No destination URLs or run identifiers appear in metric labels.
