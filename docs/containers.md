# Containers

hestan is a library, so there is no hestan image to pull. what goes in an
image is **your** binary: the one that builds a registry and calls `serve` or
`work`. this page is that pattern, built and run rather than described, plus
what happened when the process holding the deciding lease was cut off from the
database while it was still running.

- [`Dockerfile`](../Dockerfile) builds `examples/demo.rs` into an image.
- [`docker-compose.yml`](../docker-compose.yml) runs postgres, one scheduler
  and three workers from it.
- [`deploy/checks/`](../deploy/checks) is the fault injection, as scripts.
- [`deploy/k8s/`](../deploy/k8s) is a set of manifests **that has never been
  applied to a cluster**, and says so on every file.

## The image

one binary, started with a role. a scheduler and its workers are the same
image because they must build the [same registry](scaling.md#roles): a worker
executes runs a scheduler wrote, and the two have to agree about what a job is.

```
docker build -t hestan-demo .
docker run -p 4000:4000 -e HESTAN_ADDR=0.0.0.0:4000 -e HESTAN_TOKEN=… hestan-demo
```

it is two stages: `rust:1.88-slim-bookworm` compiles, `debian:bookworm-slim`
runs. 1.88 is the crate's own `rust-version`, so the image is built by the
oldest compiler the crate claims to work with. neither base is a `latest` tag.

### What is in it

measured on `linux/arm64`:

| | |
| --- | --- |
| unpacked on disk | 164mb |
| content to pull | 35.5mb |

and the runtime layer is five things, oldest first:

| layer | size |
| --- | --- |
| `debian:bookworm-slim` | 108mb |
| `/usr/share/ca-certificates` | 582kb |
| `/etc/ssl/certs` | 1.38mb |
| the `hestan` user, uid 10001 | 45kb |
| the binary | 18.8mb |

the debian base is most of it, and it is shared with anything else on the host
built on the same base. the binary is a release build with its symbol table
left in.

**the certificates are there for your ops, not for hestan.** hestan's own http
client is rustls with its roots compiled in and would work without them. an op
that calls somebody's api through a client that reads the system store finds an
empty store without them, and finds it at the worst possible moment.

**there is no curl, no wget and no psql, and no package manager ran in the
runtime layer at all.** the certificates are copied out of the build stage. the
container runs one process and has nothing to probe itself with, so the health
checks in the compose file and the probes in the manifests are made from
outside it, which is where a health check belongs.

### The ui is copied in, not built

the ui is embedded with `include_dir!`, so `ui/dist` has to exist before rustc
runs. it is a **committed** directory rather than something the image build
produces, because the crate has to compile on docs.rs and in anybody's `cargo
install` with no node anywhere. so the image installs no node and runs no npm.

the price is worth stating: the image carries whatever `just ui-build` last
wrote. changing the ui means rebuilding it, committing it, and then building
the image, in that order.

### Features

`--features cli,postgres`. `cli` because `examples/demo.rs` declares it as a
required feature; `postgres` because the compose stack shares one database
between a scheduler and three workers. not `--all-features`: parquet pulls in
arrow, which is tens of megabytes of build for a deployment whose op outputs
are `{"loaded": 4210}`.

nothing is apt-installed in the build stage either, which is a claim rather
than an omission. `rusqlite/bundled` compiles sqlite from source and wants a c
compiler, which the rust image has. tokio-postgres speaks the wire protocol in
rust, so there is no libpq. reqwest is rustls over ring, so there is no openssl
and no pkg-config.

## The compose stack

```
docker compose up -d --build
open http://localhost:4000        # the scheduler's ui, token in the file
```

five containers: postgres, one scheduler, three workers. the scheduler fires
the demo's schedules and enqueues runs and executes none of them; the workers
claim runs off the queue and run them. watch the queued section on the runs
page fill and drain, and `claimed_by` on a run say which worker took it.

the difference between a scheduler container and a worker container is one
environment variable, `HESTAN_ROLE`. that is the whole role split from
[scaling](scaling.md#roles) as a thing that runs.

the workers publish no host port, because there is nothing to reach a
particular one of them for. their ui is on 4000 inside the compose network, and
anything on that network can ask:

```
worker=$(docker inspect -f \
  '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' hestan-worker-1)
docker run --rm --network hestan_default busybox:1.36 \
  wget -qO- --header="Authorization: Bearer demo-token-change-me" \
  "http://$worker:4000/api/health"
```

that is how `deploy/checks/stack.sh` asks all five processes about the same run
and checks they agree. the request comes from a container beside them because
the hestan image has no http client in it.

`docker-compose.spare.yml` adds a second scheduler on port 4001. it holds a
connection, builds the same registry and evaluates nothing until the deciding
lease runs out. the fault checks need it, because a handover wants somewhere
to hand to.

### The honest limit

**this is five containers on one host.** each is its own pid and network
namespace, with its own loopback and its own view of the process table, which
is closer to a deployment than five processes sharing one namespace. it is
still one kernel, one clock and one machine to lose. nobody has run hestan on
several hosts, and [scaling](scaling.md#several-hosts) says so in the same
words.

what containers do buy over the multi-process tests in `tests/queue.rs` is that
a network is now a thing that can be taken away, which is what the next section
is about.

## Cutting the deciding process off

`docker network disconnect` on a running container is a real partition. the
process keeps running, its clock keeps going, and it cannot reach the database.
several processes on one box cannot produce that: there, a leader that has
"lost" the store is a leader somebody stopped, which is a different and much
easier thing to be right about.

the scripts are in [`deploy/checks/`](../deploy/checks) and run like this:

```
bash deploy/checks/run.sh                     # all of them, about six minutes
bash deploy/checks/partition.sh               # one of them
PARTITION_SECS=120 bash deploy/checks/partition.sh
```

they are not `cargo test` cases and will not become them. a partition takes a
minute of wall clock to be a partition, and a test binary that takes six
minutes is a test binary nobody runs. they want a docker daemon, the compose
plugin, psql, python3, and host ports 4000, 4001 and 55432.

each one brings up a stack of its own, does one thing to it, asks the **store**
what happened rather than asking a process that may be the one that is wrong
about it, and tears the stack down.

### What it showed

every number below is a measurement, on one machine, of the runs named. none
of them is a guarantee.

**a partitioned leader stops deciding.** the container stayed up with no
restarts and its clock stayed within a second of the host's, asked over
`docker exec`, which reaches a container that has no network at all. it decided
nothing for the whole 65 seconds it was cut off. another process took the term
and fired the five or six occurrences that came due meanwhile.

**it does not come back the instant the network does.** its store calls sit
inside a tcp write that has been backing off for the length of the partition,
so what decides when it notices is a retransmit timer and not anything hestan
chose. **between 3.2 and 44.5 seconds, over seven partitions of 50 to 95
seconds.** a partitioned process is unreachable for longer than the partition,
and by an amount nothing in hestan controls; that is worth knowing before you
time a deploy around one.

**reconnection does not resurrect it, and this is the term fence.** when the
blocked calls returned, the process went straight on with the pass it had been
in the middle of:

```
16:14:41.915307  INFO  schedule fired job=orders_etl expr=*/10 * * * * *
16:14:41.915615  WARN  fire refused: the deciding lease moved on before the fire landed
16:14:41.915706  WARN  deciding: the lease is no longer this process's term=1
```

read the order. the process decided to fire, the store refused it, and **only
then** did its own decider loop find out the lease had moved on. so the term
that fire named was 1, and 1 was an acquisition out of date. there is no
in-process check between deciding to fire and writing: the refusal came from
the store, in the transaction that would have written the run, and it left no
tick and no run behind. that is what
[the fence](scaling.md#what-the-term-fences-and-what-it-does-not) was built
for, and until this it had only ever been reasoned about.

the two loops race, and on other runs the decider loop noticed first by a
millisecond or two, which means the term the fire carried was zero rather than
one by the time the transaction read it. the store refuses that in the same
statement for the same reason, and the process still went ahead and tried.

two things to be precise about rather than let the paragraph above carry more
than it should.

- the occurrences the stale process tried to fire had already been fired by the
  live decider, so the [unique index](storage.md#one-fire-per-occurrence) would
  have refused them too if the fence had not. the fence is checked first and is
  what the process reported, and it is the check that would still refuse an
  occurrence nobody else had fired.
- **a reconnecting stale decider does not always attempt a fire, and when it
  does not, nothing is fenced.** a pass that finds a run of the job already
  active applies the [overlap policy](scheduling.md#overlap-policy) instead and
  records a `skipped` tick, which creates no run, so it names no term and goes
  into the store unrefused. it costs a row in the tick log that no live decider
  wrote; the unique index is over `fired` alone, so it neither blocks a real
  fire nor becomes one. `partition.sh` cycles until it gets a fire and fails if
  it never does, because a run where nothing was fenced proved nothing about
  the fence.

**handover is bounded, and the bound is the lease.** with the leader killed
outright by `docker kill`:

| | |
| --- | --- |
| lease left at the moment of the kill | 8.91s |
| until another process held the term | 8954ms |
| until the new decider fired something | 9152ms |

over four kills the handover was 8286ms, 8954ms, 9934ms and 10425ms. the
deciding lease is ten seconds and its holder renews every two, so the bound is
whatever was left of the lease plus up to one renewal interval, and 10425ms is
that upper end rather than a surprise.

the second number, time to the next decision, is not that bound and should not
be read as one: it was 9152ms on that run and 19435ms on another, because the
occurrence that came due inside the gap was not fired at all. nobody was
deciding, which is downtime, and the schedule's
[catch-up policy](scheduling.md#missed-fire-catch-up) decides what happens to
it. the demo's is the default, `Catchup::Skip`, so it was skipped and the next
one ten seconds later was the first thing the new decider fired.

**no occurrence fired twice**, across a partition, a handover and a reconnect
run one after the other: cut the leader off, let the spare take over, kill the
spare while the first one is still cut off so that nobody is deciding, then let
the first one back in holding a term two acquisitions out of date. one fired
tick per occurrence and one run per occurrence, every time, which is the unique
index doing the job it was built first to do.

### What none of this tested

the fence held under a partition on one host. it has not been put under a
partition between hosts, a clock that jumps, or a process paused past its own
expiry by a stop signal or a long gc pause. the last of those is the case the
fence's doc comment is written about and it is still reasoning rather than a
measurement.

## Signals, and why a container takes ten seconds to stop

**hestan installs no signal handler**, and the kernel drops an unhandled signal
sent to pid 1 of a container rather than applying its default action. so
SIGTERM does nothing to a hestan container:

| | |
| --- | --- |
| `docker stop` on this image | waited its full 10s timeout, exit 137 (SIGKILL) |
| the same image behind `docker run --init` | exited on the signal in 196ms, exit 143 (SIGTERM) |

three things follow, and none of them is hypothetical.

- `docker compose down` takes about ten seconds per hestan container. `init:
  true` on the service, or a signal handler in your own binary, is how to make
  that quick.
- a container being stopped goes on working normally until SIGKILL arrives. a
  worker keeps claiming runs the whole way through its grace period.
- **the deciding lease is never handed back on a stop.** `serve` hands it back
  when `serve` returns, and a signal is not what makes `serve` return.
  [scaling](scaling.md#the-deciding-lease) says a clean stop hands the lease
  back; in a container there is no clean stop, so a restart inside the lease
  waits up to ten seconds before it decides anything. that is the same ten
  seconds a kill costs, which is the case the expiry exists for.

## Kubernetes, written and not run

[`deploy/k8s/`](../deploy/k8s) has a ConfigMap, a Secret, a Deployment for the
schedulers, a Deployment for the workers, a Service, and a kustomization.

**none of it has been applied to a cluster.** no kubernetes runs on the machine
this was written on, so nothing there has been scheduled by a kubelet or probed
by one. every file says so on its first line. what has been done to it is
`kubectl kustomize deploy/k8s`, which renders; `kubectl apply
--dry-run=client` cannot validate it offline, because that downloads a schema
from a cluster.

two fields are worth the comment they carry.

**the scheduler runs two replicas.** one process at a time decides, and which
one is settled by [a lease in the store](scaling.md#the-deciding-lease) rather
than by a replica count, so two is safe rather than forbidden. the second one
buys no throughput; it buys that a pod going away is a gap of about one lease
instead of an outage lasting however long a replacement pod takes to schedule,
pull and boot. one replica is also correct if you would rather the deployment
simply stop deciding.

**the probes point at `/api/whoami`.** that is the one endpoint outside the
[auth guard](auth.md), because the ui has to be able to ask whether there is a
guard before it holds a token, and a kubelet cannot carry a bearer token out of
a secret into an httpGet header. so what the probe proves is that the process
is up and its http server is answering. what it does not prove is that the
store is reachable: `GET /api/health` is where that lives, in an `ok` field,
and it answers 200 either way on purpose, because the endpoint answering is the
news. readiness here means serving, not healthy, and something that can hold a
token has to watch `/api/health`'s body for the rest.

there is no helm chart, deliberately. what changes between deployments here is
the image, two replica counts and a secret, all three of which kustomize
already does with a tool that ships inside kubectl. a chart nobody has rendered
against a cluster would be more untested surface in the shape of a product.

## What this does not do

there is no operator, no pod template, no autoscaler and no kubernetes
executor. hestan ships [one mechanism](scaling.md) for moving work off the
process that asked for it, a durable claimable queue, and a pod running
`HESTAN_ROLE=worker` against a shared postgres is what the kubernetes executor
would have been. a container and a pod are packaging around that mechanism, not
new execution paths.

## See also

- [scaling](scaling.md): the queue, the roles, the leases, the term, and what
  several hosts needs.
- [storage](storage.md): the postgres backend, and the unique index over an
  occurrence.
- [authentication](auth.md): the token these files set, and where a real one
  comes from.
- [development](development.md): the gates, and the ui build loop the image
  depends on.
