# Containers

Hestan runs inside your application's binary. The repository includes deployment
examples:

- [`Dockerfile`](../Dockerfile): builds `examples/demo.rs`.
- [`docker-compose.yml`](../docker-compose.yml): PostgreSQL, one scheduler and three workers.
- [`deploy/checks/`](../deploy/checks): container integration checks.
- [`deploy/k8s/`](../deploy/k8s): Kubernetes manifests, not yet validated on a cluster.

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

The runtime contains the Debian base, CA certificates, a non-root `hestan` user
(uid 10001), and the application binary. Certificates support application
clients that use the system trust store. Image size depends on the build and
architecture.

The runtime does not include curl, wget or psql. Compose health checks and
Kubernetes probes run outside the application container.

### The ui is copied in, not built

The image embeds committed `ui/dist` and needs no Node installation. After
editing UI source, run `just ui-build` and commit the bundle before building
the image. See [the UI development loop](development.md#the-ui-loop).

### Features

The example image enables `cli,postgres`: the demo requires the CLI and Compose
uses PostgreSQL. Adjust features for your application. The build uses bundled
SQLite and Rust database/HTTP clients; it does not install libpq or OpenSSL.

### Which build an image is

hestan is a library compiled into the binary above. it can read its own
version, its schema version and the features it compiled, and it cannot read
your git sha: the repository the image was built from is not one it is in. so
the sha goes in at the one moment somebody knows it, which is the build:

```
docker build --build-arg HESTAN_BUILD=$(git rev-parse --short HEAD) -t hestan-demo .
```

the `Dockerfile` turns that argument into an `ENV`, the demo reads it and hands
it to `Deployment::build`, and from there **every run this image launches
records it**. `docker-compose.yml` passes the same value through:

```
HESTAN_BUILD=$(git rev-parse --short HEAD) docker compose up -d --build
```

the argument is last in the `Dockerfile`, after the layers that do work,
because it changes on every commit and a build layer under it would be rebuilt
every time.

**unset is an absence, not a build called `unknown`.** the default is the empty
string, hestan reads an empty string as nobody having said, and a run log that
says nothing about the build is a better answer than one where every run since
the beginning of time came from `unknown`.

what the running stack then says:

```
$ curl -s -H "Authorization: Bearer demo-token-change-me" \
    localhost:4000/api/health | jq .deployment
{
  "name": "compose-local",
  "build": "9f2c1ab",
  "hestan": {
    "version": "0.2.5",
    "schema": 24,
    "features": ["bundled", "cli", "postgres"],
    "platform": "linux/aarch64",
    "debug_assertions": false
  }
}
```

`name` is `HESTAN_DEPLOYMENT`, set in the compose file rather than in the
image: the same image is this deployment or another one depending on where it
is started, and the build is a fact about the image itself. everything under
`hestan` was never asked for. [deployment and build
identity](deployment.md) is the whole of the distinction, and the run column is
the half worth having:

```
$ docker compose exec scheduler hestan-demo runs --build 9f2c1ab
```

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

### Scraping the stack

every container serves `GET /metrics` on the same port as the ui and behind the
same token. the scheduler publishes 4000, so from the host:

```
curl -s -H "Authorization: Bearer demo-token-change-me" localhost:4000/metrics
```

the workers publish no host port, so they are reached over the compose network
the same way the health check above is:

```
docker run --rm --network hestan_default busybox:1.36 \
  wget -qO- --header="Authorization: Bearer demo-token-change-me" \
  "http://hestan-worker-3:4000/metrics"
```

**by container name, not by service name.** `worker:4000` resolves to whichever
of the three replicas dns hands back, and the counters above are per process: a
target that reaches a different worker each scrape reports numbers that jump
around. the gauges would survive it, because they are read off the shared run
log rather than out of the process, which is the split
[metrics](metrics.md#deployment-wide-or-per-process) is about.

without the token it is a 401, like every other read.

The Compose stack does not include Prometheus; configure your own scraper.

### Scope of the examples

The Compose checks run on one host. They do not establish multi-host fault
tolerance or behavior under clock changes. Use a shared PostgreSQL store when
running workers on multiple hosts; see [scaling](scaling.md).

## Cutting the deciding process off

The container checks exercise network partitions, scheduler takeover and worker
shutdown. They require Docker, the Compose plugin, psql, Python 3, and host
ports 4000, 4001 and 55432.

```sh
bash deploy/checks/run.sh
bash deploy/checks/partition.sh
bash deploy/checks/stop.sh
PARTITION_SECS=120 bash deploy/checks/partition.sh
```

Each script creates a stack, injects a fault, checks the recorded state and
tears the stack down. These checks run separately from `cargo test`.

## Signals, and what a stop is worth

`serve` and `work` handle SIGTERM and SIGINT, including when running as PID 1.
A graceful stop releases the deciding lease and allows active work to finish
within `Hestan::stop_within` (eight seconds by default).

Work that cannot finish before the deadline returns to the queue and may execute
again from the beginning. It is not recorded as failed. Operations should
therefore tolerate retries; see [claims and leases](scaling.md#claims-and-leases).

A forced kill cannot release leases: other processes must wait for expiry.
Allow the container's termination timeout to exceed `stop_within`.

## Kubernetes, written and not run

[`deploy/k8s/`](../deploy/k8s) has a ConfigMap, a Secret, a Deployment for the
schedulers, a Deployment for the workers, a Service, a PodMonitor, and a
kustomization.

These manifests have been rendered with `kubectl kustomize deploy/k8s` but
have not been validated on a cluster. Client dry-run validation also requires
access to a cluster schema.

three fields are worth the comment they carry.

**the scheduler runs two replicas.** one process at a time decides, and which
one is settled by [a lease in the store](scaling.md#the-deciding-lease) rather
than by a replica count, so two is safe rather than forbidden. the second one
buys no throughput; it buys that a pod going away is a gap rather than an
outage lasting however long a replacement pod takes to schedule, pull and boot.
how long a gap depends on how the pod went: one **deleted** is sent SIGTERM and
hands the lease back, so the gap is the other pod's renewal interval, while one
whose node vanished costs a whole lease. one replica is also correct if you
would rather the deployment simply stop deciding.

**the probes point at `/api/whoami`.** that is the one endpoint outside the
[auth guard](auth.md), because the ui has to be able to ask whether there is a
guard before it holds a token, and a kubelet cannot carry a bearer token out of
a secret into an httpGet header. so what the probe proves is that the process
is up and its http server is answering. what it does not prove is that the
store is reachable: `GET /api/health` is where that lives, in an `ok` field,
and it answers 200 either way on purpose, because the endpoint answering is the
news. readiness here means serving, not healthy, and something that can hold a
token has to watch `/api/health`'s body for the rest.

**the build identity has two ways in, and the manifests take the first.**
nothing in `deploy/k8s` sets `HESTAN_BUILD`, because the image already carries
it from `--build-arg`, and the image saying which build it is cannot come apart
from the image. the second way is the downward api: an
`app.kubernetes.io/version` annotation on the pod template, set per release by
whatever cuts your releases, read back with a `fieldRef`. `scheduler.yaml` has
those three lines commented out with the reason, which is that **doing both is
worse than either**: a pod env var overrides the image's, so an unset
annotation would replace a perfectly good baked-in sha with an empty string.

`HESTAN_DEPLOYMENT` does come from the downward api, off `metadata.namespace`,
because a namespace is a name the cluster already has and keeps in step with
itself. the configmap sets neither: it is shared by every release, and the
build changes with each of them.

**`podmonitor.yaml` requires the Prometheus Operator.** it selects every hestan pod (both deployments: a worker
is where the run outcomes and the claim latency are counted) and carries the
token in `authorization.credentials`, because `/metrics` is inside the guard
while `/api/whoami` is not. both pod templates also carry the
`prometheus.io/*` annotations, which do nothing on their own: something has to
relabel on them, and that something is also where the credential goes.
[metrics](metrics.md#kubernetes) has the reasoning.

Use Kustomize to customize images, replica counts and credentials.

## What this does not do

Hestan supplies a durable queue and worker role, without a Kubernetes operator,
autoscaler or per-run pod executor. Containers run the same application and
execution paths described in [scaling](scaling.md).

## See also

- [scaling](scaling.md): the queue, the roles, the leases, the term, and what
  several hosts needs.
- [storage](storage.md): the postgres backend, and the unique index over an
  occurrence.
- [authentication](auth.md): the token these files set, and where a real one
  comes from.
- [metrics](metrics.md): what a scrape of one of these containers reads.
- [deployment and build identity](deployment.md): the declaration the build
  argument above feeds, and the run column it lands in.
- [development](development.md): the gates, and the ui build loop the image
  depends on.
