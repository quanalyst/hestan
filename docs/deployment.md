# Deployment and build identity

a run log says what ran and when. until phase 50 it did not say **which build
of your code ran it**, so "this started failing on Tuesday" could not be joined
to "we deployed on Tuesday" without going outside hestan and lining up two
timelines by eye.

three identities are tangled in that sentence, and hestan keeps them apart
because only one of them is hestan's to know.

| what | who knows it | where it comes from |
| --- | --- | --- |
| hestan's own version | hestan | its own manifest, at compile time |
| your application's build | **you** | told, or absent |
| this deployment's name | **you** | told, or absent |

**the one an operator cares about is the middle one, and hestan cannot see
it.** hestan is a library compiled into your binary. the sha it would want
belongs to a repository it is not in, the image digest belongs to an image it
did not build, and a version it invented would be worse than the absence. so
it is told, and what it is told is what it carries.

## Declaring it

```rust
use hestan::{Deployment, Hestan};

Hestan::new()
    .db("postgres://hestan@db/hestan")
    .deployment(
        Deployment::new()
            .name("prod-eu")
            .build(std::env::var("APP_BUILD").unwrap_or_default()),
    )
```

beside `db` because it is the same sort of statement: where the run log is, and
whose run log it is.

**both halves are optional and declaring neither is the ordinary case.** one
process on a laptop has nothing to tell itself apart from and no build to name,
and it should not have to fill anything in to get a run log. everything below
reads as `null` for such a deployment, which is what nobody having said looks
like.

**an empty string is an absence.** `std::env::var("APP_BUILD").unwrap_or_default()`
in a deployment that meant to set the variable and did not is how you get here,
and a build called `""` on every run row would read as an answer.

### Where the build comes from

whatever your build already has. hestan neither parses nor validates it:

- **baked in at build time**, which is the one to prefer, because the binary
  and its answer cannot then come apart. `env!("APP_BUILD")` with the variable
  set by your build, or an `ARG` in a `Dockerfile`, which is what
  [the image at the repository root](containers.md#which-build-an-image-is)
  does.
- **read out of the environment at start**, which is what
  `docker-compose.yml` and the manifests in `deploy/k8s` show, and what the
  snippet above does. weaker, because the thing that started the process is
  claiming what the binary is rather than the binary saying so.

## What hestan knows without being told

everything here is a compile-time fact and none of it is asked for:

| field | what it is |
| --- | --- |
| `version` | the hestan compiled into this binary |
| `schema` | the store schema this build reads and writes |
| `features` | the [features](../README.md#using-it-from-your-project) compiled, in name order |
| `platform` | `os/arch`, as `linux/aarch64` |
| `debug_assertions` | what `--release` turns off |

`debug_assertions` is reported as the fact it is rather than as "release
build", because a profile that overrides it makes the two come apart. it is on
the page because a release build running ten times slower than expected is the
reason somebody looks.

**hestan's version is never offered in place of yours.** a deployment that
declared no build reads `"build": null` beside `"version": "0.2.0"`, and
the two are in different halves of the object for exactly that reason.

## Where it surfaces

`GET /api/health` carries the whole of it, in two halves:

```json
{
  "deployment": {
    "name": "prod-eu",
    "build": "9f2c1ab",
    "hestan": {
      "version": "0.2.0",
      "schema": 24,
      "features": ["bundled", "cli", "postgres"],
      "platform": "linux/aarch64",
      "debug_assertions": false
    }
  }
}
```

`hestan doctor` says it in two lines, and says whose binary the second one is
about:

```
ok    deployment prod-eu, running build 9f2c1ab
ok    hestan     0.2.0 in this deployment's binary, linux/aarch64, features: bundled cli postgres
```

**`hestan doctor --db /var/lib/hestan/hestan.db` reports the operator binary's
hestan, not the deployment's**, and says so, because the declaration lives in
the deployment's own binary and not in the database. `hestan doctor --server`
reads the other end's, off `/api/health`.

a deployment that declares no build gets a `note` rather than a tick, on both,
because the run log it is writing cannot answer which code produced anything in
it. it is a note and not an error: a deployment with one process has a
defensible reason to skip this.

the [ui](web-ui.md) says it once, at the top of the activity page, beside which
process is deciding. once, because which build is running is a fact about the
deployment, and repeating it on a page about one run is noise on that page.

## A run remembers the build that launched it

this is the half with the operational value.

**every run records the build in force when it was launched**, in a column of
its own:

```json
{"id": "0192...", "job": "orders_etl", "build": "9f2c1ab", "...": "..."}
```

**recorded, not joined.** the alternative was to answer "which build was this
run?" by asking the process doing the reading, which would answer every run
there has ever been with today's build. that is a confidently wrong answer, and
it is wrong about exactly the runs somebody is looking at: the ones from before
the deploy.

so nothing rewrites it. a scheduler on last week's image queues a run and a
worker already on this week's claims and executes it: the row says
`last-week`, because that is what launched it. every write after the insert
(the claim, the start, the heartbeat, the terminal row) names the columns it
changes and this is not one of them. there is a case for it that runs two
processes on two builds against one database and asserts exactly that.

`null` means **nobody told hestan**, and it means that in three ways that
hestan cannot tell apart: a run written before phase 50, a run launched by a
deployment that declares no build, and a run launched through a `Runner` that
was never given one.

### Filtering by it

"show me the runs from the build before last" is the question this exists to
answer, so it is a filter everywhere a run list is:

```
GET /api/runs?build=9f2c1ab
hestan runs --build 9f2c1ab
hestan show 0192...              # prints the build line
```

and the ui's runs page has a `build` box beside the tag one, seeded from the
url, which is what the build chip on a run page links to.

the filter composes with the others rather than replacing them:
`?job=orders_etl&build=9f2c1ab&since=...` is the three of them together. asking
for a build nothing ran under is an empty page, and asking for none is every
run, the ones recording no build included.

it is a **scan**, like the tag filter, because there is no index over the
column. see the cost below.

### What it costs

one nullable text column on `runs`, which is the largest table in the database.
measured rather than estimated, in `store.rs`: 2,000 rows written twice, once
with a forty-character git sha and once without, both vacuumed, the file sizes
compared.

**43 bytes per run row** for a forty-character sha, which is the forty
characters plus sqlite's per-value overhead and the page rounding on top. a
million runs is about 41 MiB. a short tag or a ci build number costs less in
proportion. the case asserts a range so the number cannot drift without
somebody noticing, and an index over the column would land well outside it,
which is the other thing the range is guarding.

nothing is rewritten on either backend by the migration, and the postgres half
is a catalog change. see [storage](storage.md#schema).

## What is deliberately not here

- **a build on the metrics endpoint.** a `hestan_build_info` gauge is the
  standard prometheus shape for this and it is still not here.
  [metrics](metrics.md#what-is-deliberately-not-here) has the reasoning: every
  label hestan emits is a `&'static str`, which is both the cardinality rule
  and the whole of its enforcement, and a build read out of the environment is
  not one. `/api/health` is the endpoint that answers "what is this".
- **anything that checks the build is real.** hestan carries the string. it
  does not resolve it, does not ask a registry about it, and does not notice
  when two processes on one database disagree about which build they are.
- **a build on anything but a run.** an asset materialization, a schedule tick
  and a sensor evaluation all record which run they belong to, and the run
  carries the build. a second copy per table would be the same string written
  four more times.

## See also

- [containers](containers.md#which-build-an-image-is): the build argument the
  image takes, and the manifests that show the two ways in.
- [http api](http-api.md#health-and-the-queue): the health shape and the run
  filter.
- [the command line](cli.md#looking-at-things): `--build`, and what `doctor` reports.
- [storage](storage.md#schema): the column and the migration.
