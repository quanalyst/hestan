# Deployment and build identity

Hestan records an optional deployment name and application build identifier.
The application supplies both; Hestan cannot infer them from its own package
metadata. A run retains the build identifier supplied when it was launched.

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

Both fields are optional; empty strings become absent values. Hestan carries
these strings without interpreting or validating the build identifier.

### Where the build comes from

Supply an identifier from your own build system. `env!("APP_BUILD")` embeds it
at compile time; `std::env::var("APP_BUILD")` reads it at startup. A runtime
value must be kept consistent with the binary it describes. See
[the container example](containers.md#which-build-an-image-is).

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
declared no build reads `"build": null` beside `"version": "0.2.5"`, and
the two are in different halves of the object for exactly that reason.

## Where it surfaces

`GET /api/health` carries the whole of it, in two halves:

```json
{
  "deployment": {
    "name": "prod-eu",
    "build": "9f2c1ab",
    "hestan": {
      "version": "0.2.5",
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
ok    hestan     0.2.5 in this deployment's binary, linux/aarch64, features: bundled cli postgres
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

Each run stores the build identifier supplied at launch. Claims, execution and
later deployments do not rewrite it. If a scheduler launches a run and a worker
on another build executes it, the row identifies the scheduler's build.

A null value means no build was recorded, including runs created before build
tracking and runs launched without a configured build identifier.

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

See [storage costs](storage.md#what-the-build-column-costs) for the build column
and filter behavior.

## What is deliberately not here

Build identifiers are not verified against a repository or registry. Hestan
does not enforce matching identifiers across processes.

Build metadata is exposed through health and run records, not metric labels.
Materializations and ticks refer to their runs instead of duplicating the build
identifier. See [metrics](metrics.md#what-is-deliberately-not-here).

## See also

- [containers](containers.md#which-build-an-image-is): the build argument the
  image takes, and the manifests that show the two ways in.
- [http api](http-api.md#health-and-the-queue): the health shape and the run
  filter.
- [the command line](cli.md#looking-at-things): `--build`, and what `doctor` reports.
- [storage](storage.md#schema): the column and the migration.
