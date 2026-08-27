# changelog

## unreleased

a job says which group it belongs to, and the run timeline folds a group of
lanes into one.

0.1.0 shipped `Asset::group` and nothing equivalent for a job, so the asset
graph knew about weather, eia and forecast while the timeline showing the jobs
that feed those same assets knew only the namespace. folding on the namespace
instead turns twenty lanes into two buckets, one of them holding fourteen,
which is not a fold anybody wants.

```rust
Job::builder("weather_pull").group("weather")
Asset::source("station_readings").group("weather")
```

**a group labels a picture and hestan draws it; a namespace divides the
deployment and hestan enforces it, and neither is derived from the other.**
that is the sentence `docs/namespaces.md` already used for an asset's group,
reused rather than reworded: the picture is the asset graph for an asset and
the run timeline for a job. two concepts explained differently in two places
become three concepts, so the wording moved to cover both rather than a second
one being written beside it.

**one label, one colour.** `hestan::hue` is a pure function of a name, so a job
in group `weather` and the assets in group `weather` land on the same angle
without either end being told about the other, and `Asset::hue(n)` pins that
angle for both. deliberate rather than incidental: they are the same word on
one screen, and two colours for one word is a reader's problem. the api
resolves a job's hue through the same function the asset graph uses, so a pin
cannot leave the two disagreeing, and `hestan doctor` counts a job's group
among the labels it checks for hues too close to tell apart.

**there is no fallback, and that is the one place a job's group differs from an
asset's.** an asset's falls back to the part of its name before the first `/`,
because that is how the asset graph grouped before `Asset::group` existed and
the fallback is what keeps an existing graph looking the way it looked. no
timeline has ever grouped by anything, so the same rule produces the opposite
mechanics here: a job's group is declared or it is absent, and a job called
`finance/etl` is in no group.

### The fold

`fold` chips over the jobs overview collapse a group's lanes onto one row, in
the group's hue, with the group and how many jobs it stands for in the gutter.

- **a folded row draws every run its members have.** the same greedy sub-lane
  packing one job's lane already uses, run over all the members' runs at once,
  so three runs live at one moment is a row three sub-lanes tall. a fold makes
  the plot shorter and never quieter. drawing the first run and hiding the rest
  would make a busy period look like a quiet one, which is a worse lie than not
  folding at all and would look right on a screen. there is a case that folds a
  group and asserts the drawn bar count and the run ids did not move, that no
  two overlapping runs share a sub-lane, and that the row grew by exactly the
  height three sub-lanes need.
- **a failure survives the fold**: the strip under the plot is fed from the
  same placed bars, so a failed run inside a folded group keeps its glyph. it
  is drawn in no hue at all, because shape carries state in this ui and a
  colour where an outcome already is would mean two things. a queued run keeps
  its outline and a canceled one its dimmed grey for the same reason.
- **the fold state is in the url** (`/jobs?folded=weather,eia`), beside the
  namespace filter and for the same reason: a folded view is a link somebody
  can send.
- **nothing folds where nothing declares a group.** no chip row, no "no group"
  bucket, and a job in no group is never folded into anything. inventing a fold
  out of a naming convention would be a guess.

the decisions moved out of `TimelinePlot.tsx` into `ui/src/timeline.ts` so the
claim about density can be asserted rather than described: `lanesOf`, `pack`,
`rowsOf` and `place` are what turn a job list and a run list into the rects the
svg draws, and none of them drops a bar.

### What an existing deployment sees change

- **nothing, until a job declares a group.** no schema version, no migration,
  no new route, no new query parameter. a deployment that declares none has one
  lane per job on the timeline, no fold chips, and `null` for both new keys.
  there are cases asserting that rather than assuming it.
- **the responses are not byte for byte**: `group` and `group_hue` are new keys
  on `GET /api/jobs` and `GET /api/jobs/{name}`, `null` where nothing was
  declared. a client that reads keys by name is unaffected; one that compares
  whole documents is not.
- **no source break.** `JobBuilder::group` is a new method and `Job::group` a
  new accessor.
- **two error messages are worded differently.** an asset declaring a group
  with no name in it, or with `/` in it, is refused by the same function that
  now refuses a job's, so the two say one thing. both were and remain
  `Error::Graph` from the build, naming the asset and the group.

## 0.1.0 (2026-08-24)

the first release that is not a pre-release. the surfaces in
[stability](docs/stability.md) are now held by a version number rather than by
a tag: cargo reads `hestan = "0.1.0"` as `>=0.1.0, <0.2.0`, so a break has to
wait for a `0.2.0` that nothing updates a deployment onto by itself.

**upgrading from `0.1.0-beta.3`: every source break is in the table below, one
behaviour break follows it, and `cargo update` brings a deployment here without
being asked.** a requirement written `hestan = "0.1.0-beta.3"` has the same
`<0.2.0` ceiling a released one does, so the pre-release tag held nobody back
from this. each source break is a compile error rather than a change of
meaning, and each has something to write instead:

| what moved | what to write instead |
| --- | --- |
| eleven enums are `#[non_exhaustive]`: `Error`, `Meta`, `InputError`, `Auth`, `Trigger`, `SubjectKind`, `EventKind`, `TickOutcome`, `When`, `Reclaim`, `Blocked` | a `_` arm on a `match` over one. no variant was added, renamed or removed, and `matches!`, `if let` and naming a variant to construct it are all untouched |
| `Meta` gained `Series` and `Saved`; `EventKind` gained `RunReleased` | the same `_` arm, which is what those two being open is for from here on |
| `Identity` gained a public `scope` field | `Identity::new(name, access)`, or `viewer`, `operator`, `admin`, which is what the docs have always shown |
| `RunEvent`, `RunFailure` and `LateEvent` gained an `owner` field | nothing, unless you built one: a hook is handed a payload rather than building one |
| `Run` gained a `build` field | the same. `Store::runs` hands them back |
| `Store::runs` gained a `build` parameter, before `limit` | `None`, which is what the call meant before |
| `Scope::may_touch_job` and `Scope::may_touch_asset` take the namespace as a second argument | `None`, likewise |

the behaviour break is one, and it is the sharp edge of secret params:
**a run launched with a param declared through `Op::secret_params` cannot be
replayed, resumed or retried.** the store holds a marker rather than the value,
and re-launching off that row would run the job with the literal
`[hestan:redacted]` as its credential. launch again and pass it, or move the
credential to a resource. [secrets](docs/secrets.md).

**the store goes from schema v19 to v24**, five migrations applied on first
open, on both backends. **rolling one back is not supported.** hestan writes no
down migrations, and a build refuses a database written by a newer hestan
rather than guessing at it, which is the correct half of that. an operator who
has to go back to a beta binary restores the copy taken before the upgrade:
that is what `hestan backup` is for and [backup and recovery](docs/backup.md)
is the order to work in. there is no in-place downgrade and none is planned.

**what 0.1.0 is.** a library compiled into your own binary: ops wired into
jobs, cron schedules, sensors, assets with fingerprints and provable staleness,
a run log on sqlite or postgres, notification hooks, an embedded web ui, a json
api, a command line over the same registry, and a run queue several processes
on one host share. nothing is deployed beside it and there is no daemon. the
five surfaces in [stability](docs/stability.md) are what a caller may build on
and they move together on the one version number.

**what 0.1.0 is not.** each of these is written where it belongs in the docs.
they are gathered here because somebody deciding whether to depend on hestan
should not have to find them one at a time.

- **the kubernetes manifests have never been applied to a cluster.**
  `deploy/k8s` is written from what the compose stack does, and every file in
  it says so on its own first line. it is a starting point somebody has to try,
  not a deployment anybody has run.
- **the operating envelope was measured on one machine and is not a load
  qualification.** the image sizes, the 3.2-to-44.5-second window a partitioned
  process takes to notice the network came back, and every other number in
  [containers](docs/containers.md) are measurements of the runs named, on one
  box, and none of them is a guarantee. nobody has run hestan under sustained
  load, and nobody has run its workers on more than one *host*: that a process
  on another host differs only in which socket it opens is an argument rather
  than a run, and [scaling](docs/scaling.md) keeps the difference.
- **secret params are single-process.** the value lives in the memory of
  whatever took the launch, so they work on a one-process deployment, and on a
  multi-process one only when whatever launched also executes. a scheduler
  enqueuing for separate workers is exactly the shape they do not work in; that
  shape wants a resource. they cover top-level keys only and they encrypt
  nothing.
- **a scope is an authority boundary and not a confidentiality one.** it
  narrows what a token may do and it does not narrow what a token may see:
  reads are not filtered by it, and the ui shows a scoped token controls that
  the api will then refuse. keeping data from a caller wants a proxy in front or
  `Auth::custom`, and [authentication](docs/auth.md) says why the small exact
  promise was preferred to a large approximate one.
- **sqlite is one host.** several processes on one host share the file; workers
  on more than one host need postgres. that is a property of sqlite and not
  something hestan can lift.

what changed since `0.1.0-beta.3`, in the order it landed, is the rest of this
entry.

a run log said what ran and when. it did not say **which build of your code ran
it**, so "this started failing on Tuesday" could not be joined to "we deployed
on Tuesday" without going outside hestan and lining two timelines up by eye.

**three identities, kept apart, because only one of them is hestan's to know.**

- **hestan's own version**, which it reads at compile time, along with the
  schema version, the features compiled and the platform.
- **your application's build**: a git sha, a tag, an image digest. hestan is a
  library compiled into your binary and cannot see it. **it is told or it is
  absent**, and a version it invented would be worse than the absence.
- **the deployment's name**: this installation as opposed to staging. also told.

the one an operator cares about at 3am is the second, and hestan can only carry
what it is given. `docs/deployment.md` is the whole of it.

```rust
Hestan::new()
    .db("postgres://hestan@db/hestan")
    .deployment(
        Deployment::new()
            .name("prod-eu")
            .build(std::env::var("APP_BUILD").unwrap_or_default()),
    )
```

beside `db` because it is the same sort of statement: where the run log is, and
whose run log it is. **both halves are optional and declaring neither is the
ordinary case**: one process on a laptop has nothing to tell itself apart from
and should not have to fill anything in. an empty string is an absence, because
`unwrap_or_default()` on an unset variable is how you get here and a build
called `""` on every run row would read as an answer.

**what is knowable without being told is not asked for.** `GET /api/health` and
`hestan doctor` report hestan's version, the schema version, the compiled
feature set, the platform as `os/arch` and whether debug assertions are on, all
compile-time facts, in a half of the object separate from the two declared
fields. hestan's own version is never offered in place of yours. a case reads
`[features]` out of `Cargo.toml` and asserts the reported set is exactly it,
with its own `cfg!` table, so a feature added tomorrow cannot go unreported.

**a run remembers the build that launched it.**

```
GET /api/runs?build=9f2c1ab
hestan runs --build 9f2c1ab
```

**recorded, not joined**, and that is the decision the phase turns on. looking
the build up from the process doing the *reading* would answer every run there
has ever been with today's build: a confidently wrong answer, and wrong about
exactly the runs somebody is looking at. so it is a column on `runs`, written
once by the process that created the row, and nothing rewrites it. a scheduler
on last week's image queues a run and a worker already on this week's claims
and executes it: the row says `last-week`. there is a case that runs two
processes on two builds against one database and asserts it, and the command
line suite does the same thing with three real processes.

**filterable, or it would be decoration.** `build=` on `GET /api/runs`,
`--build` on `hestan runs`, a `build` box on the runs page beside the tag one,
and a build chip on a run page that links to it. the store's existing run filter
gained a parameter rather than growing a second path; it now carries seven, and
that is as many as a positional signature should: three of them are
`Option<&str>` and two swapped arguments would still compile, so the next
filter wants a named-field query like `EventQuery`, which is said on the method
rather than left to be found.

**what it costs, measured rather than estimated.** one nullable text column on
the largest table in the database: 2,000 runs written into two databases
identical but for a forty-character git sha, both vacuumed, the files compared.
**43 bytes per run row**, so about 41 MiB per million runs. the case asserts a
range so the number cannot drift, and an index over the column would land well
outside it, which is the other thing the range guards: there is no index, and
the filter is a scan exactly as the tag filter has been since v12.

**the build stays off the metrics endpoint, deliberately.** phase 46 left it
out because nobody pages on a version string; this phase, which finally has one
to publish, agrees. the cardinality of a `hestan_build_info` gauge would be
defensible, but every label hestan emits is a `&'static str` and that is both
the rule and the whole of its enforcement, while a build read out of the
environment is not one. publishing it would mean giving up the rule or leaking
a string to get around it. `/api/health` is where "what is this" is answered,
and the run rows carry the join a label never could.

**what an existing deployment sees change:**

- **one migration, v24, and it is one nullable column** (`runs.build`). no
  index, no rewrite, and a catalog change on postgres. **every row written
  before it reads as an absence**, not as an empty string and not as this
  build: a run that predates the column and a deployment that declares no build
  are the same thing, which is nobody having told hestan.
- **`db schema v24`**, so a database opened by this build cannot then be opened
  by an older one. that has been true of every migration.
- **`GET /api/health` gains `deployment`**, and every run object gains
  `build`, which is `null` on everything launched before this or by a
  deployment that declares none.
- **`Run` gains a `build` field**, which breaks a struct literal of it, as
  `docs/stability.md` says a row type may. `Store::runs` gains a `build`
  parameter before `limit`.
- **`Deployment` is a new public type** with private fields and a builder, the
  pattern `Owner` set. it is not an enum, so nothing new has to be matched.
- **`hestan doctor` gains two lines**, `deployment` and `hestan`. a deployment
  that declares no build gets a `note`, which does not affect the exit code; a
  `doctor --db` run from the operator binary says plainly that the `hestan`
  line is about *its* binary and not the deployment's.
- **the ui says it once**, at the top of the activity page beside the deciding
  line, and nowhere else.
- **the container image takes `--build-arg HESTAN_BUILD`**, the compose stack
  passes it through and names itself `compose-local`, and `deploy/k8s` shows
  the downward-api alternative with the warning that doing both replaces a good
  baked-in sha with an empty one. everything under `deploy/` is still
  **unverified**: no cluster has seen it.
- **cases**: a deployment that declares nothing reporting only what hestan
  knows; a declared name and build reaching health, doctor, a remote doctor and
  the ui line; the reported feature set against `Cargo.toml`; a run recording
  the build that launched it; a worker on a different build claiming, executing
  and settling that run without touching it, and the same claim again in the
  queue suite across two real os processes on both backends, where the worker
  child is started on a different declared build from the parent that
  enqueued; a run launched by a deployment that names itself and declares no
  build recording an absence; a
  v23 database migrating on both backends with its old rows reading as an
  absence; the filter alone and composed with `job`; an empty `build=`
  being no filter rather than one matching nothing; the column count that the
  one index-based read of a run row depends on; and the storage measurement.

a store is one file or one database, and nothing said how to take a copy of it.
that half is small. the half worth building is what a **restored** copy means,
because every claim in one is held by a process that is somewhere else. and a
plain launch had nothing to stop a retried request becoming two runs, while a
sensor and a schedule have had exactly that for phases.

**a consistent copy, and a copy that says it is one.**

```
hestan backup /backups/hestan-2026-08-24.db
```

sqlite's online backup rather than a `cp`: the database is in WAL mode, so the
file on its own is missing whatever is still in the `-wal` beside it and gives
no sign of it. `Store::backup_to` runs `VACUUM INTO`, which takes a read
transaction and writes a whole database out of it while runs go on being
recorded. it writes to `<dest>.part` and renames, so a file under the name you
asked for is a finished copy, and it refuses a destination that already exists
rather than writing over last night's.

**postgres is your own tooling's and hestan says so** rather than shipping a
worse `pg_dump`. what it does say is what such a dump must not leave out:
every table (the consistency here is across tables, so `pg_dump -t runs` is not
a backup of a run log), `schema_version`, and the schema itself if hestan is
not in `public`.

**a restored store is a lie about the present, and a deployment refuses to come
up on one.** `hestan backup` marks the copy; `Hestan::serve` and `run_once`
refuse a marked one with `Error::NotResettled`, naming the command that fixes
it. reads still work, because pointing a report or `hestan runs --db copy.db`
at a copy is the ordinary reason to have one and none of that acts on a claim.

```
hestan resettle --db /var/lib/hestan/hestan.db
```

**it does not consult the leases, and that is the whole difference from boot
recovery.** hestan already fails runs whose claimer stopped renewing, and that
believes leases on purpose: on a live database a claim with thirty seconds left
on it is a run some other process is executing right now. on a restored
database there is no such process whatever the lease says. so `Store::resettle`
fails every run that was `running`, puts every claimed-but-unstarted run back
on the queue with its claim cleared, leaves the unclaimed queue alone, and
hands back the deciding lease. **nothing is requeued for a run that was
`running`**: it may well have finished in the original after the copy was
taken, and re-running a deploy or a payment run on hestan's own initiative is
not a small thing. those are failed with a reason and left for a resume or a
replay.

**the sharp edge, and it is a plain statement rather than a feature.**
restoring an old copy while workers are still running against the live store is
how a recovery makes things worse, and **nothing in hestan prevents it, because
nothing in hestan can**: by the time any hestan code runs against the restored
store, the file is already in place or the dump is already loaded. what
`hestan resettle` adds is a second pair of eyes at the last moment somebody is
still watching. it reads every lease, waits twenty seconds, reads them again,
and refuses if any moved, because a lease that moves is a process renewing it.
what that does not see is written down beside it: a process holding no run and
running no election renews nothing, so an idle worker is invisible to it.
`docs/backup.md` has the order an operator has to work in, including what
`mv copy.db hestan.db` means to a worker that has the old inode open.

**what a backup does not contain, asserted rather than mentioned.** an
[io manager](docs/io-managers.md) writes op outputs to files outside the
database, and no copy of the store contains them. a case builds a value through
`FileIo`, records the materialization, takes a backup, removes the directory,
and asserts both halves: the restored materialization still holds the handle
and still says the asset is built, and the value it names is gone. that is a
likely case rather than a theoretical one, and `hestan doctor` counts the runs
holding one under `values`.

**a launch key makes a retry harmless.**

```
POST /api/jobs/deploy/runs  {"params": {"env": "prod"}, "key": "ci-build-4182"}
hestan run deploy --key ci-build-4182
runner.launch_once("deploy", params, Trigger::Manual, "ci-build-4182", tags, None)
```

same key, same job, same params: **one run**, and the second call is answered
with the first one's id rather than with a conflict, because "make sure this
ran once" is what the caller meant. `202` for the call that launched and
`200 {"repeat": true}` for one that did not.

**the store refuses the second one, not a check.** `launch_key` is the primary
key of the new table and the insert that takes it is in the same transaction as
the run row, so two api processes racing one key produce one run because the
database said no to the second. that is phase 41's lesson applied where it was
still missing: a read-then-write is two callers away from a race, and a
uniqueness constraint holds where a lock does not. a case starts four threads
on four connections, releases them together, and asserts one `Yes`, three
`Held` naming that same run, one run row, one op row and one queued event.

**keyed on the key alone, not on `(job, key)`.** a key is the caller's name for
one request. keyed per job, one key would mean two things on two jobs and hand
somebody a second run without either caller seeing it; keyed on the key, that
is a collision hestan can report, and it does: **the same key with different
params, or on a different job, is `Error::LaunchKeyReused`**, naming the run the
key already has. a `409` over http, exit 2 on the command line, nothing
launched.

**what "the same params" compares is the params as stored.** so key order in an
object does not matter, and **two launches differing only in a param the job
declared secret are the same launch**, because the stored params are identical
and that is what the digest is of. hashing what is in the database rather than
what was passed is the right way round, and the cost is a case rather than a
sentence.

**how long a key is honoured**: until retention's age cutoff passes it, which
is the same knob and the same cutoff `sensor_run_keys` rides rather than a
second lifetime to reason about. with no retention policy configured nothing
prunes either. a key is also dropped **with the run it names**, through a new
`launch_keys_run` index, so it can never hand a retrying caller an id nothing
can be looked up under.

**what an existing deployment sees change:**

- **two migrations, v22 and v23, and both are one `CREATE TABLE`.** `store_copy`
  is empty on every database that is not a copy; `launch_keys` is empty until
  something passes a key. nothing is read, nothing is rewritten, and the upgrade
  costs the same on both backends. a deployment that takes no backup through
  hestan and passes no key notices neither.
- **`db schema v23`**, so a database opened by this build cannot then be opened
  by an older one. that has been true of every migration.
- **`POST /api/jobs/{name}/runs` can now answer `200`.** only when the request
  carries `key` and that key already named a run; a request without `key`
  answers exactly what it always did. `key` alongside `ops` is a `400`: a subset
  launch has no keyed form, and honouring one of the two silently would be
  worse than saying so.
- **`Error` gains `NotResettled`, `NoBackup` and `LaunchKeyReused`**, which is
  what `#[non_exhaustive]` on it is for.
- **`Launch`, `Restored` and `Resettled` are new public types with private
  fields and accessors**, the pattern `docs/stability.md` set with `Owner`.
  none of them is an enum, so nothing new has to be matched.
- **`hestan doctor` reports a restored run log**, `wrong` and therefore exit 7
  until it has been resettled and a `note` afterwards. a deploy pipeline that
  runs `doctor` catches a restored database before it serves anything.
- **a restored deployment may do work twice, and nothing can stop it.**
  occurrences fired and run keys claimed after the copy was taken left their
  trace in the copy's source and nowhere else, so a schedule with catch-up will
  re-fire them and a keyed sensor will re-launch them. `docs/backup.md` says to
  pause both before starting a restored deployment if that is expensive.
- **the deciding term goes backwards on a restore**, so a process still holding
  a term from before the copy could be accepted by a store about to issue it
  again. nothing detects that; stopping every process first is what prevents it,
  and it is one more reason the order in `docs/backup.md` is the order.

- **`docs/backup.md`** is the new page: taking a copy of each backend, what is
  not in one, what a restored store says about claims and leases, coming up on
  one, the hazard, and what to check afterwards. `docs/launching.md` gains
  "launching once", `docs/http-api.md` the `key` parameter and the `200`,
  `docs/cli.md` `--key`, `backup` and `resettle`, and `docs/storage.md` the two
  tables and the two migration steps.
- **cases**: a copy taken while a second connection writes flat out, opening and
  reading whole; the copy marked and the original not; a deployment refusing to
  come up on it and starting once it is resettled; boot recovery leaving a
  copy's live-looking claims exactly as it finds them and a resettle taking them
  anyway; the leases reading the same twice until something renews one; a
  materialization whose file is gone; one key answered with one run three times
  over; a different key making a second run; one key with different params and
  one key on another job both refused by name; a secret param flattening two
  launches into one; four threads on four connections racing one key; a key
  pruned by the cutoff and a key going with its run; both migrations from a
  populated database on both backends; and the command line for all of it.

a run failed at 3am and the log said which job, not whose. and two teams in one
deployment shared one flat list of jobs, so a token that should reach one
team's work had to name every job in it. two rows, and they belong together
because both answer "whose is this".

**a namespace divides a deployment.**

```rust
Job::builder("orders_etl").namespace("finance")
Asset::source("orders").namespace("finance")
Sensor::new("new_files", every, body).namespace("finance")
```

**the decision that had to be made first, in one sentence: a group labels the
asset graph and hestan draws it; a namespace divides the deployment and hestan
enforces it, and neither is derived from the other.** `docs/namespaces.md` is
the whole of it and `docs/assets.md` agrees with it: the group section is
retitled and its opening narrowed to what a group does, which is cluster the
graph, colour it and name a source's origin. nothing about the group changed in
code, and nothing about an existing graph changed.

they were not merged, and two cases say why. **a source's group names the
external system**, so `Asset::source("fx_rates").group("vendor")` is a vendor
feed and not a team, and two teams reading one vendor is ordinary. and **a
group falls back to the part of the name before the first `/`**, which is right
for a colour and wrong for a boundary: what a token may touch would then be
decided by a naming convention.

**a schedule declares nothing: its namespace is its job's.** a schedule is a
firing rule for exactly one job, named in its constructor, so there is nothing
for it to decide and no way for the two to disagree. a sensor names no job
until it fires and may name several, so it declares one; a probe sensor is in
whatever namespace its asset is. all four kinds are in a namespace and two of
the four are told rather than asked.

**declared, never parsed out of a name**, for the reason phase 40 established:
the name is the key in `runs.job`, in every materialization and in every api
path, so renaming a job to regroup it starts its history over. a case registers
a job, records a run, re-registers it with a namespace, and asserts the run is
still there under the same key.

**a token scoped to a namespace is the point of having one.**

```rust
Identity::operator("finance-ci").scoped_to(Scope::namespaces(["finance"]))
```

every job and every asset declared in `finance`, including the one somebody
adds next week, without the token naming any of them.

```json
{"error": "fin is scoped to namespace finance, and this changes job payslips"}
```

**it extends phase 47's one enforcement point rather than adding a second.**
`out_of_scope` is still the only place a scope is ruled on, still called from
`guard` before any handler, and still reads the subject off the matched route.
what changed is that `may_touch_job` and `may_touch_asset` take the namespace
the thing is declared in, resolved from the registry inside that same function.
a mutation added tomorrow lands on the namespace rule the way it already lands
on the job rule. **a thing in no namespace is in nobody's**: a namespace-scoped
token is refused an unnamespaced job exactly as it is refused another team's.

`?namespace=finance` narrows `GET /api/jobs`, `/api/assets`, `/api/schedules`
and `/api/sensors`. an absent parameter is every row; an empty one is the same,
since nothing is ever in the namespace `""`. **it narrows a list and is not a
permission**: reads are still not scoped, exactly as `docs/auth.md` says. the
ui keeps it in the url like every other filter, and `hestan assets --namespace`
sits beside the `--group` that was already there.

**a job and an asset say who owns them.**

```rust
Job::builder("orders_etl").owner(
    Owner::team("data-platform")
        .person("ada")
        .contact("#data-alerts")
        .escalates_to("ops@example.com"),
)
```

a team, a person, or both, plus how to reach them. that is the whole shape:
**hestan carries this and hands it to a hook. it is not a directory service**
and it never parses, resolves or dials one of these strings.

**it reaches the alert without the caller threading it through**, which is the
difference between this and a field on a struct. the executor reads the owner
off the declaration at the one place a run's terminal event is built, so
`RunEvent::owner` and `RunFailure::owner` are already there when a hook is
called:

```rust
Hestan::new().on_failure(|f: RunFailure| {
    page(f.owner.and_then(|o| o.contact_at().map(str::to_string)))
})
```

a case registers a plain closure over the event, before the job is looked at,
and asserts it is handed the team, the person, the contact and the escalation
contact. the built-in slack line ends `, owned by data-platform
(#data-alerts)`; one about something nobody claimed ends where it always did,
and a case asserts the webhook body has no `owner` key at all then. durable
delivery carries it in the notification row, and a row written by an older
hestan reads back as an event with no owner rather than a payload that will not
parse.

`LateEvent::owner` is the other half, and it is how an **asset's** owner
reaches a hook. **the limit, plainly: an asset build's run event carries the
internal `assets` job's owner and not the asset's.** one run can build several
assets with several owners and picking one would be a guess. an asset's own
owner reaches a hook through `on_late` and a person through the api, the ui and
the command line.

**escalation is a second contact and hestan does nothing with it.** it does not
wait, time anything, ask whether the first contact answered, take an
acknowledgement, repeat, or know about a rotation or a shift. that line is
drawn on purpose and written down rather than implied: an escalation *policy*
with timers and acknowledgement is a paging product, and half of one inside an
orchestrator would be the worst thing to ship, something that looks like it
will keep trying and does not. the promise is exactly that the second contact
reaches your hook beside the first.

**an owner nobody declared is an absence everywhere**: `null` in the api, no
line on the page, `-` in the cli column, and no key in a webhook body. never an
empty string dressed as a name, and there is a case on each surface.

```
$ orders owner margin
WHAT   NAME    OWNER           CONTACT      ESCALATES TO
asset  margin  ada of finance  #fin-alerts  ops@example.com
```

**what an existing deployment sees change:**

- **nothing behaves differently until something declares one.** no schema
  version, no migration, no new column, no new route, and a deployment that
  declares no namespace and no owner answers every request the way it did.
  there are cases on both halves asserting that rather than assuming it.
- **the responses are not byte for byte.** `namespace` and `owner` are new keys
  on `GET /api/jobs` and `GET /api/assets`, `namespace` on `/api/schedules` and
  `/api/sensors`, `owner` on `/api/late`, each `null` where nothing was
  declared. a client reading keys by name is unaffected; one comparing whole
  documents is not.
- **BREAKING, source only: `Scope::may_touch_job` and `Scope::may_touch_asset`
  take a second argument**, the namespace the thing is declared in. it is a
  compile error rather than a behaviour change, and `None` is exactly what
  those calls meant before. the whole scope surface otherwise stands.
- **BREAKING, source only: `RunEvent`, `RunFailure` and `LateEvent` gain an
  `owner` field**, which breaks a struct literal of one. `docs/stability.md`
  has always said these are things hestan hands you and that they gain fields.
- **`Owner` has private fields and constructors**, unlike the row types, which
  is the pattern `docs/stability.md` said it wanted for a struct a caller
  builds and did not have an example of until now.
- **a schedule's namespace over `--db` is null.** a run log holds no job
  definitions and a schedule's namespace is its job's, so the mode that cannot
  see the registry reports it as unknown rather than guessing. every other mode
  answers it.
- **the ui shows what was declared and hides what was not**: a namespace filter
  only where something declared one, an owner line only where somebody is
  named. it does not act on a namespace any more than it acts on a scope.

- **`docs/namespaces.md`** is the new page: the one-sentence rule, why a group
  and a namespace were not merged, how a scope composes with one, what reaches
  a hook, and where the escalation line is. `docs/auth.md` gains
  `Scope::namespaces`, `docs/notifications.md` and `docs/freshness.md` gain the
  owner on each payload, `docs/assets.md` narrows the group paragraph,
  `docs/http-api.md` gains the field and the parameter, `docs/cli.md` gains
  `owner`, and `docs/stability.md` records the `Owner` decision.
- **cases**: a namespace over all four kinds; a scope naming one admitting its
  jobs and assets and refusing another's, an unnamespaced job's, and the
  deployment's; a job's history surviving the declaration; the filter in the
  url on the api and in the ui; a build refusing a namespace nothing could name
  again; a group and a namespace disagreeing on one asset on purpose; a failure
  hook handed the owner it was never given; the slack line with an owner and
  without; a late crossing carrying the asset's own owner; an old notification
  row delivering; the job and asset payloads; and `hestan owner` answering for
  a job, an asset, something nobody claimed, and a name nothing is registered
  under.

a deploy token passed as a param used to be written to `runs.params`, shown on
the run page, returned by `GET /api/runs` and kept until retention pruned it.
and a token that could launch a deploy could also cancel production runs,
because the roles say how much somebody may do and nothing about what to. two
things, both about a credential being wider than it needed to be.

**a param a caller declares secret does not reach the store.**

```rust
Op::new("push", body).secret_params(["token"])
```

the op still reads the value. what changes is that nothing writes it down:
`runs.params`, `schedules.params` and `presets.params` all get
`"[hestan:redacted]"` where it was.

**the redaction is in the store, not in any renderer.** `Store` holds what each
job declared and applies it to every params column it writes, before the
insert. so the ui, the api, the event log, the cli, `hestan doctor`, a `psql`
prompt and a route added next month are all reading a row that never held the
value, and none of them had to be taught anything. a value scrubbed in a
renderer is still in the database and still on every other renderer, and the
next reader somebody adds gets it for free.

the ops get it because the process that took the launch keeps it in memory,
keyed by run id, and puts it back into the params of the run it is about to
execute. it goes to no disk on the way.

**BREAKING, in behaviour: a run launched with a secret param cannot be
replayed, resumed or retried.** this is the sharp edge and it is deliberate.
those three read a finished run's stored params back, the store holds the
marker, and re-launching from that row would run the deploy with the literal
string `[hestan:redacted]` as its credential: a run that fails confusingly at
best and authenticates as something unintended at worst. so the marker is
refused as a param value at `Runner::enqueue`, the one funnel every run goes
through, which means the four paths that read params back land on it without
any of them checking:

```
job deploy: param token is declared secret and not stored, so what came back is
the marker and not the value. a retry, a resume or a replay cannot re-read one:
launch again and pass it
```

`409` on the api, exit 2 on the cli, and the resume and replay *previews*
refuse identically so the ui never offers a button that cannot work. re-running
means launching again and passing the value. if that is not acceptable for a
job, do not declare the param: put the credential in a resource, which is
process configuration, is built where the op runs, and is rebuilt on a replay
like anything else.

**what is covered, and by what.** run params, schedule params and preset params
by the declaration; the event log, the ui, `GET /api/runs`, `hestan runs show`
and `hestan doctor` because they read the row; `hestan run --dry-run` by
applying the declaration itself, since it never touches the store; and the
refusal a `.params::<P>()` check gives, which is the classic leak, because
serde quotes back what it was handed and `Job::params_error` is the one
function that produces that sentence for the launch, for `validate_params` and
for `--dry-run`.

**a second line, said to be second.** an op holds the value and can log it or
put it in metadata, and a declaration cannot stop that. so while a run holding
secret values is executing in this process, every string bound to every
statement the store issues is scanned for those values and any that appears is
replaced. it sits at the store's parameter binding, so it covers op output,
metadata, log lines, op errors and every event including ones a future table
carries. it finds a copy and not a transformation, it skips values under
sixteen characters (a shorter needle matches inside run ids and timestamps and
rewriting those would corrupt the run log), and it is not applied to reads.

**nothing guesses.** hestan does not match param names against
`token|secret|password`. a pattern misses the credential somebody called `key2`
and redacts the innocent column named `password_column`, and a redaction that
is sometimes wrong is one nobody can reason about. a param nobody declared is
stored.

**the limits, plainly.** top-level keys only, so `{"db": {"password": …}}` is
not covered. one process: the value is in the memory of whatever took the
launch, so a worker in another process finds the marker, refuses to execute on
it and fails the run naming the param. secret params therefore work on a
single-process deployment and on a multi-process one only when whatever
launched also executes; a `Role::Scheduler` enqueuing for `Role::Worker`
processes is exactly the shape they do not work in, and a resource is the
answer there. and it encrypts nothing: the marker is a marker.

**a token scoped to the jobs it may touch.**

```rust
Identity::operator("ci").scoped_to(Scope::jobs(["deploy"]))
```

an operator on `deploy`, a stranger everywhere else. a scope is a list of jobs,
a list of assets or both, and the list is the whole of what may be touched.

```json
{"error": "ci is scoped to job deploy, and this changes job billing"}
```

**enforced in the guard, before any handler, and derived from the path rather
than from a list of routes.** the first `{placeholder}` in the matched route
names the subject and the literal before it says what kind: a job, an asset, a
run (resolved to its job off the row) or a backfill (resolved to its asset).
**anything with no placeholder at all is the deployment, and a scoped token may
not touch the deployment.** so `POST /api/assets/build`,
`POST /api/schedules/state` and `POST /api/sensors/state` are refused by the
rule, and so is a mutation added next month that names no job or asset in its
path. a case scrapes every route out of the router and asserts a token scoped
elsewhere is refused at all of them, so a route added tomorrow is covered the
moment it is written.

**a scope does nothing to a read, and that is a decision.** a write names in
its path what it is about, so one check rules on every write there will ever
be. a read is mostly a list, and narrowing those means a filter inside each
handler that builds one, which is a check that has to be remembered at every
list and will be forgotten at one. a confidentiality promise that holds nine
times in ten is worse than none, because people plan around it. so: **a scope
is not a confidentiality boundary**, a token that can read reads the whole
deployment, and a deployment that needs the read half narrowed does it in
`Auth::custom` or in front of hestan, where it can be exact.

a scope cannot be widened by whoever holds the token: an `Identity` is built by
an authenticator in the deployment's own process, and nothing in a header, a
query or a body reaches the scope on it. it is an api limit and not a
capability check inside the library: code holding a `Runner` launches whatever
it likes, exactly as it always could.

**BREAKING, source only: `Identity` gains a public `scope` field.** an
`Identity { name, role }` literal no longer compiles; `Identity::new`,
`viewer`, `operator` and `admin` are unchanged and are what the docs have
always shown. `GET /api/whoami` gains `identity.scope`, which is
`{"everything": true, "jobs": [], "assets": []}` for every token that existed
before this.

**what an existing deployment sees change:**

- **no schema version, no migration, no new column, no new route.** the store
  is at v21 exactly as it was, and nothing currently stored becomes unreadable:
  a param already in `runs.params` is still there, still plain text, still on
  the api and the ui, and still replayable. the redaction is applied at the
  write, so it only ever affects rows written after a job declares a param.
- **a job that declares no secret param writes params byte for byte as it
  did.** the redaction, the second-line scan and the marker refusal are all no
  ops when nothing is declared, and the scan costs one atomic load per write in
  that case.
- **an unscoped token is decided by the role ladder alone**, as it was, and a
  case asserts that against every row of the roles table rather than assuming
  it. `Auth::bearer` is unscoped, so the one-token deployment and the ui are
  untouched.
- **one source break**, the `Identity` literal above.
- **the ui shows a scope and does not act on one.** the header reads
  `ci · operator · jobs deploy`; the controls are not hidden, so a scoped token
  still sees another job's launch button and the api answers 403 naming the
  scope. hiding them would mean a scope check at every list, tile, palette
  entry and detail page, which is the per-call-site check the api rule exists to
  avoid. the disagreement is deliberate and `docs/auth.md` says so rather than
  leaving it to be found.

- **`docs/secrets.md`** is the whole of the first half: where the redaction is
  and why it is there, the replay consequence, what is covered by what, the
  second line and that it is second, and the limits.
- **`docs/auth.md`** gains the scopes, the read decision with its reasoning,
  the deny-by-default rule and the ui exception; its "there are no per-job or
  per-asset permissions" bullet was true and is not any more.
- **cases**: the op reading the credential while the whole database is scanned
  for it and does not have it; each of the three params columns written through
  the declaration and a job that declared nothing written unchanged; the four
  re-run paths and both previews refusing with the param named; a second
  process failing the run rather than executing on the marker; a params check
  quoting the marker instead of the value; the api and the launchpad prefill
  showing the marker and the prefill refusing to be launched back as it stands;
  the cli dry run redacting what it never stored; a scoped token driving what it
  owns and refused the rest with the reason; reads unnarrowed; an unscoped token
  unaffected across the whole roles table; the widening attempts; and every
  route in the router refused for a token scoped elsewhere.

"is orchestration healthy" is now a question a scrape can answer.

hestan had traces behind the `otel` feature and an `/api/health` about one
process. it had no counters, so the state of a deployment could only be read by
opening the ui or querying the run log yourself. **`GET /metrics` is prometheus
text exposition**, in every build.

**what an existing deployment sees change:**

- **one new route, and nothing else moved.** no public signature changed, no
  schema version, no dependency, no behaviour, no existing response shape.
  `/api/health` is byte for byte what it was.
- **it is on, and there is no switch.** it is not behind a feature, because
  unlike `otel` it adds no dependency, and a feature in this crate is a
  dependency somebody gets to decline. hestan has nine feature configurations
  in its gate already and a tenth for a text formatter would be a cost with
  nothing on the other side of it.
- **it is inside the [auth guard](docs/auth.md) and needs a viewer.** a
  deployment with a token refuses an unauthenticated scrape with a 401, and
  prometheus carries one in `authorization` (a `ServiceMonitor` and a
  `PodMonitor` too). that is the difference from `/api/whoami`, which is open
  because a kubelet's `httpGet` probe has nowhere to put a token at all. a
  deployment with no authenticator serves `/metrics` to anyone who can reach
  the port, exactly as it serves everything else, and `serve` already refuses
  to bind a reachable address without one unless you asked for `Auth::None`.
- **there is no opt-out beyond that.** a deployment that does not want the
  endpoint blocks the path at whatever is in front of it. adding a flag for it
  would be a second way to configure something the guard already covers.

**twenty-one metrics, and the two halves do not mean the same thing.**

- **eight gauges are read off the run log when the scrape arrives**: queue
  depth, how long the oldest queued run has waited, runs active, runs stalled
  past their lease, paused schedules, paused sensors, how long the decision
  lease has left, and whether this process is the one holding it. every process
  answers with the same figure for the first seven, so they aggregate with
  `max` and never with `sum`; the eighth is how you find *which* target is
  deciding.
- **three more gauges are about the process**: whether it can read the store,
  whether its last write landed, and how many runs it gave up on.
- **eight counters and two histograms belong to the process that was scraped**
  and **read zero after a restart**: run outcomes by status, claims, reclaims
  by outcome, op retries, schedule fires by outcome, three store-write
  counters, and histograms for claim latency (queued to claimed) and schedule
  lateness (due to fired).

**the counters are in-process on purpose and it is worth saying why.** every
table hestan keeps is prunable by retention, so a `_total` read off a
`COUNT(*)` would fall the first time a prune ran, and prometheus reads a
counter that fell as a restart and invents the rate to match. each counter is
instead moved at the store call that has just committed the same fact, so a
write that was refused and retried moves it once, a write that never landed
does not move it at all, and the counter and the run log agree or neither says
anything. a restart zeroes them, which is the one thing prometheus already
knows how to handle.

**no metric carries a job name, an asset name, an op name, a partition key or
a run id, and that is enforced by a type rather than by a note.** a label value
in `src/metrics.rs` is a `&'static str`, and a name read out of a database row
borrows from the row, so it does not typecheck as one. what is left is the
words hestan spells in its own source, which is why every label comes from a
**closed** enum (`RunStatus`, `Reclaim`, `TickOutcome`, whose `as_str` returns
`&'static str`) while an open enum's borrows from `self` and cannot be written
there at all. the cost is stated rather than hidden: **you cannot ask this
endpoint which job is failing.** it says runs are failing; `/api/runs?job=…`,
the ui and the run log say which.

**what was left out, and why:**

- **op outcomes by status.** an op's terminal row is written by whichever
  process ran the op, and an isolated op writes its own from a child process
  that exits immediately after, taking any counter with it. a per-process op
  counter would undercount exactly the deployments that use isolation, and
  silently. runs have no such split, because a run is executed by the one
  process that claimed it.
- anything per job, per asset or per partition; run duration, whose useful cut
  is per job; sensor tick counts, which `/api/sensors` already answers; asset
  freshness, which `on_late` already alerts on without prometheus; and a build
  info metric, because nobody pages on a version string.

**six of the gauges are facts `hestan doctor` already reports**, off the same
rows, and neither reads the other. doctor is a person asking once and getting a
sentence and a fix; a metric is a scrape asking every fifteen seconds and
getting a number you can put a threshold on. what stays in doctor and cannot be
a metric is everything that needs the registry rather than the store
(`policies`, `rates`, `groups`, `colours`, `retention`), because each is per
asset or per declaration and would need the labels this endpoint rules out.

- **`docs/metrics.md`** is the whole of it: every metric with its type and
  labels, the promql for what to alert on, the argument about which side of the
  guard it sits, what may be a label, the doctor overlap, and what is
  deliberately not there.
- **`deploy/k8s/podmonitor.yaml`**, a PodMonitor rather than a ServiceMonitor
  because the counters are per process and no service fronts every hestan pod;
  both pod templates also gain the `prometheus.io/*` annotations, which do
  nothing without something relabelling on them. **still not applied to a
  cluster**, and it now also assumes a prometheus operator, which is a second
  thing nobody here has run.
- **`docs/containers.md`** has the scrape against the running compose stack,
  including why the workers are scraped by container name rather than through
  `worker:4000`: a target that reaches a different replica each scrape reports
  counters that jump around.
- **`Health` gains a third counter**, writes the store refused that hestan
  tried again, which is the leading indicator the other two do not give.
  internal: it is on `/metrics` and `/api/health` is unchanged.
- **run on the compose stack**, which is where the first version of this was
  wrong: a fired occurrence writes its tick in the transaction that created the
  run rather than through `record_tick`, so `hestan_schedule_fires_total` read
  zero on a scheduler that had fired six. the first occurrence after a fixed
  stack came up read `fires_total{outcome="fired"} 1` and a lateness of 9ms on
  the scheduler, and `run_claims_total 1` with a claim delay of 79ms on the
  worker that took the run, with the other two workers at zero. the numbers are
  in `docs/containers.md`.
- **cases**: the counters against the writes that record the same facts, on
  both backends; the exposition parsed the way a scrape parses it, with a type
  and a help for every family and the suffix conventions checked; a counter
  moving when the thing it counts happens and a gauge following the store; a
  restart keeping the gauges and starting the counters over; and a store filled
  with two hundred job names, two hundred partition keys, two hundred schedules
  and two hundred sensor names rendering exactly the series an empty one does.

**BREAKING: eleven public enums are now `#[non_exhaustive]`.** a `match` on
one of them from outside this crate needs a `_` arm, and that is the whole of
the break: no variant was added, renamed or removed, every variant is still
constructible by name, `matches!` and `if let` are untouched, and the stored
words, the json and every api response are byte for byte what they were.

the eleven are **`Error`, `Meta`, `InputError`, `Auth`, `Trigger`,
`SubjectKind`, `EventKind`, `TickOutcome`, `When`, `Reclaim` and `Blocked`**.
what a caller adds is one arm:

```rust
let label = match run.trigger {
    Trigger::Schedule => "on a schedule",
    Trigger::Sensor => "a sensor",
    _ => "somebody asked",
};
```

three of these had already grown, and each time it was a source break somebody
met at `cargo build`: `Meta` gained `Series` and `Saved`, `Trigger` gained
`Replay`, and `EventKind` gained `RunReleased` one entry below this one, where
this file said it was "breaking at compile time only" because the enum was not
`#[non_exhaustive]`. it is now, and this is the last time any of the eleven
does that to anyone.

| now open | because |
| --- | --- |
| `Error` | a new refusal is a new variant, and which variants exist already depends on the features compiled in |
| `Meta` | it grows whenever the ui learns to draw something, and a match on it is picking a rendering |
| `InputError` | the typed accessors gain ways to come up empty as the accessors grow |
| `Auth` | another way of checking who is asking is a variant here; it is configured, not read back |
| `Trigger` | whatever causes a run next is a variant here |
| `SubjectKind` | a new thing hestan keeps rows about is a new kind |
| `EventKind` | a new kind arrives with every subsystem that learns to record something |
| `TickOutcome` | it gained `Deferred` with overlap queueing and gains another with the next thing a schedule can do |
| `When` | three trigger rules are not the whole space of them |
| `Reclaim` | hestan knows how to resume a run, so a third answer to an expired claim is one it may grow |
| `Blocked` | its own `Undefined` docs already name the variant after it |

**seventeen public enums were left closed, and that is the other half of the
decision.** an enum a caller can still match with no `_` arm is one hestan has
promised not to grow, and each of these is worth more as that promise than as
room to extend:

| still closed | because |
| --- | --- |
| `Exit` | `docs/cli.md` publishes the nine exit codes as fixed and a cron line is a `case` over them |
| `RunStatus`, `OpStatus`, `BackfillStatus`, `DeliveryState` | a state machine, where a new state changes what the existing ones mean and a `_` arm would hide that rather than absorb it |
| `Access`, `EventLevel`, `Severity` | ordered scales, where a value inserted into the order changes the comparisons already written against it |
| `Overlap`, `Catchup`, `Role`, `Freshness`, `CancelOutcome`, `SensorOutcome` | the answers cover their question, and a further choice would be a field on a variant rather than a variant beside it |
| `LateKind`, `LogStream`, `CheckStatus` | two of a thing that comes in two: job or asset, stdout or stderr, satisfied or not |

each of the twenty-eight now carries the reasoning in its own rustdoc, in one
sentence saying whether hestan will add a variant there and what a caller loses
if it cannot match exhaustively.

- **the public structs were looked at and deliberately left alone.** thirty-one
  of them have public fields, so adding a field to one is a source break for
  anybody writing a struct literal. twenty-eight are things hestan hands you
  (`Run`, `Event`, `OpRun`, `Materialization`, the hook payloads, the rest of
  the store's rows, and `IoKey`, which an `IoManager` receives rather than
  builds) and a caller only reads them. the three a caller does build are
  `EventQuery`, `RunRequest` and `Identity`, and each already has a way in that
  a new field does not break: `EventQuery` derives `Default`, so
  `EventQuery { level: Some(EventLevel::Error), ..Default::default() }`, and
  the other two have constructors. `#[non_exhaustive]` on a struct blocks the
  struct literal outright, functional update syntax included, so putting it on
  the row types would leave anybody with a legitimate reason to build one, a
  test fixture or a fake store, with no way to build it at all. that wants
  constructors first, and it is not this phase.
- **`tests/stability.rs`** matches every one of the seventeen closed sets with
  no `_` arm. it is an integration test on purpose: `#[non_exhaustive]` does
  not restrict the crate that defines the type, so the same matches inside
  `src/` would compile either way and prove nothing.
- **`Trigger`'s rustdoc carries a `compile_fail` doc example**, the exhaustive
  match that no longer builds, beside the one that does. `cargo test` runs
  both.
- **`docs/stability.md`** is the rest of the answer, and the part that was
  missing rather than wrong: the five surfaces and what each holds still,
  the closed sets beside the ones that will grow, the structs that are read
  rather than built, the extension points that are contracts because
  somebody implements them (`IoManager` first, with `drop_run` as the worked
  case), and the list of things that are deliberately not a surface at all:
  the module paths inside the crate, the wording of an error, the ui's html
  and urls, the sql, and every tuning number. linked from the readme, the
  docs index and `lib.rs`.
- **two more cases in `tests/stability.rs`**, because a stability claim
  nothing checks is a comment: `src/` is read back so a public enum cannot
  land with nothing recorded about it, and the nine exit codes are read out
  of `docs/cli.md` and asserted against the `Exit` variants.

a process asked to stop stops, and hands back what it was holding.

phase 43 measured the problem and could not fix it: hestan installed no signal
handler in its server process, and the kernel drops an unhandled signal sent to
pid 1 of a container, so `docker stop` on a hestan container waited its full ten
seconds and killed it. `Hestan::serve` and `Hestan::work` now listen for SIGTERM
and SIGINT. `run_once` and `build_asset` deliberately do not: a headless
one-shot exists to execute the run it was asked for, and a handler that made it
wait for a drain, or swallowed the signal that would have ended it, would be a
worse bug than the one being fixed.

**what an existing deployment sees change**, cost first:

- **a process takes longer to exit than it used to.** it is finishing work
  rather than dying. an idle one is gone in milliseconds; one holding a run
  waits for the run, up to `Hestan::stop_within`, **eight seconds by default**.
  a rolling restart timed around a process that died the instant it was
  signalled is timed around a different number now. eight because `docker stop`
  gives ten and the handback and the exit go in what is left; kubernetes gives
  thirty, and a deployment whose ops need longer can say so
- **the deciding lease comes back on a stop**, so the next process takes the
  term without waiting out the expiry. measured by `deploy/checks/stop.sh` on
  one compose stack, with hestan as **pid 1 and no `--init`**, polling the
  `decider` row the same way for both:

  | | until another process held the term |
  | --- | --- |
  | after `docker kill` | 10033ms, inside the 8286 to 10425ms phase 43 recorded over four kills |
  | after `docker stop` | **1448ms** |

  and the container itself exited **0** within 39ms of the signal,
  where before it waited out the whole ten second grace period and reported
  137. a stop is not instant and should not be read as instant: the lease is
  free the moment the leader goes, and what is left is the next process's two
  second renewal interval rather than the ten second expiry
- **a run a stopping process cannot finish goes back on the queue** rather than
  sitting claimed until its sixty second lease runs out, and it is **not**
  recorded as failed: no failure hook fires, no alert is sent, and its ops go
  back to `pending`. it did not fail, the process left. whoever claims it next
  runs it **from the beginning**, so an op that already ran runs again, which
  is the same trade `Reclaim::Requeue` has always made
- **an open event stream ends when the process serving it is stopped.** it is
  the one response with no natural end, so a stream that went on polling would
  be the connection holding a stopping process open. reconnect with the
  `Last-Event-ID` you already have and nothing is missed
- **`EventKind` gains `RunReleased`** (`run_released`, warn, payload
  `{"claimer": ...}`). `EVENT_SCHEMA` does not move: it says in as many words
  that a kind may be added without it. **breaking at compile time only**: the
  enum is not `#[non_exhaustive]`, so an exhaustive `match` on it needs one
  more arm, and it has an `Unknown` arm precisely so that nothing has to
- **`Hestan::stop_within(Duration)`** is the only new public method

**what an isolated op's child process gets** is nothing, and it is worth saying
plainly because it is the one process in a hestan deployment that hestan did
not start. it is not signalled and it is not asked to stop, because the op is
being given time to finish. if it finishes inside the deadline the run finishes
with it; if it does not, the parent drops it on the way out, and dropping an op
subprocess kills it, so a released run is never still being executed by the
process that gave it up. what the container runtime does with the signal is a
separate thing and `docs/isolation.md` says which: `docker stop` and a kubelet
signal pid 1 alone, while a ctrl-c at a terminal signals the whole foreground
process group and the child dies of that instead, which the parent then records
as a failed attempt.

- **`tests/stopping.rs`**, nine cases over real processes signalled from
  outside: a served process exits on the signal rather than being killed, a
  one-shot is untouched and still dies on SIGTERM, a second signal
  short-circuits the wait, the lease is handed back and the term moves, a run
  that fits inside the deadline finishes, one that does not is released rather
  than failed, another worker picks a released run up without waiting for the
  lease, the deadline is honoured, and an isolated op's child does not outlive
  the stop. the store's half of it runs on both backends
- **`deploy/checks/stop.sh`**: the stop and the kill on one stack, measured the
  same way, plus a worker stopped with a run in flight. it asserts the shape it
  is testing before it tests anything (`HostConfig.Init` unset, the hestan
  binary at pid 1), because a check behind an init shim would pass without
  going near the case that was broken
- **`deploy/k8s`**: `terminationGracePeriodSeconds` stays 10 on the scheduler
  and 30 on the worker, and the comments now say what the number is a ceiling
  on rather than a timer the pod runs out, and that hestan's own deadline is
  what decides. still not applied to a cluster
- and one thing that is not this phase's work, called out rather than folded
  in: the deciding-lease case in the store suite took a **twenty millisecond**
  lease and then asserted, one round trip later, that it was still live. on
  postgres, with the rest of the suite running beside it, twenty milliseconds
  is not reliably longer than a round trip, and it failed on this machine on
  `b544cfa` with none of the above in the tree. the lease is half a second now
  and nothing the case asserts changed

a hestan deployment in containers, and the first test of the term fence
against a network that was actually taken away.

**no library code changed.** `src/` is byte for byte what it was: no public
signature moved, no schema version, no behaviour, no dependency. the ui is
unchanged. one line of `examples/demo.rs` changed, which is published crate
content and is called out below. everything else is new files, and the manifest
declares no `include` or `exclude`, so they do ship in the `.crate`: about 70kb
of Dockerfile, compose files, shell scripts, kubernetes manifests and a docs
page, none of which is compiled or read by anything at build time.

- **a `Dockerfile` that builds a hestan application**, using `examples/demo.rs`
  as the worked case, because hestan is a library and there is no hestan server
  to publish an image of. two stages, `rust:1.88-slim-bookworm` to
  `debian:bookworm-slim`, neither a `latest` tag. **164mb unpacked and 35.5mb
  to pull on arm64**, of which the debian base is 108mb and the binary is
  18.8mb. the runtime layer holds those two, the ca-certificates bundle copied
  out of the build stage, and a uid 10001 to run as; no package manager runs in
  it and there is no curl, no wget and no psql, so every health check is made
  from outside the container
- **the ui is copied in, not built.** `ui/dist` is committed because
  `include_dir!` needs it before rustc runs and the crate has to compile with
  no node anywhere, so the image installs no node and runs no npm. the price is
  that it carries whatever `just ui-build` last wrote
- **`docker-compose.yml` is now postgres, one scheduler and three workers**
  rather than two workers against a shared sqlite volume. the role split from
  `docs/scaling.md` as five containers that run. `docker-compose.spare.yml`
  adds a second scheduler, which is what a handover needs somewhere to hand to.
  **it is still one host**, and the file, the docs and this entry all say so
- **`examples/demo.rs` reads `HESTAN_SCHEDULE`**, defaulting to the
  `*/2 * * * *` it always had. six fields is seconds first, so the checks can
  drive it at an occurrence every ten seconds instead of waiting two minutes
  for each one. this is the only change to anything the crate compiles
- **`deploy/checks/`: the fault injection, as scripts somebody can run.**
  `bash deploy/checks/run.sh`, or `just checks`. they are not `cargo test`
  cases and will not become them: a partition takes a minute of wall clock to
  be a partition. each one brings up a stack of its own, does one thing to it,
  and asks the **store** what happened rather than asking a process that may be
  the one that is wrong about it

**the term fence held under a real partition, and this is the first time it has
been tested rather than reasoned about.** `docker network disconnect` leaves a
process running with its clock going and no route to the database, which
several processes on one box cannot produce: there a leader that has lost the
store is a leader somebody stopped. cut off for 65 seconds, the process decided
nothing, another took the term, and when its blocked store calls returned it
went straight on with the pass it had been in the middle of:

```
16:14:41.915307  INFO  schedule fired job=orders_etl expr=*/10 * * * * *
16:14:41.915615  WARN  fire refused: the deciding lease moved on before the fire landed
16:14:41.915706  WARN  deciding: the lease is no longer this process's term=1
```

read the order. it decided to fire, the store refused it, and **only then** did
its own decider loop find out the lease had moved on, so the term that fire
named was 1 and 1 was out of date. nothing in the process stopped it. the store
did, in the transaction that would have written the run, and it left no tick and
no run.

- **handover measured rather than asserted**: 8954ms from `docker kill` to
  another process holding the term, with 8.91s left on the ten second lease at
  the moment of the kill, and 9152ms to the first occurrence the new decider
  fired. over four kills it was 8286ms, 8954ms, 9934ms and 10425ms, which is
  whatever was left of the lease plus up to one two second renewal interval.
  time to the next *decision* is not that bound and was 9152ms on that run and
  19435ms on another, because the occurrence that came due inside the gap was
  not fired at all: `Catchup::Skip` treating a handover as downtime, exactly as
  documented
- **no occurrence fired twice** across a partition, a handover and a reconnect
  run one after the other, which is the unique index over
  `(job, expr, scheduled_for)` doing the job it was built first to do
- **a partitioned process is unreachable for longer than the partition**, and
  by an amount nothing in hestan controls: its store calls sit in a tcp write
  that has been backing off, and came back between 3.2 and 44.5 seconds after
  the interface did, over seven partitions of 50 to 95 seconds. new, and worth
  knowing before you time a deploy around one
- **a runless tick is not fenced, and `docs/scaling.md` did not say so.** a
  stale decider that wakes to find a run of the job already active applies the
  overlap policy and records a `skipped` tick instead of attempting the fire the
  store would have refused. it creates no run, so it names no term and goes in
  unrefused; what it costs is a tick log row no live decider wrote, and the
  unique index is over `fired` alone so it neither blocks a real fire nor
  becomes one. the not-fenced table has a row for it now, and
  `partition.sh` cycles until it gets a fire, because a partition where nothing
  was fenced proves nothing about the fence
- **a hestan container ignores SIGTERM**, measured: `docker stop` waited its
  full ten seconds and killed it (exit 137), while the same image behind
  `docker run --init` exited on the signal in 196ms (exit 143). hestan installs
  no signal handler and the kernel drops an unhandled signal sent to pid 1 of a
  container. so `docker compose down` takes ten seconds a container, a
  container being stopped keeps working until SIGKILL, and **the deciding lease
  is never handed back on a stop**: `docs/scaling.md` said a clean stop hands it
  back, and in a container there is no clean stop. the sentence now says so
- **`deploy/k8s/`: manifests that have never been applied to a cluster**, and
  every file says that on its first line. a ConfigMap, a Secret, a Deployment
  for the schedulers, a Deployment for the workers, a Service and a
  kustomization. `kubectl kustomize` renders them and that is the whole of what
  anybody has done to them. the scheduler defaults to **two replicas**, which
  the deciding lease is what makes safe rather than forbidden, and the probes
  point at `/api/whoami` because it is the one endpoint outside the auth guard
  and a kubelet cannot carry a token into an httpGet header. what that probe
  proves is that the process is serving, not that its store is reachable, and
  the file says which. there is no helm chart, deliberately: what changes
  between deployments here is an image, two replica counts and a secret, and a
  chart nobody has rendered against a cluster is untested surface in the shape
  of a product
- **`docs/containers.md`**, linked from `docs/scaling.md` where it talks about
  several hosts, with the image, the stack, the fault injection numbers, the
  signal, and what none of it tested: a partition between hosts, a clock that
  jumps, or a process paused past its own expiry, which is the case the fence's
  own doc comment is written about and is still reasoning

a finished run's page shows the data the run produced, and not only that it
succeeded and how long it took.

**an existing deployment sees no change until an op calls `saved`.** the
section is absent from every run that has none, every metadata value written
so far is stored byte for byte as it was, **no public signature changed**, and
nothing about what runs, or when, is different. what is added is two `Meta`
variants, one method on `OpCtx`, and a section on the run page that appears
when there is something to put in it.

**breaking, at compile time only**: `Meta` gains `Series` and `Saved`. the enum
is not `#[non_exhaustive]`, so an exhaustive `match` on it needs two more arms.
no signature moved and nothing stored changed, so this reaches only code that
matches `Meta` without a `_`.

**hestan runs no query.** the op supplies the sample, because the op is the
one already holding the connection; three lines of its own sql select its own
rows back. there are no credentials to hand hestan, no dialect for it to know,
and nothing to configure for a warehouse it has never heard of.
`examples/demo.rs` writes to a real sqlite table and reads its own sample back
out of it.

- **`ctx.saved(name, value)`, beside `ctx.meta`.** the same staging and the
  same storage, marked as a sample of what the op wrote, and stamped with the
  moment it was taken. the mark is what makes a run-level section possible:
  without it a sample is one entry among counts and paths, reachable only by
  selecting the op it belongs to
- **it is a snapshot, and the page says so.** what is stored is what the op
  read when it called `saved`; a later write to that table does not reach it
  and nothing goes back to look again. that is right for a record of a run and
  it is not what "what is in the table now" means, so the section leads with
  the sentence, every entry carries when it was taken, and a saved value
  anywhere else in the ui is marked `snapshot`. the cost of the mark is that
  `Meta::as_f64` reports no number for one, so a saved key gets no delta and
  no trend: a key that wants those wants `ctx.meta`
- **`Meta::series(points)`, sampled across its range rather than off its
  head.** `Meta::table` keeps the first hundred rows, which is right for rows
  and wrong for a series in a way that looks right: the first two hundred
  points of an hourly year are January, drawn across an axis labelled as a
  year. a series keeps its first and last points always, spreads the rest
  evenly between them, and records how many points the sample stands for, so
  the ui says "200 of 8,760 points". **the cap is 200 and it was chosen**:
  past what a chart the width of a run page resolves, and about seven and a
  half kilobytes of json, which rides on the op run and again on every
  materialization the op wrote, so ten ops each saving one put roughly a
  hundred and fifty kilobytes on a run. retention prunes them with the rows
  they sit on and nothing else does
- **every awkward series has a decided answer**, documented and tested one by
  one. non-finite values are dropped before anything else and are not counted
  either, since json has no NaN and one of them makes the range meaningless;
  points are sorted, because a warehouse returns what it returns and unsorted
  is the common case; two points sharing a timestamp are both kept in the
  order they were given, because which of two readings for one instant is
  right is not hestan's to decide; an empty series is an empty series, which
  is the op having looked and found nothing; one point is one mark
- **the section on the run page**, under the gantt and above the log: every
  sample from every op, ordered by op name (which is how the api returns
  them, not dag order), each labelled with the op that took it. a
  series draws with its value range on the axis, its first and last timestamps
  under it in utc as the op stored them, and every point reachable as a number
  under that. **it takes no hue.** colour means group or origin in this ui and
  shape carries status; one series has neither to say, so it is drawn in the
  monochrome everything else is

more than one process may now decide, and the store rather than a lock is what
makes that safe.

**an existing single-process deployment sees almost nothing.** two new rows in
its database (a `decider` row and, on upgrade, a `schedule_ticks_fire` index)
and one new line on `GET /api/health` and on the activity page saying this
process is the one deciding. **no public signature changed**; nothing about
what runs, or when, is different; one process on one database takes the
deciding lease before `serve` binds its socket and starts deciding with nothing
added to its boot.

the exceptions, both stated rather than buried:

- a process **killed** without handing the lease back leaves it held until it
  expires, so a restart inside that window waits up to ten seconds before it
  decides anything. a clean stop hands it back and a first boot finds it free.
  ten seconds of nobody deciding is ten more seconds of downtime, which
  catch-up already has an answer for.
- a database that has **already** been run with two schedulers has duplicate
  fires in its tick log, and the v20 migration collapses them to the earliest
  of each occurrence and says how many at warn level. those duplicate runs
  executed; deleting a tick does not unlaunch one. the count is how many times
  that deployment fired an occurrence twice.

**the constraint is the guarantee and the election is an optimisation**, and
the order is the point. a distributed lock fails exactly when a process pauses,
a disk stalls or a network splits, which is to say at the moment its holder is
most certain it still holds it. so the unique index went in first, alone, and
the lease went in on top of it.

- **schema v20: one `fired` tick per `(job, expr, scheduled_for)`**, on a
  unique index, on both backends. the tick and the run are written in one
  transaction, so a refused tick launches nothing at all: no run row, no op
  rows, no event. two processes reaching for the same occurrence produce one
  run because the database refuses the second, not because either of them
  looked first. the index is **partial, over `fired` alone**: the tick log is
  also the queue, an occurrence legitimately holds a `deferred` tick and then
  the `fired` tick that drained it, and what has to be unique is the decision
  that launched something. the loser records a `skipped` tick saying another
  process had already fired the occurrence, so the refusal is visible in the
  log an operator is already reading
- **schema v21: the deciding lease.** one row in a `decider` table, in the run
  lease's vocabulary (`claimed_by`, `claimed_at`, `lease_until`) because it is
  the same mechanism aimed at a different thing. ten seconds, renewed every
  two: a quarter of the run lease, because losing this one wrongly costs a
  handover and losing a run lease wrongly costs a whole re-execution. what it
  adds is a **term**, a counter that goes up on every acquisition and never on
  a renewal
- **every deciding loop takes it**: schedules, sensors, freshness, automation
  policies, backfill chunking, the retention sweep and durable delivery. each
  one waits on the lease rather than polling for it, so a process that takes it
  starts its first pass at once rather than on its next tick, and each one has
  a case that asserts it does nothing without it and then does it with it
- **a decision names the term it was made under**, checked in the transaction
  that writes it. this is the part a lease cannot do: a leader that stops the
  world past its own expiry and resumes agrees with every check it makes in its
  own memory, and disagrees with the row. fenced this way: cron fires, sensor
  requests keyed or not, policy builds and backfill chunks, all of which create
  a run for the term to ride on. **not fenced, and each one's cost written down
  in `docs/scaling.md`**: the retention sweep, freshness crossings, durable
  delivery and the sensor cursor
- **handover is downtime.** an occurrence that comes due while one decider is
  going and the next has not taken over is an occurrence due during downtime,
  and the schedule's own catch-up policy decides: skipped by default, fired
  late under `Catchup::One` or `Catchup::All`. the schedule cursor is how far
  the *deployment* has accounted for rather than how far a process has, so the
  next decider reads the dead one's cursor and sees the gap. a fire already
  queued survives either way, because the tick log is the queue
- **`GET /api/health` gained `deciding`**, and `hestan doctor` gained a
  `deciding` check. pointed at a database it says who holds the lease and
  whether anybody does; pointed at a deployment over `--server` it answers the
  sharper question, which is whether the process you are talking to is that
  one. a schedule that has not fired is a question about the deciding process,
  and pointing at a process that is not it and finding nothing wrong is the
  confusion this ends. the activity page says the same thing in one line
- **`Role::All` and `Role::Scheduler` are no longer "exactly one process".**
  starting a second is a warm spare: it holds a connection, builds the same
  registry, evaluates nothing, and takes over within ten seconds of the first
  one going away. `src/app.rs`, `src/model.rs`, `docs/scaling.md`,
  `docs/embedding.md` and `docs/scheduling.md` all said otherwise and now say
  this

an asset declares the group it belongs to, hestan computes what it descends
from, and the ui colours by one or the other.

**every existing deployment sees one change, and it is visible: colour appears
on the assets page where there was none.** the ui opens coloured by group, so
a section heading and a graph node now carry a hue beside the name they
already had, there is a legend under the graph and a `descends from` column in
the table, and `no colour` in the toggle beside the graph puts the page back
to exactly the monochrome it was. nothing else moves: **no public signature
changed**, grouping resolves byte for byte as it did on any graph that does
not call the new `.group`, and nothing about what runs, or when, is different.
`hestan assets` grew two columns, which is a change to what a script parsing
that table reads, and `GET /api/assets` gained three keys with nothing removed
or renamed.

**colour never means status, and it is never the only carrier.** the palette
everywhere else is grey and shape carries state, which is exactly what leaves
a hue free to mean something else; the moment one meant "failed" the channel
would be carrying two answers and neither reliably. every hue has its name
written on the same screen, so somebody who cannot tell two of them apart
loses speed and loses no information.

- **`Asset::group(name)`, on sources and derived assets alike.** one flat name,
  and **the resolution order is the declared group, else the part of the name
  before the first `/`, else no group at all**. the point is that regrouping
  stops being renaming: the name is the key in `asset_materializations`, in
  every lineage ref and in every op run, so moving `sales/orders` into
  `finance` by renaming it leaves a new asset with no past, and moving it with
  `.group` leaves the history where it is. three groups fail the build, each
  naming both the asset and the group: an empty one, one containing `/` (a
  folded group draws as `sales/`, so `a/b` would draw as nesting that is not
  there), and one that is also the name of an ungrouped source, which would
  make one legend entry point at two things
- **what an asset descends from, computed once.** the set of source groups it
  reaches transitively, in one forward pass over the topological order the
  build already walks, so the api, the command line and `doctor` read one
  answer rather than three. a source's own origin is itself, an ungrouped
  source contributes its own name, and an asset with no source anywhere
  upstream has an empty set, which is a real state and reads as "no source"
  rather than as a blank. ordered by name wherever it is exposed, because a
  set that reorders between requests makes a swatch flicker. a partition
  mapping changes nothing here: a mapping is about keys, not about lineage
- **a hue for a group and for a source**, 0..=359 degrees from
  `hestan::hue(name)`. a pure function of the name and **not an index into a
  palette**: an index renumbers every group after the one somebody added, and
  a graph that repaints itself when an asset is declared is a graph nobody
  trusts the colours of. the server sends the angle and the client picks the
  shade, since what lightness is legible depends on a theme the server cannot
  see. the limit is stated rather than hidden: two names can hash close enough
  to be hard to tell apart, no function of one name can prevent it, so
  `hestan doctor` reports any pair within eight degrees, names both, and
  `Asset::hue(n)` pins one of the two
- **the ui colours by group, by origin, or not at all**, in the url with every
  other view state. one meaning at a time, because two hue meanings on one
  screen is noise. an asset descending from several sources gets a **split
  swatch, one stripe per source in name order and never a blend**: averaging
  two hues makes a third that stands for a source nobody has. the catalog
  sections by the declared group and gains a `group` filter; the graph folds
  by it too, rewiring exactly the edges the prefix fold rewired. saturation and
  lightness are pinned per theme and checked across all 360 angles against both
  grounds of both themes, so nothing generated is illegible and nothing can be
  read as a status grey
- `GET /api/assets` gained `group`, `group_hue` and `provenance` (a name and a
  hue per origin) on every asset. `hestan assets` gained group and origin
  columns and a `--group` filter, and prints `-` for both against a bare run
  log, which has no registry to resolve either from and is not the same claim
  as "no source". `doctor` gained `groups`, which finds a declared group at
  odds with the name it is on (a rename somebody started and did not finish),
  and `colours`, which finds the hue collisions above; both are notes and
  neither changes an exit code

a policy on an asset says when hestan rebuilds it, evaluated per key, and one
process acts on it.

**breaking, and mostly in what a field means.** `.auto()` is unchanged in what it
is called and in what it does on every graph that had a probe reaching it: it is
now `AutoPolicy::when_stale()`, the same rule with the others beside it. one case
does change. an auto asset whose sources have all been observed, sitting in a
part of the graph no probe reaches, is now rebuilt by the policy pass within a
minute of going stale, where before nothing looked at it at all. that is the rule
doing what it always said; it is written here because a deployment relying on the
old silence would see builds it did not see before.

`"auto"` in `GET /api/assets` now means "hestan rebuilds this one itself", which
is any policy rather than that one rule, and a graph declaring a policy on a
source fails the build saying "an automation policy on a source" where it used
to say "auto on a source".

- **four rules and no more**, each of them something people were writing sensors
  for: `when_stale` (today's `auto`), `when_missing` (the fresh deployment and
  the newly declared asset, which staleness alone never gets to say anything
  about), `after_cron` ("nightly, but do not rebuild what has not moved", read
  in utc until `.tz()` says otherwise), and `and_upstream_ready` on any of them,
  which holds the build until everything it reads is there so a daily rollup
  waits for its last hour rather than recording a partial day
- **evaluated one key at a time.** a partitioned asset gets a verdict per key,
  so a pass builds the keys that qualify and leaves the ones that do not, newest
  first and capped by the same `build_limit` a build that names no keys respects
- **readiness is the mapping's answer, asked again.** what a key reads and what
  of it a dep will never hold is what phase 37 already resolves for staleness,
  so a rule that waits for upstream cannot disagree with the verdict that said
  the key was stale in the first place
- **a policy under a source nothing has observed does not fire**, which is what
  `.auto()` without a probe upstream has always done: there is no fingerprint to
  compare against, so a build would consume null and be owed again forever.
  `doctor` reports the ones whose source has no probe at all, beside a window
  that promises keys its dep will never hold
- **one process decides.** the pass runs beside the freshness checker on the
  deciding role, every minute. it checks for an active assets run before
  planning rather than tripping `asset build already running` every minute, and
  it launches everything it wants as one plan and one run. a rule that cannot be
  satisfied writes nothing at all: no run, no event, however many passes go by
- **every launch is attributable**: a `policy_launched` event per asset naming
  the rule that fired and the keys it asked for, and the run tagged `policy` with
  the rule (and `asset` when it is the only one). the probe path evaluates the
  same policies over its own descendants, so a probe and the pass cannot
  disagree about a key
- `GET /api/assets` reports each asset's `policy` and, when it wants a build it
  cannot have yet, what it is waiting for; `/partitions` says it per key. the
  assets table tags it `auto` or `waiting`, the asset page writes the sentence
  ("when stale, once upstream is ready · 2026-08-14 waiting for
  `hours[2026-08-14T23]`"), and the partition grid says it in a cell's tooltip

## 0.1.0-beta.3

rate limits as declared token buckets, an asset's value stored through its io manager,
resources scoped to one run, fan-out that nests, and partition mappings so a daily asset
can roll up hourly data. the prose lost 2,286 em dashes.

**breaking, none of them signature changes.** an asset's value column now holds what the
io manager returned rather than the value itself, and `Store::materialization` hands that
back. `Trigger` gained `Replay` in beta.2 and nothing new joins it here. two jobs that used
to build now do not: an op named what an instance of a mapped op of the same job is named,
and a fan-out label containing a bracket. both were silently misread before.

a partition may read a mapping of its dep's keys: a daily rollup of hourly
data, yesterday's key, every key at once.

**breaking**: no signature moved, and identity (every dep declared with
`from`, which is every dep that existed before this) resolves, builds and
fingerprints exactly as it did. what changes is that two graphs which used to
be refused now build (an unpartitioned asset reading every key of a
partitioned one, declared `PartitionMapping::all`), and that a build naming a
partition whose window its dep cannot fill is now `Error::Graph` naming the
missing upstream key rather than a rollup of the hours that happened to be
there.

- **the mapping is on the edge**, since which keys are read is a property of
  neither asset: `Asset::reads(&dep, PartitionMapping::covering())` beside the
  `from` it generalizes. four shapes and no more: `identity`, the default and
  the same key; `covering`, the dep keys inside this one, so a daily key reads
  its 24 hours; `offset(n)`, the key n steps along the dep's order; and `all`,
  every key the dep has, which is the only one an unpartitioned asset can mean
- **a pairing none of them could resolve fails the build**, where the asset
  graph is validated and with both partitionings in the message: a window over
  a static key set, which spans no time to cover; an hourly asset trying to
  cover a daily one, since an hour sits inside a day rather than the other way
  round; an offset along a static set, whose declaration order is not an order
  to step along; any mapping but identity on a dep with no keys at all
- **staleness follows the mapping, which is the whole point of having one.** a
  rollup that reported fresh because it only checked its own key would be worse
  than no mapping, so a build records the fingerprint of every upstream key it
  consumed (one object keyed by partition where it read a set, the bare string
  identity has always written where it read one) and a daily key is stale when
  any hour it covers has moved. no schema change: `inputs` is json and both
  shapes live in the column it already had
- **the reason names the hour rather than the day.** `{dep, partition, had,
  now}` carries the key of the dep it is about, and the grid's tooltip and the
  asset page's causal chain both say `hours[2026-01-05T07]` where they could
  only say `hours` before. `partition` is null on every identity read, which is
  every reason recorded before this
- **building follows it too**: a plan walks from the sinks up resolving each
  consumer's mapping, so materializing a daily key materializes the hours under
  it that are missing or stale, and building an aggregation pulls in the keys
  it has never read. one thing a window cannot do is promise a range its dep
  does not hold (a day whose hourly asset starts at 06:00) and naming such a
  key at a build or covering it with a backfill range says which hour is
  missing, while a build that names no keys leaves it out of its target set
  rather than refusing everything else with it
- **the build limit counts keys of the target, not instances of the run.**
  under identity those were nearly the same number; under a window one key is
  25 op runs, so a default build of a daily rollup is up to 775 and a backfill
  chunk is the same multiple. `Hestan::max_instances` is the ceiling that
  actually holds, and `docs/assets.md` says so where the limit is chosen
- `GET /api/assets` reports each asset's `mappings` (the deps read at anything
  but the same key, and how) and `/partitions` reports per key what it `reads`
  of them and its own `reasons`. the drawer writes the mapping beside the dep,
  the cli's asset table writes it in the deps column, and hovering a grid cell
  says which upstream keys that key reads and which of them left it stale

a fan-out may expand inside a fan-out, under a ceiling on what one run may
expand to.

**breaking**: no signature moved, but two jobs that used to build now do not,
and one that used to be refused now builds. an op named what an instance of a
mapped op of the same job is named (`probe[0]` beside a mapped `probe`) is
`Error::Graph` at build rather than a row misread as an instance for the life
of the deployment; and a run whose fan-outs expand into more than 1000 op runs
fails at the expansion rather than writing them, which `Hestan::max_instances`
raises. `.over` naming a mapped op is no longer refused.

- **a mapped op may map over a mapped op.** each of the outer op's instances
  produces an array of its own, and the inner op runs once per element of each:
  a region list, a site list per region, a probe per site, written as three ops
  and no plumbing. what it was before was a build error (`fan-out does not
  nest`, refused because the second level had no honest name for its rows)
  and the reason it was refused is what this phase went and fixed
- **the name is the fix.** an instance carries one `[label]` per level of
  fan-out it sits inside, outermost first: `sites[1]`, then `probe[1][0]`. it
  round-trips because a label can hold neither bracket (an expansion that
  would write one is refused, naming the key) so the parse is "the op is
  everything before the first `[`, the coordinates are the groups after it",
  at any depth. `op_runs` is keyed `(run_id, op)` and every instance name is
  distinct within a run by construction, since sibling labels are already
  checked for repeats and every level's prefix differs
- **it cannot collide with an op somebody named by hand**, because that is now
  refused at build. an op whose name splits at the first `[` onto a mapped op
  of the same job is `Error::Graph`, which is exactly the case a previous
  phase found and could only work around: `keys[extra]` beside a mapped `keys`
  read as an instance of it, in op stats, in the ui, in a resume. `keys[extra]`
  beside a *non-mapped* `keys` still builds and is still an op name and nothing
  else, since what a fan-out expands over is not a fan-out
- **collection is nested, not flattened.** an op downstream of the inner
  fan-out gets one array per outer element (`[["north-a", "north-b"],
  ["south-a"]]`) because flattening loses which outer element a value came
  from, and that is the only reason to nest rather than build a `Vec` in the
  outer op. an outer element yielding `[]` contributes an empty array in its
  place rather than a gap, and does not stall the run
- **failure and everything else work at every level.** one inner instance
  failing fails the fan-out it belongs to and, through it, the mapped op:
  downstream is skipped and there is no partial array, while the instances
  under the outer elements beside it run to the end, exactly as an op's
  siblings do. pools, rates, limits, retries, trigger rules, timeouts and
  cancellation apply per instance with no special cases at either depth
- **a ceiling, because multiplication is the hazard.** `Hestan::max_instances`
  is the most op runs one run may expand its fan-outs into, across every level;
  the default is 1000. past it the run fails at the expansion, naming the op,
  how many instances it was about to make and how many elements it was about
  to make them from, **before** a row of it is written, because a runaway
  found by counting op rows in the ui is a runaway that already happened. the
  budget is the run's rather than any one op's, since what a nesting multiplies
  is the run. a thousand is thirty times what a partitioned build launches by
  default and far more than a hand-written fan-out; forty elements each
  yielding forty is 1600, which is the case this exists for
- **the docs say flattening is usually better**, on `Op::mapped`, in
  `concepts.md` and beside the ceiling: nesting is two lines that each look
  small and every instance is a row, a gantt span and a value held until the
  fan-out collects
- **everything that reads an instance reads a nested one.** a resume reassembles
  the value in the shape it was collected in and reuses it, or re-expands the
  inner fan-out over an outer one it only has the value of; a replay re-runs
  over the arrays the run expanded over rather than today's; an io manager
  writes `probe[1][0].json` under the run's directory and phase 29's
  containment check holds for it unchanged; op stats and the metadata trend
  roll every level up into the op, which is the only history a mapped op has.
  the run page groups the instances as a **tree** (a block per outer element)
  so which outer element a failure sits under is on the page and not only in
  the name, and the gantt lays an inner instance out after its own outer
  instance rather than after all of them

an asset's value goes through the io manager, and a resource can belong to one run.

**breaking**: no signature moved, but `asset_materializations.value` (which
`Store::materialization` and `Store::materializations` hand back) now holds what
the io manager returned rather than always holding the value. under the default
`Inline` that is the value, unchanged; under a configured manager it is a handle,
and code reading the column directly has to resolve it. retention also keeps a run
an asset's current value is inside, which is new.

- **an asset's value is stored where every other output is stored.** `asset_materializations.value` held the value as json in the row whatever manager a deployment had configured, so an asset of rows under a file manager was stored twice (once as a handle in `op_runs.output`, once inline in the materialization) and the inline copy was the one a memoized build read. the materialization now records what `put` returned, which under a file manager is the same handle the op run already holds: one stored thing named twice. `Asset::io(name)` and `MultiAsset::io(name)` select a manager per asset, which is what makes `ParquetIo` usable for one: the process default has to be right for every op in the deployment, and a check op returning an object is not a table
- **seeding resolves it back through the manager that wrote it**, so a build that memoizes an upstream asset hands the op downstream the rows out of the parquet file rather than json out of the run log. the fingerprint is of the value the op returned, so where the value ended up cannot make anything stale, asserted by building the same asset under both and comparing
- **no migration, and none was needed.** a row written before this holds the value itself, which is not one of any manager's handles, so `get`'s pass-through rule (a documented requirement of the trait since it landed) hands it straight back. nothing has to tell an old row from a new one and no column says which kind a row is. the residual ambiguity is the one `op_runs.output` has always carried: a value that happens to look like a live manager's handle is read as one. a v8-era row is planted by hand and seeds a build in the suite, so "the old database still works" is a case rather than a claim
- a **multi-asset** keeps each produced asset's slice inline: one op returns one object, the manager has one handle for the whole of it, and no part of it has a handle of its own. inventing one would mean storing something the op never returned
- **retention now keeps a run an asset's current value is inside**: rows and files together, until something rebuilds the asset, at which point it is history like any other run and the next sweep takes it. pruning it would leave the row pointing at nothing and the next build would either fail on a hole or silently redo work somebody paid for. the cost is stated rather than discovered: a policy no longer strictly bounds what is here, and an asset built a year ago and never rebuilt keeps its run for as long as it stays current. `hestan doctor` counts them under `values`, `Retention`'s own docs say it, and `storage.md` and `io-managers.md` say it where a number is being chosen. nothing is held back under `Inline`, whose values are in the row itself: a deployment that never configured a manager prunes exactly as it did
- **a resource can belong to one run**: `Hestan::run_resource(name, ..)` builds a value when a run starts and drops it when the run ends, for a scratch directory, a per-tenant client or a token that belongs to one execution. `ctx.resource::<T>(name)` and `Op::requires([name])` are the same call for both scopes (how long a value lives is the deployment's decision rather than the op's) and `ResourceCtx::run_id` says which run a value is being built for. a name declared in both scopes is `Error::Resource` at build, since `ctx.resource("x")` must never mean two things
- **it is dropped when the run ends by every route**: success, failure, cancellation, and the process giving up on recording an outcome at all. the value is held by the task driving the run and by nothing else, so what drops it is that task ending: a `Drop` rather than a line at the end of the run loop, which is what makes the last route work, since that one has no terminal row to hang a teardown off. all four are asserted, each by the run id that came back
- **dropping happens on the blocking pool**, for the reason phase 30 moved io manager work there: a `Drop` that removes a directory or closes a socket blocks, and the task driving a run is the one thread that must not. the case proves it by thread id rather than by clock (the drop lands on a thread the test is not on) and a runtime already shutting down runs nothing new and drops what it was handed instead. the one limit is `Arc`'s: an op that kept its handle past the end of the run holds the value up until it lets go, and that is written down rather than papered over
- **what a run-scoped resource costs is on the constructor that declares one.** it is built per run, so a connection pool built this way is a pool per run (a hundred pools on a busy afternoon) which is almost always a mistake. the docs steer at every point somebody would choose: `Hestan::run_resource`, `docs/resources.md`'s table of the two scopes, and `concepts.md`. a constructor that fails fails the run before any op of it runs, with `resource {name}: {reason}` on the run row and every op recorded `skipped` saying so: nothing else could have been true of an op that needed it
- `GET /api/resources` reports each resource's `scope` beside its name and type, a run-scoped one as *declared* rather than as built: nothing of it exists between runs, and its type is the one its constructor was written to return

rates: the limit an external system actually publishes (n calls per period,
declared once and taken a token at a time) beside the pools that cap how many
run at once.

- **a declared rate, and an op that takes a token from it.** `Hestan::rate("api", 5, Duration::from_secs(1))` is five calls a second shared by every job in the process, and `Op::rate("api")` takes one before the body runs. the same bargain a pool makes on the same surface: a name declared twice or named by an op and never declared is `Error::Graph` at build, and an op may hold a pool permit and a rate token at once. a pool caps how many calls are in flight, which is a rate only if you know how long each one takes: `max_parallel(3)` is how that limit usually gets approximated, and the failure mode is a 429 at 06:00
- **a token bucket, not a fixed window.** a window admits its whole allowance either side of a boundary: five calls at 0.99s and five at 1.01s are two legal windows and ten calls in fifty milliseconds, and the api sees ten. tokens accrue continuously here (`n` over `per`, up to `n` at once, one more every `per / n`) so there is no boundary to be either side of. the burst is deliberate rather than a side effect: an api publishing "5 a second" generally tolerates five and then a second of quiet, and metering them out from the start would be slower than it asked for. the case that pins this puts a batch at 0.99s and a batch at 1.01s and asserts that no fifty-millisecond span holds more than a second's worth; swapped for a fixed window it reports ten
- **a token is spent, not returned.** that is the whole difference from a pool, and it is why there is nothing here to release and nothing held until work stops: the op has had its call, and the bucket refills on its own clock whatever it does next. one per attempt, because a retry is another call, and one per fan-out instance for the same reason
- **waiting is asynchronous, ordered and cancellable.** an op that finds the bucket empty parks without holding a runtime thread, does not spend its `Op::timeout` doing it, and logs `waiting for a {name} token` beside the line a pool already writes: an op sitting in `running` with nothing happening is what makes people stop believing a scheduler. the order is first-come-first-served by construction: a token is reserved when the op asks rather than handed out when it arrives, so nothing can overtake and a long queue cannot starve its head
- **a canceled run's waiting op takes no token with it.** the reservation goes to the op behind it in the queue, which is woken to find it has moved up rather than sleeping out the one it was given, and the whole queue behind it moves up a place: a token spent on an op that is already dying is a call nobody makes and a call somebody else should have been making. the wake is asserted on a paused clock, so what it proves is the instant the waiter went at rather than how long the test took on a busy machine
- **the bucket lives in one process, and that is said where somebody declaring a rate will read it**: on `Hestan::rate`, on `Op::rate`, in the concepts page, in `hestan doctor` when the role is a worker, and at length in `docs/scaling.md`. two workers each honouring five a second send ten, and the deployment shape that makes hestan scale is exactly the one that breaks the guarantee. `tests/queue.rs` asserts it with two real worker processes rather than describing it: each spaces its own calls a period apart, and the two together put two calls inside one period. what to do about it is arithmetic (divide the limit by the number of workers) and the reasons a store-backed bucket is not here (a write transaction per call against the run log's own database, waiting that becomes polling and loses the ordering with it, a token that dies with its host and would need a lease to come back, and postgres-only besides) are written down beside it
- `GET /api/rates` reports every declared rate and how many ops are queued for a token here; `GET /api/jobs/{name}` reports each op's `rate` and the job's `rates`; the runs page grows a "waiting for a token" section above the queue when anything is behind one, the op inspector and the job's policy line show what was declared, `hestan explain` lists them beside the pools, and `hestan doctor` says both halves: what this process declares, and what is piling up behind one over `--server`

## 0.1.0-beta.2

replay: re-run a past run's ops on the inputs they actually had, for reproducing a
failure on the value that caused it. the original run is never written to, and a run
whose values retention has taken is refused by name rather than replayed on a hole.
schema v19.

**breaking**: `Trigger` gains a `Replay` variant. the enum is not `#[non_exhaustive]`,
so an exhaustive `match` on it needs one more arm.

- **a run can be replayed: its ops re-run on the inputs it gave them.** an op failed in production two months ago and you have a fix, and the question worth answering is whether the fix works on the input that broke it rather than on one reconstructed by hand. `runner.replay(&run)` launches a new run of the ops that run recorded as failed, with every dep of them seeded from what it recorded, so the op reads byte for byte what it read then; `replay_ops` names the ops instead. `POST /api/runs/{id}/replay`, `hestan replay <run> [--op ..]`, and a control on the run page beside resume's
- **it is the opposite operation to a resume, and the run log can say which happened.** a resume re-runs what did *not* succeed together with everything downstream of it; a replay re-runs what *did*, exactly the ops named and nothing below them. so a replay is its own trigger and its own column (`replay_of` beside `resumed_from`, schema v19) rather than a second meaning for the one that was there. one column meaning either would have left the history unable to answer which of two opposite things somebody did, and would have let the resume planner walk into a replay's lineage as if it were a continuation
- **the original run is not written to.** not a status, not an event, not a materialization, not a byte of what its io manager wrote: it is history, and a replay is a new run. the test asserts that by holding the whole of the run (its row, its op runs and every event of it) as text before and after, so a column added next year is covered by it without anybody remembering to add it
- **a run whose inputs are gone refuses, naming what is missing.** retention takes an io manager's files with the run, so an old run's values go when its rows do. every seed is read back through its manager while the replay is being planned, before anything launches, and a value that cannot be produced is `cannot replay load of run ...: its input extract cannot be read back: ...` instead of a run. a replay that ran with a hole in it (or with an input quietly defaulted to null) would be a reproduction that reproduces nothing, which is worse than no answer. the ui shows that sentence where the button would be, from the same check, so nobody clicks to find out
- **what a replay does not reproduce is written down rather than left to be discovered**, because a reader who believes it is deterministic will trust a result that misleads them. the code is today's, which is the point, since you are testing a fix, but it means this is not a bit-for-bit re-execution. resources are rebuilt, so an op that read something through a connection or a client read a world that has moved on. the clock, randomness and anything the op fetches itself are not captured and cannot be: an op that calls an api gets today's answer. and retention is the horizon, so a pruned run is an unreplayable one; `docs/replay.md` says all four and how to keep a run longer when replay is why you want it, `docs/storage.md` says the last one where a policy is chosen
- a run that was itself a subset (a resume, a replay, an asset build) did not produce everything its ops read, and what it *was handed* is on its plan and nowhere else. that is what a replay of one seeds from, so replaying an op of a resumed run reads what that op read, whichever run originally produced it. an op the run never ran is refused rather than launched on nothing: it has no inputs of its own to reproduce, and running it anyway would be a partial launch wearing a replay's name

## 0.1.0-beta.1

feature-complete and hardened: every finding from the external review is closed, and the
storage, web interface, developer-experience and integration surfaces are done. still 0.x,
so a breaking change is still possible without a deprecation cycle: pin an exact version.
`IoManager` is the one place a further break is likely, since an asset's value does not yet
go through it.

**breaking since alpha.2**: `Runner::new` and `Runner::with_failure_hooks` return
`Result<Runner, Error>`; `IoManager` gains a required `drop_run`.

- **a run never reports an outcome the store did not take.** the executor let every store write go through one function that logged a warning and carried on, and forty of its call sites were in the run loop, twenty-one of them the run's own record of itself. a dropped `op_finished` left the op row `running` while the run carried on and finished `success`: hestan reporting a result the run log does not hold, which the next resume then builds on and the asset catalog then trusts. the fix is the twenty-one call sites; the point of the phase is that the twenty-second cannot be written
- an event write returns a `BestEffort` and `note` takes one of those and nothing else, so `note(store.op_finished(..))` is a **type error**. only the store module can make one, and only in a signature that says so, which means **a store write added next year is critical by default**, because it returns `Result` like everything else and `note` will not take it. four writes are declared best-effort and each is a decision rather than a leftover: the event log, captured op output, an isolated op's pid (the parent holds the handle it stops the child with either way), and the audit line beside a cancel signal. the property is put to rustc rather than described: a test builds two source files out of the signatures the store actually declares (`note`'s parameter and each write's return type, both scraped, so a signature that moves moves the test with it) and asserts that an event compiles, that an op's terminal row does not, that it was refused as a *type* error rather than a typo, and that the same write not noted compiles fine
- **a critical write that fails is retried**: four attempts, capped exponential with full jitter on the same `backoff` an op's retries use, under a second in the worst case. what makes that safe is which errors are retried rather than which writes are made: `op_finished` appends materializations and their events beside the row it updates, and repeating it after a *partial* apply would record a build twice. there is no partial apply to repeat: sqlite's commit is atomic and rusqlite rolls back a transaction it could not commit, and postgres aborts the transaction it reports a serialization failure or a deadlock for. every error hestan retries is one a live backend raised, having already undone the work
- the exception is the one that matters and it is **not** retried: a connection that died may have carried a commit whose acknowledgement nobody received. that is the only failure whose outcome hestan cannot know and the only retry that could double-apply anything, so `is_closed` and a postgres error with no sqlstate at all end the attempts. this deliberately declines the "retry when you cannot tell" rule, because the cost is not symmetric here: a needless retry is fifty milliseconds and a double-applied one is a materialization that never happened. every write on the path is also idempotent as written: absolute values, a coalesced start time, an upsert, an insert that ignores a conflict, a guarded mark, and an attempt count the caller computes rather than sql incrementing
- **a run whose write will not land stops without recording an outcome.** it does not report success, and it does not report failure either: the work may well have finished and the row that would say so is the write that failed, so reporting either way is picking one of two guesses. what it leaves behind is stated rather than dressed up: the run sits `running`, claimed, with a lease nobody renews, until it lapses and `Reclaim` settles it as `Fail` or `Requeue`. that is worse than a clean failure and far better than a false success, which is a lie that outlives the incident. the lease is left to lapse *on purpose*: a process that kept renewing a claim it had given up on would hold that run out of every reclaimer's reach for as long as it lived
- returning from the run's task is also what stops the work: the `JoinSet` goes with it, so in-flight ops are aborted and an isolated op's child is killed by the `kill_on_drop` that has always been its backstop. an op whose *start* cannot be written never has its body called, since there is nothing to be gained by running work nothing is keeping the score of. and **a process whose store is refusing writes stops claiming**, because claiming a run is promising to record what it does; the lease loop's renewal is both the probe and the recovery, so the queue moves again fifteen seconds after the database does
- **the control plane says so.** `GET /api/health` is `ok: false` while the store is refusing writes, with what it has lost since the process started (best-effort writes dropped, run outcomes unrecorded) and the runs it gave up on, which are no longer counted as held. `hestan doctor` gains a `writes` check that asks a database directly whether it would take a write lock and give it back, writing nothing; over `--server` it reads the deployment's own account of its store, which is the half only the running process knows. `docs/concepts.md` gains what hestan promises about writes and what it stops promising, `storage.md` what a refusal means for each backend (including that a postgres store is one connection with no reconnect, so a dropped one is not something a retry rides out) and `scaling.md` how a lease is used to say something a process cannot write down
- every fix is held to reverting, one mechanism at a time. sharpening the fault injector was part of that: with *every* write failing, a run that carried on regardless would fail its next write too and leave exactly the rows a run that stopped leaves, and two cases passed either way. it now fails one named write and no other, so a run that carried on would record its outcome perfectly well, and the cases can tell the two apart. phase 31's multi-asset case changes shape with it: it still asserts that a refused materialization takes the op's terminal row back with it, and now also that the run is left `running` and claimed rather than going on to report a status nothing holds

- **a pool permit outlasts the work it admitted.** a pool is a promise to something outside hestan (at most n of these at once) and the permit was on the op task's stack, so a cancelled run took the stack and gave the slot back while the work was still calling the api. the narrow case is the one that bites and is worth stating: an op blocking inside its own poll already held the permit, because the task is not dropped until that poll returns, and its `JoinHandle` resolves at the same instant, so tying the permit to the handle would have moved nothing. what leaked was work the body started and the body's future no longer owns, which is `spawn_blocking`: the pattern hestan itself documents for anything that cannot be dropped. measured on a pool of one, the next op started 200µs after the cancel while the first was still in the call
- so the permit rides the `OpCtx` rather than the task. blocking work has to keep its ctx to see a cancellation at all (that is what `is_cancelled` is for) and keeping it is now also what keeps the pool's count true: the slot goes back when the last holder of that ctx lets go, which is the last moment hestan can observe the work at all. an op that never stops holds its permit until the process does, because the work genuinely has not stopped. work that keeps nothing of hestan's is work hestan cannot see the end of, and that limit is documented rather than papered over. nothing got slower: the abort lands where it landed, an op that yields is still dropped at its next await, and an isolated op drops its ctx once the child has been watched to die so a retry backoff still never sits on the slot it backed off from
- **an asset is recorded as built when the op that built it succeeded.** the materialization row went in from inside the op body, before the output reached the io manager and before anything recorded that the op finished, so the failure that mattered most was the one it could not survive: a manager that refuses the output failed the op and left a row saying the asset was current. the next build read that row, found nothing stale, and skipped. the asset was missing and hestan was sure it was fine
- the body now stages what only the body can know (the fingerprint, the deps it consumed, the value, what the build reported) and the executor writes it **in the transaction that records the op finishing**, the same rule phases 21 and 24 applied to notifications and events. an op that fails, panics, times out, is cancelled or cannot have its output stored records no materialization at all, and the retry or the next build rebuilds it; a multi-asset op writes all of its materializations or none of them, because they are one fact about one op run. `Meta` is unchanged: the map the body staged still goes on the materialization and on the op run, and the manager's own counts still go only on the op run. `record_materialization` is now what it always meant: a row about something observed rather than about an op, which is a probe finding new bytes on a source
- both fixes are asserted by construction rather than described. an op whose blocking call outlives its cancellation holds its slot (a second op on a pool of one does not start while the call is in flight, and gets in the moment it returns) while an op that yields gives its slot back inside two seconds. a manager that refuses an asset's output leaves no materialization and the asset still reads stale, and a trigger that refuses one insert of a multi-asset takes the other materialization *and the op run's terminal row* back with it, which is the only way to show the three are one write rather than three in a row
- `docs/assets.md` replaces "at-least-once materialization" with when a build is recorded and what each way of not finishing leaves behind; `concepts.md` says what a permit means for blocking work, in both the pools section and the cancellation one; `events.md` notes that a build's materialization event now joins the op's terminal write

- **BREAKING: `IoManager` gains a required `drop_run(run_id, job)`**, and retention calls it as it prunes a run. a custom manager gains one method and the compiler names the file it belongs in; the bundled ones remove `{dir}/{run_id}`, and `Inline` returns `Ok(())` because an inline output *is* the row being deleted. it is required rather than defaulted on purpose: a no-op default keeps every manager anybody has already written compiling and goes on silently leaking every file each of them ever wrote, and a manager of your own is by definition the one storing things hestan cannot see, which makes it exactly the one whose silence costs the most. 0.x alpha, one line to satisfy, and the decision belongs to whoever wrote the manager
- **retention collects what an io manager wrote.** the sweep used to take the run rows and leave the files they pointed at, forever; phase 28 confirmed that and pinned it in a test rather than fixing it, and the run rows were the only record that those runs existed, so once they were gone nothing was left that even knew which files to look for. now every registered manager is asked to drop each doomed run, and **the files go before the rows**: rows first and a crash in between loses the ids and makes the leak permanent, while files first leaves rows pointing at outputs that are gone, for runs already past retention, which the next sweep deletes anyway. dropping is idempotent by contract, so a directory that was already taken is `Ok(())` rather than an error every hour forever
- a manager that **cannot** drop something is logged and the sweep carries on to the rows and to the next job: a file left behind is one run's worth of waste that whoever owns the directory can still find, while a sweep that stopped there would grow the database forever behind one unwritable directory. the directory it removes is computed from a run id, so it goes through the same containment check a written file's path does before anything is removed: an `rm -rf` of a path nothing verified would be a worse bug than the leak it was fixing
- the rows are deleted **by id** rather than by policy a second time, so the runs whose outputs were dropped are exactly the runs deleted and a run that comes due in the moment between the two keeps both halves for the next pass instead of losing one of them. `prune_job_runs` splits into `doomed_runs` and `delete_runs`, which binds its ids in batches, since both backends cap how many values one statement may bind and the first sweep of a year of history is not a small number of runs
- **an io manager's work no longer happens on the task driving the run.** `put` and `get` are `std::fs` in both bundled managers and a network call in most of the ones anybody would write, and they were made on that task: a megabyte of json was a stalled tokio worker, a parquet file was one for as long as the write took, and everything the run had left to do (dispatching what is ready, collecting what finished) waited behind it. on a single-threaded runtime so did every op of every other run in the process. every call on a run's path now goes to the blocking pool: both `put`s in the executor, the `get` behind an op's inputs, the `get` that assembles a fan-out's collected array, and an op subprocess's own `put` and the `get` for each input it was handed
- the trait stayed synchronous and the call moved. the registry holds `Arc<dyn IoManager>` and `async fn` in a trait is not dyn-compatible without boxing every return: a large change to the surface every custom manager implements, for a problem that lives at the call site, and an async trait would have had nothing to say to `Runner::resume_plan`, a synchronous api that resolves an earlier run's outputs and could only have called `block_on`, which panics on a current-thread runtime. resume planning is now the one manager call made on its caller's own thread, says so in a comment, and is not a run's task: nothing is executing while a resume is being planned. a manager that panics now fails the op it was called for rather than taking the run's task with it, and the retention sweeper's own loop moved off the runtime for the same reason its managers did
- the tests measure no time at all: a manager blocks inside `put` until an op running beside it says it has run, and inside `get` until another one does (a deadlock on the run's own task, and only a slow manager off it) and each half fails with the call put back where it was. `retention_takes_the_run_and_leaves_the_file_behind` asserts the opposite now rather than being deleted, `FileIo` is held to the same thing beside it, and the ordering is proved rather than commented: a manager that records what it was asked about reports the run's row still in the log when it was asked
- `docs/io-managers.md` gains what a custom manager owes: the third rule, the no-default and why, that the work is off the runtime, and a section on what retention takes including the two things to know before pointing a manager at a directory (only what a policy deletes is collected, and the process that decides is the process that deletes). `docs/storage.md` says where the files go in the sweep and why that order, and `docs/connecting.md` no longer says the managers clean up nothing

- **BREAKING: `Runner::new` and `Runner::with_failure_hooks` return `Result<Runner, Error>`.** they used to keep whichever job of a name was registered last and log a warning, which made the dag that ran under that name depend on the order the jobs were handed over. every call site gains a `?` or an `.unwrap()` and the compiler names each one; nothing changes behaviour underneath anybody, which is what makes this the cheap kind of break. the alternative was leaving the convenience constructor and the builder above it disagreeing about whether a duplicate is fatal, and two answers to one question is how the two of them drift. this is 0.x alpha and the api changes without deprecation, as the readme says
- the error is `Error::DuplicateJob`, the one `Hestan` already raised for the same mistake, rather than a second error meaning the same thing. `with_pools`, `with_io` and `with_resources` were fallible already and refuse through the same registration, on the precedent of the pool that is declared twice
- **a schedule declared twice for one job is refused** as well. the run log keys a schedule on `(job, expr)`, so the second declaration was never a second schedule: it was that row carrying whichever timezone and params came last, with both entries firing and the ui showing one. two *expressions* on one job are untouched: that is a job with two schedules, and both of them fire. duplicate asset names, multi-assets, checks, sensors and pools were already refused; the job map was the last name in a definition that could be claimed twice and only warned about, and `docs/concepts.md` now has the whole list in one table
- **a response body has a ceiling, and a failed one is read only as far as it is printed.** the error path buffered the whole of a failed response to print two hundred characters of it, so a server answering 500 with a gigabyte of html cost this process a gigabyte to read two lines. it stops at 4 KiB now. the success path had no ceiling at all: `HttpSource::max_body(bytes)` is one, **64 MiB by default**, generous on purpose, because a paged json api answering with several megabytes is the ordinary case this exists to serve and the limit is here for the gigabyte
- the ceiling is applied **while the body arrives**, not to a body already read: `Response::bytes` has assembled the whole thing by the time it returns, so a length check on what it hands back is a check on something already held. a `content-length` the server sent refuses the body before a byte of it is read; without one (chunked, streaming) or with one that does not match what follows it, the read itself is what stops, so neither the header's absence nor its honesty changes the outcome. what is held past the limit is one socket read
- a body past the ceiling is **not truncated and parsed**. what hit the limit is not a smaller valid document, and a json error there would send the reader looking at the api's formatting instead of at its size; the message names the url and the limit, and the failure is fatal, since the same request fetches the same body. the tests are a server misbehaving on purpose rather than a mock that proves nothing: a content-length promising a gigabyte with the connection held open, an endless chunked body with no length at all, and a content-length that understates what follows it
- **an io manager writes inside the directory it was given.** `FileIo` and `ParquetIo` joined the run id and the op name onto their directory and trusted both, so `..`, an absolute name, a name that is only dots and an empty one each put the file somewhere the manager was never pointed at. what is refused is a name that leaves the directory, **not** a name with a separator in it: asset names contain `/` on purpose (`sales/orders` is a directory and a file, and the catalog groups on that same prefix) and one instance's `fetch[0]` keeps its brackets
- validated rather than encoded, because an encoding would have to change where every file lands: it would flatten the grouping a slashed name gets today and orphan everything a running deployment has already written. the failure arrives as any other failed `put` does (`could not persist the output: op "../escape" does not name a file under the io directory`) and both managers call the same function, since a key that is a file for one of them and an error for the other is the same drift in another place. a `get` still resolves the path in the handle rather than recomputing one, so an output written before the directory moved still reads back
- `docs/http-sources.md` gains how much of a response is held and what the ceiling costs; `docs/io-managers.md` gains what a name may be, including the two things a name check cannot do; `docs/concepts.md` gains where every duplicate name is refused and what each refusal says

- **an io manager that writes parquet**, behind `--features parquet`: `ParquetIo::new(dir)` against the `IoManager` trait that was already there: no new trait and no new concept. an op that returns rows gets them written as one parquet file per op, `op_runs.output` keeps a handle, and the op downstream reads the same rows back. arrow and parquet are optional and pinned to one major together, since they are one project released together; snappy is the only codec compiled in, and a default build has neither crate in `cargo tree`
- the round trip is the whole of the value, so it is asserted value for value: a column of each family json has (`int64`, `float64`, `utf8`, `bool`, a list, a struct, and a column that is null the whole way down) with nulls inside the rows and not only around them, and the file's own schema read back through a plain parquet reader so a column that quietly became a string would fail. what cannot survive is written down rather than discovered: a column mixing `1` and `1.5` is one `float64` column, and a key missing from one row reads back as an explicit null, because a parquet column has one type and a table has the same columns in every row
- an output that is **not** a table is an error naming what it got, not a quiet fallback to json: an op whose output went somewhere it did not ask for is a value nobody finds again. `null` passes through untouched, since an op that produced nothing has no table to write
- **a handle can say how much it stored**, and `rows`/`bytes` on one become `Meta::Count` and `Meta::Bytes` on the op run without the op asking: the manager knows both and the op does not, because the op returned a value rather than a file. it is a rule about handles rather than about parquet, so a manager of your own gets it by putting either key in what `put` returns, and anything the op staged under the same name wins
- **nothing about retention changed, and that is deliberate.** a pruned run leaves its parquet behind exactly as it leaves `FileIo`'s json: retention prunes run rows, not files. matching the existing behaviour beat growing a second answer for one manager, and there is a test that says so rather than a comment that hopes so, and the io managers' page says it where somebody choosing a directory reads it. **superseded above**: retention collects what either manager wrote, and the test that pinned the old behaviour asserts the opposite
- **a dbt project's models as assets**, behind `--features dbt` and with no dependency of its own, since a manifest is json and `serde_json` was already here. `Dbt::from_manifest(path)` reads the dag dbt compiled and produces one asset per model plus a source asset per source a model reads, wired from the manifest's own `depends_on`, so the lineage in hestan's graph is dbt's real lineage rather than something a human retyped. this is the one thing a wrapper around somebody's client cannot give you: another tool's dag inside this one
- building one invokes `dbt run --select <model>` in the project directory with both pipes read into the run's captured output: your dbt, your profile, your environment, invoked and not reimplemented. a non-zero exit fails the asset with the code, a cancelled run kills dbt, stdin is `/dev/null`, and a dbt that is not installed says which program could not be started
- the subprocess capture that already existed for isolated ops moved from `isolate` to `logs`, where the other two writers of `op_logs` live, rather than being written a second time. the comment about draining both pipes concurrently (a child blocked writing into a full pipe never exits) moved with it, because it is the reason the code is shaped the way it is
- **the manifest schema versions are named**: v9 through v12, dbt 1.5 through 1.10. anything else is refused by version, naming the file, at startup. parsing hopefully produces an empty asset graph, which looks exactly like a project nobody has compiled yet: a failure somebody debugs for an afternoon. two nodes that would become one asset are refused the same way, since keeping the second quietly would drop the first's lineage
- **dbt does not have to be installed to test any of this, and is not.** the fixture manifest is committed (a diamond over a source, with a seed, a data test, a hook and a disabled model beside it) and the parse, the graph and the registration are asserted against it; the shell-out is asserted against a script standing in for dbt, down to the arguments, the working directory and which stream each line came out of. what no test here can assert is that `dbt run` builds your warehouse, and the page says so rather than shipping a test that passes by not running
- hestan does not query your warehouse, so a model it rebuilt gets a new fingerprint and everything downstream of it is stale, while a dbt source arrives with no probe and leaves every plan that reaches it stale. give the source a probe and the graph becomes incremental. `Asset::name()` and `Asset::deps()` are new, which is how you find the one asset you want in a vec you did not write
- **`docs/connecting.md`**, the page that was missing: nothing anywhere said how to connect to a database, so every reader worked it out again from `Op` and `Hestan::resource`. a client called from an op, a pool built once as a resource, the credential out of the environment rather than out of run params (which are stored on the run and served over the api) retries against timeouts against pools, and where the io managers fit
- the worked postgres example on that page **is a doctest**: it lives on `Hestan::resource`, it connects to a real postgres and runs a job against it when the suite is given one, and a test holds the page to it character for character. a docs example nobody compiles is a guess, and this one is the guess most worth not making
- and the decision stated plainly where somebody looking for a `SnowflakeResource` will read it: **hestan wraps no vendor client, and will not.** a wrapper is a subset of somebody else's api, a version behind, with bugs and docs of its own. rust's ecosystem is crates, and an op is a place to call one. what hestan owns is when the work runs and what is recorded about it
- `docs/dbt.md` and a parquet section in `docs/io-managers.md`, both with a "what this is not" of their own: no partitioned datasets, no compaction, no object store, no `run_results.json`, no `dbt test`, models only, and one `dbt run` per model with the cost of that written down

- **`#![deny(missing_docs)]`**, and the 323 public items (types, fields, variants, methods) that had to be written to earn it. writing them was an afternoon; the deny is what holds, because a public item shipping bare is a gap nothing else reports: the build stays green, the docs page grows a signature with nothing under it, and the number only ever goes up. it holds under **every feature combination**, since a feature-gated item is exactly the one nobody notices is undocumented
- **`#![deny(rustdoc::broken_intra_doc_links)]`** with it, which found five links to items that no longer exist under the feature they were written for and one to a method that never existed (`JobBuilder::on_run_finished`, unresolvable from the module it was written in). a link to a renamed item is worse than no link: it reads as a promise that the thing on the other end is still there
- the `capture` module is public now. its module docs (what the layer deliberately does not capture, and why an in-process `println!` cannot be) were on a private module, so rustdoc rendered none of it and the link to them resolved to nothing
- **every entry point a reader lands on has an example that compiles as a doctest**: `Hestan`, `Job`, `Op`, `Asset`, `Sensor`, `Schedule`, `Store`, `Auth`, `cli::run`, and a whole pipeline at the crate root in under twenty lines. `no_run` where it would bind a port or open a database; the one ```` ```ignore ```` block in the crate is gone, since an example nothing compiles is an example that stops compiling and nobody finds out
- `[package.metadata.docs.rs]` gains `rustdoc-args = ["--cfg", "docsrs"]`, and the feature-gated items carry `doc(cfg(..))`, so docs.rs shows the postgres, cli, capture, otel and http surfaces with the feature each one needs written on it. nothing else sets that cfg, so an ordinary `cargo doc` is unaffected
- **the http api page and the router are now checked against each other.** every `.route(...)` in `src/server.rs` must appear in `docs/http-api.md`'s table and every row of that table must be a route: a documented endpoint that no longer exists sends a reader somewhere that 404s, and one the router serves but the page never mentions is an api nobody can find. it caught seven undocumented endpoints on the first run: the partitions listing, all three backfill endpoints, the backfill launch, `/api/late` and `/api/notifications`
- **the docs index is asserted to list every page**, because an unlinked page is one nobody reads and therefore nobody updates, and **the readme's feature table is asserted against `Cargo.toml`**: a feature nobody wrote down is a feature nobody turns on
- the prose, read against the code rather than trusted. what was wrong: `scaling.md` opened with a `serve(([0,0,0,0], 4000))` example that the auth refusal would now reject, on the same page that explains the refusal; `assets.md` said `POST /api/assets/{name}/build` "always rebuilds its target" when it answers `up_to_date` on a fresh one, which mattered, because that sentence was the documented way to force a newly-added check to run; `assets.md` also said the one-build-at-a-time gate covered two paths where the code has four; `notifications.md` put the durable-retry budget at "somewhere over two hours" when eight attempts is seven gaps and about twenty minutes at the outside, and the source comment said the same thing; `embedding.md` said `serve` aborts "both loop tasks" when it spawns up to eight; `web-ui.md` said seven pages and there are eight, since the activity feed had no section at all; `metadata.md` used a byte count that renders as `1.3 GB` to illustrate `1.2 GB`; `development.md`'s migration recipe was still numbered for v3, fifteen versions after v3 shipped; `io-managers.md`, `isolation.md`, `resources.md` and `logs.md` each still assumed sqlite was the only backend; `cli.md`'s `doctor` sample showed a schema version one behind; three cross-page anchors pointed at headings that had been renamed
- `docs/choosing.md` is new, and is the page that was missing: job or asset, sqlite or postgres, in-process or isolated, schedule or sensor, with a closing section on the pairs that look like choices and are not (overlap against a concurrency limit, freshness against staleness, scheduler against worker, the run log against captured output)
- `docs/getting-started.md` rewritten to be readable by somebody who has not seen the crate: `cargo add` through to a scheduled job with a ui in one pass, then what each line of it actually is

- **`serve` refuses to start on an address that is not loopback while nothing checks who is asking.** the api launches runs, cancels them, pauses schedules and changes limits, and the only thing standing between that and a public port was a sentence in the docs. it is a refusal rather than a warning because a warning is a line in a log that scrolled past three deploys ago. the error names the address and the three ways out: bind loopback, configure an authenticator, or say `Auth::None` and mean it
- **loopback with nothing configured is untouched**: no token, no header, no configuration, one process on one machine, exactly as before. loopback means every spelling of it: `127.0.0.0/8`, `::1`, and `::ffff:127.0.0.1`, which is v4 loopback wearing a v6 address and which `Ipv6Addr::is_loopback` says nothing about. `0.0.0.0` and `[::]` are every interface and are refused. the check is made on the address the **listener is holding** rather than the one it was handed, so nothing put between the ask and the bind can make the guarded address and the served one two different things
- **`Auth::bearer(token)`** for one shared admin token, and **`Auth::custom(|req| -> Option<Identity>)`** for a deployment that already knows who its people are: a header its proxy set, a signature it can check. a token is hashed when it is handed over and the plaintext dropped, so the process holds a digest and not the secret, and the comparison is **constant time**: a byte-by-byte `==` turns how long it took to say no into how much of the token was right
- **three roles that contain each other**: viewer reads (every `GET`), operator drives what is running now (launch, cancel, retry, resume, build, backfill), admin changes how the deployment behaves (pause, unpause, priority, presets). the mapping is a table in `docs/auth.md` and the code is derived from it: default-deny by method, so an endpoint added tomorrow lands on the rule rather than in a hole, and a test scrapes every route out of the router and fails if one of them is not asserted
- **401 for a credential that is absent or unrecognized, 403 for an identity that may not.** the 401 says nothing about what was wrong with what it refused; the 403 says what it would have taken. the ui's own files and `GET /api/whoami` are outside the guard, or the page that asks for a token could not load and `doctor` could not tell an authenticated deployment from an open one
- **a credential never reaches a log, an event, an error, a response body or the ui.** `tests/auth.rs` is the test that says so: it drives an authenticated deployment through a launch, a retry, a cancel and a pause, then greps both of the server's streams, every response it sent, every event and run row, and every byte of the database file, for the token and for the wrong token it refused
- **the ui asks who you are before anything else, and does not render a control your role may not use.** a viewer's job page says `launching needs an operator` where the launch controls are; cancel, re-run, resume, build, backfill, pause, presets and the queue's bump are absent the same way. a button that answers 403 teaches people the ui lies about what they can do. the token lives in `sessionStorage` (this tab, this origin, gone when the tab closes) and what that does **not** protect against is written down in the prompt and at length in `docs/auth.md`: any script on the page can read it, it does not expire, and a bearer token is an admin token
- an authenticated tab **polls** the event log instead of opening an `EventSource`, which cannot carry a header: the only other way to authenticate a stream is to put the token in the url, where it lands in the browser's history and in every access log on the way
- **the command line takes `--token`, and `HESTAN_TOKEN` for the cron line that must not put a secret in argv** where `ps` shows it. the environment is read by hestan rather than by the argument parser, which would print the value in `--help`. a refusal is **exit 8**, a code of its own so a script can tell work that failed from a credential that was not accepted
- **`doctor` says whether the deployment it is pointed at is authenticated at all**: in the binary from what it is configured with, and over `--server` from `/api/whoami`, which needs no credentials. that is one finding and a long list of what an http api cannot show it, rather than the flat refusal `--server doctor` used to be
- **the event log says who.** schema v18 adds `runs.actor` and `events.actor`: the identity that caused a run, a cancel, a pause or a backfill, by name and never by credential. `Trigger::Manual` becomes "manual, by whom", which is the useful half of an audit trail. pausing a schedule or a sensor is now an event at all (`schedule_paused`, `sensor_paused`), because a paused schedule outlives whoever paused it
- **an unauthenticated deployment records no actor rather than a fabricated one.** an empty name is not "system": `manual` with no actor means a person asked and nothing was checking who. a cancel of a run that is already executing is two events: the run's terminal event belongs to whichever process is executing it and does not know who asked, so the asking is a line of its own and it is the line with the name on it
- `docs/auth.md` is new: the refusal, the two authenticators, the roles endpoint by endpoint, the ui's token and what it does not protect against, the command line, the audit trail, and an honest section on what this deliberately is not: no user store, no sessions, no oauth, no per-job permissions, no rate limiting, no tls

- **a command line, and it is your own binary.** hestan could only be driven from the ui, the http api, or rust you wrote, which is awkward in exactly the place a scheduler belongs, which is a cron line, a ci step, a systemd unit and a terminal at three in the morning. `hestan::cli::run(app, addr)` in place of `app.serve(addr)`, behind `--features cli`, and that binary has one
- **with no arguments it serves**, on the address it was handed, with the same loops and the same error if the socket will not bind. that is a compatibility promise rather than a convenience: a deployment that swaps the one call for the other and changes nothing else behaves as it did. it asks the isolated-op guard before it looks at argv, because an op subprocess is this binary re-executed with *no arguments*, and a mount that skipped that would answer a request to run one op by binding a socket
- **nothing is loaded and nothing is configured.** the jobs, assets, schedules and sensors are already compiled in, so starting is opening a database: no workspace file, no module path, no import that fails for reasons unrelated to your data. the two things that follow are the ones usually out of reach: `explain` resolves a real plan, and shell completion of your own job and asset names is asked of the binary at the moment you press tab rather than baked in at build time
- **`run --wait` executes the run here, streams it to stderr, and exits with what it did**, which is the whole point: a cron line is only as good as its exit status. 0 succeeded, 1 failed, 2 the command line was wrong, 3 canceled, 4 timed out, 5 the store or server was out of reach, 6 this mode cannot serve this command, 7 doctor found something actionable. each has a case of its own that runs the real binary and reads what it exited with
- the stream and the exit code are a race, and the ordering is written down where it is made: the status is read *before* the drain that follows it, so a job that finishes in milliseconds (before a follower could plausibly attach) still has every line it wrote come out. the executor already writes a run's terminal event before its terminal status, so stopping at the status leaves nothing behind
- **stdout belongs to the answer.** `--json` is one object, `--quiet` is the id alone for `$(...)`, anything streaming is one object per line, and under either of those nothing else may reach stdout. `NO_COLOR`, a pipe, and a machine-readable mode each mean plain text, asserted by running the binary with both streams piped and looking for an escape byte
- **a launch that does not wait enqueues under a role that does not execute.** an enqueue pokes the dispatcher, so a process that both decides and executes would start the run and then exit out from under it a millisecond later: a launch that reliably killed what it launched. caught by running it rather than by reading it
- **three ways to reach a deployment**, with the same commands meaning the same things: this binary, `--db <path|url>` for a run log opened directly with no server running, and `--server <url>` over the http api the ui already uses, so it works against an instance that predates this. every command answers with one json object shaped exactly as the api shapes it, whichever mode produced it, and the tables are renderings of that object rather than a second thing to keep in step with it
- **where a mode knows less, the keys it cannot fill are absent** rather than null or invented: `--db assets` has no staleness column because staleness is a claim about a registry, and `--db queue` has no "waiting for" column because the blame belongs to whoever owns the limits. and where a mode genuinely cannot serve a command it says so in one sentence and exits 6: launching from a run log names the run log, says it holds no job definitions, and names the two things that would work
- **a standalone `hestan` binary** behind the same feature, for an operator who has a database or a url but not your code: `cargo install hestan --features cli`. a default build compiles no binary and no argument parser at all
- **`doctor` answers "why is nothing running"** from the one place that can see all of it: the store, the registry and the disk under them. a cron expression or timezone that no longer resolves, so the schedule silently never fires again; a schedule or sensor paused; runs held past a lease by a process that stopped renewing it; runs waiting on a limit, and separately runs waiting on *nothing*, which is a deployment where no process executes and has no other symptom; a retention policy in a role that never sweeps; free space where the run log lives
- **every check looks at something, and a check a mode cannot make is not run**: it is listed as not run, under its own heading. a check that always passes because it cannot see the thing it is about is worse than no check, so each condition is constructed in a test and asserted found. three levels: `wrong` is actionable and exits 7, `note` is worth knowing and does not: a paused schedule is usually something somebody chose, and a check nobody can satisfy is one everybody learns to ignore
- **`explain` resolves the plan without running it**: the dag in stages, where a stage is what runs together (every op in it has its dependencies behind it and none on each other) with the pools that gate it, the trigger rules that could skip an op, where isolation applies, and what a mapped op fans out over. `run --dry-run` validates params through the same call a launch validates them with, prints that plan, and creates nothing
- **completions for bash, zsh and fish that cannot go stale.** the script calls the binary back for candidates, which is a process start and a walk over a registry that is already in memory. a job added this morning completes this afternoon with nothing regenerated and nothing running
- the command line's event follower and the sse stream now share `Store::readable`, so a terminal tailing the log and a browser watching it cannot come to different conclusions about what has settled: for a rule that subtle, two copies is how they would
- `docs/cli.md` is new: the mount, the three modes, every command, the exit-code table, the output contract, doctor's checks and what each can actually see, and cron and ci examples that would work as written

- **the event log stopped being only about runs.** `events.run_id` was `NOT NULL`, so an event could describe a run and nothing else: an asset materialized, a schedule that fired or was skipped, a sensor tick, a backfill's chunks, a notification nobody received, a lease taken back from a dead worker, a retention sweep: each happened in a table of its own and reached no stream at all. you could ask a run what it did and you could not ask the deployment. v17 makes `run_id` nullable and adds a **subject**: `subject_kind` (`run`, `job`, `asset`, `schedule`, `sensor`, `backfill`, `system`) and `subject`, with seventeen new kinds filling it in
- **each event is written by the subsystem that does the work, in the transaction that does it.** an event is a claim that something happened; one written *next to* the row instead is a claim a crash can falsify, in one of two directions: a log that says a thing happened which did not, or a thing that happened and left no record. so the materialization event goes in the materialization's transaction, the check's with the check row, the tick's with the tick, the backfill's with its status, the notification's with its mark, and retention's with the deletes it counts. the three places with no transaction to join (an op's own progress, a run's terminal event, a fired schedule's run) are named in `docs/events.md` with the exact window each leaves
- `subject` is deliberately **not** a copy of `run_id` on a run event: filling it in would rewrite every row of the largest table in the database to store a second copy of an indexed column. `Event::about()` is where the two become one answer, and the api's `subject=` filter matches either
- **v17 is the one migration where the two backends do genuinely different amounts of work.** sqlite has no `ALTER COLUMN`, so dropping the `NOT NULL` rebuilds the table and copies every row; postgres drops a constraint and adds two defaulted columns in the catalog and reads the table exactly once, to build the new index. tested against a populated database on both. postgres also gets a forward migration chain for the first time, since before this there had never been an older postgres database to move
- **every kind has a documented payload**, and the ones that describe work carry the phase-19 `Meta` map the work reported: a materialization its rows and bytes, a check its severity and its value, an op success what `ctx.meta` staged, a backfill its counts. the schema has a version (`hestan::EVENT_SCHEMA`, reported by the api) and a promise: while hestan is 0.x a documented key keeps its name, type and meaning, payloads may gain keys, and kinds may appear, so read the keys you know and ignore the rest
- a kind this build has never heard of now reads as itself rather than failing the query and taking every row around it with it. a newer writer is entitled to write kinds an older reader does not know, and one unrecognised word breaking the page it is on is exactly the failure a log has to not have
- **`GET /api/events`**: the whole log, newest first, filtered by kind, subject, level and time, cursored on `seq`. this is the "what happened last night" query, and it did not exist
- **`GET /api/events/stream`**: the same log live, as server-sent events, from a cursor, so a consumer that drops off and reconnects gets the gap before the tail and misses nothing
- **the cursor does not skip, and the reason it could is worth knowing.** `seq` is allocated on insert rather than on commit, so a writer holding 5 uncommitted is invisible while one that took 6 and committed is not, and a follower that takes 6 and moves on never comes back for 5. sqlite cannot reach that state, because its writers hold the write lock until they commit and seq order *is* commit order. postgres can, so a follower there delivers only the unbroken run above its cursor, waits two seconds on a hole and steps over it after that: a hole is either a transaction still committing or one that aborted, and nothing outside the database can tell those apart. the assumption that leaves is stated where it is made, and forced in a case that holds a real uncommitted insert open
- **a consumer that falls behind is dropped, and told.** the queue is 256 events; past that the cursor moves on without it and a `dropped` event carries the count and the seq it ran through, so the gap can be fetched back exactly. never unbounded buffering: a consumer that stopped reading must not become memory in the orchestrator
- **the ui gets an Activity view** over the whole system: one row per event, what it was about, filterable by subject kind, level and text, following the stream so a run that starts while you are looking appears at the top. an empty database gets a designed empty state rather than a blank table
- **a sensor evaluation that looked and found nothing no longer writes an event.** it is still a tick, and the sensors page still reads every one: that is the sensor's health record. but a sensor polling every five seconds is seventeen thousand a day, and an activity log where those are ninety-nine rows in a hundred is one nobody can read anything else out of
- the events that belong to no run belong to no run's retention either, so they get a cap of their own: the newest 50,000, swept by the same loop, unconditionally
- **`--features otel`: a run as a distributed trace.** a run is `hestan.run`, an attempt is `hestan.op` beneath it, a retry is a span of its own, and hestan's run log becomes span events on them. hestan installs no subscriber, no provider and no exporter: the host composes `tracing-opentelemetry` into the subscriber it was building anyway, exactly as it does for `capture_layer`
- **an isolated op carries the trace across the process boundary**, which is the part nothing else does: the child is handed its parent attempt's w3c `traceparent` in its environment and parents its own span to it, so spans the child's code opens nest under the op that spawned them. what that cannot do is written down beside it: a child's spans are exported only if the child's binary composes a layer, and hestan will not flush an exporter it does not own, so a child role that exports has to flush before `main` returns
- `docs/events.md` is new: the kinds, the payloads, the atomicity table with its three windows, the queries, the stream and both of its failure modes, the otel mapping and its limits

- **a postgres backend.** `Hestan::db("postgres://user:pw@host/db")` with `--features postgres`, and nothing else about a deployment changes: the queue, the claims, the leases and the roles were always backend-agnostic. sqlite stays the default and is not the lesser option: for one process, or several on one host, it is the right answer and the one with nothing to operate
- **one schema, and the two things it does differently.** the sixteen tables the sqlite chain arrives at, created whole at the current version: there are no postgres databases in the world that predate this, so walking sixteen migrations would be a re-enactment. timestamps stay rfc3339 `TEXT` and the boolean columns stay integers, because every query compares and orders them as strings and `timestamptz` would change those semantics across the whole store for no gain here, and a row reads back identically off either backend, which is the point. every text column is `COLLATE "C"`, because sqlite sorts text by byte and postgres sorts it by locale, and the same query would otherwise answer two different things
- **the store's suite runs against both**, one suite twice rather than two suites: a second set of cases is exactly how two backends come to disagree, since the case nobody copied across is the one nobody misses. it caught four things: a parameter whose first use is `?1 IS NULL` never gets a type in postgres and the statement is refused; an integer literal is the narrow integer, where every column of this schema is the wide one; postgres orders nulls last where sqlite orders them first, which silently reversed the unpartitioned asset's place in two listings; and a postgres error's own `Display` is the words "db error", which would have put every storage failure in a log saying nothing
- eleven store cases are new, covering the families no store case reached (claims, backfills, checks, freshness, metadata series) because each of those was tested only through the executor's or the registry's own suites, and every one of those runs on sqlite alone
- **claims that skip locked rows.** a dispatcher reserves the one run it decided on with `FOR UPDATE SKIP LOCKED`, so a second dispatcher reaching for the same run is handed nothing for it and moves on rather than waiting for a claim to commit only to find it lost. counting capacity and spending it still have to be one decision, and one transaction is not enough for that on postgres, since four dispatchers each read the same free slot from their own snapshot and all four started a run under a global limit of one, every time it was tried. so when a limit is in force, and only then, claimers take turns on an advisory lock
- the phase-18 invariants are asserted on both backends now, with real threads on connections of their own released together: exactly one claimer comes away with a contested run, four claimers split a queue of four without overlapping, a live lease survives another process's boot while an expired one does not. `tests/queue.rs` runs its three worker-process cases twice, against a sqlite file and against one postgres schema, with one mark line per run either way
- **the client is `tokio-postgres`, driven on a runtime hestan owns.** the sync `postgres` crate holds a runtime and calls `block_on` on it, which panics outright on a thread already driving one, and hestan calls the store from inside ops, hooks and api handlers. `Store` stays synchronous, no call site changed, and the blocking is the blocking sqlite already does
- **what is proven and what merely follows**, in `docs/scaling.md`: several *processes* against one postgres is what the suite runs. several *hosts* has not been run, because the machine it runs on is one machine. no tls yet either: the connection is `sslmode=disable`, and that is written down rather than implied away
- **an asset has a page.** `/assets/{name}`, deep-linkable, with the drawer's content as a real page: one implementation of that content, so the quick look on the table and the permanent address can never drift apart. lineage as links in both directions, the downstream side computed from the deps every asset already carries rather than from a new endpoint. the name is a path segment per separator, so `sales/orders` reads as a url rather than as an escape sequence, and `Meta::asset_ref` now points at the page
- **why it is stale, as a causal chain: the thing content fingerprints buy that clocks cannot.** the dep whose content moved, the fingerprint this asset consumed against the one that dep holds now, the build that fingerprint arrived in as a link to its run, and when. then the same question asked of *that* build, from the input fingerprints it recorded, four levels up: "customers is stale because orders changed, in run 3f2a1b8c four hours ago, because the events source moved under it". the build named is the oldest consecutive one holding the fingerprint, since a rebuild that produced the same bytes is not when it changed, and a fingerprint the recorded history does not reach names no build rather than the nearest plausible one
- **three claims kept apart**, because collapsing them is the easy lie: a dep whose fingerprint moved *changed*; a dep that is only stale itself has moved nothing **yet** and says so; a dep with no fingerprint at all has never been built. the api records a reason for all three and calling them all "changed" would be a fabrication on top of real data
- `freshness` on the api gains `within_secs`. how far into its window a fresh thing is cannot be derived from `late_by_secs`, which is null exactly while it is inside the window, so the page draws the window as a length and prints what is spent of it
- **a catalog that survives having assets in it.** search by name substring as you type; a state filter over four questions the engine answers with one word: fresh, stale, never built (the same verdict as stale, a different thing to look at) and failed check, which cuts across the rest; grouping by the prefix before the first `/`, collapsible, with the prefix dropped from the rows underneath. no separator anywhere means no grouping, since a common substring is not a namespace
- the columns are the ones worth the width (state, last built, the run that built it, freshness where a policy is set, partition coverage where it is partitioned) and all sortable, the last two drawn only where something fills them. deps and the fingerprint go: both are on the asset's own page and neither was ever read across three hundred rows
- **the graph at scale**: focus on a node and its neighbourhood to 1, 2 or 3 hops in both directions, capped at 40 nodes because one source with sixty dependents has a neighbourhood the size of the graph; fold a prefix group into one node with the edges that crossed its boundary rewired to it; and search-to-highlight, which marks what matches (a folded group by what it swallowed) and dims the rest, except when nothing matches, since a uniformly grey graph reads as a fault. **past 60 nodes the graph opens focused rather than whole**, on the selection or the first stale asset, which is about where the tallest column stops fitting on a screen
- every control on the catalog is in the url (`q`, `state`, `sort`, `dir`, `closed`, `graph`, `depth`) so a filtered, folded, sorted view is a link somebody else opens
- **backfills you can start.** the engine, the endpoints and a list with cancel have existed since phase 13 and there was no way to launch one, which made the feature staff-only in practice. drag across the partition grid on an asset's page and the cells you crossed are the range: two dates typed into boxes would be the ui guessing at a partition scheme, and the key set is already on the screen
- **the cost before it is paid**, from `op_stats`: the median of what a *successful* build of one of this asset's partitions has actually taken, since a failure's duration is how long it took to break. quoted as work rather than wall clock, because chunks go out one after another, and **with no history it says so** rather than quoting a number with nothing behind it
- an empty range, a range holding no partitions, a range already entirely fresh, and an asset whose previous backfill is still running are each a disabled button with the reason beside it, rather than a 400 after the click
- **a backfill has a page**: `/backfills/{id}`, progress per partition in the usual shapes, which chunk each key went out in, what failed, what is left, and cancel. which run built which key is arithmetic (the chunk size is `launched` over the runs it took) since the asset's build limit is not on the wire
- **`op_stats` stopped lying about mapped ops.** one writes no `op_runs` row of its own, so reading its history under its own name found nothing and it reported "no runs yet", for every mapped op there had ever been, including the op that materializes a partitioned asset, which is the one a backfill needs a duration from. it now reads its instances, using the executor's own rule for what an instance is rather than a second copy of it. the run page had the same bug in regex form, assuming an instance's label was an index: a partitioned asset's node read "not in run" on the page of the run that built it
- **the log pane searches and follows.** a find box over both sources at once, marking every hit and counting the lines that hold one, with an `only` toggle that narrows to them: a second decision, since the line above a match is often the point. follow pins the pane to the newest line while a run is live and **lets go the moment you scroll up**, and does not re-arm itself when you scroll back, because a pane that yanks you back is worse than one that does not follow at all. it is absent on a finished run, which has no newest line to pin to
- the ui's suites grow to the decisions this phase added: the staleness reads, the catalog's filtering, grouping and sorting, the two graph transforms, the backfill range, its estimate arithmetic and its refusal logic, and the log search: all pure functions over fixtures, because a table of three hundred assets is not a thing to assert through the dom

## 0.1.0-alpha.2

still alpha, and still pin an exact version. `Store` is public api and postgres
will reshape it; the note below about hooks is the other reason.


- **retention that runs.** `retention_days(n)` swept once, at startup, over every job, so a server that boots and stays up for three months pruned nothing after its first second, which is the one deployment shape a retention policy is for. a sweep now runs at startup *and* every `Hestan::retention_interval` (an hour by default), because a process that runs for an hour and exits should still tidy up and one that runs for a quarter should keep tidying
- **`Retention` is a type, and per job.** `Hestan::retention(Retention::days(30).keep_last(20).failed_days(90))`, with `JobBuilder::retention` overriding the lot for one job, and an archive job beside forty chatty ones is the case. `keep_last(n)` holds the newest n finished runs back from the age cutoff whatever their age, and `failed_days(n)` ages failures and cancellations slower than successes: a success is noise a week later, and the failure you want next quarter is the one about to go. `retention_days(n)` stays, means `Retention::days(n)`, and is asserted to
- **a run goes only when every rule would take it.** keep-if-either is the conservative direction and the other reading deletes history you find out about afterwards, so it is stated in the docs and asserted at the boundary from both sides: keep_last holding a run past the age cutoff, and the age cutoff holding one past keep_last. `keep_last` on its own deletes nothing, since with no age policy there is nothing for it to hold anything back *from*. a run that has not finished is never pruned whatever its age: a queued run older than the cutoff is a queue problem, not a retention one
- **the sweep is role-gated**, the same rule every other loop follows and the same mistake the phase-17 boot sweep made: several processes share one database, a worker owns none of the history, and a worker pruning the scheduler's runs is data loss nothing would ever report. tested from both ends: the same registry under `Role::Worker` prunes nothing and under `Role::Scheduler` prunes the same run
- one index seek per **job**, not one visit per run: the jobs with runs are walked by a loose index scan over `runs_job_created` rather than `SELECT DISTINCT job`, which reads the same answer by visiting every entry in that index, and each job's doomed rows are a range seek on the same index. both query plans are asserted, because an index that quietly stops being used is the kind of thing nobody notices until the sweep takes a minute. one transaction per job, so a run and its children still go together without holding the write lock for fifty of them. the tick logs join the sweep for the reason the runs did: they were capped at boot and grew unbounded after it

- **hooks for the things that happen.** `on_failure` was the only callback there was, so hestan could tell you a run broke and nothing else: no success, no cancel, nothing at op level, nothing scoped to one job. `Hestan::on_run_finished` takes a `RunEvent` and fires for every terminal status with `status` saying which, carrying the trigger, the failing op and its error, the timings and how long it *executed* for, which is not how long it existed for, since a run held back by a limit was not running while it waited
- **`on_op_finished` fires once per attempt**, not once per op, and that is the useful shape rather than an accident: an op that failed twice and worked on the third try is three facts and only the hook knows which of them it wanted. `attempt` and `status` are how it says so, and the timing is the attempt's own rather than the row's, which keeps the first attempt's start. an op skipped by its trigger rule produces nothing at all, because there was no attempt. that is asserted, since the tempting implementation reports a skip as a zero-length failure
- **per job**: `JobBuilder::on_run_finished` and `on_op_finished` scope either hook to one job, so an alert can cover the nightly production run without covering every backfill beside it, and without a hook that has to keep a job list by hand
- **`on_failure` is that path with a filter on it**, not a mechanism beside it. `RunFailure`, its signature, `FailureHook` and `Runner::with_failure_hooks` are exactly what they were; one dispatch means one place an event can go missing from rather than two traversals of the executor to keep in step. the old behaviour is still asserted directly: a success and a cancel reach nobody, a failure arrives once with its op and its message
- the webhook and slack helpers grow to both new types, and a run that succeeded reads `job orders_etl succeeded in 12.3s` rather than anything that looks like a page: a channel where the good news looks like the bad news is a channel people stop reading. the event types, the hook aliases and the dispatch move to `src/hooks.rs`, which is now the one place describing what hestan calls you about

- **delivery that survives the process.** a hook was a `spawn_blocking` call and nothing else: if the post failed the alert was gone, and if the process died between the run finishing and the hook running, the alert was never sent and nothing recorded that it should have been. an alerting system that loses alerts on exactly the failure it exists to report is not one. `Hestan::durable_notifications()`, off by default and meant to stay off for anything whose hook is a metric rather than a page
- **the event row is written in the same transaction as the run's terminal row.** written after it, a crash in the gap is the exact hole this closes, so `run_finished` became a transaction and takes the payload, and that is asserted rather than described: the case installs a trigger that refuses the insert and checks the run row went back with it. a run failed by the lease reclaimer is written the same way, in the transaction that fails it
- a delivery loop takes what is due, runs the hooks and marks delivered. a hook that panics is a failed delivery, retried on the existing `backoff` with full jitter and given up on after eight attempts, and **giving up is loud**: the row stays, `failed`, with the error that stopped it, on `GET /api/notifications?state=pending|failed|delivered` and in a section of the runs page. an alert nobody received should be visible in the ui the alert was about, rather than in a log line from Tuesday
- **at-least-once, said plainly and next to the api.** a crash between the hook returning and the mark landing re-delivers, because the alternative is marking first and losing the delivery instead, and of those two, a receiver seeing an alert twice is the one you can do something about. the redelivery path is exercised rather than described: the case puts the row back where that crash would have left it and asserts the hook sees the same event again
- retention takes delivered notifications on the age cutoff and leaves undelivered ones at any age: one that never got through is not history, it is something outstanding. covers run events; op hooks and `on_late` stay in-process, since they fire per attempt and per poll and a table of those is a different bargain
- schema v16: the `notifications` table, with the state carried by `next_attempt_at`: set and undelivered is pending, null and undelivered is given up on. so a row is inserted due now and giving up clears it, which keeps a permanently failing notification out of the scan while leaving it visible. the scan is a partial index over the undelivered rows, since the delivered ones are the table
- **what durable delivery does not cover, stated here rather than found later**: `notify::webhook` and `notify::slack` hand the request to `tokio::spawn` and return, so the hook is finished before the post is attempted and the delivery is marked the moment it is spawned: a 5xx or a dead endpoint is a log line, not a retry. the retry path is the panic, so it covers your own hooks and not the two shipped helpers. `FailureHook` is `Fn(RunFailure)` and cannot express failure, so fixing this is an api decision rather than a patch, and it is not made here

- **hestan captures what an op printed, not only what it said.** `ctx.info` was the only thing that ever reached a run log, so every op that calls a library (which is every real op) was half invisible: the run log said "attempt 1 failed" and the reason sat on a terminal nobody was watching. new page: [docs/logs.md](docs/logs.md)
- **an [isolated op](docs/isolation.md)'s output is captured whole, always, with nothing to switch on.** it is a subprocess, so its stdout and stderr are hestan's: both pipes are piped, read and stored line by line under the op's attempt: `println!`, a python subprocess, a linked c library writing to fd 2, verbatim. no new dependency and no user action; it is what `isolated()` should always have done
- **both pipes are drained concurrently, by a task each,** and that is correctness rather than tidiness: reading stdout to its end and stderr afterwards leaves stderr's 64 KiB pipe buffer to fill, and a child blocked writing into a full pipe never exits, so the parent waits forever for a process waiting for the parent. that shows up as a slow op under load and as nothing at all in a test, which is the kind of bug that ships. the chatty case floods 600 KiB down each pipe on purpose, so a serial drain hangs the suite instead of passing it
- a killed child, one that aborted, and one that died mid-line all keep what they wrote: the pipes are drained *after* the kill, because a pipe ends when the process on the other side of it is gone. for a segfault that output is the only evidence there is. a child that printed nothing stores nothing, rather than a marker saying it was quiet, and a retry's output is stored under its own attempt beside the attempt it replaced
- **`hestan::capture_layer(&store)` behind the new `capture` feature**: a `tracing` Layer you compose into your own subscriber. an in-process op's `println!` cannot be captured and the docs say so rather than hiding it: fd 1 belongs to the whole process, and redirecting it would hand the host application's own output to whichever op happened to be running. what an in-process op *does* emit is tracing events, which come with a level, a target and fields, so this is the better half of the trade rather than a consolation
- **attribution is by span, not by thread or by clock.** every in-process op body now runs inside `info_span!("hestan.op", run_id, op, attempt)`, entered across every await, and the layer stores an event only when the walk outward from it reaches one of those spans. the host's logging is untouched by construction rather than by filtering, and an op's own nested spans still attribute to the attempt above them. the documented edge, asserted rather than described: an event from a task the op spawned is *not* captured, because `tokio::spawn` carries no span, and `.instrument(Span::current())` puts it back
- **the emitting thread never waits on sqlite.** events go into a bounded buffer and a writer thread stores them; a full buffer drops, counts what it dropped per attempt, and writes one line saying so. a gap that says it is a gap is worth something and one that does not is worse than nothing
- **capping is a correctness property, not a nicety**: an op in a `println!` loop must not fill the disk the run log lives on. `Hestan::log_limit(bytes)` (1 MiB) and `Hestan::log_lines(n)` (10,000) per *attempt*, since a retry starts from a full budget and the attempt that failed is the one worth reading. past either, capture stops and exactly one line says what was dropped and why, and the op carries on, because capture stopping is not the op failing. a single line past 8 KiB is stored clipped with a marker rather than dropped, and the reader holds at most one line, so a child that never prints a newline cannot grow the parent's heap
- `GET /api/runs/{id}/logs?op=&after=&limit=` pages on `id` exactly as the events endpoint pages on `seq`, and `GET /api/runs/{id}/logs/download` answers `text/plain`, because at some point everyone wants to grep it
- the run page's log pane gains a source filter (`events`, `output`, or both interleaved by time) with the level and op filters working across both. a captured line shows its op, its attempt once there has been more than one, and its stream or level; a line hestan wrote about the capture itself is set apart, since that is hestan speaking and not the op. a line off a pipe has no level, so a level filter hides it rather than guessing that stderr means error
- schema v15: the `op_logs` table, indexed on `(run_id, op, id)`. exactly one half of `stream` and `level`/`target` is filled per row and which half says where the line came from. retention takes it with the run; a reclaimed run is queued rather than terminal, so what its first claimer captured survives for the second one

- **metadata types that carry units.** `Meta` grows `Bytes`, `Duration` and `Count` beside `Int` and `Float`, so an op says which kind of number it reported and the ui renders `1.2 GB`, `3.4s` and `1,240` instead of `1288490188`, `3.4` and `1240`. they are display types over the same one number (`Meta::as_f64` reads all five) so nothing computing over metadata has to care which was used
- **`Meta::Table`**, the single most useful thing a data pipeline can report and the one hestan had no way to say: named columns with an optional type each, and rows of json cells. capped at 100 rows at construction with `truncated` recorded, because a metadata table lands in every run page, every history entry and every api response carrying that row, and rectangular at construction, every row padded and trimmed to the column count, so nothing downstream has to decide what a ragged row means
- **`Meta::RunRef` and `Meta::AssetRef` are links hestan can follow.** an op that derived something from another run or asset names it by id or name, and the ui makes it clickable, with no url to configure because hestan is the ui. the assets page now keeps its selection in the url (`/assets?asset=orders`), which is where an asset reference goes and what makes a panel worth sending to somebody. `Meta::Path` renders monospace with the basename emphasised, since the basename is what you are looking for and the directory is context
- **the wire format is append-only.** `int`, `float`, `text`, `url`, `markdown` and `json` mean exactly what they meant in phase 12, and rows written then are on disk in every demo database; this phase added tags beside them and renamed none. `Meta::from_tagged` is the reader (`None` for a tag it does not know, rather than a guess) and there is a test carrying a phase-12 row that fails the moment any of that stops being true
- `Meta::count(n)`, `Meta::bytes(n)`, `Meta::duration(d)`, `Meta::path(p)`, `Meta::run_ref(id)`, `Meta::asset_ref(name)` and `Meta::table(cols, rows)` read at a call site; `Duration` also converts on its own, and `u64`/`usize` still deliberately do not: say `Meta::count` and get the units with the cast

- **markdown metadata is rendered**, by about 180 lines in the ui and no dependency. the subset is deliberate and [documented exhaustively](docs/metadata.md#the-markdown-subset): headings, paragraphs, bold, italic, code spans, fenced blocks, unordered and ordered lists, links and horizontal rules. everything else (tables, blockquotes, images, reference links, nested lists, html) is the literal text it was written as, which is the honest thing to do with a construct you have decided not to support
- **it parses to react elements, never to html.** the parser produces a tree of `{kind: "text" | "strong" | "link" | …}` nodes and the renderer maps that tree onto elements; no html string is built anywhere along the path and nothing in the ui uses `dangerouslySetInnerHTML`, which the suite asserts by scanning the source. so injection is impossible **by construction** rather than by escaping correctly: `<img src=x onerror=...>` in a metadata value is a text child react escapes, and there is no code path that could do otherwise
- **a link is made only for an `http(s)` target**, opened in a new tab with `rel="noreferrer"`. `javascript:`, `data:`, a protocol-relative `//host`, a path, an empty target: not a link, and the construct stays the text it was written as, so what it pointed at is visible rather than quietly dropped
- the ui grows its first test suite, `ui/test/markdown.test.tsx`, run by `npm test` and by ci: every construct, a nesting case, and both attacks asserted against the exact string react renders rather than against the parse tree. no test framework and no browser: vite bundles it for node with the app's own config, `node:test` runs it

- **deltas: what changed, not only what is.** a build reporting its numbers tells you the state; what you actually want is what moved. every numeric metadata value on `GET /api/runs/{id}` and `GET /api/assets/{name}/history` now carries `delta` and `delta_pct` beside it (`1,240 +37`, `1.2 GB −4%`) against the newest earlier run of that op, or the previous build of that same `(asset, partition)`
- **computed server-side, in the two endpoints that already read those rows.** the ui renders a row without fetching a history to do it, and the number on the page is the number the api computed rather than one the client derived. the comparison is one window function on the history query and one `ROW_NUMBER` query per run, not a query per row
- **the rule, stated once and applied everywhere**: `delta` always; `delta_pct` only when the previous value was 100 or more in absolute value, because under a hundred one unit is more than one percent and the percentage says less than the number it came from. that also disposes of a previous value of zero, which is the division that would have gone wrong. an op run that reported no metadata at all is skipped rather than ending the search: a failed op records none, and one bad run between two good ones should not erase the comparison between them
- **a key with nothing to compare against has no delta rather than a fake zero.** new, dropped, or numeric last time and text this time: absent from `deltas` entirely. "did not move" and "nothing to compare against" are different facts and only one of them is information. the numeric types all compare against each other, so an op that starts reporting a size as `Meta::bytes` instead of an int keeps its history
- the ui puts it after the value, muted, always signed (`+`, `−`, or `±` for something measured that did not move) and never in colour, because the ui is monochrome and colour alone would be the wrong way to say it anyway. a size or a duration shows the percentage and a count shows itself, with the other form on the hover
- `Store::materializations` now returns a `HistoryEntry` (the build, whether its fingerprint moved, and what the build before it reported) rather than a tuple that was about to grow a third element

- **trends**: `GET /api/assets/{name}/metadata/{key}` and `GET /api/jobs/{name}/ops/{op}/metadata/{key}` return one numeric metadata key over recent history, oldest first, and the asset panel and op inspector draw it as a sparkline under the value, the same `MicroBars` the job page draws durations with. a delta is one step back; this is the rest of the line
- **three points or nothing.** two points are a delta, which the row already says, and one is the value itself, so under three the ui draws nothing at all rather than a chart with no shape in it
- `limit` (default 20, max 200) is how many builds or runs are **read**, not how many points come back: an entry that did not report the key, or reported it as something that is not a number, contributes nothing rather than a gap or a zero. `partition=` narrows an asset's series to one key, since interleaving every key by time is a trend of the asset rather than of any partition
- `op_stats` gains `metadata`, the newest facts each op reported inside the window it already summarises: the op inspector needed a value to hang a trend under, and "what has this op been reporting" is a question the job page could not answer at all before

- **a durable, claimable run queue.** launching stops meaning starting: a launch writes a `queued` run and returns its id exactly as it did, and a dispatcher starts it as soon as no limit says otherwise; with no limits declared, the same instant, which is why nothing about a single-process deployment reads differently. the queue is the `runs` table, so it survives a restart and something else can pull from it. new page: [docs/scaling.md](docs/scaling.md)
- **one mechanism, not three.** hestan ships a queue and lets deployment shape decide where the claimers live, rather than a separate executor per place work can run; a container and a pod are then packaging rather than new execution paths. there is no celery integration because celery has no rust analogue worth porting: the queue and the workers *are* the equivalent capability, and saying so beats an integration page for something that is not there
- **limits at four scopes**, each counting runs that are *executing*: `Hestan::max_concurrent_runs(n)` across the deployment, `JobBuilder::max_concurrent_runs(n)` per job (the readme's outstanding item, now off the roadmap), `Hestan::tag_limit("env", "prod", 2)` for what belongs to no job in particular, and `Hestan::slots(n)` for what lands in *this* process. a run waiting on the queue costs nothing and counts as nothing. limits are read at the top of every dispatch pass, so raising one drains the queue it was holding back without a restart
- **priority is a preference, not an order.** `Hestan::priority(n)` and `{"priority": n}` on a launch; higher first, ties by `created_at`. the dispatcher **skips** a run a limit would block and starts the next one that fits, so a high-priority `env:prod` run waiting on its tag limit does not stop an unrelated run behind it. head-of-line blocking would be the worse trade, but it does mean start order is not priority order and nothing should be built on the assumption that it is
- **`Overlap` and a concurrency limit are different questions, and a queued run counts as outstanding.** overlap decides whether a scheduled fire should exist at all while its job has a run outstanding; a limit decides how many execute at once. so `has_active_run` still means queued-or-running, claimed or not: a job held back by a limit would otherwise collect a fire a minute behind it, which is the pile-up `Overlap::Skip` exists to prevent, and a backfill would fire every chunk of a 400-day range at once
- **claiming is a compare-and-set**: `UPDATE runs SET claimed_by = ? WHERE id = ? AND claimed_by IS NULL`, one winner by construction, with the capacity count and the claim sharing one immediate transaction so two dispatchers cannot both fill the last free slot. postgres would use `SELECT ... FOR UPDATE SKIP LOCKED` here and hold no global write lock
- **leases, and the review finding they close.** a claimer renews `lease_until` every 15s and the lease is good for 60, so four missed beats mean a claimer is gone rather than slow. `fail_interrupted` is now **lease-aware**: it used to mark every queued and running run failed at boot on the assumption that the process starting was the only one there had ever been, which with a claimable queue means a second process would fail a live one's in-flight work, mid-run. a run is now swept only if its lease expired or it is `running` with no claim at all; a **queued run nobody has claimed is left where it is, because that row is not a casualty, it is the queue**. the invariant is enforced instead of assumed, and asserted directly in a test
- `Hestan::reclaim(Reclaim::Fail | Reclaim::Requeue)`, default fail: a run that got halfway may have done half its side effects, and doing them again quietly is worse than a stall somebody has to look at. a reclaimed run's ops carry `claimer went away`, naming the claimer, and a failed one fires the failure hooks. every process runs the lease loop, including one holding nothing: noticing a dead claimer cannot be the dead claimer's job
- **`Hestan::role(Role::All | Role::Scheduler | Role::Worker)`**, with `Hestan::work(addr)` as `role(Role::Worker)` with the address made optional, the same shape `schedule` has against `add_schedule`. role rather than two entry points, so which loops run is a value the process can log and serve rather than a property of which function was called. exactly one process may decide (schedules, sensors, freshness, backfill chunking); any number may execute, which is the whole point of a claimable queue
- **an op subprocess is not a queue worker**, and they are now named apart everywhere. `Op::isolated()` spawns an **op subprocess**: one op of one run, then exit, claiming nothing, which is containment. `Hestan::work` is a **queue worker**: long-lived, claims whole runs, and spawns op subprocesses itself like anything else, which is throughput. renamed with it: `HESTAN_WORKER_RUN`/`_OP` are now `HESTAN_ISOLATED_RUN`/`_OP`
- `GET /api/queue` (depth, each waiting run's position, and what blocks it), `POST /api/runs/{id}/priority`, and `/api/health` gains this process's instance id and the runs it is holding, which is how you tell which of three workers has your run. the run json gains `priority`, `claimed_by`, `claimed_at` and `lease_until`, and the runs page grows a queued section with a bump button
- a `Dockerfile` and a `docker-compose.yml` running one scheduler and two workers against a shared volume: `docker compose up --build`. one image, differing only by `HESTAN_ROLE`, because a worker executes runs a scheduler wrote and the two must build the same registry
- **the honest limit, stated rather than discovered**: this is multi-*process* on one host, which is real and is exactly the compose case. it is not multi-node: sqlite is not reachable over a network and hestan will not ship a config pretending otherwise. **a postgres backend is the next piece of work**, and it is the only thing between the compose example and several machines; the queue, claims, leases and roles are already backend-agnostic. nothing here implies kubernetes works today
- new integration target `tests/queue.rs`: a worker executes a run another process wrote, two workers split eight runs with a marker file proving nothing ran twice, and a worker leaves a sensor due every 100ms unfired while the same registry under `Role::Scheduler` fires it within a beat
- schema v14: `runs.priority`, `runs.claimed_by`, `claimed_at`, `lease_until`, and `runs.plan`: the ops and seeds a launch decided on, written down because a resume's reused outputs and an asset build's memoized seeds live in the launching process's memory and whoever claims the run may not be that process

- **`Op::isolated()` runs one op's body in a child process.** an op that segfaults, aborts or exhausts memory takes down the process it runs in, and in-process that process is the orchestrator, and every other run with it. isolated, the blast radius is one attempt: the child dies, the parent records what killed it, and the other forty ops carry on. new page: [docs/isolation.md](docs/isolation.md)
- **per op, not per job**, which is the point: the one risky parser is contained while the other forty ops stay in-process and free. and the child is **this same binary re-executed**, not a runtime being loaded: it rebuilds the same jobs because it runs the same `main`, so the cost is a process spawn (~ms) rather than an interpreter start and a code re-import
- **the store is the whole channel between the two processes.** no pipe, no protocol, nothing to keep in sync: params and `scheduled_for` are on the run row, committed state is in `op_state`, and the child writes its output through its [io manager](docs/io-managers.md), its terminal status onto its own `op_runs` row and its log lines straight into the run's events. the one thing the parent hands over is `op_runs.inputs`, the handles it holds for each dep, because a **seeded** input (a resume's reused output, an asset build's memoized value) belongs to an earlier run and is on no row of this one. handles rather than payloads, so a gigabyte is read once, in the process that wants it
- **a child that dies without recording a result is recorded by the parent**, with what happened to the process: `op exited with signal 9 (killed) without recording a result`, `signal 6 (aborted)`, `status 101`. that containment is the whole point, so the message says what an operator needs at 3am rather than that something went wrong. a child that *did* record one is believed whatever its exit status says afterwards: it is the process that ran the body
- **the worker guard is the first thing `serve`, `run_once` and `build_asset` do**, before the address, before the store. every line of boot behaviour assumes the process owns the database, and `fail_interrupted` in particular marks queued and running runs as interrupted on the assumption that the last process died, which in a child means marking its own parent's in-flight runs, mid-run. so a worker runs no boot recovery, no schedule sync, no tick prune, no retention sweep, no scheduler, sensor, freshness or backfill loop, and binds no listener. tested directly, from both ends
- **cancellation and timeouts stop being requests.** cancelling a run with an isolated op in flight sends SIGTERM, waits the same three-second grace the rest of hestan uses, then SIGKILLs; `Op::timeout` does the same on expiry. the op run row then carries a **real `finished_at`** and says which signal ended it (`canceled: it ignored SIGTERM for 3s and was killed`) where an in-process op that polls nothing is recorded canceled with no finish time and an error saying hestan asked and never saw it stop. [concepts.md](docs/concepts.md#cancellation) now sets the two side by side, because that contrast is the reason to reach for `isolated()`
- SIGTERM arrives inside the child as ordinary cancellation, so `ctx.is_cancelled()` and `ctx.cancelled()` work there exactly as they do in-process and an op written to wind down gets to. the run's cancellation drain **does not abort an isolated op's task**: it is busy killing its own child and a dropped task would kill the process outright instead, so it is left to finish and the drain's wait is doubled to cover the grace being spent inside it
- **`.memory_limit(bytes)` and `.cpu_limit(d)`**, applied by the child to itself with `setrlimit` before the body runs. exceeding memory fails an allocation, which in rust aborts; exceeding cpu arrives as SIGXCPU, and either way the parent names **the limit**, not the signal: `it exceeded its cpu limit of 30s`. the cpu hard limit sits a second above the soft one so the kernel sends the signal that says what happened before the one that does not
- a limit without `.isolated()` is a **build error**: a limit applies to a process, and in-process that process is the orchestrator, so honouring it is impossible and ignoring it silently is worse than refusing. `isolated()` off unix is a build error naming the platform for the same reason: an isolation guarantee that quietly is not one is the worst option available
- refused at build, each for a reason worth stating: an isolated op may not be [mapped](docs/concepts.md#dynamic-fan-out) and a fan-out's instances may not be isolated (an instance's element is a slice of the parent's array, the one input that is nowhere a child could read); and an isolated op against a `":memory:"` store is refused too, since that database is private to one connection and the child would open an empty one
- job summaries gain `isolated`, `memory_limit_bytes` and `cpu_limit_secs` per op; run op rows gain `pid`, the process an isolated op is running in, cleared by the terminal write, because the field says what is running *where* and a pid outliving its process would answer that wrongly. the dag marks isolated nodes, the op inspector shows the limits, and the run page shows the pid while it is live
- every connection now carries a five-second `busy_timeout`. two hestan processes write the same file the moment an isolated op runs, and sqlite's default is to fail the second one instantly, which would be a lost event or a lost terminal row
- schema v13: `op_runs.pid` and `op_runs.inputs`

- **named parameter presets.** `Hestan::preset("orders_etl", "nightly", json!({..}))` declares one and the launchpad saves one, and both write the same row: `GET/PUT/DELETE /api/jobs/{name}/presets[/{preset}]`, plus `POST /api/jobs/{name}/runs {"preset": "nightly"}` to launch by name. what you want at 2am is "launch the one that works", not to retype json into a textarea
- a declared preset is an **upsert**, not a sync. the code that declares one owns its params so a redeploy lands on the next start, and a preset saved in the ui beside it is left alone, which also means dropping the declaration leaves the row, since presets are runtime data and nothing sweeps them
- validated **before** they are stored, at the endpoint and at build: declared presets go through the job's op validators exactly as a schedule's params do, and a preset that could never launch is a startup error or a 400 rather than a surprise the night you reach for it. naming both `preset` and `params` on a launch is a 400: two answers to "what params" is one too many
- the job page gains a preset dropdown beside the launch button. picking one fills the editor rather than launching, since the point of a stored set is that it is a starting point you can still edit; the editor block gains a name field with save and delete
- new page: [docs/launching.md](docs/launching.md): presets, params schemas, run tags, subset launches and cloning
- schema v12: the `presets` table, plus `runs.tags` for the part that follows

- **params schemas for the launchpad.** `Op::params_schema(value)` records a json schema beside the existing `.params::<P>()` validator, and the params editor lists the fields (name, type, required, description) instead of showing a type name over an empty textarea. **no schemars dependency**: the argument is a caller-supplied `serde_json::Value`, and `schemars::schema_for!(T)` produces exactly it in one line
- it is a **ui aid, not a second validator**, and the code is arranged so that cannot drift: nothing ever checks a params value against the schema, the serde round-trip stays the only authority, and a schema that contradicts its type can therefore only describe params wrongly rather than admit ones the type refuses. tested explicitly, in both directions: a lying schema's params are still a 400 at the launch, at `validate_params` and at a preset write, and params the type accepts still launch
- the job's schema is every op's **merged**: `properties`, `required` and `$defs`/`definitions` unioned, since a launch's params go to every op that runs. a name two ops give different shapes is a build error naming both: picking a winner would describe a field in terms half the job disagrees with, and a legend nobody can trust is worse than none
- `GET /api/jobs` gains `params_schema` on the job and on each op object, null where nothing was declared; a job with no schema behaves exactly as it did. the ui also marks editor keys the schema has never heard of, pointed at rather than refused

- **run tags**: a flat `{"k": "v"}` map on every run (`runs.tags`, v12). `trigger` says what kind of thing launched a run; tags say the rest: this is a backfill, this was a manual smoke test, this chunk belongs to backfill 41. set per launch with `{"tags": {..}}`, process-wide with `Hestan::run_tags([("env", "prod")])`, and automatically on machine-made runs
- the automatic ones add **only what `trigger` cannot say**, which is the whole rule: `sensor: {name}` on a sensor launch (and on a probe's auto build, a probe being a sensor named after its source), `asset: {name}` on a build asked for one asset, `backfill: {id}` beside it on a chunk. a build of everything stale gets no `asset` tag (there is no single asset to name) and nothing tags a manual launch, a schedule fire or a retry, where the trigger already says it all
- defaults are defaults: a launch naming a key `run_tags` also sets wins, since a default describes the deployment and the launch is closer to the truth about the run
- `GET /api/runs?tag=key:value` filters on an exact pair, composing with `job`, `since`, `before` and the paging cursor; the split is at the first colon so a value may hold one, and a `tag` that is not a pair is a 400 rather than a filter that quietly does nothing. the runs page gains a tag box (served rather than client-side, since a tag on a run the page never loaded cannot be filtered for locally) and shows each run's tags as muted chips

- **launch a subset of a job**: `POST /api/jobs/{name}/runs {"ops": ["clean", "publish"]}` runs exactly those ops and everything downstream of them. the machinery is not new, since `launch_subset` already powers asset builds and resumes. this exposes it, and works out the downstream closure so the request only has to name where to start
- it seeds nothing, which is what separates it from a resume: a resume has a finished run behind it, so an op it skips still has an output to hand its dependents, and a fresh subset launch has none. an upstream left out is therefore a 400 naming exactly what is missing: **the existing subset check's refusal, in its own words**, not a second implementation of the same rule. an empty `ops` list is a 400 rather than a launch of everything, and `params`, `preset` and `tags` all still apply
- the job page's dag gains "launch from here" on a selected node, with the op count, mirroring the run page's "re-run from here" so the two read the same. whether a selection can run is the server's answer and its refusal is what shows

- **clone a past run into the launchpad.** the run page gains `clone`, which opens the job page prefilled with that run's params and tags and launches nothing: the commonest real launch is "that run again with one field changed", and editing is the point. a clone that launched immediately would be `re-run`, which is right beside it
- it goes through `/jobs/{name}?from={run_id}` and fetches the values from the new `GET /api/runs/{id}/clone` rather than carrying them in the url: a run's whole params do not belong in a query string. cloning a run whose job has left the code is a `409 job no longer defined: {job}` (the same refusal in the same words a retry of that run gets) rather than a launchpad half-loaded for a job that cannot launch
- the launchpad's params block gains a tags line (`env:prod, kind:smoke`, the same `key:value` spelling the runs page filters on), so a cloned run's tags arrive editable rather than dropped. a line that is not tags disables the launch instead of quietly dropping the part that could not be read

- **run keys make a sensor launch effectively-once.** `RunRequest::new("publish").params(..).key("2026-08-09")` launches at most once per key, ever, for that sensor: the loop skips a claimed key rather than launching it, and the tick counts it under `skipped`. sensors are at-least-once by design (a partial launch failure replays the whole batch and so does a failed cursor write) which is defensible, but it put deduplication on every caller
- the key is claimed **in the same transaction that creates the run**, not before it and not with a delete on the failure path. a key recorded for a run that never launched drops that work forever and nobody notices, which is strictly worse than the duplicate a key exists to prevent; insert-then-delete leaves exactly that window open and a transaction does not. tested on the failure path, not only the happy one
- effectively-once **per sensor**: keys are scoped to the name that used them, two sensors may use the same string for different things, and a keyless request is unchanged at-least-once. keys are never collected on their own (a daily-keyed sensor writes a row a day) so `retention_days(n)` prunes them on the same cutoff it prunes runs on
- `RunRequest` is a builder (`new` / `params` / `key`). the struct-literal form no longer constructs, since the new field would have to be spelled out at every call site; the four in this repo moved to the builder
- schema v11: the `sensor_run_keys` table, plus `sensor_ticks.skipped` and `sensor_ticks.duration_ms` for the parts that follow

- **due sensors evaluate concurrently**, at most 8 at a time. the loop evaluated in sequence, so one closure blocking on a dead endpoint delayed every sensor and every probe behind it: the failure mode where a fifteen-second sensor quietly becomes a fifteen-minute one. the bound is there because "every due entry at once" is how a hundred sensors become a hundred concurrent api calls
- **two evaluations of the same sensor never overlap.** a sensor whose previous evaluation is still going is skipped for that turn rather than queued behind it: a queued second evaluation could commit a cursor over a newer one. the skip is a `skipped` tick, once per stall, not once per turn, so a wedged sensor cannot bury every other sensor's history under its own
- `Sensor::timeout(d)` and `RunStatusSensor::timeout(d)`, 60s by default and for every probe. on expiry the evaluation is abandoned, the tick records the timeout, and the staged cursor is not committed
- abandoning is not stopping, and it is documented as exactly the limit ops have: an `.await` is where an abandoned evaluation goes away, and a closure blocking between await points keeps its thread until it returns. `SensorCtx::is_cancelled()` is the cooperative half, mirroring `OpCtx::is_cancelled`: true once the deadline has passed, cheap to poll between chunks of blocking work

- **a failing sensor backs off.** the gap doubles from the sensor's own interval to a 15 minute cap, with jitter, and the first success collapses it straight back. an endpoint down for an hour does not need polling every five seconds, and hammering it is how one broken sensor becomes a log flood and a rate-limit ban. built on the existing `capped_exponential` / `full_jitter`, not a second implementation
- the floor doubles along with the ceiling, so the wait genuinely lengthens rather than lengthening on average; a sensor whose interval already exceeds the cap is left alone rather than sped up
- `GET /api/sensors` gains `next_eval` and `consecutive_failures`, and the sensors table shows when each one is next due with a `backing off` tag on the degraded ones: a sensor that is failing should not read as merely slow. both live in memory, so a restart starts everything fresh

- **paused state fails closed**, closing a review finding: a store read that failed while determining pause state used to leave the sensor (and the schedule) reading as *not* paused, which is an administrative stop failing open. a sensor now holds its turn and a scheduler pass now fires nothing and moves no cursor, both with a warning. a missed occurrence is recoverable (the next pass's catch-up sees it and honours the flag it can read by then) and a launch nobody asked for is not
- deliberately narrow: launching is still at-least-once, and nothing about which fires are allowed has changed. this is the switch only

- sensor ticks record `duration_ms` beside `launched`, `skipped` and the outcome (v11 columns): between them they answer "is this sensor healthy" without reading the log. a duration that is climbing is a sensor about to hit its timeout, and `launched: 0, skipped: 3` is a different fact from launching nothing. `duration_ms` is 0 on a `skipped` tick, which records a turn that was never taken
- the sensors table shows the last tick's launched, skipped and duration alongside when each sensor is next due

- run-status sensors: `RunStatusSensor::new("chain", |ctx, run: RunSummary| ..).on([RunStatus::Success]).for_job("orders_etl").every(..)`, registered with `Hestan::run_sensor`, is "when job A succeeds, run job B". built as a **third source on the existing sensor loop** exactly as probes are a second: registered as `run:{name}`, so it shares the interval handling, pausing, tick history and cursor column with everything else. no fourth loop and no second set of concepts
- the closure is handed a `RunSummary` (id, job, status, trigger, started_at, finished_at, error), not the internal `Run`: what a chain needs is which run it was and how it went, and a small public struct is a promise that can be kept
- its cursor is the last terminal run it read, as `{finished_at, id}`, ordered and compared as a pair, so two runs finishing in the same instant are neither skipped nor seen twice. each evaluation reads the runs past it, calls the closure once per match, launches, and commits the cursor only after every launch succeeded: the same at-least-once contract user sensors have, documented as such
- the cursor covers every run *read*, not every run matched, so a filtered-out failure is consumed rather than re-read forever, and a page is capped at 200 runs so a sensor resumed after a long pause drains over a few ticks. a new run sensor seeds from the newest terminal run and chains nothing first time: it is there for what happens next, not for the run log it was added to
- **a chain can feed itself**, and nothing stops you: "re-run until a condition holds" is a real thing to want. `for_job`, the status list, or a check inside the closure has to break the cycle; documented, with a test showing a self-chaining sensor that a filter stops
- `GET /api/sensors` gains `filter` (the job and terminal statuses a run sensor watches, null for a user sensor or a probe) and run sensors appear in the existing sensors table in the ui with no new page

- **a durable scheduler cursor.** every `(job, expr)` pair records the newest occurrence it has accounted for (fired, skipped, held or dropped) in `schedules.cursor`. computing fires relative to *now* meant downtime was invisible: a process dead from 08:00 to 10:30 came back and simply never knew 08:00, 09:00 and 10:00 had existed. with a cursor, everything strictly between it and now is the missed set, and that set is what a policy can act on
- missed-fire catch-up, per schedule: `Catchup::Skip` (the default, and exactly what the scheduler did before), `Catchup::One` (the most recent missed occurrence only), `Catchup::All { limit }` (each one, oldest first, capped). past the cap the oldest are dropped and the drop is recorded: a `skipped` tick at the oldest dropped occurrence carrying `catch-up cap 24: dropped 9 missed occurrences up to ...`, never a silence
- **the same cursor makes deferred fires durable**, which closes a known review finding: `Overlap::Queue` held its intention in a process-memory `HashMap` that a restart discarded. the pending queue is now the tick log (a `deferred` tick with no later tick for the same occurrence is a fire still waiting) so a fire held when the process died is still held when it comes back. one mechanism, both problems
- caught-up fires **queue rather than overlap**: the first launches if the job is free and the rest drain one at a time. overlap policy governs a live fire landing on a busy job; catch-up governs occurrences the process was never there for, and firing 24 hours of backlog at once is not what "catch up" means
- `runs.scheduled_for` and `ctx.scheduled_for()`: the cron occurrence a run stands for, not the clock it started at. without it a caught-up run has no idea which logical time it represents, and catch-up is useless for anything that pulls data *for* an hour. null on a manual launch, retry, resume, build or sensor fire
- the schedule surface grew past what positional arguments can carry, so there is a builder: `Schedule::new(job, cron).tz(..).params(..).catchup(..)` with `Hestan::add_schedule`. `schedule`, `schedule_tz`, `schedule_with` and `schedule_tz_with` all build one with the defaults filled in and are unchanged
- a paused schedule advances its cursor over the gap without firing, and drops any fire it was holding: pause means stop, including the catch-up. resuming a schedule paused for a week must not fire a week of backlog
- `GET /api/schedules` and job summaries gain `catchup` and `cursor`; run json gains `scheduled_for`. the job page shows a non-default catch-up policy beside the timezone, and the run page shows which occurrence a run is for

- freshness policies: `Asset::new(..).fresh_within(Duration::from_secs(3600))` and `Job::builder(..).fresh_within(..)` declare how old the latest success may get. the verdict is `Freshness::{Fresh, Late { by }, Never}`, computed at read time from the latest successful materialization (assets) or run (jobs). `never` is deliberately not late: a policy caps how old a success may get, and something with no success has no age to measure
- on a partitioned asset the policy applies **per key**: the asset is late as soon as any one key is, measured from the oldest key's build. keys that were never built are skipped rather than counted late, for the same reason `never` is not late
- a declared policy **replaces** the cron-derived `overdue` heuristic rather than sitting beside it. both fields stay on `GET /api/jobs`, and `overdue` is always false once `freshness` is non-null: two answers to "is this job behind" is one answer too many
- **the alert is the point, not the badge.** `Hestan::on_late(hook)` fires a `LateEvent {kind, name, late_by, last_success}` when something crosses from fresh to late, once per crossing, not once per poll, so a job late for a week pages once. the last-notified state lives in `freshness_state` (v10), so a restart does not re-announce a crossing, and a recovery clears the row so the next relapse is news again
- hooks go out through the same blocking-safe dispatch `on_failure` uses, and `notify::webhook` / `notify::slack` now serve either event: the call sites are unchanged, and which event a helper is built for is inferred from the hook it is handed to
- a checker task runs beside the scheduler on a 60s tick, started only when something declares a policy. `serve` runs it; `run_once` does not
- `GET /api/late` lists everything currently late in exactly the shape `on_late` hands its hooks; `freshness: {status, late_by_secs, last_success}` lands on job and asset summaries. the ui tags late jobs and assets and counts them on the overview statline
- new page: [docs/freshness.md](docs/freshness.md): policies, the three states, `on_late`, and how a policy relates to `overdue` and to staleness
- schema v10: the `freshness_state` table, plus `schedules.cursor`, `schedules.catchup` and `runs.scheduled_for` for the parts that follow

- backfills: `POST /api/assets/{name}/backfill {"from", "to", "only_missing"}` records a request to materialize a range of one asset's partitions and launches it. the range resolves against the key set at the moment it is made and is then fixed: a daily set grows, and a backfill should build what it was asked for. `only_missing` (default true) drops the keys that are already fresh
- it launches **in chunks of `Partitions::build_limit`**, one run at a time: the first goes out immediately and each next one starts as the previous finishes, so a 400-day range is not 400 instances at somebody's api at once. each chunk is an ordinary build run, so the run page, gantt, cancel and events all apply
- the `backfills` table (v9) records `asset, from_key, to_key, partitions, run_ids, total, launched, created_at, finished_at, status`. status derives from the runs: `running` between chunks, `complete` when the last succeeded, `failed` when one failed (chunking stops), `canceled` when one was canceled or the backfill was. a range that resolves to nothing is `complete` on arrival rather than a 400
- `GET /api/backfills?limit=`, `GET /api/backfills/{id}` (with its runs), `POST /api/backfills/{id}/cancel`. one backfill per asset at a time (a second is a 409) and no cross-asset backfills. a chunk also waits on the existing one-build-at-a-time gate rather than overlapping an active assets run
- the assets page gains a backfills section: asset, range, launched/total, status glyph, a link to the chunk running now and a cancel action while it is

- partitioned assets: `Asset::partitioned(Partitions::daily("2026-01-01"))` materializes an asset once per key instead of once: `Partitions::daily`, `::hourly` and `::keys([..])`, with `ctx.partition()` handing the body its key. materializations, fingerprints, history, checks and metadata all key on `(asset, partition)`
- **a build is a fan-out.** the lowered `assets` job gains an external `partitions:{asset}`, the asset's op expands over it, and one instance per target key runs as `{asset}[{key}]` through the machinery mapped ops already use. no second expansion path, so `max_parallel`, pools, retries, cancellation and per-instance rows come along untouched. `Op` instances are now named by their element where an op asks for it, rather than always by index
- dependencies between partitioned assets are **identity mapping only**: the same key, read from the store at `(dep, key)` so it means one thing whether the upstream partition was rebuilt this run or was already fresh. partitioned on unpartitioned is fine; **unpartitioned on partitioned is refused at build** (that needs an aggregation this phase does not define) and two partitioned assets in a dep relationship must use the same kind of key set
- a build with no keys named targets the missing or stale ones, newest first, capped by `Partitions::build_limit` (default 31), so an unbounded daily range cannot start a thousand instances by accident. `POST /api/assets/{name}/build` takes an optional `{"partitions": [..]}` to name them outright; an unknown key is a 400
- a probe fingerprint change marks **all** partitions of a descendant stale. that is not a special rule: an unpartitioned dep is read whole, so every key's recorded input disagrees at once. crude but honest, and documented as the current one
- `GET /api/assets` reports a partitioned asset as `partitions: {total, materialized, stale, missing}` instead of a fingerprint; `GET /api/assets/{name}/partitions` is one row per key for the grid, and `history`/`checks` take `partition=`
- the asset detail panel gains the **partition grid**: one cell per key, newest first, solid materialized / hatched stale / hollow missing, hovering for key, fingerprint and build time, clicking to build that key

- multi-assets: `MultiAsset::new("split_orders", f).produces(["orders_clean", "orders_rejected"])`, registered with `Hestan::multi_assets`, is one op that materializes several assets: the query or pull whose result splits into two tables you do not want to fetch twice. the body returns a json object whose keys are exactly the produced names; a key it did not return, or one nothing declared, fails the op naming the discrepancy
- the asset registry is now asset -> op **N:1**. staleness stays per asset and the op is stale when any asset it produces is; a plan holds the op once however many of its outputs are stale, so building either output is one run of one computation and materializes both
- each produced asset gets its own materialization row, fingerprint (the content hash of that key's value) and history. `ctx.set_fingerprint_of(asset, fp)` and `ctx.meta_of(asset, name, value)` override per output; plain `ctx.meta` describes the computation and lands on the op run, and `ctx.set_fingerprint` covers every output that staged none of its own
- downstream assets depend on the produced *name* (`Asset::from_named("orders_clean")`, read as `ctx.input("orders_clean")`) so an asset moving into or out of a multi-asset does not change anything reading it. a memoized build seeds the whole object the op returns, so whichever key a consumer reads is there
- `GET /api/assets` gains `op`, the op that materializes each asset, and the detail panel shows it when it is not simply the asset's own name
- schema v9: `asset_materializations.partition` and `asset_checks.partition` (null = unpartitioned, which is every existing row), latest lookups and history re-keyed per `(asset, partition)`, and the `backfills` table. `Store::materialization`, `materializations`, `asset_checks` and `record_materialization` take the key; passing `None` is exactly today's behaviour

- materialization history: `asset_materializations` is append-only (schema v8), so every build leaves an entry instead of overwriting the last one. an asset's newest entry is its current state: staleness, memoized seeding and `GET /api/assets` all read exactly what they read before, and the existing suite is the proof. every row an older database holds carries across as that asset's first entry
- `GET /api/assets/{name}/history?limit=` (default 20, clamped 1..=200) returns those entries newest first, each with `changed`: true when its fingerprint differs from the entry before it in time. that flag is the point: a rebuild and a change are different facts, and the keyed table could not tell them apart. the oldest entry counts as changed, and a page's oldest entry is compared against the entry just off the page rather than reported as a change the window invented
- `Store::materializations(asset, limit)` is the history read; the old no-argument `Store::materializations()` is now `latest_materializations()`, which is what it always returned
- history is capped rather than left to grow: at startup each asset is trimmed to its newest 200 entries, `Hestan::asset_history(n)` sets the number, and the newest entry is never trimmed at any `n`. run retention still never touches materializations: a latest value outlives the run that built it, like op state
- clicking an asset row (or its dag node) opens a detail panel listing recent materializations: relative time, short fingerprint, a mark on the ones that changed, and a link to the run

- metadata: `ctx.meta("rows", 1_234)` attaches typed facts to what an op produced: `Meta::{Int, Float, Text, Url, Markdown, Json}`, with the obvious rust types converting on their own. `u64` and `usize` deliberately do not convert; narrowing them silently is a lie waiting to happen
- staged per attempt like `set_state`, so a failed attempt's metadata is discarded whole and what lands is what the attempt that worked reported. committed in the op's terminal write, so an op run never carries facts about work that did not finish
- stored as one json object per op run in `op_runs.metadata`, keyed by name with a tagged value (`{"rows": {"int": 1234}}`); an op that reported nothing stores null rather than `{}`. an asset op's map is written to its materialization as well, so history carries what each build reported and keeps it after retention deletes the run
- `Meta::Markdown` is stored and shown as source. there is no markdown parser in this crate and this does not add one: the variant says which strings are worth rendering elsewhere
- surfaced on `GET /api/runs/{id}` op rows and on `GET /api/assets/{name}/history` entries. the ui renders by type rather than as raw json: numbers right-aligned and tabular, urls as links, text inline, markdown and json in a muted preformatted block
- new page: [docs/metadata.md](docs/metadata.md): metadata is an op feature that assets carry, not an asset feature, so it reads next to op state rather than inside assets.md

- asset checks: `AssetCheck::new(name, asset, |ctx, value| ..)` registered with `Hestan::check(..)` asserts something about the value an asset just materialized, returning `CheckResult::pass()` or `CheckResult::fail(msg)` with `.meta(..)` facts attached. the value arrives owned rather than borrowed: a closure returning `async move` cannot tie its future's lifetime to a `&Value` argument
- checks lower into ops of the existing internal `assets` job, named `check:{asset}:{check}` and depending on the asset's own op. that is the whole implementation: no parallel execution path, so retries, cancellation, the gantt, the event log and `max_parallel` apply because a check *is* an op
- `Severity::Error` (the default) fails the check's op and so the run; `Severity::Warn` records the failed result and lets the op, and the run, succeed. either way the result is recorded before the verdict is acted on, so a failing error check leaves its message and metadata behind
- a failing error check does **not** un-materialize the asset. the materialization was written inside the asset's op, which succeeded, and checks hang off that op rather than feeding it, so downstream assets still see the value. what it does is fail the run that produced it
- **a memoized asset is not re-checked**: a check is in a build plan exactly when the asset it checks is, which follows from checks being ops in the plan. an asset that was seeded rather than rebuilt produced no new value, and its last result still describes the value that is still current. the consequence is that a check added after an asset last built waits for that asset's next build; `POST /api/assets/{name}/build` always rebuilds its target
- naming an unknown asset, naming a source (a check runs on what a build produced), and two checks with one name on one asset are all build errors
- results land in the v8 `asset_checks` table, capped per check by the same `Hestan::asset_history(n)`. `GET /api/assets/{name}/checks?limit=` lists them newest first, and each asset in `GET /api/assets` gains `"checks": {"passed", "failed", "last_run_at"}` from the latest result per name
- the assets table gains a checks cell in the established shape vocabulary (solid glyph all passed, × any failed, nothing at all when no check has recorded anything) and the asset detail panel lists each check's latest status, severity, message and metadata

- pluggable io managers: `IoManager::put` persists an op's output and returns the handle recorded in `op_runs.output`, `get` turns a handle back into the value. the default `Inline` makes an output its own handle, so it is byte-for-byte what hestan has always done; the whole existing suite is the proof
- bundled `FileIo::new(dir)` writes `{dir}/{run_id}/{op}.json` and records `{"$io": "file", "path": ".."}`, an object rather than a bare path so anything reading the run log can tell a reference from a value. nothing is ever cleaned up: retention prunes run rows, not files
- `Hestan::io(manager)` sets the default and `Hestan::io_named(name, manager)` plus `Op::io(name)` select one per op; naming an unregistered manager is a build error rather than a quiet fall back to the run log. `Runner::with_io` is the direct-executor form
- handles are resolved on every path an op reads: downstream inputs as each op is spawned, the array a mapped op expands over, a fan-out's collected instance outputs, resume seeds from an earlier run, and an asset build's memoized seeds. an input that cannot be fetched fails that op rather than reading as "produced nothing"
- `put` runs **before** the success is recorded, so a failed put fails the op: a row claiming success for a value that was never stored would strand the next resume
- `get` is required to be total (it returns anything it did not produce unchanged) because a run seeds source assets `null`, assembles fan-out arrays itself, and can mix managers op by op
- the run page shows the selected op's output on one line, an `$io` handle as the reference it is; job summaries report each op's `io`
- new page: [docs/io-managers.md](docs/io-managers.md)

- resources: `Hestan::resource(name, |ctx| async { .. })` builds a value once at startup and shares it with every op that asks, replacing "capture a client in a closure". `ctx.resource::<T>(name)` hands back the same `Arc<T>` everywhere, and the error distinguishes "no such resource" from "there is one, and it is something else"
- constructors are async and fallible and run **before the store opens**, so one that fails aborts startup with `Error::Resource { name, reason }` and leaves no database behind. they run in declaration order, each handed a `ResourceCtx` holding the ones before it, so a client can lean on the config it reads; declaring one name twice is an error
- `Op::requires(["api"])` declares the dependency, making a resource nobody registered a build error rather than a run that gets halfway. ops may also just ask without declaring
- resources live for the process: no per-run scoping and no teardown hooks in this phase
- `GET /api/resources` reports names and declared types, never values; job summaries carry each op's `requires` and the op inspector shows it
- new page: [docs/resources.md](docs/resources.md)

- reusable graphs: `Graph::builder(name).op(..).input(..).output(..).build()` bundles ops into a unit a job can instantiate more than once with `JobBuilder::graph("clean_a", &clean).after([..])`. purely a build-time transformation: `JobBuilder::build` flattens each instance into ordinary ops named `{instance}.{inner}`, so the executor, resume, fan-out, assets and the ui are untouched
- declared `input` ops additionally wait on the instance's own deps (the only way into a graph, since an inner dep naming something outside is a build error), and anything depending on the instance name is rewired to the op it declared as its `output`. duplicate instance names, an instance colliding with an op, and an unknown or dot-containing `input`/`output` are all `Error::Graph`
- a graph may contain a graph, and `input`/`output` may name a nested instance, which resolves through it; names compound (`s.inner.pages`). self-inclusion is refused rather than flattened forever
- ops keep their own vocabulary through the rename: `dedupe` inside `clean` still reads `ctx.input("parse")`, and a job-level op reads `ctx.input("clean_a")`, the name it wrote in `.after`, rather than the inner op that supplied it
- `OpCtx::inputs()` lists every dep that produced output, name and value, sorted: a reusable graph's input op cannot know what the job called the dep it was handed
- the dag mutes an op's `{instance}.` prefix so a graph instance's ops read as a group

- trigger rules: `Op::when(When::Always | When::AnyFailed | When::AllSucceeded)` decides whether an op runs once its deps settle, so a summary, an alert or a cleanup after a failure is expressible at last: the thing you most want after a failure used to be exactly what got skipped. `AllSucceeded` is the default and is what every op has always meant
- readiness moved from "every dep produced output" to "every dep reached a terminal status"; the rule then decides run vs skip. an op a rule turns down is `skipped` with an `op_skipped` event naming the rule (`skipped by rule any_failed: every dep succeeded`, `data: {"when": ...}`), worded apart from the upstream-failure skip so the log says which happened
- `OpCtx::dep_status(dep)` reports what each declared dep did; `ctx.input(dep)` for a dep that produced nothing stays `None`. deps seeded from outside the run (a resume's reused output, a memoized asset value, a source asset) read as `success`
- skip propagation asks each candidate's rule instead of blanket-skipping: the walk stops at an op that would still run, and at whatever hangs off it, which waits on that op instead. everything reached through plain `all_succeeded` ops is still skipped as one group naming the original root
- a rule applies to a mapped op whole. one admitted when its array never arrived expands into zero instances: no bodies, no rows, `op_expanded` with `instances: 0`, and `[]` downstream
- the run outcome is unchanged: any op failure still fails the run, however many cleanup ops succeed afterwards. there is no "recovered" state
- job summaries report each op's `when`; the dag marks such nodes with a muted `always` / `if failed`, and the op inspector spells the rule out

- dynamic fan-out: `Op::mapped(name, f).over(dep)` runs one instance per element of a dep's json array, discovered at run time. each instance is named `{op}[{i}]`, gets its own `op_runs` row and its element as a typed second argument, and is an ordinary spawned task, so `max_parallel`, pools, retries, timeouts and cancellation apply with no special cases
- a mapped op's output, seen downstream under its plain name, is the array of instance outputs **in element order**, not completion order, and exists only if every instance succeeded: one failure fails the mapped op, skips its downstream and fails the run naming the instance. an empty array is legal: no instances, output `[]`, downstream runs normally
- the mapped op itself gets no `op_runs` row; the instances are the record, and an `op_expanded` event carries the count. resume reuses a mapped op only when it fully succeeded, and otherwise re-expands it whole, since the array can differ on a re-run
- fan-out does not nest, and the build says so: mapping over a mapped op, a mapped op without `.over`, and `.over` on an op that isn't mapped are all `Error::Graph`
- job summaries report each op's `mapped_over`; the dag badges a mapped node with its instance count (`process ×3`), the gantt lists instances as their own rows, and selecting the node lists them with per-instance status

- params on schedules: `Hestan::schedule_with(job, expr, params)` and `schedule_tz_with(job, expr, tz, params)` give a cron entry the params every fire launches with, closing the hole where a job whose ops declare `.params::<P>()` could never fire from cron (scheduled fires used to launch with `{}`, always). `schedule`/`schedule_tz` keep their signatures and mean `{}`
- schedule params are validated **at build**: `serve`/`run_once` run each schedule's params through the same op validators a launch runs, so an impossible schedule is `Error::InvalidParams` at startup instead of a tick that fails every night at 3am. `Job::params_error` is that check store-free, and `POST /api/jobs/{name}/validate_params` exposes it, and the ui's params editor calls it on blur and shows the server's message inline
- `/api/schedules` rows and the job summary's `schedules` carry `params`; the job page shows a schedule's params beside its expression. a deferred (queue-policy) fire keeps the params it was held with
- schema v7: `schedules.params`

- `OpCtx::is_cancelled()` and `OpCtx::cancelled()`: blocking work can poll (or an async op can `select!`) and stop on request, which is the only way blocking work ever stops, since tokio cannot abort it
- honest cancellation: after aborting, a canceled run gives its ops three seconds to actually come back. ops that do are recorded as what really happened; ops that don't are `canceled` with `not observed to stop` and **no** `finished_at`, instead of a finish time for work that is still running
- named concurrency pools shared process-wide: `Hestan::pool(name, limit)` declares one, `Op::pool(name)` takes a permit for the length of an attempt. that is the limit an external api actually imposes, which per-job `max_parallel` cannot express once two jobs overlap. an undeclared pool is a build-time `Error::Graph`; `Runner::with_pools` is the direct-executor form
- `Op::timeout(d)`: a hung attempt fails with `timed out after 30s` and retries normally, instead of running forever, holding its slot, and blocking `Overlap::Skip`. expiry trips the same signal `is_cancelled()` reads
- retries now back off exponentially with full jitter by default (`1s * 2^n`, capped at 30s), so ops that fail together stop retrying in lockstep and re-tripping the same rate limit. `.retry_backoff(base, max)` tunes it; `.retry_delay(d)` still means a fixed pause. http sources jitter their backoff too, and still honor `Retry-After` exactly
- a failed run carries its own `error` (`op {name} failed: {message}` from the first terminal failure, the same pair `on_failure` receives) instead of null. `GET /api/runs` and `/api/runs/{id}` return it, and the run page shows it
- job summaries report each op's `pool` and `timeout_secs`, plus the job's `pools` and their limits; the op inspector shows them
- schema v6: `runs.error`

- resume a finished run instead of redoing it: every op that did not succeed runs again with its downstream, every op that did is seeded from its recorded output
- `Runner::resume`, `Runner::resume_from` (re-run from a chosen op and its downstream, on any terminal run), and `Runner::resume_plan` for the same answer without launching
- `POST /api/runs/{id}/resume` (optional `{"from": [...]}`) and `GET /api/runs/{id}/resume_preview`; resumed runs carry the `resume` trigger and a `resumed_from` link, and the ui offers resume beside re-run plus "re-run from here" on a dag node
- resuming a resume walks the `resumed_from` chain for outputs; a resume is refused when the job's ops no longer match what the chain recorded
- schema v5: `runs.resumed_from`

## 0.1.0-alpha.1

first public release. the api will change; pin an exact version.

- ops wired into job dags, with serde-typed io and params validated before a run is created
- cron schedules in iana timezones, with pause/resume, tick history, and per-job overlap policy
- assets: content fingerprints make staleness provable, and builds materialize only the stale subgraph
- sensors with persisted cursors; asset probes run on the same loop
- run cancellation, per-op watermark state, and failure hooks (webhook and slack helpers behind the `http` feature)
- http sources: one builder lowers a rest endpoint into fan-out ops with http-aware retries
- sqlite run log with versioned migrations, crash recovery on boot, and optional retention
- embedded web ui: runs timeline, per-op gantt, asset graph, command palette
