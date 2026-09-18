# Storage

Hestan supports SQLite and PostgreSQL through the same `Store` API.

## Choosing one

|  | sqlite | postgres |
| --- | --- | --- |
| services to run | none | one |
| processes that can share it | any number, on **one host** | any number, on any number of hosts |
| how a claim is decided | one writer at a time, by the file lock | a row lock, `SKIP LOCKED` |
| feature | on by default | `--features postgres` |

sqlite is not the lesser option and is not deprecated. for one process, or for
several on one host (which is the compose example and a great many real
deployments), it is the right answer and the one with nothing to operate. reach
for postgres when the workers have to live on more than one machine, and not
before.

whichever it is, the schema is the same schema, the api is the same api, and
the same test suite runs against both; see
[development](development.md#the-store-suite-runs-twice).

## Configuring it

```rust
Hestan::new().db("hestan.db")                                // a sqlite file
Hestan::new().db("postgres://user:pw@db.internal/hestan")    // a postgres server
```

`Hestan::db` takes either. a target beginning `postgres://` or `postgresql://`
is a url and anything else is a path, and that one string is what an
[isolated op](isolation.md)'s child process and every
[queue worker](scaling.md) is handed, so all of them reach the same database.
without `--features postgres` a url is refused by name (`unsupported
database: postgres://…`) rather than opened as a very strange filename.

directly, the two constructors are `Store::open(path)` and
`Store::connect(url)`. `Store::open(":memory:")` gives a throwaway store for
tests, private to the connection that made it.

## sqlite

File databases use WAL mode and a five-second busy timeout. Each process holds
one mutex-protected connection. Run claims use compare-and-set inside an
immediate transaction, allowing multiple Hestan processes on one host.

Use PostgreSQL when workers need to share a store across hosts. Isolated ops
need a persistent database accessible to their child process.

## Postgres

Enable `--features postgres` and pass a PostgreSQL URL. Compared with SQLite:

- Auto-increment keys use `BIGSERIAL`; integer columns use `BIGINT`.
- Timestamps remain RFC3339 text and booleans remain integers.
- Text columns use `COLLATE "C"` for consistent ordering across backends.
- A `schema_version` table replaces SQLite's `PRAGMA user_version`.

Fresh-store creation and forward migrations are transactional. An advisory
lock serializes startup schema work. A binary refuses a newer schema than it
understands.

Each process uses one connection, with no pool or automatic reconnect. Restart
the process after a lost connection. The synchronous `Store` API drives the
PostgreSQL client on a separate runtime.

The connection does not support TLS. Use a Unix socket, a trusted network or
a proxy that provides transport protection.

## Schema

seventeen tables. `trigger` is a reserved word in sqlite, hence the quoted
column name in the schema and every statement that touches it.

```sql
CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    job TEXT NOT NULL,
    status TEXT NOT NULL,
    "trigger" TEXT NOT NULL,
    params TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    resumed_from TEXT,
    error TEXT,
    scheduled_for TEXT,
    tags TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    claimed_by TEXT,
    claimed_at TEXT,
    lease_until TEXT,
    plan TEXT,
    actor TEXT,
    replay_of TEXT,
    build TEXT
);
CREATE INDEX runs_job_created ON runs(job, created_at DESC);
CREATE INDEX runs_queue ON runs(status, claimed_by, priority DESC, created_at);

CREATE TABLE op_runs (
    run_id TEXT NOT NULL,
    op TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    finished_at TEXT,
    output TEXT,
    error TEXT,
    metadata TEXT,
    pid INTEGER,
    inputs TEXT,
    PRIMARY KEY (run_id, op)
);

CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    op TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'log',
    data TEXT,
    subject_kind TEXT NOT NULL DEFAULT 'run',
    subject TEXT,
    actor TEXT
);
CREATE INDEX events_run ON events(run_id, seq);
CREATE INDEX events_subject ON events(subject_kind, subject, seq DESC);

CREATE TABLE schedules (
    job TEXT NOT NULL,
    expr TEXT NOT NULL,
    tz TEXT NOT NULL DEFAULT 'UTC',
    paused INTEGER NOT NULL DEFAULT 0,
    params TEXT NOT NULL DEFAULT '{}',
    cursor TEXT,
    catchup TEXT NOT NULL DEFAULT 'skip',
    PRIMARY KEY (job, expr)
);

CREATE TABLE schedule_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job TEXT NOT NULL,
    expr TEXT NOT NULL,
    scheduled_for TEXT NOT NULL,
    fired_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    run_id TEXT,
    error TEXT
);
CREATE UNIQUE INDEX schedule_ticks_fire
    ON schedule_ticks(job, expr, scheduled_for) WHERE outcome = 'fired';

CREATE TABLE op_state (
    job TEXT NOT NULL,
    op TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job, op)
);

CREATE TABLE asset_materializations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,      -- not unique: this is append-only history
    partition TEXT,
    fingerprint TEXT NOT NULL,
    inputs TEXT NOT NULL,     -- json map: dep name -> consumed fingerprint
    value TEXT,               -- what the io manager returned; null for sources
    run_id TEXT,              -- null for probe-written source rows
    built_at TEXT NOT NULL,
    metadata TEXT
);
CREATE INDEX asset_materializations_asset
    ON asset_materializations(asset, partition, id DESC);

CREATE TABLE asset_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    partition TEXT,
    check_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,     -- passed | failed
    severity TEXT NOT NULL,   -- warn | error
    message TEXT,
    metadata TEXT,
    checked_at TEXT NOT NULL
);
CREATE INDEX asset_checks_asset ON asset_checks(asset, partition, id DESC);

CREATE TABLE backfills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    from_key TEXT NOT NULL,
    to_key TEXT NOT NULL,
    partition_keys TEXT NOT NULL,  -- json array: the keys it resolved to
    run_ids TEXT NOT NULL DEFAULT '[]',  -- json array, one per chunk launched
    total INTEGER NOT NULL,
    launched INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL      -- running | complete | failed | canceled
);
CREATE INDEX backfills_asset ON backfills(asset, id DESC);

CREATE TABLE sensors (
    name TEXT NOT NULL PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    cursor TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE sensor_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    outcome TEXT NOT NULL,    -- fired | error | skipped
    launched INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE TABLE sensor_run_keys (
    sensor TEXT NOT NULL,
    run_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    launched_at TEXT NOT NULL,
    PRIMARY KEY (sensor, run_key)
);

CREATE TABLE launch_keys (
    launch_key TEXT NOT NULL PRIMARY KEY,
    job TEXT NOT NULL,
    params_hash TEXT NOT NULL, -- sha-256 of the params as stored
    run_id TEXT NOT NULL,
    launched_at TEXT NOT NULL
);
CREATE INDEX launch_keys_run ON launch_keys(run_id);

CREATE TABLE store_copy (
    only_row INTEGER PRIMARY KEY CHECK (only_row = 1),
    taken_at TEXT,             -- when hestan took the copy; null if it did not
    taken_from TEXT,           -- the store it was copied from
    settled_at TEXT            -- when `hestan resettle` handed its claims back
);

CREATE TABLE presets (
    job TEXT NOT NULL,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job, name)
);

CREATE TABLE op_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    op TEXT NOT NULL,
    attempt INTEGER NOT NULL,  -- which attempt of that op printed it
    at TEXT NOT NULL,
    stream TEXT,               -- stdout | stderr, for subprocess capture
    level TEXT,                -- info | warn | error, for a captured event
    target TEXT,               -- the event's module path
    message TEXT NOT NULL
);
CREATE INDEX op_logs_run ON op_logs(run_id, op, id);

CREATE TABLE notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,        -- which event shape payload holds; "run" today
    payload TEXT NOT NULL,     -- the event, as the hook will receive it
    created_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,      -- when it is next due; null once nothing will
    delivered_at TEXT,
    last_error TEXT
);
CREATE INDEX notifications_due ON notifications(next_attempt_at)
    WHERE delivered_at IS NULL;
CREATE INDEX notifications_delivered ON notifications(delivered_at);

CREATE TABLE decider (
    only_row INTEGER PRIMARY KEY CHECK (only_row = 1),
    term INTEGER NOT NULL DEFAULT 0,    -- +1 on every acquisition, never on a renewal
    claimed_by TEXT,                    -- the instance id holding it
    claimed_at TEXT,
    lease_until TEXT
);
```

`notifications` is empty unless
[`durable_notifications()`](notifications.md#durable-delivery) is on. the row
is written in the same transaction as the run's terminal row, which is the
whole of what durable delivery is: written after it, a crash in the gap loses
the alert about the failure the alert existed to report, and nothing records
that it was owed. `next_attempt_at` carries the state (set and undelivered is
`pending`, **null** and undelivered is given up on), so a row is inserted due
now rather than null, and giving up clears it, which keeps a permanently
failing notification out of the delivery scan while leaving it visible with
the error that stopped it. the partial index is that scan and nothing else:
the pending rows are a handful and the delivered ones are the table.

`op_logs` is what an op *printed*, as opposed to what hestan said about it in
`events`. it is a table of its own precisely because a chatty op would
otherwise bury the eight events that describe what the run did. exactly one
half of the middle three columns is filled per row, and which half says where
the line came from: `stream` for an [isolated op](isolation.md)'s pipe, which
has no levels and no targets, and `level`/`target` for a `tracing` event
captured by the [`capture` layer](logs.md), which was never on a pipe. rows are
capped per attempt, at 1 MiB and 10,000 lines by default; see [logs](logs.md).

`presets` holds named parameter sets ([launching](launching.md#presets)).
they are runtime data, not part of a job definition: `Hestan::preset` seeds
one at build with an upsert and the launchpad writes others beside it, so the
table is the only place the two can meet. that is also why nothing sweeps it:
unlike `schedules`, which mirrors the code exactly, a preset whose declaration
was deleted stays until somebody deletes the preset. `created_at` survives a
rewrite, so it means when the preset first appeared rather than when the
process last booted.

`sensor_run_keys` is what makes a keyed sensor request
[effectively-once](sensors.md#run-keys). the row is inserted in the same
transaction that creates the run it names, never before and never after: a key
recorded for a run that was never created would drop that work forever, and
silently, which is worse than the duplicate the key exists to prevent. the
primary key is the claim, so two evaluations racing the same key still launch
one run. `run_id` is a record of which run took the key rather than a foreign
key: retention deletes runs and leaves keys, which is the right way round.

`launch_keys` is what makes a plain launch
[idempotent](launching.md#launching-once), and it is the same mechanism
`sensor_run_keys` is, aimed at a caller rather than at a sensor. the row is
inserted in the same transaction that creates the run it names, and the primary
key on `launch_key` alone is what refuses a second launch: keyed per job
instead, one key would mean two things on two jobs and hand somebody a second
run without either caller seeing it. `params_hash` is a sha-256 of the params
**as stored**, so two launches differing only in a [secret](secrets.md) param
hash the same and the second is answered with the first one's run. unlike
`sensor_run_keys`, these go **with** the run they name, through
`launch_keys_run`: a key that outlived its run would hand a retrying caller an
id nothing can be looked up under.

`store_copy` is empty on every database but a copy. `Store::backup_to` writes
the row into the copy it takes, which is what lets a deployment refuse to come
up on a restored run log before it acts on claims that describe another
machine; `Store::resettle` sets `settled_at`. only a copy hestan took carries
it, and [backup and recovery](backup.md) is the whole of what that means.

`runs.error` is the run's own failure summary: the first op that terminally
failed, as `op {name} failed: {message}`, written in the same statement as
the terminal status. it is stored rather than derived from `op_runs` on read
for three reasons: only the executor knows which failure came *first*
(`op_runs.finished_at` is a proxy that ties and lies under retries), the run
list is polled by the ui and a correlated subquery per row would be paid on
every poll, and a stored column is what keeps the run row and the
[`on_failure` hook](notifications.md) saying the same thing by construction.
runs and their op runs are pruned together, so the two can never drift apart.

`op_runs` rows are usually written in one transaction with the run, but not
always: a [mapped op](concepts.md#dynamic-fan-out) has no row of its own, and
one row per instance (`fetch_page[0]`, …) is inserted mid-run, the moment the
expansion knows how many there are. those inserts happen on the run's own task
and before the instances are spawned, so a row can never land after the run's
terminal status write, and a cancel or a skip always has something to write
to. an instance's name is an op name everywhere else in the schema, including
`op_state` and the event log.

an `op_runs` row with a terminal status and a null `finished_at` is not a
bug: it is a [canceled op that was never observed to stop](concepts.md#cancellation),
and the missing timestamp is the record refusing to invent one. anything
computing durations (op stats, the gantt) skips those rows.

statuses, triggers, levels, kinds, and outcomes are stored as their
lowercase/snake_case string forms (`success`, `type_check_failed`, ...).
params, outputs, and event data are json text. timestamps are rfc3339 text,
always written from utc values, which makes plain string comparison on
`created_at` correct; the runs api leans on that for its `since`/`before`
filters. run listing orders by `created_at DESC, id DESC` (run ids are uuid
v7, so the tiebreak follows creation order) and pages on the composite
`(created_at, id)` cursor (`before` plus `before_id`), so runs created in
the same millisecond can't be dropped or repeated across pages; `before`
alone keeps the old timestamp-only exclusive compare.

## Migrations

SQLite records its schema in `PRAGMA user_version`; PostgreSQL records a schema
stamp in the store. Opening a store applies pending migrations and updates the
stamp in one transaction. Failure leaves the database unmigrated, with its rows
intact. A binary refuses to open a schema newer than it understands.

Existing materializations retain their history, and rows created before
partition support remain unpartitioned. New optional fields retain their absence
on older rows. Migration SQL lives in `src/store.rs` and `src/pg.rs`.

Take a [backup](backup.md) before upgrading. Downgrades require restoring that
backup; there are no down migrations.

### One fire per occurrence

A unique index prevents two `fired` ticks for the same
`(job, expr, scheduled_for)`. The [scheduler](scheduling.md#one-fire-per-occurrence)
records the tick and creates its run in one transaction, so a refused tick
launches nothing.

the index is **partial, over `fired` alone.** the tick log is also the queue: a
`deferred` tick with no later tick for the same occurrence is a fire still
waiting, and that occurrence legitimately holds a `deferred` tick and then the
`fired` tick that drained it. what has to be unique is the decision that
launched something. `deferred`, `skipped` and `error` ticks stay
unconstrained, and a duplicate among them is a duplicate line in a log rather
than a duplicate run.

When adding this constraint to an older store, migration keeps the earliest
`fired_at` for each occurrence and removes duplicate fired ticks. It logs a
warning with the count. Runs launched by those duplicates remain in history
and their effects are not undone; inspect the run log for duplicate executions.

For older event tables, making `run_id` nullable requires SQLite to rebuild and
copy the table. PostgreSQL changes the constraint without copying rows, though
index creation still reads the table. Allow time for this work on large stores.

Stores created before schema stamping are recognized by their existing `runs`
table. Opening them preserves existing rows and gives old events the `log` kind.

## Crash recovery

`serve`, `work` and `run_once` sweep the database at startup, before anything
new launches (an [op subprocess](isolation.md) does not: it owns nothing and
is here to run one op). the sweep is **lease-aware**, and that is the whole of
how several processes share one file safely:

- **claimed, lease still good**: somebody is executing it and it is not this
  process. left entirely alone.
- **claimed, lease expired**: its claimer stopped renewing. swept.
- **`running` with no claim**: written before the queue existed, by a process
  that is gone. swept.
- **`queued` with no claim**: not a casualty, [the queue](scaling.md). left
  for a dispatcher to claim.

a swept run's `running` op runs become `failed` with error
`interrupted: process exited`, its `pending` op runs become `skipped`, a
`run_failed` event (`run interrupted: process exited`) is appended, and the
run itself is marked `failed` with a finish time and that same message as its
`error`. terminal runs are untouched. constructing a `Runner` directly skips
the sweep: it belongs to process startup, not to the executor.

the sweep only catches a claimer that was already gone when this process
started. one that dies while everything is up is caught by the same test on a
loop: every process renews the leases it holds every 15 seconds and takes back
anything nobody has renewed for 60, failing it or requeueing it per
[`Reclaim`](scaling.md#claims-and-leases).

**a restored database is the one case this gets wrong on purpose.** believing
leases is right on a live store and wrong on a copy, where a claim with time
left on it names a process that is not executing anything here. so a copy is
handled by [`hestan resettle`](backup.md#resettle) instead, and a deployment
refuses to come up on a copy that has not had it. see
[backup and recovery](backup.md).

## When the database will not take a write

Critical writes receive bounded retries for recoverable backend errors;
[execution write guarantees](concepts.md#what-hestan-promises-about-writes)
describe the retry and abandonment behavior. A lost connection may leave an
unknown commit outcome and is not blindly retried.

PostgreSQL connections do not reconnect: restart the affected process. Runs
left behind are handled by lease recovery. While writes fail, the process
stops claiming work and `/api/health` reports `ok: false` with failure counts.
`hestan doctor` can independently test whether the store accepts a write lock.

## Retention

by default nothing is ever deleted: runs, op runs, events and captured output
accumulate for as long as the file exists. `Hestan::retention(policy)` opts in,
and `Retention` says how much history to keep:

```rust
Hestan::new()
    .retention(Retention::days(30).keep_last(20).failed_days(90))
    .job(Job::builder("audit_export").retention(Retention::days(365)).op(export).build()?)
```

| knob | what it says |
| --- | --- |
| `Retention::days(n)` | delete a terminal run `n` days after it was **created** |
| `.keep_last(n)` | hold the newest `n` finished runs of the job back from that cutoff, whatever their age |
| `.failed_days(n)` | a longer age for runs that failed or were canceled; without it they age like successes |

`retention_days(n)` is still there and still means `Retention::days(n)`.
`JobBuilder::retention` overrides the global policy for one job entirely: it
is that job's whole policy, not an addition to the deployment's.

the age is measured from `created_at` rather than from the finish, so a run
that sat on the queue for a week ages while it waits, which is what "keep 30
days" means to whoever asked for it. `failed_days` is worth reaching for: a
successful run is noise a week later, and the failure you want next quarter is
the one about to go.

### The combination rule

**a run is deleted only when every knob would delete it.** `days(7)` with
`keep_last(50)` keeps a run that is eight days old if it is among the last
fifty, and keeps the last fifty only until they are eight days old. whichever
rule holds it back wins. keep-if-either is the conservative direction, and the
other reading silently deletes history you find out about afterwards.

that also means `keep_last` on its own deletes nothing. with no age policy
there is nothing for it to hold anything back *from*, and reading it as "delete
everything past the newest n" would make an unconfigured `Retention` empty a
database.

a run that has not finished is **never** pruned, whatever its age: a queued run
older than the cutoff is a queue problem, not a retention one. a
[reclaimed](scaling.md) run is back on the queue rather than terminal, so what
its first claimer captured is still there for the second one. `op_state` is
never touched either: watermarks outlive their runs, so a job that fires
rarely keeps its cursor even after every run that wrote it is gone. an asset's
latest materialization is the same: it survives the run that built it being
retired, so a materialization's `run_id` can point at a run retention has since
deleted.

### The run an asset's value is inside

one more thing a policy does not delete, and the one worth knowing before you
choose a number. an [asset](assets.md) whose value goes through an
[io manager](io-managers.md) has that value inside the run that built it, and
the sweep takes what a run wrote when it takes the run. so **a run that an
asset's current materialization still reads is held back**, rows and files
together, until something rebuilds the asset. the next sweep after that takes
it like any other run past its policy.

pruning it instead would leave the row pointing at nothing, and the next build
would either fail on a hole or silently redo work somebody paid for. but it
does mean a policy no longer strictly bounds what is here: `days(30)` is "no
run older than thirty days, except the ones holding a value something still
reads", and an asset built a year ago and never rebuilt keeps its run forever.
`hestan doctor` counts them so a disk filling is not a mystery:

```
note  values     3 run(s) are held back from retention: an asset's current value is what they wrote, and a later build reads it
```

nothing is held back under the default `Inline`, whose values are in
`asset_materializations.value` itself: a deployment that never configured a
manager prunes exactly as it did before.

### The sweep

a sweep runs at startup **and every `Hestan::retention_interval` after it**,
an hour by default. the interval is the point: retention used to run once, at
boot, so a server up for three months pruned nothing after its first second:
the one deployment shape a retention policy is for is the one where it never
ran. the startup sweep stays as well, because a process that runs for an hour
and exits should still tidy up.

**only a process that [decides](scaling.md) sweeps**: `Role::All` or
`Role::Scheduler`. a worker owns none of the history, and one pruning the
scheduler's runs would be data loss nothing reports.

each job is swept in its own transaction, so a run and its children always go
together and a database with fifty jobs in it does not hold the write lock for
the length of all fifty. the cost is one index seek per job rather than one
visit per run: the jobs with runs are walked by a loose index scan over
`runs_job_created`, and each job's doomed rows are a range seek on the same
index.

**what a pruned run wrote goes first.** before the rows are deleted, every
registered [io manager](io-managers.md) is asked to drop each doomed run:
`FileIo` and `ParquetIo` remove `{dir}/{run_id}` whole, and the default
`Inline` has nothing to drop, since its outputs *are* the rows. the order is
the point: a run row is the only record that the run existed, so deleting it
first and crashing in between would leave files nothing could ever name
again. this way round a crash leaves rows pointing at outputs that are gone,
for runs that are already past retention and go on the next sweep. a manager
that fails to drop something is logged and the rows are pruned anyway: a
file left behind is a smaller problem than a sweep that stops. the io
managers' page has [the whole of it](io-managers.md#what-retention-takes),
including what to know before pointing a manager at a directory.

**a pruned run is an unreplayable one**, and that is worth knowing while
choosing a policy rather than afterwards. [replay](replay.md) re-runs ops of
an old run on the values that run recorded, so a sweep that takes the rows and
the files takes the inputs with them: the replay is refused, naming the op
whose input is gone, rather than run on a hole. `failed_days` is the knob that
matters here, since a failure is what anybody replays: `Retention::days(30)
.failed_days(180)` keeps six months of the runs worth re-running and a month
of the ones that worked.

the sweep also takes both kinds of key older than the age cutoff, on one
cutoff rather than two: [sensor run keys](sensors.md) and
[launch keys](launching.md#launching-once). nothing else collects either, and a
sensor keyed by the day or an api called a thousand times an hour would keep a
row per request forever. **with no retention policy configured neither is
pruned**, so a key is honoured for as long as the database lives, which is what
"how long is a launch key honoured" comes to. a launch key also goes with the
run it names, whenever that run is pruned, so it can never hand a retrying
caller an id nothing can be looked up under. delivered
[notifications](notifications.md) older than the cutoff go the same way.
undelivered notifications stay at any age: one that never got through is not
history, it is something outstanding.

three logs are trimmed by the same sweep whether or not a retention policy is
configured, because all of them grow with time rather than with what you keep:
`schedule_ticks` and `sensor_ticks` are each capped at their newest 5000 rows,
and the [events](events.md) that belong to no run at
their newest 50,000. a run's own events go when the run does and always did;
what is new is that an asset built every five minutes writes a row nothing
would otherwise ever collect.
`asset_materializations` is capped *per asset*, and `asset_checks` per
`(asset, check)`, at the newest 200 each (or whatever
`Hestan::asset_history(n)` says), and those two are trimmed at startup. the
newest row of either is never trimmed at any `n`: an asset's latest
materialization is its current state and a check's latest result is what the
asset summary counts ([assets](assets.md)).

### What the build column costs

`runs.build` is nullable text, stored once per run. Storage grows with the length
of the supplied identifier. The column has no index, so filtering by build
scans the run rows, as filtering by tag does. Adding the column does not rewrite
existing rows on either backend; their value remains null.

## What's stored and what stays in memory

job and op definitions are code, not rows: the database records history
(names, statuses, timings, outputs, events), never the dag itself. that's why
a retried run whose job has left the code is a 409, and why the store carries
no job table to migrate when you refactor.

`op_runs.output` holds whatever the op's [io manager](io-managers.md)
returned from `put`. under the default `Inline` manager that is the output
itself, json in sqlite, which is what it has always been; under another it is
a handle (`{"$io": "file", "path": ".."}` for `FileIo`) and the value lives
wherever that manager put it. the write happens before the success row, so a
row never claims success for a value that was not persisted.

`asset_materializations.value` holds the same thing for the same reason: what
the manager returned for the asset's value, which under `Inline` is the value
and under a file manager is the handle its op run already holds: one stored
thing named twice rather than a file and a json copy of it. a row written
before an asset's value went through a manager holds the value itself and is
read back the same way, since a manager hands back what it did not write; no
migration turned those rows into anything. a multi-asset is the exception:
several assets share one output and one handle, so each keeps its own slice.

within a run, dependents are handed handles and resolve them as they are
spawned; a resume and an asset build resolve the handles they seed the same
way. the executor still never reads *outputs* back from sqlite during a run
(it carries them), but it does read them back on a resume, which is where a
pruned run breaks a chain: the rows that held those handles are gone, and so
is what they pointed at, because the sweep took both.

every one of those calls is made on tokio's blocking pool rather than on the
task driving the run, so a manager talking to something slow costs the op it
is persisting and not the ops beside it.

two tables are keyed by names instead of run ids and hold current state
rather than history. `op_state`: one json value per `(job, op)`, upserted
when an op that called `ctx.set_state` succeeds. the success row commits
first, the state second, so a crash between the two re-runs from the old
value rather than skipping a window (the reasoning is in
[op state](state.md)). `sensors`: one cursor per sensor, committed only
after a fully successful evaluation ([sensors](sensors.md)). runs and op
runs come and go; these rows persist until overwritten.

`asset_materializations` is the third of that family and the odd one out: it
is append-only, and an asset's *newest* row is its current state rather than
its only one. each is written inside the asset op just before it reports
success, the mirror image of the op-state order, with the same
at-least-once outcome ([assets](assets.md)). `asset_checks` is append-only
the same way, written inside the check's own op before it decides whether to
fail, so a failing error check records its verdict as well as failing the
run.

writes are ordered for readers. a run row is created in one transaction with
its `pending` op runs and its `run_queued` event, so a visible run always has
its skeleton; the terminal `run_success`/`run_failed`/`run_canceled` event
commits before the terminal status. one deliberate exception:
`ctx.info/warn/error` event writes that fail only log a process-level
warning: a lost log line doesn't fail the op.
