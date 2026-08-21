# Storage

two backends and one schema. sqlite is the default and needs no server;
postgres is for the deployment one file cannot serve.

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

one file, no extra services. file databases run in WAL mode. the store is a
single rusqlite connection behind a mutex, cloneable and shareable across
tasks; one writer is plenty at this scale, and it also means hestan assumes it
is the one *orchestrator* writing (see [embedding](embedding.md)).

there are other writers, and they are hestan's own. an [isolated
op](isolation.md) runs in an op subprocess that opens this same file and
records its result through it, and a [queue worker](scaling.md) is a whole
second orchestrator process pulling runs off the same tables. every connection
therefore carries a five-second `busy_timeout`, so two processes writing at
the same instant wait for each other instead of the second one failing
outright; the claim that decides who executes a run is a compare-and-set in an
immediate transaction, so it does not depend on timing at all.

that is multi-process on one host. it is not multi-node: sqlite is not
reachable over a network, and hestan will not ship a config pretending
otherwise. that is what the other backend is for.

## Postgres

`--features postgres`, then a url. the schema below is the schema, with four
deliberate differences and no others:

- `BIGSERIAL` where sqlite has `INTEGER PRIMARY KEY AUTOINCREMENT`, and
  `BIGINT` where it has `INTEGER`. postgres has two integer widths and this
  schema only ever means the wide one.
- **timestamps stay `TEXT`, rfc3339.** every query compares and orders them as
  strings, and `timestamptz` would change comparison and ordering semantics
  across the whole store for no gain here. the columns hestan reads as booleans
  stay integers for the same reason. a row therefore reads back identically off
  either backend, which is the point.
- **every text column is `COLLATE "C"`.** sqlite compares text byte by byte and
  postgres compares it in the database's collation; on an `en_US.UTF-8`
  database that sorts names and keys in a place byte order does not, so the
  same query would answer two different things. `C` is byte order, which is
  what an opaque id, name or timestamp wants.
- the version stamp is a one-row `schema_version` table, because postgres has
  no `user_version`.

**a fresh database is created at the current version in one statement batch.**
there are no postgres databases in the world that predate this backend, so
there is nothing for the sqlite chain's accumulated steps to migrate and
walking them would be a re-enactment; from here it is forward-only. postgres ddl is
transactional, so an interrupted first boot leaves the database exactly as
found, and two processes booting against the same empty database take turns on
an advisory lock rather than racing to create the same table. a database
stamped with a version newer than the build is refused, exactly as a sqlite
file is.

**it is one connection, not a pool.** a pool would buy parallel statements and
with them reconnection, a second set of failure modes and transactions that no
longer sit where the code around them thinks. sqlite already blocks on one
connection and this matches it deliberately: what postgres is here for is
several *processes* sharing a run log, not one process issuing more statements
at once. the client is async and hestan drives it on a runtime of its own, so
`Store` stays synchronous and no call site changed.

**there is no tls.** the connection is what libpq calls `sslmode=disable`. use
a unix socket, a private network, or a proxy that terminates tls for you. this
is a real limitation and it is written down here rather than implied away.

what the two backends spell differently is nine `AUTOINCREMENT`s that are ddl
and nothing else, four inserts that yield to a row already there, sqlite's
null-safe `IS`, one json walk for the [tag filter](launching.md#run-tags), one
json array append, the placeholder sigil, and the claim itself. every one of
them is named in one place in `src/store.rs`; everything else is the same text
on both.

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
    resumed_from TEXT,                  -- added in v5
    error TEXT,                         -- added in v6
    scheduled_for TEXT,                 -- added in v10
    tags TEXT,                          -- added in v12
    priority INTEGER NOT NULL DEFAULT 0,-- added in v14
    claimed_by TEXT,                    -- added in v14
    claimed_at TEXT,                    -- added in v14
    lease_until TEXT,                   -- added in v14
    plan TEXT,                          -- added in v14
    actor TEXT                          -- added in v18
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
    metadata TEXT,                      -- added in v8
    pid INTEGER,                        -- added in v13
    inputs TEXT,                        -- added in v13
    PRIMARY KEY (run_id, op)
);

CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,                        -- nullable since v17
    op TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'log',   -- added in v2
    data TEXT,                          -- added in v2
    subject_kind TEXT NOT NULL DEFAULT 'run',  -- added in v17
    subject TEXT,                       -- added in v17
    actor TEXT                          -- added in v18
);
CREATE INDEX events_run ON events(run_id, seq);
CREATE INDEX events_subject ON events(subject_kind, subject, seq DESC);

CREATE TABLE schedules (
    job TEXT NOT NULL,
    expr TEXT NOT NULL,
    tz TEXT NOT NULL DEFAULT 'UTC',
    paused INTEGER NOT NULL DEFAULT 0,
    params TEXT NOT NULL DEFAULT '{}',  -- added in v7
    cursor TEXT,                        -- added in v10
    catchup TEXT NOT NULL DEFAULT 'skip', -- added in v10
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
-- added in v20: one fire per occurrence, whoever asks
CREATE UNIQUE INDEX schedule_ticks_fire
    ON schedule_ticks(job, expr, scheduled_for) WHERE outcome = 'fired';

CREATE TABLE op_state (          -- added in v3
    job TEXT NOT NULL,
    op TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job, op)
);

CREATE TABLE asset_materializations (  -- added in v4, rebuilt in v8
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,      -- not unique: this is append-only history
    partition TEXT,           -- added in v9; null = unpartitioned
    fingerprint TEXT NOT NULL,
    inputs TEXT NOT NULL,     -- json map: dep name -> consumed fingerprint
    value TEXT,               -- what the io manager returned; null for sources
    run_id TEXT,              -- null for probe-written source rows
    built_at TEXT NOT NULL,
    metadata TEXT             -- added in v8
);
CREATE INDEX asset_materializations_asset
    ON asset_materializations(asset, partition, id DESC);

CREATE TABLE asset_checks (            -- added in v8
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    partition TEXT,           -- added in v9; null = unpartitioned
    check_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,     -- passed | failed
    severity TEXT NOT NULL,   -- warn | error
    message TEXT,
    metadata TEXT,
    checked_at TEXT NOT NULL
);
CREATE INDEX asset_checks_asset ON asset_checks(asset, partition, id DESC);

CREATE TABLE backfills (               -- added in v9
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

CREATE TABLE sensors (                 -- added in v4
    name TEXT NOT NULL PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    cursor TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE sensor_ticks (            -- added in v4
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor TEXT NOT NULL,
    evaluated_at TEXT NOT NULL,
    outcome TEXT NOT NULL,    -- fired | error | skipped
    launched INTEGER NOT NULL DEFAULT 0,
    skipped INTEGER NOT NULL DEFAULT 0,      -- added in v11: keyed duplicates
    duration_ms INTEGER NOT NULL DEFAULT 0,  -- added in v11; 0 on a skipped tick
    error TEXT
);

CREATE TABLE sensor_run_keys (         -- added in v11
    sensor TEXT NOT NULL,
    run_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    launched_at TEXT NOT NULL,
    PRIMARY KEY (sensor, run_key)
);

CREATE TABLE presets (                  -- added in v12
    job TEXT NOT NULL,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job, name)
);

CREATE TABLE op_logs (                  -- added in v15
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

CREATE TABLE notifications (            -- added in v16
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

CREATE TABLE decider (                  -- added in v21
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

this section is sqlite's chain, which every existing file walks and no
postgres database ever will; see [postgres](#postgres) for why one is created
whole instead.

the schema version lives in `PRAGMA user_version` and `Store::open` migrates
forward on every open. version 1 is the phase-1 schema (`runs`, `op_runs`,
`events` without `kind`/`data`); version 2 adds `events.kind` and
`events.data` plus the `schedules` and `schedule_ticks` tables; version 3
adds `op_state`; version 4 adds `asset_materializations`, `sensors`, and
`sensor_ticks`; version 5 adds `runs.resumed_from`, the link a
[resume](concepts.md#resume) follows back to the run it continued; version 6
adds `runs.error`; version 7 adds `schedules.params`, the params a cron fire
launches with ([scheduling](scheduling.md)), and schedules declared before it
default to `{}`, which is what they always fired with; version 8 rebuilds
`asset_materializations` as append-only [history](assets.md), adds
`op_runs.metadata` ([metadata](metadata.md)) and adds the `asset_checks`
table ([checks](assets.md#asset-checks)); version 9 adds `partition` to
`asset_materializations` and `asset_checks` and re-keys every latest lookup
per `(asset, partition)` ([partitions](assets.md#partitioned-assets)), and
adds the `backfills` table ([backfills](assets.md#backfills)); version 10
adds the `freshness_state` table ([freshness](freshness.md)), `schedules.cursor`
and `schedules.catchup` ([catch-up](scheduling.md#missed-fire-catch-up)) and
`runs.scheduled_for`, the logical time a scheduled or caught-up run stands
for; version 11 adds the `sensor_run_keys` table
([run keys](sensors.md#run-keys)) plus `sensor_ticks.skipped` and
`sensor_ticks.duration_ms`, which existing ticks read as 0 (they were never
measured, and 0 is the only honest thing to say about that); version 12 adds
the `presets` table and `runs.tags`, the flat `{"k": "v"}` map a run carries
([tags](launching.md#run-tags)), null on every run written before it and on
every run launched without any, which reads back as `{}`; version 13 adds
`op_runs.pid` and `op_runs.inputs`, both for [isolated ops](isolation.md) and
both null for every op that runs in this process; version 14 adds the
[queue](scaling.md) columns to `runs` (`priority`, `claimed_by`,
`claimed_at`, `lease_until` and `plan`) plus the `runs_queue` index. every
run written before it reads back as priority 0 and unclaimed, which is what a
run that finished before there was a queue is. `plan` is what a launch decided
the run would execute (`{"ops": [...], "seeds": {...}}`) and is null for a run
of the whole job, which is most of them: it exists because a resume's reused
outputs and an asset build's memoized seeds live in the launching process's
memory, and whoever claims the run may not be that process; version 15 adds
the `op_logs` table ([logs](logs.md)), empty for every run that finished
before there was anywhere to put what an op printed; version 16 adds the
`notifications` table ([durable delivery](notifications.md#durable-delivery)),
which stays empty unless a process asks for it; version 17 makes
`events.run_id` nullable and adds `events.subject_kind` and `events.subject`,
which is what stops the log being only about runs ([events](events.md)):
every existing row is a run event and is stamped `subject_kind = 'run'`, and
`subject` stays null on a run event because the run is already `run_id` and
copying it would rewrite the largest table in the database to say the same
thing twice; version 18 adds `runs.actor` and `events.actor`, the name of the
[identity](auth.md) that asked for a run, a cancel, a pause or a backfill:
null on every row written before it, and null on everything a schedule, a
sensor or a loop did on its own, which is the same thing those rows always
meant; version 19 adds `runs.replay_of`, the run a
[replay](replay.md) re-ran, beside `resumed_from` rather than sharing it,
because a resume continues a run and a replay re-runs one; version 20 adds the
`schedule_ticks_fire` unique index, which is
[one fire per occurrence](#one-fire-per-occurrence) and is the only unique
constraint in this schema that was not already a primary key; version 21 adds
the `decider` table, one row holding the
[deciding lease](scaling.md#the-deciding-lease) and the term it is on.

an older file at any version opens straight into the current one, rows
intact: the v8 rebuild copies every keyed materialization across, where it becomes
that asset's first history entry and stays its current one, and v9 leaves every
existing row with a null partition, which is exactly what an unpartitioned
asset is. every pending step and the version stamp run in one transaction
(sqlite DDL is transactional), so a crash or failure mid-migration leaves the
file exactly as it was found, never half-migrated. a database stamped
with a version newer than the build refuses to open (`db schema v21 is newer
than this build`) instead of quietly writing an older stamp over it.

### One fire per occurrence

`schedule_ticks` had an autoincrement id and no unique index on anything, so
two processes firing the same `(job, expr, scheduled_for)` each inserted a tick
and each launched a run, and nothing refused either. v20 adds the index that
refuses the second one, and the [scheduler](scheduling.md#one-fire-per-occurrence)
records the tick and creates the run in one transaction, so a refused tick
launches nothing.

the index is **partial, over `fired` alone.** the tick log is also the queue: a
`deferred` tick with no later tick for the same occurrence is a fire still
waiting, and that occurrence legitimately holds a `deferred` tick and then the
`fired` tick that drained it. what has to be unique is the decision that
launched something. `deferred`, `skipped` and `error` ticks stay
unconstrained, and a duplicate among them is a duplicate line in a log rather
than a duplicate run.

**the migration is the hazard.** a deployment that has already been running two
schedulers has duplicate `fired` ticks in this table now, and
`CREATE UNIQUE INDEX` over them fails outright. so v20 collapses them first,
keeping the earliest `fired_at` of each occurrence and deleting the rest, and
reports the count at warn level:

```
schema v20: 37 duplicate schedule fires collapsed. more than one process has
been firing the same occurrences against this store. each collapsed tick may
have launched a run of its own: those runs are still in the run log, they still
executed, and deleting a tick does not unlaunch one. check the run log for
scheduled runs that came in pairs
```

that last sentence is the point. **the collapse does not undo anything.** each
deleted tick may have launched a run; that run executed, wrote what it wrote and
is still in the run log. the count is how many times this deployment fired an
occurrence twice, and the runs are the thing to go and look at. the report is a
`tracing` warning emitted from `Store::open`, before there is a store to write
an event to, so a process with no subscriber installed will not see it.

**v17 is the one step where the two backends do genuinely different amounts of
work,** and it is worth knowing which way round. sqlite has no
`ALTER TABLE ... ALTER COLUMN`, so dropping a `NOT NULL` means rebuilding the
table and copying every row. on a database with a year of events in it that is
the expensive part of the upgrade, and it happens inside the one transaction
like everything else, so an interrupted one leaves the file as it was found.
postgres drops the constraint and adds two defaulted columns in the catalog and
touches no row at all; only the new index reads the table. a large postgres
database migrates in about as long as it takes to build one index, and a large
sqlite one takes as long as it takes to copy the table.

postgres has a forward chain of its own as of v17. before it, a postgres
database was always created whole at the current version (there had never been
an older one to move), so `pg::migrate` only ever stamped or refused. it now
reads the stamp and applies the steps above it, in order, in one transaction,
exactly as the sqlite chain does.

one wrinkle: databases written before the migration mechanism existed carry
the v1 tables at `user_version` 0. open detects that case (version 0 with a
`runs` table already present) and treats the file as v1, so the
`ALTER TABLE`s aren't run twice and old rows survive. existing events get
`kind = 'log'` backfilled by the column default.

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

## When the database will not take a write

a store is a dependency like any other and it can refuse. the rules are the
same on both backends and are written out in
[what hestan promises about writes](concepts.md#what-hestan-promises-about-writes);
this is what they mean for the database in front of you.

a write that records what a run did is **retried four times** with jittered
backoff before hestan gives up on it. that covers the ordinary case, which is
another writer holding sqlite's write lock past its 5-second busy timeout, or
a postgres serialization failure or deadlock. what is *not* retried is a
failure the backend cannot have rolled back on its own: a connection that
died. that write may have been committed with its acknowledgement lost, and
going back for it is the one retry that could record a build twice.

which is worth knowing about the postgres backend specifically: it is **one
connection with no pool and no reconnect**. a connection that drops stays
dropped for the life of the process, so a postgres restart under a live
deployment is not something a retry rides out: the runs in flight are left
for a reclaimer and the process stops claiming new ones until it is restarted.
sqlite has no equivalent: a file that comes back is a file that works again.

a run whose write cannot land is left `running` with a lease that lapses; see
the section above for what a reclaimer then does with it. **the process stops
claiming** while its store is refusing writes, so a queue does not drain into
something that cannot record it, and `GET /api/health` reports `ok: false`
with the counts. `hestan doctor` asks a database directly whether it would
take a write lock, which is the question a command line can answer from
outside a deployment.

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

the sweep also takes [sensor run keys](sensors.md) older than the age cutoff
(nothing else collects them, and a sensor keyed by the day would keep a row
per day forever) and delivered [notifications](notifications.md) older than
it.
undelivered notifications stay at any age: one that never got through is not
history, it is something outstanding.

three logs are trimmed by the same sweep whether or not a retention policy is
configured, because all of them grow with time rather than with what you keep:
`schedule_ticks` and `sensor_ticks` are each capped at their newest 5000 rows,
and the [events](events.md) that belong to no run (everything v17 added) at
their newest 50,000. a run's own events go when the run does and always did;
what is new is that an asset built every five minutes writes a row nothing
would otherwise ever collect.
`asset_materializations` is capped *per asset*, and `asset_checks` per
`(asset, check)`, at the newest 200 each (or whatever
`Hestan::asset_history(n)` says), and those two are trimmed at startup. the
newest row of either is never trimmed at any `n`: an asset's latest
materialization is its current state and a check's latest result is what the
asset summary counts ([assets](assets.md)).

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
