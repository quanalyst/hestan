# Storage

everything lands in one sqlite file — no extra services. `Store::open(path)`
opens (and migrates) it; `":memory:"` gives a throwaway store for tests. file
databases run in WAL mode. the store is a single rusqlite connection behind a
mutex, cloneable and shareable across tasks; one writer is plenty at this
scale, and it also means hestan assumes it is the only process writing (see
[embedding](embedding.md)).

## Schema

thirteen tables. `trigger` is a reserved word in sqlite, hence the quoted
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
    scheduled_for TEXT                  -- added in v10
);
CREATE INDEX runs_job_created ON runs(job, created_at DESC);

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
    PRIMARY KEY (run_id, op)
);

CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    op TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'log',   -- added in v2
    data TEXT                           -- added in v2
);
CREATE INDEX events_run ON events(run_id, seq);

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
    value TEXT,               -- null for sources
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
    outcome TEXT NOT NULL,    -- fired | error
    launched INTEGER NOT NULL DEFAULT 0,
    error TEXT
);
```

`runs.error` is the run's own failure summary: the first op that terminally
failed, as `op {name} failed: {message}`, written in the same statement as
the terminal status. it is stored rather than derived from `op_runs` on read
for three reasons — only the executor knows which failure came *first*
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

the schema version lives in `PRAGMA user_version` and `Store::open` migrates
forward on every open. version 1 is the phase-1 schema (`runs`, `op_runs`,
`events` without `kind`/`data`); version 2 adds `events.kind` and
`events.data` plus the `schedules` and `schedule_ticks` tables; version 3
adds `op_state`; version 4 adds `asset_materializations`, `sensors`, and
`sensor_ticks`; version 5 adds `runs.resumed_from`, the link a
[resume](concepts.md#resume) follows back to the run it continued; version 6
adds `runs.error`; version 7 adds `schedules.params`, the params a cron fire
launches with ([scheduling](scheduling.md)) — schedules declared before it
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
for. an older file at any version opens straight into v10, rows intact — the v8 rebuild copies
every keyed materialization across, where it becomes that asset's first
history entry and stays its current one, and v9 leaves every existing row
with a null partition, which is exactly what an unpartitioned asset is. every
pending step
and the version stamp run in one transaction
(sqlite DDL is transactional), so a crash or failure mid-migration leaves
the file exactly as it was found, never half-migrated. a database stamped
with a version newer than the build refuses to open (`db schema v11 is newer
than this build`) instead of quietly writing an older stamp over it.

one wrinkle: databases written before the migration mechanism existed carry
the v1 tables at `user_version` 0. open detects that case (version 0 with a
`runs` table already present) and treats the file as v1, so the
`ALTER TABLE`s aren't run twice and old rows survive. existing events get
`kind = 'log'` backfilled by the column default.

## Crash recovery

`serve` and `run_once` sweep the database at startup, before anything new
launches. any run still `queued` or `running` was left behind by a dead
process; its `running` op runs become `failed` with error
`interrupted: process exited`, its `pending` op runs become `skipped`, a
`run_failed` event (`run interrupted: process exited`) is appended, and the
run itself is marked `failed` with a finish time and that same message as its
`error`. terminal runs are untouched. constructing a `Runner` directly skips the sweep — it belongs to
process startup, not to the executor.

## Retention

by default nothing is ever deleted — runs, op runs, and events accumulate
for as long as the file exists. `retention_days(n)` on the builder opts in
to pruning: at startup, after the crash sweep, terminal runs (success,
failed, or canceled) created more than `n` days ago are deleted together
with their op runs and events, all in one transaction. active runs are never
pruned, whatever their age. neither is `op_state` — watermarks outlive their
runs, so a job that fires rarely keeps its cursor even after every run that
wrote it is gone. an asset's latest materialization is the same: it survives
the run that built it being retired, so a materialization's `run_id` can
point at a run retention has since deleted.

three logs are trimmed at every startup whether retention is configured or
not, because all three grow with time rather than with what you keep.
`schedule_ticks` and `sensor_ticks` are each capped at their newest 5000
rows. `asset_materializations` is capped *per asset*, and `asset_checks` per
`(asset, check)`, at the newest 200 each — or whatever
`Hestan::asset_history(n)` says. the newest row of either is never trimmed at
any `n`: an asset's latest materialization is its current state and a check's
latest result is what the asset summary counts ([assets](assets.md)).

## What's stored and what stays in memory

job and op definitions are code, not rows — the database records history
(names, statuses, timings, outputs, events), never the dag itself. that's why
a retried run whose job has left the code is a 409, and why the store carries
no job table to migrate when you refactor.

`op_runs.output` holds whatever the op's [io manager](io-managers.md)
returned from `put`. under the default `Inline` manager that is the output
itself, json in sqlite, which is what it has always been; under another it is
a handle — `{"$io": "file", "path": ".."}` for `FileIo` — and the value lives
wherever that manager put it. the write happens before the success row, so a
row never claims success for a value that was not persisted.

within a run, dependents are handed handles and resolve them as they are
spawned; a resume and an asset build resolve the handles they seed the same
way. the executor still never reads *outputs* back from sqlite during a run —
it carries them — but it does read them back on a resume, which is where a
pruned run breaks a chain (`get` is asked for a value the manager may no
longer have; `FileIo` cleans up nothing, so this is on you to sweep).

two tables are keyed by names instead of run ids and hold current state
rather than history. `op_state`: one json value per `(job, op)`, upserted
when an op that called `ctx.set_state` succeeds — the success row commits
first, the state second, so a crash between the two re-runs from the old
value rather than skipping a window (the reasoning is in
[op state](state.md)). `sensors`: one cursor per sensor, committed only
after a fully successful evaluation ([sensors](sensors.md)). runs and op
runs come and go; these rows persist until overwritten.

`asset_materializations` is the third of that family and the odd one out: it
is append-only, and an asset's *newest* row is its current state rather than
its only one. each is written inside the asset op just before it reports
success — the mirror image of the op-state order, with the same
at-least-once outcome ([assets](assets.md)). `asset_checks` is append-only
the same way, written inside the check's own op before it decides whether to
fail, so a failing error check records its verdict as well as failing the
run.

writes are ordered for readers. a run row is created in one transaction with
its `pending` op runs and its `run_queued` event, so a visible run always has
its skeleton; the terminal `run_success`/`run_failed`/`run_canceled` event
commits before the terminal status. one deliberate exception:
`ctx.info/warn/error` event writes that fail only log a process-level
warning — a lost log line doesn't fail the op.
