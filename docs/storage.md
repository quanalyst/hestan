# Storage

everything lands in one sqlite file — no extra services. `Store::open(path)`
opens (and migrates) it; `":memory:"` gives a throwaway store for tests. file
databases run in WAL mode. the store is a single rusqlite connection behind a
mutex, cloneable and shareable across tasks; one writer is plenty at this
scale, and it also means hestan assumes it is the only process writing (see
[embedding](embedding.md)).

## Schema

nine tables. `trigger` is a reserved word in sqlite, hence the quoted column
name in the schema and every statement that touches it.

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
    resumed_from TEXT                   -- added in v5
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

CREATE TABLE asset_materializations (  -- added in v4
    asset TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    inputs TEXT NOT NULL,     -- json map: dep name -> consumed fingerprint
    value TEXT,               -- null for sources
    run_id TEXT,              -- null for probe-written source rows
    built_at TEXT NOT NULL
);

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
[resume](concepts.md#resume) follows back to the run it continued. an older
file at any version opens straight into v5, rows intact. every pending step
and the version stamp run in one transaction
(sqlite DDL is transactional), so a crash or failure mid-migration leaves
the file exactly as it was found, never half-migrated. a database stamped
with a version newer than the build refuses to open (`db schema v6 is newer
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
run itself is marked `failed` with a finish time. terminal runs are
untouched. constructing a `Runner` directly skips the sweep — it belongs to
process startup, not to the executor.

## Retention

by default nothing is ever deleted — runs, op runs, and events accumulate
for as long as the file exists. `retention_days(n)` on the builder opts in
to pruning: at startup, after the crash sweep, terminal runs (success,
failed, or canceled) created more than `n` days ago are deleted together
with their op runs and events, all in one transaction. active runs are never
pruned, whatever their age. neither is `op_state` — watermarks outlive their
runs, so a job that fires rarely keeps its cursor even after every run that
wrote it is gone. `asset_materializations` and `sensors` follow the same
rule: current state keyed by name, never pruned. an asset keeps its latest
value and fingerprint after the run that built it is retired, so a
materialization's `run_id` can point at a run retention has since deleted.
tick logs are the exception either way: `schedule_ticks` and `sensor_ticks`
are each trimmed to their newest 5000 rows at every startup, retention
configured or not.

## What's stored and what stays in memory

job and op definitions are code, not rows — the database records history
(names, statuses, timings, outputs, events), never the dag itself. that's why
a retried run whose job has left the code is a 409, and why the store carries
no job table to migrate when you refactor.

op outputs go both places: persisted in `op_runs.output` for the api and ui,
and handed to dependents directly in memory during the run. the executor
never reads outputs back from sqlite.

three tables are keyed by names instead of run ids and hold current state
rather than history. `op_state`: one json value per `(job, op)`, upserted
when an op that called `ctx.set_state` succeeds — the success row commits
first, the state second, so a crash between the two re-runs from the old
value rather than skipping a window (the reasoning is in
[op state](state.md)). `asset_materializations`: one row per asset, its
latest value and fingerprint, written inside the asset op just before it
reports success, which is the mirror image of that order with the same
at-least-once outcome ([assets](assets.md)). `sensors`: one cursor per
sensor, committed only after a fully successful evaluation
([sensors](sensors.md)). runs and op runs come and go; these rows persist
until overwritten.

writes are ordered for readers. a run row is created in one transaction with
its `pending` op runs and its `run_queued` event, so a visible run always has
its skeleton; the terminal `run_success`/`run_failed`/`run_canceled` event
commits before the terminal status. one deliberate exception:
`ctx.info/warn/error` event writes that fail only log a process-level
warning — a lost log line doesn't fail the op.
