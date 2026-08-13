use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};

use crate::error::Error;
use crate::executor::{Blocked, InFlight, Limits, QUEUE_SCAN, Queued};
use crate::logs::{Attempt, Source};
use crate::model::{
    AssetCheckRow, Backfill, BackfillStatus, CheckStatus, DeliveryState, Event, EventKind,
    EventLevel, FreshnessRow, HistoryEntry, Materialization, MetaPoint, Notification, OpLog, OpRun,
    OpStatus, Preset, Reclaim, Run, RunCursor, RunStatus, RunTags, ScheduleRow, SensorOutcome,
    SensorRow, SensorTick, Severity, SubjectKind, Tick, TickOutcome,
};
use crate::op;
use crate::retention::Retention;
use crate::schedule::Schedule;

// `trigger` is a reserved word in sqlite, hence the quoted column name in
// every statement that touches it.
const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    job TEXT NOT NULL,
    status TEXT NOT NULL,
    "trigger" TEXT NOT NULL,
    params TEXT NOT NULL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS runs_job_created ON runs(job, created_at DESC);
CREATE TABLE IF NOT EXISTS op_runs (
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
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    op TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_run ON events(run_id, seq);
"#;

const SCHEMA_V2: &str = r#"
ALTER TABLE events ADD COLUMN kind TEXT NOT NULL DEFAULT 'log';
ALTER TABLE events ADD COLUMN data TEXT;
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
"#;

const SCHEMA_V3: &str = r#"
CREATE TABLE op_state (
    job TEXT NOT NULL,
    op TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (job, op)
);
"#;

const SCHEMA_V4: &str = r#"
CREATE TABLE asset_materializations (
    asset TEXT PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    inputs TEXT NOT NULL,
    value TEXT,
    run_id TEXT,
    built_at TEXT NOT NULL
);
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
    outcome TEXT NOT NULL,
    launched INTEGER NOT NULL DEFAULT 0,
    error TEXT
);
"#;

// the run a resumed run continues, so a chain of resumes can be walked back to
// the outputs its ancestors recorded
const SCHEMA_V5: &str = r#"
ALTER TABLE runs ADD COLUMN resumed_from TEXT;
"#;

// the run's own error: the first op that terminally failed, named. derivable
// from op_runs in principle, but only the executor knows which failure came
// first, and a stored column keeps the run row and the failure hook saying the
// same thing without a correlated subquery on every list query.
const SCHEMA_V6: &str = r#"
ALTER TABLE runs ADD COLUMN error TEXT;
"#;

// the params a scheduled fire launches with. before this a cron fire always
// used `{}`, so a job whose ops declare `.params::<P>()` could never fire.
const SCHEMA_V7: &str = r#"
ALTER TABLE schedules ADD COLUMN params TEXT NOT NULL DEFAULT '{}';
"#;

// materializations become append-only history. the keyed table kept only the
// latest, so "when did this asset actually change" had no answer at all; every
// existing row carries across as that asset's first history entry. the other
// two changes are the same phase's later parts — `op_runs.metadata` and the
// `asset_checks` table — landed here so nothing after this migrates again.
const SCHEMA_V8: &str = r#"
CREATE TABLE asset_materializations_v8 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    inputs TEXT NOT NULL,
    value TEXT,
    run_id TEXT,
    built_at TEXT NOT NULL,
    metadata TEXT
);
INSERT INTO asset_materializations_v8 (asset, fingerprint, inputs, value, run_id, built_at)
    SELECT asset, fingerprint, inputs, value, run_id, built_at
    FROM asset_materializations ORDER BY asset;
DROP TABLE asset_materializations;
ALTER TABLE asset_materializations_v8 RENAME TO asset_materializations;
CREATE INDEX asset_materializations_asset ON asset_materializations(asset, id DESC);
ALTER TABLE op_runs ADD COLUMN metadata TEXT;
CREATE TABLE asset_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    check_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT,
    metadata TEXT,
    checked_at TEXT NOT NULL
);
CREATE INDEX asset_checks_asset ON asset_checks(asset, id DESC);
"#;

// materializations and check results become per `(asset, partition)`, with
// NULL standing for an unpartitioned asset — which is every asset that exists
// before this migration, so existing rows carry across unchanged and every
// lookup below reads them exactly as it did. `backfills` is the same phase's
// last part, landed here so nothing after this migrates again.
const SCHEMA_V9: &str = r#"
ALTER TABLE asset_materializations ADD COLUMN partition TEXT;
DROP INDEX IF EXISTS asset_materializations_asset;
CREATE INDEX asset_materializations_asset
    ON asset_materializations(asset, partition, id DESC);
ALTER TABLE asset_checks ADD COLUMN partition TEXT;
DROP INDEX IF EXISTS asset_checks_asset;
CREATE INDEX asset_checks_asset ON asset_checks(asset, partition, id DESC);
CREATE TABLE backfills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asset TEXT NOT NULL,
    from_key TEXT NOT NULL,
    to_key TEXT NOT NULL,
    partition_keys TEXT NOT NULL,
    run_ids TEXT NOT NULL DEFAULT '[]',
    total INTEGER NOT NULL,
    launched INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL
);
CREATE INDEX backfills_asset ON backfills(asset, id DESC);
"#;

// the whole phase's schema, landed at once so nothing after this migrates
// again. `freshness_state` remembers what the last freshness check concluded,
// so a job late for a week alerts once rather than every minute across every
// restart. the `schedules` columns are the scheduler's durable cursor and its
// catch-up policy, and `runs.scheduled_for` is the logical time a scheduled or
// caught-up run stands for — null on a manual launch, which represents nothing
// but itself. run-status sensors need no table of their own: they are entries
// of `sensors` like every other sensor, and the cursor column is already there.
const SCHEMA_V10: &str = r#"
CREATE TABLE freshness_state (
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    late INTEGER NOT NULL,
    since TEXT,
    PRIMARY KEY (kind, name)
);
ALTER TABLE schedules ADD COLUMN cursor TEXT;
ALTER TABLE schedules ADD COLUMN catchup TEXT NOT NULL DEFAULT 'skip';
ALTER TABLE runs ADD COLUMN scheduled_for TEXT;
"#;

// run keys, which turn a sensor's at-least-once launching into effectively-once
// per sensor. the key is claimed in the same transaction that creates the run,
// so a key can never name a run that was never created — a key recorded for a
// run that did not launch drops that work forever, which is strictly worse than
// the duplicate the key exists to prevent. the two `sensor_ticks` columns are
// the same phase's last part — how long an evaluation took and how many keyed
// requests it skipped — landed here so nothing after this migrates again.
const SCHEMA_V11: &str = r#"
CREATE TABLE sensor_run_keys (
    sensor TEXT NOT NULL,
    run_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    launched_at TEXT NOT NULL,
    PRIMARY KEY (sensor, run_key)
);
ALTER TABLE sensor_ticks ADD COLUMN skipped INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sensor_ticks ADD COLUMN duration_ms INTEGER NOT NULL DEFAULT 0;
"#;

// named parameter sets, plus `runs.tags` for the part that follows, landed
// here so nothing after this migrates again. presets are runtime data and not
// part of a job definition: `Hestan::preset` seeds one at build and the
// launchpad writes others beside it, so the table is the only place both can
// meet. tags are a flat `{"k": "v"}` map, null on every run written before
// this and on every run that carries none — which is not the same as `{}` on
// the wire, but is the same thing to read.
const SCHEMA_V12: &str = r#"
CREATE TABLE presets (
    job TEXT NOT NULL,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (job, name)
);
ALTER TABLE runs ADD COLUMN tags TEXT;
"#;

// what an [isolated op](crate::Op::isolated) needs the run log to carry, since
// the run log is the only channel between the parent and the child.
//
// `pid` is what is running where, cleared by the terminal write: an op run row
// is the ui's answer to "where is this happening", and a pid that outlived its
// process would answer it wrongly. `inputs` is what the parent hands the child
// — `{"held": {dep: handle}, "deps": {dep: status}}`, one row rather than a
// reconstruction of the run's state, and handles rather than payloads so an
// [io manager](crate::IoManager) still keeps the bulk out of sqlite. null for
// every op that runs in this process, which is nearly all of them.
const SCHEMA_V13: &str = r#"
ALTER TABLE op_runs ADD COLUMN pid INTEGER;
ALTER TABLE op_runs ADD COLUMN inputs TEXT;
"#;

// the run queue, and the claims that make it safe for more than one process to
// pull from. the whole phase's schema, landed at once so nothing after this
// migrates again.
//
// `priority` orders the queue (higher first, ties by `created_at`), and the
// three claim columns are the whole of the ownership protocol: `claimed_by` is
// the instance executing the run, `lease_until` is how long that is believed
// for, and a claim past its lease is reclaimable by anyone. a queued run with
// `claimed_by IS NULL` is a run nobody owns — which is what makes the queue
// durable, and what a second process may take.
//
// `plan` is what the launch decided the run would execute:
// `{"ops": [...] | null, "seeds": {op: handle}}`, null for a run of the whole
// job. it exists because starting a run is no longer the job of the process
// that asked for it: an asset build's memoized seeds and a resume's reused
// outputs live in the launching process's memory and nowhere else, and a
// claimer in another process has to be able to start the run without them.
const SCHEMA_V14: &str = r#"
ALTER TABLE runs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0;
ALTER TABLE runs ADD COLUMN claimed_by TEXT;
ALTER TABLE runs ADD COLUMN claimed_at TEXT;
ALTER TABLE runs ADD COLUMN lease_until TEXT;
ALTER TABLE runs ADD COLUMN plan TEXT;
CREATE INDEX runs_queue ON runs(status, claimed_by, priority DESC, created_at);
"#;

// what an op printed, as opposed to what it said with `ctx.info`. the run log
// is hestan's channel and this table is the op's own, which is why it is a
// table rather than more `events`: a chatty op would otherwise bury the eight
// lines that describe what the run did.
//
// exactly one half of the middle three columns is filled per row and which
// half says where the line came from. `stream` is `stdout`/`stderr` and the
// other two null for an [isolated op](crate::Op::isolated)'s subprocess
// capture — a pipe has no levels and no targets. `level` and `target` are set
// and `stream` null for a tracing event captured by the `capture` feature's
// layer — an event was never on a pipe. `attempt` is which attempt of the op
// produced it, because the output of the attempt that failed and the output of
// the retry that worked are different things.
//
// the index is the cursor: `(run_id, op, id)` serves both the whole run and
// one op of it, in insertion order, which is the order the lines were read in.
const SCHEMA_V15: &str = r#"
CREATE TABLE op_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    op TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    at TEXT NOT NULL,
    stream TEXT,
    level TEXT,
    target TEXT,
    message TEXT NOT NULL
);
CREATE INDEX op_logs_run ON op_logs(run_id, op, id);
"#;

// notifications that have to survive the process that decided to send them.
// opt-in with `Hestan::durable_notifications`, so on most databases this table
// stays empty — an embedder using a hook to bump a metric wants a callback,
// not a table and a delivery loop.
//
// the row is written in the same transaction as the run's terminal row, which
// is the whole point: written after it, a crash in between loses the alert
// about the failure the alert existed to report, and nothing anywhere records
// that it should have been sent.
//
// `next_attempt_at` is when this row is next due and is what says which of the
// three states it is in: set and undelivered is pending, **null** and
// undelivered is given up on, and a delivery time is a delivery. so a row is
// inserted due now rather than null, and the give-up clears it — which also
// keeps a permanently failing notification out of the scan while leaving it
// visible, with the error that stopped it.
//
// the partial index is the scan the delivery loop runs and nothing else: the
// pending rows are a handful and the delivered ones are the table.
const SCHEMA_V16: &str = r#"
CREATE TABLE notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    delivered_at TEXT,
    last_error TEXT
);
CREATE INDEX notifications_due ON notifications(next_attempt_at)
    WHERE delivered_at IS NULL;
CREATE INDEX notifications_delivered ON notifications(delivered_at);
"#;

// the event log stops being about runs. `run_id` was NOT NULL, so an event
// could only ever describe a run — everything else hestan does happened in its
// own table and reached no stream at all.
//
// `subject_kind` and `subject` are what a non-run event says it is about:
// `('asset', 'sales/orders')`, `('sensor', 'watch')`, `('backfill', '12')`.
// existing rows are runs and are stamped as such, which is what they were.
//
// **`subject` is not a copy of `run_id`.** a run event leaves it null and is
// found by the column that already named it; filling it in would rewrite every
// row of the largest table in the database to store a second copy of an indexed
// column. `Event::about` is where the two become one answer.
//
// two queries matter and one index is added: newest first *within a subject*.
// newest first globally is `seq` descending, and `seq` is the primary key on
// both backends, so it is the primary key read backwards and an index of its
// own would only be a second copy of it.
//
// sqlite has no `ALTER COLUMN`, so dropping the NOT NULL means rebuilding the
// table — the v8 pattern, and the expensive half of this migration on a
// database with a year of events in it. postgres drops a NOT NULL and adds two
// defaulted columns in the catalog and touches no row at all; the two backends
// are genuinely not doing the same amount of work here, and `docs/storage.md`
// says so.
const SCHEMA_V17: &str = r#"
CREATE TABLE events_v17 (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT,
    op TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    ts TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'log',
    data TEXT,
    subject_kind TEXT NOT NULL DEFAULT 'run',
    subject TEXT
);
INSERT INTO events_v17 (seq, run_id, op, level, message, ts, kind, data, subject_kind)
    SELECT seq, run_id, op, level, message, ts, kind, data, 'run' FROM events;
DROP TABLE events;
ALTER TABLE events_v17 RENAME TO events;
CREATE INDEX events_run ON events(run_id, seq);
CREATE INDEX events_subject ON events(subject_kind, subject, seq DESC);
"#;

// who did it, on the two tables that record something somebody asked for. two
// nullable columns and no rewrite on either backend: null is what every row
// written before this says, and it is also what an unauthenticated deployment
// keeps writing — an empty name is not "system", and a fabricated actor is
// worse than none.
const SCHEMA_V18: &str = r#"
ALTER TABLE runs ADD COLUMN actor TEXT;
ALTER TABLE events ADD COLUMN actor TEXT;
"#;

pub(crate) const SCHEMA_VERSION: u32 = 18;

// one transaction around every pending step and the version stamp (sqlite DDL
// is transactional), so a crash mid-migration leaves the db exactly as found
fn migrate(conn: &mut Connection) -> Result<(), Error> {
    let tx = conn.transaction()?;
    let mut version: u32 = tx.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version == 0 && table_exists(&tx, "runs")? {
        // phase-1 dbs predate versioning: v1 schema at user_version 0
        version = 1;
    }
    if version > SCHEMA_VERSION {
        return Err(Error::SchemaTooNew(version));
    }
    if version < 1 {
        tx.execute_batch(SCHEMA_V1)?;
    }
    if version < 2 {
        tx.execute_batch(SCHEMA_V2)?;
    }
    if version < 3 {
        tx.execute_batch(SCHEMA_V3)?;
    }
    if version < 4 {
        tx.execute_batch(SCHEMA_V4)?;
    }
    if version < 5 {
        tx.execute_batch(SCHEMA_V5)?;
    }
    if version < 6 {
        tx.execute_batch(SCHEMA_V6)?;
    }
    if version < 7 {
        tx.execute_batch(SCHEMA_V7)?;
    }
    if version < 8 {
        tx.execute_batch(SCHEMA_V8)?;
    }
    if version < 9 {
        tx.execute_batch(SCHEMA_V9)?;
    }
    if version < 10 {
        tx.execute_batch(SCHEMA_V10)?;
    }
    if version < 11 {
        tx.execute_batch(SCHEMA_V11)?;
    }
    if version < 12 {
        tx.execute_batch(SCHEMA_V12)?;
    }
    if version < 13 {
        tx.execute_batch(SCHEMA_V13)?;
    }
    if version < 14 {
        tx.execute_batch(SCHEMA_V14)?;
    }
    if version < 15 {
        tx.execute_batch(SCHEMA_V15)?;
    }
    if version < 16 {
        tx.execute_batch(SCHEMA_V16)?;
    }
    if version < 17 {
        tx.execute_batch(SCHEMA_V17)?;
    }
    if version < 18 {
        tx.execute_batch(SCHEMA_V18)?;
    }
    if version != SCHEMA_VERSION {
        tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    }
    tx.commit()?;
    Ok(())
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, Error> {
    let found = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [name],
            |_| Ok(()),
        )
        .optional()?;
    Ok(found.is_some())
}

/// the key a sensor-launched run claims: at most one run per `(sensor, key)`
/// pair, ever. [`Store::create_run_keyed`] writes it in the same transaction as
/// the run row, which is what makes the two impossible to disagree.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RunKey<'a> {
    pub sensor: &'a str,
    pub key: &'a str,
}

/// every column [`event_from_row`] reads, in the order it reads them.
const EVENT_COLS: &str =
    "seq, run_id, subject_kind, subject, op, level, kind, message, data, ts, actor";

/// every column [`run_from_row`] reads, in the order it reads them. one list
/// rather than four copies of it, since a run now carries enough columns that
/// two of them drifting apart is a real way to spend an afternoon.
const RUN_COLS: &str = r#"id, job, status, "trigger", params, created_at, started_at, finished_at,
    resumed_from, error, scheduled_for, tags, priority, claimed_by, claimed_at, lease_until,
    actor"#;

/// every column [`notification_from_row`] reads, in the order it reads them.
const NOTIFICATION_COLS: &str =
    "id, kind, payload, created_at, attempts, next_attempt_at, delivered_at, last_error";

/// what `notifications.kind` says about a row whose payload is a
/// [`RunEvent`](crate::RunEvent). the column is there so a second event shape
/// can join the table without a migration; today there is one.
const RUN_NOTIFICATION: &str = "run";

/// how long a write waits for another connection to let go of the file before
/// giving up. an [isolated op](crate::Op::isolated) means two processes write
/// this database at once, and sqlite's default is to fail the second one
/// immediately — which would be a lost event, or a lost terminal row.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// the url scheme a postgres target is named with. libpq accepts both
/// spellings and so does everything downstream of here.
const PG_SCHEMES: [&str; 2] = ["postgres://", "postgresql://"];

/// what the two backends spell differently, listed once — the seven methods
/// below, plus the placeholder sigil, which is a lexical rewrite on the way
/// out (`?1` to `$1`, in [`pg`](crate::pg)) and needs no branch anywhere.
///
/// the survey that opened this phase found six of the seven: nine
/// `AUTOINCREMENT`s that are DDL and nothing else, four inserts that yield to
/// whatever is already there, sqlite's null-safe `IS`, one json walk, one json
/// append and the claim itself. running the store's suite against postgres
/// found the seventh, which is what running it was for. everything else is the
/// same text on both.
///
/// naming each one here rather than at eighty call sites is the point: an
/// explicit branch at a divergence is auditable, and a renderer that
/// translated the eighty and silently mis-rendered the eighty-first would not
/// be. there is deliberately no such renderer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Dialect {
    Sqlite,
    #[cfg(feature = "postgres")]
    Postgres,
}

impl Dialect {
    /// what an optional filter's parameter needs saying about it.
    ///
    /// half the queries below are shaped `WHERE (?1 IS NULL OR job = ?1)` — a
    /// filter that is a filter only when it was asked for. postgres works
    /// through an expression left to right and gives a parameter its type at
    /// the first place that implies one; `?1 IS NULL` implies nothing at all,
    /// so it reaches the end still not knowing and refuses the statement. the
    /// cast says it once, up front. sqlite infers nothing from anything and
    /// needs none of this.
    fn text_param(self) -> &'static str {
        match self {
            Dialect::Sqlite => "",
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "::text",
        }
    }

    /// sqlite's null-safe comparison, which postgres spells out. `partition IS
    /// ?2` matches a null column against a null parameter and `partition = ?2`
    /// never does, which is the difference between "the unpartitioned asset"
    /// and "no rows".
    fn null_safe_eq(self) -> &'static str {
        match self {
            Dialect::Sqlite => "IS",
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "IS NOT DISTINCT FROM",
        }
    }

    /// an insert that yields to the row already there. sqlite says so at the
    /// front of the statement and postgres at the back, so this is a pair
    /// rather than a word.
    fn insert_or_ignore(self) -> (&'static str, &'static str) {
        match self {
            Dialect::Sqlite => ("INSERT OR IGNORE INTO", ""),
            #[cfg(feature = "postgres")]
            Dialect::Postgres => ("INSERT INTO", "ON CONFLICT DO NOTHING"),
        }
    }

    /// how a dispatcher takes the one row it means to claim, out of anyone
    /// else's way, before spending a statement on it.
    ///
    /// this is the reason to want postgres. `SKIP LOCKED` hands a second
    /// dispatcher reaching for the same run *nothing* for it, immediately, so
    /// it moves on to the next run rather than waiting on a claim to commit
    /// only to find it lost. one row, not the queue: a dispatcher that locked
    /// every candidate it looked at would hand every other dispatcher an empty
    /// queue. sqlite has no such clause and needs none — inside an immediate
    /// transaction there is no other writer to wait for.
    fn claim_lock(self) -> &'static str {
        match self {
            Dialect::Sqlite => "",
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "FOR UPDATE SKIP LOCKED",
        }
    }

    /// whether a run carries exactly the tag `?5 = ?6`. sqlite walks the
    /// stored json with `json_each`; postgres reads the one key out of it and
    /// gets null for a run with no tags at all, which is the same answer.
    fn tag_filter(self) -> &'static str {
        match self {
            Dialect::Sqlite => {
                "EXISTS (SELECT 1 FROM json_each(runs.tags)
                         WHERE json_each.key = ?5 AND json_each.value = ?6)"
            }
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "runs.tags::json ->> ?5 = ?6",
        }
    }

    /// append `?3` to the json array in `run_ids`. both keep the column as
    /// text; only the function that edits it differs.
    fn json_append(self) -> &'static str {
        match self {
            Dialect::Sqlite => "json_insert(run_ids, '$[#]', ?3)",
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "(run_ids::jsonb || to_jsonb(?3::text))::text",
        }
    }

    /// whether this history entry's fingerprint differs from the one before
    /// it, as 0 or 1. sqlite's null-safe `IS NOT` is already an integer and
    /// postgres's `IS DISTINCT FROM` is a boolean, so one of them says it the
    /// long way and the column reads the same either way.
    fn fingerprint_changed(self) -> &'static str {
        match self {
            Dialect::Sqlite => {
                "fingerprint IS NOT LAG(fingerprint) OVER (PARTITION BY partition ORDER BY id)"
            }
            // and cast, because postgres has two integer widths and an
            // integer literal is the narrow one, which is not what a column
            // of this schema ever is
            #[cfg(feature = "postgres")]
            Dialect::Postgres => {
                "(CASE WHEN fingerprint IS DISTINCT FROM
                       LAG(fingerprint) OVER (PARTITION BY partition ORDER BY id)
                       THEN 1 ELSE 0 END)::bigint"
            }
        }
    }
}

/// the database behind a [`Store`], open.
///
/// one connection behind one mutex either way. a postgres pool would buy
/// parallel statements, and with them reconnection, a second set of failure
/// modes and transactions that no longer sit where the code around them thinks
/// — sqlite already blocks on one connection and that is the architecture this
/// matches. what postgres is for here is several *processes* sharing a run log,
/// not one process issuing more statements at once.
enum Db {
    Sqlite(Mutex<Connection>),
    #[cfg(feature = "postgres")]
    Postgres(Mutex<crate::pg::Client>),
}

/// a bound parameter. one list serves both backends: `Val` rather than each
/// crate's own, so [`args!`] can be written once at every call site.
///
/// three shapes, because the schema has three — text (which is every
/// timestamp, every status word and every piece of json), integers, and null.
#[derive(Debug)]
pub(crate) enum Val<'a> {
    Null,
    Text(Cow<'a, str>),
    Int(i64),
}

/// the bound parameters of one statement, in source order: `args![job, limit]`
/// is `?1, ?2`.
macro_rules! args {
    () => { &[] as &[Val<'_>] };
    ($($v:expr),+ $(,)?) => { &[$(Val::from($v)),+] as &[Val<'_>] };
}

// the postgres backend binds the same lists; without that feature the macro is
// only ever used in this file
#[cfg(feature = "postgres")]
pub(crate) use args;

/// `?1, ?2, ..` for `n` values, which is how a list of ids goes into an `IN`.
///
/// the ids are still bound rather than pasted in: everything else in this file
/// binds its values, and a list is not the place to start making an exception.
fn placeholders(n: usize) -> String {
    (1..=n)
        .map(|i| format!("?{i}"))
        .collect::<Vec<String>>()
        .join(", ")
}

impl<'a> From<&'a str> for Val<'a> {
    fn from(v: &'a str) -> Val<'a> {
        Val::Text(Cow::Borrowed(v))
    }
}

impl<'a> From<&'a String> for Val<'a> {
    fn from(v: &'a String) -> Val<'a> {
        Val::Text(Cow::Borrowed(v))
    }
}

impl From<String> for Val<'_> {
    fn from(v: String) -> Val<'static> {
        Val::Text(Cow::Owned(v))
    }
}

impl From<i64> for Val<'_> {
    fn from(v: i64) -> Val<'static> {
        Val::Int(v)
    }
}

impl From<u32> for Val<'_> {
    fn from(v: u32) -> Val<'static> {
        Val::Int(i64::from(v))
    }
}

// hestan's booleans are stored as 0 and 1 on both backends — see the note on
// the postgres schema — so this is where one becomes the other
impl From<bool> for Val<'_> {
    fn from(v: bool) -> Val<'static> {
        Val::Int(i64::from(v))
    }
}

impl<'a, T: Into<Val<'a>>> From<Option<T>> for Val<'a> {
    fn from(v: Option<T>) -> Val<'a> {
        v.map_or(Val::Null, Into::into)
    }
}

impl rusqlite::ToSql for Val<'_> {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, ValueRef};
        Ok(match self {
            Val::Null => ToSqlOutput::Borrowed(ValueRef::Null),
            Val::Text(s) => ToSqlOutput::Borrowed(ValueRef::Text(s.as_bytes())),
            Val::Int(i) => ToSqlOutput::Borrowed(ValueRef::Integer(*i)),
        })
    }
}

/// what a statement runs against: a connection, or a transaction on one.
///
/// the same three calls either way, so a query written once runs in either
/// place — which several methods below depend on, being handed a transaction
/// by one caller and a bare connection by another.
trait Exec {
    fn dialect(&self) -> Dialect;

    /// rows affected.
    fn execute(&mut self, sql: &str, args: &[Val<'_>]) -> Result<usize, Error>;

    fn query<T>(
        &mut self,
        sql: &str,
        args: &[Val<'_>],
        row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error>;

    /// the first row, or none. the first rather than "at most one": that is
    /// what every caller here means and what rusqlite has always done, and a
    /// postgres client that errored on a second row instead would be a
    /// difference between the backends rather than a check on anything.
    fn query_opt<T>(
        &mut self,
        sql: &str,
        args: &[Val<'_>],
        row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        Ok(self.query(sql, args, row)?.into_iter().next())
    }
}

/// an open connection with the store's mutex held.
enum Conn<'a> {
    Sqlite(MutexGuard<'a, Connection>),
    #[cfg(feature = "postgres")]
    Postgres(MutexGuard<'a, crate::pg::Client>),
}

impl Conn<'_> {
    /// a transaction. sqlite gets a deferred one, which is what the callers
    /// that only write want.
    fn begin(&mut self) -> Result<Tx<'_>, Error> {
        match self {
            Conn::Sqlite(c) => Ok(Tx::Sqlite(c.transaction()?)),
            #[cfg(feature = "postgres")]
            Conn::Postgres(c) => Ok(Tx::Postgres(c.transaction()?)),
        }
    }

    /// a transaction that takes the write lock at `BEGIN` rather than at the
    /// first write, for the read-then-write sequences that must not have
    /// another writer in the middle of them.
    ///
    /// postgres has no such knob and needs none: its writers do not queue
    /// behind a database-wide lock, and what those callers rely on is a row
    /// lock or a conditional update, both of which hold inside an ordinary
    /// transaction.
    fn begin_immediate(&mut self) -> Result<Tx<'_>, Error> {
        match self {
            Conn::Sqlite(c) => Ok(Tx::Sqlite(
                c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?,
            )),
            #[cfg(feature = "postgres")]
            Conn::Postgres(c) => Ok(Tx::Postgres(c.transaction()?)),
        }
    }

    /// several statements at once, no parameters: ddl and nothing else.
    #[cfg(test)]
    fn batch(&mut self, sql: &str) -> Result<(), Error> {
        match self {
            Conn::Sqlite(c) => Ok(c.execute_batch(sql)?),
            #[cfg(feature = "postgres")]
            Conn::Postgres(c) => c.batch(sql),
        }
    }
}

impl Exec for Conn<'_> {
    fn dialect(&self) -> Dialect {
        match self {
            Conn::Sqlite(_) => Dialect::Sqlite,
            #[cfg(feature = "postgres")]
            Conn::Postgres(_) => Dialect::Postgres,
        }
    }

    fn execute(&mut self, sql: &str, args: &[Val<'_>]) -> Result<usize, Error> {
        match self {
            Conn::Sqlite(c) => sqlite_execute(c, sql, args),
            #[cfg(feature = "postgres")]
            Conn::Postgres(c) => c.execute(sql, args),
        }
    }

    fn query<T>(
        &mut self,
        sql: &str,
        args: &[Val<'_>],
        row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        match self {
            Conn::Sqlite(c) => sqlite_query(c, sql, args, row),
            #[cfg(feature = "postgres")]
            Conn::Postgres(c) => c.query(sql, args, row),
        }
    }
}

/// a transaction on either backend. dropping one rolls it back, which is what
/// several of the methods below use to abandon a write they decided against.
enum Tx<'a> {
    Sqlite(rusqlite::Transaction<'a>),
    #[cfg(feature = "postgres")]
    Postgres(crate::pg::Transaction<'a>),
}

impl Tx<'_> {
    /// hold the claim lock for the rest of this transaction, so that a
    /// dispatcher counting capacity and a dispatcher about to spend it take
    /// turns.
    ///
    /// the count and the claim sharing one transaction is enough on sqlite,
    /// where an immediate transaction is already the only writer. it is not
    /// enough on postgres: two transactions read the same free slot from their
    /// own snapshots and both spend it, which is the one way two dispatchers
    /// break a limit. asked for only when a limit is in force — see
    /// [`Limits::binding`] — so that the ordinary case, where nothing is
    /// capped, still has dispatchers never meeting.
    fn take_turns(&mut self) -> Result<(), Error> {
        match self {
            Tx::Sqlite(_) => Ok(()),
            #[cfg(feature = "postgres")]
            Tx::Postgres(tx) => tx.claim_lock(),
        }
    }

    fn commit(self) -> Result<(), Error> {
        match self {
            Tx::Sqlite(tx) => Ok(tx.commit()?),
            #[cfg(feature = "postgres")]
            Tx::Postgres(tx) => tx.commit(),
        }
    }
}

impl Exec for Tx<'_> {
    fn dialect(&self) -> Dialect {
        match self {
            Tx::Sqlite(_) => Dialect::Sqlite,
            #[cfg(feature = "postgres")]
            Tx::Postgres(_) => Dialect::Postgres,
        }
    }

    fn execute(&mut self, sql: &str, args: &[Val<'_>]) -> Result<usize, Error> {
        match self {
            Tx::Sqlite(tx) => sqlite_execute(tx, sql, args),
            #[cfg(feature = "postgres")]
            Tx::Postgres(tx) => tx.execute(sql, args),
        }
    }

    fn query<T>(
        &mut self,
        sql: &str,
        args: &[Val<'_>],
        row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        match self {
            Tx::Sqlite(tx) => sqlite_query(tx, sql, args, row),
            #[cfg(feature = "postgres")]
            Tx::Postgres(tx) => tx.query(sql, args, row),
        }
    }
}

fn sqlite_execute(conn: &Connection, sql: &str, args: &[Val<'_>]) -> Result<usize, Error> {
    Ok(conn.execute(sql, rusqlite::params_from_iter(args))?)
}

fn sqlite_query<T>(
    conn: &Connection,
    sql: &str,
    args: &[Val<'_>],
    mut row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(args))?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        out.push(row(&AnyRow::Sqlite(r))?);
    }
    Ok(out)
}

/// one row from either backend.
///
/// the accessors are here rather than at the call sites because this is where
/// the two disagree about types: rusqlite reads a column as whatever the value
/// in it is, postgres as what the column was declared, and both hold hestan's
/// timestamps as rfc3339 text and its booleans as 0 and 1.
pub(crate) enum AnyRow<'a> {
    Sqlite(&'a rusqlite::Row<'a>),
    #[cfg(feature = "postgres")]
    Postgres(&'a tokio_postgres::Row),
}

impl AnyRow<'_> {
    fn text(&self, idx: usize) -> Result<String, Error> {
        match self {
            AnyRow::Sqlite(r) => Ok(r.get(idx)?),
            #[cfg(feature = "postgres")]
            AnyRow::Postgres(r) => Ok(r.try_get(idx)?),
        }
    }

    fn opt_text(&self, idx: usize) -> Result<Option<String>, Error> {
        match self {
            AnyRow::Sqlite(r) => Ok(r.get(idx)?),
            #[cfg(feature = "postgres")]
            AnyRow::Postgres(r) => Ok(r.try_get(idx)?),
        }
    }

    pub(crate) fn int(&self, idx: usize) -> Result<i64, Error> {
        match self {
            AnyRow::Sqlite(r) => Ok(r.get(idx)?),
            #[cfg(feature = "postgres")]
            AnyRow::Postgres(r) => Ok(r.try_get(idx)?),
        }
    }

    fn opt_int(&self, idx: usize) -> Result<Option<i64>, Error> {
        match self {
            AnyRow::Sqlite(r) => Ok(r.get(idx)?),
            #[cfg(feature = "postgres")]
            AnyRow::Postgres(r) => Ok(r.try_get(idx)?),
        }
    }

    /// a stored 0/1 column, which is how both backends keep a boolean here.
    fn flag(&self, idx: usize) -> Result<bool, Error> {
        Ok(self.int(idx)? != 0)
    }

    fn count(&self, idx: usize) -> Result<u32, Error> {
        let n = self.int(idx)?;
        u32::try_from(n).map_err(|_| Error::Column(idx, format!("{n} does not fit a u32")))
    }

    fn size(&self, idx: usize) -> Result<usize, Error> {
        let n = self.int(idx)?;
        usize::try_from(n).map_err(|_| Error::Column(idx, format!("{n} does not fit a usize")))
    }

    fn millis(&self, idx: usize) -> Result<u64, Error> {
        let n = self.int(idx)?;
        u64::try_from(n).map_err(|_| Error::Column(idx, format!("{n} does not fit a u64")))
    }

    fn ts(&self, idx: usize) -> Result<DateTime<Utc>, Error> {
        parse_ts(idx, &self.text(idx)?)
    }

    fn opt_ts(&self, idx: usize) -> Result<Option<DateTime<Utc>>, Error> {
        self.opt_text(idx)?.map(|s| parse_ts(idx, &s)).transpose()
    }

    fn json(&self, idx: usize) -> Result<Value, Error> {
        parse_json(idx, &self.text(idx)?)
    }

    fn opt_json(&self, idx: usize) -> Result<Option<Value>, Error> {
        self.opt_text(idx)?.map(|s| parse_json(idx, &s)).transpose()
    }

    // the bound is `Display` rather than `Err = String` because the two enums
    // that tolerate an unknown word parse infallibly, and one that cannot fail
    // should not have to invent an error type to be read with the rest
    fn parse<T>(&self, idx: usize) -> Result<T, Error>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        self.text(idx)?
            .parse()
            .map_err(|e: T::Err| Error::Column(idx, e.to_string()))
    }

    fn opt_parse<T>(&self, idx: usize) -> Result<Option<T>, Error>
    where
        T: FromStr,
        T::Err: std::fmt::Display,
    {
        match self.opt_text(idx)? {
            Some(s) => s
                .parse()
                .map(Some)
                .map_err(|e: T::Err| Error::Column(idx, e.to_string())),
            None => Ok(None),
        }
    }
}

fn parse_ts(idx: usize, text: &str) -> Result<DateTime<Utc>, Error> {
    DateTime::parse_from_rfc3339(text)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| Error::Column(idx, e.to_string()))
}

fn parse_json(idx: usize, text: &str) -> Result<Value, Error> {
    serde_json::from_str(text).map_err(|e| Error::Column(idx, e.to_string()))
}

/// a store write the run can survive losing, and the only thing [`note`] takes.
///
/// an event, a captured log line, the pid of a child: each is worth having and
/// none of them is what a run *did*. a write that records that — a terminal
/// row, a status, a watermark — returns `Result<(), Error>` like everything
/// else and goes through [`Store::landed`], so `note(store.op_finished(..))`
/// is a thing the compiler refuses rather than a thing to remember not to
/// write.
///
/// only this module makes one, and only in the signature of a write it has
/// declared best-effort. **a write added later is critical until somebody says
/// otherwise**, which is the way round that survives being forgotten.
#[must_use = "a best-effort write is still a write: note it, so a store that is dropping them says so"]
pub(crate) struct BestEffort {
    wrote: Result<(), Error>,
    health: Arc<Health>,
}

impl BestEffort {
    /// panic unless it landed. tests only, and about fixtures rather than
    /// about runs: a case that plants an event and then asserts on it wants to
    /// hear that the row is there.
    #[cfg(test)]
    #[track_caller]
    pub(crate) fn unwrap(self) {
        self.wrote.unwrap();
    }
}

/// let a best-effort write go, and count it.
///
/// losing a log line is survivable where losing a run's outcome is not — but
/// being quiet about it is not part of the deal. what this drops is counted on
/// the store's [health](Store::health), which is what `/api/health` and
/// `hestan doctor` report: a deployment whose run pages are missing half their
/// events should find that out from the control plane rather than from the
/// gap.
pub(crate) fn note(write: BestEffort) {
    match write.wrote {
        Ok(()) => write.health.wrote(),
        Err(e) => {
            tracing::warn!("store write dropped: {e}");
            write.health.dropped();
        }
    }
}

/// how many times a critical write is attempted before hestan stops believing
/// the store is about to take it.
///
/// four rather than more: what is being waited out here is a lock or a
/// stumble, and past a second of them the run is better off stopping than
/// holding a claim it may not be able to close. sqlite has already spent its
/// [`BUSY_TIMEOUT`] inside each of these attempts before returning at all.
const WRITE_ATTEMPTS: u32 = 4;

/// the first gap between attempts, doubled per attempt up to [`WRITE_MAX`]
/// with full jitter — the same pacing an op's retries and the notification
/// loop's use, and for the same reason: a hundred ops that lost the same lock
/// must not come back for it on the same millisecond.
const WRITE_BASE: Duration = Duration::from_millis(50);
const WRITE_MAX: Duration = Duration::from_secs(1);

/// whether a failed write is worth another attempt.
///
/// **every error this says yes to is one the backend raised on a live
/// connection, having already undone the transaction.** that is what makes a
/// retry safe rather than lucky: `op_finished` appends materializations and
/// events beside the row it updates, and repeating that after a *partial*
/// apply would record a build twice. there is no partial apply to repeat —
/// sqlite's commit is atomic and rusqlite rolls back a transaction it could
/// not commit, and postgres aborts the transaction it reports a serialization
/// failure or a deadlock for.
///
/// the one failure that leaves the outcome genuinely unknown is a connection
/// that died: a commit may have been executed and its acknowledgement lost.
/// **that is the case hestan does not retry.** it would also be futile — a
/// [postgres store](crate::pg) is one connection with no pool behind it and
/// no reconnect, so nothing this process writes will land again — but futility
/// is not the reason. the reason is that a retry there is the one that can
/// double-apply.
///
/// where the backend leaves room to be unsure, the answer is yes, because the
/// cost of a needless retry is fifty milliseconds and the cost of not trying
/// is a run nobody can close.
fn transient(e: &Error) -> bool {
    match e {
        Error::Sqlite(e) => sqlite_transient(e),
        #[cfg(feature = "postgres")]
        Error::Postgres(e) => postgres_transient(e),
        // a column that does not parse, a database from a later build, a
        // target this build cannot open: none of them is about this moment,
        // and none of them is reachable from a write in any case
        Error::Column(..) | Error::SchemaTooNew(_) | Error::UnsupportedDb(_) => false,
        // which leaves the filesystem, where a call that failed once may
        // perfectly well work now
        _ => true,
    }
}

/// sqlite's side of [`transient`]: everything but the codes that mean the same
/// statement will say the same thing.
fn sqlite_transient(e: &rusqlite::Error) -> bool {
    use rusqlite::ErrorCode;
    match e {
        rusqlite::Error::SqliteFailure(e, _) => !matches!(
            e.code,
            ErrorCode::ConstraintViolation
                | ErrorCode::TypeMismatch
                | ErrorCode::ApiMisuse
                | ErrorCode::NotADatabase
                | ErrorCode::DatabaseCorrupt
                | ErrorCode::DiskFull
                | ErrorCode::ReadOnly
                | ErrorCode::PermissionDenied
                | ErrorCode::TooBig
                | ErrorCode::ParameterOutOfRange
        ),
        // a parameter hestan bound wrongly or a column it read wrongly: this
        // crate's own bug, and it will still be one in fifty milliseconds
        _ => false,
    }
}

/// postgres's side of [`transient`]: a sqlstate the server answered with,
/// minus the classes that are about the statement rather than the moment.
///
/// no sqlstate at all means the server did not answer — the connection, the
/// protocol, a value this client could not encode — and that is the case
/// [`transient`] refuses on principle.
#[cfg(feature = "postgres")]
fn postgres_transient(e: &tokio_postgres::Error) -> bool {
    if e.is_closed() {
        return false;
    }
    match e.code() {
        None => false,
        Some(state) => {
            // 08 connection, 22 data, 23 integrity, 42 syntax and access, and
            // a disk that is full whatever class it is filed under
            !matches!(
                state.code().get(..2),
                Some("08") | Some("22") | Some("23") | Some("42")
            ) && state != &tokio_postgres::error::SqlState::DISK_FULL
        }
    }
}

/// what this process has seen one store do.
///
/// counters rather than a verdict: a store either takes a write or it does
/// not, and how often it did not is the fact worth reporting. one per store,
/// shared by every clone of it, and never reset — a deployment that dropped a
/// hundred events an hour ago dropped them.
#[derive(Default)]
pub(crate) struct Health {
    dropped: AtomicU64,
    unrecorded: AtomicU64,
    /// whether the last write this process attempted did not land and none
    /// has landed since. what a process asks before it claims anything.
    failing: AtomicBool,
    /// how many of the writes a run makes are to fail before the database is
    /// allowed to see them. tests only — see [`Store::fail_writes`].
    #[cfg(test)]
    injected: AtomicU64,
    /// which write those failures are for, `None` being all of them.
    #[cfg(test)]
    injected_into: Mutex<Option<&'static str>>,
}

impl Health {
    /// a write landed, so whatever was wrong is no longer wrong.
    fn wrote(&self) {
        self.failing.store(false, Ordering::Relaxed);
    }

    /// a best-effort write did not land, and nothing will try again.
    fn dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        self.failing.store(true, Ordering::Relaxed);
    }

    /// a write that records authoritative state did not land.
    fn unrecorded(&self) {
        self.unrecorded.fetch_add(1, Ordering::Relaxed);
        self.failing.store(true, Ordering::Relaxed);
    }

    /// whether this store is refusing writes as far as this process can tell.
    ///
    /// one bit about the last attempt rather than a rate: what asks is a
    /// dispatcher deciding whether to claim a run, and "the last thing i tried
    /// to write did not land" is exactly the question it is asking.
    pub(crate) fn failing(&self) -> bool {
        self.failing.load(Ordering::Relaxed)
    }

    /// how many events, log lines and other best-effort writes this store has
    /// lost.
    pub(crate) fn dropped_writes(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// how many times something a run did could not be written down.
    pub(crate) fn unrecorded_writes(&self) -> u64 {
        self.unrecorded.load(Ordering::Relaxed)
    }
}

/// run history on sqlite or postgres. cheap to clone; safe to share across
/// tasks.
///
/// ```no_run
/// # use hestan::Store;
/// # fn f() -> Result<(), hestan::Error> {
/// let store = Store::open("hestan.db")?;
/// for run in store.runs(Some("nightly"), None, None, None, None, 20)? {
///     println!("{} {} {}", run.id, run.status, run.created_at);
/// }
/// # Ok(())
/// # }
/// ```
///
/// nothing above needs a server, a registry or a running deployment: the run
/// log is a database and this is a reader for it. that is what a report, an
/// export, or a test asserting what a run actually did is written against.
/// opening it does migrate the schema, so point it at a copy if that is not
/// wanted.
#[derive(Clone)]
pub struct Store {
    db: Arc<Db>,
    /// the target it was opened at — a path or a url — kept so a runner can
    /// tell whether a child process could reach the same database.
    target: Arc<str>,
    /// what this process has seen this database do, shared by every clone.
    health: Arc<Health>,
}

impl Store {
    /// open (and migrate) the sqlite database at `path`; `":memory:"` works
    /// too.
    pub fn open(path: &str) -> Result<Store, Error> {
        let mut conn = Connection::open(path)?;
        if path != ":memory:" {
            conn.pragma_update(None, "journal_mode", "wal")?;
        }
        conn.busy_timeout(BUSY_TIMEOUT)?;
        migrate(&mut conn)?;
        Ok(Store::new(Db::Sqlite(Mutex::new(conn)), path))
    }

    fn new(db: Db, target: &str) -> Store {
        Store {
            db: Arc::new(db),
            target: target.into(),
            health: Arc::new(Health::default()),
        }
    }

    /// open (and migrate) the postgres database at `url` —
    /// `postgres://user:password@host/database`, as libpq spells it.
    ///
    /// a fresh database is created at the current schema version in one go.
    /// there are no postgres databases in the world that predate this, so
    /// there is nothing for the sqlite chain's accumulated steps to migrate
    /// and walking them would only be a re-enactment.
    #[cfg(feature = "postgres")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
    pub fn connect(url: &str) -> Result<Store, Error> {
        let client = crate::pg::open(url)?;
        Ok(Store::new(Db::Postgres(Mutex::new(client)), url))
    }

    /// whichever of the two `target` names: a `postgres://` url connects, and
    /// anything else is a path. what [`Hestan::db`](crate::Hestan::db) is
    /// handed, and what an [isolated op](crate::Op::isolated)'s child is
    /// handed again so that it opens the same database its parent did.
    pub(crate) fn at(target: &str) -> Result<Store, Error> {
        match PG_SCHEMES.iter().any(|s| target.starts_with(s)) {
            #[cfg(feature = "postgres")]
            true => Store::connect(target),
            #[cfg(not(feature = "postgres"))]
            true => Err(Error::UnsupportedDb(target.to_string())),
            false => Store::open(target),
        }
    }

    /// the connection, with the mutex held. every method below goes through
    /// one of these or a [transaction](Conn::begin) on one.
    fn conn(&self) -> Conn<'_> {
        match &*self.db {
            Db::Sqlite(db) => Conn::Sqlite(db.lock().unwrap()),
            #[cfg(feature = "postgres")]
            Db::Postgres(db) => Conn::Postgres(db.lock().unwrap()),
        }
    }

    /// whether this database lives only in this process's memory, and so
    /// cannot be reached by a child. `":memory:"` is private per connection,
    /// which is exactly right for a test and exactly wrong for an isolated op.
    pub(crate) fn is_private(&self) -> bool {
        &*self.target == ":memory:"
    }

    /// what this process has seen this store do: what it dropped, what it
    /// could not record at all, and whether it is taking writes now.
    pub(crate) fn health(&self) -> &Health {
        &self.health
    }

    /// a write that records authoritative state — what a run did, what an op
    /// did, where a watermark got to. says whether it landed.
    ///
    /// `what` names the write in the log, since the caller is the only thing
    /// that knows which one this is; `write` is the call itself rather than
    /// its result, because a failure here is worth another attempt.
    ///
    /// a [transient](transient) failure is tried again, up to
    /// [`WRITE_ATTEMPTS`] times, on the same pacing an op's retries use. every
    /// write reachable from here is safe to repeat — see the note on
    /// [`transient`] for why that is a property of which errors are retried
    /// rather than a hope about which writes are idempotent.
    ///
    /// every caller has to say what it does when a write did not land, and the
    /// answer is never "carry on as if it had" — `docs/concepts.md` is what
    /// hestan promises about writes, and what it stops promising here.
    pub(crate) async fn landed(&self, what: &str, write: impl Fn() -> Result<(), Error>) -> bool {
        let mut attempt = 0;
        loop {
            let e = match write() {
                Ok(()) => {
                    self.health.wrote();
                    return true;
                }
                Err(e) => e,
            };
            if attempt + 1 == WRITE_ATTEMPTS || !transient(&e) {
                tracing::error!("{what} could not be written: {e}");
                self.health.unrecorded();
                return false;
            }
            tracing::warn!("{what} did not land, trying again: {e}");
            tokio::time::sleep(crate::backoff::jittered_exponential(
                WRITE_BASE, attempt, WRITE_MAX,
            ))
            .await;
            attempt += 1;
        }
    }

    /// a [`BestEffort`] over this store, which is the only way one is made.
    fn best_effort(&self, wrote: Result<(), Error>) -> BestEffort {
        BestEffort {
            wrote,
            health: self.health.clone(),
        }
    }

    /// fail the next `n` writes a run makes, whatever the database would have
    /// said. `0` is a store that works again.
    ///
    /// tests only, and the whole of this phase is about the state it produces:
    /// a store that will not take a write is the one thing a test cannot ask a
    /// working database for. what it fails with is sqlite's "database is
    /// locked" whichever backend is underneath, because what a caller needs
    /// here is a transient failure and not a particular one's spelling.
    #[cfg(test)]
    pub(crate) fn fail_writes(&self, n: u64) {
        *self.health.injected_into.lock().unwrap() = None;
        self.health.injected.store(n, Ordering::SeqCst);
    }

    /// [`fail_writes`](Self::fail_writes) for one write and no others, named
    /// as [`Store::landed`] names it.
    ///
    /// which is what a case asserting where a run *stopped* needs: with every
    /// write failing, a run that carried on regardless would fail its next
    /// write too and leave the same rows behind as one that stopped, and the
    /// case would pass either way.
    #[cfg(test)]
    pub(crate) fn fail_writes_to(&self, what: &'static str, n: u64) {
        *self.health.injected_into.lock().unwrap() = Some(what);
        self.health.injected.store(n, Ordering::SeqCst);
    }

    /// one of the failures [`fail_writes`](Self::fail_writes) asked for, if
    /// any are left and this is the write they were asked for.
    #[cfg(test)]
    fn injected(&self, what: &'static str) -> Option<Error> {
        let into = *self.health.injected_into.lock().unwrap();
        if into.is_some_and(|into| into != what) {
            return None;
        }
        self.health
            .injected
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .ok()
            .map(|_| {
                Error::Sqlite(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(5),
                    Some("database is locked (injected)".to_string()),
                ))
            })
    }

    /// [`create_run_keyed`](Self::create_run_keyed) with no key and no plan,
    /// which is what every launch of a whole job that is not a keyed sensor
    /// request does. tests plant runs through it; the executor calls the keyed
    /// form.
    #[cfg(test)]
    pub(crate) fn create_run(&self, run: &Run, ops: &[String]) -> Result<(), Error> {
        self.create_run_keyed(run, ops, None, None).map(|_| ())
    }

    /// [`create_run`](Self::create_run) with the [run key](RunKey) this run
    /// claims, inserted in the same transaction as the run row.
    ///
    /// same transaction rather than insert-then-delete-on-failure, because the
    /// two failure modes are not equally bad: a duplicate launch is a request
    /// the caller sees twice, while a key left behind for a run that never
    /// launched drops that work forever and nothing ever notices. only a
    /// transaction rules the second one out — a delete on the failure path
    /// still leaves the window where the process dies between the insert and
    /// the launch.
    ///
    /// returns false when the key was already claimed: nothing is written, and
    /// the caller launches nothing.
    /// `plan` is what this run will execute when something claims it — see the
    /// `runs.plan` note on the v14 migration. `None` means the whole job.
    pub(crate) fn create_run_keyed(
        &self,
        run: &Run,
        ops: &[String],
        key: Option<RunKey<'_>>,
        plan: Option<&Value>,
    ) -> Result<bool, Error> {
        let mut conn = self.conn();
        let (insert, ignore) = conn.dialect().insert_or_ignore();
        let mut tx = conn.begin()?;
        if let Some(k) = key {
            let claimed = tx.execute(
                &format!(
                    "{insert} sensor_run_keys (sensor, run_key, run_id, launched_at)
                     VALUES (?1, ?2, ?3, ?4) {ignore}"
                ),
                args![k.sensor, k.key, &run.id, Utc::now().to_rfc3339()],
            )?;
            // dropping the transaction rolls it back, so losing the claim
            // leaves neither a run nor a key behind
            if claimed == 0 {
                return Ok(false);
            }
        }
        tx.execute(
            r#"INSERT INTO runs (id, job, status, "trigger", params, created_at, started_at,
                                 finished_at, error, resumed_from, scheduled_for, tags,
                                 priority, plan, actor)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)"#,
            args![
                &run.id,
                &run.job,
                run.status.as_str(),
                run.trigger.as_str(),
                run.params.to_string(),
                run.created_at.to_rfc3339(),
                run.started_at.map(|t| t.to_rfc3339()),
                run.finished_at.map(|t| t.to_rfc3339()),
                run.error.as_deref(),
                run.resumed_from.as_deref(),
                run.scheduled_for.map(|t| t.to_rfc3339()),
                tags_col(&run.tags),
                run.priority,
                plan.map(|v| v.to_string()),
                run.actor.as_deref(),
            ],
        )?;
        for op in ops {
            tx.execute(
                "INSERT INTO op_runs (run_id, op, status) VALUES (?1, ?2, ?3)",
                args![&run.id, op, OpStatus::Pending.as_str()],
            )?;
        }
        // same transaction as the row, so a run never exists without its queued event
        write_event(
            &mut tx,
            &NewEvent::run(&run.id, EventKind::RunQueued, "run queued")
                .actor(run.actor.as_deref())
                .data(json!({
                    "job": run.job,
                    "trigger": run.trigger,
                    "priority": run.priority,
                    "tags": run.tags,
                })),
            Utc::now(),
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// whether `sensor` has already launched a run under `key`.
    pub(crate) fn run_key_claimed(&self, sensor: &str, key: &str) -> Result<bool, Error> {
        let found = self.conn().query_opt(
            "SELECT 1 FROM sensor_run_keys WHERE sensor = ?1 AND run_key = ?2",
            args![sensor, key],
            |_| Ok(()),
        )?;
        Ok(found.is_some())
    }

    /// drop run keys claimed before `older_than`. nothing collects them on
    /// their own — a sensor keyed by the day would keep a row per day for as
    /// long as the file exists — so they ride the retention knob.
    pub(crate) fn prune_sensor_run_keys(&self, older_than: DateTime<Utc>) -> Result<usize, Error> {
        self.conn().execute(
            "DELETE FROM sensor_run_keys WHERE launched_at < ?1",
            args![older_than.to_rfc3339()],
        )
    }

    /// add a `pending` op_runs row to a run already under way, for one
    /// instance a fan-out just created. the run's own loop is the only caller
    /// and it inserts before spawning, so a row can never land after the run's
    /// terminal status write; ignoring a conflict keeps a repeat harmless.
    pub(crate) fn create_op_run(&self, run_id: &str, op: &str) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("create_op_run") {
                return Err(e);
            }
        }
        let mut conn = self.conn();
        let (insert, ignore) = conn.dialect().insert_or_ignore();
        conn.execute(
            &format!("{insert} op_runs (run_id, op, status) VALUES (?1, ?2, ?3) {ignore}"),
            args![run_id, op, OpStatus::Pending.as_str()],
        )?;
        Ok(())
    }

    /// which runs a boot sweep may declare dead, as a `WHERE` fragment over
    /// `runs` with the current time as `?1`.
    ///
    /// this is where an assumption became a mechanism. boot recovery used to
    /// assume the process starting up was the only one there had ever been, so
    /// every non-terminal run belonged to a process that was gone and every one
    /// of them was over. with a claimable queue that assumption is false and
    /// destructive — a second process starting would have failed a live one's
    /// runs, mid-run, and skipped their ops. what a run's fate turns on now is
    /// its claim:
    ///
    /// - **claimed, lease still good**: somebody is executing it and it is not
    ///   this process. left entirely alone. this is the case the assumption got
    ///   wrong.
    /// - **claimed, lease expired**: its claimer stopped saying it was there.
    ///   dead, and swept.
    /// - **`running` with no claim**: written before the queue existed, by a
    ///   process that is gone. dead, and swept.
    /// - **`queued` with no claim**: not a casualty — the queue. left for a
    ///   dispatcher, which is the whole point of making it durable.
    const INTERRUPTED: &'static str = "status IN ('queued', 'running') AND (
             (claimed_by IS NOT NULL AND (lease_until IS NULL OR lease_until < ?1))
             OR (claimed_by IS NULL AND status = 'running'))";

    /// mark runs left behind by a dead process as failed; called at startup.
    /// lease-aware: see [`INTERRUPTED`](Self::INTERRUPTED) for what that means
    /// and why it has to be.
    pub(crate) fn fail_interrupted(&self) -> Result<(), Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let now = Utc::now().to_rfc3339();
        let doomed = format!("SELECT id FROM runs WHERE {}", Self::INTERRUPTED);
        tx.execute(
            &format!(
                "UPDATE op_runs SET
                     status = CASE status WHEN 'running' THEN 'failed' ELSE 'skipped' END,
                     error = CASE status WHEN 'running' THEN 'interrupted: process exited'
                             ELSE error END,
                     finished_at = ?1
                 WHERE status IN ('pending', 'running') AND run_id IN ({doomed})"
            ),
            args![&now],
        )?;
        tx.execute(
            &format!(
                "INSERT INTO events (run_id, subject_kind, level, kind, message, ts)
                 SELECT id, 'run', 'error', 'run_failed', 'run interrupted: process exited', ?1
                 FROM runs WHERE {}",
                Self::INTERRUPTED
            ),
            args![&now],
        )?;
        tx.execute(
            &format!(
                "UPDATE runs SET status = 'failed', finished_at = ?1, lease_until = NULL,
                     error = COALESCE(error, 'interrupted: process exited')
                 WHERE {}",
                Self::INTERRUPTED
            ),
            args![&now],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// whether `job` has work outstanding: a run of it queued or running,
    /// claimed or not.
    ///
    /// deliberately unchanged by the queue, and the reason is worth writing
    /// down. this is what [`Overlap`](crate::Overlap) asks, and what the
    /// backfill chunker and the asset build endpoints ask, and every one of
    /// them means "does this job already have a run outstanding" rather than
    /// "is one executing this second". a queued run that nobody has claimed
    /// yet is outstanding: a schedule that ignored it would enqueue another
    /// every minute a limit held the first one back, which is exactly the
    /// pile-up `Overlap::Skip` exists to prevent, and a backfill that ignored
    /// it would fire every chunk of a 400-day range at once.
    ///
    /// [`Limits`](crate::Limits) asks the other question, and counts the other
    /// set.
    pub(crate) fn has_active_run(&self, job: &str) -> Result<bool, Error> {
        let found = self.conn().query_opt(
            "SELECT 1 FROM runs WHERE job = ?1 AND status IN ('queued', 'running') LIMIT 1",
            args![job],
            |_| Ok(()),
        )?;
        Ok(found.is_some())
    }

    /// take the best queued run this claimer is allowed to start, and claim it.
    ///
    /// "best" is highest priority first, ties by `created_at`, **skipping any
    /// run a limit would break**. skipping is deliberate: head-of-line blocking
    /// — a `env:prod` run at the front of the queue stopping every unrelated
    /// run behind it — is worse than a priority order that is a preference
    /// rather than a promise. it does mean priority is not strict, and that is
    /// documented where a user meets it.
    ///
    /// the claim itself is `UPDATE ... WHERE claimed_by IS NULL`: one winner by
    /// construction, whoever else is reaching for the same row. that holds on
    /// both backends, and it is what makes a race here a thing that resolves
    /// rather than a thing to be avoided.
    ///
    /// how the two get there differs, and it is the whole reason to run this
    /// on postgres. sqlite serializes writers for us: an immediate transaction
    /// is the only one there is, which is a complete guarantee on one host and
    /// none at all across several. postgres reserves the one row this claimer
    /// decided on with [`SKIP LOCKED`](Dialect::claim_lock), so several
    /// dispatchers walk the same queue at the same moment and come away with
    /// different runs, none of them waiting on any other.
    ///
    /// counting capacity and spending it still have to be one decision, and on
    /// postgres one transaction does not make them one — two snapshots can
    /// each see the same last slot free. so when a limit is actually in force,
    /// and only then, claimers [take turns](Tx::take_turns).
    ///
    /// returns the claimed run and its [plan](Self::create_run_keyed).
    pub(crate) fn claim_next(
        &self,
        claimer: &str,
        lease: Duration,
        limits: &Limits,
        defined: &HashSet<String>,
    ) -> Result<Option<(Run, Option<Value>)>, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        if limits.binding() {
            tx.take_turns()?;
        }
        let counts = in_flight(&mut tx, limits)?;
        let candidates = queued(&mut tx, QUEUE_SCAN)?;
        let reserve = format!(
            "SELECT id FROM runs
             WHERE id = ?1 AND claimed_by IS NULL AND status = 'queued' {}",
            tx.dialect().claim_lock()
        );
        let now = Utc::now();
        let until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        for (mut run, plan) in candidates {
            if !defined.contains(&run.job) {
                continue;
            }
            if counts.blocker(limits, &run.job, &run.tags).is_some() {
                continue;
            }
            if tx
                .query_opt(&reserve, args![&run.id], |_| Ok(()))?
                .is_none()
            {
                continue;
            }
            let won = tx.execute(
                "UPDATE runs SET claimed_by = ?1, claimed_at = ?2, lease_until = ?3
                 WHERE id = ?4 AND claimed_by IS NULL AND status = 'queued'",
                args![claimer, now.to_rfc3339(), until.to_rfc3339(), &run.id],
            )?;
            if won == 0 {
                continue;
            }
            tx.commit()?;
            run.claimed_by = Some(claimer.to_string());
            run.claimed_at = Some(now);
            run.lease_until = Some(until);
            return Ok(Some((run, plan)));
        }
        tx.commit()?;
        Ok(None)
    }

    /// the queue as somebody looking at it wants it: in the order a dispatcher
    /// would take them, each with what is holding it back.
    ///
    /// the walk is the dispatcher's own, dry: a run that would start counts
    /// against the limits for everything behind it, so what this reports is
    /// what the next pass will actually do rather than a per-run guess that
    /// would call the whole queue unblocked.
    pub(crate) fn queue(
        &self,
        limits: &Limits,
        defined: &HashSet<String>,
        limit: u32,
    ) -> Result<Vec<Queued>, Error> {
        let mut conn = self.conn();
        let mut counts = in_flight(&mut conn, limits)?;
        let mut out = Vec::new();
        for (position, (run, _)) in queued(&mut conn, limit)?.into_iter().enumerate() {
            let blocked = match defined.contains(&run.job) {
                false => Some(Blocked::Undefined(run.job.clone())),
                true => counts.blocker(limits, &run.job, &run.tags),
            };
            if blocked.is_none() {
                counts.take(limits, &run.job, &run.tags);
            }
            out.push(Queued {
                run,
                position: position + 1,
                blocked,
            });
        }
        Ok(out)
    }

    /// the queue in the order a dispatcher would take it, and nothing about
    /// why anything is waiting.
    ///
    /// that omission is the whole reason this exists beside
    /// [`queue`](Self::queue). the blame belongs to whoever owns the limits,
    /// and a reader that has only opened the database owns none: it would
    /// either have to invent them, and report a queue that nothing is holding
    /// back, or report every job as undefined because this process defines
    /// none. saying only what it knows is the third option.
    /// runs whose claimer stopped saying it was still there: claimed, not
    /// terminal, and past the lease it was holding them under.
    ///
    /// a deployment with a process running is already reclaiming these on its
    /// lease loop, so finding any means either that the loop is behind or that
    /// nothing is running one — and the second is the case where they sit there
    /// forever, which is exactly the thing worth being told about.
    #[cfg(any(test, feature = "cli"))]
    pub(crate) fn stalled_claims(&self, now: DateTime<Utc>) -> Result<Vec<Run>, Error> {
        self.conn().query(
            &format!(
                "SELECT {RUN_COLS} FROM runs
                 WHERE claimed_by IS NOT NULL AND status IN ('queued', 'running')
                   AND lease_until IS NOT NULL AND lease_until < ?1
                 ORDER BY lease_until"
            ),
            args![now.to_rfc3339()],
            run_from_row,
        )
    }

    /// the schema version this database is at, which after an open is always
    /// the one this build writes — the number is worth reporting rather than
    /// checking, since a database from the future refuses to open at all.
    #[cfg(any(test, feature = "cli"))]
    pub(crate) fn schema_version(&self) -> Result<u32, Error> {
        let mut conn = self.conn();
        let sql = match conn.dialect() {
            Dialect::Sqlite => "PRAGMA user_version",
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "SELECT version FROM schema_version",
        };
        let version = conn.query_opt(sql, args![], |r| r.int(0))?;
        Ok(version.unwrap_or_default() as u32)
    }

    /// whether this database would take a write, asked without making one.
    ///
    /// a transaction opened for writing and rolled back: on sqlite that takes
    /// the write lock, so it answers for the file's permissions *and* for
    /// another writer holding it past the [busy timeout](BUSY_TIMEOUT); on
    /// postgres it answers for the connection. neither leaves a row behind,
    /// which is what lets `doctor` ask it of a live deployment's database.
    ///
    /// this is the question a process that has not written anything yet cannot
    /// answer from its [health](Store::health) — those counters are what *this*
    /// process has seen, and a command line that just started has seen nothing.
    #[cfg(any(test, feature = "cli"))]
    pub(crate) fn writable(&self) -> Result<(), Error> {
        let mut conn = self.conn();
        let tx = conn.begin_immediate()?;
        // dropped rather than committed: there is nothing in it, and asking is
        // not a reason to write to somebody's run log
        drop(tx);
        Ok(())
    }

    /// which backend this is and where, for a line that says what was opened.
    #[cfg(any(test, feature = "cli"))]
    pub(crate) fn backend(&self) -> &'static str {
        match self.conn().dialect() {
            Dialect::Sqlite => "sqlite",
            #[cfg(feature = "postgres")]
            Dialect::Postgres => "postgres",
        }
    }

    #[cfg(any(test, feature = "cli"))]
    pub(crate) fn queue_rows(&self, limit: u32) -> Result<Vec<Run>, Error> {
        let rows = queued(&mut self.conn(), limit)?;
        Ok(rows.into_iter().map(|(run, _)| run).collect())
    }

    /// how many runs are queued and unclaimed, which is what "queue depth"
    /// means. counted rather than taken from [`queue`](Self::queue), which caps.
    pub(crate) fn queue_depth(&self) -> Result<usize, Error> {
        let n = self.conn().query_opt(
            "SELECT COUNT(*) FROM runs WHERE status = 'queued' AND claimed_by IS NULL",
            args![],
            |r| r.size(0),
        )?;
        Ok(n.unwrap_or_default())
    }

    /// move a run up or down the queue. false when there is no such run, and
    /// [`Error::RunActive`] once something has claimed it — by then the
    /// priority has already been spent.
    pub(crate) fn set_run_priority(&self, id: &str, priority: i64) -> Result<bool, Error> {
        let mut conn = self.conn();
        let found = conn.query_opt(
            "SELECT status, claimed_by FROM runs WHERE id = ?1",
            args![id],
            |r| Ok((r.text(0)?, r.opt_text(1)?)),
        )?;
        let Some((status, claimed_by)) = found else {
            return Ok(false);
        };
        if status != RunStatus::Queued.as_str() || claimed_by.is_some() {
            return Err(Error::RunActive(id.to_string()));
        }
        conn.execute(
            "UPDATE runs SET priority = ?2 WHERE id = ?1 AND claimed_by IS NULL",
            args![id, priority],
        )?;
        Ok(true)
    }

    /// say that `claimer` is still here, for every run it holds — except the
    /// ones in `given_up`, which are the runs it has stopped executing because
    /// it could not record them.
    ///
    /// leaving those out is the whole of how a lease lapses on purpose. a
    /// process that keeps renewing a claim it has abandoned holds that run
    /// out of every reclaimer's reach for as long as the process lives, which
    /// is the one outcome worse than the failure that got it there.
    ///
    /// returns how many leases moved, which is how many runs this process is
    /// still executing.
    pub(crate) fn renew_leases(
        &self,
        claimer: &str,
        lease: Duration,
        given_up: &[String],
    ) -> Result<usize, Error> {
        let until = Utc::now() + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        let mut args: Vec<Val<'_>> = vec![Val::from(claimer), Val::from(until.to_rfc3339())];
        // numbered from 3, since the claimer and the new lease are already
        // bound; an empty list is no clause rather than an empty `IN ()`
        let except = match given_up.is_empty() {
            true => String::new(),
            false => {
                let list: Vec<String> = (3..3 + given_up.len()).map(|i| format!("?{i}")).collect();
                args.extend(given_up.iter().map(Val::from));
                format!(" AND id NOT IN ({})", list.join(", "))
            }
        };
        let moved = self.conn().execute(
            &format!(
                "UPDATE runs SET lease_until = ?2
                 WHERE claimed_by = ?1 AND status IN ('queued', 'running'){except}"
            ),
            &args,
        );
        // the loop that calls this every fifteen seconds is also the cheapest
        // thing hestan has that says whether the store is back
        match &moved {
            Ok(_) => self.health.wrote(),
            Err(_) => self.health.unrecorded(),
        }
        moved
    }

    /// the runs `claimer` currently holds, so a process can say what it is
    /// executing and anyone else can tell who holds what.
    pub(crate) fn held_by(&self, claimer: &str) -> Result<Vec<String>, Error> {
        self.conn().query(
            "SELECT id FROM runs
             WHERE claimed_by = ?1 AND status IN ('queued', 'running') ORDER BY created_at",
            args![claimer],
            |r| r.text(0),
        )
    }

    /// take back every run whose claimer stopped saying it was there, and
    /// either fail it or put it back on the queue.
    ///
    /// its ops are marked either way, and with the reason: an op left `running`
    /// by a process that vanished did not finish, and a row that says otherwise
    /// is what the next resume would build on. returns `(run id, the claimer
    /// that went away)` for each.
    /// `note` is asked for the [durable notification](Self::run_finished) each
    /// failed run owes, and is handed the row as this transaction leaves it —
    /// so a reclaimed run's alert is written with its terminal status exactly
    /// as an ordinary one's is, rather than a statement later.
    pub(crate) fn reclaim_expired(
        &self,
        policy: Reclaim,
        note: impl Fn(&Run) -> Option<Value>,
    ) -> Result<Vec<Run>, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let at = Utc::now();
        let now = at.to_rfc3339();
        let mut expired: Vec<Run> = tx.query(
            &format!(
                "SELECT {RUN_COLS} FROM runs
                 WHERE claimed_by IS NOT NULL AND status IN ('queued', 'running')
                   AND (lease_until IS NULL OR lease_until < ?1)"
            ),
            args![&now],
            run_from_row,
        )?;
        for run in &mut expired {
            let id = run.id.clone();
            let claimer = run.claimed_by.clone().unwrap_or_default();
            let why = format!("claimer went away: {claimer} stopped renewing its lease");
            tx.execute(
                "UPDATE op_runs SET
                     status = CASE status WHEN 'running' THEN 'failed' ELSE 'skipped' END,
                     error = CASE status WHEN 'running' THEN ?2 ELSE error END,
                     finished_at = ?3, pid = NULL
                 WHERE run_id = ?1 AND status IN ('pending', 'running')",
                args![&id, &why, &now],
            )?;
            // the reclaim is its own fact whichever policy is in force: the run
            // ends up failed or requeued, and "somebody's lease ran out" is the
            // thing you are looking for when you ask why
            let message = match policy {
                Reclaim::Fail => why.clone(),
                Reclaim::Requeue => format!("{why}; requeued for another claimer"),
            };
            write_event(
                &mut tx,
                &NewEvent::run(&id, EventKind::RunReclaimed, message)
                    .level(EventLevel::Warn)
                    .data(json!({ "claimer": claimer, "policy": policy })),
                at,
            )?;
            match policy {
                Reclaim::Fail => {
                    tx.execute(
                        "UPDATE runs SET status = 'failed', finished_at = ?2, lease_until = NULL,
                             error = COALESCE(error, ?3)
                         WHERE id = ?1",
                        args![&id, &now, &why],
                    )?;
                    // and the run's own terminal event after it: the reclaim is
                    // why, and this is what the run did
                    write_event(
                        &mut tx,
                        &NewEvent::run(&id, EventKind::RunFailed, &*why).level(EventLevel::Error),
                        at,
                    )?;
                    run.status = RunStatus::Failed;
                    run.finished_at = Some(at);
                    run.lease_until = None;
                    run.error.get_or_insert(why);
                    if let Some(payload) = note(run) {
                        queue_note(&mut tx, &payload, at)?;
                    }
                }
                // back to exactly what an unclaimed queued run is: no owner, no
                // lease, and no start time it turned out not to have had
                Reclaim::Requeue => {
                    tx.execute(
                        "UPDATE runs SET status = 'queued', claimed_by = NULL, claimed_at = NULL,
                             lease_until = NULL, started_at = NULL
                         WHERE id = ?1",
                        args![&id],
                    )?;
                    run.status = RunStatus::Queued;
                    run.claimed_at = None;
                    run.lease_until = None;
                    run.started_at = None;
                }
            }
        }
        tx.commit()?;
        Ok(expired)
    }

    /// write the claim another process would have written. tests only: this is
    /// the one thing a single process cannot do to itself honestly.
    #[cfg(test)]
    pub(crate) fn plant_claim(
        &self,
        id: &str,
        claimer: &str,
        lease_until: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.conn().execute(
            "UPDATE runs SET claimed_by = ?2, claimed_at = ?3, lease_until = ?4 WHERE id = ?1",
            args![
                id,
                claimer,
                Utc::now().to_rfc3339(),
                lease_until.map(|t| t.to_rfc3339())
            ],
        )?;
        Ok(())
    }

    /// cancel a run out of the queue: only one that nobody has claimed, and
    /// atomically, so a claimer racing this either wins the run or finds it
    /// canceled. false means it was claimed in the meantime and has to be
    /// stopped the ordinary way.
    pub(crate) fn cancel_queued(&self, id: &str, actor: Option<&str>) -> Result<bool, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let at = Utc::now();
        let now = at.to_rfc3339();
        let taken = tx.execute(
            "UPDATE runs SET status = 'canceled', finished_at = ?2,
                 error = COALESCE(error, 'canceled before it started')
             WHERE id = ?1 AND status = 'queued' AND claimed_by IS NULL",
            args![id, &now],
        )?;
        if taken == 0 {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute(
            "UPDATE op_runs SET status = 'canceled', error = 'canceled before it started',
                 finished_at = ?2
             WHERE run_id = ?1 AND status = 'pending'",
            args![id, &now],
        )?;
        write_event(
            &mut tx,
            &NewEvent::run(id, EventKind::RunCanceled, "canceled before it started")
                .level(EventLevel::Warn)
                .actor(actor),
            at,
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// somebody asked a run that is already executing to stop.
    ///
    /// its terminal event belongs to whichever process is executing it, which
    /// may not be this one and does not know who asked — so the request is a
    /// line of its own, written here, and it is the line the audit trail
    /// reads. an unauthenticated deployment writes it with no actor, which is
    /// still true: something asked.
    /// best-effort: what stops the run is the signal, not this line, and the
    /// terminal row the executing process writes is the record either way.
    pub(crate) fn cancel_requested(&self, id: &str, actor: Option<&str>) -> BestEffort {
        let message = match actor {
            Some(who) => format!("cancel requested by {who}"),
            None => "cancel requested".to_string(),
        };
        self.best_effort(write_event(
            &mut self.conn(),
            &NewEvent::run(id, EventKind::Log, message).actor(actor),
            Utc::now(),
        ))
    }

    /// `at` is passed in rather than read here so the row and the event the
    /// executor hands its hooks carry the same instant.
    pub(crate) fn run_started(&self, id: &str, at: DateTime<Utc>) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("run_started") {
                return Err(e);
            }
        }
        self.conn().execute(
            "UPDATE runs SET status = ?1, started_at = ?2 WHERE id = ?3",
            args![RunStatus::Running.as_str(), at.to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// `error` is the run's own failure summary: the first op that terminally
    /// failed, named. `None` leaves any existing value alone.
    ///
    /// `note` is the [durable notification](Self::queue_notification) this
    /// run's terminal event owes, and it goes in **this** transaction. that is
    /// the whole of what durable delivery buys: written afterwards, a crash in
    /// the gap leaves a failed run nothing ever alerted about and no record
    /// that anything should have. `None` is every process that did not ask for
    /// durable notifications, which is the default.
    pub(crate) fn run_finished(
        &self,
        id: &str,
        status: RunStatus,
        error: Option<&str>,
        at: DateTime<Utc>,
        note: Option<&Value>,
    ) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("run_finished") {
                return Err(e);
            }
        }
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        // and the lease with it: there is nothing left to renew, and a run that
        // is over must never look reclaimable
        tx.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2, error = COALESCE(?3, error),
                 lease_until = NULL
             WHERE id = ?4",
            args![status.as_str(), at.to_rfc3339(), error, id],
        )?;
        if let Some(note) = note {
            queue_note(&mut tx, note, at)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// the undelivered notifications due at `now`, oldest first.
    pub(crate) fn due_notifications(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<Notification>, Error> {
        self.conn().query(
            &format!(
                "SELECT {NOTIFICATION_COLS} FROM notifications
                 WHERE delivered_at IS NULL AND next_attempt_at <= ?1
                 ORDER BY id LIMIT ?2"
            ),
            args![now.to_rfc3339(), limit],
            notification_from_row,
        )
    }

    /// what `state` covers, newest first. `None` is all of them.
    pub fn notifications(
        &self,
        state: Option<DeliveryState>,
        limit: u32,
    ) -> Result<Vec<Notification>, Error> {
        let filter = match state {
            None => "1 = 1",
            Some(DeliveryState::Delivered) => "delivered_at IS NOT NULL",
            Some(DeliveryState::Pending) => "delivered_at IS NULL AND next_attempt_at IS NOT NULL",
            Some(DeliveryState::Failed) => "delivered_at IS NULL AND next_attempt_at IS NULL",
        };
        self.conn().query(
            &format!(
                "SELECT {NOTIFICATION_COLS} FROM notifications
                 WHERE {filter} ORDER BY id DESC LIMIT ?1"
            ),
            args![limit],
            notification_from_row,
        )
    }

    /// mark one delivered. guarded on it not already being so, which is what
    /// makes a second delivery loop unable to claim the same row twice —
    /// though it says nothing about the *hook* having run twice, and hestan
    /// promises at-least-once and no more.
    ///
    /// the [event](EventKind::NotificationDelivered) is in the same
    /// transaction as the mark and is therefore about the mark, not about the
    /// hook: the hook already returned, and the gap between it returning and
    /// this landing is the at-least-once window this method's guard exists for.
    pub(crate) fn delivered(&self, id: i64, at: DateTime<Utc>) -> Result<bool, Error> {
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        let marked = tx.execute(
            "UPDATE notifications SET delivered_at = ?1, last_error = NULL
             WHERE id = ?2 AND delivered_at IS NULL",
            args![at.to_rfc3339(), id],
        )?;
        if marked > 0 {
            write_event(
                &mut tx,
                &NewEvent::about(
                    SubjectKind::System,
                    id.to_string(),
                    EventKind::NotificationDelivered,
                    format!("notification {id} delivered"),
                )
                .data(json!({ "notification_id": id })),
                at,
            )?;
        }
        tx.commit()?;
        Ok(marked > 0)
    }

    /// record a failed attempt. `next` of `None` is giving up: the row leaves
    /// the due scan and stays visible as failed, carrying the error that
    /// stopped it — and only *that* gets an event. an attempt that will be
    /// retried in ninety seconds is not news, and eight of them per alert
    /// would bury the one that matters.
    pub(crate) fn delivery_failed(
        &self,
        id: i64,
        attempts: u32,
        next: Option<DateTime<Utc>>,
        error: &str,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        tx.execute(
            "UPDATE notifications SET attempts = ?1, next_attempt_at = ?2, last_error = ?3
             WHERE id = ?4",
            args![attempts, next.map(|t| t.to_rfc3339()), error, id],
        )?;
        if next.is_none() {
            write_event(
                &mut tx,
                &NewEvent::about(
                    SubjectKind::System,
                    id.to_string(),
                    EventKind::NotificationFailed,
                    format!("notification {id} given up on after {attempts} attempts: {error}"),
                )
                .level(EventLevel::Error)
                .data(json!({
                    "notification_id": id,
                    "attempts": attempts,
                    "error": error,
                })),
                at,
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// put a delivered notification back where a crash between the hook
    /// returning and the mark landing would have left it. tests only: this is
    /// the one thing a process cannot do to itself honestly.
    #[cfg(test)]
    pub(crate) fn undeliver(&self, id: i64, due: DateTime<Utc>) -> Result<(), Error> {
        self.conn().execute(
            "UPDATE notifications SET delivered_at = NULL, next_attempt_at = ?1 WHERE id = ?2",
            args![due.to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// drop delivered notifications older than `older_than`. undelivered ones
    /// stay whatever their age — an alert nobody received is not history, it is
    /// something outstanding, and a sweep that quietly cleared it would be the
    /// same loss this table exists to prevent.
    pub(crate) fn prune_notifications(&self, older_than: DateTime<Utc>) -> Result<usize, Error> {
        self.conn().execute(
            "DELETE FROM notifications WHERE delivered_at IS NOT NULL AND delivered_at < ?1",
            args![older_than.to_rfc3339()],
        )
    }

    pub(crate) fn op_started(&self, run_id: &str, op: &str, attempts: u32) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("op_started") {
                return Err(e);
            }
        }
        // coalesce so retries keep the first attempt's start time. the finish
        // and the error are cleared: a fresh attempt has neither, and an
        // isolated op's child records its failure on this row before the parent
        // decides to retry it
        self.conn().execute(
            "UPDATE op_runs SET status = ?1, attempts = ?2, started_at = COALESCE(started_at, ?3),
                 finished_at = NULL, error = NULL
             WHERE run_id = ?4 AND op = ?5",
            args![
                OpStatus::Running.as_str(),
                attempts,
                Utc::now().to_rfc3339(),
                run_id,
                op
            ],
        )?;
        Ok(())
    }

    /// the op's terminal write. `metadata` is whatever the successful attempt
    /// staged with `ctx.meta`, committed here so an op run never claims facts
    /// about work that did not finish.
    ///
    /// `built` is what an [asset](crate::Asset) op produced, and it goes in
    /// this transaction for the same reason: a materialization says the asset
    /// is current, which is a claim about an op that succeeded and an output
    /// that was stored. written before this and the claim outlives every way
    /// the op can still fail; written after it and a crash in the gap leaves a
    /// build nothing recorded. an op that produces several assets writes all
    /// of them here or none of them.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn op_finished(
        &self,
        run_id: &str,
        op: &str,
        status: OpStatus,
        output: Option<&Value>,
        metadata: Option<&Value>,
        error: Option<&str>,
        built: &[Built],
    ) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("op_finished") {
                return Err(e);
            }
        }
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        // pid goes with it: the row says where an op is running, and nothing is
        // running once this write lands
        tx.execute(
            "UPDATE op_runs SET status = ?1, finished_at = ?2, output = ?3, metadata = ?4,
                 error = ?5, pid = NULL
             WHERE run_id = ?6 AND op = ?7",
            args![
                status.as_str(),
                at.to_rfc3339(),
                output.map(|v| v.to_string()),
                metadata.map(|v| v.to_string()),
                error,
                run_id,
                op,
            ],
        )?;
        for one in built {
            write_materialization(&mut tx, one, Some(run_id), at)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// mark an op canceled without claiming a finish time: cancellation was
    /// requested and the task never joined, so when — or whether — the work
    /// stopped is exactly what this process does not know.
    pub(crate) fn op_unstopped(&self, run_id: &str, op: &str, error: &str) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("op_unstopped") {
                return Err(e);
            }
        }
        self.conn().execute(
            "UPDATE op_runs SET status = ?1, finished_at = NULL, output = NULL, metadata = NULL,
                 error = ?2, pid = NULL
             WHERE run_id = ?3 AND op = ?4",
            args![OpStatus::Canceled.as_str(), error, run_id, op],
        )?;
        Ok(())
    }

    /// record the child process an [isolated](crate::Op::isolated) op is
    /// running in.
    ///
    /// guarded on `running`, because a fast child can record its own terminal
    /// row before the parent gets here — and a pid written onto a finished op
    /// would name a process that no longer exists.
    ///
    /// best-effort: the pid is for whoever is *looking* at the run. the parent
    /// holds the child handle it stops the process with, and drops it — which
    /// kills the child — whether or not this row says where it was.
    pub(crate) fn op_spawned(&self, run_id: &str, op: &str, pid: u32) -> BestEffort {
        self.best_effort(
            self.conn()
                .execute(
                    "UPDATE op_runs SET pid = ?1
                     WHERE run_id = ?2 AND op = ?3 AND status = 'running'",
                    args![pid, run_id, op],
                )
                .map(|_| ()),
        )
    }

    /// record what an isolated op is being handed, before the child that reads
    /// it exists. see the `op_runs.inputs` note on the v13 migration.
    pub(crate) fn set_op_inputs(
        &self,
        run_id: &str,
        op: &str,
        inputs: &Value,
    ) -> Result<(), Error> {
        self.conn().execute(
            "UPDATE op_runs SET inputs = ?1 WHERE run_id = ?2 AND op = ?3",
            args![inputs.to_string(), run_id, op],
        )?;
        Ok(())
    }

    /// what the parent recorded for this op, read by the child that runs it.
    pub(crate) fn op_inputs(&self, run_id: &str, op: &str) -> Result<Option<Value>, Error> {
        let inputs = self.conn().query_opt(
            "SELECT inputs FROM op_runs WHERE run_id = ?1 AND op = ?2",
            args![run_id, op],
            |r| r.opt_json(0),
        )?;
        Ok(inputs.flatten())
    }

    /// one event about a run, on its own.
    ///
    /// the run's own progress is the one part of the log with nothing to be
    /// atomic *with*: an op starting is not a row anywhere else, so this is a
    /// statement of its own and a crash between the work and the event loses
    /// the event. the terminal ones are not written here — a run's queued and
    /// finished rows go in the transaction that moves the run.
    ///
    /// which is also why it is best-effort: an event narrates a run and no
    /// part of the run turns on one. the row it narrates is written somewhere
    /// else, by something that does not carry on without it.
    pub(crate) fn append_event(
        &self,
        run_id: &str,
        op: Option<&str>,
        level: EventLevel,
        kind: EventKind,
        message: &str,
        data: Option<&Value>,
    ) -> BestEffort {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("append_event") {
                return self.best_effort(Err(e));
            }
        }
        let mut event = NewEvent::run(run_id, kind, message).op(op).level(level);
        if let Some(data) = data {
            event = event.data(data.clone());
        }
        self.best_effort(write_event(&mut self.conn(), &event, Utc::now()))
    }

    /// append one captured line. the cap lives in [`logs::Budget`], which is
    /// the only thing that calls this — writing here directly would be a way
    /// to fill a disk.
    ///
    /// best-effort, and the clearest case of it: a line an op printed is worth
    /// keeping and worth nothing beside the op's own row.
    pub(crate) fn append_op_log(
        &self,
        at: &Attempt,
        source: Source<'_>,
        message: &str,
    ) -> BestEffort {
        let (stream, level, target) = source.columns();
        self.best_effort(
            self.conn()
                .execute(
                    "INSERT INTO op_logs (run_id, op, attempt, at, stream, level, target, message)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    args![
                        &at.run_id,
                        &at.op,
                        at.attempt,
                        Utc::now().to_rfc3339(),
                        stream,
                        level,
                        target,
                        message
                    ],
                )
                .map(|_| ()),
        )
    }

    /// captured output for one run, oldest first, after cursor `after`.
    ///
    /// `op` narrows to one op, `limit` bounds the page. cursored on `id` like
    /// [`events`](Self::events) is on `seq`: ids only go up, so the last one a
    /// caller saw is the whole of what it has to remember.
    pub fn op_logs(
        &self,
        run_id: &str,
        op: Option<&str>,
        after: i64,
        limit: u32,
    ) -> Result<Vec<OpLog>, Error> {
        let mut conn = self.conn();
        let opt = conn.dialect().text_param();
        conn.query(
            &format!(
                "SELECT id, run_id, op, attempt, at, stream, level, target, message
                 FROM op_logs
                 WHERE run_id = ?1 AND (?2{opt} IS NULL OR op = ?2) AND id > ?3
                 ORDER BY id LIMIT ?4"
            ),
            args![run_id, op, after, limit],
            op_log_from_row,
        )
    }

    /// make the schedules table mirror the code: insert new (job, expr) pairs,
    /// refresh tz and params on existing ones (pause state survives), drop the
    /// rest.
    pub(crate) fn sync_schedules(&self, defined: &[Schedule]) -> Result<(), Error> {
        let mut conn = self.conn();
        let (insert, ignore) = conn.dialect().insert_or_ignore();
        let mut tx = conn.begin()?;
        for s in defined {
            let declared = s.params.to_string();
            let catchup = s.catchup.to_string();
            tx.execute(
                &format!(
                    "{insert} schedules (job, expr, tz, params, catchup)
                     VALUES (?1, ?2, ?3, ?4, ?5) {ignore}"
                ),
                args![&s.job, &s.expr, &s.tz, &declared, &catchup],
            )?;
            // the cursor is deliberately not touched: it is what the scheduler
            // knows about this pair, and a restart that rewrote it would be a
            // restart that forgot the downtime it is meant to detect
            tx.execute(
                "UPDATE schedules SET tz = ?3, params = ?4, catchup = ?5
                 WHERE job = ?1 AND expr = ?2",
                args![&s.job, &s.expr, &s.tz, &declared, &catchup],
            )?;
        }
        let existing: Vec<(String, String)> =
            tx.query("SELECT job, expr FROM schedules", args![], |r| {
                Ok((r.text(0)?, r.text(1)?))
            })?;
        let keep: HashSet<(&str, &str)> = defined
            .iter()
            .map(|s| (s.job.as_str(), s.expr.as_str()))
            .collect();
        for (job, expr) in &existing {
            if !keep.contains(&(job.as_str(), expr.as_str())) {
                tx.execute(
                    "DELETE FROM schedules WHERE job = ?1 AND expr = ?2",
                    args![job, expr],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// every stored schedule, by job then expression. what is here is what a
    /// past start declared, so a process that has not synced its own
    /// declarations yet still reports the previous ones.
    pub fn schedules(&self) -> Result<Vec<ScheduleRow>, Error> {
        self.conn().query(
            "SELECT job, expr, tz, paused, params, catchup, cursor
             FROM schedules ORDER BY job, expr",
            args![],
            |r| {
                Ok(ScheduleRow {
                    job: r.text(0)?,
                    expr: r.text(1)?,
                    tz: r.text(2)?,
                    paused: r.flag(3)?,
                    params: r.json(4)?,
                    catchup: r.parse(5)?,
                    cursor: r.opt_ts(6)?,
                })
            },
        )
    }

    /// returns false if the (job, expr) pair isn't registered.
    /// pause or unpause one schedule, and record who did.
    ///
    /// the event is in the same transaction as the flag for the reason every
    /// event here is: a log that says a schedule was paused when it was not,
    /// or is silent about one that was, is worse than no log.
    pub fn set_schedule_paused(
        &self,
        job: &str,
        expr: &str,
        paused: bool,
        actor: Option<&str>,
    ) -> Result<bool, Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let n = tx.execute(
            "UPDATE schedules SET paused = ?3 WHERE job = ?1 AND expr = ?2",
            args![job, expr, paused],
        )?;
        if n == 0 {
            tx.commit()?;
            return Ok(false);
        }
        let verb = if paused { "paused" } else { "resumed" };
        write_event(
            &mut tx,
            &NewEvent::about(
                SubjectKind::Schedule,
                job,
                EventKind::SchedulePaused,
                format!("schedule {expr} on {job} {verb}"),
            )
            .actor(actor)
            .data(json!({ "expr": expr, "paused": paused })),
            at,
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// move a schedule's cursor to `at`, never backwards. rfc3339 utc sorts
    /// lexicographically, so the guard is a plain string compare — and it is
    /// what lets a held fire drain long after its occurrence without
    /// un-accounting for everything since.
    pub(crate) fn set_schedule_cursor(
        &self,
        job: &str,
        expr: &str,
        at: DateTime<Utc>,
    ) -> Result<(), Error> {
        self.conn().execute(
            "UPDATE schedules SET cursor = ?3
             WHERE job = ?1 AND expr = ?2 AND (cursor IS NULL OR cursor < ?3)",
            args![job, expr, at.to_rfc3339()],
        )?;
        Ok(())
    }

    /// every fire still waiting to launch, oldest occurrence first: a
    /// `deferred` tick with no later tick for the same `(job, expr,
    /// scheduled_for)`. the tick log *is* the queue — a fire held in memory
    /// dies with the process, and this one does not.
    pub(crate) fn pending_fires(&self) -> Result<Vec<(String, String, DateTime<Utc>)>, Error> {
        self.conn().query(
            "SELECT job, expr, scheduled_for FROM schedule_ticks d
             WHERE d.outcome = 'deferred'
               AND NOT EXISTS (
                   SELECT 1 FROM schedule_ticks f
                   WHERE f.job = d.job AND f.expr = d.expr
                     AND f.scheduled_for = d.scheduled_for AND f.id > d.id
               )
             ORDER BY d.scheduled_for, d.id",
            args![],
            |r| Ok((r.text(0)?, r.text(1)?, r.ts(2)?)),
        )
    }

    /// every [preset](Preset) stored for `job`, by name.
    pub fn presets(&self, job: &str) -> Result<Vec<Preset>, Error> {
        self.conn().query(
            "SELECT job, name, params, created_at FROM presets WHERE job = ?1 ORDER BY name",
            args![job],
            preset_from_row,
        )
    }

    /// one [preset](Preset) by `(job, name)`.
    pub fn preset(&self, job: &str, name: &str) -> Result<Option<Preset>, Error> {
        self.conn().query_opt(
            "SELECT job, name, params, created_at FROM presets WHERE job = ?1 AND name = ?2",
            args![job, name],
            preset_from_row,
        )
    }

    /// store `params` under `(job, name)`, replacing whatever was there.
    ///
    /// an upsert rather than an insert because a declared preset is seeded on
    /// every start: the code that declares one owns its params, and a preset
    /// the launchpad made under another name is nobody else's business.
    /// `created_at` survives the rewrite, so a preset's age means when it first
    /// appeared rather than when the process last booted.
    pub fn put_preset(&self, job: &str, name: &str, params: &Value) -> Result<(), Error> {
        self.conn().execute(
            "INSERT INTO presets (job, name, params, created_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (job, name) DO UPDATE SET params = excluded.params",
            args![job, name, params.to_string(), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// returns false when there was no such preset.
    pub fn delete_preset(&self, job: &str, name: &str) -> Result<bool, Error> {
        let n = self.conn().execute(
            "DELETE FROM presets WHERE job = ?1 AND name = ?2",
            args![job, name],
        )?;
        Ok(n > 0)
    }

    pub(crate) fn prune_ticks(&self, keep: usize) -> Result<(), Error> {
        self.conn().execute(
            "DELETE FROM schedule_ticks WHERE id NOT IN
             (SELECT id FROM schedule_ticks ORDER BY id DESC LIMIT ?1)",
            args![keep as i64],
        )?;
        Ok(())
    }

    /// every job with a run in the log, however long ago and whether or not
    /// this process still defines it.
    ///
    /// the recursive form is a loose index scan: each step is a seek into
    /// `runs_job_created` for the next job name up, so this costs one seek per
    /// *job* rather than one visit per run. `SELECT DISTINCT job` reads the
    /// same answer off the same index by walking every entry in it, which on a
    /// table with a year of runs in it is the sweep's whole cost.
    pub(crate) fn run_jobs(&self) -> Result<Vec<String>, Error> {
        self.conn().query(
            "WITH RECURSIVE names(job) AS (
                 SELECT MIN(job) FROM runs
                 UNION ALL
                 SELECT (SELECT MIN(job) FROM runs WHERE job > names.job)
                 FROM names WHERE job IS NOT NULL)
             SELECT job FROM names WHERE job IS NOT NULL",
            args![],
            |r| r.text(0),
        )
    }

    /// which of one job's runs its [policy](Retention) says it may no longer
    /// keep, newest first.
    ///
    /// read out before anything is deleted, because the rows are the only
    /// record that these runs existed: the sweep drops what each of them wrote
    /// through the [io managers](crate::IoManager) first and deletes them
    /// second.
    ///
    /// non-terminal runs survive at any age. a queued run older than the cutoff
    /// is a queue problem and not a retention one, and a [reclaimed](Reclaim)
    /// run is back on the queue rather than terminal, so what its first claimer
    /// captured is still there when the second one finishes it.
    pub(crate) fn doomed_runs(
        &self,
        job: &str,
        policy: &Retention,
        now: DateTime<Utc>,
    ) -> Result<Vec<String>, Error> {
        let cutoffs = policy.cutoffs(now);
        if !cutoffs.any() {
            return Ok(Vec::new());
        }
        // a null cutoff is no age policy at all: every comparison against it is
        // null, so nothing matches, which is the direction an absent setting
        // has to mean. `keep_last` holds the newest finished runs back from
        // whatever the age rule says — a run goes only when both would take it
        const DOOMED: &str = "SELECT id FROM runs
             WHERE job = ?1 AND status IN ('success', 'failed', 'canceled')
               AND created_at < CASE status WHEN 'success' THEN ?2 ELSE ?3 END
               AND id NOT IN (
                   SELECT id FROM runs
                   WHERE job = ?1 AND status IN ('success', 'failed', 'canceled')
                   ORDER BY created_at DESC LIMIT ?4)";
        let success = cutoffs.success.map(|t| t.to_rfc3339());
        let failed = cutoffs.failed.map(|t| t.to_rfc3339());
        self.conn().query(
            DOOMED,
            args![
                job,
                success.as_deref(),
                failed.as_deref(),
                cutoffs.keep_last
            ],
            |r| r.text(0),
        )
    }

    /// delete `runs` of `job`, with each one's op_runs, events and captured
    /// output.
    ///
    /// by id rather than by policy, so the runs whose outputs the sweep just
    /// dropped are exactly the runs it deletes: one that came due in between
    /// keeps its rows and goes on the next pass, with its files, rather than
    /// losing the second half of the pair. `op_state` is never touched — a
    /// watermark outlives every run that wrote it.
    ///
    /// one transaction per job rather than one for the sweep: a run and its
    /// children still go together, and a database with fifty jobs in it does
    /// not hold the write lock for the length of all fifty.
    pub(crate) fn delete_runs(
        &self,
        job: &str,
        runs: &[String],
        now: DateTime<Utc>,
    ) -> Result<usize, Error> {
        // both backends cap how many values one statement may bind, and the
        // first sweep of a database with a year of history in it is not a
        // small number of runs. one transaction still, so the job's history
        // goes whole or not at all
        const BATCH: usize = 500;
        if runs.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        let mut removed = 0;
        for batch in runs.chunks(BATCH) {
            let list = placeholders(batch.len());
            let mut binds: Vec<Val<'_>> = batch.iter().map(Val::from).collect();
            // children first: the transaction should make it moot, the order makes it true anyway
            for table in ["op_runs", "events", "op_logs"] {
                tx.execute(
                    &format!("DELETE FROM {table} WHERE run_id IN ({list})"),
                    &binds,
                )?;
            }
            // the job as well as the id, so a caller that mixed two jobs'
            // runs cannot delete under the name of one of them
            binds.push(Val::from(job));
            removed += tx.execute(
                &format!(
                    "DELETE FROM runs WHERE id IN ({list}) AND job = ?{}",
                    batch.len() + 1
                ),
                &binds,
            )?;
        }
        // in the same transaction as the deletes, and only when there were
        // some: a sweep that took nothing is not an event, and this one runs
        // every hour against every job that has ever had a run
        if removed > 0 {
            write_event(
                &mut tx,
                &NewEvent::about(
                    SubjectKind::Job,
                    job,
                    EventKind::RetentionPruned,
                    format!("retention removed {removed} runs of {job}"),
                )
                .data(json!({ "job": job, "runs": removed })),
                now,
            )?;
        }
        tx.commit()?;
        Ok(removed)
    }

    /// trim the events that belong to no run down to the newest `keep`.
    ///
    /// run events are deleted with their run by
    /// [`delete_runs`](Self::delete_runs) and always were. everything
    /// v17 added belongs to no run, so nothing collected it: an asset built
    /// every five minutes writes a row here forever. a count cap rather than
    /// the retention policy's age, for the same reason the two tick logs have
    /// one — this grows with time rather than with the history somebody asked
    /// to keep.
    pub(crate) fn prune_events(&self, keep: usize) -> Result<usize, Error> {
        self.conn().execute(
            "DELETE FROM events WHERE run_id IS NULL AND seq NOT IN
             (SELECT seq FROM events WHERE run_id IS NULL ORDER BY seq DESC LIMIT ?1)",
            args![keep as i64],
        )
    }

    /// record one occurrence and what the scheduler did about it, with its
    /// [event](EventKind::ScheduleFired) in the same transaction.
    ///
    /// `caught_up` is the one thing the tick row cannot say for itself: a fire
    /// for an occurrence that came due while nothing was running looks exactly
    /// like an ordinary one except for the gap between `scheduled_for` and
    /// `fired_at`, and only the caller knows which it made.
    ///
    /// **the run is not in this transaction.** a fired tick's run was created
    /// by a launch that committed first, so a crash in between leaves a run
    /// with no tick and no event — the run is still there, still queued, and
    /// still executes. the other direction, a tick claiming a run that was
    /// never created, is the one that cannot happen.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_tick(
        &self,
        job: &str,
        expr: &str,
        scheduled_for: DateTime<Utc>,
        outcome: TickOutcome,
        caught_up: bool,
        run_id: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        tx.execute(
            "INSERT INTO schedule_ticks (job, expr, scheduled_for, fired_at, outcome, run_id, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            args![
                job,
                expr,
                scheduled_for.to_rfc3339(),
                at.to_rfc3339(),
                outcome.as_str(),
                run_id,
                error
            ],
        )?;
        let (kind, level) = match (outcome, caught_up) {
            (TickOutcome::Fired, false) => (EventKind::ScheduleFired, EventLevel::Info),
            (TickOutcome::Fired, true) => (EventKind::ScheduleCaughtUp, EventLevel::Info),
            (TickOutcome::Skipped, _) => (EventKind::ScheduleSkipped, EventLevel::Info),
            (TickOutcome::Deferred, _) => (EventKind::ScheduleDeferred, EventLevel::Info),
            (TickOutcome::Error, _) => (EventKind::ScheduleError, EventLevel::Error),
        };
        let message = match error {
            Some(why) => format!("{expr}: {} — {why}", outcome.as_str()),
            None => format!("{expr}: {}", outcome.as_str()),
        };
        write_event(
            &mut tx,
            &NewEvent::about(SubjectKind::Schedule, job, kind, message)
                .level(level)
                .data(json!({
                    "job": job,
                    "expr": expr,
                    "scheduled_for": scheduled_for,
                    "outcome": outcome,
                    "run_id": run_id,
                    "error": error,
                })),
            at,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// recent [schedule ticks](Tick), newest first, for one job or for all of
    /// them. this is the log that says why a schedule did *not* fire.
    pub fn ticks(&self, job: Option<&str>, limit: u32) -> Result<Vec<Tick>, Error> {
        match job {
            Some(job) => self.conn().query(
                "SELECT id, job, expr, scheduled_for, fired_at, outcome, run_id, error
                 FROM schedule_ticks WHERE job = ?1 ORDER BY id DESC LIMIT ?2",
                args![job, limit],
                tick_from_row,
            ),
            None => self.conn().query(
                "SELECT id, job, expr, scheduled_for, fired_at, outcome, run_id, error
                 FROM schedule_ticks ORDER BY id DESC LIMIT ?1",
                args![limit],
                tick_from_row,
            ),
        }
    }

    /// one run by id, or `None` if this store has never had it — or has
    /// pruned it.
    pub fn run(&self, id: &str) -> Result<Option<Run>, Error> {
        self.conn().query_opt(
            &format!("SELECT {RUN_COLS} FROM runs WHERE id = ?1"),
            args![id],
            run_from_row,
        )
    }

    /// a page of runs, newest first, narrowed by whichever of the filters are
    /// set.
    ///
    /// `before` with `before_id` is the **cursor**, and they belong together:
    /// several runs can share a `created_at`, so a page that asked only for
    /// "older than this timestamp" would skip the rest of that instant. pass
    /// the last row of the previous page as both, and no run is seen twice or
    /// missed. `since` is a window rather than a cursor.
    // created_at is always rfc3339 utc, so the comparisons below are plain string
    // ordering; `before_id` only refines `before`, never stands alone
    #[allow(clippy::too_many_arguments)]
    pub fn runs(
        &self,
        job: Option<&str>,
        since: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        before_id: Option<&str>,
        tag: Option<(&str, &str)>,
        limit: u32,
    ) -> Result<Vec<Run>, Error> {
        let mut conn = self.conn();
        let (tagged, opt) = (conn.dialect().tag_filter(), conn.dialect().text_param());
        let since = since.map(|t| t.to_rfc3339());
        let before = before.map(|t| t.to_rfc3339());
        let (key, value) = (tag.map(|t| t.0), tag.map(|t| t.1));
        conn.query(
            &format!(
                r#"SELECT {RUN_COLS}
                   FROM runs
                   WHERE (?1{opt} IS NULL OR job = ?1)
                     AND (?2{opt} IS NULL OR created_at >= ?2)
                     AND (?3{opt} IS NULL OR created_at < ?3
                          OR (?4{opt} IS NOT NULL AND created_at = ?3 AND id < ?4))
                     AND (?5{opt} IS NULL OR {tagged})
                   ORDER BY created_at DESC, id DESC LIMIT ?7"#
            ),
            args![job, since, before, before_id, key, value, limit],
            run_from_row,
        )
    }

    /// terminal runs finished after `after`, oldest first — what a
    /// [run-status sensor](crate::RunStatusSensor) reads each evaluation.
    /// `job` narrows it to one job; `after` of `None` is every terminal run
    /// there has ever been, which is why a fresh sensor seeds its cursor
    /// instead of asking for that.
    ///
    /// the ordering is `(finished_at, id)` and so is the comparison, so a
    /// strict cursor never drops a run that shares a finish instant with the
    /// one before it.
    pub(crate) fn terminal_runs_after(
        &self,
        job: Option<&str>,
        after: Option<&RunCursor>,
        limit: u32,
    ) -> Result<Vec<Run>, Error> {
        let at = after.map(|c| c.finished_at.to_rfc3339());
        let id = after.map(|c| c.id.as_str());
        let mut conn = self.conn();
        let opt = conn.dialect().text_param();
        conn.query(
            &format!(
                r#"SELECT {RUN_COLS}
                   FROM runs
                   WHERE status IN ('success', 'failed', 'canceled') AND finished_at IS NOT NULL
                     AND (?1{opt} IS NULL OR job = ?1)
                     AND (?2{opt} IS NULL OR finished_at > ?2
                          OR (finished_at = ?2 AND id > ?3))
                   ORDER BY finished_at, id LIMIT ?4"#
            ),
            args![job, at, id, limit],
            run_from_row,
        )
    }

    /// the newest terminal run as a cursor, for a sensor starting from now
    /// rather than from the whole history it was added to.
    pub(crate) fn latest_terminal_run(
        &self,
        job: Option<&str>,
    ) -> Result<Option<RunCursor>, Error> {
        let mut conn = self.conn();
        let opt = conn.dialect().text_param();
        conn.query_opt(
            &format!(
                "SELECT finished_at, id FROM runs
                 WHERE status IN ('success', 'failed', 'canceled') AND finished_at IS NOT NULL
                   AND (?1{opt} IS NULL OR job = ?1)
                 ORDER BY finished_at DESC, id DESC LIMIT 1"
            ),
            args![job],
            |r| {
                Ok(RunCursor {
                    finished_at: r.ts(0)?,
                    id: r.text(1)?,
                })
            },
        )
    }

    /// finish time of the job's most recent successful run.
    pub fn last_success(&self, job: &str) -> Result<Option<DateTime<Utc>>, Error> {
        let ts = self.conn().query_opt(
            "SELECT MAX(finished_at) FROM runs WHERE job = ?1 AND status = 'success'",
            args![job],
            |r| r.opt_ts(0),
        )?;
        Ok(ts.flatten())
    }

    /// every op row of one run, by op name. a row exists per op from the
    /// moment the run is created, so this is the plan as much as the record.
    pub fn op_runs(&self, run_id: &str) -> Result<Vec<OpRun>, Error> {
        self.conn().query(
            "SELECT run_id, op, status, attempts, started_at, finished_at, output, metadata, error,
                    pid
             FROM op_runs WHERE run_id = ?1 ORDER BY op",
            args![run_id],
            op_run_from_row,
        )
    }

    /// one op's row. the parent of an isolated op reads back what its child
    /// recorded through this — the whole of what a worker process reports.
    pub fn op_run(&self, run_id: &str, op: &str) -> Result<Option<OpRun>, Error> {
        self.conn().query_opt(
            "SELECT run_id, op, status, attempts, started_at, finished_at, output, metadata,
                    error, pid
             FROM op_runs WHERE run_id = ?1 AND op = ?2",
            args![run_id, op],
            op_run_from_row,
        )
    }

    /// op_run rows across the job's most recent `runs` runs, newest run first.
    pub fn recent_op_runs(&self, job: &str, runs: u32) -> Result<Vec<OpRun>, Error> {
        self.conn().query(
            "SELECT o.run_id, o.op, o.status, o.attempts, o.started_at, o.finished_at, o.output,
                    o.metadata, o.error, o.pid
             FROM op_runs o
             JOIN (SELECT id, created_at FROM runs WHERE job = ?1
                   ORDER BY created_at DESC LIMIT ?2) r ON r.id = o.run_id
             ORDER BY r.created_at DESC",
            args![job, runs],
            op_run_from_row,
        )
    }

    /// what each of `job`'s ops last reported, from the newest run before the
    /// one named — the map a run's [deltas](crate::Meta) are computed against.
    /// keyed by op name, so a fan-out instance (`fetch[0]`) compares against
    /// the same instance of the previous run.
    ///
    /// rows with no metadata at all are skipped rather than ending the search:
    /// a failed op records none, and one bad run between two good ones should
    /// not erase the comparison between them. the ordering is `(created_at,
    /// id)` and so is the cursor, so a run sharing a creation instant with
    /// this one is still strictly before it.
    pub fn previous_op_metadata(
        &self,
        job: &str,
        before: DateTime<Utc>,
        run_id: &str,
    ) -> Result<HashMap<String, Value>, Error> {
        // the subquery is named because postgres insists on it and sqlite does
        // not mind, which is one fewer branch
        let rows = self.conn().query(
            "SELECT op, metadata FROM (
                 SELECT o.op AS op, o.metadata AS metadata,
                        ROW_NUMBER() OVER (
                            PARTITION BY o.op ORDER BY r.created_at DESC, r.id DESC
                        ) AS rn
                 FROM op_runs o JOIN runs r ON r.id = o.run_id
                 WHERE r.job = ?1 AND o.metadata IS NOT NULL
                   AND (r.created_at < ?2 OR (r.created_at = ?2 AND r.id < ?3))
             ) latest WHERE rn = 1",
            args![job, before.to_rfc3339(), run_id],
            |r| Ok((r.text(0)?, r.json(1)?)),
        )?;
        Ok(rows.into_iter().collect())
    }

    /// what a numeric metadata key was across `job`'s recent runs of one op,
    /// oldest first — the trend the op inspector draws.
    ///
    /// `limit` is how many **runs** are read, not how many points come back:
    /// a run that reported nothing, or reported `key` as something that is
    /// not a number, contributes no point rather than a gap or a zero.
    pub fn op_metadata_series(
        &self,
        job: &str,
        op: &str,
        key: &str,
        limit: u32,
    ) -> Result<Vec<MetaPoint>, Error> {
        let rows = self.conn().query(
            "SELECT o.run_id, o.metadata, COALESCE(o.finished_at, r.created_at)
             FROM op_runs o JOIN runs r ON r.id = o.run_id
             WHERE r.job = ?1 AND o.op = ?2 AND o.metadata IS NOT NULL
             ORDER BY r.created_at DESC, r.id DESC LIMIT ?3",
            args![job, op, limit],
            |r| Ok((r.json(1)?, r.ts(2)?, r.text(0)?)),
        )?;
        let mut points = Vec::new();
        for row in rows {
            let (metadata, at, run_id) = row;
            if let Some(value) = op::numeric_key(&metadata, key) {
                points.push(MetaPoint {
                    at,
                    value,
                    run_id: Some(run_id),
                });
            }
        }
        points.reverse();
        Ok(points)
    }

    /// the same for one asset's recent builds, oldest first. `partition`
    /// narrows it to one key of a [partitioned asset](crate::Partitions);
    /// without it the builds of every key interleave by time, which is a
    /// trend of the asset rather than of any one key.
    pub fn asset_metadata_series(
        &self,
        asset: &str,
        partition: Option<&str>,
        key: &str,
        limit: u32,
    ) -> Result<Vec<MetaPoint>, Error> {
        let mut conn = self.conn();
        let (same, opt) = (conn.dialect().null_safe_eq(), conn.dialect().text_param());
        let rows = conn.query(
            &format!(
                "SELECT run_id, metadata, built_at FROM asset_materializations
                 WHERE asset = ?1 AND (?2{opt} IS NULL OR partition {same} ?2)
                   AND metadata IS NOT NULL
                 ORDER BY id DESC LIMIT ?3"
            ),
            args![asset, partition, limit],
            |r| Ok((r.json(1)?, r.ts(2)?, r.opt_text(0)?)),
        )?;
        let mut points = Vec::new();
        for row in rows {
            let (metadata, at, run_id) = row;
            if let Some(value) = op::numeric_key(&metadata, key) {
                points.push(MetaPoint { at, value, run_id });
            }
        }
        points.reverse();
        Ok(points)
    }

    /// the state an op's last successful run committed, if any.
    pub fn op_state(&self, job: &str, op: &str) -> Result<Option<Value>, Error> {
        self.conn().query_opt(
            "SELECT value FROM op_state WHERE job = ?1 AND op = ?2",
            args![job, op],
            |r| r.json(0),
        )
    }

    pub(crate) fn set_op_state(&self, job: &str, op: &str, value: &Value) -> Result<(), Error> {
        #[cfg(test)]
        {
            if let Some(e) = self.injected("set_op_state") {
                return Err(e);
            }
        }
        self.conn().execute(
            "INSERT INTO op_state (job, op, value, updated_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (job, op) DO UPDATE SET value = ?3, updated_at = ?4",
            args![job, op, value.to_string(), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// every op state a job carries, ordered by op.
    pub fn job_states(&self, job: &str) -> Result<Vec<(String, Value, DateTime<Utc>)>, Error> {
        self.conn().query(
            "SELECT op, value, updated_at FROM op_state WHERE job = ?1 ORDER BY op",
            args![job],
            |r| Ok((r.text(0)?, r.json(1)?, r.ts(2)?)),
        )
    }

    /// one run's events, oldest first, after cursor `after`.
    pub fn events(&self, run_id: &str, after: i64) -> Result<Vec<Event>, Error> {
        self.conn().query(
            &format!("SELECT {EVENT_COLS} FROM events WHERE run_id = ?1 AND seq > ?2 ORDER BY seq"),
            args![run_id, after],
            event_from_row,
        )
    }

    /// the whole log, newest first: what happened, across every subsystem.
    ///
    /// `before` pages backwards — the last seq of the page you have, and the
    /// next page is what is below it. a page of the past is exact, because
    /// nothing is still being written down there; the newest page has the same
    /// in-flight window [`event_tail`](Self::event_tail) documents, and for the
    /// same reason.
    pub fn event_log(&self, q: &EventQuery, limit: u32) -> Result<Vec<Event>, Error> {
        let mut conn = self.conn();
        let (where_sql, args) = q.sql(conn.dialect());
        let mut args = args;
        args.push(Val::Int(i64::from(limit)));
        let n = args.len();
        conn.query(
            &format!(
                "SELECT {EVENT_COLS} FROM events WHERE {where_sql} ORDER BY seq DESC LIMIT ?{n}"
            ),
            &args,
            event_from_row,
        )
    }

    /// the log oldest first from a cursor: what a follower reads to catch up
    /// and then to keep up.
    ///
    /// **`seq` is allocated on insert, not on commit**, and that is the whole
    /// difficulty with following this table. a writer that has taken seq 5 and
    /// not committed is invisible while a writer that took 6 and did commit is
    /// not, so a follower that takes what it can see and moves its cursor to 6
    /// will never come back for 5. `up_to` is how a caller keeps that from
    /// happening: it is a ceiling the caller believes nothing can still appear
    /// below, and [`event_watermark`](Self::event_watermark) is where one comes
    /// from.
    pub fn event_tail(
        &self,
        q: &EventQuery,
        after: i64,
        up_to: Option<i64>,
        limit: u32,
    ) -> Result<Vec<Event>, Error> {
        let mut conn = self.conn();
        let (where_sql, args) = q.sql(conn.dialect());
        let mut args = args;
        args.push(Val::Int(after));
        let cursor = args.len();
        args.push(Val::Int(up_to.unwrap_or(i64::MAX)));
        let ceiling = args.len();
        args.push(Val::Int(i64::from(limit)));
        let n = args.len();
        conn.query(
            &format!(
                "SELECT {EVENT_COLS} FROM events
                 WHERE {where_sql} AND seq > ?{cursor} AND seq <= ?{ceiling}
                 ORDER BY seq LIMIT ?{n}"
            ),
            &args,
            event_from_row,
        )
    }

    /// the newest seq anything has committed, or 0 on an empty log.
    pub fn event_watermark(&self) -> Result<i64, Error> {
        let seq = self
            .conn()
            .query_opt("SELECT MAX(seq) FROM events", args![], |r| r.opt_int(0))?;
        Ok(seq.flatten().unwrap_or(0))
    }

    /// whether this backend allocates `seq` in commit order.
    ///
    /// **sqlite: yes.** a writer takes the database's write lock at its first
    /// write and holds it until it commits, so no transaction can commit below
    /// one that already has. a follower may read straight up to
    /// [`event_watermark`](Self::event_watermark) and cannot skip anything.
    ///
    /// **postgres: no.** several processes write at once, `seq` comes off a
    /// sequence at insert, and a transaction holding 5 can commit after one
    /// that took 6. a follower there uses [`settled_after`](Self::settled_after)
    /// instead.
    pub fn settles_in_order(&self) -> bool {
        matches!(self.conn().dialect(), Dialect::Sqlite)
    }

    /// how far above `after` the log is unbroken, and what stopped it.
    ///
    /// this is what keeps a follower on a backend that does not settle in order
    /// from skipping an event. a missing seq is one of two things and they look
    /// identical from here: a transaction that has allocated it and not yet
    /// committed, or one that aborted and never will. so a follower delivers up
    /// to [`upto`](Settled::upto), waits on the gap for a bounded grace, and
    /// steps over it only after that — which is the one assumption in the whole
    /// mechanism, stated where it is made: **a transaction that appends an event
    /// and takes longer than the grace to commit may be skipped.** hestan's are
    /// a handful of statements each.
    ///
    /// `scan` bounds the walk. it also bounds what one poll may deliver, which
    /// is fine: what is above it is still there next poll.
    pub fn settled_after(&self, after: i64, scan: u32) -> Result<Settled, Error> {
        let seqs: Vec<i64> = self.conn().query(
            "SELECT seq FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2",
            args![after, scan],
            |r| r.int(0),
        )?;
        let mut upto = after;
        let mut gap = None;
        let mut after_gap = None;
        for seq in seqs {
            match gap {
                // the first visible seq above the gap, which is where a
                // follower that gives up on it resumes: one grace for a whole
                // range of missing seqs rather than one per seq, and a range is
                // exactly what a retention sweep leaves behind
                Some(_) => {
                    after_gap = Some(seq);
                    break;
                }
                None if seq == upto + 1 => upto = seq,
                None => gap = Some(upto + 1),
            }
        }
        Ok(Settled {
            upto,
            gap,
            after_gap,
        })
    }

    /// what a follower may take this pass.
    ///
    /// on a backend that [settles in order](Self::settles_in_order) that is
    /// everything committed, full stop. on one that does not, it is the
    /// unbroken run above the cursor — and the gap that ended it is remembered
    /// in `waiting`, so that the same gap seen for longer than
    /// [`SETTLE_GRACE`] is stepped over rather than stalling the follower
    /// forever on a rolled-back write.
    ///
    /// **every follower of this table goes through here**, so the sse stream
    /// and a command line tailing the log cannot come to different conclusions
    /// about what is safe to read — which, for a rule this subtle, they
    /// otherwise would.
    pub(crate) fn readable(
        &self,
        cursor: i64,
        waiting: &mut Option<(i64, Instant)>,
    ) -> Result<Step, Error> {
        if self.settles_in_order() {
            return Ok(Step {
                ceiling: self.event_watermark()?,
                skip_to: None,
            });
        }
        let settled = self.settled_after(cursor, SETTLE_SCAN)?;
        let Some(gap) = settled.gap else {
            *waiting = None;
            return Ok(Step {
                ceiling: settled.upto,
                skip_to: None,
            });
        };
        let since = match waiting {
            Some((at, since)) if *at == gap => *since,
            _ => {
                let now = Instant::now();
                *waiting = Some((gap, now));
                now
            }
        };
        let expired = since.elapsed() >= SETTLE_GRACE;
        if expired {
            tracing::debug!("event log: seq {gap} never arrived; stepping over it");
            *waiting = None;
        }
        Ok(Step {
            ceiling: settled.upto,
            // resume at the next visible seq when the scan found one; otherwise
            // just past the gap, and the next pass walks on from there
            skip_to: expired.then(|| settled.after_gap.map_or(gap, |next| next - 1)),
        })
    }

    /// append a materialization outside any op's terminal write: a
    /// [source](crate::AssetBuilder::source) asset a probe found new bytes
    /// for, and the fixtures the suites build histories out of. a build an op
    /// did goes through [`op_finished`](Store::op_finished) instead, which is
    /// the whole difference between a row that observes something and a row
    /// that asserts an op succeeded.
    ///
    /// the table is history, so a rebuild that came out fingerprint-identical
    /// is still an entry — that a build happened and that it changed anything
    /// are different facts.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_materialization(
        &self,
        asset: &str,
        partition: Option<&str>,
        fingerprint: &str,
        inputs: &Value,
        value: Option<&Value>,
        run_id: Option<&str>,
        metadata: Option<&Value>,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let built = Built {
            asset: asset.to_string(),
            partition: partition.map(str::to_string),
            fingerprint: fingerprint.to_string(),
            inputs: inputs.clone(),
            value: value.cloned(),
            meta: metadata.cloned(),
        };
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        write_materialization(&mut tx, &built, run_id, at)?;
        tx.commit()?;
        Ok(())
    }

    /// the current materialization of one `(asset, partition)` pair: the newest
    /// entry of its history, which is what staleness, seeding and the assets
    /// api all read. `partition` is `None` for an unpartitioned asset.
    pub fn materialization(
        &self,
        asset: &str,
        partition: Option<&str>,
    ) -> Result<Option<Materialization>, Error> {
        let mut conn = self.conn();
        let same = conn.dialect().null_safe_eq();
        conn.query_opt(
            &format!(
                "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                        metadata
                 FROM asset_materializations WHERE asset = ?1 AND partition {same} ?2
                 ORDER BY id DESC LIMIT 1"
            ),
            args![asset, partition],
            materialization_from_row,
        )
    }

    /// the current materialization of every `(asset, partition)` pair, one row
    /// each, ordered by asset then partition.
    ///
    /// `NULLS FIRST` is written out because the two backends disagree about
    /// where an unpartitioned asset's null sorts by default, and both accept
    /// being told.
    pub fn latest_materializations(&self) -> Result<Vec<Materialization>, Error> {
        self.conn().query(
            "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at, metadata
             FROM asset_materializations
             WHERE id IN
                 (SELECT MAX(id) FROM asset_materializations GROUP BY asset, partition)
             ORDER BY asset, partition NULLS FIRST",
            args![],
            materialization_from_row,
        )
    }

    /// one asset's history, newest first, each entry carrying whether its
    /// fingerprint differs from the entry before it in time — which is what
    /// turns a list of rebuilds into a list of changes — and what that entry
    /// reported, which is what the deltas beside its numbers are against.
    /// `partition` narrows it to one key; `None` is every key of the asset,
    /// interleaved by time.
    ///
    /// both comparisons run over the whole history before the limit applies,
    /// so the oldest entry on a page is compared against the entry just off it
    /// rather than reported as a change it isn't. the very first entry has
    /// nothing before it and counts as changed: nothing to something.
    pub fn materializations(
        &self,
        asset: &str,
        partition: Option<&str>,
        limit: u32,
    ) -> Result<Vec<HistoryEntry>, Error> {
        let mut conn = self.conn();
        let (same, changed, opt) = (
            conn.dialect().null_safe_eq(),
            conn.dialect().fingerprint_changed(),
            conn.dialect().text_param(),
        );
        // both look one row back within the partition: one key's rebuild says
        // nothing about whether another key's fingerprint or row count moved
        conn.query(
            &format!(
                "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                        metadata, changed, previous_metadata FROM (
                     SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                            metadata,
                            {changed} AS changed,
                            LAG(metadata) OVER (PARTITION BY partition ORDER BY id)
                                AS previous_metadata
                     FROM asset_materializations
                     WHERE asset = ?1 AND (?2{opt} IS NULL OR partition {same} ?2)
                 ) history ORDER BY id DESC LIMIT ?3"
            ),
            args![asset, partition, limit],
            |r| {
                Ok(HistoryEntry {
                    mat: materialization_from_row(r)?,
                    changed: r.flag(9)?,
                    previous_metadata: r.opt_json(10)?,
                })
            },
        )
    }

    /// trim every `(asset, partition)` pair's history to its newest `keep`
    /// entries. `keep` is floored at 1: the latest materialization is current
    /// state, not history, and dropping it would read as a partition that has
    /// never been built.
    pub(crate) fn prune_materializations(&self, keep: usize) -> Result<usize, Error> {
        let mut conn = self.conn();
        let same = conn.dialect().null_safe_eq();
        conn.execute(
            &format!(
                "DELETE FROM asset_materializations WHERE id NOT IN
                 (SELECT id FROM asset_materializations AS newest
                  WHERE newest.asset = asset_materializations.asset
                    AND newest.partition {same} asset_materializations.partition
                  ORDER BY newest.id DESC LIMIT ?1)"
            ),
            args![keep.max(1) as i64],
        )
    }

    /// record what a check said. written inside the check's op, before it
    /// decides whether to fail, so a failing error check leaves its verdict
    /// behind rather than only a failed op. its
    /// [event](EventKind::CheckFailed) goes in the same transaction.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_check(
        &self,
        asset: &str,
        partition: Option<&str>,
        check: &str,
        run_id: &str,
        status: CheckStatus,
        severity: Severity,
        message: Option<&str>,
        metadata: Option<&Value>,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        tx.execute(
            "INSERT INTO asset_checks
                 (asset, partition, check_name, run_id, status, severity, message,
                  metadata, checked_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            args![
                asset,
                partition,
                check,
                run_id,
                status.as_str(),
                severity.as_str(),
                message,
                metadata.map(|v| v.to_string()),
                at.to_rfc3339(),
            ],
        )?;
        // a failed check at `warn` is a run that succeeded and a check that did
        // not, so the level follows the severity rather than the verdict
        let (kind, level) = match (status, severity) {
            (CheckStatus::Passed, _) => (EventKind::CheckPassed, EventLevel::Info),
            (CheckStatus::Failed, Severity::Warn) => (EventKind::CheckFailed, EventLevel::Warn),
            (CheckStatus::Failed, Severity::Error) => (EventKind::CheckFailed, EventLevel::Error),
        };
        let subject = match partition {
            None => Cow::Borrowed(asset),
            Some(key) => Cow::Owned(format!("{asset}[{key}]")),
        };
        write_event(
            &mut tx,
            &NewEvent::about(
                SubjectKind::Asset,
                asset,
                kind,
                format!("check {check} {} on {subject}", status.as_str()),
            )
            .level(level)
            .data(json!({
                "check": check,
                "partition": partition,
                "status": status,
                "severity": severity,
                "message": message,
                "run_id": run_id,
                "meta": metadata,
            })),
            at,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// one asset's recent check results, newest first, every check and every
    /// partition mixed together — the api and the ui take the first row per
    /// `(check, partition)` to get each one's latest. `partition` narrows it to
    /// a single key.
    pub fn asset_checks(
        &self,
        asset: &str,
        partition: Option<&str>,
        limit: u32,
    ) -> Result<Vec<AssetCheckRow>, Error> {
        let mut conn = self.conn();
        let (same, opt) = (conn.dialect().null_safe_eq(), conn.dialect().text_param());
        conn.query(
            &format!(
                "SELECT id, asset, partition, check_name, run_id, status, severity, message,
                        metadata, checked_at
                 FROM asset_checks WHERE asset = ?1 AND (?2{opt} IS NULL OR partition {same} ?2)
                 ORDER BY id DESC LIMIT ?3"
            ),
            args![asset, partition, limit],
            asset_check_from_row,
        )
    }

    /// the latest result of every `(asset, partition, check)` triple, ordered
    /// by all three.
    pub fn latest_asset_checks(&self) -> Result<Vec<AssetCheckRow>, Error> {
        self.conn().query(
            "SELECT id, asset, partition, check_name, run_id, status, severity, message,
                    metadata, checked_at
             FROM asset_checks
             WHERE id IN
                 (SELECT MAX(id) FROM asset_checks GROUP BY asset, partition, check_name)
             ORDER BY asset, partition NULLS FIRST, check_name",
            args![],
            asset_check_from_row,
        )
    }

    /// trim every check to its newest `keep` results per partition, floored at
    /// 1 like [`prune_materializations`](Self::prune_materializations) — the
    /// latest result is what the asset summary counts.
    pub(crate) fn prune_asset_checks(&self, keep: usize) -> Result<usize, Error> {
        let mut conn = self.conn();
        let same = conn.dialect().null_safe_eq();
        conn.execute(
            &format!(
                "DELETE FROM asset_checks WHERE id NOT IN
                 (SELECT id FROM asset_checks AS newest
                  WHERE newest.asset = asset_checks.asset
                    AND newest.partition {same} asset_checks.partition
                    AND newest.check_name = asset_checks.check_name
                  ORDER BY newest.id DESC LIMIT ?1)"
            ),
            args![keep.max(1) as i64],
        )
    }

    /// record a backfill request and the keys it resolved to, `running` with
    /// nothing launched yet. `keys` is fixed here on purpose: a backfill
    /// builds the range it was asked for even as a daily set grows under it.
    pub(crate) fn create_backfill(
        &self,
        asset: &str,
        from_key: &str,
        to_key: &str,
        keys: &[String],
        actor: Option<&str>,
    ) -> Result<i64, Error> {
        // a range that resolved to nothing is complete the moment it is made,
        // which is a truer record than refusing to write one
        let (status, finished) = match keys.is_empty() {
            true => (BackfillStatus::Complete, Some(Utc::now().to_rfc3339())),
            false => (BackfillStatus::Running, None),
        };
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        // the one place that wanted `last_insert_rowid`. `RETURNING` is the
        // portable spelling — postgres has always had it and sqlite has since
        // 3.35 — so the id comes back with the row rather than from a second
        // question about what the connection did last
        let id = tx.query_opt(
            "INSERT INTO backfills
                 (asset, from_key, to_key, partition_keys, total, created_at, finished_at, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             RETURNING id",
            args![
                asset,
                from_key,
                to_key,
                serde_json::to_string(keys).unwrap_or_else(|_| "[]".into()),
                keys.len() as i64,
                at.to_rfc3339(),
                finished,
                status.as_str(),
            ],
            |r| r.int(0),
        )?;
        let id = id.ok_or_else(|| Error::Column(0, "an insert returned no id".into()))?;
        write_event(
            &mut tx,
            &NewEvent::about(
                SubjectKind::Backfill,
                id.to_string(),
                EventKind::BackfillStarted,
                format!(
                    "backfill of {asset} {from_key}..{to_key}: {} keys",
                    keys.len()
                ),
            )
            .actor(actor)
            .data(json!({
                "asset": asset,
                "from_key": from_key,
                "to_key": to_key,
                "total": keys.len(),
            })),
            at,
        )?;
        // a range that resolved to nothing is over the moment it is made, and
        // both facts are true at once rather than one of them being suppressed
        if status == BackfillStatus::Complete {
            write_event(&mut tx, &backfill_over(id, asset, status, 0, 0), at)?;
        }
        tx.commit()?;
        Ok(id)
    }

    /// one [backfill](Backfill) by id, with its progress as of now.
    pub fn backfill(&self, id: i64) -> Result<Option<Backfill>, Error> {
        self.conn().query_opt(
            "SELECT id, asset, from_key, to_key, partition_keys, run_ids, total, launched,
                    created_at, finished_at, status
             FROM backfills WHERE id = ?1",
            args![id],
            backfill_from_row,
        )
    }

    /// recent backfills, newest first.
    pub fn backfills(&self, limit: u32) -> Result<Vec<Backfill>, Error> {
        self.conn().query(
            "SELECT id, asset, from_key, to_key, partition_keys, run_ids, total, launched,
                    created_at, finished_at, status
             FROM backfills ORDER BY id DESC LIMIT ?1",
            args![limit],
            backfill_from_row,
        )
    }

    /// every backfill still going, oldest first — what the loop that chunks
    /// them reads, and what makes a second one for the same asset a conflict.
    pub(crate) fn running_backfills(&self) -> Result<Vec<Backfill>, Error> {
        self.conn().query(
            "SELECT id, asset, from_key, to_key, partition_keys, run_ids, total, launched,
                    created_at, finished_at, status
             FROM backfills WHERE status = ?1 ORDER BY id",
            args![BackfillStatus::Running.as_str()],
            backfill_from_row,
        )
    }

    /// record that a chunk went out: its run, and how many keys are now
    /// launched in total.
    ///
    /// the run itself was created by a launch that committed first, so this is
    /// the same window [`record_tick`](Self::record_tick) has and for the same
    /// reason: a chunk run with no event is recoverable, and an event naming a
    /// run that was never launched is not.
    pub(crate) fn backfill_launched(
        &self,
        id: i64,
        asset: &str,
        run_id: &str,
        launched: usize,
        total: usize,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let append = conn.dialect().json_append();
        let mut tx = conn.begin()?;
        tx.execute(
            &format!(
                "UPDATE backfills
                 SET launched = ?2,
                     run_ids = {append}
                 WHERE id = ?1"
            ),
            args![id, launched as i64, run_id],
        )?;
        write_event(
            &mut tx,
            &NewEvent::about(
                SubjectKind::Backfill,
                id.to_string(),
                EventKind::BackfillChunk,
                format!("chunk launched: {launched} of {total} keys"),
            )
            .data(json!({
                "asset": asset,
                "run_id": run_id,
                "launched": launched,
                "total": total,
            })),
            at,
        )?;
        tx.commit()?;
        Ok(())
    }

    /// close a backfill. the first terminal status wins, so a cancel racing
    /// the chunker cannot be overwritten by what the run did next — and the
    /// event is written only by the writer that won, so there is exactly one.
    pub(crate) fn finish_backfill(
        &self,
        id: i64,
        asset: &str,
        status: BackfillStatus,
        launched: usize,
        total: usize,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        let closed = tx.execute(
            "UPDATE backfills SET status = ?2, finished_at = ?3 WHERE id = ?1 AND status = ?4",
            args![
                id,
                status.as_str(),
                at.to_rfc3339(),
                BackfillStatus::Running.as_str(),
            ],
        )?;
        if closed > 0 {
            write_event(
                &mut tx,
                &backfill_over(id, asset, status, launched, total),
                at,
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// what the last freshness check concluded about everything it has ever
    /// seen, ordered by kind then name.
    pub fn freshness_states(&self) -> Result<Vec<FreshnessRow>, Error> {
        self.conn().query(
            "SELECT kind, name, late, since FROM freshness_state ORDER BY kind, name",
            args![],
            |r| {
                Ok(FreshnessRow {
                    kind: r.text(0)?,
                    name: r.text(1)?,
                    late: r.flag(2)?,
                    since: r.opt_ts(3)?,
                })
            },
        )
    }

    /// record a crossing. `since` is when it went late and is dropped on the
    /// way back to fresh, so a relapse is a new interval rather than the old
    /// one resumed.
    pub(crate) fn set_freshness_state(
        &self,
        kind: &str,
        name: &str,
        late: bool,
        since: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        self.conn().execute(
            "INSERT INTO freshness_state (kind, name, late, since) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (kind, name) DO UPDATE SET late = ?3, since = ?4",
            args![kind, name, late, since.map(|t| t.to_rfc3339())],
        )?;
        Ok(())
    }

    /// make the sensors table mirror the code: insert new names, drop the
    /// rest. existing rows keep their paused flag and cursor.
    pub(crate) fn sync_sensors(&self, defined: &[String]) -> Result<(), Error> {
        let mut conn = self.conn();
        let (insert, ignore) = conn.dialect().insert_or_ignore();
        let mut tx = conn.begin()?;
        let now = Utc::now().to_rfc3339();
        for name in defined {
            tx.execute(
                &format!("{insert} sensors (name, updated_at) VALUES (?1, ?2) {ignore}"),
                args![name, &now],
            )?;
        }
        let existing: Vec<String> = tx.query("SELECT name FROM sensors", args![], |r| r.text(0))?;
        let keep: HashSet<&str> = defined.iter().map(String::as_str).collect();
        for name in &existing {
            if !keep.contains(name.as_str()) {
                tx.execute("DELETE FROM sensors WHERE name = ?1", args![name])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// every stored sensor, by name — the paused flags and the cursors, not
    /// the closures, which live in code.
    pub fn sensors(&self) -> Result<Vec<SensorRow>, Error> {
        self.conn().query(
            "SELECT name, paused, cursor, updated_at FROM sensors ORDER BY name",
            args![],
            |r| {
                Ok(SensorRow {
                    name: r.text(0)?,
                    paused: r.flag(1)?,
                    cursor: r.opt_json(2)?,
                    updated_at: r.ts(3)?,
                })
            },
        )
    }

    /// returns false if no sensor with that name is registered.
    /// pause or unpause one sensor, and record who did — see
    /// [`set_schedule_paused`](Self::set_schedule_paused).
    pub fn set_sensor_paused(
        &self,
        name: &str,
        paused: bool,
        actor: Option<&str>,
    ) -> Result<bool, Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin_immediate()?;
        let n = tx.execute(
            "UPDATE sensors SET paused = ?2 WHERE name = ?1",
            args![name, paused],
        )?;
        if n == 0 {
            tx.commit()?;
            return Ok(false);
        }
        let verb = if paused { "paused" } else { "resumed" };
        write_event(
            &mut tx,
            &NewEvent::about(
                SubjectKind::Sensor,
                name,
                EventKind::SensorPaused,
                format!("sensor {name} {verb}"),
            )
            .actor(actor)
            .data(json!({ "paused": paused })),
            at,
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub(crate) fn set_sensor_cursor(&self, name: &str, cursor: &Value) -> Result<(), Error> {
        self.conn().execute(
            "UPDATE sensors SET cursor = ?2, updated_at = ?3 WHERE name = ?1",
            args![name, cursor.to_string(), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// `skipped` counts the requests this evaluation did not launch because
    /// their run key was already claimed — distinct from launching nothing —
    /// and `duration_ms` is how long the evaluation took, which is the other
    /// half of "is this sensor healthy". `runs` is what it launched, by id,
    /// which is the tick's whole reason for existing and the one thing the
    /// counts cannot give you.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn record_sensor_tick(
        &self,
        sensor: &str,
        outcome: SensorOutcome,
        launched: u32,
        skipped: u32,
        duration_ms: u64,
        runs: &[String],
        error: Option<&str>,
    ) -> Result<(), Error> {
        let at = Utc::now();
        let mut conn = self.conn();
        let mut tx = conn.begin()?;
        tx.execute(
            "INSERT INTO sensor_ticks
                 (sensor, evaluated_at, outcome, launched, skipped, duration_ms, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            args![
                sensor,
                at.to_rfc3339(),
                outcome.as_str(),
                launched,
                skipped,
                duration_ms as i64,
                error
            ],
        )?;
        // **a tick that did nothing is not an event.** every evaluation is a
        // row in `sensor_ticks` and always was — that is the sensor's own
        // health record, and the sensors page reads it. but a sensor polling
        // every five seconds writes seventeen thousand of those a day, and an
        // activity log in which they are 99% of the rows is one nobody can read
        // anything else out of. so the log gets the ones that did something:
        // launched a run, declined a keyed request, or failed.
        let quiet = outcome == SensorOutcome::Fired && launched == 0 && skipped == 0;
        if !quiet {
            let level = match outcome {
                SensorOutcome::Error => EventLevel::Error,
                _ => EventLevel::Info,
            };
            write_event(
                &mut tx,
                &NewEvent::about(
                    SubjectKind::Sensor,
                    sensor,
                    EventKind::SensorTick,
                    match error {
                        Some(why) => format!("{} — {why}", outcome.as_str()),
                        None => format!("{}, {launched} launched", outcome.as_str()),
                    },
                )
                .level(level)
                .data(json!({
                    "outcome": outcome,
                    "launched": launched,
                    "skipped": skipped,
                    "duration_ms": duration_ms,
                    "runs": runs,
                    "error": error,
                })),
                at,
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// recent [sensor ticks](SensorTick), newest first, for one sensor or for
    /// all of them. a sensor that is running and finding nothing looks quite
    /// unlike one that is not running, and this is where the difference is.
    pub fn sensor_ticks(&self, sensor: Option<&str>, limit: u32) -> Result<Vec<SensorTick>, Error> {
        let mut conn = self.conn();
        let opt = conn.dialect().text_param();
        conn.query(
            &format!(
                "SELECT id, sensor, evaluated_at, outcome, launched, skipped, duration_ms, error
                 FROM sensor_ticks WHERE (?1{opt} IS NULL OR sensor = ?1)
                 ORDER BY id DESC LIMIT ?2"
            ),
            args![sensor, limit],
            sensor_tick_from_row,
        )
    }

    /// move a finished run's finish time. tests that need history older than
    /// the test itself have no other way to make one, and freshness is entirely
    /// about how old a success is.
    #[cfg(test)]
    pub(crate) fn backdate_run(&self, id: &str, finished_at: DateTime<Utc>) -> Result<(), Error> {
        self.conn().execute(
            "UPDATE runs SET finished_at = ?2 WHERE id = ?1",
            args![id, finished_at.to_rfc3339()],
        )?;
        Ok(())
    }

    /// [`backdate_run`](Self::backdate_run) for the newest materialization of
    /// one `(asset, partition)` pair.
    #[cfg(test)]
    pub(crate) fn backdate_materialization(
        &self,
        asset: &str,
        partition: Option<&str>,
        built_at: DateTime<Utc>,
    ) -> Result<(), Error> {
        let mut conn = self.conn();
        let same = conn.dialect().null_safe_eq();
        conn.execute(
            &format!(
                "UPDATE asset_materializations SET built_at = ?3
                 WHERE id = (SELECT MAX(id) FROM asset_materializations
                             WHERE asset = ?1 AND partition {same} ?2)"
            ),
            args![asset, partition, built_at.to_rfc3339()],
        )?;
        Ok(())
    }

    /// refuse every write from here on, the way a database mounted read-only
    /// or opened without permission does. tests only.
    ///
    /// sqlite has a switch for exactly this. postgres has no per-connection
    /// equivalent that a transaction cannot simply open anyway, so the
    /// [writable](Self::writable) probe is asked of sqlite, where the answer
    /// means something on both.
    #[cfg(test)]
    pub(crate) fn refuse_writes(&self) -> Result<(), Error> {
        self.conn().execute("PRAGMA query_only = ON", args![])?;
        Ok(())
    }

    /// drop a table, so that every read touching it fails.    /// drop a table, so that every read touching it fails. a test proving a
    /// control-plane read fails closed has no other way to break one.
    #[cfg(test)]
    pub(crate) fn drop_table(&self, name: &str) -> Result<(), Error> {
        self.conn().batch(&format!("DROP TABLE {name}"))
    }

    /// refuse every insert into `notifications` from here on, which is what a
    /// crash between a run's terminal row and the alert it owes looks like to
    /// the transaction around both. tests only, and each backend has its own
    /// way of being made to say no.
    #[cfg(test)]
    pub(crate) fn refuse_notifications(&self) -> Result<(), Error> {
        let mut conn = self.conn();
        match conn.dialect() {
            Dialect::Sqlite => conn.batch(
                "CREATE TRIGGER refused BEFORE INSERT ON notifications
                 BEGIN SELECT RAISE(ABORT, 'refused'); END",
            ),
            // not validated against the rows already there: those are history,
            // and this is about the next write
            #[cfg(feature = "postgres")]
            Dialect::Postgres => conn
                .batch("ALTER TABLE notifications ADD CONSTRAINT refused CHECK (false) NOT VALID"),
        }
    }

    /// how sqlite says it will answer `sql`. the one case that reads a plan is
    /// about sqlite's own planner and runs on nothing else.
    #[cfg(test)]
    pub(crate) fn sqlite_plan(&self, sql: &str) -> String {
        let rows = self
            .conn()
            .query(&format!("EXPLAIN QUERY PLAN {sql}"), args![], |r| r.text(3))
            .unwrap();
        rows.join(" | ")
    }

    pub(crate) fn prune_sensor_ticks(&self, keep: usize) -> Result<(), Error> {
        self.conn().execute(
            "DELETE FROM sensor_ticks WHERE id NOT IN
             (SELECT id FROM sensor_ticks ORDER BY id DESC LIMIT ?1)",
            args![keep as i64],
        )?;
        Ok(())
    }
}

/// what is executing right now, counted against `limits`. "executing" is
/// claimed and not finished — the set a concurrency limit is about.
fn in_flight(db: &mut impl Exec, limits: &Limits) -> Result<InFlight, Error> {
    let mut counts = InFlight::new();
    let rows = db.query(
        "SELECT job, tags FROM runs
         WHERE claimed_by IS NOT NULL AND status IN ('queued', 'running')",
        args![],
        |r| Ok((r.text(0)?, tags_from_col(r, 1)?)),
    )?;
    for (job, tags) in rows {
        counts.take(limits, &job, &tags);
    }
    Ok(counts)
}

/// the queue itself: runs nobody has claimed, in the order a dispatcher takes
/// them, with the plan each would execute.
///
/// a plain read on both backends, the dispatcher's own walk included: what a
/// dispatcher locks is the one row it decides on, not every row it considered.
fn queued(db: &mut impl Exec, limit: u32) -> Result<Vec<(Run, Option<Value>)>, Error> {
    db.query(
        &format!(
            "SELECT {RUN_COLS}, plan FROM runs
             WHERE status = 'queued' AND claimed_by IS NULL
             ORDER BY priority DESC, created_at, id LIMIT ?1"
        ),
        args![limit],
        |r| Ok((run_from_row(r)?, r.opt_json(17)?)),
    )
}

fn run_from_row(row: &AnyRow<'_>) -> Result<Run, Error> {
    Ok(Run {
        id: row.text(0)?,
        job: row.text(1)?,
        status: row.parse(2)?,
        trigger: row.parse(3)?,
        params: row.json(4)?,
        created_at: row.ts(5)?,
        started_at: row.opt_ts(6)?,
        finished_at: row.opt_ts(7)?,
        resumed_from: row.opt_text(8)?,
        error: row.opt_text(9)?,
        scheduled_for: row.opt_ts(10)?,
        tags: tags_from_col(row, 11)?,
        priority: row.int(12)?,
        claimed_by: row.opt_text(13)?,
        claimed_at: row.opt_ts(14)?,
        lease_until: row.opt_ts(15)?,
        actor: row.opt_text(16)?,
    })
}

/// a tag map as it is stored, or `None` when it is empty — a null column, not
/// an empty object, so an untagged run and a run written before tags existed
/// are the same row.
fn tags_col(tags: &RunTags) -> Option<String> {
    (!tags.is_empty()).then(|| serde_json::to_string(tags).expect("string map serializes"))
}

// null and anything that is not a flat string map read as no tags: the column
// is a fact about a run, and a run is not worth failing to list over it
fn tags_from_col(row: &AnyRow<'_>, idx: usize) -> Result<RunTags, Error> {
    match row.opt_text(idx)? {
        Some(s) => Ok(serde_json::from_str(&s).unwrap_or_default()),
        None => Ok(RunTags::new()),
    }
}

fn op_run_from_row(row: &AnyRow<'_>) -> Result<OpRun, Error> {
    Ok(OpRun {
        run_id: row.text(0)?,
        op: row.text(1)?,
        status: row.parse(2)?,
        attempts: row.count(3)?,
        started_at: row.opt_ts(4)?,
        finished_at: row.opt_ts(5)?,
        output: row.opt_json(6)?,
        metadata: row.opt_json(7)?,
        error: row.opt_text(8)?,
        pid: row.opt_int(9)?,
    })
}

fn notification_from_row(row: &AnyRow<'_>) -> Result<Notification, Error> {
    let next_attempt_at = row.opt_ts(5)?;
    let delivered_at = row.opt_ts(6)?;
    Ok(Notification {
        id: row.int(0)?,
        kind: row.text(1)?,
        payload: row.json(2)?,
        created_at: row.ts(3)?,
        attempts: row.count(4)?,
        // the state is these two columns and nothing else, worked out here so
        // the api and the delivery loop cannot come to different conclusions
        state: match (&delivered_at, &next_attempt_at) {
            (Some(_), _) => DeliveryState::Delivered,
            (None, Some(_)) => DeliveryState::Pending,
            (None, None) => DeliveryState::Failed,
        },
        next_attempt_at,
        delivered_at,
        last_error: row.opt_text(7)?,
    })
}

/// the event a backfill's last write leaves. a cancel is its own kind because
/// it is the one ending somebody asked for, and the two places that close a
/// backfill say it the same way.
fn backfill_over(
    id: i64,
    asset: &str,
    status: BackfillStatus,
    launched: usize,
    total: usize,
) -> NewEvent<'_> {
    let (kind, level) = match status {
        BackfillStatus::Canceled => (EventKind::BackfillCanceled, EventLevel::Warn),
        BackfillStatus::Failed => (EventKind::BackfillFinished, EventLevel::Error),
        _ => (EventKind::BackfillFinished, EventLevel::Info),
    };
    NewEvent::about(
        SubjectKind::Backfill,
        id.to_string(),
        kind,
        format!(
            "backfill of {asset} {}: {launched} of {total} keys launched",
            status.as_str()
        ),
    )
    .level(level)
    .data(json!({
        "asset": asset,
        "status": status,
        "launched": launched,
        "total": total,
    }))
}

/// how far a follower may read, from [`Store::settled_after`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settled {
    /// the highest seq with no hole between it and the cursor. everything up
    /// to here has committed and nothing can appear underneath it.
    pub upto: i64,
    /// the seq that stopped the walk: allocated and not visible, so either
    /// still committing or aborted. `None` when the log is unbroken.
    pub gap: Option<i64>,
    /// the next seq that *is* visible above the gap, when the scan reached one.
    /// a follower giving up on the gap resumes here rather than one seq at a
    /// time, so a pruned range costs one wait rather than one per row.
    pub after_gap: Option<i64>,
}

/// how long a follower waits on a missing seq before deciding it is never
/// coming.
///
/// a hole is a transaction still committing or one that aborted, and nothing
/// can tell those apart from outside. waiting forever stalls the follower on
/// every rolled-back write; not waiting at all skips events that were about to
/// land. so: wait, bounded, and say in the docs that a transaction slower than
/// this may be skipped. hestan's event-writing transactions are a handful of
/// statements each.
pub(crate) const SETTLE_GRACE: Duration = Duration::from_secs(2);

/// how far ahead the gap walk looks, which also caps what one poll delivers.
pub(crate) const SETTLE_SCAN: u32 = 2_000;

/// how far one pass of a follower may read, and whether it is also stepping
/// over a gap — from [`Store::readable`].
pub(crate) struct Step {
    pub(crate) ceiling: i64,
    /// the cursor to jump to afterwards, when a gap has been waited out.
    pub(crate) skip_to: Option<i64>,
}

/// bind one parameter and answer with its number, which is what a `?n` in the
/// fragment being built beside it needs.
fn bind<'a>(args: &mut Vec<Val<'a>>, v: Val<'a>) -> usize {
    args.push(v);
    args.len()
}

/// what a reader is asking the [log](Store::event_log) for. every field
/// narrows, an unset one does not, and they compose.
#[derive(Debug, Default, Clone)]
pub struct EventQuery {
    /// exactly this kind. an [`Unknown`](EventKind::Unknown) carrying a word
    /// this build does not know is a legitimate filter, and matches it.
    pub kind: Option<EventKind>,
    /// everything about one sort of thing: every asset event, every schedule
    /// event.
    pub subject_kind: Option<SubjectKind>,
    /// what it is about, matched the way [`Event::about`] reports it: a run
    /// event has no `subject` of its own and is found by its run id.
    pub subject: Option<String>,
    /// this level exactly, not this level and worse: three levels and a
    /// filter that means "show me the errors" is the one anybody types.
    pub level: Option<EventLevel>,
    /// at or after this, by the writer's clock rather than by `seq` — which
    /// is a window and not a cursor. `before` is the cursor.
    pub since: Option<DateTime<Utc>>,
    /// strictly before this, on the same terms.
    pub until: Option<DateTime<Utc>>,
    /// only what is below this seq, which is how a page asks for the one
    /// before it.
    pub before: Option<i64>,
}

impl EventQuery {
    /// the `WHERE` fragment and the parameters it binds, numbered from 1.
    ///
    /// the two reads share this so a filter cannot mean one thing to the query
    /// and another to the stream — which, given that the stream exists to
    /// deliver what the query would have returned, would be a hard thing to
    /// notice and an easy thing to do.
    fn sql(&self, dialect: Dialect) -> (String, Vec<Val<'_>>) {
        let opt = dialect.text_param();
        let mut args: Vec<Val<'_>> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();
        if let Some(kind) = &self.kind {
            let n = bind(&mut args, Val::Text(Cow::Borrowed(kind.as_str())));
            clauses.push(format!("kind = ?{n}"));
        }
        if let Some(sk) = &self.subject_kind {
            let n = bind(&mut args, Val::Text(Cow::Borrowed(sk.as_str())));
            clauses.push(format!("subject_kind = ?{n}"));
        }
        if let Some(subject) = &self.subject {
            let n = bind(&mut args, Val::Text(Cow::Borrowed(subject.as_str())));
            // a run event keeps its subject null and is named by `run_id`, so
            // asking for one by id has to look in both places — the v17
            // migration says why the column was not filled in
            clauses.push(format!(
                "(subject = ?{n} OR (subject IS NULL AND run_id = ?{n}))"
            ));
        }
        if let Some(level) = self.level {
            let n = bind(&mut args, Val::Text(Cow::Borrowed(level.as_str())));
            clauses.push(format!("level = ?{n}"));
        }
        if let Some(since) = self.since {
            let n = bind(&mut args, Val::Text(Cow::Owned(since.to_rfc3339())));
            clauses.push(format!("ts >= ?{n}{opt}"));
        }
        if let Some(until) = self.until {
            let n = bind(&mut args, Val::Text(Cow::Owned(until.to_rfc3339())));
            clauses.push(format!("ts < ?{n}{opt}"));
        }
        if let Some(before) = self.before {
            let n = bind(&mut args, Val::Int(before));
            clauses.push(format!("seq < ?{n}"));
        }
        // nothing asked for is every row, and `1 = 1` is what makes the
        // fragment a fragment rather than a special case at every call site
        let where_sql = match clauses.is_empty() {
            true => "1 = 1".to_string(),
            false => clauses.join(" AND "),
        };
        (where_sql, args)
    }
}

/// one event on its way into the log.
///
/// built by whichever subsystem did the thing and written by [`write_event`],
/// which every writer goes through — including the ones already inside a
/// transaction, which is nearly all of them. an event asserts that something
/// happened, so it is written where that happens and, wherever there is one,
/// in the same transaction: written next to the call instead, a crash in the
/// gap leaves a log that says a thing happened which did not, or a thing that
/// happened and left no trace. `docs/events.md` lists the three places that
/// have no transaction to join and says what the window is.
pub(crate) struct NewEvent<'a> {
    run_id: Option<&'a str>,
    subject_kind: SubjectKind,
    subject: Option<Cow<'a, str>>,
    op: Option<&'a str>,
    level: EventLevel,
    kind: EventKind,
    message: Cow<'a, str>,
    data: Option<Value>,
    /// who asked for the thing this event is about, where a person did. never
    /// set on anything a schedule, a sensor or a loop did on its own, and
    /// never a credential — only an [`Identity`](crate::Identity)'s name.
    actor: Option<&'a str>,
}

impl<'a> NewEvent<'a> {
    /// an event about one run. `subject` stays null: the run is `run_id`.
    pub(crate) fn run(run_id: &'a str, kind: EventKind, message: impl Into<Cow<'a, str>>) -> Self {
        NewEvent {
            run_id: Some(run_id),
            subject_kind: SubjectKind::Run,
            subject: None,
            op: None,
            level: EventLevel::Info,
            kind,
            message: message.into(),
            data: None,
            actor: None,
        }
    }

    /// an event about something that is not a run: an asset, a schedule, a
    /// sensor, a backfill, a job, or hestan itself.
    pub(crate) fn about(
        subject_kind: SubjectKind,
        subject: impl Into<Cow<'a, str>>,
        kind: EventKind,
        message: impl Into<Cow<'a, str>>,
    ) -> Self {
        NewEvent {
            run_id: None,
            subject_kind,
            subject: Some(subject.into()),
            op: None,
            level: EventLevel::Info,
            kind,
            message: message.into(),
            data: None,
            actor: None,
        }
    }

    pub(crate) fn op(mut self, op: Option<&'a str>) -> Self {
        self.op = op;
        self
    }

    pub(crate) fn level(mut self, level: EventLevel) -> Self {
        self.level = level;
        self
    }

    pub(crate) fn data(mut self, data: Value) -> Self {
        self.data = Some(data);
        self
    }

    pub(crate) fn actor(mut self, actor: Option<&'a str>) -> Self {
        self.actor = actor;
        self
    }
}

/// hestan's own run log, mirrored onto the `tracing` bus as it is written.
///
/// this is what "an event becomes a span event" means, and it is the whole of
/// it: the event lands on whatever span is current where it was written, which
/// for anything an op says is that attempt's `hestan.op` and for hestan's own
/// narration of a run is that run's `hestan.run`. a host with an otel layer
/// composed sees them on the spans; a host with none pays a level check.
///
/// `crate::logs::TRACE_TARGET` rather than the module path, because
/// [`CaptureLayer`](crate::CaptureLayer) has to be able to tell hestan talking
/// about the op from the op talking.
fn trace_event(ev: &NewEvent<'_>) {
    let (kind, subject) = (ev.kind.as_str(), ev.subject.as_deref().or(ev.run_id));
    let message = ev.message.as_ref();
    match ev.level {
        EventLevel::Info => {
            tracing::info!(target: crate::logs::TRACE_TARGET, kind, subject, "{message}");
        }
        EventLevel::Warn => {
            tracing::warn!(target: crate::logs::TRACE_TARGET, kind, subject, "{message}");
        }
        EventLevel::Error => {
            tracing::error!(target: crate::logs::TRACE_TARGET, kind, subject, "{message}");
        }
    }
}

/// append one event, inside whatever the caller is already in.
fn write_event(tx: &mut impl Exec, ev: &NewEvent<'_>, at: DateTime<Utc>) -> Result<(), Error> {
    trace_event(ev);
    tx.execute(
        "INSERT INTO events
             (run_id, subject_kind, subject, op, level, kind, message, data, ts, actor)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        args![
            ev.run_id,
            ev.subject_kind.as_str(),
            ev.subject.as_deref(),
            ev.op,
            ev.level.as_str(),
            ev.kind.as_str(),
            ev.message.as_ref(),
            ev.data.as_ref().map(|v| v.to_string()),
            at.to_rfc3339(),
            ev.actor
        ],
    )?;
    Ok(())
}

/// one asset build on its way into the history, everything about it that only
/// the op body could know.
///
/// staged by the body that computed it — the fingerprint, what it was built
/// from, the value, what the build reported — and written by whoever knows the
/// build actually landed. for an op that is
/// [`op_finished`](Store::op_finished), in the transaction that records the op
/// succeeding, because "this asset is current" is not a separate fact from
/// "the op that built it worked".
pub(crate) struct Built {
    pub(crate) asset: String,
    /// the key one build of a [partitioned asset](crate::Partitions) produced,
    /// and `None` for every unpartitioned one: history, staleness and seeding
    /// are all per `(asset, partition)`.
    pub(crate) partition: Option<String>,
    pub(crate) fingerprint: String,
    pub(crate) inputs: Value,
    pub(crate) value: Option<Value>,
    pub(crate) meta: Option<Value>,
}

/// insert one materialization and the [event](EventKind::AssetMaterialized)
/// that says so, inside the caller's transaction.
///
/// the event goes in that transaction rather than beside it for the reason
/// every other event does: "this asset was built" is exactly what this row is,
/// and a second copy written next to it is a copy a crash can disagree with.
fn write_materialization(
    tx: &mut impl Exec,
    built: &Built,
    run_id: Option<&str>,
    at: DateTime<Utc>,
) -> Result<(), Error> {
    let partition = built.partition.as_deref();
    tx.execute(
        "INSERT INTO asset_materializations
             (asset, partition, fingerprint, inputs, value, run_id, built_at, metadata)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        args![
            built.asset.as_str(),
            partition,
            built.fingerprint.as_str(),
            built.inputs.to_string(),
            built.value.as_ref().map(|v| v.to_string()),
            run_id,
            at.to_rfc3339(),
            built.meta.as_ref().map(|v| v.to_string()),
        ],
    )?;
    let message = match partition {
        None => format!("{} materialized", built.asset),
        Some(key) => format!("{}[{key}] materialized", built.asset),
    };
    let event = NewEvent::about(
        SubjectKind::Asset,
        built.asset.as_str(),
        EventKind::AssetMaterialized,
        message,
    )
    .data(json!({
        "partition": partition,
        "fingerprint": built.fingerprint,
        // where it happened, which is not what it is about — a probe
        // materializes outside any run and this is null there
        "run_id": run_id,
        // what the build reported, exactly as the op run carries it: the
        // rows, the bytes and the seconds are already tagged `Meta`
        "meta": built.meta,
    }));
    write_event(tx, &event, at)?;
    Ok(())
}

/// insert one notification, due immediately. inside whatever transaction the
/// caller is already in — that is the only way it is worth anything.
fn queue_note(tx: &mut impl Exec, payload: &Value, at: DateTime<Utc>) -> Result<(), Error> {
    tx.execute(
        "INSERT INTO notifications (kind, payload, created_at, next_attempt_at)
         VALUES (?1, ?2, ?3, ?3)",
        args![RUN_NOTIFICATION, payload.to_string(), at.to_rfc3339()],
    )?;
    Ok(())
}

fn preset_from_row(row: &AnyRow<'_>) -> Result<Preset, Error> {
    Ok(Preset {
        job: row.text(0)?,
        name: row.text(1)?,
        params: row.json(2)?,
        created_at: row.ts(3)?,
    })
}

fn event_from_row(row: &AnyRow<'_>) -> Result<Event, Error> {
    Ok(Event {
        seq: row.int(0)?,
        run_id: row.opt_text(1)?,
        // neither of these two can fail to parse, by construction: a word this
        // build does not know is an `Unknown` arm rather than an error, so one
        // row from a newer writer cannot break the page it is on
        subject_kind: row.parse(2)?,
        subject: row.opt_text(3)?,
        op: row.opt_text(4)?,
        level: row.parse(5)?,
        kind: row.parse(6)?,
        message: row.text(7)?,
        data: row.opt_json(8)?,
        ts: row.ts(9)?,
        actor: row.opt_text(10)?,
    })
}

fn op_log_from_row(row: &AnyRow<'_>) -> Result<OpLog, Error> {
    Ok(OpLog {
        id: row.int(0)?,
        run_id: row.text(1)?,
        op: row.text(2)?,
        attempt: row.count(3)?,
        at: row.ts(4)?,
        stream: row.opt_parse(5)?,
        level: row.opt_parse(6)?,
        target: row.opt_text(7)?,
        message: row.text(8)?,
    })
}

fn materialization_from_row(row: &AnyRow<'_>) -> Result<Materialization, Error> {
    Ok(Materialization {
        id: row.int(0)?,
        asset: row.text(1)?,
        partition: row.opt_text(2)?,
        fingerprint: row.text(3)?,
        inputs: row.json(4)?,
        value: row.opt_json(5)?,
        run_id: row.opt_text(6)?,
        built_at: row.ts(7)?,
        metadata: row.opt_json(8)?,
    })
}

fn asset_check_from_row(row: &AnyRow<'_>) -> Result<AssetCheckRow, Error> {
    Ok(AssetCheckRow {
        id: row.int(0)?,
        asset: row.text(1)?,
        partition: row.opt_text(2)?,
        check: row.text(3)?,
        run_id: row.text(4)?,
        status: row.parse(5)?,
        severity: row.parse(6)?,
        message: row.opt_text(7)?,
        metadata: row.opt_json(8)?,
        checked_at: row.ts(9)?,
    })
}

fn backfill_from_row(row: &AnyRow<'_>) -> Result<Backfill, Error> {
    let list = |idx: usize| -> Result<Vec<String>, Error> {
        serde_json::from_str(&row.text(idx)?).map_err(|e| Error::Column(idx, e.to_string()))
    };
    Ok(Backfill {
        id: row.int(0)?,
        asset: row.text(1)?,
        from_key: row.text(2)?,
        to_key: row.text(3)?,
        partitions: list(4)?,
        run_ids: list(5)?,
        total: row.size(6)?,
        launched: row.size(7)?,
        created_at: row.ts(8)?,
        finished_at: row.opt_ts(9)?,
        status: row.parse(10)?,
    })
}

fn sensor_tick_from_row(row: &AnyRow<'_>) -> Result<SensorTick, Error> {
    Ok(SensorTick {
        id: row.int(0)?,
        sensor: row.text(1)?,
        evaluated_at: row.ts(2)?,
        outcome: row.parse(3)?,
        launched: row.count(4)?,
        skipped: row.count(5)?,
        duration_ms: row.millis(6)?,
        error: row.opt_text(7)?,
    })
}

fn tick_from_row(row: &AnyRow<'_>) -> Result<Tick, Error> {
    Ok(Tick {
        id: row.int(0)?,
        job: row.text(1)?,
        expr: row.text(2)?,
        scheduled_for: row.ts(3)?,
        fired_at: row.ts(4)?,
        outcome: row.parse(5)?,
        run_id: row.opt_text(6)?,
        error: row.opt_text(7)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Trigger;
    use crate::schedule::Schedule;
    use chrono::TimeZone;
    use serde_json::json;

    fn mk_run(id: &str, job: &str, created_at: DateTime<Utc>) -> Run {
        Run {
            id: id.into(),
            job: job.into(),
            status: RunStatus::Queued,
            trigger: Trigger::Schedule,
            params: json!({"limit": 5}),
            created_at,
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: None,
            scheduled_for: None,
            tags: Default::default(),
            priority: 0,
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
            actor: None,
        }
    }

    /// where the postgres cases connect. unset means no postgres on this
    /// machine and every postgres case skips itself, so the suite still passes
    /// with nothing installed.
    #[cfg(feature = "postgres")]
    const TEST_PG: &str = "HESTAN_TEST_PG";

    /// one case's database, on one backend.
    ///
    /// cases are handed this rather than a `Store` because several of them
    /// want a *second* handle to the same database: two connections is what a
    /// race needs, and one `Store` cloned is one connection behind one mutex.
    /// which also means sqlite cases run against a file rather than
    /// `":memory:"`, since a private memory database cannot be opened twice.
    enum Backend {
        Sqlite(tempfile::TempDir),
        #[cfg(feature = "postgres")]
        Postgres(Scratch),
    }

    impl Backend {
        /// the postgres half of a run of the suite, when there is one.
        #[cfg(feature = "postgres")]
        fn postgres() -> Option<Backend> {
            Scratch::new().map(Backend::Postgres)
        }

        #[cfg(not(feature = "postgres"))]
        fn postgres() -> Option<Backend> {
            None
        }

        /// what a child process would be handed to reach the same database: a
        /// path or a url.
        fn target(&self) -> String {
            match self {
                Backend::Sqlite(dir) => dir.path().join("hestan.db").display().to_string(),
                #[cfg(feature = "postgres")]
                Backend::Postgres(pg) => pg.url.clone(),
            }
        }

        /// a new handle to this database — a new connection, every call.
        fn store(&self) -> Store {
            match self {
                Backend::Sqlite(dir) => Store::open(dir.path().join("hestan.db").to_str().unwrap()),
                #[cfg(feature = "postgres")]
                Backend::Postgres(pg) => Store::connect(&pg.url),
            }
            .unwrap()
        }

        fn name(&self) -> &'static str {
            match self {
                Backend::Sqlite(_) => "sqlite",
                #[cfg(feature = "postgres")]
                Backend::Postgres(_) => "postgres",
            }
        }
    }

    /// run one case against every backend this build can reach: sqlite always,
    /// and postgres when `HESTAN_TEST_PG` names a server.
    ///
    /// one suite run twice, rather than two suites. a second set of cases for
    /// the second backend is exactly how two backends quietly come to disagree
    /// — the cases that were never copied are the ones nobody notices — so
    /// there is no second set. a machine with no postgres runs the sqlite half
    /// and passes, which is what makes that honest rather than optional.
    fn both(case: impl Fn(&Backend)) {
        let mut backends = vec![Backend::Sqlite(tempfile::tempdir().unwrap())];
        backends.extend(Backend::postgres());
        for db in &backends {
            // the same case runs twice and a bare panic would not say which
            // half of it failed, which is the first thing you want to know
            let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| case(db)));
            if let Err(panic) = ran {
                eprintln!("^^ the case above failed on {}", db.name());
                std::panic::resume_unwind(panic);
            }
        }
    }

    /// one case's postgres database: a schema of its own on the server
    /// `HESTAN_TEST_PG` names, dropped when the fixture is.
    ///
    /// a schema rather than a database per case because the isolation is the
    /// same and creating one is milliseconds. `options` is how a url carries a
    /// session setting, which is what puts every connection a case opens —
    /// including one in a child process — in the same schema.
    #[cfg(feature = "postgres")]
    struct Scratch {
        server: String,
        url: String,
        schema: String,
    }

    #[cfg(feature = "postgres")]
    impl Scratch {
        fn new() -> Option<Scratch> {
            let server = std::env::var(TEST_PG).ok()?;
            let schema = format!("hestan_{}", uuid::Uuid::now_v7().simple());
            crate::pg::unmigrated(&server)
                .expect("HESTAN_TEST_PG names a server this test can reach")
                .batch(&format!("CREATE SCHEMA {schema}"))
                .unwrap();
            let sep = match server.contains('?') {
                true => '&',
                false => '?',
            };
            let url = format!("{server}{sep}options=-c%20search_path%3D{schema}");
            Some(Scratch {
                server,
                url,
                schema,
            })
        }

        fn store(&self) -> Store {
            Store::connect(&self.url).unwrap()
        }
    }

    #[cfg(feature = "postgres")]
    impl Drop for Scratch {
        fn drop(&mut self) {
            if let Ok(mut admin) = crate::pg::unmigrated(&self.server) {
                let _ = admin.batch(&format!("DROP SCHEMA {} CASCADE", self.schema));
            }
        }
    }

    #[test]
    fn run_lifecycle_roundtrips() {
        both(|db| {
            let store = db.store();
            let run = mk_run("r1", "etl", Utc::now());
            store.create_run(&run, &["a".into(), "b".into()]).unwrap();

            let got = store.run("r1").unwrap().unwrap();
            assert_eq!(got.status, RunStatus::Queued);
            assert_eq!(got.trigger, Trigger::Schedule);
            assert_eq!(got.params, json!({"limit": 5}));

            let ops = store.op_runs("r1").unwrap();
            assert_eq!(ops.len(), 2);
            assert!(
                ops.iter()
                    .all(|o| o.status == OpStatus::Pending && o.attempts == 0)
            );

            store.run_started("r1", Utc::now()).unwrap();
            store.op_started("r1", "a", 1).unwrap();
            let first_start = store.op_runs("r1").unwrap()[0].started_at.unwrap();
            store.op_started("r1", "a", 2).unwrap();
            store
                .op_finished(
                    "r1",
                    "a",
                    OpStatus::Success,
                    Some(&json!({"rows": 3})),
                    Some(&json!({"rows": {"int": 3}})),
                    None,
                    &[],
                )
                .unwrap();
            store
                .op_finished("r1", "b", OpStatus::Failed, None, None, Some("boom"), &[])
                .unwrap();
            store
                .run_finished("r1", RunStatus::Failed, None, Utc::now(), None)
                .unwrap();

            let got = store.run("r1").unwrap().unwrap();
            assert_eq!(got.status, RunStatus::Failed);
            assert!(got.started_at.is_some() && got.finished_at.is_some());

            let ops = store.op_runs("r1").unwrap();
            assert_eq!(ops[0].attempts, 2);
            assert_eq!(ops[0].started_at.unwrap(), first_start);
            assert_eq!(ops[0].output, Some(json!({"rows": 3})));
            assert_eq!(ops[0].metadata, Some(json!({"rows": {"int": 3}})));
            assert_eq!(ops[1].status, OpStatus::Failed);
            assert_eq!(ops[1].error.as_deref(), Some("boom"));
            assert_eq!(ops[1].metadata, None, "a failure reported no facts");
        });
    }

    #[test]
    fn events_filter_by_seq() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &[])
                .unwrap();
            store
                .append_event(
                    "r1",
                    Some("a"),
                    EventLevel::Warn,
                    EventKind::Log,
                    "flaky",
                    None,
                )
                .unwrap();
            store
                .append_event(
                    "r1",
                    Some("a"),
                    EventLevel::Error,
                    EventKind::OpFailed,
                    "boom",
                    Some(&json!({"error": "boom"})),
                )
                .unwrap();

            let all = store.events("r1", 0).unwrap();
            assert_eq!(all.len(), 3);
            assert_eq!(all[0].op, None);
            assert_eq!(all[0].kind, EventKind::RunQueued);
            assert_eq!(all[1].level, EventLevel::Warn);
            assert_eq!(all[1].data, None);
            assert_eq!(all[2].kind, EventKind::OpFailed);
            assert_eq!(all[2].data, Some(json!({"error": "boom"})));

            let tail = store.events("r1", all[0].seq).unwrap();
            assert_eq!(tail.len(), 2);
            assert_eq!(tail[0].message, "flaky");

            // a run event says which run in the column it always did, and its
            // subject stays null: `about` is where the two become one answer
            assert!(all.iter().all(|e| e.run_id.as_deref() == Some("r1")));
            assert!(all.iter().all(|e| e.subject_kind == SubjectKind::Run));
            assert!(all.iter().all(|e| e.about() == Some("r1")));
        });
    }

    /// the newest event of `kind`, or a panic naming what was there instead.
    fn newest(store: &Store, kind: EventKind) -> Event {
        let q = EventQuery {
            kind: Some(kind.clone()),
            ..EventQuery::default()
        };
        store.event_log(&q, 1).unwrap().pop().unwrap_or_else(|| {
            let seen: Vec<String> = store
                .event_log(&EventQuery::default(), 50)
                .unwrap()
                .iter()
                .map(|e| e.kind.to_string())
                .collect();
            panic!("no {kind} event; the log holds {seen:?}")
        })
    }

    // the point of the whole phase: every subsystem's work reaches the one log,
    // saying what it was about. written by the subsystem that does the work and
    // in the transaction that does it, which is what the cases below reach
    // through the store method rather than through an event api of their own
    #[test]
    fn each_subsystem_writes_an_event_about_its_own_subject() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                .unwrap();

            store
                .record_materialization(
                    "sales/orders",
                    Some("2026-01-01"),
                    "fp",
                    &json!({}),
                    None,
                    Some("r1"),
                    Some(&json!({"rows": {"count": 12}})),
                )
                .unwrap();
            let ev = newest(&store, EventKind::AssetMaterialized);
            assert_eq!(ev.subject_kind, SubjectKind::Asset);
            assert_eq!(ev.subject.as_deref(), Some("sales/orders"));
            assert_eq!(
                ev.run_id, None,
                "the run is where it happened, not what it is about"
            );
            let data = ev.data.unwrap();
            assert_eq!(data["partition"], json!("2026-01-01"));
            assert_eq!(data["run_id"], json!("r1"));
            assert_eq!(data["meta"], json!({"rows": {"count": 12}}));

            store
                .record_check(
                    "sales/orders",
                    None,
                    "not_empty",
                    "r1",
                    CheckStatus::Failed,
                    Severity::Warn,
                    Some("0 rows"),
                    None,
                )
                .unwrap();
            let ev = newest(&store, EventKind::CheckFailed);
            assert_eq!(ev.subject.as_deref(), Some("sales/orders"));
            // a warn check that failed did not fail the run, and the level says
            // so rather than the verdict saying it twice
            assert_eq!(ev.level, EventLevel::Warn);
            assert_eq!(ev.data.unwrap()["check"], json!("not_empty"));

            let due = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            store
                .record_tick(
                    "etl",
                    "0 * * * *",
                    due,
                    TickOutcome::Fired,
                    false,
                    Some("r1"),
                    None,
                )
                .unwrap();
            let ev = newest(&store, EventKind::ScheduleFired);
            assert_eq!(ev.subject_kind, SubjectKind::Schedule);
            assert_eq!(ev.subject.as_deref(), Some("etl"));
            assert_eq!(ev.data.unwrap()["run_id"], json!("r1"));

            // the same tick outcome, for an occurrence downtime swallowed: a
            // different kind, because "it fired" and "it caught up" are the two
            // things anybody asks a schedule after a restart
            store
                .record_tick(
                    "etl",
                    "0 * * * *",
                    due,
                    TickOutcome::Fired,
                    true,
                    Some("r1"),
                    None,
                )
                .unwrap();
            assert_eq!(
                newest(&store, EventKind::ScheduleCaughtUp)
                    .subject
                    .as_deref(),
                Some("etl")
            );

            store
                .record_sensor_tick(
                    "watch",
                    SensorOutcome::Fired,
                    1,
                    0,
                    12,
                    &["r1".to_string()],
                    None,
                )
                .unwrap();
            let ev = newest(&store, EventKind::SensorTick);
            assert_eq!(ev.subject_kind, SubjectKind::Sensor);
            assert_eq!(ev.subject.as_deref(), Some("watch"));
            assert_eq!(ev.data.unwrap()["runs"], json!(["r1"]));

            // an evaluation that looked and found nothing is a tick and not an
            // event: a sensor polling every five seconds would otherwise be
            // ninety-nine rows in a hundred of the log
            let before = store.event_watermark().unwrap();
            store
                .record_sensor_tick("watch", SensorOutcome::Fired, 0, 0, 3, &[], None)
                .unwrap();
            assert_eq!(store.sensor_ticks(Some("watch"), 10).unwrap().len(), 2);
            assert_eq!(store.event_watermark().unwrap(), before);
            // one that declined a keyed request did something, and says so
            store
                .record_sensor_tick("watch", SensorOutcome::Fired, 0, 1, 3, &[], None)
                .unwrap();
            assert_eq!(
                newest(&store, EventKind::SensorTick).data.unwrap()["skipped"],
                json!(1)
            );

            let id = store
                .create_backfill("sales/orders", "a", "c", &["a".into(), "b".into()], None)
                .unwrap();
            let ev = newest(&store, EventKind::BackfillStarted);
            assert_eq!(ev.subject_kind, SubjectKind::Backfill);
            assert_eq!(ev.subject.as_deref(), Some(id.to_string().as_str()));
            store
                .backfill_launched(id, "sales/orders", "r1", 1, 2)
                .unwrap();
            assert_eq!(
                newest(&store, EventKind::BackfillChunk).data.unwrap()["launched"],
                json!(1)
            );
            store
                .finish_backfill(id, "sales/orders", BackfillStatus::Canceled, 1, 2)
                .unwrap();
            assert_eq!(
                newest(&store, EventKind::BackfillCanceled)
                    .subject
                    .as_deref(),
                Some(id.to_string().as_str())
            );

            // a delivery, and one hestan has stopped trying: only the second
            // failure kind is an event, since the seven retries before it are
            // the mechanism working rather than news
            store
                .run_finished(
                    "r1",
                    RunStatus::Failed,
                    None,
                    Utc::now(),
                    Some(&json!({"run_id": "r1"})),
                )
                .unwrap();
            let note = store.notifications(None, 10).unwrap().pop().unwrap();
            store
                .delivery_failed(note.id, 1, Some(Utc::now()), "503")
                .unwrap();
            assert!(
                store
                    .event_log(
                        &EventQuery {
                            kind: Some(EventKind::NotificationFailed),
                            ..EventQuery::default()
                        },
                        1
                    )
                    .unwrap()
                    .is_empty(),
                "a retry that is still coming is not an event"
            );
            store.delivery_failed(note.id, 8, None, "503").unwrap();
            let ev = newest(&store, EventKind::NotificationFailed);
            assert_eq!(ev.subject_kind, SubjectKind::System);
            assert_eq!(ev.level, EventLevel::Error);
            store.delivered(note.id, Utc::now()).unwrap();
            assert_eq!(
                newest(&store, EventKind::NotificationDelivered)
                    .subject
                    .as_deref(),
                Some(note.id.to_string().as_str())
            );

            // and what retention took, per job, in the transaction that took it
            store
                .backdate_run("r1", Utc::now() - chrono::Duration::days(30))
                .unwrap();
            store
                .conn()
                .execute(
                    "UPDATE runs SET created_at = ?2 WHERE id = ?1",
                    args!["r1", (Utc::now() - chrono::Duration::days(30)).to_rfc3339()],
                )
                .unwrap();
            let removed = prune(&store, &Retention::days(7));
            assert_eq!(removed, 1);
            let ev = newest(&store, EventKind::RetentionPruned);
            assert_eq!(ev.subject_kind, SubjectKind::Job);
            assert_eq!(ev.subject.as_deref(), Some("etl"));
            assert_eq!(ev.data.unwrap()["runs"], json!(1));
            // the run's own events went with it and this one did not: it
            // belongs to the job, which is still there
            assert!(store.events("r1", 0).unwrap().is_empty());
        });
    }

    // a lease that ran out is the one run event that is not about what the run
    // did — and both policies write it, because "who stopped answering" is the
    // question either way
    #[test]
    fn a_reclaim_says_so_whichever_policy_took_it() {
        both(|db| {
            let store = db.store();
            for (id, policy) in [("failed", Reclaim::Fail), ("requeued", Reclaim::Requeue)] {
                store
                    .create_run(&mk_run(id, "etl", Utc::now()), &[])
                    .unwrap();
                store
                    .plant_claim(id, "gone", Some(Utc::now() - chrono::Duration::minutes(5)))
                    .unwrap();
                store.reclaim_expired(policy, |_| None).unwrap();
                let kinds: Vec<EventKind> = store
                    .events(id, 0)
                    .unwrap()
                    .into_iter()
                    .map(|e| e.kind)
                    .collect();
                assert!(kinds.contains(&EventKind::RunReclaimed), "{id}: {kinds:?}");
                let ev = store
                    .events(id, 0)
                    .unwrap()
                    .into_iter()
                    .find(|e| e.kind == EventKind::RunReclaimed)
                    .unwrap();
                assert_eq!(ev.data.unwrap()["claimer"], json!("gone"));
                assert_eq!(ev.subject_kind, SubjectKind::Run);
                assert_eq!(ev.run_id.as_deref(), Some(id));
            }
            // failing one also ends it, and the terminal event is the last word
            assert_eq!(
                store.events("failed", 0).unwrap().pop().unwrap().kind,
                EventKind::RunFailed
            );
        });
    }

    // an event log consumers cannot rely on is a log nobody consumes, so every
    // kind's payload is documented and this is what holds the documentation to
    // it: the keys `docs/events.md` promises, read back off the column
    #[test]
    fn every_payload_round_trips_the_keys_its_kind_promises() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                .unwrap();
            let queued = store.events("r1", 0).unwrap().remove(0);
            let data = queued.data.unwrap();
            assert_eq!(data["job"], json!("etl"));
            assert_eq!(data["trigger"], json!("schedule"));
            assert_eq!(data["priority"], json!(0));

            // the phase-19 tagged map, unchanged from what the op reported:
            // a reader that already renders `Meta` renders this
            let meta = json!({
                "rows": {"count": 1_240},
                "size": {"bytes": 4_096},
                "took": {"duration_secs": 1.5},
                "source": {"url": "https://example.invalid/x"},
            });
            store
                .record_materialization("sales", None, "fp", &json!({}), None, None, Some(&meta))
                .unwrap();
            assert_eq!(
                newest(&store, EventKind::AssetMaterialized).data.unwrap()["meta"],
                meta
            );
            // and each of those tags still reads as the type it was written as
            for (name, value) in meta.as_object().unwrap() {
                assert!(
                    crate::op::Meta::from_tagged(value).is_some(),
                    "{name} stopped being a Meta"
                );
            }

            let check_meta = json!({"rows": {"count": 0}});
            store
                .record_check(
                    "sales",
                    Some("2026-01-01"),
                    "not_empty",
                    "r1",
                    CheckStatus::Failed,
                    Severity::Error,
                    Some("0 rows"),
                    Some(&check_meta),
                )
                .unwrap();
            let data = newest(&store, EventKind::CheckFailed).data.unwrap();
            assert_eq!(data["severity"], json!("error"));
            assert_eq!(data["status"], json!("failed"));
            assert_eq!(data["partition"], json!("2026-01-01"));
            assert_eq!(data["meta"], check_meta);

            let id = store
                .create_backfill("sales", "a", "b", &["a".into(), "b".into()], None)
                .unwrap();
            let data = newest(&store, EventKind::BackfillStarted).data.unwrap();
            assert_eq!(data["total"], json!(2));
            assert_eq!(data["asset"], json!("sales"));
            store
                .finish_backfill(id, "sales", BackfillStatus::Complete, 2, 2)
                .unwrap();
            let data = newest(&store, EventKind::BackfillFinished).data.unwrap();
            assert_eq!(data["launched"], json!(2));
            assert_eq!(data["status"], json!("complete"));

            store
                .record_tick(
                    "etl",
                    "0 * * * *",
                    Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                    TickOutcome::Skipped,
                    false,
                    None,
                    Some("run still active"),
                )
                .unwrap();
            let data = newest(&store, EventKind::ScheduleSkipped).data.unwrap();
            assert_eq!(data["expr"], json!("0 * * * *"));
            assert_eq!(data["scheduled_for"], json!("2026-01-01T00:00:00Z"));
            assert_eq!(data["error"], json!("run still active"));
            assert_eq!(data["run_id"], Value::Null);
        });
    }

    // a payload written by a build that knows more kinds than this one. the
    // whole page must still read: one unrecognised word breaking the query
    // around it is exactly the failure a documented log cannot have
    #[test]
    fn a_kind_from_a_newer_writer_reads_rather_than_breaking_the_page() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &[])
                .unwrap();
            store
                .conn()
                .execute(
                    "INSERT INTO events (run_id, subject_kind, subject, level, kind, message, data, ts)
                     VALUES (NULL, ?1, ?2, 'info', ?3, 'from the future', ?4, ?5)",
                    args![
                        "quantum",
                        "q1",
                        "quantum_entangled",
                        r#"{"spin": "up"}"#,
                        Utc::now().to_rfc3339()
                    ],
                )
                .unwrap();

            let log = store.event_log(&EventQuery::default(), 50).unwrap();
            assert_eq!(
                log.len(),
                2,
                "the row from the future took the page with it"
            );
            assert_eq!(
                log[0].kind,
                EventKind::Unknown("quantum_entangled".to_string())
            );
            assert_eq!(
                log[0].subject_kind,
                SubjectKind::Unknown("quantum".to_string())
            );
            assert_eq!(log[0].data, Some(json!({"spin": "up"})));
            // it reads as itself on the way out too, rather than as a word this
            // build made up to stand in for it
            assert_eq!(log[0].kind.to_string(), "quantum_entangled");
            assert_eq!(
                serde_json::to_value(&log[0]).unwrap()["kind"],
                json!("quantum_entangled")
            );
            // and it is filterable by the name it was written under
            let q = EventQuery {
                kind: Some("quantum_entangled".parse().unwrap()),
                ..EventQuery::default()
            };
            assert_eq!(store.event_log(&q, 10).unwrap().len(), 1);
        });
    }

    /// read what has settled, the way a follower does: everything committed on
    /// a backend that settles in order, and the unbroken run above the cursor
    /// on one that does not.
    ///
    /// the stream's rule without the stream's grace timer, so a case can drive
    /// it without a socket and without waiting two seconds to find out that a
    /// gap it deliberately made is still there.
    fn settled_batch(store: &Store, cursor: i64, limit: u32) -> Vec<Event> {
        let ceiling = match store.settles_in_order() {
            true => store.event_watermark().unwrap(),
            false => store.settled_after(cursor, 10_000).unwrap().upto,
        };
        store
            .event_tail(&EventQuery::default(), cursor, Some(ceiling), limit)
            .unwrap()
    }

    // eight writers, a follower reading beside them, and then the whole table
    // compared against what it saw: every seq exactly once, in order, none
    // skipped and none twice, on both backends.
    //
    // this is the ordinary-operation case and it is worth knowing what it does
    // *not* do: the window in which a real writer is mid-commit is microseconds
    // wide, and a follower polling beside it lands in that window rarely enough
    // that removing the rule below does not reliably fail this. the case under
    // it forces the state instead, and that is the one with teeth.
    #[test]
    fn a_follower_reads_every_event_exactly_once_under_concurrent_writers() {
        both(|db| {
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let writers: Vec<_> = (0..8)
                .map(|w| {
                    let store = db.store();
                    std::thread::spawn(move || {
                        for i in 0..25 {
                            store
                                .record_materialization(
                                    &format!("a{w}"),
                                    Some(&format!("k{i}")),
                                    "fp",
                                    &json!({}),
                                    None,
                                    None,
                                    None,
                                )
                                .unwrap();
                        }
                    })
                })
                .collect();

            let seen = {
                let (store, stop) = (db.store(), stop.clone());
                std::thread::spawn(move || {
                    let mut seen: Vec<i64> = Vec::new();
                    let mut cursor = 0;
                    // one pass after the writers are done, so the tail of the
                    // log is read with everything committed
                    let mut draining = false;
                    loop {
                        let batch = settled_batch(&store, cursor, 500);
                        for ev in &batch {
                            assert!(ev.seq > cursor, "the follower went backwards");
                            cursor = ev.seq;
                            seen.push(ev.seq);
                        }
                        if draining && batch.is_empty() {
                            return seen;
                        }
                        draining = stop.load(std::sync::atomic::Ordering::SeqCst);
                        std::thread::yield_now();
                    }
                })
            };

            for w in writers {
                w.join().unwrap();
            }
            stop.store(true, std::sync::atomic::Ordering::SeqCst);
            let seen = seen.join().unwrap();

            let all: Vec<i64> = db
                .store()
                .event_log(&EventQuery::default(), 1000)
                .unwrap()
                .into_iter()
                .rev()
                .map(|e| e.seq)
                .collect();
            assert_eq!(all.len(), 200, "not every write landed");
            assert_eq!(seen, all, "the follower skipped or repeated an event");
        });
    }

    // and the state the case above can only make likely, made certain: one
    // writer holding an uncommitted event while a later one commits over it.
    //
    // postgres only, and that is the finding rather than a gap: sqlite's
    // writers hold the database's write lock until they commit, so this state
    // is unreachable there and seq order is commit order.
    #[cfg(feature = "postgres")]
    #[test]
    fn an_uncommitted_event_holds_the_follower_back_rather_than_being_skipped() {
        let Some(pg) = Scratch::new() else {
            return;
        };
        let store = pg.store();
        let insert = "INSERT INTO events (subject_kind, subject, level, kind, message, ts)
                      VALUES ('asset', ?1, 'info', 'asset_materialized', 'held', ?2)";
        let (ready, is_ready) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let holder = {
            let url = pg.url.clone();
            std::thread::spawn(move || {
                let mut client = crate::pg::unmigrated(&url).unwrap();
                let mut tx = client.transaction().unwrap();
                tx.execute(insert, args!["first", Utc::now().to_rfc3339()])
                    .unwrap();
                ready.send(()).unwrap();
                released.recv().unwrap();
                tx.commit().unwrap();
            })
        };
        is_ready.recv().unwrap();

        // committed, and above the seq the holder is sitting on
        store
            .record_materialization("second", None, "fp", &json!({}), None, None, None)
            .unwrap();

        // a follower reading now must deliver neither: the visible one is above
        // a seq that is still in flight, and taking it would strand the other
        let mut cursor = 0;
        for _ in 0..3 {
            for ev in settled_batch(&store, cursor, 100) {
                assert!(
                    ev.subject.as_deref() != Some("second"),
                    "the follower took an event over an uncommitted one"
                );
                cursor = ev.seq;
            }
        }

        release.send(()).unwrap();
        holder.join().unwrap();
        // and once it commits, both arrive, in seq order
        let mut seen: Vec<String> = Vec::new();
        for _ in 0..3 {
            for ev in settled_batch(&store, cursor, 100) {
                cursor = ev.seq;
                seen.push(ev.subject.clone().unwrap_or_default());
            }
        }
        assert_eq!(seen, ["first", "second"]);
    }

    // a run's events go when its run does, and always did. what v17 added
    // belongs to no run, so without a cap of its own an asset built every five
    // minutes would write a row here forever
    #[test]
    fn the_events_that_belong_to_no_run_have_a_cap_of_their_own() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &[])
                .unwrap();
            for i in 0..10 {
                store
                    .record_materialization(
                        &format!("a{i}"),
                        None,
                        "fp",
                        &json!({}),
                        None,
                        None,
                        None,
                    )
                    .unwrap();
            }

            assert_eq!(store.prune_events(4).unwrap(), 6);
            let kept = store
                .event_log(
                    &EventQuery {
                        subject_kind: Some(SubjectKind::Asset),
                        ..EventQuery::default()
                    },
                    50,
                )
                .unwrap();
            assert_eq!(kept.len(), 4);
            assert_eq!(kept[0].subject.as_deref(), Some("a9"), "the newest went");
            // and the run's own event is untouched: it is the run's to lose
            assert_eq!(store.events("r1", 0).unwrap().len(), 1);
        });
    }

    #[test]
    fn runs_filter_order_and_limit() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for (i, job) in [(0, "etl"), (1, "etl"), (2, "health"), (3, "etl")] {
                let run = mk_run(&format!("r{i}"), job, t0 + chrono::Duration::minutes(i));
                store.create_run(&run, &[]).unwrap();
            }

            let etl = store.runs(Some("etl"), None, None, None, None, 10).unwrap();
            assert_eq!(
                etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r3", "r1", "r0"]
            );
            assert_eq!(
                store.runs(None, None, None, None, None, 2).unwrap().len(),
                2
            );
            assert!(
                store
                    .runs(Some("nope"), None, None, None, None, 10)
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn runs_since_cutoff() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for (i, job) in [(0, "etl"), (1, "etl"), (2, "health"), (3, "etl")] {
                let run = mk_run(&format!("r{i}"), job, t0 + chrono::Duration::minutes(i));
                store.create_run(&run, &[]).unwrap();
            }

            let since = t0 + chrono::Duration::minutes(2);
            let recent = store.runs(None, Some(since), None, None, None, 10).unwrap();
            assert_eq!(
                recent.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r3", "r2"]
            );

            let etl = store
                .runs(Some("etl"), Some(since), None, None, None, 10)
                .unwrap();
            assert_eq!(
                etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r3"]
            );

            let future = t0 + chrono::Duration::hours(1);
            assert!(
                store
                    .runs(None, Some(future), None, None, None, 10)
                    .unwrap()
                    .is_empty()
            );
        });
    }

    #[test]
    fn runs_before_cutoff() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for (i, job) in [(0, "etl"), (1, "etl"), (2, "health"), (3, "etl")] {
                let run = mk_run(&format!("r{i}"), job, t0 + chrono::Duration::minutes(i));
                store.create_run(&run, &[]).unwrap();
            }

            let before = t0 + chrono::Duration::minutes(2);
            let older = store
                .runs(None, None, Some(before), None, None, 10)
                .unwrap();
            assert_eq!(
                older.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r1", "r0"]
            );

            let since = t0 + chrono::Duration::minutes(1);
            let etl = store
                .runs(Some("etl"), Some(since), Some(before), None, None, 10)
                .unwrap();
            assert_eq!(
                etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r1"]
            );

            let page = store.runs(None, None, None, None, None, 2).unwrap();
            assert_eq!(
                page.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r3", "r2"]
            );
            let next = store
                .runs(None, None, Some(page[1].created_at), None, None, 2)
                .unwrap();
            assert_eq!(
                next.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["r1", "r0"]
            );
        });
    }

    #[test]
    fn runs_composite_cursor_pages_ties() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            // r1 and r2 share an identical created_at string, deliberately
            store.create_run(&mk_run("r0", "etl", t0), &[]).unwrap();
            let tied = t0 + chrono::Duration::minutes(1);
            store.create_run(&mk_run("r1", "etl", tied), &[]).unwrap();
            store.create_run(&mk_run("r2", "etl", tied), &[]).unwrap();

            let mut seen: Vec<String> = Vec::new();
            let mut cursor: Option<(DateTime<Utc>, String)> = None;
            loop {
                let page = match &cursor {
                    Some((ts, id)) => store
                        .runs(None, None, Some(*ts), Some(id.as_str()), None, 1)
                        .unwrap(),
                    None => store.runs(None, None, None, None, None, 1).unwrap(),
                };
                let Some(run) = page.into_iter().next() else {
                    break;
                };
                cursor = Some((run.created_at, run.id.clone()));
                seen.push(run.id);
            }
            assert_eq!(seen, ["r2", "r1", "r0"]);
        });
    }

    #[test]
    fn recent_op_runs_window_and_order() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for i in 0..3 {
                let run = mk_run(&format!("r{i}"), "etl", t0 + chrono::Duration::minutes(i));
                store.create_run(&run, &["a".into(), "b".into()]).unwrap();
            }
            store
                .create_run(&mk_run("hx", "health", t0), &["h".into()])
                .unwrap();

            let rows = store.recent_op_runs("etl", 2).unwrap();
            assert_eq!(
                rows.iter().map(|o| o.run_id.as_str()).collect::<Vec<_>>(),
                ["r2", "r2", "r1", "r1"]
            );
            assert!(rows.iter().all(|o| o.op == "a" || o.op == "b"));

            assert_eq!(store.recent_op_runs("etl", 10).unwrap().len(), 6);
            assert!(store.recent_op_runs("nope", 5).unwrap().is_empty());
        });
    }

    /// the whole-store sweep, for cases that are about one policy rather than
    /// about which job got which. `retention::sweep` is the same walk with the
    /// per-job overrides resolved and the io managers asked in between.
    fn prune(store: &Store, policy: &Retention) -> usize {
        let now = Utc::now();
        store
            .run_jobs()
            .unwrap()
            .iter()
            .map(|job| {
                let doomed = store.doomed_runs(job, policy, now).unwrap();
                store.delete_runs(job, &doomed, now).unwrap()
            })
            .sum()
    }

    #[test]
    fn prune_runs_cascades_and_keeps_the_rest() {
        both(|db| {
            let store = db.store();
            let old = Utc::now() - chrono::Duration::days(10);
            for (id, status) in [
                ("os", RunStatus::Success),
                ("of", RunStatus::Failed),
                ("oc", RunStatus::Canceled),
            ] {
                store
                    .create_run(&mk_run(id, "etl", old), &["a".into()])
                    .unwrap();
                store
                    .run_finished(id, status, None, Utc::now(), None)
                    .unwrap();
            }
            store
                .create_run(&mk_run("live", "etl", old), &["a".into()])
                .unwrap();
            store.run_started("live", Utc::now()).unwrap();
            store
                .create_run(&mk_run("young", "etl", Utc::now()), &["a".into()])
                .unwrap();
            store
                .run_finished("young", RunStatus::Success, None, Utc::now(), None)
                .unwrap();
            store
                .set_op_state("etl", "a", &json!({"cursor": 9}))
                .unwrap();

            assert_eq!(prune(&store, &Retention::days(7)), 3);

            for id in ["os", "of", "oc"] {
                assert!(store.run(id).unwrap().is_none());
                assert!(
                    store.op_runs(id).unwrap().is_empty(),
                    "orphan op_runs for {id}"
                );
                assert!(
                    store.events(id, 0).unwrap().is_empty(),
                    "orphan events for {id}"
                );
            }
            let live = store.run("live").unwrap().unwrap();
            assert_eq!(live.status, RunStatus::Running);
            assert_eq!(store.op_runs("live").unwrap().len(), 1);
            assert!(store.run("young").unwrap().is_some());
            assert!(!store.events("young", 0).unwrap().is_empty());
            assert_eq!(
                store.op_state("etl", "a").unwrap(),
                Some(json!({"cursor": 9}))
            );

            assert_eq!(prune(&store, &Retention::days(7)), 0);
        });
    }

    // the conservative direction, at the boundary and from both sides: a run
    // is deleted only when the age rule and keep_last would both take it
    #[test]
    fn keep_last_and_the_age_cutoff_each_hold_a_run_the_other_would_delete() {
        both(|db| {
            let store = db.store();
            for (id, age) in [("oldest", 30), ("older", 20), ("old", 10), ("new", 1)] {
                let at = Utc::now() - chrono::Duration::days(age);
                store
                    .create_run(&mk_run(id, "etl", at), &["a".into()])
                    .unwrap();
                store
                    .run_finished(id, RunStatus::Success, None, Utc::now(), None)
                    .unwrap();
            }

            // keep_last holds two runs past an age cutoff that would take three
            assert_eq!(prune(&store, &Retention::days(7).keep_last(3)), 1);
            assert!(store.run("oldest").unwrap().is_none());
            assert!(store.run("older").unwrap().is_some(), "keep_last let it go");

            // and the age cutoff holds a run keep_last would drop: two of the three
            // left are inside 15 days, and keep_last(1) does not reach them
            assert_eq!(prune(&store, &Retention::days(15).keep_last(1)), 1);
            assert!(store.run("older").unwrap().is_none());
            assert!(
                store.run("old").unwrap().is_some(),
                "the age cutoff let it go"
            );
            assert!(store.run("new").unwrap().is_some());

            // keep_last on its own is a protection, not a policy: with no age rule
            // to hold anything back from, it deletes nothing at all
            assert_eq!(prune(&store, &Retention::default().keep_last(1)), 0);
            assert_eq!(
                store.runs(None, None, None, None, None, 10).unwrap().len(),
                2
            );
        });
    }

    #[test]
    fn failed_days_keeps_a_failure_and_drops_a_success_of_the_same_age() {
        both(|db| {
            let store = db.store();
            let old = Utc::now() - chrono::Duration::days(30);
            for (id, status) in [
                ("won", RunStatus::Success),
                ("lost", RunStatus::Failed),
                ("stopped", RunStatus::Canceled),
            ] {
                store
                    .create_run(&mk_run(id, "etl", old), &["a".into()])
                    .unwrap();
                store
                    .run_finished(id, status, None, Utc::now(), None)
                    .unwrap();
            }

            assert_eq!(prune(&store, &Retention::days(7).failed_days(90)), 1);
            assert!(store.run("won").unwrap().is_none());
            // a cancel is not a success either: what you keep longer is what went
            // wrong, and someone stopping a run is a thing that went wrong
            assert!(store.run("lost").unwrap().is_some());
            assert!(store.run("stopped").unwrap().is_some());

            assert_eq!(prune(&store, &Retention::days(7).failed_days(14)), 2);
        });
    }

    // a queued run older than the cutoff is a queue problem, not a retention
    // one, and deleting it would take work nobody has done yet
    #[test]
    fn retention_never_takes_a_run_that_has_not_finished() {
        both(|db| {
            let store = db.store();
            let old = Utc::now() - chrono::Duration::days(400);
            for id in ["waiting", "working"] {
                store
                    .create_run(&mk_run(id, "etl", old), &["a".into()])
                    .unwrap();
            }
            store.run_started("working", Utc::now()).unwrap();

            assert_eq!(prune(&store, &Retention::days(1).keep_last(0)), 0);
            assert_eq!(
                store.run("waiting").unwrap().unwrap().status,
                RunStatus::Queued
            );
            assert_eq!(
                store.run("working").unwrap().unwrap().status,
                RunStatus::Running
            );
        });
    }

    // the point of the whole part: written after the terminal row, a crash in
    // between loses the alert about the failure the alert existed to report.
    // one transaction is the only thing that rules it out, so the case makes
    // the insert fail and asserts the run row went back with it
    #[test]
    fn a_notification_and_its_run_row_land_together_or_not_at_all() {
        both(|db| {
            let store = db.store();
            let note = json!({"run_id": "r1", "job": "etl", "status": "failed"});
            for id in ["r1", "r2"] {
                store
                    .create_run(&mk_run(id, "etl", Utc::now()), &["a".into()])
                    .unwrap();
                store.run_started(id, Utc::now()).unwrap();
            }

            store
                .run_finished(
                    "r1",
                    RunStatus::Failed,
                    Some("boom"),
                    Utc::now(),
                    Some(&note),
                )
                .unwrap();
            assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Failed);
            assert_eq!(store.notifications(None, 10).unwrap().len(), 1);

            // and now with the insert refused, which is a crash between the two as
            // far as the transaction is concerned
            store.refuse_notifications().unwrap();
            let err = store
                .run_finished(
                    "r2",
                    RunStatus::Failed,
                    Some("boom"),
                    Utc::now(),
                    Some(&note),
                )
                .unwrap_err();
            assert!(err.to_string().contains("refused"), "{err}");
            // neither half landed: the run is still running and there is still one
            // notification, not two
            assert_eq!(store.run("r2").unwrap().unwrap().status, RunStatus::Running);
            assert_eq!(store.run("r2").unwrap().unwrap().error, None);
            assert_eq!(store.notifications(None, 10).unwrap().len(), 1);
        });
    }

    #[test]
    fn a_notifications_state_is_its_two_timestamps() {
        both(|db| {
            let store = db.store();
            let note = json!({"run_id": "r1"});
            for id in ["r1", "r2", "r3"] {
                store
                    .create_run(&mk_run(id, "etl", Utc::now()), &["a".into()])
                    .unwrap();
                store
                    .run_finished(id, RunStatus::Failed, None, Utc::now(), Some(&note))
                    .unwrap();
            }
            let queued = store.notifications(None, 10).unwrap();
            assert_eq!(queued.len(), 3);
            assert!(queued.iter().all(|n| n.state == DeliveryState::Pending));
            assert!(queued.iter().all(|n| n.next_attempt_at.is_some()));
            assert_eq!(queued[0].kind, "run");
            assert_eq!(queued[0].payload, note);

            // ids descend, so the last row written is first
            let (delivered, given_up) = (queued[0].id, queued[1].id);
            assert!(store.delivered(delivered, Utc::now()).unwrap());
            // a second mark finds nothing: two loops cannot both claim one row
            assert!(!store.delivered(delivered, Utc::now()).unwrap());
            store
                .delivery_failed(given_up, 8, None, "connection refused")
                .unwrap();

            let of = |state| store.notifications(Some(state), 10).unwrap();
            assert_eq!(of(DeliveryState::Delivered).len(), 1);
            assert_eq!(of(DeliveryState::Delivered)[0].id, delivered);
            let failed = of(DeliveryState::Failed);
            assert_eq!(failed.len(), 1);
            assert_eq!(failed[0].last_error.as_deref(), Some("connection refused"));
            assert_eq!(failed[0].attempts, 8);
            assert_eq!(of(DeliveryState::Pending).len(), 1);

            // and a given-up row is out of the delivery loop's way for good
            let due = store.due_notifications(Utc::now(), 10).unwrap();
            assert_eq!(due.len(), 1);
            assert_eq!(due[0].id, queued[2].id);
        });
    }

    #[test]
    fn retention_takes_delivered_notifications_and_leaves_the_rest() {
        both(|db| {
            let store = db.store();
            let old = Utc::now() - chrono::Duration::days(30);
            let note = json!({"run_id": "r1"});
            for id in ["sent", "waiting", "given-up"] {
                store
                    .create_run(&mk_run(id, "etl", old), &["a".into()])
                    .unwrap();
                store
                    .run_finished(id, RunStatus::Failed, None, old, Some(&note))
                    .unwrap();
            }
            let rows = store.notifications(None, 10).unwrap();
            store.delivered(rows[2].id, old).unwrap();
            store
                .delivery_failed(rows[0].id, 8, None, "gave up")
                .unwrap();

            assert_eq!(
                store
                    .prune_notifications(Utc::now() - chrono::Duration::days(7))
                    .unwrap(),
                1
            );
            let left = store.notifications(None, 10).unwrap();
            // an alert nobody received is not history however old it is: it is
            // something outstanding, and a sweep that cleared it would be the
            // same loss this table exists to prevent
            assert_eq!(left.len(), 2);
            assert!(left.iter().all(|n| n.delivered_at.is_none()));
        });
    }

    // what makes the sweep affordable on a table with a year of runs in it:
    // both halves are index seeks, and neither reads a run it is not about
    #[test]
    fn the_sweep_reaches_its_rows_through_the_index() {
        let store = Store::open(":memory:").unwrap();
        let plan = |sql: &str| store.sqlite_plan(sql);

        // the jobs walk seeks once per job rather than reading every run
        let jobs = plan(
            "WITH RECURSIVE names(job) AS (
                 SELECT MIN(job) FROM runs
                 UNION ALL
                 SELECT (SELECT MIN(job) FROM runs WHERE job > names.job)
                 FROM names WHERE job IS NOT NULL)
             SELECT job FROM names WHERE job IS NOT NULL",
        );
        assert!(jobs.contains("runs_job_created"), "{jobs}");
        assert!(!jobs.contains("SCAN runs"), "{jobs}");

        // and so does the walk to the doomed rows of one job
        let doomed = plan(
            "SELECT id FROM runs
             WHERE job = 'etl' AND status IN ('success', 'failed', 'canceled')
               AND created_at < '2026-01-01T00:00:00Z'",
        );
        assert!(doomed.contains("runs_job_created"), "{doomed}");
        assert!(!doomed.contains("SCAN runs"), "{doomed}");
    }

    #[test]
    fn the_log_cursor_pages_and_narrows_to_one_op() {
        both(|db| {
            let store = db.store();
            let mut budget = crate::logs::Budget::new();
            for op in ["load", "clean"] {
                for i in 0..5 {
                    budget.line(
                        &store,
                        &crate::logs::Attempt::new("r1", op, 1),
                        crate::logs::Source::Stream(crate::model::LogStream::Stdout),
                        &format!("{op} {i}"),
                    );
                }
            }

            let first = store.op_logs("r1", None, 0, 4).unwrap();
            assert_eq!(first.len(), 4);
            assert_eq!(first[0].message, "load 0");
            let next = store.op_logs("r1", None, first[3].id, 4).unwrap();
            assert_eq!(next[0].message, "load 4");
            // the cursor is the id, so the pages meet exactly once
            let rest = store.op_logs("r1", None, next[3].id, 100).unwrap();
            assert_eq!(rest.len(), 2);
            assert_eq!(rest[1].message, "clean 4");

            let one = store.op_logs("r1", Some("clean"), 0, 100).unwrap();
            assert_eq!(one.len(), 5);
            assert!(one.iter().all(|l| l.op == "clean"));
            assert!(
                store
                    .op_logs("r1", Some("nope"), 0, 100)
                    .unwrap()
                    .is_empty()
            );
            assert!(store.op_logs("other", None, 0, 100).unwrap().is_empty());
        });
    }

    #[test]
    fn retention_takes_captured_output_with_its_run() {
        both(|db| {
            let store = db.store();
            let old = Utc::now() - chrono::Duration::days(10);
            let mut budget = crate::logs::Budget::new();
            for (id, created) in [("gone", old), ("kept", Utc::now())] {
                store
                    .create_run(&mk_run(id, "etl", created), &["a".into()])
                    .unwrap();
                store
                    .run_finished(id, RunStatus::Success, None, Utc::now(), None)
                    .unwrap();
                budget.line(
                    &store,
                    &crate::logs::Attempt::new(id, "a", 1),
                    crate::logs::Source::Stream(crate::model::LogStream::Stdout),
                    "printed something",
                );
            }
            // and a run that is only halfway: a reclaim puts one back on the queue,
            // and the second claimer's page must still carry the first's output
            store
                .create_run(&mk_run("live", "etl", old), &["a".into()])
                .unwrap();
            budget.line(
                &store,
                &crate::logs::Attempt::new("live", "a", 1),
                crate::logs::Source::Stream(crate::model::LogStream::Stdout),
                "half done",
            );

            assert_eq!(prune(&store, &Retention::days(7)), 1);
            assert!(
                store.op_logs("gone", None, 0, 100).unwrap().is_empty(),
                "orphan op_logs outlived their run"
            );
            assert_eq!(store.op_logs("kept", None, 0, 100).unwrap().len(), 1);
            assert_eq!(store.op_logs("live", None, 0, 100).unwrap().len(), 1);
        });
    }

    #[test]
    fn active_run_check_tracks_lifecycle() {
        both(|db| {
            let store = db.store();
            assert!(!store.has_active_run("etl").unwrap());
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                .unwrap();
            assert!(store.has_active_run("etl").unwrap());
            store.run_started("r1", Utc::now()).unwrap();
            assert!(store.has_active_run("etl").unwrap());
            store
                .run_finished("r1", RunStatus::Failed, None, Utc::now(), None)
                .unwrap();
            assert!(!store.has_active_run("etl").unwrap());
        });
    }

    #[test]
    fn interrupted_runs_failed_on_startup() {
        both(|db| {
            let store = db.store();
            store
                .create_run(
                    &mk_run("dead", "etl", Utc::now()),
                    &["a".into(), "b".into()],
                )
                .unwrap();
            store.run_started("dead", Utc::now()).unwrap();
            store.op_started("dead", "a", 1).unwrap();

            let done = mk_run("done", "etl", Utc::now());
            store.create_run(&done, &["a".into()]).unwrap();
            store.run_started("done", Utc::now()).unwrap();
            store
                .op_finished("done", "a", OpStatus::Success, None, None, None, &[])
                .unwrap();
            store
                .run_finished("done", RunStatus::Success, None, Utc::now(), None)
                .unwrap();

            store.fail_interrupted().unwrap();

            let dead = store.run("dead").unwrap().unwrap();
            assert_eq!(dead.status, RunStatus::Failed);
            assert!(dead.finished_at.is_some());
            let ops = store.op_runs("dead").unwrap();
            assert_eq!(ops[0].status, OpStatus::Failed);
            assert_eq!(ops[0].error.as_deref(), Some("interrupted: process exited"));
            assert_eq!(ops[1].status, OpStatus::Skipped);
            let last = store.events("dead", 0).unwrap().pop().unwrap();
            assert!(last.message.contains("interrupted"));
            assert_eq!(last.kind, EventKind::RunFailed);

            assert_eq!(
                store.run("done").unwrap().unwrap().status,
                RunStatus::Success
            );
        });
    }

    // the schema as phase 1 shipped it: no kind/data on events, no schedule tables
    const PHASE1_SCHEMA: &str = r#"
    CREATE TABLE runs (
        id TEXT PRIMARY KEY,
        job TEXT NOT NULL,
        status TEXT NOT NULL,
        "trigger" TEXT NOT NULL,
        params TEXT NOT NULL,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT
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
        ts TEXT NOT NULL
    );
    CREATE INDEX events_run ON events(run_id, seq);
    INSERT INTO runs VALUES ('r1', 'etl', 'success', 'manual', '{}',
        '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:01+00:00', '2026-01-01T00:00:02+00:00');
    INSERT INTO op_runs (run_id, op, status, attempts) VALUES ('r1', 'a', 'success', 1);
    INSERT INTO events (run_id, op, level, message, ts)
        VALUES ('r1', NULL, 'info', 'run queued', '2026-01-01T00:00:00+00:00');
    "#;

    fn phase1_db(path: &str, user_version: u32) {
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.pragma_update(None, "user_version", user_version)
            .unwrap();
    }

    fn assert_migrated(path: &str) {
        let store = Store::open(path).unwrap();
        let run = store.run("r1").unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Success);
        let events = store.events("r1", 0).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::Log);
        assert_eq!(events[0].data, None);
        assert!(store.schedules().unwrap().is_empty());
        assert!(
            !store
                .set_schedule_paused("etl", "* * * * *", true, None)
                .unwrap()
        );
        assert!(store.job_states("etl").unwrap().is_empty());
        assert!(store.latest_materializations().unwrap().is_empty());
        assert!(store.sensors().unwrap().is_empty());
        assert!(store.sensor_ticks(None, 10).unwrap().is_empty());
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(store.events("r1", 0).unwrap().len(), 1);
    }

    #[test]
    fn v1_db_migrates_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.db");
        let path = path.to_str().unwrap();
        phase1_db(path, 1);
        assert_migrated(path);
    }

    #[test]
    fn unversioned_phase1_db_detected_as_v1() {
        // dbs written before the migration mechanism existed: v1 tables, user_version 0
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v0.db");
        let path = path.to_str().unwrap();
        phase1_db(path, 0);
        assert_migrated(path);
    }

    #[test]
    fn interrupted_first_boot_leaves_nothing_behind() {
        // a conflicting user table makes SCHEMA_V2 fail after SCHEMA_V1 has run
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fresh.db");
        let path = path.to_str().unwrap();
        {
            let conn = Connection::open(path).unwrap();
            conn.execute_batch("CREATE TABLE schedules (x)").unwrap();
        }
        assert!(Store::open(path).is_err());

        let conn = Connection::open(path).unwrap();
        assert!(!table_exists(&conn, "runs").unwrap());
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 0);
        let err = Store::open(path).err().unwrap();
        assert!(err.to_string().contains("schedules"), "{err}");
        conn.execute_batch("DROP TABLE schedules").unwrap();
        drop(conn);
        let store = Store::open(path).unwrap();
        assert!(store.schedules().unwrap().is_empty());
    }

    #[test]
    fn interrupted_v2_migration_rolls_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v1.db");
        let path = path.to_str().unwrap();
        phase1_db(path, 1);
        {
            let conn = Connection::open(path).unwrap();
            conn.execute_batch("CREATE TABLE schedules (x)").unwrap();
        }
        assert!(Store::open(path).is_err());

        let err = Store::open(path).err().unwrap();
        assert!(err.to_string().contains("schedules"), "{err}");
        let conn = Connection::open(path).unwrap();
        let kind_cols: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('events') WHERE name = 'kind'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kind_cols, 0);
        conn.execute_batch("DROP TABLE schedules").unwrap();
        drop(conn);
        assert_migrated(path);
    }

    #[test]
    fn newer_schema_refused_not_downgraded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("future.db");
        let path = path.to_str().unwrap();
        phase1_db(path, 19);
        let err = Store::open(path).err().unwrap();
        assert_eq!(err.to_string(), "db schema v19 is newer than this build");
        let conn = Connection::open(path).unwrap();
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 19);
    }

    #[test]
    fn v14_db_migrates_to_v15_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v14.db");
        let path = path.to_str().unwrap();
        // every batch up to v14, stamped 14: runs and events, and nowhere at
        // all for what an op printed
        let conn = Connection::open(path).unwrap();
        for batch in [
            PHASE1_SCHEMA,
            SCHEMA_V2,
            SCHEMA_V3,
            SCHEMA_V4,
            SCHEMA_V5,
            SCHEMA_V6,
            SCHEMA_V7,
            SCHEMA_V8,
            SCHEMA_V9,
            SCHEMA_V10,
            SCHEMA_V11,
            SCHEMA_V12,
            SCHEMA_V13,
            SCHEMA_V14,
        ] {
            conn.execute_batch(batch).unwrap();
        }
        conn.pragma_update(None, "user_version", 14).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Success);
        assert!(store.op_logs("r1", None, 0, 100).unwrap().is_empty());
        crate::logs::Budget::new().line(
            &store,
            &crate::logs::Attempt::new("r1", "a", 1),
            crate::logs::Source::Stream(crate::model::LogStream::Stdout),
            "after the migration",
        );
        drop(store);
        let store = Store::open(path).unwrap();
        let rows = store.op_logs("r1", None, 0, 100).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].message, "after the migration");
    }

    #[test]
    fn v15_db_migrates_to_v16_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v15.db");
        let path = path.to_str().unwrap();
        // every batch up to v15, stamped 15: a run log with nowhere to write
        // down an alert it owes
        let conn = Connection::open(path).unwrap();
        for batch in [
            PHASE1_SCHEMA,
            SCHEMA_V2,
            SCHEMA_V3,
            SCHEMA_V4,
            SCHEMA_V5,
            SCHEMA_V6,
            SCHEMA_V7,
            SCHEMA_V8,
            SCHEMA_V9,
            SCHEMA_V10,
            SCHEMA_V11,
            SCHEMA_V12,
            SCHEMA_V13,
            SCHEMA_V14,
            SCHEMA_V15,
        ] {
            conn.execute_batch(batch).unwrap();
        }
        conn.pragma_update(None, "user_version", 15).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Success);
        // an older file has no notifications and owes none: the table is
        // empty rather than backfilled with alerts nobody is waiting for
        assert!(store.notifications(None, 100).unwrap().is_empty());

        store
            .create_run(&mk_run("r2", "etl", Utc::now()), &["a".into()])
            .unwrap();
        store
            .run_finished(
                "r2",
                RunStatus::Failed,
                None,
                Utc::now(),
                Some(&json!({"run_id": "r2"})),
            )
            .unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        let rows = store.notifications(None, 100).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, DeliveryState::Pending);
        assert_eq!(rows[0].payload, json!({"run_id": "r2"}));
    }

    /// bring `db` to v16 with `events` rows in it, ready for v17 to move.
    ///
    /// the two backends get there differently and cannot not. sqlite walks the
    /// chain it has always walked. postgres is created whole at the current
    /// version, so the only honest way to make a v16 postgres database out of
    /// this build is to walk the one step *backwards* — which also means the
    /// fixture fails loudly if v17 ever stops being exactly these four
    /// statements. either way what the migration then meets is a populated
    /// events table with a NOT NULL `run_id`.
    fn at_v16(db: &Backend, events: usize) {
        let rows: String = (0..events)
            .map(|i| {
                format!(
                    "INSERT INTO events (run_id, op, level, kind, message, ts)
                     VALUES ('r1', 'a', 'info', 'log', 'line {i}',
                             '2026-01-01T00:00:00+00:00');"
                )
            })
            .collect();
        match db {
            Backend::Sqlite(dir) => {
                let conn = Connection::open(dir.path().join("hestan.db")).unwrap();
                for batch in [
                    PHASE1_SCHEMA,
                    SCHEMA_V2,
                    SCHEMA_V3,
                    SCHEMA_V4,
                    SCHEMA_V5,
                    SCHEMA_V6,
                    SCHEMA_V7,
                    SCHEMA_V8,
                    SCHEMA_V9,
                    SCHEMA_V10,
                    SCHEMA_V11,
                    SCHEMA_V12,
                    SCHEMA_V13,
                    SCHEMA_V14,
                    SCHEMA_V15,
                    SCHEMA_V16,
                    &rows,
                ] {
                    conn.execute_batch(batch).unwrap();
                }
                conn.pragma_update(None, "user_version", 16).unwrap();
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pg) => {
                let store = pg.store();
                store
                    .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                    .unwrap();
                store
                    .run_finished("r1", RunStatus::Success, None, Utc::now(), None)
                    .unwrap();
                store
                    .conn()
                    .batch(&format!(
                        "{rows}
                         DROP INDEX events_subject;
                         ALTER TABLE events DROP COLUMN subject_kind;
                         ALTER TABLE events DROP COLUMN subject;
                         ALTER TABLE events ALTER COLUMN run_id SET NOT NULL;
                         ALTER TABLE runs DROP COLUMN actor;
                         ALTER TABLE events DROP COLUMN actor;
                         UPDATE schema_version SET version = 16;"
                    ))
                    .unwrap();
            }
        }
    }

    #[test]
    fn a_populated_v16_db_migrates_to_v17_keeping_its_rows() {
        both(|db| {
            // two hundred events rather than one. sqlite has no `ALTER COLUMN`
            // and rebuilds the table to drop the NOT NULL, and a rebuild that
            // preserved one row is no evidence about one that has to preserve
            // the seq of every row a reader's cursor might be sitting on
            at_v16(db, 200);
            let store = db.store();

            let events = store.events("r1", 0).unwrap();
            assert!(events.len() >= 200, "rows were lost: {}", events.len());
            assert!(events.iter().all(|e| e.run_id.as_deref() == Some("r1")));
            assert!(events.iter().all(|e| e.subject_kind == SubjectKind::Run));
            // and the run event that migrated is still found by the run
            assert!(events.iter().all(|e| e.subject.is_none()));
            assert!(events.iter().all(|e| e.about() == Some("r1")));
            assert!(
                events.windows(2).all(|w| w[0].seq < w[1].seq),
                "seq stopped being the order it was"
            );
            assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Success);

            // the column the whole migration was for: an event about no run
            let seq_before = store.event_watermark().unwrap();
            store
                .record_materialization("sales", None, "fp", &json!({}), None, None, None)
                .unwrap();
            let log = store.event_log(&EventQuery::default(), 5).unwrap();
            assert_eq!(log[0].kind, EventKind::AssetMaterialized);
            assert_eq!(log[0].run_id, None);
            assert_eq!(log[0].about(), Some("sales"));
            assert!(log[0].seq > seq_before);

            // reopening does not migrate a second time
            drop(store);
            let store = db.store();
            assert_eq!(store.events("r1", 0).unwrap().len(), events.len());
        });
    }

    /// bring `db` to v17: the schema from before anything recorded who did it.
    ///
    /// the two backends get there differently, for the same reason
    /// [`at_v16`] gives: sqlite walks the chain forward, and postgres — which
    /// is created whole at the current version — walks the one step back. the
    /// backwards step failing loudly if v18 ever stops being exactly these two
    /// columns is the point of writing it out.
    fn at_v17(db: &Backend) {
        match db {
            Backend::Sqlite(dir) => {
                let conn = Connection::open(dir.path().join("hestan.db")).unwrap();
                for batch in [
                    PHASE1_SCHEMA,
                    SCHEMA_V2,
                    SCHEMA_V3,
                    SCHEMA_V4,
                    SCHEMA_V5,
                    SCHEMA_V6,
                    SCHEMA_V7,
                    SCHEMA_V8,
                    SCHEMA_V9,
                    SCHEMA_V10,
                    SCHEMA_V11,
                    SCHEMA_V12,
                    SCHEMA_V13,
                    SCHEMA_V14,
                    SCHEMA_V15,
                    SCHEMA_V16,
                    SCHEMA_V17,
                ] {
                    conn.execute_batch(batch).unwrap();
                }
                conn.pragma_update(None, "user_version", 17).unwrap();
            }
            #[cfg(feature = "postgres")]
            Backend::Postgres(pg) => {
                let store = pg.store();
                store
                    .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                    .unwrap();
                store
                    .run_finished("r1", RunStatus::Success, None, Utc::now(), None)
                    .unwrap();
                store
                    .conn()
                    .batch(
                        "ALTER TABLE runs DROP COLUMN actor;
                         ALTER TABLE events DROP COLUMN actor;
                         UPDATE schema_version SET version = 17;",
                    )
                    .unwrap();
            }
        }
    }

    #[test]
    fn a_populated_v17_db_migrates_to_v18_and_starts_recording_who() {
        both(|db| {
            at_v17(db);
            let store = db.store();

            // what was there is still there, attributed to nobody — which is
            // what every row written before this honestly says
            let run = store.run("r1").unwrap().unwrap();
            assert_eq!(run.status, RunStatus::Success);
            assert_eq!(run.actor, None);
            assert!(
                store
                    .events("r1", 0)
                    .unwrap()
                    .iter()
                    .all(|e| e.actor.is_none())
            );

            // and from here a run somebody asked for carries them, on the row
            // and on the event written in the same transaction as it
            let mut asked = mk_run("r2", "etl", Utc::now());
            asked.trigger = Trigger::Manual;
            asked.actor = Some("ada".into());
            store.create_run(&asked, &["a".into()]).unwrap();
            let stored = store.run("r2").unwrap().unwrap();
            assert_eq!(stored.actor.as_deref(), Some("ada"));
            let queued = &store.events("r2", 0).unwrap()[0];
            assert_eq!(queued.kind, EventKind::RunQueued);
            assert_eq!(queued.actor.as_deref(), Some("ada"));

            // reopening does not migrate a second time
            drop(store);
            let store = db.store();
            assert_eq!(
                store.run("r2").unwrap().unwrap().actor.as_deref(),
                Some("ada")
            );
        });
    }

    // a paused schedule is a decision that outlives whoever made it, and until
    // now the log said nothing about either half of that
    #[test]
    fn pausing_a_schedule_or_a_sensor_says_who_did_it() {
        both(|db| {
            let store = db.store();
            store
                .sync_schedules(&[Schedule::new("etl", "0 * * * *")])
                .unwrap();
            store.sync_sensors(&["watch".to_string()]).unwrap();

            assert!(
                store
                    .set_schedule_paused("etl", "0 * * * *", true, Some("ada"))
                    .unwrap()
            );
            let ev = newest(&store, EventKind::SchedulePaused);
            assert_eq!(ev.actor.as_deref(), Some("ada"));
            assert_eq!(ev.about(), Some("etl"));
            assert_eq!(ev.data.unwrap()["paused"], true);

            // unpausing is the same event saying the other thing, and an
            // unauthenticated deployment records nobody rather than "system"
            assert!(
                store
                    .set_schedule_paused("etl", "0 * * * *", false, None)
                    .unwrap()
            );
            let ev = newest(&store, EventKind::SchedulePaused);
            assert_eq!(ev.actor, None);
            assert_eq!(ev.data.unwrap()["paused"], false);
            assert!(!store.schedules().unwrap()[0].paused);

            assert!(store.set_sensor_paused("watch", true, Some("ola")).unwrap());
            let ev = newest(&store, EventKind::SensorPaused);
            assert_eq!(ev.actor.as_deref(), Some("ola"));
            assert_eq!(ev.about(), Some("watch"));

            // and a name nobody knows writes nothing at all
            assert!(!store.set_sensor_paused("ghost", true, Some("ada")).unwrap());
            assert!(
                !store
                    .set_schedule_paused("ghost", "0 * * * *", true, Some("ada"))
                    .unwrap()
            );
        });
    }

    #[test]
    fn v11_db_migrates_to_v12_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v11.db");
        let path = path.to_str().unwrap();
        // every batch up to v11, stamped 11: no presets table, no runs.tags
        let conn = Connection::open(path).unwrap();
        for batch in [
            PHASE1_SCHEMA,
            SCHEMA_V2,
            SCHEMA_V3,
            SCHEMA_V4,
            SCHEMA_V5,
            SCHEMA_V6,
            SCHEMA_V7,
            SCHEMA_V8,
            SCHEMA_V9,
            SCHEMA_V10,
            SCHEMA_V11,
        ] {
            conn.execute_batch(batch).unwrap();
        }
        conn.pragma_update(None, "user_version", 11).unwrap();
        drop(conn);

        // the run planted by the phase-1 batch reads back, and presets start empty
        let store = Store::open(path).unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Success);
        assert!(store.presets("etl").unwrap().is_empty());
        store
            .put_preset("etl", "nightly", &json!({"days": 7}))
            .unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(store.presets("etl").unwrap().len(), 1);
    }

    #[test]
    fn v12_db_migrates_to_v13_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v12.db");
        let path = path.to_str().unwrap();
        // every batch up to v12, stamped 12: op_runs has neither pid nor inputs
        let conn = Connection::open(path).unwrap();
        for batch in [
            PHASE1_SCHEMA,
            SCHEMA_V2,
            SCHEMA_V3,
            SCHEMA_V4,
            SCHEMA_V5,
            SCHEMA_V6,
            SCHEMA_V7,
            SCHEMA_V8,
            SCHEMA_V9,
            SCHEMA_V10,
            SCHEMA_V11,
            SCHEMA_V12,
        ] {
            conn.execute_batch(batch).unwrap();
        }
        conn.pragma_update(None, "user_version", 12).unwrap();
        drop(conn);

        // the op run written before the migration reads back, claiming no
        // process and no recorded inputs — which is what every op that runs in
        // this process says too
        let store = Store::open(path).unwrap();
        let row = store.op_run("r1", "a").unwrap().unwrap();
        assert_eq!(row.status, OpStatus::Success);
        assert_eq!(row.pid, None);
        assert_eq!(store.op_inputs("r1", "a").unwrap(), None);

        store.op_spawned("r1", "a", 4242).unwrap();
        // guarded on `running`: a finished op cannot be given a process
        assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, None);
        store.op_started("r1", "a", 2).unwrap();
        store.op_spawned("r1", "a", 4242).unwrap();
        assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, Some(4242));

        store
            .set_op_inputs(
                "r1",
                "a",
                &json!({"held": {"up": 1}, "deps": {"up": "success"}}),
            )
            .unwrap();
        assert_eq!(
            store.op_inputs("r1", "a").unwrap().unwrap()["deps"]["up"],
            json!("success")
        );
        // and the terminal write hands the process back
        store
            .op_finished("r1", "a", OpStatus::Success, None, None, None, &[])
            .unwrap();
        assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, None);
    }

    #[test]
    fn run_tags_round_trip_and_the_filter_matches_exactly() {
        both(|db| {
            let store = db.store();
            let tagged = |id: &str, tags: RunTags| {
                let mut run = mk_run(id, "etl", Utc::now());
                run.tags = tags;
                store.create_run(&run, &[]).unwrap();
            };
            tagged(
                "r1",
                RunTags::from([
                    ("kind".to_string(), "backfill".to_string()),
                    ("env".to_string(), "prod".to_string()),
                ]),
            );
            tagged("r2", RunTags::from([("kind".to_string(), "smoke".into())]));
            tagged("r3", RunTags::new());

            let read = store.run("r1").unwrap().unwrap().tags;
            assert_eq!(read["kind"], "backfill");
            assert_eq!(read["env"], "prod");
            // an untagged run reads as no tags, not as a null anything
            assert!(store.run("r3").unwrap().unwrap().tags.is_empty());

            let ids = |tag: Option<(&str, &str)>| -> Vec<String> {
                store
                    .runs(None, None, None, None, tag, 10)
                    .unwrap()
                    .into_iter()
                    .map(|r| r.id)
                    .collect()
            };
            assert_eq!(ids(Some(("kind", "backfill"))), ["r1"]);
            assert_eq!(ids(Some(("env", "prod"))), ["r1"]);
            // exactly: neither a different value nor a prefix of one matches, and
            // an unknown key matches nothing rather than everything
            assert!(ids(Some(("kind", "back"))).is_empty());
            assert!(ids(Some(("kind", "backfills"))).is_empty());
            assert!(ids(Some(("kind", "prod"))).is_empty());
            assert!(ids(Some(("ghost", "backfill"))).is_empty());
            // and no filter is still every run, tagged or not
            assert_eq!(ids(None).len(), 3);

            // the filter composes with the others rather than replacing them
            let run = mk_run("r4", "other", Utc::now());
            store.create_run(&run, &[]).unwrap();
            assert_eq!(
                store
                    .runs(Some("etl"), None, None, None, Some(("kind", "smoke")), 10)
                    .unwrap()
                    .len(),
                1
            );
        });
    }

    #[test]
    fn presets_are_stored_upserted_and_deleted_per_job() {
        both(|db| {
            let store = db.store();
            assert!(store.presets("etl").unwrap().is_empty());
            assert!(store.preset("etl", "nightly").unwrap().is_none());

            store
                .put_preset("etl", "nightly", &json!({"days": 7}))
                .unwrap();
            store
                .put_preset("etl", "backfill", &json!({"days": 90}))
                .unwrap();
            // another job's preset of the same name is a different preset
            store
                .put_preset("other", "nightly", &json!({"days": 1}))
                .unwrap();

            let all = store.presets("etl").unwrap();
            assert_eq!(
                all.iter().map(|p| p.name.as_str()).collect::<Vec<_>>(),
                ["backfill", "nightly"],
                "presets come back sorted by name"
            );
            assert_eq!(all[1].params, json!({"days": 7}));
            assert_eq!(
                store.preset("other", "nightly").unwrap().unwrap().params,
                json!({"days": 1})
            );

            // a rewrite replaces the params and keeps the age
            let first = store.preset("etl", "nightly").unwrap().unwrap().created_at;
            store
                .put_preset("etl", "nightly", &json!({"days": 14}))
                .unwrap();
            let again = store.preset("etl", "nightly").unwrap().unwrap();
            assert_eq!(again.params, json!({"days": 14}));
            assert_eq!(again.created_at, first);
            assert_eq!(store.presets("etl").unwrap().len(), 2);

            assert!(store.delete_preset("etl", "nightly").unwrap());
            assert!(!store.delete_preset("etl", "nightly").unwrap());
            assert!(store.preset("etl", "nightly").unwrap().is_none());
            assert!(store.preset("other", "nightly").unwrap().is_some());
        });
    }

    #[test]
    fn v10_db_migrates_to_v11_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v10.db");
        let path = path.to_str().unwrap();
        // every batch up to v10, stamped 10: no run keys and no tick metrics
        let conn = Connection::open(path).unwrap();
        for batch in [
            PHASE1_SCHEMA,
            SCHEMA_V2,
            SCHEMA_V3,
            SCHEMA_V4,
            SCHEMA_V5,
            SCHEMA_V6,
            SCHEMA_V7,
            SCHEMA_V8,
            SCHEMA_V9,
            SCHEMA_V10,
        ] {
            conn.execute_batch(batch).unwrap();
        }
        conn.execute(
            "INSERT INTO sensor_ticks (sensor, evaluated_at, outcome, launched)
             VALUES ('watch', '2026-01-01T00:00:00+00:00', 'fired', 2)",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 10).unwrap();
        drop(conn);

        // an existing tick keeps its counts and reads zero for the new ones
        let store = Store::open(path).unwrap();
        let ticks = store.sensor_ticks(Some("watch"), 10).unwrap();
        assert_eq!(ticks[0].launched, 2);
        assert_eq!(ticks[0].skipped, 0);
        assert!(!store.run_key_claimed("watch", "2026-01-01").unwrap());
    }

    #[test]
    fn a_run_key_is_claimed_with_its_run_or_not_at_all() {
        both(|db| {
            let store = db.store();
            let key = RunKey {
                sensor: "watch",
                key: "2026-08-09",
            };
            assert!(
                store
                    .create_run_keyed(
                        &mk_run("r1", "etl", Utc::now()),
                        &["a".into()],
                        Some(key),
                        None
                    )
                    .unwrap()
            );
            assert!(store.run_key_claimed("watch", "2026-08-09").unwrap());
            assert_eq!(store.op_runs("r1").unwrap().len(), 1);

            // the same key again writes nothing at all — no run, no op rows, no
            // second key — because the whole thing is one transaction
            assert!(
                !store
                    .create_run_keyed(
                        &mk_run("r2", "etl", Utc::now()),
                        &["a".into()],
                        Some(key),
                        None
                    )
                    .unwrap()
            );
            assert!(store.run("r2").unwrap().is_none());
            assert!(store.op_runs("r2").unwrap().is_empty());
            // and the key means nothing to another sensor
            assert!(!store.run_key_claimed("other", "2026-08-09").unwrap());

            let day = chrono::Duration::days(1);
            assert_eq!(store.prune_sensor_run_keys(Utc::now() - day).unwrap(), 0);
            assert_eq!(store.prune_sensor_run_keys(Utc::now() + day).unwrap(), 1);
            assert!(
                !store.run_key_claimed("watch", "2026-08-09").unwrap(),
                "retention left the key behind"
            );
            // pruning a key does not touch the run it launched
            assert!(store.run("r1").unwrap().is_some());
        });
    }

    #[test]
    fn v2_db_migrates_to_v3_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v2.db");
        let path = path.to_str().unwrap();
        // the phase-1 schema plus the v2 batch, stamped 2
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute(
            "INSERT INTO schedules (job, expr) VALUES ('etl', '0 * * * *')",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 2).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Success);
        assert_eq!(store.schedules().unwrap().len(), 1);
        assert!(store.job_states("etl").unwrap().is_empty());
        store
            .set_op_state("etl", "a", &json!({"cursor": 7}))
            .unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(
            store.op_state("etl", "a").unwrap(),
            Some(json!({"cursor": 7}))
        );
    }

    #[test]
    fn v3_db_migrates_to_v4_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v3.db");
        let path = path.to_str().unwrap();
        // the phase-1 schema plus the v2 and v3 batches, with rows in every
        // generation of table, stamped 3
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute(
            "INSERT INTO schedules (job, expr) VALUES ('etl', '0 * * * *')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO op_state (job, op, value, updated_at)
             VALUES ('etl', 'a', '7', '2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 3).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        assert_eq!(store.run("r1").unwrap().unwrap().status, RunStatus::Success);
        assert_eq!(store.schedules().unwrap().len(), 1);
        assert_eq!(store.op_state("etl", "a").unwrap(), Some(json!(7)));
        assert!(store.latest_materializations().unwrap().is_empty());
        store
            .record_materialization("docs", None, "abc", &json!({}), None, None, None)
            .unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(
            store
                .materialization("docs", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "abc"
        );
        assert_eq!(store.sensors().unwrap().len(), 1);
    }

    #[test]
    fn v4_db_migrates_to_v5_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v4.db");
        let path = path.to_str().unwrap();
        // every batch up to v4, stamped 4: the runs table has no resumed_from yet
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute_batch(SCHEMA_V4).unwrap();
        conn.pragma_update(None, "user_version", 4).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        let old = store.run("r1").unwrap().unwrap();
        assert_eq!(old.status, RunStatus::Success);
        assert_eq!(old.resumed_from, None);

        let mut resumed = mk_run("r2", "etl", Utc::now());
        resumed.resumed_from = Some("r1".into());
        store.create_run(&resumed, &["a".into()]).unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(
            store.run("r2").unwrap().unwrap().resumed_from,
            Some("r1".to_string())
        );
        assert_eq!(
            store.runs(None, None, None, None, None, 10).unwrap()[0].resumed_from,
            Some("r1".to_string())
        );
    }

    #[test]
    fn v5_db_migrates_to_v6_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v5.db");
        let path = path.to_str().unwrap();
        // every batch up to v5, stamped 5: the runs table has no error yet
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute_batch(SCHEMA_V4).unwrap();
        conn.execute_batch(SCHEMA_V5).unwrap();
        conn.pragma_update(None, "user_version", 5).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        // a run recorded before the column existed keeps its row, without one
        let old = store.run("r1").unwrap().unwrap();
        assert_eq!(old.status, RunStatus::Success);
        assert_eq!(old.error, None);

        store
            .create_run(&mk_run("r2", "etl", Utc::now()), &["a".into()])
            .unwrap();
        store
            .run_finished(
                "r2",
                RunStatus::Failed,
                Some("op a failed: boom"),
                Utc::now(),
                None,
            )
            .unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(
            store.run("r2").unwrap().unwrap().error.as_deref(),
            Some("op a failed: boom")
        );
        assert_eq!(
            store.runs(None, None, None, None, None, 10).unwrap()[0]
                .error
                .as_deref(),
            Some("op a failed: boom")
        );
    }

    #[test]
    fn v6_db_migrates_to_v7_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v6.db");
        let path = path.to_str().unwrap();
        // every batch up to v6, stamped 6: schedules has no params column yet
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute_batch(SCHEMA_V4).unwrap();
        conn.execute_batch(SCHEMA_V5).unwrap();
        conn.execute_batch(SCHEMA_V6).unwrap();
        conn.execute(
            "INSERT INTO schedules (job, expr, paused) VALUES ('etl', '0 * * * *', 1)",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 6).unwrap();
        drop(conn);

        // a schedule declared before params existed reads back as `{}`, paused
        let store = Store::open(path).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].paused);
        assert_eq!(rows[0].params, json!({}));

        store
            .sync_schedules(&[Schedule::new("etl", "0 * * * *").params(json!({"region": "eu"}))])
            .unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows[0].params, json!({"region": "eu"}));
        assert!(rows[0].paused, "sync dropped the paused flag");
    }

    #[test]
    fn v7_db_migrates_to_v8_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v7.db");
        let path = path.to_str().unwrap();
        // every batch up to v7, stamped 7: asset_materializations is still
        // keyed by asset, op_runs has no metadata, asset_checks does not exist
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        conn.execute_batch(SCHEMA_V2).unwrap();
        conn.execute_batch(SCHEMA_V3).unwrap();
        conn.execute_batch(SCHEMA_V4).unwrap();
        conn.execute_batch(SCHEMA_V5).unwrap();
        conn.execute_batch(SCHEMA_V6).unwrap();
        conn.execute_batch(SCHEMA_V7).unwrap();
        conn.execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, value, run_id, built_at)
             VALUES ('stats', 'f1', '{\"docs\":\"d1\"}', '{\"files\":12}', 'r1', '2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, built_at)
             VALUES ('docs', 'd1', '{}', '2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 7).unwrap();
        drop(conn);

        let store = Store::open(path).unwrap();
        // the row that was current state is now the first entry of a history
        let m = store.materialization("stats", None).unwrap().unwrap();
        assert_eq!(m.fingerprint, "f1");
        assert_eq!(m.inputs, json!({"docs": "d1"}));
        assert_eq!(m.value, Some(json!({"files": 12})));
        assert_eq!(m.run_id.as_deref(), Some("r1"));
        assert_eq!(store.latest_materializations().unwrap().len(), 2);
        let carried = store.materializations("stats", None, 10).unwrap();
        assert_eq!(carried.len(), 1);
        assert!(
            carried[0].changed,
            "a carried row is that asset's first change"
        );

        // and the same asset now appends rather than replacing
        record(&store, "stats", "f2");
        assert_eq!(
            store
                .materialization("stats", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "f2"
        );
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(store.materializations("stats", None, 10).unwrap().len(), 2);
        drop(store);

        // the rest of v8 is columns and a table later parts of this phase
        // fill: they exist from this one migration on
        let conn = Connection::open(path).unwrap();
        assert!(table_exists(&conn, "asset_checks").unwrap());
        let metadata_cols: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('op_runs') WHERE name = 'metadata'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(metadata_cols, 1);
    }

    #[test]
    fn v8_db_migrates_to_v9_keeping_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v8.db");
        let path = path.to_str().unwrap();
        // every batch up to v8, stamped 8: no partition column anywhere and
        // no backfills table
        let conn = Connection::open(path).unwrap();
        conn.execute_batch(PHASE1_SCHEMA).unwrap();
        for batch in [
            SCHEMA_V2, SCHEMA_V3, SCHEMA_V4, SCHEMA_V5, SCHEMA_V6, SCHEMA_V7, SCHEMA_V8,
        ] {
            conn.execute_batch(batch).unwrap();
        }
        conn.execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, value, built_at)
             VALUES ('stats', 'f1', '{}', '{\"files\":12}', '2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO asset_checks
                 (asset, check_name, run_id, status, severity, checked_at)
             VALUES ('stats', 'has_files', 'r1', 'passed', 'error',
                     '2026-01-01T00:00:00+00:00')",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "user_version", 8).unwrap();
        drop(conn);

        // an existing row is an unpartitioned one and reads exactly as before
        let store = Store::open(path).unwrap();
        let m = store.materialization("stats", None).unwrap().unwrap();
        assert_eq!(m.partition, None);
        assert_eq!(m.value, Some(json!({"files": 12})));
        let checks = store.asset_checks("stats", None, 10).unwrap();
        assert_eq!(checks.len(), 1);
        assert_eq!(checks[0].partition, None);

        // and a key of the same asset is now a history of its own
        store
            .record_materialization(
                "stats",
                Some("2026-01-02"),
                "p1",
                &json!({}),
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(
            store
                .materialization("stats", None)
                .unwrap()
                .unwrap()
                .fingerprint,
            "f1",
            "a partitioned row displaced the unpartitioned one"
        );
        assert_eq!(
            store
                .materialization("stats", Some("2026-01-02"))
                .unwrap()
                .unwrap()
                .fingerprint,
            "p1"
        );
        assert_eq!(store.latest_materializations().unwrap().len(), 2);

        // the backfills table lands with this migration, for part three
        let conn = Connection::open(path).unwrap();
        assert!(table_exists(&conn, "backfills").unwrap());
    }

    #[test]
    fn run_error_survives_a_later_status_write() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                .unwrap();
            store
                .run_finished(
                    "r1",
                    RunStatus::Failed,
                    Some("op a failed: boom"),
                    Utc::now(),
                    None,
                )
                .unwrap();
            // None must not blank an error a caller already recorded
            store
                .run_finished("r1", RunStatus::Failed, None, Utc::now(), None)
                .unwrap();
            assert_eq!(
                store.run("r1").unwrap().unwrap().error.as_deref(),
                Some("op a failed: boom")
            );
        });
    }

    #[test]
    fn unstopped_op_keeps_no_finish_time() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                .unwrap();
            store.op_started("r1", "a", 1).unwrap();
            store
                .op_unstopped("r1", "a", "not observed to stop")
                .unwrap();

            let op = &store.op_runs("r1").unwrap()[0];
            assert_eq!(op.status, OpStatus::Canceled);
            assert_eq!(op.error.as_deref(), Some("not observed to stop"));
            assert!(op.started_at.is_some());
            assert_eq!(
                op.finished_at, None,
                "claimed a finish time for work it never saw finish"
            );
        });
    }

    #[test]
    fn materialization_records_and_latest_wins() {
        both(|db| {
            let store = db.store();
            assert!(store.materialization("stats", None).unwrap().is_none());

            store
                .record_materialization(
                    "stats",
                    None,
                    "f1",
                    &json!({"docs": "d1"}),
                    Some(&json!({"files": 12})),
                    Some("r1"),
                    Some(&json!({"files": {"int": 12}})),
                )
                .unwrap();
            store
                .record_materialization("docs", None, "d1", &json!({}), None, None, None)
                .unwrap();

            let m = store.materialization("stats", None).unwrap().unwrap();
            assert_eq!(m.fingerprint, "f1");
            assert_eq!(m.inputs, json!({"docs": "d1"}));
            assert_eq!(m.value, Some(json!({"files": 12})));
            assert_eq!(m.run_id.as_deref(), Some("r1"));
            let first_built = m.built_at;

            let d = store.materialization("docs", None).unwrap().unwrap();
            assert_eq!(d.value, None);
            assert_eq!(d.run_id, None);

            store
                .record_materialization(
                    "stats",
                    None,
                    "f2",
                    &json!({"docs": "d2"}),
                    Some(&json!({"files": 13})),
                    Some("r2"),
                    None,
                )
                .unwrap();
            let all = store.latest_materializations().unwrap();
            assert_eq!(all.len(), 2);
            let m = store.materialization("stats", None).unwrap().unwrap();
            assert_eq!(m.fingerprint, "f2");
            assert_eq!(m.run_id.as_deref(), Some("r2"));
            assert!(m.built_at >= first_built);
            // the first entry survives the second: this is history, not a slot
            assert_eq!(store.materializations("stats", None, 10).unwrap().len(), 2);
        });
    }

    // an asset build is written by the transaction that records the op, so
    // that write is the one both backends have to be run through
    #[test]
    fn an_ops_terminal_write_carries_what_it_built() {
        both(|db| {
            let store = db.store();
            let run = mk_run("r1", "assets", Utc::now());
            store.create_run(&run, &["split".into()]).unwrap();
            store.op_started("r1", "split", 1).unwrap();
            let built = |asset: &str, rows: i64| Built {
                asset: asset.to_string(),
                partition: Some("2024-01-01".to_string()),
                fingerprint: format!("{asset}-fp"),
                inputs: json!({ "source": "s-fp" }),
                value: Some(json!({ "rows": rows })),
                meta: Some(json!({ "rows": { "int": rows } })),
            };
            store
                .op_finished(
                    "r1",
                    "split",
                    OpStatus::Success,
                    Some(&json!("out")),
                    None,
                    None,
                    &[built("clean", 2), built("rejected", 1)],
                )
                .unwrap();

            let op = store.op_run("r1", "split").unwrap().unwrap();
            assert_eq!(op.status, OpStatus::Success);
            for (asset, rows) in [("clean", 2), ("rejected", 1)] {
                let m = store
                    .materialization(asset, Some("2024-01-01"))
                    .unwrap()
                    .unwrap();
                assert_eq!(m.fingerprint, format!("{asset}-fp"));
                assert_eq!(m.inputs, json!({ "source": "s-fp" }));
                assert_eq!(m.value, Some(json!({ "rows": rows })));
                assert_eq!(m.run_id.as_deref(), Some("r1"));
                assert_eq!(m.metadata, Some(json!({ "rows": { "int": rows } })));
            }
            // the rows, and the events that announce them, in the one write
            let q = EventQuery {
                kind: Some(EventKind::AssetMaterialized),
                ..EventQuery::default()
            };
            let mut said: Vec<String> = store
                .event_log(&q, 10)
                .unwrap()
                .into_iter()
                .filter_map(|e| e.subject)
                .collect();
            said.sort();
            assert_eq!(said, ["clean", "rejected"]);
        });
    }

    fn record(store: &Store, asset: &str, fp: &str) {
        store
            .record_materialization(asset, None, fp, &json!({}), None, None, None)
            .unwrap();
    }

    #[test]
    fn history_flags_only_real_fingerprint_transitions() {
        both(|db| {
            let store = db.store();
            assert!(
                store
                    .materializations("stats", None, 10)
                    .unwrap()
                    .is_empty()
            );

            // built four times, moved twice
            for fp in ["f1", "f1", "f2", "f2"] {
                record(&store, "stats", fp);
            }
            record(&store, "other", "x1");

            let history = store.materializations("stats", None, 10).unwrap();
            let seen: Vec<(&str, bool)> = history
                .iter()
                .map(|e| (e.mat.fingerprint.as_str(), e.changed))
                .collect();
            // newest first; the oldest entry counts as a change from nothing
            assert_eq!(
                seen,
                [("f2", false), ("f2", true), ("f1", false), ("f1", true)]
            );
            assert!(history.windows(2).all(|w| w[0].mat.id > w[1].mat.id));

            // a page's oldest entry is compared with the entry just off it, not
            // reported as a change because the window cut its predecessor away
            let page = store.materializations("stats", None, 3).unwrap();
            assert_eq!(page.len(), 3);
            assert!(!page[2].changed, "the page edge invented a change");
        });
    }

    #[test]
    fn history_carries_what_the_build_before_it_reported() {
        both(|db| {
            let store = db.store();
            let meta = |rows: i64| json!({ "rows": { "count": rows } });
            for (key, rows) in [(None, 10), (Some("k"), 400), (None, 14), (None, 21)] {
                store
                    .record_materialization(
                        "stats",
                        key,
                        "f",
                        &json!({}),
                        None,
                        None,
                        Some(&meta(rows)),
                    )
                    .unwrap();
            }

            let history = store.materializations("stats", None, 10).unwrap();
            let seen: Vec<(Value, Option<Value>)> = history
                .iter()
                .map(|e| (e.mat.metadata.clone().unwrap(), e.previous_metadata.clone()))
                .collect();
            // each entry against the build before it *of its own partition*: the
            // 400 belongs to key k and is nobody else's predecessor
            assert_eq!(
                seen,
                [
                    (meta(21), Some(meta(14))),
                    (meta(14), Some(meta(10))),
                    (meta(400), None),
                    (meta(10), None),
                ]
            );

            // and a page's oldest entry still sees the entry just off it
            let page = store.materializations("stats", None, 1).unwrap();
            assert_eq!(page[0].previous_metadata, Some(meta(14)));
        });
    }

    #[test]
    fn the_previous_metadata_of_an_op_skips_the_runs_that_reported_none() {
        both(|db| {
            let store = db.store();
            let at = |n: i64| Utc.timestamp_opt(1_700_000_000 + n, 0).unwrap();
            let meta = |rows: i64| json!({ "rows": { "int": rows } });
            for (i, reported) in [Some(3), Some(5), None, None].into_iter().enumerate() {
                let id = format!("r{i}");
                let run = mk_run(&id, "etl", at(i as i64));
                store
                    .create_run(&run, &["load".into(), "quiet".into()])
                    .unwrap();
                store
                    .op_finished(
                        &id,
                        "load",
                        OpStatus::Success,
                        None,
                        reported.map(meta).as_ref(),
                        None,
                        &[],
                    )
                    .unwrap();
            }
            // another job's op of the same name says nothing about this one
            let other = mk_run("x", "elsewhere", at(9));
            store.create_run(&other, &["load".into()]).unwrap();
            store
                .op_finished(
                    "x",
                    "load",
                    OpStatus::Success,
                    None,
                    Some(&meta(999)),
                    None,
                    &[],
                )
                .unwrap();

            let now = mk_run("r9", "etl", at(9));
            store.create_run(&now, &["load".into()]).unwrap();
            let previous = store
                .previous_op_metadata("etl", now.created_at, &now.id)
                .unwrap();
            // the last two runs recorded nothing, which is not the same as
            // recording that there was nothing to say
            assert_eq!(previous.get("load"), Some(&meta(5)));
            // an op that has never reported anything has no entry at all
            assert_eq!(previous.get("quiet"), None);

            // strictly before, by (created_at, id): a run does not compare
            // against itself, and the first run of all has nothing behind it
            let r0 = store.run("r0").unwrap().unwrap();
            assert!(
                store
                    .previous_op_metadata("etl", r0.created_at, &r0.id)
                    .unwrap()
                    .is_empty()
            );
            let r1 = store.run("r1").unwrap().unwrap();
            assert_eq!(
                store
                    .previous_op_metadata("etl", r1.created_at, &r1.id)
                    .unwrap()
                    .get("load"),
                Some(&meta(3))
            );
        });
    }

    #[test]
    fn history_prunes_to_the_cap_and_never_drops_the_latest() {
        both(|db| {
            let store = db.store();
            for i in 0..5 {
                record(&store, "stats", &format!("f{i}"));
            }
            for i in 0..3 {
                record(&store, "docs", &format!("d{i}"));
            }

            assert_eq!(store.prune_materializations(2).unwrap(), 4);
            let stats: Vec<String> = store
                .materializations("stats", None, 10)
                .unwrap()
                .into_iter()
                .map(|e| e.mat.fingerprint)
                .collect();
            assert_eq!(stats, ["f4", "f3"]);
            assert_eq!(store.materializations("docs", None, 10).unwrap().len(), 2);
            assert_eq!(
                store
                    .materialization("docs", None)
                    .unwrap()
                    .unwrap()
                    .fingerprint,
                "d2"
            );

            // a cap of zero still leaves current state standing
            assert_eq!(store.prune_materializations(0).unwrap(), 2);
            assert_eq!(store.materializations("stats", None, 10).unwrap().len(), 1);
            assert_eq!(
                store
                    .materialization("stats", None)
                    .unwrap()
                    .unwrap()
                    .fingerprint,
                "f4"
            );
            assert_eq!(store.latest_materializations().unwrap().len(), 2);
            assert_eq!(store.prune_materializations(1).unwrap(), 0);
        });
    }

    #[test]
    fn sensor_sync_preserves_paused_and_cursor() {
        both(|db| {
            let store = db.store();
            store
                .sync_sensors(&["watch".into(), "probe:docs".into()])
                .unwrap();
            let rows = store.sensors().unwrap();
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|r| !r.paused && r.cursor.is_none()));

            assert!(store.set_sensor_paused("watch", true, None).unwrap());
            assert!(!store.set_sensor_paused("nope", true, None).unwrap());
            store
                .set_sensor_cursor("watch", &json!({"mtime": 42}))
                .unwrap();

            store
                .sync_sensors(&["watch".into(), "fresh".into()])
                .unwrap();
            let rows = store.sensors().unwrap();
            assert_eq!(rows.len(), 2);
            let watch = rows.iter().find(|r| r.name == "watch").unwrap();
            assert!(watch.paused);
            assert_eq!(watch.cursor, Some(json!({"mtime": 42})));
            let fresh = rows.iter().find(|r| r.name == "fresh").unwrap();
            assert!(!fresh.paused && fresh.cursor.is_none());
            assert!(!rows.iter().any(|r| r.name == "probe:docs"));
        });
    }

    #[test]
    fn sensor_ticks_record_filter_and_prune() {
        both(|db| {
            let store = db.store();
            store
                .record_sensor_tick("watch", SensorOutcome::Fired, 2, 1, 12, &[], None)
                .unwrap();
            store
                .record_sensor_tick("watch", SensorOutcome::Error, 0, 0, 4, &[], Some("boom"))
                .unwrap();
            store
                .record_sensor_tick("probe:docs", SensorOutcome::Fired, 0, 0, 0, &[], None)
                .unwrap();

            let all = store.sensor_ticks(None, 10).unwrap();
            assert_eq!(all.len(), 3);
            assert_eq!(all[0].sensor, "probe:docs");

            let watch = store.sensor_ticks(Some("watch"), 10).unwrap();
            assert_eq!(watch.len(), 2);
            assert_eq!(watch[0].outcome, SensorOutcome::Error);
            assert_eq!(watch[0].error.as_deref(), Some("boom"));
            assert_eq!(watch[1].outcome, SensorOutcome::Fired);
            assert_eq!(watch[1].launched, 2);
            assert_eq!((watch[1].skipped, watch[1].duration_ms), (1, 12));
            assert_eq!(store.sensor_ticks(None, 1).unwrap().len(), 1);

            store.prune_sensor_ticks(1).unwrap();
            let left = store.sensor_ticks(None, 10).unwrap();
            assert_eq!(left.len(), 1);
            assert_eq!(left[0].sensor, "probe:docs");
        });
    }

    #[test]
    fn op_state_roundtrip_and_upsert() {
        both(|db| {
            let store = db.store();
            assert_eq!(store.op_state("etl", "pull").unwrap(), None);
            assert!(store.job_states("etl").unwrap().is_empty());

            store
                .set_op_state("etl", "pull", &json!({"cursor": 1}))
                .unwrap();
            store.set_op_state("etl", "clean", &json!(42)).unwrap();
            store.set_op_state("health", "pull", &json!("x")).unwrap();
            assert_eq!(
                store.op_state("etl", "pull").unwrap(),
                Some(json!({"cursor": 1}))
            );
            assert_eq!(store.op_state("etl", "nope").unwrap(), None);

            let states = store.job_states("etl").unwrap();
            assert_eq!(states.len(), 2);
            assert_eq!(states[0].0, "clean");
            assert_eq!(states[1].0, "pull");
            let first_update = states[1].2;

            store
                .set_op_state("etl", "pull", &json!({"cursor": 2}))
                .unwrap();
            assert_eq!(
                store.op_state("etl", "pull").unwrap(),
                Some(json!({"cursor": 2}))
            );
            let states = store.job_states("etl").unwrap();
            assert_eq!(states.len(), 2);
            assert!(states[1].2 >= first_update);
        });
    }

    #[test]
    fn schedule_sync_and_pause_roundtrip() {
        both(|db| {
            let store = db.store();
            let defined = vec![
                Schedule::new("etl", "0 * * * *").params(json!({"full": true})),
                Schedule::new("health", "*/5 * * * *").catchup(crate::model::Catchup::One),
            ];
            store.sync_schedules(&defined).unwrap();
            let rows = store.schedules().unwrap();
            assert_eq!(rows.len(), 2);
            assert!(rows.iter().all(|r| !r.paused));
            assert_eq!(rows[0].params, json!({"full": true}));
            assert_eq!(rows[1].params, json!({}));
            assert_eq!(rows[0].catchup, crate::model::Catchup::Skip);
            assert_eq!(rows[1].catchup, crate::model::Catchup::One);
            assert!(rows.iter().all(|r| r.cursor.is_none()));

            assert!(
                store
                    .set_schedule_paused("etl", "0 * * * *", true, None)
                    .unwrap()
            );
            assert!(
                !store
                    .set_schedule_paused("etl", "bogus", true, None)
                    .unwrap()
            );

            // tz and params follow the declaration; the paused flag stays put
            let cursor = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            store
                .set_schedule_cursor("etl", "0 * * * *", cursor)
                .unwrap();
            let defined = vec![
                Schedule::new("etl", "0 * * * *")
                    .tz("Europe/London")
                    .params(json!({"full": false}))
                    .catchup(crate::model::Catchup::All { limit: 6 }),
            ];
            store.sync_schedules(&defined).unwrap();
            let rows = store.schedules().unwrap();
            assert_eq!(rows.len(), 1);
            assert!(rows[0].paused);
            assert_eq!(rows[0].tz, "Europe/London");
            assert_eq!(rows[0].params, json!({"full": false}));
            assert_eq!(rows[0].catchup, crate::model::Catchup::All { limit: 6 });
            // the cursor is the scheduler's, not the declaration's: a sync that
            // reset it would be a restart that forgot the downtime it must detect
            assert_eq!(rows[0].cursor, Some(cursor));
        });
    }

    #[test]
    fn ticks_record_and_query() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            store
                .record_tick(
                    "etl",
                    "0 * * * *",
                    t0,
                    TickOutcome::Fired,
                    false,
                    Some("r1"),
                    None,
                )
                .unwrap();
            store
                .record_tick(
                    "etl",
                    "0 * * * *",
                    t0 + chrono::Duration::hours(1),
                    TickOutcome::Error,
                    false,
                    None,
                    Some("boom"),
                )
                .unwrap();
            store
                .record_tick(
                    "health",
                    "*/5 * * * *",
                    t0,
                    TickOutcome::Fired,
                    false,
                    Some("r2"),
                    None,
                )
                .unwrap();

            let all = store.ticks(None, 10).unwrap();
            assert_eq!(all.len(), 3);
            assert_eq!(all[0].job, "health");

            let etl = store.ticks(Some("etl"), 10).unwrap();
            assert_eq!(etl.len(), 2);
            assert_eq!(etl[0].outcome, TickOutcome::Error);
            assert_eq!(etl[0].error.as_deref(), Some("boom"));
            assert_eq!(etl[0].run_id, None);
            assert_eq!(etl[1].outcome, TickOutcome::Fired);
            assert_eq!(etl[1].run_id.as_deref(), Some("r1"));
            assert_eq!(etl[1].scheduled_for, t0);

            assert_eq!(store.ticks(None, 1).unwrap().len(), 1);
        });
    }

    // ------------------------------------------------------------------------
    // the cases below exist because of the second backend. what they cover was
    // covered before — by the executor's tests, the backfill loop's, the asset
    // registry's — and all of those run on sqlite and only sqlite. a query
    // nothing exercises on postgres is a query nobody has run on postgres, so
    // each of these puts one family of statements through the shared suite.

    #[test]
    fn the_queue_is_taken_in_priority_order_and_a_claim_takes_a_run_off_it() {
        both(|db| {
            let store = db.store();
            let defined = HashSet::from(["etl".to_string()]);
            let lease = Duration::from_secs(30);
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for (i, job) in [(0, "etl"), (1, "etl"), (2, "other")] {
                let at = t0 + chrono::Duration::minutes(i);
                let run = mk_run(&format!("r{i}"), job, at);
                store.create_run(&run, &["a".into()]).unwrap();
            }
            assert_eq!(store.queue_depth().unwrap(), 3);

            // a job this process does not define is blocked where it stands
            // rather than holding up everything behind it
            let queue = store.queue(&Limits::new(), &defined, 10).unwrap();
            let order: Vec<&str> = queue.iter().map(|q| q.run.id.as_str()).collect();
            assert_eq!(order, ["r0", "r1", "r2"]);
            assert_eq!(queue[0].position, 1);
            assert_eq!(queue[2].blocked, Some(Blocked::Undefined("other".into())));

            assert!(store.set_run_priority("r1", 5).unwrap());
            assert!(!store.set_run_priority("nobody", 5).unwrap());
            assert_eq!(
                store.queue(&Limits::new(), &defined, 10).unwrap()[0].run.id,
                "r1"
            );
            // what the doctor reads: this database says which backend it is
            // and what version it is at, on either of them
            assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
            assert!(["sqlite", "postgres"].contains(&store.backend()));

            // the same order to a reader that owns no limits and so says
            // nothing about what is holding anything back
            let rows = store.queue_rows(10).unwrap();
            let order: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
            assert_eq!(order, ["r1", "r0", "r2"]);

            let (claimed, plan) = store
                .claim_next("alpha", lease, &Limits::new(), &defined)
                .unwrap()
                .unwrap();
            assert_eq!(claimed.id, "r1", "priority did not order the claim");
            assert_eq!(plan, None);
            assert_eq!(claimed.claimed_by.as_deref(), Some("alpha"));
            assert_eq!(store.queue_depth().unwrap(), 2);
            assert_eq!(store.held_by("alpha").unwrap(), ["r1"]);
            // a claim is not stalled while its lease has time on it, and is
            // once it does not — which is the only way a claimer that went away
            // can be told from one that is working
            let now = Utc::now();
            assert!(store.stalled_claims(now).unwrap().is_empty());
            let later = now + chrono::Duration::seconds(31);
            let stalled = store.stalled_claims(later).unwrap();
            assert_eq!(stalled.len(), 1);
            assert_eq!(stalled[0].id, "r1");
            // by then the priority has been spent
            let err = store.set_run_priority("r1", 9).unwrap_err();
            assert!(matches!(err, Error::RunActive(_)), "{err}");

            // a limit skips rather than blocks, and the queue says so
            let one = Limits::new().global(1);
            assert!(
                store
                    .claim_next("beta", lease, &one, &defined)
                    .unwrap()
                    .is_none()
            );
            let queue = store.queue(&one, &defined, 10).unwrap();
            assert_eq!(queue[0].blocked, Some(Blocked::Global(1)));

            // a heartbeat moves the leases this claimer holds and nobody else's
            let before = store.run("r1").unwrap().unwrap().lease_until.unwrap();
            let longer = Duration::from_secs(600);
            assert_eq!(store.renew_leases("alpha", longer, &[]).unwrap(), 1);
            assert!(store.run("r1").unwrap().unwrap().lease_until.unwrap() > before);
            assert_eq!(store.renew_leases("beta", lease, &[]).unwrap(), 0);

            // cancelling takes an unclaimed run off the queue and leaves a
            // claimed one to be stopped the ordinary way
            assert!(store.cancel_queued("r0", None).unwrap());
            assert!(!store.cancel_queued("r1", None).unwrap());
            assert_eq!(
                store.run("r0").unwrap().unwrap().status,
                RunStatus::Canceled
            );
            assert_eq!(store.op_runs("r0").unwrap()[0].status, OpStatus::Canceled);

            // the run somebody else is executing: its terminal event belongs
            // to that process and does not know who asked, so the asking is a
            // line of its own and it is the line with the name on it
            store.cancel_requested("r1", Some("ada")).unwrap();
            let asked = store.events("r1", 0).unwrap().pop().unwrap();
            assert_eq!(asked.actor.as_deref(), Some("ada"));
            assert!(
                asked.message.contains("cancel requested by ada"),
                "{asked:?}"
            );

            // and a fan-out adds op rows to a run already under way, twice
            // without complaint
            store.create_op_run("r1", "a[0]").unwrap();
            store.create_op_run("r1", "a[0]").unwrap();
            assert_eq!(store.op_runs("r1").unwrap().len(), 2);
        });
    }

    #[test]
    fn a_backfill_records_the_range_it_fixed_and_the_runs_it_chunked_out() {
        both(|db| {
            let store = db.store();
            let keys = ["2026-01-01".to_string(), "2026-01-02".to_string()];
            let id = store
                .create_backfill("stats", "2026-01-01", "2026-01-02", &keys, None)
                .unwrap();
            let nothing = store.create_backfill("docs", "a", "b", &[], None).unwrap();
            assert_ne!(id, nothing, "two backfills, two ids");

            let row = store.backfill(id).unwrap().unwrap();
            assert_eq!(row.asset, "stats");
            assert_eq!(row.partitions, keys);
            assert_eq!((row.total, row.launched), (2, 0));
            assert_eq!(row.status, BackfillStatus::Running);
            assert!(row.run_ids.is_empty());
            // a range that resolved to nothing is complete the moment it is made
            let row = store.backfill(nothing).unwrap().unwrap();
            assert_eq!(row.status, BackfillStatus::Complete);
            assert!(row.finished_at.is_some());
            assert!(store.backfill(9_999).unwrap().is_none());

            assert_eq!(store.running_backfills().unwrap().len(), 1);
            store.backfill_launched(id, "sales", "r1", 1, 2).unwrap();
            store.backfill_launched(id, "sales", "r2", 2, 2).unwrap();
            let row = store.backfill(id).unwrap().unwrap();
            assert_eq!(row.run_ids, ["r1", "r2"], "the chunks did not append");
            assert_eq!(row.launched, 2);

            store
                .finish_backfill(id, "sales", BackfillStatus::Complete, 2, 2)
                .unwrap();
            // the first terminal status wins: a cancel racing the chunker
            // cannot be overwritten by what the run did next
            store
                .finish_backfill(id, "sales", BackfillStatus::Canceled, 2, 2)
                .unwrap();
            let row = store.backfill(id).unwrap().unwrap();
            assert_eq!(row.status, BackfillStatus::Complete);
            assert!(store.running_backfills().unwrap().is_empty());
            assert_eq!(store.backfills(10).unwrap().len(), 2);
        });
    }

    #[test]
    fn check_results_are_history_per_partition_and_prune_to_the_latest() {
        both(|db| {
            let store = db.store();
            let facts = json!({"rows": {"int": 3}});
            let record = |partition, status, message| {
                store
                    .record_check(
                        "stats",
                        partition,
                        "has_rows",
                        "r1",
                        status,
                        Severity::Error,
                        message,
                        Some(&facts),
                    )
                    .unwrap();
            };
            record(None, CheckStatus::Passed, None);
            record(None, CheckStatus::Failed, Some("no rows"));
            record(Some("2026-01-01"), CheckStatus::Passed, None);

            let all = store.asset_checks("stats", None, 10).unwrap();
            assert_eq!(all.len(), 3, "every key of the asset, newest first");
            assert_eq!(all[0].partition.as_deref(), Some("2026-01-01"));
            assert_eq!(all[1].status, CheckStatus::Failed);
            assert_eq!(all[1].message.as_deref(), Some("no rows"));
            assert_eq!(all[1].metadata, Some(facts));
            assert_eq!(all[1].severity, Severity::Error);
            assert_eq!(
                store
                    .asset_checks("stats", Some("2026-01-01"), 10)
                    .unwrap()
                    .len(),
                1
            );
            assert!(store.asset_checks("nothing", None, 10).unwrap().is_empty());

            // the latest of every (asset, partition, check) triple, with the
            // unpartitioned one first
            let latest = store.latest_asset_checks().unwrap();
            assert_eq!(latest.len(), 2);
            assert_eq!(latest[0].partition, None);
            assert_eq!(latest[0].status, CheckStatus::Failed);

            assert_eq!(store.prune_asset_checks(1).unwrap(), 1);
            assert_eq!(store.asset_checks("stats", None, 10).unwrap().len(), 2);
            assert_eq!(
                store.prune_asset_checks(0).unwrap(),
                0,
                "the latest result is what the summary counts, at any cap"
            );
        });
    }

    #[test]
    fn freshness_state_records_a_crossing_and_drops_it_on_the_way_back() {
        both(|db| {
            let store = db.store();
            assert!(store.freshness_states().unwrap().is_empty());
            let since = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            store
                .set_freshness_state("job", "etl", true, Some(since))
                .unwrap();
            store
                .set_freshness_state("asset", "stats", false, None)
                .unwrap();

            let rows = store.freshness_states().unwrap();
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0].kind, "asset");
            assert!(!rows[0].late && rows[0].since.is_none());
            assert!(rows[1].late);
            assert_eq!(rows[1].since, Some(since));

            // the way back is the same row rewritten, and the interval goes
            // with it so a relapse is a new one
            store
                .set_freshness_state("job", "etl", false, None)
                .unwrap();
            let rows = store.freshness_states().unwrap();
            assert_eq!(rows.len(), 2);
            assert!(!rows[1].late);
            assert_eq!(rows[1].since, None);
        });
    }

    #[test]
    fn a_metadata_series_follows_one_key_across_runs_and_builds() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for (i, rows) in [Some(3), None, Some(5)].into_iter().enumerate() {
                let id = format!("r{i}");
                let at = t0 + chrono::Duration::minutes(i as i64);
                store
                    .create_run(&mk_run(&id, "etl", at), &["load".into()])
                    .unwrap();
                let reported = rows.map(|n| json!({"rows": {"int": n}, "note": {"text": "x"}}));
                store
                    .op_finished(
                        &id,
                        "load",
                        OpStatus::Success,
                        None,
                        reported.as_ref(),
                        None,
                        &[],
                    )
                    .unwrap();
            }

            // oldest first, and the run that reported nothing is no point
            // rather than a gap or a zero
            let series = store.op_metadata_series("etl", "load", "rows", 10).unwrap();
            assert_eq!(
                series.iter().map(|p| p.value).collect::<Vec<_>>(),
                [3.0, 5.0]
            );
            assert_eq!(series[0].run_id.as_deref(), Some("r0"));
            assert!(
                store
                    .op_metadata_series("etl", "load", "note", 10)
                    .unwrap()
                    .is_empty(),
                "something that is not a number is not a point"
            );

            for (key, rows) in [(None, 10), (Some("k"), 400), (None, 14)] {
                store
                    .record_materialization(
                        "stats",
                        key,
                        "f",
                        &json!({}),
                        None,
                        Some("r0"),
                        Some(&json!({"rows": {"int": rows}})),
                    )
                    .unwrap();
            }
            // without a key the builds of every key interleave by time, which
            // is a trend of the asset rather than of any one of them
            let every = store
                .asset_metadata_series("stats", None, "rows", 10)
                .unwrap();
            assert_eq!(
                every.iter().map(|p| p.value).collect::<Vec<_>>(),
                [10.0, 400.0, 14.0]
            );
            let one = store
                .asset_metadata_series("stats", Some("k"), "rows", 10)
                .unwrap();
            assert_eq!(one.iter().map(|p| p.value).collect::<Vec<_>>(), [400.0]);
            assert_eq!(one[0].run_id.as_deref(), Some("r0"));
        });
    }

    #[test]
    fn terminal_runs_read_forward_from_a_cursor_that_never_repeats_a_tie() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            assert!(store.latest_terminal_run(None).unwrap().is_none());
            assert!(store.last_success("etl").unwrap().is_none());

            // two of them finish at the same instant, deliberately
            for (id, job, status, at) in [
                ("a", "etl", RunStatus::Success, t0),
                (
                    "b",
                    "etl",
                    RunStatus::Failed,
                    t0 + chrono::Duration::minutes(1),
                ),
                (
                    "c",
                    "other",
                    RunStatus::Success,
                    t0 + chrono::Duration::minutes(1),
                ),
            ] {
                store.create_run(&mk_run(id, job, t0), &[]).unwrap();
                store.run_finished(id, status, None, at, None).unwrap();
            }
            // and one that has not finished at all
            store.create_run(&mk_run("live", "etl", t0), &[]).unwrap();
            store.run_started("live", Utc::now()).unwrap();

            let all = store.terminal_runs_after(None, None, 10).unwrap();
            let seen: Vec<&str> = all.iter().map(|r| r.id.as_str()).collect();
            assert_eq!(seen, ["a", "b", "c"], "oldest finish first, ties by id");

            let cursor = store.latest_terminal_run(None).unwrap().unwrap();
            assert_eq!(cursor.id, "c");
            assert!(
                store
                    .terminal_runs_after(None, Some(&cursor), 10)
                    .unwrap()
                    .is_empty(),
                "a cursor at the newest run read it again"
            );
            // strictly after, so the run sharing an instant with the cursor is
            // not read twice either
            let tied = RunCursor {
                finished_at: t0 + chrono::Duration::minutes(1),
                id: "b".into(),
            };
            let after = store.terminal_runs_after(None, Some(&tied), 10).unwrap();
            assert_eq!(
                after.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["c"]
            );

            let etl = store.terminal_runs_after(Some("etl"), None, 10).unwrap();
            assert_eq!(
                etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
                ["a", "b"]
            );
            assert_eq!(
                store.latest_terminal_run(Some("etl")).unwrap().unwrap().id,
                "b"
            );
            assert_eq!(store.last_success("etl").unwrap(), Some(t0));
            assert!(store.last_success("nothing").unwrap().is_none());
        });
    }

    #[test]
    fn an_isolated_ops_row_carries_the_process_and_the_inputs_it_was_handed() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
                .unwrap();
            assert_eq!(store.op_inputs("r1", "a").unwrap(), None);
            assert!(store.op_run("r1", "nobody").unwrap().is_none());

            // guarded on `running`: a fast child can record its own terminal
            // row first, and a pid on a finished op names a dead process
            store.op_spawned("r1", "a", 4242).unwrap();
            assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, None);
            store.op_started("r1", "a", 1).unwrap();
            store.op_spawned("r1", "a", 4242).unwrap();
            assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, Some(4242));

            let handed = json!({"held": {"up": "h1"}, "deps": {"up": "success"}});
            store.set_op_inputs("r1", "a", &handed).unwrap();
            assert_eq!(store.op_inputs("r1", "a").unwrap(), Some(handed));

            // and the terminal write hands the process back
            store
                .op_finished("r1", "a", OpStatus::Success, None, None, None, &[])
                .unwrap();
            assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, None);
        });
    }

    #[test]
    fn a_deferred_fire_waits_on_the_tick_log_until_a_later_tick_replaces_it() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            let hour = chrono::Duration::hours(1);
            let tick = |at, outcome| {
                store
                    .record_tick("etl", "0 * * * *", at, outcome, false, None, None)
                    .unwrap();
            };
            tick(t0, TickOutcome::Deferred);
            tick(t0 + hour, TickOutcome::Deferred);
            // the first occurrence launched on a later pass; the second has not
            store
                .record_tick(
                    "etl",
                    "0 * * * *",
                    t0,
                    TickOutcome::Fired,
                    false,
                    Some("r1"),
                    None,
                )
                .unwrap();

            let waiting = store.pending_fires().unwrap();
            assert_eq!(waiting.len(), 1, "a fire that launched is not still due");
            assert_eq!(waiting[0], ("etl".into(), "0 * * * *".into(), t0 + hour));

            // the cursor only ever goes forward, so a held fire draining late
            // does not un-account for everything since
            store
                .sync_schedules(&[Schedule::new("etl", "0 * * * *")])
                .unwrap();
            store
                .set_schedule_cursor("etl", "0 * * * *", t0 + hour)
                .unwrap();
            store.set_schedule_cursor("etl", "0 * * * *", t0).unwrap();
            assert_eq!(store.schedules().unwrap()[0].cursor, Some(t0 + hour));

            store.prune_ticks(1).unwrap();
            let left = store.ticks(None, 10).unwrap();
            assert_eq!(left.len(), 1);
            assert_eq!(left[0].outcome, TickOutcome::Fired);
        });
    }

    // ------------------------------------------------------------- contention
    // what a shared run log is for, and the four things that have to be true of
    // one. the racing cases below start real threads on connections of their
    // own and release them together: two claimers that never actually overlap
    // would prove nothing whatsoever about either backend.

    /// `hands` claimers reaching for the queue at once, each on a connection of
    /// its own, released together. returns what each came away with.
    fn race(db: &Backend, hands: usize, limits: Limits) -> Vec<Option<(String, String)>> {
        let defined = HashSet::from(["etl".to_string()]);
        let gate = std::sync::Arc::new(std::sync::Barrier::new(hands));
        let claimers: Vec<_> = (0..hands)
            .map(|i| {
                let (store, gate) = (db.store(), gate.clone());
                let (defined, limits) = (defined.clone(), limits.clone());
                std::thread::spawn(move || {
                    let me = format!("claimer-{i}");
                    gate.wait();
                    let claimed = store
                        .claim_next(&me, Duration::from_secs(30), &limits, &defined)
                        .unwrap();
                    claimed.map(|(run, _)| (me, run.id))
                })
            })
            .collect();
        claimers.into_iter().map(|c| c.join().unwrap()).collect()
    }

    #[test]
    fn several_claimers_race_one_run_and_exactly_one_comes_away_with_it() {
        both(|db| {
            let store = db.store();
            store
                .create_run(&mk_run("contested", "etl", Utc::now()), &["a".into()])
                .unwrap();

            let won: Vec<_> = race(db, 4, Limits::new()).into_iter().flatten().collect();
            assert_eq!(won.len(), 1, "one run went to several claimers: {won:?}");
            let (winner, id) = &won[0];
            assert_eq!(id, "contested");

            let row = store.run("contested").unwrap().unwrap();
            assert_eq!(row.claimed_by.as_ref(), Some(winner));
            assert!(row.lease_until.is_some());
            // and it is off the queue for good
            assert_eq!(store.queue_depth().unwrap(), 0);
        });
    }

    #[test]
    fn several_claimers_race_a_full_queue_and_split_it_without_overlapping() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for i in 0..4 {
                let run = mk_run(&format!("r{i}"), "etl", t0 + chrono::Duration::minutes(i));
                store.create_run(&run, &["a".into()]).unwrap();
            }

            let taken: Vec<_> = race(db, 4, Limits::new()).into_iter().flatten().collect();
            assert_eq!(taken.len(), 4, "a claimer came away empty: {taken:?}");
            let mut ids: Vec<&str> = taken.iter().map(|(_, id)| id.as_str()).collect();
            ids.sort_unstable();
            ids.dedup();
            assert_eq!(ids.len(), 4, "two claimers took the same run: {taken:?}");

            // every row says who holds it, and each of them holds exactly one
            for (claimer, id) in &taken {
                let row = store.run(id).unwrap().unwrap();
                assert_eq!(row.claimed_by.as_ref(), Some(claimer));
                assert_eq!(store.held_by(claimer).unwrap(), [id.as_str()]);
            }
            assert_eq!(store.queue_depth().unwrap(), 0);
        });
    }

    // what a limit is: counting the free slot and spending it are one decision,
    // however many dispatchers are counting at the time
    #[test]
    fn two_dispatchers_cannot_both_take_the_last_slot() {
        both(|db| {
            let store = db.store();
            let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
            for i in 0..4 {
                let run = mk_run(&format!("r{i}"), "etl", t0 + chrono::Duration::minutes(i));
                store.create_run(&run, &["a".into()]).unwrap();
            }

            let taken: Vec<_> = race(db, 4, Limits::new().global(1))
                .into_iter()
                .flatten()
                .collect();
            assert_eq!(taken.len(), 1, "a global limit of one started {taken:?}");
            assert_eq!(store.queue_depth().unwrap(), 3);
        });
    }

    // the review finding from the phase that made the queue durable, asserted
    // on both backends: process B booting must not touch process A's work
    #[test]
    fn a_live_lease_survives_another_processs_boot_and_an_expired_one_does_not() {
        both(|db| {
            let store = db.store();
            for id in ["live", "stranded", "waiting"] {
                store
                    .create_run(&mk_run(id, "etl", Utc::now()), &["work".into()])
                    .unwrap();
            }
            // two are being executed by processes that are not this one; one is
            // still saying so and the other stopped a while ago
            let minute = chrono::Duration::seconds(60);
            store
                .plant_claim("live", "other-process", Some(Utc::now() + minute))
                .unwrap();
            store
                .plant_claim("stranded", "dead-process", Some(Utc::now() - minute))
                .unwrap();
            for id in ["live", "stranded"] {
                store.run_started(id, Utc::now()).unwrap();
                store.op_started(id, "work", 1).unwrap();
            }

            // this is what booting is
            store.fail_interrupted().unwrap();

            let live = store.run("live").unwrap().unwrap();
            assert_eq!(live.status, RunStatus::Running, "{:?}", live.error);
            assert_eq!(live.claimed_by.as_deref(), Some("other-process"));
            assert_eq!(
                store.op_run("live", "work").unwrap().unwrap().status,
                OpStatus::Running,
                "a boot elsewhere interrupted a live process's op"
            );
            assert!(
                !store
                    .events("live", 0)
                    .unwrap()
                    .iter()
                    .any(|e| e.message.contains("interrupted")),
                "a boot elsewhere announced an interruption on a live run"
            );

            let stranded = store.run("stranded").unwrap().unwrap();
            assert_eq!(stranded.status, RunStatus::Failed);
            assert!(stranded.error.unwrap().contains("interrupted"));
            assert_eq!(
                store.op_run("stranded", "work").unwrap().unwrap().status,
                OpStatus::Failed
            );

            // and a queued run nobody has claimed is the queue, not a casualty
            let waiting = store.run("waiting").unwrap().unwrap();
            assert_eq!(waiting.status, RunStatus::Queued);
            assert_eq!(store.queue_depth().unwrap(), 1);
        });
    }

    #[test]
    fn an_expired_lease_is_reclaimed_and_a_live_one_is_left_where_it_is() {
        both(|db| {
            let store = db.store();
            for id in ["stalled", "held"] {
                store
                    .create_run(&mk_run(id, "etl", Utc::now()), &["work".into()])
                    .unwrap();
                store.run_started(id, Utc::now()).unwrap();
                store.op_started(id, "work", 1).unwrap();
            }
            let minute = chrono::Duration::seconds(60);
            store
                .plant_claim("stalled", "vanished", Some(Utc::now() - minute))
                .unwrap();
            store
                .plant_claim("held", "alive", Some(Utc::now() + minute))
                .unwrap();

            let asked = std::cell::RefCell::new(Vec::new());
            let taken = store
                .reclaim_expired(Reclaim::Fail, |run| {
                    asked.borrow_mut().push(run.id.clone());
                    Some(json!({"run_id": run.id, "status": run.status.as_str()}))
                })
                .unwrap();
            assert_eq!(taken.len(), 1, "a live lease was reclaimed: {taken:?}");
            assert_eq!(taken[0].id, "stalled");
            // the alert is written with the terminal status, in the transaction
            // that wrote it, rather than a statement later
            assert_eq!(asked.into_inner(), ["stalled"]);
            let alerts = store.notifications(None, 10).unwrap();
            assert_eq!(alerts.len(), 1);
            assert_eq!(alerts[0].payload["status"], "failed");

            let run = store.run("stalled").unwrap().unwrap();
            assert_eq!(run.status, RunStatus::Failed);
            assert_eq!(run.lease_until, None);
            let why = run.error.unwrap();
            assert!(why.contains("claimer went away"), "{why}");
            assert!(why.contains("vanished"), "{why}");
            let op = store.op_run("stalled", "work").unwrap().unwrap();
            assert_eq!(op.status, OpStatus::Failed);
            assert_eq!(op.pid, None);
            assert_eq!(
                store.run("held").unwrap().unwrap().status,
                RunStatus::Running
            );

            // and under requeue it goes back to exactly what an unclaimed
            // queued run is, for whoever takes it next
            store
                .plant_claim("held", "vanished-too", Some(Utc::now() - minute))
                .unwrap();
            let taken = store.reclaim_expired(Reclaim::Requeue, |_| None).unwrap();
            assert_eq!(taken.len(), 1);
            let run = store.run("held").unwrap().unwrap();
            assert_eq!(run.status, RunStatus::Queued);
            assert_eq!(run.claimed_by, None);
            assert_eq!(run.claimed_at, None);
            assert_eq!(run.started_at, None);
            assert_eq!(store.queue_depth().unwrap(), 1);
            assert!(
                store
                    .events("held", 0)
                    .unwrap()
                    .iter()
                    .any(|e| e.message.contains("requeued for another claimer"))
            );
        });
    }

    // the throughput half of the claim, which is postgres's alone: a run
    // somebody else is mid-claim on is skipped, not waited for. without
    // `SKIP LOCKED` the claimer below blocks on the held row until the holder
    // lets go, and the outcome is the same run claimed a great deal later.
    #[cfg(feature = "postgres")]
    #[test]
    fn a_run_another_dispatcher_is_holding_is_skipped_rather_than_waited_on() {
        let Some(pg) = Scratch::new() else {
            return;
        };
        let store = pg.store();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        for i in 0..2 {
            let run = mk_run(&format!("r{i}"), "etl", t0 + chrono::Duration::minutes(i));
            store.create_run(&run, &["a".into()]).unwrap();
        }

        // another dispatcher, mid-claim on the head of the queue
        let mut other = crate::pg::unmigrated(&pg.url).unwrap();
        let mut holding = other.transaction().unwrap();
        holding
            .execute("SELECT id FROM runs WHERE id = 'r0' FOR UPDATE", args![])
            .unwrap();

        let claiming = {
            let (store, defined) = (pg.store(), HashSet::from(["etl".to_string()]));
            std::thread::spawn(move || {
                store
                    .claim_next("beta", Duration::from_secs(30), &Limits::new(), &defined)
                    .unwrap()
            })
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !claiming.is_finished() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            claiming.is_finished(),
            "the dispatcher is waiting for a row it could have skipped"
        );
        let (claimed, _) = claiming.join().unwrap().unwrap();
        assert_eq!(claimed.id, "r1");

        // and once the holder lets go, the head of the queue is claimable again
        drop(holding);
        let defined = HashSet::from(["etl".to_string()]);
        let (claimed, _) = store
            .claim_next("gamma", Duration::from_secs(30), &Limits::new(), &defined)
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, "r0");
    }

    // the three writes a test needs and a running process never makes: history
    // older than the test itself, and a delivery put back where a crash
    // between the hook returning and the mark landing would have left it
    #[test]
    fn history_can_be_backdated_and_a_delivery_undone() {
        both(|db| {
            let store = db.store();
            let old = Utc::now() - chrono::Duration::days(30);
            store
                .create_run(&mk_run("r1", "etl", Utc::now()), &[])
                .unwrap();
            store
                .run_finished(
                    "r1",
                    RunStatus::Success,
                    None,
                    Utc::now(),
                    Some(&json!({"run_id": "r1"})),
                )
                .unwrap();
            store.backdate_run("r1", old).unwrap();
            assert_eq!(store.last_success("etl").unwrap(), Some(old));

            for key in [None, Some("k")] {
                store
                    .record_materialization("stats", key, "f", &json!({}), None, None, None)
                    .unwrap();
            }
            store.backdate_materialization("stats", None, old).unwrap();
            assert_eq!(
                store
                    .materialization("stats", None)
                    .unwrap()
                    .unwrap()
                    .built_at,
                old
            );
            assert!(
                store
                    .materialization("stats", Some("k"))
                    .unwrap()
                    .unwrap()
                    .built_at
                    > old,
                "backdating one key moved another"
            );

            let note = store.notifications(None, 10).unwrap().pop().unwrap();
            assert!(store.delivered(note.id, Utc::now()).unwrap());
            assert!(store.due_notifications(Utc::now(), 10).unwrap().is_empty());
            store.undeliver(note.id, old).unwrap();
            let due = store.due_notifications(Utc::now(), 10).unwrap();
            assert_eq!(due.len(), 1);
            assert_eq!(due[0].state, DeliveryState::Pending);
        });
    }

    // what `Hestan::db` hands a store, and what an isolated op's child is
    // handed again: one string that says which backend it means
    #[test]
    fn a_target_is_a_path_or_a_url_and_opens_the_right_backend() {
        both(|db| {
            let store = Store::at(&db.target()).unwrap();
            assert!(store.schedules().unwrap().is_empty());
            assert!(!store.is_private(), "a case's database is reachable twice");
        });
        assert!(Store::open(":memory:").unwrap().is_private());
    }

    /// every table the sqlite chain arrives at after sixteen migrations. a
    /// fresh postgres database has all of them from its first statement.
    #[cfg(feature = "postgres")]
    const TABLES: [&str; 17] = [
        "asset_checks",
        "asset_materializations",
        "backfills",
        "events",
        "freshness_state",
        "notifications",
        "op_logs",
        "op_runs",
        "op_state",
        "presets",
        "runs",
        "schedule_ticks",
        "schedules",
        "schema_version",
        "sensor_run_keys",
        "sensor_ticks",
        "sensors",
    ];

    #[cfg(feature = "postgres")]
    #[test]
    fn a_fresh_postgres_database_is_created_whole_at_the_current_version() {
        let Some(pg) = Scratch::new() else {
            return;
        };
        let store = pg.store();

        let mut found: Vec<String> = store
            .conn()
            .query(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_schema = current_schema()",
                args![],
                |r| r.text(0),
            )
            .unwrap();
        found.sort();
        assert_eq!(found, TABLES, "the schema is not the one sqlite arrives at");

        let version = store
            .conn()
            .query_opt("SELECT version FROM schema_version", args![], |r| r.int(0))
            .unwrap();
        assert_eq!(version, Some(i64::from(SCHEMA_VERSION)));

        // and the partial index the delivery loop scans, which is the one
        // index that is more than a column list
        let due: Option<String> = store
            .conn()
            .query_opt(
                "SELECT indexdef FROM pg_indexes
                 WHERE schemaname = current_schema() AND indexname = 'notifications_due'",
                args![],
                |r| r.text(0),
            )
            .unwrap();
        assert!(
            due.unwrap_or_default()
                .contains("WHERE (delivered_at IS NULL)"),
            "the undelivered-notifications index is not partial"
        );

        // opening it again finds it already there and writes nothing
        let again = pg.store();
        assert!(again.schedules().unwrap().is_empty());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn a_postgres_database_from_a_later_build_is_refused_not_downgraded() {
        let Some(pg) = Scratch::new() else {
            return;
        };
        let store = pg.store();
        store
            .conn()
            .execute("UPDATE schema_version SET version = ?1", args![19_i64])
            .unwrap();
        drop(store);

        let err = Store::connect(&pg.url).err().unwrap();
        assert_eq!(err.to_string(), "db schema v19 is newer than this build");

        // and it is still v19: a build that cannot read a database must not
        // rewrite it either
        let version = crate::pg::unmigrated(&pg.url)
            .unwrap()
            .query("SELECT version FROM schema_version", args![], |r| r.int(0))
            .unwrap();
        assert_eq!(version, [19]);
    }

    // ------------------------------------------- what a write is worth losing

    /// every store write that declares itself best-effort, read out of this
    /// file: the name in front of each `-> BestEffort`.
    ///
    /// the return type *is* the declaration. nothing outside this file can
    /// make one — the fields are private to it — so a scrape of this file is
    /// the whole of the boundary rather than a sample of it.
    fn best_effort_writes() -> Vec<String> {
        // spelled at runtime so this line is not itself one of the matches
        let declaration = format!("-> {} {{", "BestEffort");
        include_str!("store.rs")
            .match_indices(&declaration)
            .filter_map(|(at, _)| {
                let before = &include_str!("store.rs")[..at];
                let name = &before[before.rfind("fn ")? + "fn ".len()..];
                Some(name[..name.find('(')?].to_string())
            })
            // the one that makes them, which is not one of them
            .filter(|name| name != "best_effort")
            .collect()
    }

    // the compiler stops a critical write being handed to `note`. what it
    // does not stop is one being thrown away by hand, which is the whole of
    // what is left of this hole — so the whole of what is left is a grep
    #[test]
    fn a_store_write_is_never_thrown_away() {
        for (file, src) in [
            ("executor.rs", include_str!("executor.rs")),
            ("hooks.rs", include_str!("hooks.rs")),
            ("isolate.rs", include_str!("isolate.rs")),
            ("logs.rs", include_str!("logs.rs")),
            ("op.rs", include_str!("op.rs")),
            ("store.rs", include_str!("store.rs")),
        ] {
            // spelled at runtime so these lines are not themselves matches
            for thrown in [
                format!("let _ = {}.", "store"),
                format!("let _ = self.{}.", "store"),
                format!("{}(store.", "drop"),
            ] {
                assert!(
                    !src.contains(&thrown),
                    "{file} discards a store write with `{thrown}`"
                );
            }
        }
    }

    // the list is short on purpose and every entry on it is a decision: an
    // event, a captured line, a pid, and the line that says who asked for a
    // cancel. everything else a store writes is authoritative until this file
    // says otherwise, which is the way round that survives being forgotten
    #[test]
    fn only_the_writes_named_here_may_be_dropped() {
        let mut declared = best_effort_writes();
        declared.sort();
        assert_eq!(
            declared,
            [
                "append_event",
                "append_op_log",
                "cancel_requested",
                "op_spawned"
            ]
        );
    }

    /// rustc, wherever the cargo that built this test came from.
    fn rustc() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO"))
            .parent()
            .map(|bin| bin.join("rustc"))
            .filter(|rustc| rustc.exists())
            .unwrap_or_else(|| "rustc".into())
    }

    /// compile `source` on its own, and hand back what rustc said about it.
    fn compiled(dir: &std::path::Path, name: &str, source: &str) -> std::process::Output {
        let file = dir.join(format!("{name}.rs"));
        std::fs::write(&file, source).unwrap();
        std::process::Command::new(rustc())
            .args([
                "--edition",
                "2024",
                "--crate-type",
                "lib",
                "--emit",
                "metadata",
            ])
            .arg("--out-dir")
            .arg(dir)
            .arg(&file)
            .output()
            .expect("rustc built this test and is still on this machine")
    }

    /// one store write's return type, as this file declares it, in front of a
    /// `note` that takes what this file says `note` takes.
    ///
    /// both halves are scraped rather than written out, so that a signature
    /// moving moves this with it: a `note` that quietly started taking a
    /// `Result` again would otherwise leave a hand-written model compiling,
    /// and that is the regression worth catching.
    fn as_declared(write: &str, noted: bool) -> String {
        let src = include_str!("store.rs");
        let after = |what: &str| {
            let at = src.find(what).expect("a declaration to read");
            &src[at + what.len()..]
        };
        let returns = after(&format!("fn {write}("));
        let returns = &returns[returns.find("->").expect("a return type") + 2..];
        let returns = returns[..returns.find(" {").expect("a body")].trim();
        let takes = after(&format!("fn {}(", "note"));
        let takes = &takes[..takes.find(')').expect("one parameter")];
        let takes = takes[takes.find(':').expect("its type") + 1..].trim();
        format!(
            "#[derive(Debug)] pub struct Error;\n\
             pub struct BestEffort;\n\
             pub fn note(_: {takes}) {{}}\n\
             pub fn wrote() -> {returns} {{ unimplemented!() }}\n\
             pub fn check() {{ {} }}\n",
            match noted {
                true => "note(wrote());",
                false => "wrote();",
            }
        )
    }

    // the property this phase exists for is a type error, and a type error is
    // the one thing a green suite cannot show you. so it is put to rustc
    // directly, against the return types this file actually declares: an event
    // may be dropped, and what an op did is not the kind of thing `note` takes
    #[test]
    fn a_write_that_records_what_a_run_did_cannot_be_noted() {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        let event = compiled(dir, "event", &as_declared("append_event", true));
        assert!(
            event.status.success(),
            "an event write could not be noted either, so this proves nothing: {}",
            String::from_utf8_lossy(&event.stderr)
        );

        let terminal = compiled(dir, "terminal", &as_declared("op_finished", true));
        let said = String::from_utf8_lossy(&terminal.stderr);
        assert!(
            !terminal.status.success(),
            "an op's terminal write compiled"
        );
        // and refused for the reason claimed, rather than for a typo in the
        // source this test just wrote
        assert!(
            said.contains("expected `BestEffort`") && said.contains("mismatched types"),
            "refused, but not as a type error: {said}"
        );

        // and the same write, not noted, still compiles: what is being refused
        // is dropping it, not writing it
        assert!(
            compiled(dir, "kept", &as_declared("op_finished", false))
                .status
                .success()
        );
    }

    /// a write that fails `n` times, then does what `then` does, counting the
    /// attempts it took.
    fn flaky(
        n: u64,
        tries: &AtomicU64,
        then: impl Fn() -> Result<(), Error>,
    ) -> impl Fn() -> Result<(), Error> {
        move || match tries.fetch_add(1, Ordering::SeqCst) < n {
            true => Err(Error::Sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(5),
                Some("database is locked".to_string()),
            ))),
            false => then(),
        }
    }

    // a lock is the ordinary way a write fails and it is over in milliseconds,
    // so the write goes back for it rather than the run stopping over one
    #[tokio::test]
    async fn a_write_that_was_locked_out_twice_lands_on_the_third_try() {
        let store = Store::open(":memory:").unwrap();
        store
            .create_run(&mk_run("r1", "etl", Utc::now()), &["a".to_string()])
            .unwrap();
        let tries = AtomicU64::new(0);
        let landed = store
            .landed(
                "op_finished",
                flaky(2, &tries, || {
                    store.op_finished("r1", "a", OpStatus::Success, None, None, None, &[])
                }),
            )
            .await;

        assert!(landed);
        assert_eq!(tries.load(Ordering::SeqCst), 3);
        // and what landed is the write, once
        let row = store.op_run("r1", "a").unwrap().unwrap();
        assert_eq!(row.status, OpStatus::Success);
        assert_eq!(store.health().unrecorded_writes(), 0);
    }

    // and a store that is not coming back is given a bounded number of
    // chances: a run holding its claim while it waits is a run nothing else
    // can take either
    #[tokio::test]
    async fn a_store_that_never_takes_the_write_is_not_waited_on_forever() {
        let store = Store::open(":memory:").unwrap();
        let tries = AtomicU64::new(0);
        let began = std::time::Instant::now();
        let landed = store
            .landed("op_finished", flaky(u64::MAX, &tries, || Ok(())))
            .await;

        assert!(!landed);
        assert_eq!(tries.load(Ordering::SeqCst), u64::from(WRITE_ATTEMPTS));
        assert!(
            began.elapsed() < Duration::from_secs(5),
            "it waited too long"
        );
        assert_eq!(store.health().unrecorded_writes(), 1);
    }

    // a statement the database refuses is not a lock: the same call will be
    // refused the same way, and three more of them is three more of nothing
    #[tokio::test]
    async fn a_write_the_database_will_never_take_is_not_repeated() {
        let store = Store::open(":memory:").unwrap();
        let tries = AtomicU64::new(0);
        let landed = store
            .landed("op_finished", || {
                tries.fetch_add(1, Ordering::SeqCst);
                Err(Error::Sqlite(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(19),
                    Some("constraint failed".to_string()),
                )))
            })
            .await;

        assert!(!landed);
        assert_eq!(tries.load(Ordering::SeqCst), 1);
    }

    // the one failure whose outcome hestan cannot know. a connection that died
    // may have carried a commit the server ran and the acknowledgement of it
    // nobody received — so going back for it is the one retry that could
    // record a build twice, and it is the one retry hestan does not make
    #[cfg(feature = "postgres")]
    #[tokio::test]
    async fn a_connection_that_died_is_not_retried_because_its_write_may_have_landed() {
        let Some(pg) = Scratch::new() else {
            return;
        };
        let store = pg.store();
        store
            .create_run(&mk_run("r1", "etl", Utc::now()), &["a".to_string()])
            .unwrap();
        let built = Built {
            asset: "orders".to_string(),
            partition: None,
            fingerprint: "fp".to_string(),
            inputs: json!({}),
            value: None,
            meta: None,
        };

        // this store's own backend, told to go away by another connection
        let pid = store
            .conn()
            .query("SELECT pg_backend_pid()::bigint", args![], |r| r.int(0))
            .unwrap()[0];
        let killer = pg.store();
        // pasted rather than bound: a backend id this test just read back is
        // not a value anybody typed, and `pg_terminate_backend` takes an int4
        killer
            .conn()
            .query(
                &format!("SELECT pg_terminate_backend({pid})"),
                args![],
                |_| Ok(()),
            )
            .unwrap();

        let tries = AtomicU64::new(0);
        let landed = store
            .landed("op_finished", || {
                tries.fetch_add(1, Ordering::SeqCst);
                store.op_finished(
                    "r1",
                    "a",
                    OpStatus::Success,
                    None,
                    None,
                    None,
                    std::slice::from_ref(&built),
                )
            })
            .await;
        assert!(!landed);
        assert!(
            tries.load(Ordering::SeqCst) < u64::from(WRITE_ATTEMPTS),
            "a dead connection was retried to exhaustion"
        );

        // and nothing this process writes lands again, which is the whole of
        // why a retry could not have doubled anything up: the materialization
        // is not there once, let alone twice
        assert!(
            !store
                .landed("op_finished", || store.op_finished(
                    "r1",
                    "a",
                    OpStatus::Success,
                    None,
                    None,
                    None,
                    std::slice::from_ref(&built)
                ))
                .await
        );
        assert!(
            killer
                .materializations("orders", None, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            killer.op_run("r1", "a").unwrap().unwrap().status,
            OpStatus::Pending
        );
    }

    // sqlite's read-only switch, which is what a doctor's probe has to be able
    // to see: `writable` is a claim about a write lock, so a database that
    // will not give one must come back as a refusal rather than as an `ok`
    #[test]
    fn a_store_that_will_not_take_a_write_says_so_when_asked() {
        let store = Store::open(":memory:").unwrap();
        store.writable().unwrap();
        store.refuse_writes().unwrap();
        let refused = store.writable().unwrap_err();
        assert!(
            refused.to_string().contains("readonly") || refused.to_string().contains("read-only"),
            "{refused}"
        );
    }

    // an event that does not land is survivable and is not silent: the store
    // says how many it has lost, and `/api/health` is where that is read
    #[test]
    fn a_dropped_event_is_counted_where_somebody_can_see_it() {
        let store = Store::open(":memory:").unwrap();
        assert_eq!(store.health().dropped_writes(), 0);

        store.fail_writes(1);
        note(store.append_event("r1", None, EventLevel::Info, EventKind::Log, "hello", None));
        assert_eq!(store.health().dropped_writes(), 1);
        assert_eq!(store.health().unrecorded_writes(), 0);

        // the next one lands, and is not counted
        note(store.append_event("r1", None, EventLevel::Info, EventKind::Log, "hello", None));
        assert_eq!(store.health().dropped_writes(), 1);
    }
}
