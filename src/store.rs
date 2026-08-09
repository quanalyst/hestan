use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::Value;

use crate::error::Error;
use crate::executor::{Blocked, InFlight, Limits, QUEUE_SCAN, Queued};
use crate::model::{
    AssetCheckRow, Backfill, BackfillStatus, CheckStatus, Event, EventKind, EventLevel,
    FreshnessRow, HistoryEntry, Materialization, MetaPoint, OpRun, OpStatus, Preset, Reclaim, Run,
    RunCursor, RunStatus, RunTags, ScheduleRow, SensorOutcome, SensorRow, SensorTick, Severity,
    Tick, TickOutcome,
};
use crate::op;
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

const SCHEMA_VERSION: u32 = 14;

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
            params![name],
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

/// every column [`run_from_row`] reads, in the order it reads them. one list
/// rather than four copies of it, since a run now carries enough columns that
/// two of them drifting apart is a real way to spend an afternoon.
const RUN_COLS: &str = r#"id, job, status, "trigger", params, created_at, started_at, finished_at,
    resumed_from, error, scheduled_for, tags, priority, claimed_by, claimed_at, lease_until"#;

/// how long a write waits for another connection to let go of the file before
/// giving up. an [isolated op](crate::Op::isolated) means two processes write
/// this database at once, and sqlite's default is to fail the second one
/// immediately — which would be a lost event, or a lost terminal row.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// sqlite-backed run history. cheap to clone; safe to share across tasks.
///
/// the second field is the path it was opened at, kept so a runner can tell
/// whether a child process could open the same database.
#[derive(Clone)]
pub struct Store(Arc<Mutex<Connection>>, Arc<str>);

impl Store {
    /// open (and migrate) the database at `path`; `":memory:"` works too.
    pub fn open(path: &str) -> Result<Store, Error> {
        let mut conn = Connection::open(path)?;
        if path != ":memory:" {
            conn.pragma_update(None, "journal_mode", "wal")?;
        }
        conn.busy_timeout(BUSY_TIMEOUT)?;
        migrate(&mut conn)?;
        Ok(Store(Arc::new(Mutex::new(conn)), path.into()))
    }

    /// whether this database lives only in this process's memory, and so
    /// cannot be reached by a child. `":memory:"` is private per connection,
    /// which is exactly right for a test and exactly wrong for an isolated op.
    pub(crate) fn is_private(&self) -> bool {
        &*self.1 == ":memory:"
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
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        if let Some(k) = key {
            let claimed = tx.execute(
                "INSERT OR IGNORE INTO sensor_run_keys (sensor, run_key, run_id, launched_at)
                 VALUES (?1, ?2, ?3, ?4)",
                params![k.sensor, k.key, run.id, Utc::now().to_rfc3339()],
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
                                 priority, plan)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"#,
            params![
                run.id,
                run.job,
                run.status.as_str(),
                run.trigger.as_str(),
                run.params.to_string(),
                run.created_at.to_rfc3339(),
                run.started_at.map(|t| t.to_rfc3339()),
                run.finished_at.map(|t| t.to_rfc3339()),
                run.error,
                run.resumed_from,
                run.scheduled_for.map(|t| t.to_rfc3339()),
                tags_col(&run.tags),
                run.priority,
                plan.map(|v| v.to_string()),
            ],
        )?;
        {
            let mut stmt =
                tx.prepare("INSERT INTO op_runs (run_id, op, status) VALUES (?1, ?2, ?3)")?;
            for op in ops {
                stmt.execute(params![run.id, op, OpStatus::Pending.as_str()])?;
            }
        }
        // same transaction as the row, so a run never exists without its queued event
        tx.execute(
            "INSERT INTO events (run_id, op, level, kind, message, ts)
             VALUES (?1, NULL, ?2, ?3, ?4, ?5)",
            params![
                run.id,
                EventLevel::Info.as_str(),
                EventKind::RunQueued.as_str(),
                "run queued",
                Utc::now().to_rfc3339()
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// whether `sensor` has already launched a run under `key`.
    pub(crate) fn run_key_claimed(&self, sensor: &str, key: &str) -> Result<bool, Error> {
        let conn = self.0.lock().unwrap();
        let found = conn
            .query_row(
                "SELECT 1 FROM sensor_run_keys WHERE sensor = ?1 AND run_key = ?2",
                params![sensor, key],
                |_| Ok(()),
            )
            .optional()?;
        Ok(found.is_some())
    }

    /// drop run keys claimed before `older_than`. nothing collects them on
    /// their own — a sensor keyed by the day would keep a row per day for as
    /// long as the file exists — so they ride the retention knob.
    pub(crate) fn prune_sensor_run_keys(&self, older_than: DateTime<Utc>) -> Result<usize, Error> {
        let conn = self.0.lock().unwrap();
        let removed = conn.execute(
            "DELETE FROM sensor_run_keys WHERE launched_at < ?1",
            params![older_than.to_rfc3339()],
        )?;
        Ok(removed)
    }

    /// add a `pending` op_runs row to a run already under way, for one
    /// instance a fan-out just created. the run's own loop is the only caller
    /// and it inserts before spawning, so a row can never land after the run's
    /// terminal status write; `OR IGNORE` keeps a repeat harmless.
    pub(crate) fn create_op_run(&self, run_id: &str, op: &str) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO op_runs (run_id, op, status) VALUES (?1, ?2, ?3)",
            params![run_id, op, OpStatus::Pending.as_str()],
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
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
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
            params![now],
        )?;
        tx.execute(
            &format!(
                "INSERT INTO events (run_id, op, level, kind, message, ts)
                 SELECT id, NULL, 'error', 'run_failed', 'run interrupted: process exited', ?1
                 FROM runs WHERE {}",
                Self::INTERRUPTED
            ),
            params![now],
        )?;
        tx.execute(
            &format!(
                "UPDATE runs SET status = 'failed', finished_at = ?1, lease_until = NULL,
                     error = COALESCE(error, 'interrupted: process exited')
                 WHERE {}",
                Self::INTERRUPTED
            ),
            params![now],
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
        let conn = self.0.lock().unwrap();
        let found = conn
            .query_row(
                "SELECT 1 FROM runs WHERE job = ?1 AND status IN ('queued', 'running') LIMIT 1",
                params![job],
                |_| Ok(()),
            )
            .optional()?;
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
    /// construction, whoever else is looking at the same row. counting and
    /// claiming share one immediate transaction, so two dispatchers cannot both
    /// read capacity for the last slot and both take it. postgres would use
    /// `SELECT ... FOR UPDATE SKIP LOCKED` here and hold no global write lock;
    /// sqlite serializes writers for us, which is the same guarantee on one
    /// host and no guarantee at all across several.
    ///
    /// returns the claimed run and its [plan](Self::create_run_keyed).
    pub(crate) fn claim_next(
        &self,
        claimer: &str,
        lease: Duration,
        limits: &Limits,
        defined: &HashSet<String>,
    ) -> Result<Option<(Run, Option<Value>)>, Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let counts = in_flight(&tx, limits)?;
        let candidates = queued(&tx, QUEUE_SCAN)?;
        let now = Utc::now();
        let until = now + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        for (mut run, plan) in candidates {
            if !defined.contains(&run.job) {
                continue;
            }
            if counts.blocker(limits, &run.job, &run.tags).is_some() {
                continue;
            }
            let won = tx.execute(
                "UPDATE runs SET claimed_by = ?1, claimed_at = ?2, lease_until = ?3
                 WHERE id = ?4 AND claimed_by IS NULL AND status = 'queued'",
                params![
                    claimer,
                    now.to_rfc3339(),
                    until.to_rfc3339(),
                    run.id.clone()
                ],
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
        let conn = self.0.lock().unwrap();
        let mut counts = in_flight(&conn, limits)?;
        let mut out = Vec::new();
        for (position, (run, _)) in queued(&conn, limit)?.into_iter().enumerate() {
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

    /// how many runs are queued and unclaimed, which is what "queue depth"
    /// means. counted rather than taken from [`queue`](Self::queue), which caps.
    pub(crate) fn queue_depth(&self) -> Result<usize, Error> {
        let conn = self.0.lock().unwrap();
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM runs WHERE status = 'queued' AND claimed_by IS NULL",
            [],
            |r| r.get(0),
        )?;
        Ok(n as usize)
    }

    /// move a run up or down the queue. false when there is no such run, and
    /// [`Error::RunActive`] once something has claimed it — by then the
    /// priority has already been spent.
    pub(crate) fn set_run_priority(&self, id: &str, priority: i64) -> Result<bool, Error> {
        let conn = self.0.lock().unwrap();
        let found: Option<(String, Option<String>)> = conn
            .query_row(
                "SELECT status, claimed_by FROM runs WHERE id = ?1",
                params![id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((status, claimed_by)) = found else {
            return Ok(false);
        };
        if status != RunStatus::Queued.as_str() || claimed_by.is_some() {
            return Err(Error::RunActive(id.to_string()));
        }
        conn.execute(
            "UPDATE runs SET priority = ?2 WHERE id = ?1 AND claimed_by IS NULL",
            params![id, priority],
        )?;
        Ok(true)
    }

    /// say that `claimer` is still here, for every run it holds. returns how
    /// many leases moved, which is how many runs this process is executing.
    pub(crate) fn renew_leases(&self, claimer: &str, lease: Duration) -> Result<usize, Error> {
        let conn = self.0.lock().unwrap();
        let until = Utc::now() + chrono::Duration::from_std(lease).unwrap_or(chrono::Duration::MAX);
        let n = conn.execute(
            "UPDATE runs SET lease_until = ?2
             WHERE claimed_by = ?1 AND status IN ('queued', 'running')",
            params![claimer, until.to_rfc3339()],
        )?;
        Ok(n)
    }

    /// the runs `claimer` currently holds, so a process can say what it is
    /// executing and anyone else can tell who holds what.
    pub(crate) fn held_by(&self, claimer: &str) -> Result<Vec<String>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id FROM runs
             WHERE claimed_by = ?1 AND status IN ('queued', 'running') ORDER BY created_at",
        )?;
        let rows = stmt.query_map(params![claimer], |r| r.get(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// take back every run whose claimer stopped saying it was there, and
    /// either fail it or put it back on the queue.
    ///
    /// its ops are marked either way, and with the reason: an op left `running`
    /// by a process that vanished did not finish, and a row that says otherwise
    /// is what the next resume would build on. returns `(run id, the claimer
    /// that went away)` for each.
    pub(crate) fn reclaim_expired(&self, policy: Reclaim) -> Result<Vec<(String, String)>, Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let now = Utc::now().to_rfc3339();
        let expired: Vec<(String, String)> = {
            let mut stmt = tx.prepare(
                "SELECT id, claimed_by FROM runs
                 WHERE claimed_by IS NOT NULL AND status IN ('queued', 'running')
                   AND (lease_until IS NULL OR lease_until < ?1)",
            )?;
            let rows = stmt.query_map(params![now], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        for (id, claimer) in &expired {
            let why = format!("claimer went away: {claimer} stopped renewing its lease");
            tx.execute(
                "UPDATE op_runs SET
                     status = CASE status WHEN 'running' THEN 'failed' ELSE 'skipped' END,
                     error = CASE status WHEN 'running' THEN ?2 ELSE error END,
                     finished_at = ?3, pid = NULL
                 WHERE run_id = ?1 AND status IN ('pending', 'running')",
                params![id, why, now],
            )?;
            let (level, kind, message) = match policy {
                Reclaim::Fail => (EventLevel::Error, EventKind::RunFailed, why.clone()),
                Reclaim::Requeue => (
                    EventLevel::Warn,
                    EventKind::Log,
                    format!("{why}; requeued for another claimer"),
                ),
            };
            tx.execute(
                "INSERT INTO events (run_id, op, level, kind, message, ts)
                 VALUES (?1, NULL, ?2, ?3, ?4, ?5)",
                params![id, level.as_str(), kind.as_str(), message, now],
            )?;
            match policy {
                Reclaim::Fail => tx.execute(
                    "UPDATE runs SET status = 'failed', finished_at = ?2, lease_until = NULL,
                         error = COALESCE(error, ?3)
                     WHERE id = ?1",
                    params![id, now, why],
                )?,
                // back to exactly what an unclaimed queued run is: no owner, no
                // lease, and no start time it turned out not to have had
                Reclaim::Requeue => tx.execute(
                    "UPDATE runs SET status = 'queued', claimed_by = NULL, claimed_at = NULL,
                         lease_until = NULL, started_at = NULL
                     WHERE id = ?1",
                    params![id],
                )?,
            };
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
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE runs SET claimed_by = ?2, claimed_at = ?3, lease_until = ?4 WHERE id = ?1",
            params![
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
    pub(crate) fn cancel_queued(&self, id: &str) -> Result<bool, Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let now = Utc::now().to_rfc3339();
        let taken = tx.execute(
            "UPDATE runs SET status = 'canceled', finished_at = ?2,
                 error = COALESCE(error, 'canceled before it started')
             WHERE id = ?1 AND status = 'queued' AND claimed_by IS NULL",
            params![id, now],
        )?;
        if taken == 0 {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute(
            "UPDATE op_runs SET status = 'canceled', error = 'canceled before it started',
                 finished_at = ?2
             WHERE run_id = ?1 AND status = 'pending'",
            params![id, now],
        )?;
        tx.execute(
            "INSERT INTO events (run_id, op, level, kind, message, ts)
             VALUES (?1, NULL, 'warn', 'run_canceled', 'canceled before it started', ?2)",
            params![id, now],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub(crate) fn run_started(&self, id: &str) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE runs SET status = ?1, started_at = ?2 WHERE id = ?3",
            params![RunStatus::Running.as_str(), Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    /// `error` is the run's own failure summary: the first op that terminally
    /// failed, named. `None` leaves any existing value alone.
    pub(crate) fn run_finished(
        &self,
        id: &str,
        status: RunStatus,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        // and the lease with it: there is nothing left to renew, and a run that
        // is over must never look reclaimable
        conn.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2, error = COALESCE(?3, error),
                 lease_until = NULL
             WHERE id = ?4",
            params![status.as_str(), Utc::now().to_rfc3339(), error, id],
        )?;
        Ok(())
    }

    pub(crate) fn op_started(&self, run_id: &str, op: &str, attempts: u32) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        // coalesce so retries keep the first attempt's start time. the finish
        // and the error are cleared: a fresh attempt has neither, and an
        // isolated op's child records its failure on this row before the parent
        // decides to retry it
        conn.execute(
            "UPDATE op_runs SET status = ?1, attempts = ?2, started_at = COALESCE(started_at, ?3),
                 finished_at = NULL, error = NULL
             WHERE run_id = ?4 AND op = ?5",
            params![
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
    pub(crate) fn op_finished(
        &self,
        run_id: &str,
        op: &str,
        status: OpStatus,
        output: Option<&Value>,
        metadata: Option<&Value>,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        // pid goes with it: the row says where an op is running, and nothing is
        // running once this write lands
        conn.execute(
            "UPDATE op_runs SET status = ?1, finished_at = ?2, output = ?3, metadata = ?4,
                 error = ?5, pid = NULL
             WHERE run_id = ?6 AND op = ?7",
            params![
                status.as_str(),
                Utc::now().to_rfc3339(),
                output.map(|v| v.to_string()),
                metadata.map(|v| v.to_string()),
                error,
                run_id,
                op,
            ],
        )?;
        Ok(())
    }

    /// mark an op canceled without claiming a finish time: cancellation was
    /// requested and the task never joined, so when — or whether — the work
    /// stopped is exactly what this process does not know.
    pub(crate) fn op_unstopped(&self, run_id: &str, op: &str, error: &str) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE op_runs SET status = ?1, finished_at = NULL, output = NULL, metadata = NULL,
                 error = ?2, pid = NULL
             WHERE run_id = ?3 AND op = ?4",
            params![OpStatus::Canceled.as_str(), error, run_id, op],
        )?;
        Ok(())
    }

    /// record the child process an [isolated](crate::Op::isolated) op is
    /// running in.
    ///
    /// guarded on `running`, because a fast child can record its own terminal
    /// row before the parent gets here — and a pid written onto a finished op
    /// would name a process that no longer exists.
    pub(crate) fn op_spawned(&self, run_id: &str, op: &str, pid: u32) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE op_runs SET pid = ?1 WHERE run_id = ?2 AND op = ?3 AND status = 'running'",
            params![i64::from(pid), run_id, op],
        )?;
        Ok(())
    }

    /// record what an isolated op is being handed, before the child that reads
    /// it exists. see the `op_runs.inputs` note on the v13 migration.
    pub(crate) fn set_op_inputs(
        &self,
        run_id: &str,
        op: &str,
        inputs: &Value,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE op_runs SET inputs = ?1 WHERE run_id = ?2 AND op = ?3",
            params![inputs.to_string(), run_id, op],
        )?;
        Ok(())
    }

    /// what the parent recorded for this op, read by the child that runs it.
    pub(crate) fn op_inputs(&self, run_id: &str, op: &str) -> Result<Option<Value>, Error> {
        let conn = self.0.lock().unwrap();
        let inputs = conn
            .query_row(
                "SELECT inputs FROM op_runs WHERE run_id = ?1 AND op = ?2",
                params![run_id, op],
                |r| opt_json_col(r, 0),
            )
            .optional()?;
        Ok(inputs.flatten())
    }

    pub(crate) fn append_event(
        &self,
        run_id: &str,
        op: Option<&str>,
        level: EventLevel,
        kind: EventKind,
        message: &str,
        data: Option<&Value>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO events (run_id, op, level, kind, message, data, ts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                run_id,
                op,
                level.as_str(),
                kind.as_str(),
                message,
                data.map(|v| v.to_string()),
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    /// make the schedules table mirror the code: insert new (job, expr) pairs,
    /// refresh tz and params on existing ones (pause state survives), drop the
    /// rest.
    pub(crate) fn sync_schedules(&self, defined: &[Schedule]) -> Result<(), Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut insert = tx.prepare(
                "INSERT OR IGNORE INTO schedules (job, expr, tz, params, catchup)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            // the cursor is deliberately not touched: it is what the scheduler
            // knows about this pair, and a restart that rewrote it would be a
            // restart that forgot the downtime it is meant to detect
            let mut update = tx.prepare(
                "UPDATE schedules SET tz = ?3, params = ?4, catchup = ?5
                 WHERE job = ?1 AND expr = ?2",
            )?;
            for s in defined {
                let declared = s.params.to_string();
                let catchup = s.catchup.to_string();
                insert.execute(params![s.job, s.expr, s.tz, declared, catchup])?;
                update.execute(params![s.job, s.expr, s.tz, declared, catchup])?;
            }
        }
        let existing: Vec<(String, String)> = {
            let mut stmt = tx.prepare("SELECT job, expr FROM schedules")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let keep: HashSet<(&str, &str)> = defined
            .iter()
            .map(|s| (s.job.as_str(), s.expr.as_str()))
            .collect();
        for (job, expr) in &existing {
            if !keep.contains(&(job.as_str(), expr.as_str())) {
                tx.execute(
                    "DELETE FROM schedules WHERE job = ?1 AND expr = ?2",
                    params![job, expr],
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn schedules(&self) -> Result<Vec<ScheduleRow>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT job, expr, tz, paused, params, catchup, cursor
             FROM schedules ORDER BY job, expr",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(ScheduleRow {
                job: r.get(0)?,
                expr: r.get(1)?,
                tz: r.get(2)?,
                paused: r.get(3)?,
                params: json_col(r, 4)?,
                catchup: parse_col(r, 5)?,
                cursor: opt_ts_col(r, 6)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// returns false if the (job, expr) pair isn't registered.
    pub fn set_schedule_paused(&self, job: &str, expr: &str, paused: bool) -> Result<bool, Error> {
        let conn = self.0.lock().unwrap();
        let n = conn.execute(
            "UPDATE schedules SET paused = ?3 WHERE job = ?1 AND expr = ?2",
            params![job, expr, paused],
        )?;
        Ok(n > 0)
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
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE schedules SET cursor = ?3
             WHERE job = ?1 AND expr = ?2 AND (cursor IS NULL OR cursor < ?3)",
            params![job, expr, at.to_rfc3339()],
        )?;
        Ok(())
    }

    /// every fire still waiting to launch, oldest occurrence first: a
    /// `deferred` tick with no later tick for the same `(job, expr,
    /// scheduled_for)`. the tick log *is* the queue — a fire held in memory
    /// dies with the process, and this one does not.
    pub(crate) fn pending_fires(&self) -> Result<Vec<(String, String, DateTime<Utc>)>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT job, expr, scheduled_for FROM schedule_ticks d
             WHERE d.outcome = 'deferred'
               AND NOT EXISTS (
                   SELECT 1 FROM schedule_ticks f
                   WHERE f.job = d.job AND f.expr = d.expr
                     AND f.scheduled_for = d.scheduled_for AND f.id > d.id
               )
             ORDER BY d.scheduled_for, d.id",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, ts_col(r, 2)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// every [preset](Preset) stored for `job`, by name.
    pub fn presets(&self, job: &str) -> Result<Vec<Preset>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT job, name, params, created_at FROM presets WHERE job = ?1 ORDER BY name",
        )?;
        let rows = stmt.query_map(params![job], preset_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn preset(&self, job: &str, name: &str) -> Result<Option<Preset>, Error> {
        let conn = self.0.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT job, name, params, created_at FROM presets WHERE job = ?1 AND name = ?2",
                params![job, name],
                preset_from_row,
            )
            .optional()?;
        Ok(row)
    }

    /// store `params` under `(job, name)`, replacing whatever was there.
    ///
    /// an upsert rather than an insert because a declared preset is seeded on
    /// every start: the code that declares one owns its params, and a preset
    /// the launchpad made under another name is nobody else's business.
    /// `created_at` survives the rewrite, so a preset's age means when it first
    /// appeared rather than when the process last booted.
    pub fn put_preset(&self, job: &str, name: &str, params: &Value) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO presets (job, name, params, created_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (job, name) DO UPDATE SET params = excluded.params",
            params![job, name, params.to_string(), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// returns false when there was no such preset.
    pub fn delete_preset(&self, job: &str, name: &str) -> Result<bool, Error> {
        let conn = self.0.lock().unwrap();
        let n = conn.execute(
            "DELETE FROM presets WHERE job = ?1 AND name = ?2",
            params![job, name],
        )?;
        Ok(n > 0)
    }

    pub(crate) fn prune_ticks(&self, keep: usize) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "DELETE FROM schedule_ticks WHERE id NOT IN
             (SELECT id FROM schedule_ticks ORDER BY id DESC LIMIT ?1)",
            params![keep as i64],
        )?;
        Ok(())
    }

    /// delete terminal runs created before `older_than`, with their op_runs and
    /// events. active runs survive at any age, and op_state is never touched.
    pub(crate) fn prune_runs(&self, older_than: DateTime<Utc>) -> Result<usize, Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        let cutoff = older_than.to_rfc3339();
        const DOOMED: &str = "SELECT id FROM runs
             WHERE status IN ('success', 'failed', 'canceled') AND created_at < ?1";
        // children first: the transaction should make it moot, the order makes it true anyway
        tx.execute(
            &format!("DELETE FROM op_runs WHERE run_id IN ({DOOMED})"),
            params![cutoff],
        )?;
        tx.execute(
            &format!("DELETE FROM events WHERE run_id IN ({DOOMED})"),
            params![cutoff],
        )?;
        let removed = tx.execute(
            "DELETE FROM runs
             WHERE status IN ('success', 'failed', 'canceled') AND created_at < ?1",
            params![cutoff],
        )?;
        tx.commit()?;
        Ok(removed)
    }

    pub(crate) fn record_tick(
        &self,
        job: &str,
        expr: &str,
        scheduled_for: DateTime<Utc>,
        outcome: TickOutcome,
        run_id: Option<&str>,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO schedule_ticks (job, expr, scheduled_for, fired_at, outcome, run_id, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                job,
                expr,
                scheduled_for.to_rfc3339(),
                Utc::now().to_rfc3339(),
                outcome.as_str(),
                run_id,
                error
            ],
        )?;
        Ok(())
    }

    pub fn ticks(&self, job: Option<&str>, limit: u32) -> Result<Vec<Tick>, Error> {
        let conn = self.0.lock().unwrap();
        let rows = match job {
            Some(job) => {
                let mut stmt = conn.prepare(
                    "SELECT id, job, expr, scheduled_for, fired_at, outcome, run_id, error
                     FROM schedule_ticks WHERE job = ?1 ORDER BY id DESC LIMIT ?2",
                )?;
                let rows = stmt.query_map(params![job, limit], tick_from_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
            None => {
                let mut stmt = conn.prepare(
                    "SELECT id, job, expr, scheduled_for, fired_at, outcome, run_id, error
                     FROM schedule_ticks ORDER BY id DESC LIMIT ?1",
                )?;
                let rows = stmt.query_map(params![limit], tick_from_row)?;
                rows.collect::<Result<Vec<_>, _>>()?
            }
        };
        Ok(rows)
    }

    pub fn run(&self, id: &str) -> Result<Option<Run>, Error> {
        let conn = self.0.lock().unwrap();
        let run = conn
            .query_row(
                &format!("SELECT {RUN_COLS} FROM runs WHERE id = ?1"),
                params![id],
                run_from_row,
            )
            .optional()?;
        Ok(run)
    }

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
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            r#"SELECT {RUN_COLS}
               FROM runs
               WHERE (?1 IS NULL OR job = ?1) AND (?2 IS NULL OR created_at >= ?2)
                 AND (?3 IS NULL OR created_at < ?3
                      OR (?4 IS NOT NULL AND created_at = ?3 AND id < ?4))
                 AND (?5 IS NULL OR EXISTS (
                      SELECT 1 FROM json_each(runs.tags)
                      WHERE json_each.key = ?5 AND json_each.value = ?6))
               ORDER BY created_at DESC, id DESC LIMIT ?7"#
        ))?;
        let since = since.map(|t| t.to_rfc3339());
        let before = before.map(|t| t.to_rfc3339());
        let (key, value) = (tag.map(|t| t.0), tag.map(|t| t.1));
        let rows = stmt.query_map(
            params![job, since, before, before_id, key, value, limit],
            run_from_row,
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            r#"SELECT {RUN_COLS}
               FROM runs
               WHERE status IN ('success', 'failed', 'canceled') AND finished_at IS NOT NULL
                 AND (?1 IS NULL OR job = ?1)
                 AND (?2 IS NULL OR finished_at > ?2 OR (finished_at = ?2 AND id > ?3))
               ORDER BY finished_at, id LIMIT ?4"#
        ))?;
        let at = after.map(|c| c.finished_at.to_rfc3339());
        let id = after.map(|c| c.id.as_str());
        let rows = stmt.query_map(params![job, at, id, limit], run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// the newest terminal run as a cursor, for a sensor starting from now
    /// rather than from the whole history it was added to.
    pub(crate) fn latest_terminal_run(
        &self,
        job: Option<&str>,
    ) -> Result<Option<RunCursor>, Error> {
        let conn = self.0.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT finished_at, id FROM runs
                 WHERE status IN ('success', 'failed', 'canceled') AND finished_at IS NOT NULL
                   AND (?1 IS NULL OR job = ?1)
                 ORDER BY finished_at DESC, id DESC LIMIT 1",
                params![job],
                |r| {
                    Ok(RunCursor {
                        finished_at: ts_col(r, 0)?,
                        id: r.get(1)?,
                    })
                },
            )
            .optional()?;
        Ok(row)
    }

    /// finish time of the job's most recent successful run.
    pub fn last_success(&self, job: &str) -> Result<Option<DateTime<Utc>>, Error> {
        let conn = self.0.lock().unwrap();
        let ts = conn.query_row(
            "SELECT MAX(finished_at) FROM runs WHERE job = ?1 AND status = 'success'",
            params![job],
            |r| opt_ts_col(r, 0),
        )?;
        Ok(ts)
    }

    pub fn op_runs(&self, run_id: &str) -> Result<Vec<OpRun>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT run_id, op, status, attempts, started_at, finished_at, output, metadata, error,
                    pid
             FROM op_runs WHERE run_id = ?1 ORDER BY op",
        )?;
        let rows = stmt.query_map(params![run_id], op_run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// one op's row. the parent of an isolated op reads back what its child
    /// recorded through this — the whole of what a worker process reports.
    pub fn op_run(&self, run_id: &str, op: &str) -> Result<Option<OpRun>, Error> {
        let conn = self.0.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT run_id, op, status, attempts, started_at, finished_at, output, metadata,
                        error, pid
                 FROM op_runs WHERE run_id = ?1 AND op = ?2",
                params![run_id, op],
                op_run_from_row,
            )
            .optional()?;
        Ok(row)
    }

    /// op_run rows across the job's most recent `runs` runs, newest run first.
    pub fn recent_op_runs(&self, job: &str, runs: u32) -> Result<Vec<OpRun>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT o.run_id, o.op, o.status, o.attempts, o.started_at, o.finished_at, o.output,
                    o.metadata, o.error, o.pid
             FROM op_runs o
             JOIN (SELECT id, created_at FROM runs WHERE job = ?1
                   ORDER BY created_at DESC LIMIT ?2) r ON r.id = o.run_id
             ORDER BY r.created_at DESC",
        )?;
        let rows = stmt.query_map(params![job, runs], op_run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT op, metadata FROM (
                 SELECT o.op AS op, o.metadata AS metadata,
                        ROW_NUMBER() OVER (
                            PARTITION BY o.op ORDER BY r.created_at DESC, r.id DESC
                        ) AS rn
                 FROM op_runs o JOIN runs r ON r.id = o.run_id
                 WHERE r.job = ?1 AND o.metadata IS NOT NULL
                   AND (r.created_at < ?2 OR (r.created_at = ?2 AND r.id < ?3))
             ) WHERE rn = 1",
        )?;
        let rows = stmt.query_map(params![job, before.to_rfc3339(), run_id], |r| {
            Ok((r.get(0)?, json_col(r, 1)?))
        })?;
        Ok(rows.collect::<Result<HashMap<_, _>, _>>()?)
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
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT o.run_id, o.metadata, COALESCE(o.finished_at, r.created_at)
             FROM op_runs o JOIN runs r ON r.id = o.run_id
             WHERE r.job = ?1 AND o.op = ?2 AND o.metadata IS NOT NULL
             ORDER BY r.created_at DESC, r.id DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![job, op, limit], |r| {
            Ok((json_col(r, 1)?, ts_col(r, 2)?, r.get::<_, String>(0)?))
        })?;
        let mut points = Vec::new();
        for row in rows {
            let (metadata, at, run_id) = row?;
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
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT run_id, metadata, built_at FROM asset_materializations
             WHERE asset = ?1 AND (?2 IS NULL OR partition IS ?2) AND metadata IS NOT NULL
             ORDER BY id DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![asset, partition, limit], |r| {
            Ok((
                json_col(r, 1)?,
                ts_col(r, 2)?,
                r.get::<_, Option<String>>(0)?,
            ))
        })?;
        let mut points = Vec::new();
        for row in rows {
            let (metadata, at, run_id) = row?;
            if let Some(value) = op::numeric_key(&metadata, key) {
                points.push(MetaPoint { at, value, run_id });
            }
        }
        points.reverse();
        Ok(points)
    }

    /// the state an op's last successful run committed, if any.
    pub fn op_state(&self, job: &str, op: &str) -> Result<Option<Value>, Error> {
        let conn = self.0.lock().unwrap();
        let value = conn
            .query_row(
                "SELECT value FROM op_state WHERE job = ?1 AND op = ?2",
                params![job, op],
                |r| json_col(r, 0),
            )
            .optional()?;
        Ok(value)
    }

    pub(crate) fn set_op_state(&self, job: &str, op: &str, value: &Value) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO op_state (job, op, value, updated_at) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (job, op) DO UPDATE SET value = ?3, updated_at = ?4",
            params![job, op, value.to_string(), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// every op state a job carries, ordered by op.
    pub fn job_states(&self, job: &str) -> Result<Vec<(String, Value, DateTime<Utc>)>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT op, value, updated_at FROM op_state WHERE job = ?1 ORDER BY op")?;
        let rows = stmt.query_map(params![job], |r| {
            Ok((r.get(0)?, json_col(r, 1)?, ts_col(r, 2)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn events(&self, run_id: &str, after: i64) -> Result<Vec<Event>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq, run_id, op, level, kind, message, data, ts
             FROM events WHERE run_id = ?1 AND seq > ?2 ORDER BY seq",
        )?;
        let rows = stmt.query_map(params![run_id, after], event_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// append a materialization. the table is history, so a rebuild that came
    /// out fingerprint-identical is still an entry — that a build happened and
    /// that it changed anything are different facts.
    ///
    /// `partition` is the key one build of a [partitioned
    /// asset](crate::Partitions) produced, and `None` for every unpartitioned
    /// one: history, staleness and seeding are all per `(asset, partition)`.
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
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO asset_materializations
                 (asset, partition, fingerprint, inputs, value, run_id, built_at, metadata)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                asset,
                partition,
                fingerprint,
                inputs.to_string(),
                value.map(|v| v.to_string()),
                run_id,
                Utc::now().to_rfc3339(),
                metadata.map(|v| v.to_string()),
            ],
        )?;
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
        let conn = self.0.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at, metadata
                 FROM asset_materializations WHERE asset = ?1 AND partition IS ?2
                 ORDER BY id DESC LIMIT 1",
                params![asset, partition],
                materialization_from_row,
            )
            .optional()?;
        Ok(row)
    }

    /// the current materialization of every `(asset, partition)` pair, one row
    /// each, ordered by asset then partition.
    pub fn latest_materializations(&self) -> Result<Vec<Materialization>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at, metadata
             FROM asset_materializations
             WHERE id IN
                 (SELECT MAX(id) FROM asset_materializations GROUP BY asset, partition)
             ORDER BY asset, partition",
        )?;
        let rows = stmt.query_map([], materialization_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
        let conn = self.0.lock().unwrap();
        // both look one row back within the partition: one key's rebuild says
        // nothing about whether another key's fingerprint or row count moved
        let mut stmt = conn.prepare(
            "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                    metadata, changed, previous_metadata FROM (
                 SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                        metadata,
                        fingerprint IS NOT
                            LAG(fingerprint) OVER (PARTITION BY partition ORDER BY id)
                            AS changed,
                        LAG(metadata) OVER (PARTITION BY partition ORDER BY id)
                            AS previous_metadata
                 FROM asset_materializations
                 WHERE asset = ?1 AND (?2 IS NULL OR partition IS ?2)
             ) ORDER BY id DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![asset, partition, limit], |r| {
            Ok(HistoryEntry {
                mat: materialization_from_row(r)?,
                changed: r.get(9)?,
                previous_metadata: opt_json_col(r, 10)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// trim every `(asset, partition)` pair's history to its newest `keep`
    /// entries. `keep` is floored at 1: the latest materialization is current
    /// state, not history, and dropping it would read as a partition that has
    /// never been built.
    pub(crate) fn prune_materializations(&self, keep: usize) -> Result<usize, Error> {
        let conn = self.0.lock().unwrap();
        let removed = conn.execute(
            "DELETE FROM asset_materializations WHERE id NOT IN
             (SELECT id FROM asset_materializations AS newest
              WHERE newest.asset = asset_materializations.asset
                AND newest.partition IS asset_materializations.partition
              ORDER BY newest.id DESC LIMIT ?1)",
            params![keep.max(1) as i64],
        )?;
        Ok(removed)
    }

    /// record what a check said. written inside the check's op, before it
    /// decides whether to fail, so a failing error check leaves its verdict
    /// behind rather than only a failed op.
    #[allow(clippy::too_many_arguments)]
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
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO asset_checks
                 (asset, partition, check_name, run_id, status, severity, message,
                  metadata, checked_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                asset,
                partition,
                check,
                run_id,
                status.as_str(),
                severity.as_str(),
                message,
                metadata.map(|v| v.to_string()),
                Utc::now().to_rfc3339(),
            ],
        )?;
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
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, partition, check_name, run_id, status, severity, message,
                    metadata, checked_at
             FROM asset_checks WHERE asset = ?1 AND (?2 IS NULL OR partition IS ?2)
             ORDER BY id DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![asset, partition, limit], asset_check_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// the latest result of every `(asset, partition, check)` triple, ordered
    /// by all three.
    pub fn latest_asset_checks(&self) -> Result<Vec<AssetCheckRow>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, partition, check_name, run_id, status, severity, message,
                    metadata, checked_at
             FROM asset_checks
             WHERE id IN
                 (SELECT MAX(id) FROM asset_checks GROUP BY asset, partition, check_name)
             ORDER BY asset, partition, check_name",
        )?;
        let rows = stmt.query_map([], asset_check_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// trim every check to its newest `keep` results per partition, floored at
    /// 1 like [`prune_materializations`](Self::prune_materializations) — the
    /// latest result is what the asset summary counts.
    pub(crate) fn prune_asset_checks(&self, keep: usize) -> Result<usize, Error> {
        let conn = self.0.lock().unwrap();
        let removed = conn.execute(
            "DELETE FROM asset_checks WHERE id NOT IN
             (SELECT id FROM asset_checks AS newest
              WHERE newest.asset = asset_checks.asset
                AND newest.partition IS asset_checks.partition
                AND newest.check_name = asset_checks.check_name
              ORDER BY newest.id DESC LIMIT ?1)",
            params![keep.max(1) as i64],
        )?;
        Ok(removed)
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
    ) -> Result<i64, Error> {
        let conn = self.0.lock().unwrap();
        // a range that resolved to nothing is complete the moment it is made,
        // which is a truer record than refusing to write one
        let (status, finished) = match keys.is_empty() {
            true => (BackfillStatus::Complete, Some(Utc::now().to_rfc3339())),
            false => (BackfillStatus::Running, None),
        };
        conn.execute(
            "INSERT INTO backfills
                 (asset, from_key, to_key, partition_keys, total, created_at, finished_at, status)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                asset,
                from_key,
                to_key,
                serde_json::to_string(keys).unwrap_or_else(|_| "[]".into()),
                keys.len() as i64,
                Utc::now().to_rfc3339(),
                finished,
                status.as_str(),
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn backfill(&self, id: i64) -> Result<Option<Backfill>, Error> {
        let conn = self.0.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id, asset, from_key, to_key, partition_keys, run_ids, total, launched,
                        created_at, finished_at, status
                 FROM backfills WHERE id = ?1",
                params![id],
                backfill_from_row,
            )
            .optional()?;
        Ok(row)
    }

    /// recent backfills, newest first.
    pub fn backfills(&self, limit: u32) -> Result<Vec<Backfill>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, from_key, to_key, partition_keys, run_ids, total, launched,
                    created_at, finished_at, status
             FROM backfills ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], backfill_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// every backfill still going, oldest first — what the loop that chunks
    /// them reads, and what makes a second one for the same asset a conflict.
    pub(crate) fn running_backfills(&self) -> Result<Vec<Backfill>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, asset, from_key, to_key, partition_keys, run_ids, total, launched,
                    created_at, finished_at, status
             FROM backfills WHERE status = ?1 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![BackfillStatus::Running.as_str()], backfill_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// record that a chunk went out: its run, and how many keys are now
    /// launched in total.
    pub(crate) fn backfill_launched(
        &self,
        id: i64,
        run_id: &str,
        launched: usize,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE backfills
             SET launched = ?2,
                 run_ids = json_insert(run_ids, '$[#]', ?3)
             WHERE id = ?1",
            params![id, launched as i64, run_id],
        )?;
        Ok(())
    }

    /// close a backfill. the first terminal status wins, so a cancel racing
    /// the chunker cannot be overwritten by what the run did next.
    pub(crate) fn finish_backfill(&self, id: i64, status: BackfillStatus) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE backfills SET status = ?2, finished_at = ?3 WHERE id = ?1 AND status = ?4",
            params![
                id,
                status.as_str(),
                Utc::now().to_rfc3339(),
                BackfillStatus::Running.as_str(),
            ],
        )?;
        Ok(())
    }

    /// what the last freshness check concluded about everything it has ever
    /// seen, ordered by kind then name.
    pub fn freshness_states(&self) -> Result<Vec<FreshnessRow>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT kind, name, late, since FROM freshness_state ORDER BY kind, name")?;
        let rows = stmt.query_map([], |r| {
            Ok(FreshnessRow {
                kind: r.get(0)?,
                name: r.get(1)?,
                late: r.get(2)?,
                since: opt_ts_col(r, 3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO freshness_state (kind, name, late, since) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (kind, name) DO UPDATE SET late = ?3, since = ?4",
            params![kind, name, late, since.map(|t| t.to_rfc3339())],
        )?;
        Ok(())
    }

    /// make the sensors table mirror the code: insert new names, drop the
    /// rest. existing rows keep their paused flag and cursor.
    pub(crate) fn sync_sensors(&self, defined: &[String]) -> Result<(), Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut insert =
                tx.prepare("INSERT OR IGNORE INTO sensors (name, updated_at) VALUES (?1, ?2)")?;
            let now = Utc::now().to_rfc3339();
            for name in defined {
                insert.execute(params![name, now])?;
            }
        }
        let existing: Vec<String> = {
            let mut stmt = tx.prepare("SELECT name FROM sensors")?;
            let rows = stmt.query_map([], |r| r.get(0))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let keep: HashSet<&str> = defined.iter().map(String::as_str).collect();
        for name in &existing {
            if !keep.contains(name.as_str()) {
                tx.execute("DELETE FROM sensors WHERE name = ?1", params![name])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn sensors(&self) -> Result<Vec<SensorRow>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT name, paused, cursor, updated_at FROM sensors ORDER BY name")?;
        let rows = stmt.query_map([], |r| {
            Ok(SensorRow {
                name: r.get(0)?,
                paused: r.get(1)?,
                cursor: opt_json_col(r, 2)?,
                updated_at: ts_col(r, 3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// returns false if no sensor with that name is registered.
    pub fn set_sensor_paused(&self, name: &str, paused: bool) -> Result<bool, Error> {
        let conn = self.0.lock().unwrap();
        let n = conn.execute(
            "UPDATE sensors SET paused = ?2 WHERE name = ?1",
            params![name, paused],
        )?;
        Ok(n > 0)
    }

    pub(crate) fn set_sensor_cursor(&self, name: &str, cursor: &Value) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE sensors SET cursor = ?2, updated_at = ?3 WHERE name = ?1",
            params![name, cursor.to_string(), Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// `skipped` counts the requests this evaluation did not launch because
    /// their run key was already claimed — distinct from launching nothing —
    /// and `duration_ms` is how long the evaluation took, which is the other
    /// half of "is this sensor healthy".
    pub(crate) fn record_sensor_tick(
        &self,
        sensor: &str,
        outcome: SensorOutcome,
        launched: u32,
        skipped: u32,
        duration_ms: u64,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO sensor_ticks
                 (sensor, evaluated_at, outcome, launched, skipped, duration_ms, error)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                sensor,
                Utc::now().to_rfc3339(),
                outcome.as_str(),
                launched,
                skipped,
                duration_ms,
                error
            ],
        )?;
        Ok(())
    }

    pub fn sensor_ticks(&self, sensor: Option<&str>, limit: u32) -> Result<Vec<SensorTick>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, sensor, evaluated_at, outcome, launched, skipped, duration_ms, error
             FROM sensor_ticks WHERE (?1 IS NULL OR sensor = ?1)
             ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![sensor, limit], sensor_tick_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// move a finished run's finish time. tests that need history older than
    /// the test itself have no other way to make one, and freshness is entirely
    /// about how old a success is.
    #[cfg(test)]
    pub(crate) fn backdate_run(&self, id: &str, finished_at: DateTime<Utc>) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE runs SET finished_at = ?2 WHERE id = ?1",
            params![id, finished_at.to_rfc3339()],
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
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE asset_materializations SET built_at = ?3
             WHERE id = (SELECT MAX(id) FROM asset_materializations
                         WHERE asset = ?1 AND partition IS ?2)",
            params![asset, partition, built_at.to_rfc3339()],
        )?;
        Ok(())
    }

    /// drop a table, so that every read touching it fails. a test proving a
    /// control-plane read fails closed has no other way to break one.
    #[cfg(test)]
    pub(crate) fn drop_table(&self, name: &str) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute_batch(&format!("DROP TABLE {name}"))?;
        Ok(())
    }

    pub(crate) fn prune_sensor_ticks(&self, keep: usize) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "DELETE FROM sensor_ticks WHERE id NOT IN
             (SELECT id FROM sensor_ticks ORDER BY id DESC LIMIT ?1)",
            params![keep as i64],
        )?;
        Ok(())
    }
}

/// what is executing right now, counted against `limits`. "executing" is
/// claimed and not finished — the set a concurrency limit is about.
fn in_flight(conn: &Connection, limits: &Limits) -> Result<InFlight, Error> {
    let mut counts = InFlight::new();
    let mut stmt = conn.prepare(
        "SELECT job, tags FROM runs
         WHERE claimed_by IS NOT NULL AND status IN ('queued', 'running')",
    )?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, tags_from_col(r, 1)?)))?;
    for row in rows {
        let (job, tags) = row?;
        counts.take(limits, &job, &tags);
    }
    Ok(counts)
}

/// the queue itself: runs nobody has claimed, in the order a dispatcher takes
/// them, with the plan each would execute.
fn queued(conn: &Connection, limit: u32) -> Result<Vec<(Run, Option<Value>)>, Error> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {RUN_COLS}, plan FROM runs
         WHERE status = 'queued' AND claimed_by IS NULL
         ORDER BY priority DESC, created_at, id LIMIT ?1"
    ))?;
    let rows = stmt.query_map(params![limit], |r| {
        Ok((run_from_row(r)?, opt_json_col(r, 16)?))
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn run_from_row(row: &Row) -> rusqlite::Result<Run> {
    Ok(Run {
        id: row.get(0)?,
        job: row.get(1)?,
        status: parse_col(row, 2)?,
        trigger: parse_col(row, 3)?,
        params: json_col(row, 4)?,
        created_at: ts_col(row, 5)?,
        started_at: opt_ts_col(row, 6)?,
        finished_at: opt_ts_col(row, 7)?,
        resumed_from: row.get(8)?,
        error: row.get(9)?,
        scheduled_for: opt_ts_col(row, 10)?,
        tags: tags_from_col(row, 11)?,
        priority: row.get(12)?,
        claimed_by: row.get(13)?,
        claimed_at: opt_ts_col(row, 14)?,
        lease_until: opt_ts_col(row, 15)?,
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
fn tags_from_col(row: &Row, idx: usize) -> rusqlite::Result<RunTags> {
    match row.get::<_, Option<String>>(idx)? {
        Some(s) => Ok(serde_json::from_str(&s).unwrap_or_default()),
        None => Ok(RunTags::new()),
    }
}

fn op_run_from_row(row: &Row) -> rusqlite::Result<OpRun> {
    Ok(OpRun {
        run_id: row.get(0)?,
        op: row.get(1)?,
        status: parse_col(row, 2)?,
        attempts: row.get(3)?,
        started_at: opt_ts_col(row, 4)?,
        finished_at: opt_ts_col(row, 5)?,
        output: opt_json_col(row, 6)?,
        metadata: opt_json_col(row, 7)?,
        error: row.get(8)?,
        pid: row.get(9)?,
    })
}

fn preset_from_row(row: &Row) -> rusqlite::Result<Preset> {
    Ok(Preset {
        job: row.get(0)?,
        name: row.get(1)?,
        params: json_col(row, 2)?,
        created_at: ts_col(row, 3)?,
    })
}

fn event_from_row(row: &Row) -> rusqlite::Result<Event> {
    Ok(Event {
        seq: row.get(0)?,
        run_id: row.get(1)?,
        op: row.get(2)?,
        level: parse_col(row, 3)?,
        kind: parse_col(row, 4)?,
        message: row.get(5)?,
        data: opt_json_col(row, 6)?,
        ts: ts_col(row, 7)?,
    })
}

fn materialization_from_row(row: &Row) -> rusqlite::Result<Materialization> {
    Ok(Materialization {
        id: row.get(0)?,
        asset: row.get(1)?,
        partition: row.get(2)?,
        fingerprint: row.get(3)?,
        inputs: json_col(row, 4)?,
        value: opt_json_col(row, 5)?,
        run_id: row.get(6)?,
        built_at: ts_col(row, 7)?,
        metadata: opt_json_col(row, 8)?,
    })
}

fn asset_check_from_row(row: &Row) -> rusqlite::Result<AssetCheckRow> {
    Ok(AssetCheckRow {
        id: row.get(0)?,
        asset: row.get(1)?,
        partition: row.get(2)?,
        check: row.get(3)?,
        run_id: row.get(4)?,
        status: parse_col(row, 5)?,
        severity: parse_col(row, 6)?,
        message: row.get(7)?,
        metadata: opt_json_col(row, 8)?,
        checked_at: ts_col(row, 9)?,
    })
}

fn backfill_from_row(row: &Row) -> rusqlite::Result<Backfill> {
    let list = |idx: usize| -> rusqlite::Result<Vec<String>> {
        let text: String = row.get(idx)?;
        serde_json::from_str(&text).map_err(|e| conv_err(idx, e))
    };
    Ok(Backfill {
        id: row.get(0)?,
        asset: row.get(1)?,
        from_key: row.get(2)?,
        to_key: row.get(3)?,
        partitions: list(4)?,
        run_ids: list(5)?,
        total: row.get::<_, i64>(6)? as usize,
        launched: row.get::<_, i64>(7)? as usize,
        created_at: ts_col(row, 8)?,
        finished_at: opt_ts_col(row, 9)?,
        status: parse_col(row, 10)?,
    })
}

fn sensor_tick_from_row(row: &Row) -> rusqlite::Result<SensorTick> {
    Ok(SensorTick {
        id: row.get(0)?,
        sensor: row.get(1)?,
        evaluated_at: ts_col(row, 2)?,
        outcome: parse_col(row, 3)?,
        launched: row.get(4)?,
        skipped: row.get(5)?,
        duration_ms: row.get(6)?,
        error: row.get(7)?,
    })
}

fn tick_from_row(row: &Row) -> rusqlite::Result<Tick> {
    Ok(Tick {
        id: row.get(0)?,
        job: row.get(1)?,
        expr: row.get(2)?,
        scheduled_for: ts_col(row, 3)?,
        fired_at: ts_col(row, 4)?,
        outcome: parse_col(row, 5)?,
        run_id: row.get(6)?,
        error: row.get(7)?,
    })
}

fn conv_err(idx: usize, e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(idx, rusqlite::types::Type::Text, e.into())
}

fn parse_col<T: FromStr<Err = String>>(row: &Row, idx: usize) -> rusqlite::Result<T> {
    row.get::<_, String>(idx)?
        .parse()
        .map_err(|e: String| conv_err(idx, e))
}

fn ts_col(row: &Row, idx: usize) -> rusqlite::Result<DateTime<Utc>> {
    let s: String = row.get(idx)?;
    DateTime::parse_from_rfc3339(&s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| conv_err(idx, e))
}

fn opt_ts_col(row: &Row, idx: usize) -> rusqlite::Result<Option<DateTime<Utc>>> {
    match row.get::<_, Option<String>>(idx)? {
        Some(s) => DateTime::parse_from_rfc3339(&s)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|e| conv_err(idx, e)),
        None => Ok(None),
    }
}

fn json_col(row: &Row, idx: usize) -> rusqlite::Result<Value> {
    let s: String = row.get(idx)?;
    serde_json::from_str(&s).map_err(|e| conv_err(idx, e))
}

fn opt_json_col(row: &Row, idx: usize) -> rusqlite::Result<Option<Value>> {
    match row.get::<_, Option<String>>(idx)? {
        Some(s) => serde_json::from_str(&s)
            .map(Some)
            .map_err(|e| conv_err(idx, e)),
        None => Ok(None),
    }
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
        }
    }

    #[test]
    fn run_lifecycle_roundtrips() {
        let store = Store::open(":memory:").unwrap();
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

        store.run_started("r1").unwrap();
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
            )
            .unwrap();
        store
            .op_finished("r1", "b", OpStatus::Failed, None, None, Some("boom"))
            .unwrap();
        store.run_finished("r1", RunStatus::Failed, None).unwrap();

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
    }

    #[test]
    fn events_filter_by_seq() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn runs_filter_order_and_limit() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn runs_since_cutoff() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn runs_before_cutoff() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn runs_composite_cursor_pages_ties() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn recent_op_runs_window_and_order() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn prune_runs_cascades_and_keeps_the_rest() {
        let store = Store::open(":memory:").unwrap();
        let old = Utc::now() - chrono::Duration::days(10);
        for (id, status) in [
            ("os", RunStatus::Success),
            ("of", RunStatus::Failed),
            ("oc", RunStatus::Canceled),
        ] {
            store
                .create_run(&mk_run(id, "etl", old), &["a".into()])
                .unwrap();
            store.run_finished(id, status, None).unwrap();
        }
        store
            .create_run(&mk_run("live", "etl", old), &["a".into()])
            .unwrap();
        store.run_started("live").unwrap();
        store
            .create_run(&mk_run("young", "etl", Utc::now()), &["a".into()])
            .unwrap();
        store
            .run_finished("young", RunStatus::Success, None)
            .unwrap();
        store
            .set_op_state("etl", "a", &json!({"cursor": 9}))
            .unwrap();

        let cutoff = Utc::now() - chrono::Duration::days(7);
        assert_eq!(store.prune_runs(cutoff).unwrap(), 3);

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

        assert_eq!(store.prune_runs(cutoff).unwrap(), 0);
    }

    #[test]
    fn active_run_check_tracks_lifecycle() {
        let store = Store::open(":memory:").unwrap();
        assert!(!store.has_active_run("etl").unwrap());
        store
            .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
            .unwrap();
        assert!(store.has_active_run("etl").unwrap());
        store.run_started("r1").unwrap();
        assert!(store.has_active_run("etl").unwrap());
        store.run_finished("r1", RunStatus::Failed, None).unwrap();
        assert!(!store.has_active_run("etl").unwrap());
    }

    #[test]
    fn interrupted_runs_failed_on_startup() {
        let store = Store::open(":memory:").unwrap();
        store
            .create_run(
                &mk_run("dead", "etl", Utc::now()),
                &["a".into(), "b".into()],
            )
            .unwrap();
        store.run_started("dead").unwrap();
        store.op_started("dead", "a", 1).unwrap();

        let done = mk_run("done", "etl", Utc::now());
        store.create_run(&done, &["a".into()]).unwrap();
        store.run_started("done").unwrap();
        store
            .op_finished("done", "a", OpStatus::Success, None, None, None)
            .unwrap();
        store
            .run_finished("done", RunStatus::Success, None)
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
        assert!(!store.set_schedule_paused("etl", "* * * * *", true).unwrap());
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
        phase1_db(path, 15);
        let err = Store::open(path).err().unwrap();
        assert_eq!(err.to_string(), "db schema v15 is newer than this build");
        let conn = Connection::open(path).unwrap();
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 15);
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
            .op_finished("r1", "a", OpStatus::Success, None, None, None)
            .unwrap();
        assert_eq!(store.op_run("r1", "a").unwrap().unwrap().pid, None);
    }

    #[test]
    fn run_tags_round_trip_and_the_filter_matches_exactly() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn presets_are_stored_upserted_and_deleted_per_job() {
        let store = Store::open(":memory:").unwrap();
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
        let store = Store::open(":memory:").unwrap();
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
            .run_finished("r2", RunStatus::Failed, Some("op a failed: boom"))
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
        let store = Store::open(":memory:").unwrap();
        store
            .create_run(&mk_run("r1", "etl", Utc::now()), &["a".into()])
            .unwrap();
        store
            .run_finished("r1", RunStatus::Failed, Some("op a failed: boom"))
            .unwrap();
        // None must not blank an error a caller already recorded
        store.run_finished("r1", RunStatus::Failed, None).unwrap();
        assert_eq!(
            store.run("r1").unwrap().unwrap().error.as_deref(),
            Some("op a failed: boom")
        );
    }

    #[test]
    fn unstopped_op_keeps_no_finish_time() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn materialization_records_and_latest_wins() {
        let store = Store::open(":memory:").unwrap();
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
    }

    fn record(store: &Store, asset: &str, fp: &str) {
        store
            .record_materialization(asset, None, fp, &json!({}), None, None, None)
            .unwrap();
    }

    #[test]
    fn history_flags_only_real_fingerprint_transitions() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn history_carries_what_the_build_before_it_reported() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn the_previous_metadata_of_an_op_skips_the_runs_that_reported_none() {
        let store = Store::open(":memory:").unwrap();
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
                )
                .unwrap();
        }
        // another job's op of the same name says nothing about this one
        let other = mk_run("x", "elsewhere", at(9));
        store.create_run(&other, &["load".into()]).unwrap();
        store
            .op_finished("x", "load", OpStatus::Success, None, Some(&meta(999)), None)
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
    }

    #[test]
    fn history_prunes_to_the_cap_and_never_drops_the_latest() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn sensor_sync_preserves_paused_and_cursor() {
        let store = Store::open(":memory:").unwrap();
        store
            .sync_sensors(&["watch".into(), "probe:docs".into()])
            .unwrap();
        let rows = store.sensors().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| !r.paused && r.cursor.is_none()));

        assert!(store.set_sensor_paused("watch", true).unwrap());
        assert!(!store.set_sensor_paused("nope", true).unwrap());
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
    }

    #[test]
    fn sensor_ticks_record_filter_and_prune() {
        let store = Store::open(":memory:").unwrap();
        store
            .record_sensor_tick("watch", SensorOutcome::Fired, 2, 1, 12, None)
            .unwrap();
        store
            .record_sensor_tick("watch", SensorOutcome::Error, 0, 0, 4, Some("boom"))
            .unwrap();
        store
            .record_sensor_tick("probe:docs", SensorOutcome::Fired, 0, 0, 0, None)
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
    }

    #[test]
    fn op_state_roundtrip_and_upsert() {
        let store = Store::open(":memory:").unwrap();
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
    }

    #[test]
    fn schedule_sync_and_pause_roundtrip() {
        let store = Store::open(":memory:").unwrap();
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

        assert!(store.set_schedule_paused("etl", "0 * * * *", true).unwrap());
        assert!(!store.set_schedule_paused("etl", "bogus", true).unwrap());

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
    }

    #[test]
    fn ticks_record_and_query() {
        let store = Store::open(":memory:").unwrap();
        let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        store
            .record_tick("etl", "0 * * * *", t0, TickOutcome::Fired, Some("r1"), None)
            .unwrap();
        store
            .record_tick(
                "etl",
                "0 * * * *",
                t0 + chrono::Duration::hours(1),
                TickOutcome::Error,
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
    }
}
