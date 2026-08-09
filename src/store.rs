use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::Value;

use crate::error::Error;
use crate::model::{
    AssetCheckRow, Backfill, BackfillStatus, CheckStatus, Event, EventKind, EventLevel,
    Materialization, OpRun, OpStatus, Run, RunStatus, ScheduleDef, ScheduleRow, SensorOutcome,
    SensorRow, SensorTick, Severity, Tick, TickOutcome,
};

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

const SCHEMA_VERSION: u32 = 9;

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

/// sqlite-backed run history. cheap to clone; safe to share across tasks.
#[derive(Clone)]
pub struct Store(Arc<Mutex<Connection>>);

impl Store {
    /// open (and migrate) the database at `path`; `":memory:"` works too.
    pub fn open(path: &str) -> Result<Store, Error> {
        let mut conn = Connection::open(path)?;
        if path != ":memory:" {
            conn.pragma_update(None, "journal_mode", "wal")?;
        }
        migrate(&mut conn)?;
        Ok(Store(Arc::new(Mutex::new(conn))))
    }

    pub(crate) fn create_run(&self, run: &Run, ops: &[String]) -> Result<(), Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            r#"INSERT INTO runs (id, job, status, "trigger", params, created_at, started_at, finished_at, error, resumed_from)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"#,
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
        Ok(())
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

    /// mark runs left queued/running by a dead process as failed; called at startup.
    pub(crate) fn fail_interrupted(&self) -> Result<(), Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "UPDATE op_runs SET
                 status = CASE status WHEN 'running' THEN 'failed' ELSE 'skipped' END,
                 error = CASE status WHEN 'running' THEN 'interrupted: process exited' ELSE error END,
                 finished_at = ?1
             WHERE status IN ('pending', 'running')
               AND run_id IN (SELECT id FROM runs WHERE status IN ('queued', 'running'))",
            params![now],
        )?;
        tx.execute(
            "INSERT INTO events (run_id, op, level, kind, message, ts)
             SELECT id, NULL, 'error', 'run_failed', 'run interrupted: process exited', ?1
             FROM runs WHERE status IN ('queued', 'running')",
            params![now],
        )?;
        tx.execute(
            "UPDATE runs SET status = 'failed', finished_at = ?1,
                 error = COALESCE(error, 'interrupted: process exited')
             WHERE status IN ('queued', 'running')",
            params![now],
        )?;
        tx.commit()?;
        Ok(())
    }

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
        conn.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2, error = COALESCE(?3, error)
             WHERE id = ?4",
            params![status.as_str(), Utc::now().to_rfc3339(), error, id],
        )?;
        Ok(())
    }

    pub(crate) fn op_started(&self, run_id: &str, op: &str, attempts: u32) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        // coalesce so retries keep the first attempt's start time
        conn.execute(
            "UPDATE op_runs SET status = ?1, attempts = ?2, started_at = COALESCE(started_at, ?3)
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
        conn.execute(
            "UPDATE op_runs SET status = ?1, finished_at = ?2, output = ?3, metadata = ?4, error = ?5
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
                 error = ?2
             WHERE run_id = ?3 AND op = ?4",
            params![OpStatus::Canceled.as_str(), error, run_id, op],
        )?;
        Ok(())
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
    pub(crate) fn sync_schedules(&self, defined: &[ScheduleDef]) -> Result<(), Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut insert = tx.prepare(
                "INSERT OR IGNORE INTO schedules (job, expr, tz, params) VALUES (?1, ?2, ?3, ?4)",
            )?;
            let mut update = tx.prepare(
                "UPDATE schedules SET tz = ?3, params = ?4 WHERE job = ?1 AND expr = ?2",
            )?;
            for (job, expr, tz, declared) in defined {
                let declared = declared.to_string();
                insert.execute(params![job, expr, tz, declared])?;
                update.execute(params![job, expr, tz, declared])?;
            }
        }
        let existing: Vec<(String, String)> = {
            let mut stmt = tx.prepare("SELECT job, expr FROM schedules")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let keep: HashSet<(&str, &str)> = defined
            .iter()
            .map(|(j, e, ..)| (j.as_str(), e.as_str()))
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
        let mut stmt =
            conn.prepare("SELECT job, expr, tz, paused, params FROM schedules ORDER BY job, expr")?;
        let rows = stmt.query_map([], |r| {
            Ok(ScheduleRow {
                job: r.get(0)?,
                expr: r.get(1)?,
                tz: r.get(2)?,
                paused: r.get(3)?,
                params: json_col(r, 4)?,
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
                r#"SELECT id, job, status, "trigger", params, created_at, started_at, finished_at,
                          resumed_from, error
                   FROM runs WHERE id = ?1"#,
                params![id],
                run_from_row,
            )
            .optional()?;
        Ok(run)
    }

    // created_at is always rfc3339 utc, so the comparisons below are plain string
    // ordering; `before_id` only refines `before`, never stands alone
    pub fn runs(
        &self,
        job: Option<&str>,
        since: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        before_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Run>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"SELECT id, job, status, "trigger", params, created_at, started_at, finished_at,
                      resumed_from, error
               FROM runs
               WHERE (?1 IS NULL OR job = ?1) AND (?2 IS NULL OR created_at >= ?2)
                 AND (?3 IS NULL OR created_at < ?3
                      OR (?4 IS NOT NULL AND created_at = ?3 AND id < ?4))
               ORDER BY created_at DESC, id DESC LIMIT ?5"#,
        )?;
        let since = since.map(|t| t.to_rfc3339());
        let before = before.map(|t| t.to_rfc3339());
        let rows = stmt.query_map(params![job, since, before, before_id, limit], run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
            "SELECT run_id, op, status, attempts, started_at, finished_at, output, metadata, error
             FROM op_runs WHERE run_id = ?1 ORDER BY op",
        )?;
        let rows = stmt.query_map(params![run_id], op_run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// op_run rows across the job's most recent `runs` runs, newest run first.
    pub fn recent_op_runs(&self, job: &str, runs: u32) -> Result<Vec<OpRun>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT o.run_id, o.op, o.status, o.attempts, o.started_at, o.finished_at, o.output,
                    o.metadata, o.error
             FROM op_runs o
             JOIN (SELECT id, created_at FROM runs WHERE job = ?1
                   ORDER BY created_at DESC LIMIT ?2) r ON r.id = o.run_id
             ORDER BY r.created_at DESC",
        )?;
        let rows = stmt.query_map(params![job, runs], op_run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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

    /// one asset's history, newest first, each entry paired with whether its
    /// fingerprint differs from the entry before it in time — which is what
    /// turns a list of rebuilds into a list of changes. `partition` narrows it
    /// to one key; `None` is every key of the asset, interleaved by time.
    ///
    /// the comparison runs over the whole history before the limit applies, so
    /// the oldest entry on a page is compared against the entry just off it
    /// rather than reported as a change it isn't. the very first entry has
    /// nothing before it and counts as changed: nothing to something.
    pub fn materializations(
        &self,
        asset: &str,
        partition: Option<&str>,
        limit: u32,
    ) -> Result<Vec<(Materialization, bool)>, Error> {
        let conn = self.0.lock().unwrap();
        // the change flag is per partition: one key's rebuild says nothing
        // about whether another key's fingerprint moved
        let mut stmt = conn.prepare(
            "SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                    metadata, changed FROM (
                 SELECT id, asset, partition, fingerprint, inputs, value, run_id, built_at,
                        metadata,
                        fingerprint IS NOT
                            LAG(fingerprint) OVER (PARTITION BY partition ORDER BY id)
                            AS changed
                 FROM asset_materializations
                 WHERE asset = ?1 AND (?2 IS NULL OR partition IS ?2)
             ) ORDER BY id DESC LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![asset, partition, limit], |r| {
            Ok((materialization_from_row(r)?, r.get(9)?))
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

    pub(crate) fn record_sensor_tick(
        &self,
        sensor: &str,
        outcome: SensorOutcome,
        launched: u32,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO sensor_ticks (sensor, evaluated_at, outcome, launched, error)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                sensor,
                Utc::now().to_rfc3339(),
                outcome.as_str(),
                launched,
                error
            ],
        )?;
        Ok(())
    }

    pub fn sensor_ticks(&self, sensor: Option<&str>, limit: u32) -> Result<Vec<SensorTick>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, sensor, evaluated_at, outcome, launched, error
             FROM sensor_ticks WHERE (?1 IS NULL OR sensor = ?1)
             ORDER BY id DESC LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![sensor, limit], sensor_tick_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
    })
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
        error: row.get(5)?,
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

        let etl = store.runs(Some("etl"), None, None, None, 10).unwrap();
        assert_eq!(
            etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r3", "r1", "r0"]
        );
        assert_eq!(store.runs(None, None, None, None, 2).unwrap().len(), 2);
        assert!(
            store
                .runs(Some("nope"), None, None, None, 10)
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
        let recent = store.runs(None, Some(since), None, None, 10).unwrap();
        assert_eq!(
            recent.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r3", "r2"]
        );

        let etl = store
            .runs(Some("etl"), Some(since), None, None, 10)
            .unwrap();
        assert_eq!(
            etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r3"]
        );

        let future = t0 + chrono::Duration::hours(1);
        assert!(
            store
                .runs(None, Some(future), None, None, 10)
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
        let older = store.runs(None, None, Some(before), None, 10).unwrap();
        assert_eq!(
            older.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r1", "r0"]
        );

        let since = t0 + chrono::Duration::minutes(1);
        let etl = store
            .runs(Some("etl"), Some(since), Some(before), None, 10)
            .unwrap();
        assert_eq!(
            etl.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r1"]
        );

        let page = store.runs(None, None, None, None, 2).unwrap();
        assert_eq!(
            page.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["r3", "r2"]
        );
        let next = store
            .runs(None, None, Some(page[1].created_at), None, 2)
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
                    .runs(None, None, Some(*ts), Some(id.as_str()), 1)
                    .unwrap(),
                None => store.runs(None, None, None, None, 1).unwrap(),
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
        phase1_db(path, 10);
        let err = Store::open(path).err().unwrap();
        assert_eq!(err.to_string(), "db schema v10 is newer than this build");
        let conn = Connection::open(path).unwrap();
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 10);
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
            store.runs(None, None, None, None, 10).unwrap()[0].resumed_from,
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
            store.runs(None, None, None, None, 10).unwrap()[0]
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
            .sync_schedules(&[(
                "etl".into(),
                "0 * * * *".into(),
                "UTC".into(),
                json!({"region": "eu"}),
            )])
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
        assert!(carried[0].1, "a carried row is that asset's first change");

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
            .map(|(m, changed)| (m.fingerprint.as_str(), *changed))
            .collect();
        // newest first; the oldest entry counts as a change from nothing
        assert_eq!(
            seen,
            [("f2", false), ("f2", true), ("f1", false), ("f1", true)]
        );
        assert!(history.windows(2).all(|w| w[0].0.id > w[1].0.id));

        // a page's oldest entry is compared with the entry just off it, not
        // reported as a change because the window cut its predecessor away
        let page = store.materializations("stats", None, 3).unwrap();
        assert_eq!(page.len(), 3);
        assert!(!page[2].1, "the page edge invented a change");
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
            .map(|(m, _)| m.fingerprint)
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
            .record_sensor_tick("watch", SensorOutcome::Fired, 2, None)
            .unwrap();
        store
            .record_sensor_tick("watch", SensorOutcome::Error, 0, Some("boom"))
            .unwrap();
        store
            .record_sensor_tick("probe:docs", SensorOutcome::Fired, 0, None)
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
            (
                "etl".to_string(),
                "0 * * * *".to_string(),
                "UTC".to_string(),
                json!({"full": true}),
            ),
            (
                "health".to_string(),
                "*/5 * * * *".to_string(),
                "UTC".to_string(),
                json!({}),
            ),
        ];
        store.sync_schedules(&defined).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| !r.paused));
        assert_eq!(rows[0].params, json!({"full": true}));
        assert_eq!(rows[1].params, json!({}));

        assert!(store.set_schedule_paused("etl", "0 * * * *", true).unwrap());
        assert!(!store.set_schedule_paused("etl", "bogus", true).unwrap());

        // tz and params follow the declaration; the paused flag stays put
        let defined = vec![(
            "etl".to_string(),
            "0 * * * *".to_string(),
            "Europe/London".to_string(),
            json!({"full": false}),
        )];
        store.sync_schedules(&defined).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].paused);
        assert_eq!(rows[0].tz, "Europe/London");
        assert_eq!(rows[0].params, json!({"full": false}));
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
