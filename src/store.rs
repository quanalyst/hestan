use std::collections::HashSet;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::Value;

use crate::error::Error;
use crate::model::{
    Event, EventKind, EventLevel, Materialization, OpRun, OpStatus, Run, RunStatus, ScheduleRow,
    SensorOutcome, SensorRow, SensorTick, Tick, TickOutcome,
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

const SCHEMA_VERSION: u32 = 4;

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
            r#"INSERT INTO runs (id, job, status, "trigger", params, created_at, started_at, finished_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)"#,
            params![
                run.id,
                run.job,
                run.status.as_str(),
                run.trigger.as_str(),
                run.params.to_string(),
                run.created_at.to_rfc3339(),
                run.started_at.map(|t| t.to_rfc3339()),
                run.finished_at.map(|t| t.to_rfc3339()),
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
            "UPDATE runs SET status = 'failed', finished_at = ?1
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

    pub(crate) fn run_finished(&self, id: &str, status: RunStatus) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE runs SET status = ?1, finished_at = ?2 WHERE id = ?3",
            params![status.as_str(), Utc::now().to_rfc3339(), id],
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

    pub(crate) fn op_finished(
        &self,
        run_id: &str,
        op: &str,
        status: OpStatus,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "UPDATE op_runs SET status = ?1, finished_at = ?2, output = ?3, error = ?4
             WHERE run_id = ?5 AND op = ?6",
            params![
                status.as_str(),
                Utc::now().to_rfc3339(),
                output.map(|v| v.to_string()),
                error,
                run_id,
                op,
            ],
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
    /// refresh tz on existing ones (pause state survives), drop the rest.
    pub(crate) fn sync_schedules(&self, defined: &[(String, String, String)]) -> Result<(), Error> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut insert =
                tx.prepare("INSERT OR IGNORE INTO schedules (job, expr, tz) VALUES (?1, ?2, ?3)")?;
            let mut update =
                tx.prepare("UPDATE schedules SET tz = ?3 WHERE job = ?1 AND expr = ?2")?;
            for (job, expr, tz) in defined {
                insert.execute(params![job, expr, tz])?;
                update.execute(params![job, expr, tz])?;
            }
        }
        let existing: Vec<(String, String)> = {
            let mut stmt = tx.prepare("SELECT job, expr FROM schedules")?;
            let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };
        let keep: HashSet<(&str, &str)> = defined
            .iter()
            .map(|(j, e, _)| (j.as_str(), e.as_str()))
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
            conn.prepare("SELECT job, expr, tz, paused FROM schedules ORDER BY job, expr")?;
        let rows = stmt.query_map([], |r| {
            Ok(ScheduleRow {
                job: r.get(0)?,
                expr: r.get(1)?,
                tz: r.get(2)?,
                paused: r.get(3)?,
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
                r#"SELECT id, job, status, "trigger", params, created_at, started_at, finished_at
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
            r#"SELECT id, job, status, "trigger", params, created_at, started_at, finished_at
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
            "SELECT run_id, op, status, attempts, started_at, finished_at, output, error
             FROM op_runs WHERE run_id = ?1 ORDER BY op",
        )?;
        let rows = stmt.query_map(params![run_id], op_run_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// op_run rows across the job's most recent `runs` runs, newest run first.
    pub fn recent_op_runs(&self, job: &str, runs: u32) -> Result<Vec<OpRun>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT o.run_id, o.op, o.status, o.attempts, o.started_at, o.finished_at, o.output, o.error
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

    /// record an asset's current materialization, replacing any previous one.
    pub(crate) fn upsert_materialization(
        &self,
        asset: &str,
        fingerprint: &str,
        inputs: &Value,
        value: Option<&Value>,
        run_id: Option<&str>,
    ) -> Result<(), Error> {
        let conn = self.0.lock().unwrap();
        conn.execute(
            "INSERT INTO asset_materializations (asset, fingerprint, inputs, value, run_id, built_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT (asset) DO UPDATE SET
                 fingerprint = ?2, inputs = ?3, value = ?4, run_id = ?5, built_at = ?6",
            params![
                asset,
                fingerprint,
                inputs.to_string(),
                value.map(|v| v.to_string()),
                run_id,
                Utc::now().to_rfc3339()
            ],
        )?;
        Ok(())
    }

    pub fn materialization(&self, asset: &str) -> Result<Option<Materialization>, Error> {
        let conn = self.0.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT asset, fingerprint, inputs, value, run_id, built_at
                 FROM asset_materializations WHERE asset = ?1",
                params![asset],
                materialization_from_row,
            )
            .optional()?;
        Ok(row)
    }

    pub fn materializations(&self) -> Result<Vec<Materialization>, Error> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT asset, fingerprint, inputs, value, run_id, built_at
             FROM asset_materializations ORDER BY asset",
        )?;
        let rows = stmt.query_map([], materialization_from_row)?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
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
        error: row.get(7)?,
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
        asset: row.get(0)?,
        fingerprint: row.get(1)?,
        inputs: json_col(row, 2)?,
        value: opt_json_col(row, 3)?,
        run_id: row.get(4)?,
        built_at: ts_col(row, 5)?,
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
                None,
            )
            .unwrap();
        store
            .op_finished("r1", "b", OpStatus::Failed, None, Some("boom"))
            .unwrap();
        store.run_finished("r1", RunStatus::Failed).unwrap();

        let got = store.run("r1").unwrap().unwrap();
        assert_eq!(got.status, RunStatus::Failed);
        assert!(got.started_at.is_some() && got.finished_at.is_some());

        let ops = store.op_runs("r1").unwrap();
        assert_eq!(ops[0].attempts, 2);
        assert_eq!(ops[0].started_at.unwrap(), first_start);
        assert_eq!(ops[0].output, Some(json!({"rows": 3})));
        assert_eq!(ops[1].status, OpStatus::Failed);
        assert_eq!(ops[1].error.as_deref(), Some("boom"));
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
            store.run_finished(id, status).unwrap();
        }
        store
            .create_run(&mk_run("live", "etl", old), &["a".into()])
            .unwrap();
        store.run_started("live").unwrap();
        store
            .create_run(&mk_run("young", "etl", Utc::now()), &["a".into()])
            .unwrap();
        store.run_finished("young", RunStatus::Success).unwrap();
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
        store.run_finished("r1", RunStatus::Failed).unwrap();
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
            .op_finished("done", "a", OpStatus::Success, None, None)
            .unwrap();
        store.run_finished("done", RunStatus::Success).unwrap();

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
        assert!(store.materializations().unwrap().is_empty());
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
        phase1_db(path, 5);
        let err = Store::open(path).err().unwrap();
        assert_eq!(err.to_string(), "db schema v5 is newer than this build");
        let conn = Connection::open(path).unwrap();
        let version: u32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 5);
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
        assert!(store.materializations().unwrap().is_empty());
        store
            .upsert_materialization("docs", "abc", &json!({}), None, None)
            .unwrap();
        store.sync_sensors(&["watch".into()]).unwrap();
        drop(store);
        let store = Store::open(path).unwrap();
        assert_eq!(
            store.materialization("docs").unwrap().unwrap().fingerprint,
            "abc"
        );
        assert_eq!(store.sensors().unwrap().len(), 1);
    }

    #[test]
    fn materialization_upsert_and_read() {
        let store = Store::open(":memory:").unwrap();
        assert!(store.materialization("stats").unwrap().is_none());

        store
            .upsert_materialization(
                "stats",
                "f1",
                &json!({"docs": "d1"}),
                Some(&json!({"files": 12})),
                Some("r1"),
            )
            .unwrap();
        store
            .upsert_materialization("docs", "d1", &json!({}), None, None)
            .unwrap();

        let m = store.materialization("stats").unwrap().unwrap();
        assert_eq!(m.fingerprint, "f1");
        assert_eq!(m.inputs, json!({"docs": "d1"}));
        assert_eq!(m.value, Some(json!({"files": 12})));
        assert_eq!(m.run_id.as_deref(), Some("r1"));
        let first_built = m.built_at;

        let d = store.materialization("docs").unwrap().unwrap();
        assert_eq!(d.value, None);
        assert_eq!(d.run_id, None);

        store
            .upsert_materialization(
                "stats",
                "f2",
                &json!({"docs": "d2"}),
                Some(&json!({"files": 13})),
                Some("r2"),
            )
            .unwrap();
        let all = store.materializations().unwrap();
        assert_eq!(all.len(), 2);
        let m = store.materialization("stats").unwrap().unwrap();
        assert_eq!(m.fingerprint, "f2");
        assert_eq!(m.run_id.as_deref(), Some("r2"));
        assert!(m.built_at >= first_built);
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
            ),
            (
                "health".to_string(),
                "*/5 * * * *".to_string(),
                "UTC".to_string(),
            ),
        ];
        store.sync_schedules(&defined).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| !r.paused));

        assert!(store.set_schedule_paused("etl", "0 * * * *", true).unwrap());
        assert!(!store.set_schedule_paused("etl", "bogus", true).unwrap());

        let defined = vec![(
            "etl".to_string(),
            "0 * * * *".to_string(),
            "Europe/London".to_string(),
        )];
        store.sync_schedules(&defined).unwrap();
        let rows = store.schedules().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].paused);
        assert_eq!(rows[0].tz, "Europe/London");
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
