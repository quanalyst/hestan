//! the postgres half of the [store](crate::Store): a connection, the schema,
//! and the two calls every query goes through.
//!
//! **a sync store over an async client.** `Store` is eighty-odd synchronous
//! methods behind a mutex and every call site in the crate expects that;
//! making them async would change all eighty and every caller for nothing,
//! since sqlite blocks the runtime on one connection today and that is the
//! accepted architecture. so this blocks too — but it drives the client
//! itself rather than using the sync `postgres` crate, which owns a runtime
//! and calls `block_on` on it, and so panics outright the moment it is called
//! from a thread already driving one. hestan calls the store from async code
//! nearly everywhere. one connection, one runtime of its own to carry it, and
//! a caller that waits: the same blocking sqlite already does, and no pool —
//! what postgres is for here is several processes sharing a run log, not one
//! process issuing more statements at once.
//!
//! **timestamps stay text.** every query in the store compares and orders them
//! as rfc3339 strings, and `timestamptz` would change ordering and comparison
//! semantics across all eighty for no gain in this phase. the columns hestan
//! reads as booleans stay integers for the same reason. both are deliberate,
//! and both are why a row reads identically off either backend.
//!
//! **every text column is `COLLATE "C"`.** sqlite compares text byte by byte;
//! postgres compares it in the database's collation, which on a `en_US.UTF-8`
//! database sorts `probe:docs` in a place byte order does not. the run log's
//! text is ids, names, keys and timestamps — opaque strings that must sort the
//! same way on both backends or the same query answers two things.
//!
//! **no tls.** the connection is what libpq would call `sslmode=disable`: a
//! unix socket, a private network or a local proxy. it is written down in
//! `docs/storage.md` rather than pretended about.

use std::future::Future;

use tokio::runtime::Runtime;
use tokio_postgres::types::{IsNull, ToSql, Type};
use tokio_postgres::{GenericClient, NoTls, Row};

use crate::error::Error;
use crate::store::{AnyRow, SCHEMA_VERSION, Val, args};

/// the whole schema at [`SCHEMA_VERSION`], created in one statement batch.
///
/// the same sixteen tables the sqlite chain arrives at, with the same columns
/// and the same indexes; what each one is for is written on the migration that
/// added it. the differences are `BIGSERIAL` where sqlite writes `INTEGER
/// PRIMARY KEY AUTOINCREMENT`, `BIGINT` where it writes `INTEGER`, and the
/// collation.
const SCHEMA: &str = r#"
-- postgres has no `user_version`, so the stamp is a table. one row, and the
-- constraint says so: this is a fact about the database, not a log of them
CREATE TABLE schema_version (
    only_row BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (only_row),
    version BIGINT NOT NULL
);
CREATE TABLE runs (
    id TEXT COLLATE "C" PRIMARY KEY,
    job TEXT COLLATE "C" NOT NULL,
    status TEXT COLLATE "C" NOT NULL,
    "trigger" TEXT COLLATE "C" NOT NULL,
    params TEXT COLLATE "C" NOT NULL,
    created_at TEXT COLLATE "C" NOT NULL,
    started_at TEXT COLLATE "C",
    finished_at TEXT COLLATE "C",
    resumed_from TEXT COLLATE "C",
    error TEXT COLLATE "C",
    scheduled_for TEXT COLLATE "C",
    tags TEXT COLLATE "C",
    priority BIGINT NOT NULL DEFAULT 0,
    claimed_by TEXT COLLATE "C",
    claimed_at TEXT COLLATE "C",
    lease_until TEXT COLLATE "C",
    plan TEXT COLLATE "C"
);
CREATE INDEX runs_job_created ON runs(job, created_at DESC);
CREATE INDEX runs_queue ON runs(status, claimed_by, priority DESC, created_at);
CREATE TABLE op_runs (
    run_id TEXT COLLATE "C" NOT NULL,
    op TEXT COLLATE "C" NOT NULL,
    status TEXT COLLATE "C" NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    started_at TEXT COLLATE "C",
    finished_at TEXT COLLATE "C",
    output TEXT COLLATE "C",
    error TEXT COLLATE "C",
    metadata TEXT COLLATE "C",
    pid BIGINT,
    inputs TEXT COLLATE "C",
    PRIMARY KEY (run_id, op)
);
CREATE TABLE events (
    seq BIGSERIAL PRIMARY KEY,
    run_id TEXT COLLATE "C" NOT NULL,
    op TEXT COLLATE "C",
    level TEXT COLLATE "C" NOT NULL,
    message TEXT COLLATE "C" NOT NULL,
    ts TEXT COLLATE "C" NOT NULL,
    kind TEXT COLLATE "C" NOT NULL DEFAULT 'log',
    data TEXT COLLATE "C"
);
CREATE INDEX events_run ON events(run_id, seq);
CREATE TABLE schedules (
    job TEXT COLLATE "C" NOT NULL,
    expr TEXT COLLATE "C" NOT NULL,
    tz TEXT COLLATE "C" NOT NULL DEFAULT 'UTC',
    paused BIGINT NOT NULL DEFAULT 0,
    params TEXT COLLATE "C" NOT NULL DEFAULT '{}',
    cursor TEXT COLLATE "C",
    catchup TEXT COLLATE "C" NOT NULL DEFAULT 'skip',
    PRIMARY KEY (job, expr)
);
CREATE TABLE schedule_ticks (
    id BIGSERIAL PRIMARY KEY,
    job TEXT COLLATE "C" NOT NULL,
    expr TEXT COLLATE "C" NOT NULL,
    scheduled_for TEXT COLLATE "C" NOT NULL,
    fired_at TEXT COLLATE "C" NOT NULL,
    outcome TEXT COLLATE "C" NOT NULL,
    run_id TEXT COLLATE "C",
    error TEXT COLLATE "C"
);
CREATE TABLE op_state (
    job TEXT COLLATE "C" NOT NULL,
    op TEXT COLLATE "C" NOT NULL,
    value TEXT COLLATE "C" NOT NULL,
    updated_at TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (job, op)
);
CREATE TABLE asset_materializations (
    id BIGSERIAL PRIMARY KEY,
    asset TEXT COLLATE "C" NOT NULL,
    fingerprint TEXT COLLATE "C" NOT NULL,
    inputs TEXT COLLATE "C" NOT NULL,
    value TEXT COLLATE "C",
    run_id TEXT COLLATE "C",
    built_at TEXT COLLATE "C" NOT NULL,
    metadata TEXT COLLATE "C",
    partition TEXT COLLATE "C"
);
CREATE INDEX asset_materializations_asset
    ON asset_materializations(asset, partition, id DESC);
CREATE TABLE asset_checks (
    id BIGSERIAL PRIMARY KEY,
    asset TEXT COLLATE "C" NOT NULL,
    check_name TEXT COLLATE "C" NOT NULL,
    run_id TEXT COLLATE "C" NOT NULL,
    status TEXT COLLATE "C" NOT NULL,
    severity TEXT COLLATE "C" NOT NULL,
    message TEXT COLLATE "C",
    metadata TEXT COLLATE "C",
    checked_at TEXT COLLATE "C" NOT NULL,
    partition TEXT COLLATE "C"
);
CREATE INDEX asset_checks_asset ON asset_checks(asset, partition, id DESC);
CREATE TABLE backfills (
    id BIGSERIAL PRIMARY KEY,
    asset TEXT COLLATE "C" NOT NULL,
    from_key TEXT COLLATE "C" NOT NULL,
    to_key TEXT COLLATE "C" NOT NULL,
    partition_keys TEXT COLLATE "C" NOT NULL,
    run_ids TEXT COLLATE "C" NOT NULL DEFAULT '[]',
    total BIGINT NOT NULL,
    launched BIGINT NOT NULL DEFAULT 0,
    created_at TEXT COLLATE "C" NOT NULL,
    finished_at TEXT COLLATE "C",
    status TEXT COLLATE "C" NOT NULL
);
CREATE INDEX backfills_asset ON backfills(asset, id DESC);
CREATE TABLE sensors (
    name TEXT COLLATE "C" NOT NULL PRIMARY KEY,
    paused BIGINT NOT NULL DEFAULT 0,
    cursor TEXT COLLATE "C",
    updated_at TEXT COLLATE "C" NOT NULL
);
CREATE TABLE sensor_ticks (
    id BIGSERIAL PRIMARY KEY,
    sensor TEXT COLLATE "C" NOT NULL,
    evaluated_at TEXT COLLATE "C" NOT NULL,
    outcome TEXT COLLATE "C" NOT NULL,
    launched BIGINT NOT NULL DEFAULT 0,
    error TEXT COLLATE "C",
    skipped BIGINT NOT NULL DEFAULT 0,
    duration_ms BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE sensor_run_keys (
    sensor TEXT COLLATE "C" NOT NULL,
    run_key TEXT COLLATE "C" NOT NULL,
    run_id TEXT COLLATE "C" NOT NULL,
    launched_at TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (sensor, run_key)
);
CREATE TABLE freshness_state (
    kind TEXT COLLATE "C" NOT NULL,
    name TEXT COLLATE "C" NOT NULL,
    late BIGINT NOT NULL,
    since TEXT COLLATE "C",
    PRIMARY KEY (kind, name)
);
CREATE TABLE presets (
    job TEXT COLLATE "C" NOT NULL,
    name TEXT COLLATE "C" NOT NULL,
    params TEXT COLLATE "C" NOT NULL,
    created_at TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (job, name)
);
CREATE TABLE op_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT COLLATE "C" NOT NULL,
    op TEXT COLLATE "C" NOT NULL,
    attempt BIGINT NOT NULL,
    at TEXT COLLATE "C" NOT NULL,
    stream TEXT COLLATE "C",
    level TEXT COLLATE "C",
    target TEXT COLLATE "C",
    message TEXT COLLATE "C" NOT NULL
);
CREATE INDEX op_logs_run ON op_logs(run_id, op, id);
CREATE TABLE notifications (
    id BIGSERIAL PRIMARY KEY,
    kind TEXT COLLATE "C" NOT NULL,
    payload TEXT COLLATE "C" NOT NULL,
    created_at TEXT COLLATE "C" NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    next_attempt_at TEXT COLLATE "C",
    delivered_at TEXT COLLATE "C",
    last_error TEXT COLLATE "C"
);
CREATE INDEX notifications_due ON notifications(next_attempt_at)
    WHERE delivered_at IS NULL;
CREATE INDEX notifications_delivered ON notifications(delivered_at);
"#;

/// the lock two processes booting against the same empty database take turns
/// on. sqlite's file lock does this for nothing; postgres has to be asked, and
/// a deployment where several processes start at once is the deployment this
/// backend exists for. the number only has to be hestan's, so it is: the six
/// letters, in ascii.
const BOOT_LOCK: i64 = 0x0068_6573_7461_6e00;

/// and the one dispatchers take turns on when a limit is in force. a different
/// number, because a boot and a claim have nothing to say to each other.
const CLAIM_LOCK: i64 = 0x0068_6573_7461_6e01;

/// one postgres connection, and the runtime that carries it.
///
/// the runtime is one thread whose whole job is the socket. the futures
/// themselves are driven by whichever thread called — see [`wait`] — so a
/// query blocks its caller and nothing else, and a caller that is itself a
/// task on somebody else's runtime is not the special case it would be with
/// the sync client.
pub(crate) struct Client {
    client: tokio_postgres::Client,
    /// an `Option` only so that [`Drop`] can take it: dropping a runtime
    /// blocks, blocking is not allowed on a thread that is driving one, and
    /// the last handle on a store goes out of scope wherever it happens to —
    /// which is as likely as not inside a task.
    rt: Option<Runtime>,
}

impl Drop for Client {
    fn drop(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

/// connect and bring the database to the current schema.
pub(crate) fn open(url: &str) -> Result<Client, Error> {
    let mut client = connect(url)?;
    migrate(&mut client)?;
    Ok(client)
}

/// connect and nothing else. the fixtures that make and unmake the schemas the
/// postgres cases run in need a connection to a database that has no schema
/// yet, which is the one thing [`open`] will not give them.
fn connect(url: &str) -> Result<Client, Error> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("hestan-postgres")
        .enable_all()
        .build()?;
    let (client, connection) = wait(&rt, tokio_postgres::connect(url, NoTls))?;
    // the connection task ends when the client is dropped; an error before
    // that is the socket going away, which the next statement reports anyway
    rt.spawn(async {
        let _ = connection.await;
    });
    Ok(Client {
        client,
        rt: Some(rt),
    })
}

#[cfg(test)]
pub(crate) fn unmigrated(url: &str) -> Result<Client, Error> {
    connect(url)
}

/// drive `f` to completion on the calling thread.
///
/// not `Runtime::block_on`, which panics when the thread already has a runtime
/// — and every store call made from inside an op, a hook or an api handler is
/// on such a thread. entering gives the future the reactor and the connection
/// task it needs; blocking the caller is what keeps `Store` synchronous.
fn wait<T>(rt: &Runtime, f: impl Future<Output = T>) -> T {
    let _entered = rt.enter();
    futures::executor::block_on(f)
}

impl Client {
    /// the runtime carrying this connection. present for as long as the client
    /// is — [`Drop`] takes it and nothing else does.
    fn rt(&self) -> &Runtime {
        self.rt.as_ref().expect("the runtime outlives its client")
    }

    pub(crate) fn execute(&mut self, sql: &str, args: &[Val<'_>]) -> Result<usize, Error> {
        wait(self.rt(), execute(&self.client, sql, args))
    }

    pub(crate) fn query<T>(
        &mut self,
        sql: &str,
        args: &[Val<'_>],
        row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        map(wait(self.rt(), query(&self.client, sql, args))?, row)
    }

    /// several statements at once, no parameters: ddl and nothing else.
    #[cfg(test)]
    pub(crate) fn batch(&mut self, sql: &str) -> Result<(), Error> {
        wait(self.rt(), self.client.batch_execute(sql))?;
        Ok(())
    }

    pub(crate) fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let Client { client, rt } = self;
        let rt = rt.as_ref().expect("the runtime outlives its client");
        let tx = wait(rt, client.transaction())?;
        Ok(Transaction { rt, tx })
    }
}

pub(crate) struct Transaction<'a> {
    rt: &'a Runtime,
    tx: tokio_postgres::Transaction<'a>,
}

impl Transaction<'_> {
    pub(crate) fn execute(&mut self, sql: &str, args: &[Val<'_>]) -> Result<usize, Error> {
        wait(self.rt, execute(&self.tx, sql, args))
    }

    pub(crate) fn query<T>(
        &mut self,
        sql: &str,
        args: &[Val<'_>],
        row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        map(wait(self.rt, query(&self.tx, sql, args))?, row)
    }

    /// several statements at once, no parameters: ddl and nothing else.
    pub(crate) fn batch(&mut self, sql: &str) -> Result<(), Error> {
        wait(self.rt, self.tx.batch_execute(sql))?;
        Ok(())
    }

    /// hold [`CLAIM_LOCK`] until this transaction ends. see `Tx::take_turns`.
    pub(crate) fn claim_lock(&mut self) -> Result<(), Error> {
        wait(
            self.rt,
            self.tx
                .execute("SELECT pg_advisory_xact_lock($1)", &[&CLAIM_LOCK]),
        )?;
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<(), Error> {
        wait(self.rt, self.tx.commit())?;
        Ok(())
    }
}

async fn execute<C: GenericClient>(
    client: &C,
    sql: &str,
    args: &[Val<'_>],
) -> Result<usize, Error> {
    let n = client
        .execute(placeholders(sql).as_str(), &bound(args))
        .await?;
    Ok(n as usize)
}

async fn query<C: GenericClient>(
    client: &C,
    sql: &str,
    args: &[Val<'_>],
) -> Result<Vec<Row>, Error> {
    Ok(client
        .query(placeholders(sql).as_str(), &bound(args))
        .await?)
}

fn map<T>(
    rows: Vec<Row>,
    mut row: impl FnMut(&AnyRow<'_>) -> Result<T, Error>,
) -> Result<Vec<T>, Error> {
    rows.iter().map(|r| row(&AnyRow::Postgres(r))).collect()
}

fn bound<'a>(args: &'a [Val<'a>]) -> Vec<&'a (dyn ToSql + Sync)> {
    args.iter().map(|v| v as &(dyn ToSql + Sync)).collect()
}

/// create the schema, or refuse a database a later build wrote.
///
/// one transaction around the lot (postgres ddl is transactional), so an
/// interrupted first boot leaves the database exactly as found — the same
/// guarantee the sqlite chain gives. from here the chain is forward-only, and
/// a v17 step would go below the version read.
fn migrate(client: &mut Client) -> Result<(), Error> {
    let mut tx = client.transaction()?;
    tx.execute("SELECT pg_advisory_xact_lock(?1)", args![BOOT_LOCK])?;
    // asked this way rather than by reading the table, because a failed read
    // would abort the transaction the write is about to happen in
    let stamped = tx.query(
        "SELECT to_regclass('schema_version') IS NOT NULL",
        args![],
        |row| match row {
            AnyRow::Postgres(r) => Ok(r.try_get::<_, bool>(0)?),
            _ => unreachable!("this is the postgres backend"),
        },
    )?;
    match stamped.first() {
        Some(true) => {
            let found = tx.query("SELECT version FROM schema_version", args![], |r| r.int(0))?;
            let version = found.first().copied().unwrap_or_default();
            let version = u32::try_from(version).unwrap_or(u32::MAX);
            if version > SCHEMA_VERSION {
                return Err(Error::SchemaTooNew(version));
            }
        }
        _ => {
            tx.batch(SCHEMA)?;
            tx.execute(
                "INSERT INTO schema_version (version) VALUES (?1)",
                args![i64::from(SCHEMA_VERSION)],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

/// `?1` becomes `$1`. the two dialects number their placeholders identically
/// and spell the sigil differently, and that is the whole of it — a `?` not
/// followed by a digit is left alone, and no query in the store has one.
fn placeholders(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c == '?' && chars.peek().is_some_and(char::is_ascii_digit) {
            true => out.push('$'),
            false => out.push(c),
        }
    }
    out
}

impl ToSql for Val<'_> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match self {
            Val::Null => Ok(IsNull::Yes),
            Val::Text(s) => s.as_ref().to_sql(ty, out),
            Val::Int(i) => i.to_sql(ty, out),
        }
    }

    // a `Val` knows which of the two it is and this does not, so what it can
    // say is that text and bigint are the column types the schema has
    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty) || <i64 as ToSql>::accepts(ty)
    }

    tokio_postgres::types::to_sql_checked!();
}
