/// every fallible operation in this crate returns one of these. the
/// validation variants (`Graph`, `Cron`, `Timezone`, `InvalidParams`,
/// `DuplicateJob`, `Resource`) surface at build time, before anything is
/// written. the
/// resume variants (`UnknownRun`, `RunActive`, `RunNotFailed`,
/// `NothingToResume`, `ResumeChain`) say why a run cannot be resumed, and
/// `Conflict` is something already under way that this would collide with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid job graph: {0}")]
    Graph(String),
    #[error("unknown job: {0}")]
    UnknownJob(String),
    #[error("unknown run: {0}")]
    UnknownRun(String),
    #[error("run still active: {0}")]
    RunActive(String),
    #[error("run did not fail: {0}")]
    RunNotFailed(String),
    #[error("nothing to resume: every op of run {0} already succeeded")]
    NothingToResume(String),
    #[error("broken resume chain: {0}")]
    ResumeChain(String),
    #[error("unknown asset: {0}")]
    UnknownAsset(String),
    #[error("unknown backfill: {0}")]
    UnknownBackfill(i64),
    #[error("{0}")]
    Conflict(String),
    #[error("duplicate job: {0}")]
    DuplicateJob(String),
    #[error("bad cron expression {expr:?}: {reason}")]
    Cron { expr: String, reason: String },
    #[error("unknown timezone: {0}")]
    Timezone(String),
    #[error("db schema v{0} is newer than this build")]
    SchemaTooNew(u32),
    #[error("invalid params for op {op}: {reason}")]
    InvalidParams { op: String, reason: String },
    #[error("resource {name}: {reason}")]
    Resource { name: String, reason: String },
    #[error("storage: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[cfg(feature = "postgres")]
    #[error("storage: {}", chain(.0))]
    Postgres(#[from] tokio_postgres::Error),
    /// a column that could not be read as what it holds: json that does not
    /// parse, a timestamp that is not rfc3339, a status word this build does
    /// not know. the run log is not supposed to contain any of them.
    #[error("storage: column {0}: {1}")]
    Column(usize, String),
    /// a database target this build cannot open — a `postgres://` url without
    /// the `postgres` feature compiled in.
    #[error("unsupported database: {0}")]
    UnsupportedDb(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// a postgres error and everything under it, in one line.
///
/// on its own the crate's error says "db error" and puts the constraint, the
/// column and the reason in its source — so a storage failure that reached a
/// log or an api response would say nothing whatsoever about what went wrong.
#[cfg(feature = "postgres")]
fn chain(e: &tokio_postgres::Error) -> String {
    let mut out = e.to_string();
    let mut cause = std::error::Error::source(e);
    while let Some(next) = cause {
        out.push_str(&format!(": {next}"));
        cause = next.source();
    }
    out
}
