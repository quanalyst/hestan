/// every fallible operation in this crate returns one of these. the
/// validation variants (`Graph`, `Cron`, `Timezone`, `InvalidParams`,
/// `DuplicateJob`) surface at build time, before anything is written. the
/// resume variants (`UnknownRun`, `RunActive`, `RunNotFailed`,
/// `NothingToResume`, `ResumeChain`) say why a run cannot be resumed.
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
    #[error("storage: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
