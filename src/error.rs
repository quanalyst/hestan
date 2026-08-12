/// every fallible operation in this crate returns one of these. the
/// validation variants (`Graph`, `Cron`, `Timezone`, `InvalidParams`,
/// `DuplicateJob`, `Resource`) surface at build time, before anything is
/// written. the
/// resume variants (`UnknownRun`, `RunActive`, `RunNotFailed`,
/// `NothingToResume`, `ResumeChain`) say why a run cannot be resumed, and
/// `Conflict` is something already under way that this would collide with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// a declaration that does not describe anything executable: a cycle, a
    /// dep on a name nothing produces, two ops under one name, a fan-out with
    /// nothing to fan out over. every job, graph and asset registration is
    /// checked before a single row is written, so this is a startup failure
    /// rather than a run that gets halfway.
    #[error("invalid job graph: {0}")]
    Graph(String),
    /// no job of that name in this process. a run of it may well exist — the
    /// run log outlives the code that defined the job.
    #[error("unknown job: {0}")]
    UnknownJob(String),
    /// no run with that id in this store.
    #[error("unknown run: {0}")]
    UnknownRun(String),
    /// the run has not finished, and what was asked for only makes sense once
    /// it has.
    #[error("run still active: {0}")]
    RunActive(String),
    /// only a failed run can be resumed: succeeding again from a partial state
    /// is not something hestan can promise means anything.
    #[error("run did not fail: {0}")]
    RunNotFailed(String),
    /// there is nothing left for a resume to execute, which usually means the
    /// run was already resumed and it worked.
    #[error("nothing to resume: every op of run {0} already succeeded")]
    NothingToResume(String),
    /// a resume needs the outputs of the ops it is not re-running, and
    /// somewhere back along the chain of resumes they are gone — pruned by
    /// [retention](crate::Retention), or never recorded.
    #[error("broken resume chain: {0}")]
    ResumeChain(String),
    /// no asset of that name is registered in this process.
    #[error("unknown asset: {0}")]
    UnknownAsset(String),
    /// no backfill with that id.
    #[error("unknown backfill: {0}")]
    UnknownBackfill(i64),
    /// something already under way that this would collide with — the 409 of
    /// the api, and the reason a second build of the same assets is refused
    /// rather than queued.
    #[error("{0}")]
    Conflict(String),
    /// two jobs registered under one name. also what a user job called
    /// `assets` collides with, since registering any asset defines one.
    #[error("duplicate job: {0}")]
    DuplicateJob(String),
    /// a cron expression the parser rejected, quoted back with what it
    /// objected to.
    #[error("bad cron expression {expr:?}: {reason}")]
    Cron {
        /// the expression as it was written.
        expr: String,
        /// what the parser objected to.
        reason: String,
    },
    /// a timezone name the tz database does not have.
    #[error("unknown timezone: {0}")]
    Timezone(String),
    /// the database was migrated by a newer hestan. refused rather than opened
    /// read-only or migrated backwards: this build cannot know what the newer
    /// one meant by the columns it added, and guessing corrupts a run log.
    #[error("db schema v{0} is newer than this build")]
    SchemaTooNew(u32),
    /// `serve` refused an address anyone can reach with nothing checking who
    /// is asking. see [`Auth`](crate::Auth).
    #[error(
        "refusing to serve {0}: that address is reachable from outside this machine and \
         nothing here checks who is asking — this api launches runs, cancels them and \
         changes limits. bind a loopback address, give it Hestan::auth(Auth::bearer(…)) \
         or Hestan::auth(Auth::custom(…)), or say Hestan::auth(Auth::None) if something \
         in front of hestan already checks identity"
    )]
    Unguarded(std::net::SocketAddr),
    /// the launch params failed the check an op declared with
    /// [`Op::params`](crate::Op::params). raised before the run row is
    /// written, so a launch that cannot possibly work leaves no run behind to
    /// explain.
    #[error("invalid params for op {op}: {reason}")]
    InvalidParams {
        /// the op whose check rejected them.
        op: String,
        /// what it objected to.
        reason: String,
    },
    /// an op asked for a resource nothing declared, or one declared as another
    /// type.
    #[error("resource {name}: {reason}")]
    Resource {
        /// the resource, by the name the op asked for.
        name: String,
        /// what was wrong with it.
        reason: String,
    },
    /// sqlite said no. a writer waiting behind another writer is not this: the
    /// connection carries a busy timeout, so contention costs latency rather
    /// than an error.
    #[error("storage: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// postgres said no, flattened: the constraint and the column are in the
    /// error's source chain, and a message that dropped them would say
    /// nothing at all.
    #[cfg(feature = "postgres")]
    #[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
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
    /// a dbt manifest that cannot become assets: it could not be read, it is
    /// not the json a manifest is, its schema version is not one this build
    /// [reads](crate::dbt), or two of its nodes would be one asset. every one
    /// of them is a startup failure, before a run of anything exists.
    #[cfg(feature = "dbt")]
    #[cfg_attr(docsrs, doc(cfg(feature = "dbt")))]
    #[error("dbt manifest {path}: {reason}")]
    Dbt {
        /// the manifest, as it was named.
        path: String,
        /// what was wrong with it.
        reason: String,
    },
    /// the filesystem: a database path that cannot be opened, an
    /// [`IoManager`](crate::IoManager)'s directory, a listener that could not
    /// take its address.
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
