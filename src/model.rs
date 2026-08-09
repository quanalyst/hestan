use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

macro_rules! str_enum {
    ($ty:ident { $($variant:ident => $s:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(&self) -> &'static str {
                match self { $(Self::$variant => $s),+ }
            }
        }
        impl std::str::FromStr for $ty {
            type Err = String;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match s {
                    $($s => Ok(Self::$variant),)+
                    other => Err(format!("unknown {}: {other}", stringify!($ty))),
                }
            }
        }
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    Queued,
    Running,
    Success,
    Failed,
    Canceled,
}
str_enum!(RunStatus {
    Queued => "queued",
    Running => "running",
    Success => "success",
    Failed => "failed",
    Canceled => "canceled",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpStatus {
    Pending,
    Running,
    Success,
    Failed,
    Skipped,
    Canceled,
}
str_enum!(OpStatus {
    Pending => "pending",
    Running => "running",
    Success => "success",
    Failed => "failed",
    Skipped => "skipped",
    Canceled => "canceled",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trigger {
    Manual,
    Schedule,
    Retry,
    Resume,
    Build,
    Sensor,
}
str_enum!(Trigger {
    Manual => "manual",
    Schedule => "schedule",
    Retry => "retry",
    Resume => "resume",
    Build => "build",
    Sensor => "sensor",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventLevel {
    Info,
    Warn,
    Error,
}
str_enum!(EventLevel { Info => "info", Warn => "warn", Error => "error" });

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    RunQueued,
    RunStarted,
    RunSuccess,
    RunFailed,
    RunCanceled,
    OpStarted,
    OpExpanded,
    OpRetry,
    OpSuccess,
    OpFailed,
    OpSkipped,
    OpCanceled,
    TypeCheckFailed,
    Log,
}
str_enum!(EventKind {
    RunQueued => "run_queued",
    RunStarted => "run_started",
    RunSuccess => "run_success",
    RunFailed => "run_failed",
    RunCanceled => "run_canceled",
    OpStarted => "op_started",
    OpExpanded => "op_expanded",
    OpRetry => "op_retry",
    OpSuccess => "op_success",
    OpFailed => "op_failed",
    OpSkipped => "op_skipped",
    OpCanceled => "op_canceled",
    TypeCheckFailed => "type_check_failed",
    Log => "log",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TickOutcome {
    Fired,
    Error,
    Skipped,
    Deferred,
}
str_enum!(TickOutcome {
    Fired => "fired",
    Error => "error",
    Skipped => "skipped",
    Deferred => "deferred",
});

/// an op's trigger rule: what its deps have to have done for it to run, from
/// [`Op::when`](crate::Op::when). readiness is the same either way — every dep
/// terminal — and this decides run vs skip once they are.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum When {
    /// every dep succeeded. the default, and the only rule there was before
    /// trigger rules existed.
    #[default]
    AllSucceeded,
    /// at least one dep did not succeed — failed, skipped, or canceled. an op
    /// with no deps never qualifies.
    AnyFailed,
    /// whatever the deps did, including nothing.
    Always,
}
str_enum!(When {
    AllSucceeded => "all_succeeded",
    AnyFailed => "any_failed",
    Always => "always",
});

/// what a declared freshness policy says right now —
/// [`Asset::fresh_within`](crate::Asset::fresh_within) or
/// [`JobBuilder::fresh_within`](crate::JobBuilder::fresh_within) read against
/// the latest success. computed at read time; nothing caches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// a success inside the window the policy allows.
    Fresh,
    /// the window closed `by` ago.
    Late { by: Duration },
    /// nothing has ever succeeded, so there is no age to measure. deliberately
    /// not late: a policy caps how old a success may get, and this has none.
    Never,
}

impl Freshness {
    /// `fresh` / `late` / `never`, which is what the api reports.
    pub fn status(&self) -> &'static str {
        match self {
            Freshness::Fresh => "fresh",
            Freshness::Late { .. } => "late",
            Freshness::Never => "never",
        }
    }

    /// how far past the deadline, on a late one; `None` otherwise.
    pub fn late_by(&self) -> Option<Duration> {
        match self {
            Freshness::Late { by } => Some(*by),
            _ => None,
        }
    }

    pub fn is_late(&self) -> bool {
        matches!(self, Freshness::Late { .. })
    }

    /// the verdict a `within` policy reaches about `last_success` at `now`.
    pub(crate) fn of(
        last_success: Option<DateTime<Utc>>,
        within: Duration,
        now: DateTime<Utc>,
    ) -> Freshness {
        let Some(last) = last_success else {
            return Freshness::Never;
        };
        let window = chrono::Duration::from_std(within).unwrap_or(chrono::Duration::MAX);
        let deadline = last + window;
        match (now - deadline).to_std() {
            // to_std fails on a negative span, which is exactly "not yet due"
            Ok(by) if !by.is_zero() => Freshness::Late { by },
            _ => Freshness::Fresh,
        }
    }
}

/// what the last freshness check concluded about one job or asset, kept so a
/// transition fires its hook once rather than once per poll.
#[derive(Debug, Clone, Serialize)]
pub struct FreshnessRow {
    /// `job` or `asset`.
    pub kind: String,
    pub name: String,
    pub late: bool,
    /// when it went late; `None` while it is not.
    pub since: Option<DateTime<Utc>>,
}

/// what a schedule does when it fires while the job still has an active run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Overlap {
    Allow,
    #[default]
    Skip,
    Queue,
}
str_enum!(Overlap { Allow => "allow", Skip => "skip", Queue => "queue" });

/// what a schedule does about occurrences that came due while nothing was
/// running to fire them — a restart, a crash, a deploy. the scheduler's
/// [cursor](crate::Schedule::catchup) is what makes the missed set knowable at
/// all; this decides what to do with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Catchup {
    /// advance the cursor over them and fire nothing. the default, and what
    /// the scheduler did before it had a cursor.
    #[default]
    Skip,
    /// fire the most recent missed occurrence only. for a job that computes
    /// current state, where the last one subsumes the rest.
    One,
    /// fire every missed occurrence, oldest first, at most `limit` of them.
    /// for a job that does work *for* a logical time — read
    /// [`ctx.scheduled_for`](crate::OpCtx::scheduled_for) — where each hour is
    /// its own hour and skipping one leaves a hole. past the cap the oldest are
    /// dropped, loudly.
    All { limit: usize },
}

impl std::fmt::Display for Catchup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Catchup::Skip => f.write_str("skip"),
            Catchup::One => f.write_str("one"),
            Catchup::All { limit } => write!(f, "all:{limit}"),
        }
    }
}

// one text form everywhere: the stored column, the api, and the ui all read
// `skip`, `one` or `all:24` rather than three shapes of the same thing
impl std::str::FromStr for Catchup {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "skip" => Ok(Catchup::Skip),
            "one" => Ok(Catchup::One),
            other => other
                .strip_prefix("all:")
                .and_then(|n| n.parse().ok())
                .map(|limit| Catchup::All { limit })
                .ok_or_else(|| format!("unknown Catchup: {other}")),
        }
    }
}

impl Serialize for Catchup {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

/// the flat `{"k": "v"}` map a run carries: what
/// [`trigger`](Trigger) cannot say about why a run exists. set at launch, from
/// [`Hestan::run_tags`](crate::Hestan::run_tags) defaults, and automatically
/// on machine-made runs.
///
/// a `BTreeMap` rather than a `Value` because the shape is the promise: flat,
/// string to string, and stably ordered wherever it is written or shown.
pub type RunTags = BTreeMap<String, String>;

#[derive(Debug, Clone, Serialize)]
pub struct Run {
    pub id: String,
    pub job: String,
    pub status: RunStatus,
    pub trigger: Trigger,
    pub params: Value,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// why the run failed: the first op that terminally failed, named, as
    /// `"op {name} failed: {message}"`. `None` on a run that never failed.
    pub error: Option<String>,
    /// the run this one resumed, for a run launched by
    /// [`Runner::resume_from`](crate::Runner::resume_from); `None` otherwise.
    pub resumed_from: Option<String>,
    /// the occurrence this run stands for, on a scheduled or caught-up run:
    /// the logical time, not the wall clock it launched at. `None` on a manual
    /// launch, a retry, a resume, a build or a sensor fire, which represent
    /// nothing but themselves.
    pub scheduled_for: Option<DateTime<Utc>>,
    /// what this run was [tagged](RunTags) with; empty on an untagged run,
    /// which is what an untagged launch and every run older than tags both
    /// read as.
    pub tags: RunTags,
    /// where this run sits in the queue: higher starts first, ties broken by
    /// `created_at`. 0 unless the launch or
    /// [`Hestan::priority`](crate::Hestan::priority) said otherwise.
    pub priority: i64,
    /// the [instance](crate::Hestan::work) that claimed this run out of the
    /// queue, and is executing it. `None` on a run nobody has claimed — which
    /// is what a queued run is — and on every run written before the queue.
    pub claimed_by: Option<String>,
    pub claimed_at: Option<DateTime<Utc>>,
    /// how long the claim is good for. the claimer renews it on a heartbeat;
    /// past it, the claim is reclaimable by anyone. `None` once the run is over.
    pub lease_until: Option<DateTime<Utc>>,
}

/// a named parameter set stored against one job: what
/// [`Hestan::preset`](crate::Hestan::preset) declares and what the launchpad
/// saves. runtime data rather than part of the job definition — the ui creates
/// and deletes them, and a declared one is only ever seeded — so it lives in
/// the store beside the run log rather than on [`Job`](crate::Job).
#[derive(Debug, Clone, Serialize)]
pub struct Preset {
    pub job: String,
    pub name: String,
    pub params: Value,
    /// when the preset was first stored; a rewrite keeps it.
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpRun {
    pub run_id: String,
    pub op: String,
    pub status: OpStatus,
    pub attempts: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub output: Option<Value>,
    /// typed facts the op reported with [`OpCtx::meta`](crate::OpCtx::meta),
    /// one tagged value per name. `None` when it reported nothing.
    pub metadata: Option<Value>,
    pub error: Option<String>,
    /// the child process an [isolated](crate::Op::isolated) op is running in.
    /// `None` for every in-process op and for every op that has finished —
    /// this says what is running where, not where something ran.
    pub pid: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub seq: i64,
    pub run_id: String,
    pub op: Option<String>,
    pub level: EventLevel,
    pub kind: EventKind,
    pub message: String,
    pub data: Option<Value>,
    pub ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRow {
    pub job: String,
    pub expr: String,
    pub tz: String,
    pub paused: bool,
    /// the params every fire of this schedule launches with, `{}` unless the
    /// declaration set them.
    pub params: Value,
    /// what to do with occurrences that came due while nothing was running.
    pub catchup: Catchup,
    /// the newest occurrence the scheduler has accounted for — fired, skipped,
    /// held or deliberately dropped. `None` until this process has seen the
    /// schedule once; everything strictly after it and strictly before now is
    /// what downtime swallowed.
    pub cursor: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Tick {
    pub id: i64,
    pub job: String,
    pub expr: String,
    pub scheduled_for: DateTime<Utc>,
    pub fired_at: DateTime<Utc>,
    pub outcome: TickOutcome,
    pub run_id: Option<String>,
    pub error: Option<String>,
}

/// one entry of an asset's materialization history, newest of which is its
/// current state. `inputs` maps each dep name to the fingerprint this asset
/// consumed; source rows carry no value and no run.
#[derive(Debug, Clone, Serialize)]
pub struct Materialization {
    /// monotonic within the table, so ordering by it is ordering by time even
    /// when two builds land in the same millisecond.
    pub id: i64,
    pub asset: String,
    /// the key this entry is for, on a [partitioned
    /// asset](crate::Partitions); `None` for an unpartitioned one.
    pub partition: Option<String>,
    pub fingerprint: String,
    pub inputs: Value,
    pub value: Option<Value>,
    pub run_id: Option<String>,
    pub built_at: DateTime<Utc>,
    /// what the op that built this reported with
    /// [`OpCtx::meta`](crate::OpCtx::meta) — the same map its op run carries.
    pub metadata: Option<Value>,
}

/// what a failing [`AssetCheck`](crate::AssetCheck) costs. `Error` — the
/// default — fails the check's op, and so the run that produced the asset;
/// `Warn` records the failure and lets the run carry on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Warn,
    #[default]
    Error,
}
str_enum!(Severity { Warn => "warn", Error => "error" });

/// what a check said about the value it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Passed,
    Failed,
}
str_enum!(CheckStatus { Passed => "passed", Failed => "failed" });

/// one recorded check result. `failed` with severity `warn` is a run that
/// succeeded and a check that did not — the two are recorded separately on
/// purpose.
#[derive(Debug, Clone, Serialize)]
pub struct AssetCheckRow {
    pub id: i64,
    pub asset: String,
    /// the key that was checked, on a [partitioned
    /// asset](crate::Partitions); `None` for an unpartitioned one.
    pub partition: Option<String>,
    pub check: String,
    /// the run whose build this checked; checks only ever run inside one.
    pub run_id: String,
    pub status: CheckStatus,
    pub severity: Severity,
    pub message: Option<String>,
    /// what the check reported with `CheckResult::meta`, tagged by type like
    /// [op metadata](crate::Meta).
    pub metadata: Option<Value>,
    pub checked_at: DateTime<Utc>,
}

/// how a [backfill](crate::Hestan) ended, derived from the runs it launched.
/// `running` covers a chunk in flight and the pause between chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackfillStatus {
    Running,
    Complete,
    Failed,
    Canceled,
}
str_enum!(BackfillStatus {
    Running => "running",
    Complete => "complete",
    Failed => "failed",
    Canceled => "canceled",
});

/// a recorded request to materialize a range of one asset's partitions.
/// `partitions` is what the range resolved to at the moment it was made, so a
/// backfill builds what it was asked for even as the key set grows underneath
/// it; `launched` counts how many of them have been handed to a run.
#[derive(Debug, Clone, Serialize)]
pub struct Backfill {
    pub id: i64,
    pub asset: String,
    pub from_key: String,
    pub to_key: String,
    pub partitions: Vec<String>,
    /// one per chunk launched, oldest first.
    pub run_ids: Vec<String>,
    pub total: usize,
    pub launched: usize,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub status: BackfillStatus,
}

/// where a [run-status sensor](crate::RunStatusSensor) has read up to: the
/// last terminal run it saw, ordered by finish time and then id so two runs
/// finishing in the same instant can neither be skipped nor seen twice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunCursor {
    pub finished_at: DateTime<Utc>,
    pub id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SensorRow {
    pub name: String,
    pub paused: bool,
    pub cursor: Option<Value>,
    pub updated_at: DateTime<Utc>,
}

/// how a sensor evaluation ended: `fired` means the closure returned and every
/// requested run launched, possibly zero of them. `skipped` is a turn the loop
/// did not evaluate at all, because the previous evaluation was still going.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SensorOutcome {
    Fired,
    Error,
    Skipped,
}
str_enum!(SensorOutcome {
    Fired => "fired",
    Error => "error",
    Skipped => "skipped",
});

#[derive(Debug, Clone, Serialize)]
pub struct SensorTick {
    pub id: i64,
    pub sensor: String,
    pub evaluated_at: DateTime<Utc>,
    pub outcome: SensorOutcome,
    pub launched: u32,
    /// requests this evaluation did not launch because their [run
    /// key](crate::RunRequest::key) had already been claimed.
    pub skipped: u32,
    /// how long the evaluation took. 0 on a `skipped` tick, which records a
    /// turn that was never taken.
    pub duration_ms: u64,
    pub error: Option<String>,
}

pub(crate) fn new_run_id() -> String {
    uuid::Uuid::now_v7().to_string()
}
