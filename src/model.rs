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
}

/// one schedule as the code declares it — job, cron expression, timezone,
/// params — which is what a sync writes over the stored rows.
pub(crate) type ScheduleDef = (String, String, String, Value);

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

#[derive(Debug, Clone, Serialize)]
pub struct SensorRow {
    pub name: String,
    pub paused: bool,
    pub cursor: Option<Value>,
    pub updated_at: DateTime<Utc>,
}

/// how a sensor evaluation ended: `fired` means the closure returned and every
/// requested run launched, possibly zero of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SensorOutcome {
    Fired,
    Error,
}
str_enum!(SensorOutcome { Fired => "fired", Error => "error" });

#[derive(Debug, Clone, Serialize)]
pub struct SensorTick {
    pub id: i64,
    pub sensor: String,
    pub evaluated_at: DateTime<Utc>,
    pub outcome: SensorOutcome,
    pub launched: u32,
    pub error: Option<String>,
}

pub(crate) fn new_run_id() -> String {
    uuid::Uuid::now_v7().to_string()
}
