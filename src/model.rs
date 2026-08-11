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

/// which version of the [event](Event) payloads this build writes, reported by
/// `GET /api/events` beside every page.
///
/// **what it promises while hestan is 0.x.** a key documented in
/// `docs/events.md` keeps its name, its type and its meaning for as long as
/// this number does not move. what may happen without it moving: a payload
/// gains a key, a kind is added, and a key documented as optional is absent.
/// what may not: a key changing type, a key changing meaning, or a kind
/// changing what it is about. a consumer that reads the keys it knows and
/// ignores the rest keeps working across the whole of 0.x; one that matches
/// exhaustively on kinds does not, and that is why
/// [`EventKind::Unknown`](EventKind::Unknown) exists on this side of the wire
/// too.
pub const EVENT_SCHEMA: u32 = 1;

/// what an [`Event`] is about: which of hestan's tables the thing that happened
/// lives in.
///
/// the log described runs and nothing else until v17, so every event written
/// before it reads as [`Run`](SubjectKind::Run) — which is what those events
/// were.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubjectKind {
    /// one run, named by [`Event::run_id`] rather than by `subject`.
    Run,
    /// one job, by name: what retention pruned, and nothing else so far.
    Job,
    /// one asset, by name; the partition, if it has one, is in the payload.
    Asset,
    /// one schedule, by the job it fires.
    Schedule,
    /// one sensor, by name.
    Sensor,
    /// one backfill, by id.
    Backfill,
    /// hestan itself: a notification's delivery, which belongs to no one job.
    System,
    /// a kind written by a build newer than this one. carried through rather
    /// than refused — see [`EventKind::Unknown`].
    Unknown(String),
}

/// what happened. one variant per thing hestan does, and
/// [`Unknown`](EventKind::Unknown) for what a later one will.
///
/// the run kinds are the eight this log started with. the rest were added in
/// v17 and are written by the subsystem that does the work, in the transaction
/// that does it — `docs/events.md` has the table, and says which of them cannot
/// be atomic and what the window is.
///
/// **not a closed set.** a kind this build does not know reads as
/// [`Unknown`](EventKind::Unknown) carrying the stored word, because the
/// alternative — a parse error — is one row from a newer writer breaking every
/// query that would have read the rows around it. the same reason
/// [`Meta::from_tagged`](crate::Meta::from_tagged) tolerates an unknown tag.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EventKind {
    RunQueued,
    RunStarted,
    RunSuccess,
    RunFailed,
    RunCanceled,
    /// a claim expired and the run was taken back from whoever held it.
    RunReclaimed,
    OpStarted,
    OpExpanded,
    OpRetry,
    OpSuccess,
    OpFailed,
    OpSkipped,
    OpCanceled,
    TypeCheckFailed,
    /// an asset (or one partition of one) was built and recorded.
    AssetMaterialized,
    CheckPassed,
    CheckFailed,
    /// a schedule came due and launched a run.
    ScheduleFired,
    /// a schedule fired for an occurrence that came due while nothing was
    /// running to fire it.
    ScheduleCaughtUp,
    /// an occurrence was accounted for without firing: an overlap policy, a
    /// catch-up cap, or a declaration that has since gone.
    ScheduleSkipped,
    /// an occurrence held back until the job is free, and still waiting.
    ScheduleDeferred,
    /// an occurrence came due and the launch failed.
    ScheduleError,
    /// one sensor evaluation, however it ended.
    SensorTick,
    BackfillStarted,
    /// one chunk of a backfill was launched.
    BackfillChunk,
    BackfillFinished,
    BackfillCanceled,
    /// a schedule was paused or unpaused; `paused` in the payload says which.
    SchedulePaused,
    /// a sensor was paused or unpaused, the same way.
    SensorPaused,
    NotificationDelivered,
    /// a notification hestan has stopped trying to deliver.
    NotificationFailed,
    /// what one job's [retention policy](crate::Retention) deleted.
    RetentionPruned,
    Log,
    /// a kind written by a build newer than this one, carrying its word.
    Unknown(String),
}

/// the `as_str`/`FromStr`/`Display`/serde set that [`str_enum!`] generates,
/// for the two enums that also have an `Unknown` arm: a parse that cannot fail
/// and an `as_str` that borrows from `self` rather than from `'static`.
macro_rules! open_enum {
    ($ty:ident { $($variant:ident => $s:literal),+ $(,)? }) => {
        impl $ty {
            pub fn as_str(&self) -> &str {
                match self {
                    $(Self::$variant => $s,)+
                    Self::Unknown(s) => s,
                }
            }
        }
        impl std::str::FromStr for $ty {
            type Err = std::convert::Infallible;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(match s {
                    $($s => Self::$variant,)+
                    other => Self::Unknown(other.to_string()),
                })
            }
        }
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.as_str())
            }
        }
        impl Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(self.as_str())
            }
        }
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                let s = String::deserialize(d)?;
                Ok(s.parse().unwrap_or_else(|e| match e {}))
            }
        }
    };
}

open_enum!(SubjectKind {
    Run => "run",
    Job => "job",
    Asset => "asset",
    Schedule => "schedule",
    Sensor => "sensor",
    Backfill => "backfill",
    System => "system",
});

open_enum!(EventKind {
    RunQueued => "run_queued",
    RunStarted => "run_started",
    RunSuccess => "run_success",
    RunFailed => "run_failed",
    RunCanceled => "run_canceled",
    RunReclaimed => "run_reclaimed",
    OpStarted => "op_started",
    OpExpanded => "op_expanded",
    OpRetry => "op_retry",
    OpSuccess => "op_success",
    OpFailed => "op_failed",
    OpSkipped => "op_skipped",
    OpCanceled => "op_canceled",
    TypeCheckFailed => "type_check_failed",
    AssetMaterialized => "asset_materialized",
    CheckPassed => "check_passed",
    CheckFailed => "check_failed",
    ScheduleFired => "schedule_fired",
    ScheduleCaughtUp => "schedule_caught_up",
    ScheduleSkipped => "schedule_skipped",
    ScheduleDeferred => "schedule_deferred",
    ScheduleError => "schedule_error",
    SensorTick => "sensor_tick",
    BackfillStarted => "backfill_started",
    BackfillChunk => "backfill_chunk",
    BackfillFinished => "backfill_finished",
    BackfillCanceled => "backfill_canceled",
    SchedulePaused => "schedule_paused",
    SensorPaused => "sensor_paused",
    NotificationDelivered => "notification_delivered",
    NotificationFailed => "notification_failed",
    RetentionPruned => "retention_pruned",
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
    /// who asked for this run, where a person did: the name of the
    /// [`Identity`](crate::Identity) the api recognized, and never a
    /// credential.
    ///
    /// `None` on everything a schedule, a sensor, a backfill or a freshness
    /// policy launched on its own — and on every launch through an
    /// unauthenticated deployment, which has nobody to name. an empty name is
    /// not "system": `Trigger::Manual` with no actor means a person asked and
    /// nothing was checking who.
    pub actor: Option<String>,
}

/// what happens to a run whose claimer stopped renewing its lease.
///
/// the default is [`Fail`](Reclaim::Fail), and the reason is that a run that
/// died halfway may already have done half of its side effects. re-running it
/// would do them again, quietly; failing it puts a stall in front of whoever
/// is on call. [`Requeue`](Reclaim::Requeue) is right when the work is
/// idempotent and available beats exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Reclaim {
    #[default]
    Fail,
    /// put it back on the queue for another claimer to take.
    Requeue,
}
str_enum!(Reclaim { Fail => "fail", Requeue => "requeue" });

/// what a process does about the queue: decide what goes on it, take things
/// off it, or both.
///
/// the split exists because the two halves have opposite multiplicities.
/// **exactly one** process should own the schedules, the sensors, the freshness
/// checks and the backfill chunking — those are decisions, and two processes
/// deciding independently is two of every scheduled run. **any number** of
/// processes may execute, which is the entire point of a claimable queue.
///
/// this is not [isolation](crate::Op::isolated), which is a different mechanism
/// that also spawns processes: an op subprocess runs one op and exits, and a
/// queue worker is a long-lived process that claims whole runs. a queue worker
/// spawns op subprocesses like any other hestan process does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// decide and execute: one process doing everything, which is the default
    /// and is right until it is not.
    #[default]
    All,
    /// decide only. schedules, sensors, freshness and backfill chunking run
    /// here; runs are enqueued and left for a worker.
    Scheduler,
    /// execute only. claims queued runs and runs them, and fires no schedule,
    /// evaluates no sensor and chunks no backfill.
    Worker,
}
str_enum!(Role { All => "all", Scheduler => "scheduler", Worker => "worker" });

impl Role {
    /// whether this process claims runs off the queue and executes them.
    pub fn executes(&self) -> bool {
        matches!(self, Role::All | Role::Worker)
    }

    /// whether this process owns the schedules, sensors, freshness checks and
    /// backfill chunking — the loops that decide what runs.
    pub fn decides(&self) -> bool {
        matches!(self, Role::All | Role::Scheduler)
    }
}

/// where one [durable notification](crate::Hestan::durable_notifications) has
/// got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliveryState {
    /// undelivered and due again; every row starts here.
    Pending,
    /// given up on after its attempts ran out, with `last_error` saying why.
    /// nothing will retry it, which is the point of saying so out loud rather
    /// than dropping it.
    Failed,
    Delivered,
}
str_enum!(DeliveryState {
    Pending => "pending",
    Failed => "failed",
    Delivered => "delivered",
});

/// one queued notification: an event that has to reach a hook even if the
/// process that recorded it does not survive to send it.
#[derive(Debug, Clone, Serialize)]
pub struct Notification {
    pub id: i64,
    /// which event shape `payload` holds; `run` today.
    pub kind: String,
    /// the event itself, as the hook will receive it.
    pub payload: Value,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
    /// when it is next due; `None` once nothing will try again.
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub delivered_at: Option<DateTime<Utc>>,
    /// what the last failed attempt said.
    pub last_error: Option<String>,
    pub state: DeliveryState,
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

/// one thing that happened, in the order it was written down.
///
/// `subject_kind` and `subject` say what it is about, and the pair is the whole
/// of how the log describes something that is not a run.
/// [`Store::event_log`](crate::Store::event_log) filters on them.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    /// monotonic within the table and allocated on insert. see
    /// [`Store::event_log`](crate::Store::event_log) for what that does and
    /// does not promise a reader following the log.
    pub seq: i64,
    /// the run this is about; `None` on everything that is not about a run.
    pub run_id: Option<String>,
    pub subject_kind: SubjectKind,
    /// which one, by name or id. `None` on a run event, where the run is
    /// `run_id`, and on a `system` event that is about no particular thing.
    pub subject: Option<String>,
    /// which op of the run, on the run events that have one.
    pub op: Option<String>,
    pub level: EventLevel,
    pub kind: EventKind,
    pub message: String,
    /// the payload, documented per kind in `docs/events.md`.
    pub data: Option<Value>,
    pub ts: DateTime<Utc>,
    /// who caused this, where a person did — the same name the run row
    /// carries, and `None` everywhere a loop did it on its own. see
    /// [`Run::actor`].
    pub actor: Option<String>,
}

impl Event {
    /// what this event is about, as one string: `subject`, or the run id on a
    /// run event. v17 does not copy `run_id` into `subject` — see the migration
    /// — so this is where the two become one answer.
    pub fn about(&self) -> Option<&str> {
        self.subject.as_deref().or(self.run_id.as_deref())
    }
}

/// which pipe of an [isolated op](crate::Op::isolated)'s process a captured
/// line came out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogStream {
    Stdout,
    Stderr,
}
str_enum!(LogStream { Stdout => "stdout", Stderr => "stderr" });

/// one line of what an op produced, as opposed to what it
/// [said](crate::OpCtx::info).
///
/// two mechanisms write these rows and each fills one half of the middle
/// three columns. an isolated op's [subprocess capture](crate::Op::isolated)
/// carries a `stream` and no `level` or `target`: a pipe has no levels. the
/// `capture` feature's tracing layer carries a `level` and a `target` and no
/// `stream`: an event was never on a pipe. see `docs/logs.md`.
#[derive(Debug, Clone, Serialize)]
pub struct OpLog {
    pub id: i64,
    pub run_id: String,
    pub op: String,
    /// which attempt of that op produced it, counting from 1.
    pub attempt: u32,
    pub at: DateTime<Utc>,
    pub stream: Option<LogStream>,
    pub level: Option<EventLevel>,
    pub target: Option<String>,
    pub message: String,
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

/// one point of a numeric metadata key's trend: what it was, and when. `run_id`
/// is null on a materialization a probe wrote outside any run.
#[derive(Debug, Clone, Serialize)]
pub struct MetaPoint {
    pub at: DateTime<Utc>,
    pub value: f64,
    pub run_id: Option<String>,
}

/// one entry of an asset's history as
/// [`Store::materializations`](crate::Store::materializations) reads it: the
/// build, and the two things only its neighbours can say about it.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub mat: Materialization,
    /// this build's fingerprint differs from the one before it in time, which
    /// is the difference between having been rebuilt and having changed.
    pub changed: bool,
    /// what the build before it reported, for the same `(asset, partition)` —
    /// what the deltas beside this entry's numbers are computed against.
    pub previous_metadata: Option<Value>,
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
