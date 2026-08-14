use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

macro_rules! str_enum {
    ($ty:ident { $($variant:ident => $s:literal),+ $(,)? }) => {
        impl $ty {
            /// the stored word: what the column holds, what the api sends and
            /// what `FromStr` reads back. one spelling everywhere, so nothing
            /// between the database and the ui has to translate.
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

/// where a run has got to.
///
/// the last three are **terminal**: a run in one of them never moves again,
/// which is what [retention](crate::Retention), the queue and
/// [`RunStatusSensor`](crate::RunStatusSensor) all key off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// on the queue, claimed by nobody. a run waits here whether it is waiting
    /// for a free slot or for a process to exist at all.
    Queued,
    /// claimed and executing, somewhere.
    Running,
    /// no op terminally failed. ops the [trigger rules](When) skipped do not
    /// change that — a skip is a decision, not a failure.
    Success,
    /// an op ran out of retries, and [`Run::error`] names it.
    Failed,
    /// somebody asked it to stop. deliberately not a failure: paging on a
    /// cancellation teaches people to ignore the page.
    Canceled,
}
str_enum!(RunStatus {
    Queued => "queued",
    Running => "running",
    Success => "success",
    Failed => "failed",
    Canceled => "canceled",
});

/// where one op of a run has got to.
///
/// every op of a run gets a row when the run is created, so a run's op rows
/// are the plan as much as the record: an op nothing ever reached is
/// [`Pending`](OpStatus::Pending) forever, which is how a failed run shows what
/// it did not get to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpStatus {
    /// written when the run was created and not touched since.
    Pending,
    /// this attempt is executing — in this process, or in the child an
    /// [isolated](crate::Op::isolated) op spawned.
    Running,
    /// the body returned, and its output is on the row.
    Success,
    /// the last attempt failed. `attempts` says how many there were.
    Failed,
    /// its deps settled and its [trigger rule](When) said not to run it. an
    /// op skipped this way does not fail the run.
    Skipped,
    /// the run stopped before or during it.
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

/// what caused a run to exist.
///
/// it says *what* asked, never *who* — [`Run::actor`] is who, and the two are
/// separate because most runs have a cause and no person behind them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Trigger {
    /// somebody launched it: the ui, the cli, the api, or a call to
    /// [`Runner::launch`](crate::Runner::launch) in your own code.
    Manual,
    /// a cron occurrence came due, including one a
    /// [catch-up](Catchup) fired late.
    Schedule,
    /// a fresh run of an earlier run's job and params, from the beginning.
    Retry,
    /// a re-run that seeds the ops that already succeeded — see
    /// [`ResumePlan`](crate::ResumePlan).
    Resume,
    /// a re-run of ops that already ran, on the inputs they were given — see
    /// [`ReplayPlan`](crate::ReplayPlan). the opposite of a resume, which
    /// re-runs what did *not* succeed.
    Replay,
    /// an asset was materialized, whether asked for by hand, by a freshness
    /// policy or by a [backfill](Backfill) chunk.
    Build,
    /// a [sensor](crate::Sensor) evaluation asked for it.
    Sensor,
}
str_enum!(Trigger {
    Manual => "manual",
    Schedule => "schedule",
    Retry => "retry",
    Resume => "resume",
    Replay => "replay",
    Build => "build",
    Sensor => "sensor",
});

/// how loud one [`Event`] is.
///
/// three levels rather than the five a logging crate has, because this is a
/// filter an operator uses rather than a knob a developer tunes: the question
/// a run page is asked is "show me the errors".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EventLevel {
    /// something happened as intended.
    Info,
    /// something worth noticing that stopped nothing: a retry, a skipped
    /// occurrence, a canceled run.
    Warn,
    /// something failed.
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
    /// a run was created. it exists from here, whether or not anything is free
    /// to execute it.
    RunQueued,
    /// a run was claimed and began executing. the gap back to
    /// [`RunQueued`](EventKind::RunQueued) is how long it waited.
    RunStarted,
    /// a run ended with nothing failed.
    RunSuccess,
    /// a run ended failed, and `failed_op` in the payload names what did it.
    RunFailed,
    /// a run was stopped by somebody.
    RunCanceled,
    /// a claim expired and the run was taken back from whoever held it.
    RunReclaimed,
    /// one **attempt** of an op began.
    OpStarted,
    /// a [fan-out](crate::Op::mapped) resolved into its instances, and the
    /// payload says how many.
    OpExpanded,
    /// an attempt failed and another is coming. the failure that ends an op is
    /// [`OpFailed`](EventKind::OpFailed), so counting these counts retries.
    OpRetry,
    /// an op produced its output. the payload carries whatever the attempt
    /// reported with [`OpCtx::meta`](crate::OpCtx::meta), so a consumer
    /// following the log alone sees the row counts without fetching anything.
    OpSuccess,
    /// an op ran out of attempts.
    OpFailed,
    /// an op's deps settled and its [trigger rule](When) said not to run it.
    OpSkipped,
    /// an op the run stopped, with `stopped` saying whether it was seen to
    /// stop or only asked to.
    OpCanceled,
    /// a [typed op](crate::Op::typed)'s inputs or output did not deserialize.
    /// a kind of its own so that a wiring mistake reads differently from a
    /// service that was down.
    TypeCheckFailed,
    /// an asset (or one partition of one) was built and recorded.
    AssetMaterialized,
    /// an [asset check](crate::AssetCheck) passed.
    CheckPassed,
    /// an asset check failed. its level follows the check's
    /// [severity](Severity) rather than the verdict, so a `warn` check that
    /// failed is not an error in the log.
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
    /// a backfill was requested, over the keys the range resolved to then.
    BackfillStarted,
    /// one chunk of a backfill was launched.
    BackfillChunk,
    /// every chunk of a backfill is accounted for. a range that resolved to no
    /// keys at all is started and finished in the same instant, and both are
    /// written rather than one of them being suppressed.
    BackfillFinished,
    /// a backfill was stopped: the chunk in flight is canceled with it, and no
    /// further chunk is launched.
    BackfillCanceled,
    /// a schedule was paused or unpaused; `paused` in the payload says which.
    SchedulePaused,
    /// a sensor was paused or unpaused, the same way.
    SensorPaused,
    /// a [durable notification](crate::Hestan::durable_notifications) reached
    /// its hook.
    NotificationDelivered,
    /// a notification hestan has stopped trying to deliver.
    NotificationFailed,
    /// what one job's [retention policy](crate::Retention) deleted.
    RetentionPruned,
    /// a line an op said with [`OpCtx::info`](crate::OpCtx::info) and its two
    /// siblings. the op talking, as opposed to hestan talking about the op.
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
            /// the stored word — including an unknown one, given back exactly
            /// as it was read, so a build that does not know a kind still
            /// reports what the writer called it.
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

/// how one occurrence of a schedule was accounted for.
///
/// every occurrence the scheduler passes gets one of these, including the ones
/// it decided not to fire — a schedule that launched nothing last night is a
/// question the [tick log](Tick) has to be able to answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TickOutcome {
    /// a run was launched, and `run_id` on the tick names it.
    Fired,
    /// the launch itself failed — a validation error, a store that was not
    /// there. the occurrence is still accounted for.
    Error,
    /// deliberately not fired: an [overlap policy](Overlap), a
    /// [catch-up](Catchup) rule, or a declaration that has since gone.
    Skipped,
    /// held back under [`Overlap::Queue`] until the job is free, and still
    /// waiting.
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
    /// the window closed, and it is `by` past it.
    Late {
        /// how far past the deadline, now.
        by: Duration,
    },
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

    /// whether a policy is being broken right now. [`Never`](Freshness::Never)
    /// is not: nothing has succeeded, so there is no age to be past.
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
    /// the job or the asset, by name.
    pub name: String,
    /// what the last check concluded. the row exists to compare against, so
    /// this is the state a crossing is measured from rather than a fresh
    /// verdict — read [`Freshness`] for that.
    pub late: bool,
    /// when it went late; `None` while it is not.
    pub since: Option<DateTime<Utc>>,
}

/// what a schedule does when it fires while the job still has an active run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Overlap {
    /// fire anyway, and let the two runs overlap. right when the job is
    /// idempotent and lateness costs more than duplication.
    Allow,
    /// do not fire, and record the occurrence as
    /// [skipped](TickOutcome::Skipped). the default, because two of a job that
    /// writes somewhere is the failure mode nobody wants by accident.
    #[default]
    Skip,
    /// hold the occurrence until the job is free, then fire it. **one at a
    /// time**: a second occurrence arriving while one is already held is
    /// skipped like any other, so a long run cannot build a backlog to release
    /// all at once.
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
    All {
        /// the most occurrences one catch-up pass will fire. a cap rather than
        /// an option: a schedule that was down for a month is a month of runs
        /// arriving at once, and nobody means that.
        limit: usize,
    },
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

/// one execution of a job, as the run log holds it.
///
/// the row outlives the process that made it and the code that defined the
/// job: a run of a job this build no longer defines still reads back, still
/// shows its ops, and simply cannot be executed again. that is why every
/// reference here is a name rather than a handle.
#[derive(Debug, Clone, Serialize)]
pub struct Run {
    /// a uuid v7, so ids sort by creation time and a page of runs can be
    /// ordered by id when two were created in the same millisecond.
    pub id: String,
    /// which job, by name. nothing checks that this process still defines it.
    pub job: String,
    /// where it has got to. not monotonic: a run whose claimer went quiet
    /// under [`Reclaim::Requeue`] goes back from running to queued.
    pub status: RunStatus,
    /// what caused it — never who. `actor` is who, and most runs have a cause
    /// and nobody behind it.
    pub trigger: Trigger,
    /// what the launch was given, handed unchanged to every op that runs. `{}`
    /// on a launch that passed nothing.
    pub params: Value,
    /// when the run was written down, which is when it joined the queue and
    /// not when anything began.
    pub created_at: DateTime<Utc>,
    /// when execution began. `None` while it is still queued — and forever on
    /// a run nothing ever got to.
    pub started_at: Option<DateTime<Utc>>,
    /// when it reached a terminal status. `None` until it does.
    pub finished_at: Option<DateTime<Utc>>,
    /// why the run failed: the first op that terminally failed, named, as
    /// `"op {name} failed: {message}"`. `None` on a run that never failed.
    pub error: Option<String>,
    /// the run this one resumed, for a run launched by
    /// [`Runner::resume_from`](crate::Runner::resume_from); `None` otherwise.
    pub resumed_from: Option<String>,
    /// the run this one replayed, for a run launched by
    /// [`Runner::replay`](crate::Runner::replay); `None` otherwise.
    ///
    /// a column of its own rather than a second meaning for `resumed_from`,
    /// because the two say opposite things about what was re-run: a resume
    /// continues a run from where it broke, and a replay re-runs what already
    /// ran. never both.
    pub replay_of: Option<String>,
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
    /// when the claim was taken, which is what `started_at` follows within a
    /// moment.
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
    /// mark it failed and leave it for a person. the default.
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
    /// the hook took it. it stays until [retention](crate::Retention) sweeps
    /// it, which is what makes "was this delivered?" a question with an
    /// answer.
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
    /// the queue position, allocated on insert; delivery goes in this order.
    pub id: i64,
    /// which event shape `payload` holds; `run` today.
    pub kind: String,
    /// the event itself, as the hook will receive it.
    pub payload: Value,
    /// when the event happened, not when delivery was last tried.
    pub created_at: DateTime<Utc>,
    /// how many attempts have been made, so a row that is retrying says how
    /// hard.
    pub attempts: u32,
    /// when it is next due; `None` once nothing will try again.
    pub next_attempt_at: Option<DateTime<Utc>>,
    /// when it got through; `None` on everything that has not.
    pub delivered_at: Option<DateTime<Utc>>,
    /// what the last failed attempt said.
    pub last_error: Option<String>,
    /// where delivery has got to. `failed` is a decision to stop trying, not a
    /// transient error — the row stays as the record of one that never landed.
    pub state: DeliveryState,
}

/// a named parameter set stored against one job: what
/// [`Hestan::preset`](crate::Hestan::preset) declares and what the launchpad
/// saves. runtime data rather than part of the job definition — the ui creates
/// and deletes them, and a declared one is only ever seeded — so it lives in
/// the store beside the run log rather than on [`Job`](crate::Job).
#[derive(Debug, Clone, Serialize)]
pub struct Preset {
    /// the job it launches, and half of its identity: two jobs may each have a
    /// preset called `backfill`.
    pub job: String,
    /// what the launchpad lists it under, and the other half of its identity.
    pub name: String,
    /// the params a launch from this preset starts with. an editable starting
    /// point, not a constraint — nothing stops the launch changing them.
    pub params: Value,
    /// when the preset was first stored; a rewrite keeps it.
    pub created_at: DateTime<Utc>,
}

/// one op of one run: the record of an op, however many attempts it took.
///
/// there is a row per op from the moment the run is created, so the set of rows
/// is the run's plan and not only its history. a fan-out's instances get a row
/// each, named `{op}[{i}]`.
#[derive(Debug, Clone, Serialize)]
pub struct OpRun {
    /// the run this belongs to; `(run_id, op)` is the row's identity.
    pub run_id: String,
    /// the op's name in the job, flattened: an op inside a
    /// [`Graph`](crate::Graph) instance reads `{instance}.{op}`.
    pub op: String,
    /// where this op got to. `pending` on a finished run means the run never
    /// reached it.
    pub status: OpStatus,
    /// how many attempts have been made, counting from 1 once one has.
    pub attempts: u32,
    /// when the **first** attempt started; a retry does not move it, so this
    /// with `finished_at` is how long the op took including its retries.
    pub started_at: Option<DateTime<Utc>>,
    /// when the op reached a terminal status.
    pub finished_at: Option<DateTime<Utc>>,
    /// what the body returned — or, under an [`IoManager`](crate::IoManager),
    /// the handle it stored the value under rather than the value.
    pub output: Option<Value>,
    /// typed facts the op reported with [`OpCtx::meta`](crate::OpCtx::meta),
    /// one tagged value per name. `None` when it reported nothing.
    pub metadata: Option<Value>,
    /// what the last attempt failed with; `None` on an op that succeeded. the
    /// earlier attempts' messages are in the [event log](Event), not here.
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
    /// which of hestan's tables `subject` names.
    pub subject_kind: SubjectKind,
    /// which one, by name or id. `None` on a run event, where the run is
    /// `run_id`, and on a `system` event that is about no particular thing.
    pub subject: Option<String>,
    /// which op of the run, on the run events that have one.
    pub op: Option<String>,
    /// how loud. hestan picks it per kind, so filtering on it filters on what
    /// happened rather than on how somebody phrased it — except on a
    /// [`Log`](EventKind::Log) event, where the op chose.
    pub level: EventLevel,
    /// what happened, and the half of this row a program should read.
    pub kind: EventKind,
    /// one line for a person, in hestan's own words. the machine-readable half
    /// is `kind` and `data` — nothing should be parsed out of this.
    pub message: String,
    /// the payload, documented per kind in `docs/events.md`.
    pub data: Option<Value>,
    /// when it happened, as the writer saw the clock. ordering by this is not
    /// ordering by `seq`: several processes write here.
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
    /// fd 1 of the child.
    Stdout,
    /// fd 2 of the child. it is not a level: plenty of programs write ordinary
    /// progress here, and hestan does not promote it to an error.
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
    /// allocated on insert, and the cursor a tail pages by: it only ever goes
    /// up, which timestamps on lines a hundred microseconds apart do not.
    pub id: i64,
    /// the run this line was produced in.
    pub run_id: String,
    /// the op that produced it.
    pub op: String,
    /// which attempt of that op produced it, counting from 1.
    pub attempt: u32,
    /// when hestan received the line, which for a subprocess is when it was
    /// read off the pipe rather than when the child printed it.
    pub at: DateTime<Utc>,
    /// which pipe, on a line from a subprocess; `None` on a captured event.
    pub stream: Option<LogStream>,
    /// how loud, on a captured event; `None` on a line off a pipe.
    pub level: Option<EventLevel>,
    /// the tracing target on a captured event, which is a module path — except
    /// on the lines hestan writes about capture itself, which are just
    /// `hestan`.
    pub target: Option<String>,
    /// the line, `\n` stripped and clipped if it was enormous.
    pub message: String,
}

/// one declared schedule as the store holds it.
///
/// the declaration lives in code; what is stored is the state a restart must
/// not lose — whether it is paused, and how far the scheduler has got. a
/// schedule the code no longer declares is deleted from here on the next start.
#[derive(Debug, Clone, Serialize)]
pub struct ScheduleRow {
    /// the job it fires, and half of the row's identity.
    pub job: String,
    /// the cron expression, and the other half: changing the expression is a
    /// new schedule rather than an edit, so the old one's cursor cannot be
    /// read as if it were this one's.
    pub expr: String,
    /// the zone the expression is read in — which is what makes `0 3 * * *`
    /// survive a daylight-saving change.
    pub tz: String,
    /// a paused schedule is still declared and still listed; its occurrences
    /// are stepped over, `cursor` and all, so unpausing does not fire a
    /// backlog.
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

/// one occurrence of one schedule, and what the scheduler did about it.
///
/// this is where "why did nothing run last night" is answered: an occurrence
/// that was deliberately not fired leaves a row here, so silence in the run
/// log is never the only evidence. capped to the newest few thousand rows,
/// whatever any retention policy says, because it grows with time rather than
/// with what you keep.
#[derive(Debug, Clone, Serialize)]
pub struct Tick {
    /// allocated on insert; ordering by it is ordering by when the scheduler
    /// dealt with the occurrence.
    pub id: i64,
    /// the job the schedule fires.
    pub job: String,
    /// the expression, so ticks from a schedule that has since been rewritten
    /// still say which one they were.
    pub expr: String,
    /// the occurrence itself: the logical time this tick is about.
    pub scheduled_for: DateTime<Utc>,
    /// when the scheduler dealt with it, which on a
    /// [caught-up](Catchup) occurrence is well after `scheduled_for`.
    pub fired_at: DateTime<Utc>,
    /// what became of the occurrence. a [`Deferred`](TickOutcome::Deferred)
    /// tick with no later tick for the same occurrence **is** the held fire —
    /// [`Overlap::Queue`] keeps nothing in memory, so a fire held when the
    /// process died is still held when it comes back.
    pub outcome: TickOutcome,
    /// the run this launched; `None` on every outcome but
    /// [`Fired`](TickOutcome::Fired).
    pub run_id: Option<String>,
    /// why the launch failed, on an [`Error`](TickOutcome::Error) tick.
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
    /// which asset, by name.
    pub asset: String,
    /// the key this entry is for, on a [partitioned
    /// asset](crate::Partitions); `None` for an unpartitioned one.
    pub partition: Option<String>,
    /// what this build produced, as one string. two entries with the same one
    /// are the same content, which is how a downstream asset knows whether an
    /// upstream rebuild actually changed anything.
    pub fingerprint: String,
    /// the fingerprint of each dep as this build consumed it, by name. this is
    /// what staleness compares against — an asset is stale when a dep's
    /// current fingerprint is not the one recorded here.
    pub inputs: Value,
    /// what the [io manager](crate::IoManager) returned for the value: the
    /// value itself under [`Inline`](crate::Inline), a handle under a manager
    /// that stored it somewhere. `None` for a source asset and for anything
    /// that only recorded that it happened.
    ///
    /// read it back with the manager the asset stores through, exactly as an
    /// op's output is read back — a row written before assets went through a
    /// manager holds the value, and every manager passes through what it did
    /// not write.
    pub value: Option<Value>,
    /// where the build happened; `None` on one a probe recorded outside any
    /// run.
    pub run_id: Option<String>,
    /// when it was recorded.
    pub built_at: DateTime<Utc>,
    /// what the op that built this reported with
    /// [`OpCtx::meta`](crate::OpCtx::meta) — the same map its op run carries.
    pub metadata: Option<Value>,
}

/// one point of a numeric metadata key's trend: what it was, and when. `run_id`
/// is null on a materialization a probe wrote outside any run.
#[derive(Debug, Clone, Serialize)]
pub struct MetaPoint {
    /// when the build that reported it landed.
    pub at: DateTime<Utc>,
    /// the number, widened to `f64` whichever numeric [`Meta`](crate::Meta) it
    /// was reported as — a trend is a shape, not an exact count.
    pub value: f64,
    /// the run it was reported in.
    pub run_id: Option<String>,
}

/// one entry of an asset's history as
/// [`Store::materializations`](crate::Store::materializations) reads it: the
/// build, and the two things only its neighbours can say about it.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// the build itself.
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
    /// record the failure and carry on. for a check that describes the data
    /// rather than gates it.
    Warn,
    /// fail the check's op, and with it the run that produced the asset.
    #[default]
    Error,
}
str_enum!(Severity { Warn => "warn", Error => "error" });

/// what a check said about the value it was handed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    /// the check was satisfied.
    Passed,
    /// it was not. what that costs is [`Severity`], and the two are recorded
    /// separately so a failed `warn` check is still a failed check.
    Failed,
}
str_enum!(CheckStatus { Passed => "passed", Failed => "failed" });

/// one recorded check result. `failed` with severity `warn` is a run that
/// succeeded and a check that did not — the two are recorded separately on
/// purpose.
#[derive(Debug, Clone, Serialize)]
pub struct AssetCheckRow {
    /// allocated on insert; the newest row per `(asset, partition, check)` is
    /// the current verdict.
    pub id: i64,
    /// the asset that was checked.
    pub asset: String,
    /// the key that was checked, on a [partitioned
    /// asset](crate::Partitions); `None` for an unpartitioned one.
    pub partition: Option<String>,
    /// which check, by the name it was declared under.
    pub check: String,
    /// the run whose build this checked; checks only ever run inside one.
    pub run_id: String,
    /// what it said.
    pub status: CheckStatus,
    /// what a failure cost, as declared when the check ran. stored per row, so
    /// changing a check's severity does not rewrite what it used to mean.
    pub severity: Severity,
    /// what the check had to say about it, if anything.
    pub message: Option<String>,
    /// what the check reported with `CheckResult::meta`, tagged by type like
    /// [op metadata](crate::Meta).
    pub metadata: Option<Value>,
    /// when the check ran.
    pub checked_at: DateTime<Utc>,
}

/// how a [backfill](crate::Hestan) ended, derived from the runs it launched.
/// `running` covers a chunk in flight and the pause between chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackfillStatus {
    /// chunks are still being launched, or one is in flight.
    Running,
    /// every partition it was asked for has been built.
    Complete,
    /// a chunk failed, so the rest were not launched.
    Failed,
    /// somebody stopped it partway. what it already built stays built.
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
    /// what the api and the run tags refer to it by.
    pub id: i64,
    /// the asset whose partitions are being built.
    pub asset: String,
    /// the range as it was asked for, inclusive, in the asset's key vocabulary
    /// rather than as timestamps.
    pub from_key: String,
    /// the other end of that range, also inclusive.
    pub to_key: String,
    /// every key the range resolved to, in build order.
    pub partitions: Vec<String>,
    /// one per chunk launched, oldest first.
    pub run_ids: Vec<String>,
    /// how many keys there are to build — `partitions.len()`, stored so
    /// progress can be reported without reading the list.
    pub total: usize,
    /// how many of them have been handed to a run. `launched == total` with a
    /// chunk still in flight is a backfill that is nearly, not quite, done.
    pub launched: usize,
    /// when it was requested.
    pub created_at: DateTime<Utc>,
    /// when it stopped being running, however it ended.
    pub finished_at: Option<DateTime<Utc>>,
    /// how it ended. derived from the runs it launched rather than reported by
    /// anything: a chunk that failed is a backfill that failed.
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

/// one declared sensor as the store holds it: what a restart must not lose.
///
/// the closure and its interval live in code. what is here is the state that
/// makes a sensor pick up where it left off — and a sensor the code no longer
/// declares is deleted from here on the next start, cursor and all.
#[derive(Debug, Clone, Serialize)]
pub struct SensorRow {
    /// the name it was declared under, and its identity everywhere.
    pub name: String,
    /// a paused sensor is not evaluated. its cursor stays where it was, so
    /// unpausing resumes rather than starts over.
    pub paused: bool,
    /// whatever the closure last committed with
    /// [`SensorCtx::set_cursor`](crate::SensorCtx::set_cursor) — hestan stores
    /// it and never reads into it. `None` until one is set.
    pub cursor: Option<Value>,
    /// when the cursor last moved.
    pub updated_at: DateTime<Utc>,
}

/// how a sensor evaluation ended: `fired` means the closure returned and every
/// requested run launched, possibly zero of them. `skipped` is a turn the loop
/// did not evaluate at all, because the previous evaluation was still going.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SensorOutcome {
    /// the closure returned, and every run it asked for was launched — which
    /// may be none, and usually is.
    Fired,
    /// the closure returned an error or ran out of time. nothing was launched
    /// and the staged cursor was not committed.
    Error,
    /// the loop came round and did not evaluate: the previous evaluation was
    /// still going.
    Skipped,
}
str_enum!(SensorOutcome {
    Fired => "fired",
    Error => "error",
    Skipped => "skipped",
});

/// one evaluation of one sensor, however it ended.
///
/// a sensor that launches nothing for a week still writes a row every turn,
/// which is the difference between "nothing to do" and "not running". capped
/// to the newest few thousand rows for the same reason the
/// [schedule ticks](Tick) are.
#[derive(Debug, Clone, Serialize)]
pub struct SensorTick {
    /// allocated on insert; newest first is ordering by it.
    pub id: i64,
    /// which sensor, by name.
    pub sensor: String,
    /// when the evaluation started.
    pub evaluated_at: DateTime<Utc>,
    /// how it ended. on a [`Skipped`](SensorOutcome::Skipped) tick nothing was
    /// evaluated, so the counts beside it are zeros rather than measurements.
    pub outcome: SensorOutcome,
    /// how many runs this evaluation actually launched.
    pub launched: u32,
    /// requests this evaluation did not launch because their [run
    /// key](crate::RunRequest::key) had already been claimed.
    pub skipped: u32,
    /// how long the evaluation took. 0 on a `skipped` tick, which records a
    /// turn that was never taken.
    pub duration_ms: u64,
    /// what went wrong, on an [`Error`](SensorOutcome::Error) tick.
    pub error: Option<String>,
}

pub(crate) fn new_run_id() -> String {
    uuid::Uuid::now_v7().to_string()
}
