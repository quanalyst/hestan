//! dag-based job orchestration: ops wired into jobs, cron schedules, assets and
//! sensors, a sqlite run log, and an embedded web ui.

mod app;
mod asset;
mod backfill;
mod backoff;
mod error;
mod executor;
mod freshness;
mod graph;
#[cfg(feature = "http")]
mod http;
mod io;
// isolated ops are a unix feature and say so: `Op::isolated` off unix is a
// build error naming the platform, not a silent in-process fallback
#[cfg(unix)]
mod isolate;
mod job;
mod model;
#[cfg(feature = "http")]
pub mod notify;
mod op;
mod partition;
mod resource;
mod schedule;
mod sensor;
mod server;
mod store;

pub use app::Hestan;
pub use asset::{Asset, AssetCheck, CheckOutcome, CheckResult, MultiAsset};
pub use error::Error;
pub use executor::{
    Blocked, CancelOutcome, FailureHook, Limits, Queued, ResumePlan, RunFailure, Runner,
};
pub use freshness::{LateEvent, LateHook, LateKind};
#[cfg(feature = "http")]
pub use http::HttpSource;
pub use io::{FileIo, Inline, IoKey, IoManager, IoResult};
pub use job::{Graph, GraphBuilder, Job, JobBuilder};
pub use model::{
    AssetCheckRow, Backfill, BackfillStatus, Catchup, CheckStatus, Event, EventKind, EventLevel,
    Freshness, FreshnessRow, HistoryEntry, Materialization, OpRun, OpStatus, Overlap, Preset,
    Reclaim, Role, Run, RunStatus, RunTags, ScheduleRow, SensorOutcome, SensorRow, SensorTick,
    Severity, Tick, TickOutcome, Trigger, When,
};
pub use op::{InputError, META_TABLE_ROWS, Meta, MetaColumn, MetaTable, Op, OpCtx, OpResult};
pub use partition::Partitions;
pub use resource::ResourceCtx;
pub use schedule::Schedule;
pub use sensor::{RunRequest, RunStatusSensor, RunSummary, Sensor, SensorCtx};
pub use store::Store;

pub mod prelude {
    pub use crate::{
        Asset, AssetCheck, Catchup, CheckResult, Graph, Hestan, Job, Meta, MultiAsset, Op, OpCtx,
        OpResult, Partitions, RunRequest, RunStatus, RunStatusSensor, RunSummary, Schedule, Sensor,
        Severity,
    };
    pub use serde_json::{Value, json};
}
