//! dag-based job orchestration: ops wired into jobs, cron schedules, assets and
//! sensors, a sqlite run log, and an embedded web ui.

mod app;
mod asset;
mod backoff;
mod error;
mod executor;
mod graph;
#[cfg(feature = "http")]
mod http;
mod io;
mod job;
mod model;
#[cfg(feature = "http")]
pub mod notify;
mod op;
mod resource;
mod schedule;
mod sensor;
mod server;
mod store;

pub use app::Hestan;
pub use asset::{Asset, AssetCheck, CheckOutcome, CheckResult};
pub use error::Error;
pub use executor::{CancelOutcome, FailureHook, ResumePlan, RunFailure, Runner};
#[cfg(feature = "http")]
pub use http::HttpSource;
pub use io::{FileIo, Inline, IoKey, IoManager, IoResult};
pub use job::{Graph, GraphBuilder, Job, JobBuilder};
pub use model::{
    AssetCheckRow, CheckStatus, Event, EventKind, EventLevel, Materialization, OpRun, OpStatus,
    Overlap, Run, RunStatus, ScheduleRow, SensorOutcome, SensorRow, SensorTick, Severity, Tick,
    TickOutcome, Trigger, When,
};
pub use op::{InputError, Meta, Op, OpCtx, OpResult};
pub use resource::ResourceCtx;
pub use sensor::{RunRequest, Sensor, SensorCtx};
pub use store::Store;

pub mod prelude {
    pub use crate::{
        Asset, AssetCheck, CheckResult, Graph, Hestan, Job, Meta, Op, OpCtx, OpResult, RunRequest,
        Sensor, Severity,
    };
    pub use serde_json::{Value, json};
}
