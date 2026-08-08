//! dag-based job orchestration: ops wired into jobs, cron schedules, assets and
//! sensors, a sqlite run log, and an embedded web ui.

mod app;
mod asset;
mod error;
mod executor;
mod graph;
#[cfg(feature = "http")]
mod http;
mod job;
mod model;
#[cfg(feature = "http")]
pub mod notify;
mod op;
mod schedule;
mod sensor;
mod server;
mod store;

pub use app::Hestan;
pub use asset::Asset;
pub use error::Error;
pub use executor::{CancelOutcome, FailureHook, ResumePlan, RunFailure, Runner};
#[cfg(feature = "http")]
pub use http::HttpSource;
pub use job::{Job, JobBuilder};
pub use model::{
    Event, EventKind, EventLevel, Materialization, OpRun, OpStatus, Overlap, Run, RunStatus,
    ScheduleRow, SensorOutcome, SensorRow, SensorTick, Tick, TickOutcome, Trigger,
};
pub use op::{InputError, Op, OpCtx, OpResult};
pub use sensor::{RunRequest, Sensor, SensorCtx};
pub use store::Store;

pub mod prelude {
    pub use crate::{Asset, Hestan, Job, Op, OpCtx, OpResult, RunRequest, Sensor};
    pub use serde_json::{Value, json};
}
