//! dag-based job orchestration: ops wired into jobs, cron schedules, assets and
//! sensors, a run log on sqlite or postgres, and an embedded web ui.

// a public item with no rustdoc is a gap nothing reports: the build stays
// green, the docs page grows a bare signature, and the number only ever goes
// up. so it is an error, under every feature combination — a feature-gated
// item is exactly the one nobody notices is bare
#![deny(missing_docs)]
// and a link to an item that was renamed is worse than no link: it reads as a
// promise that the thing on the other end still exists
#![deny(rustdoc::broken_intra_doc_links)]

mod app;
mod asset;
// who may drive this deployment, and the refusal that keeps an unguarded one
// off any address but loopback
pub mod auth;
mod backfill;
mod backoff;
// the tracing layer a host composes into its own subscriber. optional because
// hestan installs no subscriber and will not make anyone depend on one. public
// so that what it deliberately does not capture is written somewhere a reader
// lands on, rather than in a private module rustdoc never renders
#[cfg(feature = "capture")]
pub mod capture;
// the command line this binary already knows everything to serve. optional
// because it is a dependency on an argument parser, and because owning argv is
// something a host asks for rather than something a library takes
#[cfg(feature = "cli")]
pub mod cli;
mod error;
mod executor;
mod freshness;
mod graph;
mod hooks;
#[cfg(feature = "http")]
mod http;
mod io;
// isolated ops are a unix feature and say so: `Op::isolated` off unix is a
// build error naming the platform, not a silent in-process fallback
#[cfg(unix)]
mod isolate;
mod job;
mod logs;
mod model;
#[cfg(feature = "http")]
pub mod notify;
mod op;
// a run as a distributed trace. optional because it is a dependency on the
// opentelemetry crates, and off because hestan installs no exporter
#[cfg(feature = "otel")]
pub mod otel;
mod partition;
// a shared run log on a postgres server. optional because sqlite is the right
// default for one process and needs no server at all
#[cfg(feature = "postgres")]
mod pg;
mod resource;
mod retention;
mod schedule;
mod sensor;
mod server;
mod store;

pub use app::Hestan;
pub use asset::{Asset, AssetCheck, CheckOutcome, CheckResult, MultiAsset};
pub use auth::{Access, Auth, Identity};
#[cfg(feature = "capture")]
pub use capture::{CaptureLayer, capture_layer};
pub use error::Error;
pub use executor::{Blocked, CancelOutcome, Limits, Queued, ResumePlan, Runner};
pub use freshness::{LateEvent, LateHook, LateKind};
pub use hooks::{FailureHook, OpEvent, OpHook, RunEvent, RunFailure, RunHook};
#[cfg(feature = "http")]
pub use http::HttpSource;
pub use io::{FileIo, Inline, IoKey, IoManager, IoResult};
pub use job::{Graph, GraphBuilder, Job, JobBuilder};
pub use model::EVENT_SCHEMA;
pub use model::{
    AssetCheckRow, Backfill, BackfillStatus, Catchup, CheckStatus, DeliveryState, Event, EventKind,
    EventLevel, Freshness, FreshnessRow, HistoryEntry, LogStream, Materialization, MetaPoint,
    Notification, OpLog, OpRun, OpStatus, Overlap, Preset, Reclaim, Role, Run, RunStatus, RunTags,
    ScheduleRow, SensorOutcome, SensorRow, SensorTick, Severity, SubjectKind, Tick, TickOutcome,
    Trigger, When,
};
pub use op::{InputError, META_TABLE_ROWS, Meta, MetaColumn, MetaTable, Op, OpCtx, OpResult};
pub use partition::Partitions;
pub use resource::ResourceCtx;
pub use retention::Retention;
pub use schedule::Schedule;
pub use sensor::{RunRequest, RunStatusSensor, RunSummary, Sensor, SensorCtx};
pub use store::{EventQuery, Settled, Store};

/// what a file that defines jobs, ops and assets needs, in one import.
///
/// deliberately small: the types you name when writing a pipeline, plus
/// `serde_json`'s [`Value`](serde_json::Value) and
/// [`json!`](serde_json::json), which every op body ends up touching. the
/// configuration surface — [`Auth`], [`Limits`], [`Retention`], [`Store`] —
/// is not here, because that is written once in `main` and reads better named.
pub mod prelude {
    pub use crate::{
        Asset, AssetCheck, Catchup, CheckResult, Graph, Hestan, Job, Meta, MultiAsset, Op, OpCtx,
        OpResult, Partitions, RunRequest, RunStatus, RunStatusSensor, RunSummary, Schedule, Sensor,
        Severity,
    };
    pub use serde_json::{Value, json};
}
