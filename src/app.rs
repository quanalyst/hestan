use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use serde_json::Value;

use crate::asset::{Asset, AssetRegistry, mats_map, plan_target};
use crate::error::Error;
use crate::executor::{FailureHook, RunFailure, Runner};
use crate::job::Job;
use crate::model::{Run, Trigger};
use crate::schedule::{self, ScheduleEntry};
use crate::sensor::{Sensor, SensorEntry, SensorEval, run_sensors};
use crate::server::{AppState, SensorInfo, router};
use crate::store::Store;

/// entry point: collect jobs, assets, sensors and schedules, then `serve` the ui
/// or `run_once` headless.
pub struct Hestan {
    jobs: Vec<Job>,
    schedules: Vec<(String, String, String)>,
    assets: Vec<Asset>,
    sensors: Vec<Sensor>,
    db_path: String,
    hooks: Vec<FailureHook>,
    retention_days: Option<u32>,
    #[cfg(feature = "http")]
    sources: Vec<crate::http::HttpSource>,
}

impl Default for Hestan {
    fn default() -> Self {
        Hestan {
            jobs: Vec::new(),
            schedules: Vec::new(),
            assets: Vec::new(),
            sensors: Vec::new(),
            db_path: "hestan.db".into(),
            hooks: Vec::new(),
            retention_days: None,
            #[cfg(feature = "http")]
            sources: Vec::new(),
        }
    }
}

impl Hestan {
    /// an empty builder; nothing is opened or validated until `serve`,
    /// `run_once`, or `build_asset`.
    pub fn new() -> Hestan {
        Hestan::default()
    }

    /// register a job; stackable. duplicate names collide at build.
    pub fn job(mut self, job: Job) -> Self {
        self.jobs.push(job);
        self
    }

    pub fn jobs(mut self, jobs: impl IntoIterator<Item = Job>) -> Self {
        self.jobs.extend(jobs);
        self
    }

    /// register assets; stackable. when any exist, build registers the
    /// internal job `"assets"` that materializes them — a user job with that
    /// name then collides ([`Error::DuplicateJob`]).
    pub fn assets(mut self, assets: impl IntoIterator<Item = Asset>) -> Self {
        self.assets.extend(assets);
        self
    }

    /// register a sensor; stackable. `serve` runs one loop evaluating every
    /// sensor (and every source probe) on its interval.
    pub fn sensor(mut self, sensor: Sensor) -> Self {
        self.sensors.push(sensor);
        self
    }

    /// attach a 5-field cron expression to a job, evaluated in utc;
    /// validated in serve/run_once.
    pub fn schedule(mut self, job: impl Into<String>, cron_expr: impl Into<String>) -> Self {
        self.schedules
            .push((job.into(), cron_expr.into(), "UTC".into()));
        self
    }

    /// like [`Hestan::schedule`] but evaluated in a named iana timezone.
    pub fn schedule_tz(
        mut self,
        job: impl Into<String>,
        cron_expr: impl Into<String>,
        tz: impl Into<String>,
    ) -> Self {
        self.schedules
            .push((job.into(), cron_expr.into(), tz.into()));
        self
    }

    /// register an http source: build lowers it into a job named after the
    /// source, plus a schedule if `cron` was set.
    #[cfg(feature = "http")]
    pub fn source(mut self, src: crate::http::HttpSource) -> Self {
        self.sources.push(src);
        self
    }

    /// where the run log lives; defaults to `hestan.db`. `":memory:"` works
    /// for tests and keeps nothing.
    pub fn db(mut self, path: impl Into<String>) -> Self {
        self.db_path = path.into();
        self
    }

    /// at startup, delete terminal runs older than `days` days with their op runs
    /// and events. active runs and op state survive; the default keeps everything.
    pub fn retention_days(mut self, days: u32) -> Self {
        self.retention_days = Some(days);
        self
    }

    /// call `hook` whenever a run finishes failed — never on success, on cancel,
    /// or for runs the startup sweep marks failed. callable multiple times.
    pub fn on_failure(mut self, hook: impl Fn(RunFailure) + Send + Sync + 'static) -> Self {
        self.hooks.push(Arc::new(hook));
        self
    }

    pub async fn run_once(self, job: &str, params: Value) -> Result<Run, Error> {
        let built = self.build()?;
        built.runner.run(job, params, Trigger::Manual).await
    }

    /// materialize `name` headless, like [`run_once`](Self::run_once): one run of
    /// its stale ancestors plus the target, which always rebuilds. check
    /// `GET /api/assets` first if you only want to build when stale.
    pub async fn build_asset(self, name: &str) -> Result<Run, Error> {
        let built = self.build()?;
        let mats = mats_map(built.runner.store())?;
        let plan = plan_target(&built.registry, &mats, name)?;
        built
            .runner
            .run_subset(
                crate::asset::ASSETS_JOB,
                plan.ops.into_iter().collect(),
                plan.seeds,
                serde_json::json!({}),
                Trigger::Build,
            )
            .await
    }

    pub async fn serve(self, addr: impl Into<SocketAddr>) -> Result<(), Error> {
        let addr = addr.into();
        let built = self.build()?;
        // bind before spawning the loops: a bind failure must not leave detached
        // tasks firing jobs into a server that never started
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let scheduler = tokio::spawn(schedule::run_scheduler(built.entries, built.runner.clone()));
        let sensor_infos: Vec<SensorInfo> = built
            .sensor_entries
            .iter()
            .map(|e| SensorInfo {
                name: e.name.clone(),
                every: e.every,
            })
            .collect();
        let sensors = tokio::spawn(run_sensors(
            built.sensor_entries,
            built.runner.clone(),
            built.registry.clone(),
        ));
        let state = AppState {
            jobs: Arc::new(built.runner.jobs().clone()),
            runner: built.runner,
            assets: built.registry,
            sensors: Arc::new(sensor_infos),
        };
        tracing::info!("hestan ui on http://{addr}");
        let served = axum::serve(listener, router(state)).await;
        scheduler.abort();
        sensors.abort();
        served?;
        Ok(())
    }

    fn build(self) -> Result<Built, Error> {
        #[cfg(feature = "http")]
        let (mut jobs, schedules) = {
            let (mut jobs, mut schedules) = (self.jobs, self.schedules);
            for src in &self.sources {
                let name = src
                    .name
                    .clone()
                    .ok_or_else(|| Error::Graph("http source needs a name".into()))?;
                // build_job, not into_job: the cron is consumed below, so the
                // dropped-cron warning would be a lie here
                jobs.push(src.build_job(&name)?);
                if let Some((expr, tz)) = &src.cron {
                    schedules.push((name, expr.clone(), tz.clone()));
                }
            }
            (jobs, schedules)
        };
        #[cfg(not(feature = "http"))]
        let (mut jobs, schedules) = (self.jobs, self.schedules);

        // lowered only when assets exist, so the name stays free otherwise, and
        // before the duplicate check, so a user job named "assets" collides below
        let registry = if self.assets.is_empty() {
            Arc::new(AssetRegistry::empty())
        } else {
            let registry = Arc::new(AssetRegistry::new(self.assets)?);
            jobs.push(registry.lower_job()?);
            registry
        };

        let mut names = HashSet::new();
        for job in &jobs {
            if !names.insert(job.name().to_string()) {
                return Err(Error::DuplicateJob(job.name().to_string()));
            }
        }
        let mut entries = Vec::new();
        for (job, expr, tz) in &schedules {
            if !names.contains(job.as_str()) {
                return Err(Error::UnknownJob(job.clone()));
            }
            entries.push(schedule::parse(job, expr, tz)?);
        }

        let mut sensor_entries: Vec<SensorEntry> =
            self.sensors.into_iter().map(SensorEntry::user).collect();
        for meta in registry.topo() {
            if let Some(probe) = &meta.probe {
                sensor_entries.push(SensorEntry {
                    name: format!("probe:{}", meta.name),
                    every: meta.probe_every,
                    eval: SensorEval::Probe {
                        asset: meta.name.clone(),
                        probe: probe.clone(),
                    },
                });
            }
        }
        let mut sensor_names = HashSet::new();
        for e in &sensor_entries {
            if !sensor_names.insert(e.name.clone()) {
                return Err(Error::Graph(format!("duplicate sensor {}", e.name)));
            }
        }

        let store = Store::open(&self.db_path)?;
        store.fail_interrupted()?;
        store.sync_schedules(&schedules)?;
        store.prune_ticks(5000)?;
        let sensor_names: Vec<String> = sensor_entries.iter().map(|e| e.name.clone()).collect();
        store.sync_sensors(&sensor_names)?;
        store.prune_sensor_ticks(5000)?;
        if let Some(days) = self.retention_days {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
            let removed = store.prune_runs(cutoff)?;
            if removed > 0 {
                tracing::info!("retention: removed {removed} runs older than {days} days");
            }
        }
        let runner = Runner::with_failure_hooks(jobs, store, self.hooks);
        Ok(Built {
            runner,
            entries,
            registry,
            sensor_entries,
        })
    }
}

struct Built {
    runner: Runner,
    entries: Vec<ScheduleEntry>,
    registry: Arc<AssetRegistry>,
    sensor_entries: Vec<SensorEntry>,
}
