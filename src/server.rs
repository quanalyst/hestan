use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, State};
use axum::http::{Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Duration, Utc};
use include_dir::{Dir, include_dir};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::asset::{
    ASSETS_JOB, AssetRegistry, asset_tag, launch_plan, mats_map, plan_all, plan_partitions,
    staleness,
};
use crate::backfill;
use crate::error::Error;
use crate::executor::{CancelOutcome, Runner};
use crate::freshness::{self, asset_freshness};
use crate::graph;
use crate::job::Job;
use crate::model::{
    AssetCheckRow, CheckStatus, Freshness, OpRun, OpStatus, RunStatus, RunTags, ScheduleRow,
    Trigger,
};
use crate::op;
use crate::schedule;
use crate::sensor::SensorState;

static UI_DIST: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/ui/dist");

/// how much of the queue `GET /api/queue` shows.
const QUEUE_PAGE: u32 = 200;

pub(crate) struct SensorInfo {
    pub name: String,
    pub every: std::time::Duration,
    /// what a [run-status sensor](crate::RunStatusSensor) watches; `None` for
    /// a user sensor or a probe, which watch whatever their closure looks at.
    pub filter: Option<Value>,
    /// shared with the sensor loop: when this one is next due, and how many
    /// evaluations have failed in a row.
    pub state: Arc<SensorState>,
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub jobs: Arc<HashMap<String, Job>>,
    pub runner: Runner,
    pub assets: Arc<AssetRegistry>,
    pub sensors: Arc<Vec<SensorInfo>>,
}

pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/resources", get(list_resources))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/{name}", get(get_job))
        .route("/api/jobs/{name}/runs", post(launch_run))
        .route("/api/jobs/{name}/presets", get(list_presets))
        .route(
            "/api/jobs/{name}/presets/{preset}",
            put(put_preset).delete(delete_preset),
        )
        .route("/api/jobs/{name}/validate_params", post(validate_params))
        .route("/api/jobs/{name}/op_stats", get(op_stats))
        .route("/api/jobs/{name}/state", get(job_state))
        .route("/api/runs", get(list_runs))
        .route("/api/runs/{id}", get(get_run))
        .route("/api/runs/{id}/events", get(run_events))
        .route("/api/runs/{id}/retry", post(retry_run))
        .route("/api/runs/{id}/resume", post(resume_run))
        .route("/api/runs/{id}/resume_preview", get(resume_preview))
        .route("/api/runs/{id}/clone", get(clone_run))
        .route("/api/runs/{id}/cancel", post(cancel_run))
        .route("/api/runs/{id}/priority", post(set_run_priority))
        .route("/api/queue", get(list_queue))
        .route("/api/assets", get(list_assets))
        .route("/api/assets/build", post(build_all_assets))
        .route("/api/assets/{name}/build", post(build_one_asset))
        .route("/api/assets/{name}/history", get(asset_history))
        .route("/api/assets/{name}/partitions", get(asset_partitions))
        .route("/api/assets/{name}/checks", get(asset_checks))
        .route("/api/assets/{name}/backfill", post(start_backfill))
        .route("/api/backfills", get(list_backfills))
        .route("/api/backfills/{id}", get(get_backfill))
        .route("/api/backfills/{id}/cancel", post(cancel_backfill))
        .route("/api/sensors", get(list_sensors))
        .route("/api/sensors/state", post(set_sensor_state))
        .route("/api/sensors/ticks", get(sensor_ticks))
        .route("/api/schedules", get(list_schedules))
        .route("/api/schedules/state", post(set_schedule_state))
        .route("/api/schedules/ticks", get(schedule_ticks))
        .route("/api/schedules/upcoming", get(upcoming_schedules))
        .route("/api/late", get(list_late))
        .fallback(static_ui)
        .with_state(state)
}

type ApiError = (StatusCode, Json<Value>);

fn err(status: StatusCode, msg: impl Into<String>) -> ApiError {
    (status, Json(json!({ "error": msg.into() })))
}

fn internal(e: Error) -> ApiError {
    err(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

// axum's own Query rejection is text/plain; keep every error json-shaped
fn bad_query(e: QueryRejection) -> ApiError {
    err(StatusCode::BAD_REQUEST, e.body_text())
}

// rows always came from defined schedules, so the parse can't realistically fail
fn next_fire(row: &ScheduleRow) -> Option<String> {
    let entry = schedule::parse(&row.job, &row.expr, &row.tz).ok()?;
    entry
        .schedule
        .upcoming(entry.tz)
        .next()
        .map(|t| t.with_timezone(&Utc).to_rfc3339())
}

fn fire_gap(row: &ScheduleRow) -> Option<i64> {
    let entry = schedule::parse(&row.job, &row.expr, &row.tz).ok()?;
    let mut fires = entry.schedule.upcoming(entry.tz);
    let first = fires.next()?;
    let second = fires.next()?;
    Some((second - first).num_seconds())
}

// the schedule's most recent fire at or before `now`
fn prev_fire(row: &ScheduleRow, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let entry = schedule::parse(&row.job, &row.expr, &row.tz).ok()?;
    entry
        .schedule
        .after(&now.with_timezone(&entry.tz))
        .next_back()
        .map(|t| t.with_timezone(&Utc))
}

// anchored on the previous fire so clustered schedules aren't overdue in their gaps
fn is_overdue(
    prev: DateTime<Utc>,
    interval_secs: i64,
    last_success: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    now > prev + Duration::milliseconds(interval_secs * 500)
        && last_success.is_none_or(|t| t < prev)
}

/// the shape a declared policy reports as, everywhere it is reported.
fn freshness_json(freshness: Freshness, last_success: Option<DateTime<Utc>>) -> Value {
    json!({
        "status": freshness.status(),
        "late_by_secs": freshness.late_by().map(|d| d.as_secs()),
        "last_success": last_success,
    })
}

fn job_summary(job: &Job, st: &AppState) -> Result<Value, Error> {
    let ops: Vec<Value> = job
        .ops()
        .iter()
        .map(|op| {
            json!({
                "name": op.name(),
                "deps": op.deps(),
                "when": op.runs_when(),
                "requires": op.required_resources(),
                "retries": op.max_retries(),
                "timeout_secs": op.timeout_after().map(|d| d.as_secs_f64()),
                "pool": op.pool_name(),
                "io": op.io_name(),
                // where this op's body runs, and what it is allowed to spend
                // there — null limits on every op that runs in this process
                "isolated": op.is_isolated(),
                "memory_limit_bytes": op.declared_memory_limit(),
                "cpu_limit_secs": op.declared_cpu_limit().map(|d| d.as_secs_f64()),
                "mapped_over": op.mapped_over(),
                "input_type": op.input_type(),
                "output_type": op.output_type(),
                "params_type": op.params_type(),
                "params_schema": op.declared_params_schema(),
            })
        })
        .collect();
    // the pools this job's ops take from, in first-use order, with the limit
    // each one actually carries — the cap is process-wide, not the job's
    let mut seen: Vec<&str> = Vec::new();
    for op in job.ops() {
        if let Some(pool) = op.pool_name()
            && !seen.contains(&pool)
        {
            seen.push(pool);
        }
    }
    let pools: Vec<Value> = seen
        .into_iter()
        .map(|name| json!({ "name": name, "limit": st.runner.pool_limit(name) }))
        .collect();
    let rows: Vec<ScheduleRow> = st
        .runner
        .store()
        .schedules()?
        .into_iter()
        .filter(|s| s.job == job.name())
        .collect();
    let schedules: Vec<Value> = rows
        .iter()
        .map(|s| {
            json!({
                "expr": s.expr,
                "tz": s.tz,
                "paused": s.paused,
                "params": s.params,
                "catchup": s.catchup,
                "cursor": s.cursor,
                "next_fire": next_fire(s),
            })
        })
        .collect();
    let interval_secs = rows.iter().filter(|s| !s.paused).filter_map(fire_gap).min();
    let now = Utc::now();
    let prev = rows
        .iter()
        .filter(|s| !s.paused)
        .filter_map(|s| prev_fire(s, now))
        .max();
    let last_success = st.runner.store().last_success(job.name())?;
    // a declared policy replaces the heuristic rather than sitting beside it:
    // two answers to "is this job behind" is one answer too many
    let freshness = job
        .fresh_within()
        .map(|within| freshness_json(Freshness::of(last_success, within, now), last_success));
    let overdue = match (prev, interval_secs) {
        _ if freshness.is_some() => false,
        (Some(prev), Some(gap)) => is_overdue(prev, gap, last_success, now),
        _ => false,
    };
    let last_run = st
        .runner
        .store()
        .runs(Some(job.name()), None, None, None, None, 1)?
        .pop();
    Ok(json!({
        "name": job.name(),
        "description": job.description(),
        "ops": ops,
        "params_schema": job.params_schema(),
        "schedules": schedules,
        "max_parallel": job.max_parallel(),
        "pools": pools,
        "overlap": job.overlap(),
        "last_run": last_run,
        "interval_secs": interval_secs,
        "overdue": overdue,
        "freshness": freshness,
    }))
}

/// who this process is and what it is holding.
///
/// the instance id is what a run row's `claimed_by` carries, so this is how you
/// tell which of three workers is executing the run you are looking at — and,
/// pointed at each of them in turn, which one has gone quiet.
async fn health(State(st): State<AppState>) -> Json<Value> {
    let holding = st.runner.holding().unwrap_or_default();
    Json(json!({
        "ok": true,
        "instance": st.runner.instance(),
        "holding": holding,
    }))
}

// names and declared types only: a resource is usually a client holding
// credentials, and the api has no business showing what is inside one
async fn list_resources(State(st): State<AppState>) -> Json<Value> {
    let resources: Vec<Value> = st
        .runner
        .resources()
        .into_iter()
        .map(|(name, ty)| json!({ "name": name, "type": ty }))
        .collect();
    Json(json!({ "resources": resources }))
}

async fn list_jobs(State(st): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mut jobs: Vec<&Job> = st.jobs.values().collect();
    jobs.sort_by(|a, b| a.name().cmp(b.name()));
    let jobs: Vec<Value> = jobs
        .iter()
        .map(|j| job_summary(j, &st))
        .collect::<Result<_, _>>()
        .map_err(internal)?;
    Ok(Json(json!({ "jobs": jobs })))
}

async fn get_job(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let job = st
        .jobs
        .get(&name)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown job: {name}")))?;
    Ok(Json(job_summary(job, &st).map_err(internal)?))
}

#[derive(Deserialize, Default)]
struct LaunchBody {
    params: Option<Value>,
    /// launch with a stored [preset](crate::Preset)'s params instead of
    /// inline ones; naming both is a 400.
    preset: Option<String>,
    /// a flat `{"k": "v"}` map to [tag](crate::RunTags) the run with.
    tags: Option<RunTags>,
    /// run only these ops and everything downstream of them, seeding nothing.
    ops: Option<Vec<String>>,
    /// where in the queue this run goes: higher starts first. absent is
    /// whatever `Hestan::priority` set.
    priority: Option<i64>,
}

/// a body that carries nothing but `params` — validation and preset writes.
/// an empty body means `{}`, which is what a launch would use.
#[derive(Deserialize, Default)]
struct ParamsBody {
    params: Option<Value>,
}

fn params_body(body: &Bytes) -> Result<Value, ApiError> {
    if body.is_empty() {
        return Ok(json!({}));
    }
    let parsed: ParamsBody = serde_json::from_slice(body)
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("invalid body: {e}")))?;
    Ok(parsed.params.unwrap_or_else(|| json!({})))
}

async fn launch_run(
    State(st): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let body: LaunchBody = if body.is_empty() {
        LaunchBody::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| err(StatusCode::BAD_REQUEST, format!("invalid body: {e}")))?
    };
    let params = match body.preset {
        None => body.params.unwrap_or_else(|| json!({})),
        Some(preset) => {
            // two answers to "what params" is one too many; refuse rather than
            // pick, since which one won would only ever be learned by accident
            if body.params.is_some() {
                return Err(err(
                    StatusCode::BAD_REQUEST,
                    "params and preset are alternatives; name one",
                ));
            }
            if !st.jobs.contains_key(&name) {
                return Err(err(StatusCode::NOT_FOUND, format!("unknown job: {name}")));
            }
            st.runner
                .store()
                .preset(&name, &preset)
                .map_err(internal)?
                .ok_or_else(|| {
                    err(
                        StatusCode::NOT_FOUND,
                        format!("unknown preset: {preset} on job {name}"),
                    )
                })?
                .params
        }
    };
    let tags = body.tags.unwrap_or_default();
    let launched = match body.ops {
        None => st
            .runner
            .launch_prioritized(&name, params, Trigger::Manual, tags, body.priority),
        Some(ops) => launch_subset(&st, &name, ops, params, tags, body.priority)?,
    };
    match launched {
        Ok(run_id) => Ok((StatusCode::ACCEPTED, Json(json!({ "run_id": run_id })))),
        Err(e @ Error::UnknownJob(_)) => Err(err(StatusCode::NOT_FOUND, e.to_string())),
        // a subset the job cannot satisfy is Error::Graph, raised by the same
        // check asset builds and resumes go through, and it names what is
        // missing — the request's fault, so a 400
        Err(e @ (Error::InvalidParams { .. } | Error::Graph(_))) => {
            Err(err(StatusCode::BAD_REQUEST, e.to_string()))
        }
        Err(e) => Err(internal(e)),
    }
}

/// launch exactly `ops` and everything downstream of them, seeding nothing.
///
/// "seeding nothing" is what makes this different from a resume: an upstream
/// left out has no recorded output to stand in for it, so it must be in the
/// set. that rule is [`Runner::launch_subset`]'s, not this function's — the
/// closure below only saves the caller from listing the downstream by hand,
/// and every refusal still comes from the one place asset builds and resumes
/// are refused.
///
/// the outer `Result` is a refusal about the request itself; the inner one is
/// the launch's, handled with every other launch's.
fn launch_subset(
    st: &AppState,
    name: &str,
    ops: Vec<String>,
    params: Value,
    tags: RunTags,
    priority: Option<i64>,
) -> Result<Result<String, Error>, ApiError> {
    let job = st
        .jobs
        .get(name)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown job: {name}")))?;
    if ops.is_empty() {
        return Err(err(StatusCode::BAD_REQUEST, "no ops named"));
    }
    let pairs = job.dep_pairs();
    let mut subset: HashSet<String> = ops.iter().cloned().collect();
    for root in &ops {
        subset.extend(graph::downstream(&pairs, root));
    }
    // the job's external names are seeded by every launch, subset or not: they
    // are not ops and no selection can contain them
    Ok(st.runner.launch_subset(
        name,
        subset,
        job.external_seeds(),
        params,
        Trigger::Manual,
        None,
        tags,
        priority,
    ))
}

async fn list_presets(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !st.jobs.contains_key(&name) {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown job: {name}")));
    }
    let presets = st.runner.store().presets(&name).map_err(internal)?;
    Ok(Json(json!({ "presets": presets })))
}

// validated before it is stored: a preset that cannot launch is not worth
// keeping, and finding that out at 2am is the whole thing presets exist to avoid
async fn put_preset(
    State(st): State<AppState>,
    Path((name, preset)): Path<(String, String)>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let job = st
        .jobs
        .get(&name)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown job: {name}")))?;
    let params = params_body(&body)?;
    if let Some((op, reason)) = job.params_error(&params) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            Error::InvalidParams { op, reason }.to_string(),
        ));
    }
    st.runner
        .store()
        .put_preset(&name, &preset, &params)
        .map_err(internal)?;
    Ok(Json(json!({ "ok": true })))
}

async fn delete_preset(
    State(st): State<AppState>,
    Path((name, preset)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    if !st.jobs.contains_key(&name) {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown job: {name}")));
    }
    match st
        .runner
        .store()
        .delete_preset(&name, &preset)
        .map_err(internal)?
    {
        true => Ok(Json(json!({ "deleted": true }))),
        false => Err(err(
            StatusCode::NOT_FOUND,
            format!("unknown preset: {preset} on job {name}"),
        )),
    }
}

// the same check a launch runs, without launching: the launchpad asks before
// it commits, so a typo in the params editor is a message, not a failed run
async fn validate_params(
    State(st): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let job = st
        .jobs
        .get(&name)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown job: {name}")))?;
    let params = params_body(&body)?;
    match job.params_error(&params) {
        None => Ok(Json(json!({ "ok": true }))),
        Some((op, reason)) => Err(err(
            StatusCode::BAD_REQUEST,
            Error::InvalidParams { op, reason }.to_string(),
        )),
    }
}

fn op_ms(row: &OpRun) -> Option<f64> {
    match (row.started_at, row.finished_at) {
        (Some(s), Some(f)) => Some((f - s).num_milliseconds() as f64),
        _ => None,
    }
}

// rows must be newest first; p95 is nearest-rank, null under 2 samples
fn op_stat(op: &str, rows: &[&OpRun]) -> Value {
    let mut samples: Vec<f64> = rows.iter().filter_map(|r| op_ms(r)).collect();
    samples.sort_by(f64::total_cmp);
    let avg = (!samples.is_empty()).then(|| samples.iter().sum::<f64>() / samples.len() as f64);
    let p95 =
        (samples.len() >= 2).then(|| samples[(0.95 * samples.len() as f64).ceil() as usize - 1]);
    let failures = rows.iter().filter(|r| r.status == OpStatus::Failed).count();
    let last_error = rows.iter().find_map(|r| r.error.as_deref());
    let recent: Vec<Value> = rows
        .iter()
        .take(20)
        .map(|r| json!({ "run_id": r.run_id, "status": r.status, "ms": op_ms(r) }))
        .collect();
    json!({
        "op": op,
        "runs": rows.len(),
        "failures": failures,
        "avg_ms": avg,
        "p95_ms": p95,
        "last_error": last_error,
        "recent": recent,
    })
}

#[derive(Deserialize)]
struct OpStatsQuery {
    runs: Option<u32>,
}

async fn op_stats(
    State(st): State<AppState>,
    Path(name): Path<String>,
    q: Result<Query<OpStatsQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let job = st
        .jobs
        .get(&name)
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown job: {name}")))?;
    let window = q.runs.unwrap_or(50).clamp(1, 200);
    let rows = st
        .runner
        .store()
        .recent_op_runs(&name, window)
        .map_err(internal)?;
    let mut by_op: HashMap<&str, Vec<&OpRun>> = HashMap::new();
    for row in &rows {
        by_op.entry(row.op.as_str()).or_default().push(row);
    }
    let ops: Vec<Value> = job
        .ops()
        .iter()
        .map(|op| {
            let history = by_op.get(op.name()).map_or(&[][..], |v| v.as_slice());
            op_stat(op.name(), history)
        })
        .collect();
    Ok(Json(json!({ "ops": ops })))
}

async fn job_state(
    State(st): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !st.jobs.contains_key(&name) {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown job: {name}")));
    }
    let states: Vec<Value> = st
        .runner
        .store()
        .job_states(&name)
        .map_err(internal)?
        .into_iter()
        .map(
            |(op, value, updated_at)| json!({ "op": op, "value": value, "updated_at": updated_at }),
        )
        .collect();
    Ok(Json(json!({ "states": states })))
}

async fn cancel_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    match st.runner.cancel(&id) {
        Ok(CancelOutcome::Requested) => Ok((StatusCode::ACCEPTED, Json(json!({ "ok": true })))),
        Ok(CancelOutcome::Unknown) => Err(err(StatusCode::NOT_FOUND, format!("unknown run: {id}"))),
        Ok(CancelOutcome::AlreadyFinished) => Err(err(
            StatusCode::CONFLICT,
            format!("run already finished: {id}"),
        )),
        Err(e) => Err(internal(e)),
    }
}

/// the queue: what is waiting, in the order it will be taken, and what is
/// holding each one back.
///
/// `depth` counts every unclaimed queued run; `queued` is capped, because a
/// queue ten thousand deep is a fact about the deployment rather than a list
/// anybody reads to the end.
async fn list_queue(State(st): State<AppState>) -> Result<Json<Value>, ApiError> {
    let queued: Vec<Value> = st
        .runner
        .queue(QUEUE_PAGE)
        .map_err(internal)?
        .into_iter()
        .map(|q| {
            json!({
                "run": q.run,
                "position": q.position,
                "blocked_by": q.blocked.as_ref().map(|b| json!({
                    "scope": b.scope(),
                    "reason": b.reason(),
                })),
            })
        })
        .collect();
    let limits = st.runner.limits();
    Ok(Json(json!({
        "depth": st.runner.queue_depth().map_err(internal)?,
        "queued": queued,
        "limits": {
            "global": limits.global_limit(),
            "jobs": limits.jobs().into_iter().map(|(j, n)| json!({"job": j, "limit": n}))
                .collect::<Vec<_>>(),
            "tags": limits.tag_limits().into_iter()
                .map(|(k, v, n)| json!({"key": k, "value": v, "limit": n}))
                .collect::<Vec<_>>(),
        },
    })))
}

#[derive(Deserialize)]
struct PriorityBody {
    priority: i64,
}

/// move a queued run up or down the queue. only one nobody has claimed: past
/// that the priority has already been spent, and saying so beats a 200 that
/// changed nothing.
async fn set_run_priority(
    State(st): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, ApiError> {
    let body: PriorityBody = serde_json::from_slice(&body)
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("invalid body: {e}")))?;
    match st.runner.set_priority(&id, body.priority) {
        Ok(true) => Ok(Json(json!({ "run_id": id, "priority": body.priority }))),
        Ok(false) => Err(err(StatusCode::NOT_FOUND, format!("unknown run: {id}"))),
        Err(e @ Error::RunActive(_)) => Err(err(StatusCode::CONFLICT, e.to_string())),
        Err(e) => Err(internal(e)),
    }
}

async fn list_assets(State(st): State<AppState>) -> Result<Json<Value>, ApiError> {
    let mats = mats_map(st.runner.store()).map_err(internal)?;
    let stale = staleness(&st.assets, &mats);
    let latest_checks = st.runner.store().latest_asset_checks().map_err(internal)?;
    let now = Utc::now();
    let assets: Vec<Value> = st
        .assets
        .topo()
        .map(|meta| {
            let mat = mats.get(&meta.name, None);
            let s = &stale[&meta.name];
            let reasons: Vec<Value> = s
                .reasons
                .iter()
                .map(|r| json!({ "dep": r.dep, "had": r.had, "now": r.now }))
                .collect();
            // a partitioned asset has no single fingerprint to report, so it
            // reports the shape of its key set instead: the three states are
            // disjoint and sum to `total`
            let partitions = meta.partitions.as_ref().map(|_| {
                let (mut materialized, mut stale_keys, mut missing) = (0, 0, 0);
                for (key, verdict) in &s.parts {
                    match (mats.get(&meta.name, Some(key)).is_some(), verdict.stale) {
                        (false, _) => missing += 1,
                        (true, true) => stale_keys += 1,
                        (true, false) => materialized += 1,
                    }
                }
                json!({
                    "total": s.parts.len(),
                    "materialized": materialized,
                    "stale": stale_keys,
                    "missing": missing,
                })
            });
            json!({
                "name": meta.name,
                "kind": if meta.source { "source" } else { "derived" },
                "deps": meta.deps,
                "auto": meta.auto,
                // the op that materializes it, which is the asset's own name
                // unless a multi-asset produces it alongside others
                "op": meta.op,
                "partitions": partitions,
                "fingerprint": mat.map(|m| m.fingerprint.clone()),
                "built_at": mat.map(|m| m.built_at),
                "run_id": mat.and_then(|m| m.run_id.clone()),
                "stale": s.stale,
                "reasons": reasons,
                "checks": check_summary(&latest_checks, &meta.name),
                // stale and late are different claims: stale means a dep moved,
                // late means time passed. null unless a policy was declared
                "freshness": asset_freshness(&mats, meta, now)
                    .map(|(f, last)| freshness_json(f, last)),
            })
        })
        .collect();
    Ok(Json(json!({ "assets": assets })))
}

#[derive(Deserialize)]
struct HistoryQuery {
    limit: Option<u32>,
    /// one key of a [partitioned asset](crate::Partitions); omitted, every
    /// key of it, interleaved by time.
    partition: Option<String>,
}

// what each build recorded, newest first. no `value`: like GET /api/assets,
// this reports the facts about a build and not its payload — the value is what
// a memoized build seeds, and it can be arbitrarily large.
async fn asset_history(
    State(st): State<AppState>,
    Path(name): Path<String>,
    q: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    if st.assets.get(&name).is_none() {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown asset: {name}")));
    }
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let materializations: Vec<Value> = st
        .runner
        .store()
        .materializations(&name, q.partition.as_deref(), limit)
        .map_err(internal)?
        .into_iter()
        .map(|e| {
            json!({
                "id": e.mat.id,
                "partition": e.mat.partition,
                "fingerprint": e.mat.fingerprint,
                "changed": e.changed,
                "inputs": e.mat.inputs,
                "run_id": e.mat.run_id,
                "built_at": e.mat.built_at,
                "metadata": e.mat.metadata,
                // what moved since the build before this one, computed here
                // so a row never costs the ui a second request
                "deltas": op::deltas(e.mat.metadata.as_ref(), e.previous_metadata.as_ref()),
            })
        })
        .collect();
    Ok(Json(json!({ "materializations": materializations })))
}

// one row per key of a partitioned asset, newest key first: what it is, what
// it holds, and which of the three states it is in. this is what the partition
// grid draws.
async fn asset_partitions(
    State(st): State<AppState>,
    Path(name): Path<String>,
    q: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let Some(meta) = st.assets.get(&name) else {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown asset: {name}")));
    };
    if meta.partitions.is_none() {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!("asset {name} is not partitioned"),
        ));
    }
    let mats = mats_map(st.runner.store()).map_err(internal)?;
    let verdict = &staleness(&st.assets, &mats)[&name];
    // newest first, and capped: a daily set running for years is a long list,
    // and the newest keys are the ones anyone is looking at
    let limit = q.limit.unwrap_or(90).clamp(1, 1000) as usize;
    let total = verdict.parts.len();
    let partitions: Vec<Value> = verdict
        .parts
        .iter()
        .rev()
        .take(limit)
        .map(|(key, s)| {
            let mat = mats.get(&name, Some(key));
            json!({
                "key": key,
                "state": match (mat.is_some(), s.stale) {
                    (false, _) => "missing",
                    (true, true) => "stale",
                    (true, false) => "materialized",
                },
                "fingerprint": mat.map(|m| m.fingerprint.clone()),
                "built_at": mat.map(|m| m.built_at),
                "run_id": mat.and_then(|m| m.run_id.clone()),
            })
        })
        .collect();
    Ok(Json(json!({
        "total": total,
        "shown": partitions.len(),
        "partitions": partitions,
    })))
}

// every check's recent results, newest first, all checks on the asset mixed
// together — the first row per name is that check's latest
async fn asset_checks(
    State(st): State<AppState>,
    Path(name): Path<String>,
    q: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    if st.assets.get(&name).is_none() {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown asset: {name}")));
    }
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let checks = st
        .runner
        .store()
        .asset_checks(&name, q.partition.as_deref(), limit)
        .map_err(internal)?;
    Ok(Json(json!({ "checks": checks })))
}

/// what an asset's checks currently say, from the latest result per name.
/// zero and zero means no check has ever recorded anything — which reads the
/// same whether none are declared or none have run yet.
fn check_summary(latest: &[AssetCheckRow], asset: &str) -> Value {
    let mine = latest.iter().filter(|c| c.asset == asset);
    let (mut passed, mut failed, mut last) = (0, 0, None);
    for row in mine {
        match row.status {
            CheckStatus::Passed => passed += 1,
            CheckStatus::Failed => failed += 1,
        }
        last = last.max(Some(row.checked_at));
    }
    json!({ "passed": passed, "failed": failed, "last_run_at": last })
}

// one build at a time: overlapping builds record lineage that never happened
// (assets.md). a manual launch of the assets job stays ungated
fn build_gate(st: &AppState) -> Result<(), ApiError> {
    if st
        .runner
        .store()
        .has_active_run(ASSETS_JOB)
        .map_err(internal)?
    {
        return Err(err(StatusCode::CONFLICT, "asset build already running"));
    }
    Ok(())
}

#[derive(Deserialize, Default)]
struct BuildBody {
    /// the keys to build, for a partitioned asset. omitted, the build takes
    /// the asset's default target set.
    partitions: Option<Vec<String>>,
}

// an empty body is the plain build; anything else has to parse, so a typo in
// a key name is a 400 rather than a build of something else
fn build_body(body: &Bytes) -> Result<BuildBody, ApiError> {
    if body.is_empty() {
        return Ok(BuildBody::default());
    }
    serde_json::from_slice(body).map_err(|e| err(StatusCode::BAD_REQUEST, format!("bad body: {e}")))
}

async fn build_one_asset(
    State(st): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let body = build_body(&body)?;
    let Some(meta) = st.assets.get(&name) else {
        return Err(err(StatusCode::NOT_FOUND, format!("unknown asset: {name}")));
    };
    if meta.source {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "sources are probed, never built",
        ));
    }
    let named: HashMap<String, Vec<String>> = match body.partitions {
        None => HashMap::new(),
        Some(keys) => {
            if meta.partitions.is_none() {
                return Err(err(
                    StatusCode::BAD_REQUEST,
                    format!("asset {name} is not partitioned"),
                ));
            }
            if keys.is_empty() {
                return Err(err(StatusCode::BAD_REQUEST, "no partitions named"));
            }
            HashMap::from([(name.clone(), keys)])
        }
    };
    build_gate(&st)?;
    let mats = mats_map(st.runner.store()).map_err(internal)?;
    // named keys are a rebuild of exactly those, whatever staleness says; a
    // plain build of a fresh asset has nothing to do
    if named.is_empty() && !staleness(&st.assets, &mats)[&name].stale {
        return Ok((StatusCode::OK, Json(json!({ "up_to_date": true }))));
    }
    let plan = plan_partitions(&st.assets, &mats, std::slice::from_ref(&name), &named)
        .map_err(bad_plan)?;
    match launch_plan(&st.runner, plan, Trigger::Build, asset_tag(&name)) {
        Ok(run_id) => Ok((StatusCode::ACCEPTED, Json(json!({ "run_id": run_id })))),
        Err(e) => Err(internal(e)),
    }
}

// a plan that refuses is the request's fault — an unknown key, an asset that
// is not partitioned — rather than the server's
fn bad_plan(e: Error) -> ApiError {
    match e {
        Error::Graph(msg) => err(StatusCode::BAD_REQUEST, msg),
        Error::UnknownAsset(name) => err(StatusCode::NOT_FOUND, format!("unknown asset: {name}")),
        Error::UnknownBackfill(id) => err(StatusCode::NOT_FOUND, format!("unknown backfill: {id}")),
        Error::Conflict(msg) => err(StatusCode::CONFLICT, msg),
        other => internal(other),
    }
}

#[derive(Deserialize)]
struct BackfillBody {
    from: String,
    to: String,
    /// skip the keys that are already materialized and fresh (default true).
    only_missing: Option<bool>,
}

async fn start_backfill(
    State(st): State<AppState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let body: BackfillBody = serde_json::from_slice(&body)
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("bad body: {e}")))?;
    let backfill = backfill::start(
        &st.runner,
        &st.assets,
        &name,
        &body.from,
        &body.to,
        body.only_missing.unwrap_or(true),
    )
    .map_err(bad_plan)?;
    Ok((StatusCode::ACCEPTED, Json(json!(backfill))))
}

#[derive(Deserialize)]
struct LimitQuery {
    limit: Option<u32>,
}

async fn list_backfills(
    State(st): State<AppState>,
    q: Result<Query<LimitQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let backfills = st.runner.store().backfills(limit).map_err(internal)?;
    Ok(Json(json!({ "backfills": backfills })))
}

// the record plus the runs it launched, oldest first — a backfill's progress
// is what its runs did, and this is where you go to see which one broke
async fn get_backfill(
    State(st): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    let Some(backfill) = st.runner.store().backfill(id).map_err(internal)? else {
        return Err(err(
            StatusCode::NOT_FOUND,
            format!("unknown backfill: {id}"),
        ));
    };
    let mut runs = Vec::new();
    for run_id in &backfill.run_ids {
        if let Some(run) = st.runner.store().run(run_id).map_err(internal)? {
            runs.push(run);
        }
    }
    Ok(Json(json!({ "backfill": backfill, "runs": runs })))
}

async fn cancel_backfill(
    State(st): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, ApiError> {
    match backfill::cancel(&st.runner, id).map_err(bad_plan)? {
        true => Ok(Json(json!({ "canceled": true }))),
        false => Err(err(
            StatusCode::CONFLICT,
            format!("backfill {id} already finished"),
        )),
    }
}

async fn build_all_assets(
    State(st): State<AppState>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    build_gate(&st)?;
    let mats = mats_map(st.runner.store()).map_err(internal)?;
    // one plan, one run, so a build reads as a single run in the ui
    let Some(plan) = plan_all(&st.assets, &mats) else {
        return Ok((StatusCode::OK, Json(json!({ "up_to_date": true }))));
    };
    match launch_plan(&st.runner, plan, Trigger::Build, RunTags::new()) {
        Ok(run_id) => Ok((StatusCode::ACCEPTED, Json(json!({ "run_ids": [run_id] })))),
        Err(e) => Err(internal(e)),
    }
}

async fn list_sensors(State(st): State<AppState>) -> Result<Json<Value>, ApiError> {
    let store = st.runner.store();
    let rows = store.sensors().map_err(internal)?;
    let sensors: Vec<Value> = st
        .sensors
        .iter()
        .map(|info| {
            let row = rows.iter().find(|r| r.name == info.name);
            let last_tick = store
                .sensor_ticks(Some(&info.name), 1)
                .map(|mut t| t.pop())
                .map_err(internal)?;
            let (next_eval, failures) = info.state.snapshot();
            Ok(json!({
                "name": info.name,
                "every_secs": info.every.as_secs(),
                "paused": row.is_some_and(|r| r.paused),
                "cursor": row.and_then(|r| r.cursor.clone()),
                "filter": info.filter,
                "next_eval": next_eval.to_rfc3339(),
                "consecutive_failures": failures,
                "last_tick": last_tick,
            }))
        })
        .collect::<Result<_, ApiError>>()?;
    Ok(Json(json!({ "sensors": sensors })))
}

#[derive(Deserialize)]
struct SensorStateBody {
    name: String,
    paused: bool,
}

async fn set_sensor_state(
    State(st): State<AppState>,
    Json(body): Json<SensorStateBody>,
) -> Result<Json<Value>, ApiError> {
    let known = st
        .runner
        .store()
        .set_sensor_paused(&body.name, body.paused)
        .map_err(internal)?;
    if !known {
        return Err(err(
            StatusCode::NOT_FOUND,
            format!("unknown sensor: {}", body.name),
        ));
    }
    Ok(Json(json!({ "ok": true })))
}

#[derive(Deserialize)]
struct SensorTicksQuery {
    sensor: Option<String>,
    limit: Option<u32>,
}

async fn sensor_ticks(
    State(st): State<AppState>,
    q: Result<Query<SensorTicksQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let sensor = q.sensor.as_deref().filter(|s| !s.is_empty());
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let ticks = st
        .runner
        .store()
        .sensor_ticks(sensor, limit)
        .map_err(internal)?;
    Ok(Json(json!({ "ticks": ticks })))
}

async fn list_schedules(State(st): State<AppState>) -> Result<Json<Value>, ApiError> {
    let schedules: Vec<Value> = st
        .runner
        .store()
        .schedules()
        .map_err(internal)?
        .iter()
        .map(|s| {
            json!({
                "job": s.job,
                "expr": s.expr,
                "tz": s.tz,
                "paused": s.paused,
                "params": s.params,
                "catchup": s.catchup,
                "cursor": s.cursor,
                "next_fire": next_fire(s),
            })
        })
        .collect();
    Ok(Json(json!({ "schedules": schedules })))
}

#[derive(Deserialize)]
struct ScheduleStateBody {
    job: String,
    expr: String,
    paused: bool,
}

async fn set_schedule_state(
    State(st): State<AppState>,
    Json(body): Json<ScheduleStateBody>,
) -> Result<Json<Value>, ApiError> {
    let known = st
        .runner
        .store()
        .set_schedule_paused(&body.job, &body.expr, body.paused)
        .map_err(internal)?;
    if !known {
        return Err(err(
            StatusCode::NOT_FOUND,
            format!("unknown schedule: {} {:?}", body.job, body.expr),
        ));
    }
    Ok(Json(json!({ "ok": true })))
}

#[derive(Deserialize)]
struct TicksQuery {
    job: Option<String>,
    limit: Option<u32>,
}

async fn schedule_ticks(
    State(st): State<AppState>,
    q: Result<Query<TicksQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let job = q.job.as_deref().filter(|j| !j.is_empty());
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let ticks = st.runner.store().ticks(job, limit).map_err(internal)?;
    Ok(Json(json!({ "ticks": ticks })))
}

#[derive(Deserialize)]
struct UpcomingQuery {
    window: Option<u32>,
}

async fn upcoming_schedules(
    State(st): State<AppState>,
    q: Result<Query<UpcomingQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let window = i64::from(q.window.unwrap_or(86400).clamp(60, 604800));
    let now = Utc::now();
    let upcoming: Vec<Value> = st
        .runner
        .store()
        .schedules()
        .map_err(internal)?
        .iter()
        .filter(|s| !s.paused)
        .filter_map(|s| {
            let entry = schedule::parse(&s.job, &s.expr, &s.tz).ok()?;
            let times: Vec<String> = schedule::upcoming_fires(&entry, now, window)
                .iter()
                .map(|t| t.to_rfc3339())
                .collect();
            Some(json!({ "job": s.job, "expr": s.expr, "times": times }))
        })
        .collect();
    Ok(Json(json!({ "upcoming": upcoming })))
}

// everything a declared policy currently calls late, jobs then assets, in the
// shape `on_late` hands its hooks — so an alert and this list cannot disagree
async fn list_late(State(st): State<AppState>) -> Result<Json<Value>, ApiError> {
    let now = Utc::now();
    let late: Vec<Value> = freshness::verdicts(&st.runner, &st.assets, now)
        .map_err(internal)?
        .iter()
        .filter(|v| v.freshness.is_late())
        .map(|v| {
            json!({
                "kind": v.kind.as_str(),
                "name": v.name,
                "late_by_secs": v.freshness.late_by().map(|d| d.as_secs()),
                "last_success": v.last_success,
            })
        })
        .collect();
    Ok(Json(json!({ "late": late })))
}

#[derive(Deserialize)]
struct RunsQuery {
    job: Option<String>,
    since: Option<String>,
    before: Option<String>,
    before_id: Option<String>,
    /// one `k:v` pair, matched exactly against the run's tags.
    tag: Option<String>,
    limit: Option<u32>,
}

// `k:v`, split at the first colon so a value may hold one. a pair with no
// colon, an empty key or an empty value names nothing, and saying so beats
// listing every run as if no filter had been asked for
fn tag_param(v: Option<&str>) -> Result<Option<(&str, &str)>, ApiError> {
    let Some(v) = v.filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    match v.split_once(':') {
        Some((k, value)) if !k.is_empty() && !value.is_empty() => Ok(Some((k, value))),
        _ => Err(err(
            StatusCode::BAD_REQUEST,
            format!("invalid tag: {v}; expected key:value"),
        )),
    }
}

fn time_param(v: Option<&str>, name: &str) -> Result<Option<DateTime<Utc>>, ApiError> {
    v.filter(|s| !s.is_empty())
        .map(|s| {
            DateTime::parse_from_rfc3339(s)
                .map(|t| t.with_timezone(&Utc))
                .map_err(|e| err(StatusCode::BAD_REQUEST, format!("invalid {name}: {e}")))
        })
        .transpose()
}

async fn list_runs(
    State(st): State<AppState>,
    q: Result<Query<RunsQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let job = q.job.as_deref().filter(|j| !j.is_empty());
    let since = time_param(q.since.as_deref(), "since")?;
    let before = time_param(q.before.as_deref(), "before")?;
    // before_id only refines `before`; alone it means nothing and is dropped
    let before_id = before.and(q.before_id.as_deref().filter(|s| !s.is_empty()));
    let tag = tag_param(q.tag.as_deref())?;
    // windowed fetches page through whole days of runs, hence the wider cap
    let max = if since.is_some() { 2000 } else { 500 };
    let limit = q.limit.unwrap_or(50).clamp(1, max);
    let runs = st
        .runner
        .store()
        .runs(job, since, before, before_id, tag, limit)
        .map_err(internal)?;
    Ok(Json(json!({ "runs": runs })))
}

async fn retry_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let run = st
        .runner
        .store()
        .run(&id)
        .map_err(internal)?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown run: {id}")))?;
    // a manual launch stays ungated — the documented escape hatch
    if matches!(run.status, RunStatus::Queued | RunStatus::Running) {
        return Err(err(StatusCode::CONFLICT, format!("run still active: {id}")));
    }
    match st.runner.launch(&run.job, run.params, Trigger::Retry) {
        Ok(run_id) => Ok((StatusCode::ACCEPTED, Json(json!({ "run_id": run_id })))),
        // the run exists but its job left the code since; a launch 404 would lie
        Err(Error::UnknownJob(job)) => Err(err(
            StatusCode::CONFLICT,
            format!("job no longer defined: {job}"),
        )),
        Err(e @ Error::InvalidParams { .. }) => Err(err(StatusCode::BAD_REQUEST, e.to_string())),
        Err(e) => Err(internal(e)),
    }
}

#[derive(Deserialize)]
struct ResumeBody {
    from: Option<Vec<String>>,
}

fn resume_from_body(body: &Bytes) -> Result<Vec<String>, ApiError> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let parsed: ResumeBody = serde_json::from_slice(body)
        .map_err(|e| err(StatusCode::BAD_REQUEST, format!("invalid body: {e}")))?;
    Ok(parsed.from.unwrap_or_default())
}

// the checks live in Runner::resume_plan, so the preview and the launch that
// follows it answer with the same status
fn resume_error(e: Error) -> ApiError {
    match e {
        e @ Error::UnknownRun(_) => err(StatusCode::NOT_FOUND, e.to_string()),
        e @ (Error::RunActive(_) | Error::RunNotFailed(_)) => {
            err(StatusCode::CONFLICT, e.to_string())
        }
        // the run exists but its job left the code since; a 404 would lie
        Error::UnknownJob(job) => err(
            StatusCode::CONFLICT,
            format!("job no longer defined: {job}"),
        ),
        e @ (Error::Graph(_)
        | Error::NothingToResume(_)
        | Error::ResumeChain(_)
        | Error::InvalidParams { .. }) => err(StatusCode::BAD_REQUEST, e.to_string()),
        e => internal(e),
    }
}

async fn resume_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let from = resume_from_body(&body)?;
    match st.runner.resume_from(&id, Some(&from)) {
        Ok(run_id) => Ok((StatusCode::ACCEPTED, Json(json!({ "run_id": run_id })))),
        Err(e) => Err(resume_error(e)),
    }
}

#[derive(Deserialize)]
struct ResumePreviewQuery {
    from: Option<String>,
}

async fn resume_preview(
    State(st): State<AppState>,
    Path(id): Path<String>,
    q: Result<Query<ResumePreviewQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let from: Vec<String> = q
        .from
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    let plan = st
        .runner
        .resume_plan(&id, Some(&from))
        .map_err(resume_error)?;
    Ok(Json(json!({ "reuse": plan.reuse, "rerun": plan.rerun })))
}

// what a past run was launched with, for the launchpad to open prefilled.
// cloning is a launch you get to edit first, so this hands over what to edit
// and launches nothing; the alternative — passing it through the url — puts a
// run's whole params in a query string
async fn clone_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let run = st
        .runner
        .store()
        .run(&id)
        .map_err(internal)?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown run: {id}")))?;
    // the run is there and its job is not: a 404 would blame the run, and a
    // launchpad half-loaded for a job that cannot launch is worse than either.
    // the same status and words a retry of such a run answers with
    if !st.jobs.contains_key(&run.job) {
        return Err(err(
            StatusCode::CONFLICT,
            format!("job no longer defined: {}", run.job),
        ));
    }
    Ok(Json(json!({
        "job": run.job,
        "params": run.params,
        "tags": run.tags,
    })))
}

async fn get_run(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let store = st.runner.store();
    let run = store
        .run(&id)
        .map_err(internal)?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown run: {id}")))?;
    let ops = store.op_runs(&id).map_err(internal)?;
    // what each op reported the last time this job ran it, so every row can
    // say what moved without the ui going and fetching the history itself
    let previous = store
        .previous_op_metadata(&run.job, run.created_at, &run.id)
        .map_err(internal)?;
    let ops: Vec<Value> = ops
        .iter()
        .map(|o| {
            json!({
                "run_id": o.run_id,
                "op": o.op,
                "status": o.status,
                "attempts": o.attempts,
                "started_at": o.started_at,
                "finished_at": o.finished_at,
                "output": o.output,
                "metadata": o.metadata,
                "deltas": op::deltas(o.metadata.as_ref(), previous.get(&o.op)),
                "error": o.error,
                "pid": o.pid,
            })
        })
        .collect();
    Ok(Json(json!({ "run": run, "ops": ops })))
}

#[derive(Deserialize)]
struct EventsQuery {
    after: Option<i64>,
}

async fn run_events(
    State(st): State<AppState>,
    Path(id): Path<String>,
    q: Result<Query<EventsQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiError> {
    let Query(q) = q.map_err(bad_query)?;
    let store = st.runner.store();
    store
        .run(&id)
        .map_err(internal)?
        .ok_or_else(|| err(StatusCode::NOT_FOUND, format!("unknown run: {id}")))?;
    let events = store.events(&id, q.after.unwrap_or(0)).map_err(internal)?;
    Ok(Json(json!({ "events": events })))
}

// unmatched paths fall back to index.html for client-side routing; /api and non-GET must not
async fn static_ui(method: Method, uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if path == "api" || path.starts_with("api/") {
        return err(StatusCode::NOT_FOUND, format!("no such endpoint: /{path}")).into_response();
    }
    if method != Method::GET {
        return err(StatusCode::METHOD_NOT_ALLOWED, "method not allowed").into_response();
    }
    let file = UI_DIST
        .get_file(path)
        .or_else(|| UI_DIST.get_file("index.html"));
    match file {
        Some(f) => {
            let mime = mime_guess::from_path(f.path()).first_or_octet_stream();
            ([(header::CONTENT_TYPE, mime.as_ref())], f.contents()).into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Run, RunStatus};
    use crate::op::{Meta, Op, OpCtx};
    use crate::schedule::Schedule;
    use crate::store::Store;

    fn echo_job(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new(
                "echo",
                |ctx| async move { Ok(ctx.params().clone()) },
            ))
            .build()
            .unwrap()
    }

    fn state(jobs: Vec<Job>) -> AppState {
        let runner = Runner::new(jobs, Store::open(":memory:").unwrap());
        AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: Arc::new(AssetRegistry::empty()),
            sensors: Arc::new(Vec::new()),
        }
    }

    fn insert_run(st: &AppState, id: &str, job: &str, status: RunStatus, params: Value) -> Run {
        let run = Run {
            id: id.into(),
            job: job.into(),
            status,
            trigger: Trigger::Manual,
            params,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: None,
            scheduled_for: None,
            tags: Default::default(),
            priority: 0,
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
        };
        st.runner.store().create_run(&run, &[]).unwrap();
        run
    }

    #[tokio::test]
    async fn list_runs_since_filters() {
        let st = state(vec![]);
        let t0 = Utc::now() - Duration::minutes(10);
        for (id, age) in [("r0", 5), ("r1", 0)] {
            let run = Run {
                id: id.into(),
                job: "etl".into(),
                status: RunStatus::Queued,
                trigger: Trigger::Manual,
                params: json!({}),
                created_at: t0 - Duration::minutes(age),
                started_at: None,
                finished_at: None,
                error: None,
                resumed_from: None,
                scheduled_for: None,
                tags: Default::default(),
                priority: 0,
                claimed_by: None,
                claimed_at: None,
                lease_until: None,
            };
            st.runner.store().create_run(&run, &[]).unwrap();
        }

        let q = |since: Option<&str>| RunsQuery {
            job: None,
            since: since.map(String::from),
            before: None,
            before_id: None,
            tag: None,
            limit: None,
        };
        let Json(body) = list_runs(State(st.clone()), Ok(Query(q(Some(&t0.to_rfc3339())))))
            .await
            .unwrap();
        let ids: Vec<&str> = body["runs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids, ["r1"]);

        let Json(body) = list_runs(State(st.clone()), Ok(Query(q(None))))
            .await
            .unwrap();
        assert_eq!(body["runs"].as_array().unwrap().len(), 2);

        let (status, _) = list_runs(State(st), Ok(Query(q(Some("not a time")))))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_runs_before_cursor() {
        let st = state(vec![]);
        let t0 = Utc::now() - Duration::minutes(10);
        for i in 0..3 {
            let run = Run {
                id: format!("r{i}"),
                job: "etl".into(),
                status: RunStatus::Queued,
                trigger: Trigger::Manual,
                params: json!({}),
                created_at: t0 + Duration::minutes(i),
                started_at: None,
                finished_at: None,
                error: None,
                resumed_from: None,
                scheduled_for: None,
                tags: Default::default(),
                priority: 0,
                claimed_by: None,
                claimed_at: None,
                lease_until: None,
            };
            st.runner.store().create_run(&run, &[]).unwrap();
        }

        let q = |since: Option<String>, before: Option<String>| RunsQuery {
            job: None,
            since,
            before,
            before_id: None,
            tag: None,
            limit: None,
        };
        let ids = |body: &Value| -> Vec<String> {
            body["runs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["id"].as_str().unwrap().to_string())
                .collect()
        };

        let cursor = (t0 + Duration::minutes(2)).to_rfc3339();
        let Json(body) = list_runs(State(st.clone()), Ok(Query(q(None, Some(cursor)))))
            .await
            .unwrap();
        assert_eq!(ids(&body), ["r1", "r0"]);

        let Json(body) = list_runs(
            State(st.clone()),
            Ok(Query(q(
                Some((t0 + Duration::minutes(1)).to_rfc3339()),
                Some((t0 + Duration::minutes(2)).to_rfc3339()),
            ))),
        )
        .await
        .unwrap();
        assert_eq!(ids(&body), ["r1"]);

        let (status, _) = list_runs(State(st), Ok(Query(q(None, Some("not a time".into())))))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn list_runs_composite_cursor_pages_ties() {
        let st = state(vec![]);
        let t0 = Utc::now() - Duration::minutes(10);
        let tied = t0 + Duration::minutes(1);
        for (id, at) in [("ra", t0), ("rb", tied), ("rc", tied)] {
            let run = Run {
                id: id.into(),
                job: "etl".into(),
                status: RunStatus::Queued,
                trigger: Trigger::Manual,
                params: json!({}),
                created_at: at,
                started_at: None,
                finished_at: None,
                error: None,
                resumed_from: None,
                scheduled_for: None,
                tags: Default::default(),
                priority: 0,
                claimed_by: None,
                claimed_at: None,
                lease_until: None,
            };
            st.runner.store().create_run(&run, &[]).unwrap();
        }

        let mut seen: Vec<String> = Vec::new();
        let mut cursor: Option<(String, String)> = None;
        loop {
            let (before, before_id) = match &cursor {
                Some((ts, id)) => (Some(ts.clone()), Some(id.clone())),
                None => (None, None),
            };
            let q = RunsQuery {
                job: None,
                since: None,
                before,
                before_id,
                tag: None,
                limit: Some(1),
            };
            let Json(body) = list_runs(State(st.clone()), Ok(Query(q))).await.unwrap();
            let Some(run) = body["runs"].as_array().unwrap().first() else {
                break;
            };
            let id = run["id"].as_str().unwrap().to_string();
            cursor = Some((run["created_at"].as_str().unwrap().to_string(), id.clone()));
            seen.push(id);
        }
        assert_eq!(seen, ["rc", "rb", "ra"]);
    }

    #[test]
    fn op_stat_hand_checked_percentiles() {
        let t0 = Utc::now();
        let row = |id: &str, ms: i64| OpRun {
            run_id: id.into(),
            op: "a".into(),
            status: OpStatus::Success,
            attempts: 1,
            started_at: Some(t0),
            finished_at: Some(t0 + Duration::milliseconds(ms)),
            output: None,
            metadata: None,
            error: None,
            pid: None,
        };

        // durations 100..2000 step 100: avg 1050, rank ceil(0.95*20)=19 -> 1900
        let rows: Vec<OpRun> = (1..=20).map(|i| row(&format!("r{i}"), i * 100)).collect();
        let refs: Vec<&OpRun> = rows.iter().collect();
        let s = op_stat("a", &refs);
        assert_eq!(s["runs"], 20);
        assert_eq!(s["avg_ms"], json!(1050.0));
        assert_eq!(s["p95_ms"], json!(1900.0));
        assert_eq!(s["recent"].as_array().unwrap().len(), 20);

        let rows = [row("r1", 100), row("r2", 900)];
        let refs: Vec<&OpRun> = rows.iter().collect();
        let s = op_stat("a", &refs);
        assert_eq!(s["avg_ms"], json!(500.0));
        assert_eq!(s["p95_ms"], json!(900.0));

        let rows = [row("r1", 250)];
        let refs: Vec<&OpRun> = rows.iter().collect();
        let s = op_stat("a", &refs);
        assert_eq!(s["avg_ms"], json!(250.0));
        assert_eq!(s["p95_ms"], json!(null));

        let rows: Vec<OpRun> = (1..=25).map(|i| row(&format!("r{i}"), i * 100)).collect();
        let refs: Vec<&OpRun> = rows.iter().collect();
        let s = op_stat("a", &refs);
        assert_eq!(s["runs"], 25);
        assert_eq!(s["recent"].as_array().unwrap().len(), 20);
    }

    #[tokio::test]
    async fn op_stats_aggregates_window_and_errors() {
        let job = Job::builder("etl")
            .op(Op::new("a", |_| async { Ok(json!(null)) }))
            .op(Op::new("b", |_| async { Ok(json!(null)) }).after(["a"]))
            .build()
            .unwrap();
        let st = state(vec![job]);
        let store = st.runner.store();
        let t0 = Utc::now() - Duration::minutes(10);
        for i in 0..3 {
            let run = Run {
                id: format!("r{i}"),
                job: "etl".into(),
                status: RunStatus::Failed,
                trigger: Trigger::Manual,
                params: json!({}),
                created_at: t0 + Duration::minutes(i),
                started_at: None,
                finished_at: None,
                error: None,
                resumed_from: None,
                scheduled_for: None,
                tags: Default::default(),
                priority: 0,
                claimed_by: None,
                claimed_at: None,
                lease_until: None,
            };
            store.create_run(&run, &["a".into(), "b".into()]).unwrap();
            store.op_started(&run.id, "a", 1).unwrap();
        }
        // oldest run succeeded end to end
        store
            .op_finished("r0", "a", OpStatus::Success, None, None, None)
            .unwrap();
        store.op_started("r0", "b", 1).unwrap();
        store
            .op_finished("r0", "b", OpStatus::Success, None, None, None)
            .unwrap();
        // the two newer runs failed at a, skipping b (never started)
        for (id, msg) in [("r1", "db locked"), ("r2", "timeout")] {
            store
                .op_finished(id, "a", OpStatus::Failed, None, None, Some(msg))
                .unwrap();
            store
                .op_finished(id, "b", OpStatus::Skipped, None, None, None)
                .unwrap();
        }

        let stats = |st: &AppState, runs: Option<u32>| {
            op_stats(
                State(st.clone()),
                Path("etl".into()),
                Ok(Query(OpStatsQuery { runs })),
            )
        };
        let Json(body) = stats(&st, None).await.unwrap();
        let ops = body["ops"].as_array().unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0]["op"], "a");
        assert_eq!(ops[1]["op"], "b");

        let a = &ops[0];
        assert_eq!(a["runs"], 3);
        assert_eq!(a["failures"], 2);
        assert_eq!(a["last_error"], "timeout");
        let recent: Vec<&str> = a["recent"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["run_id"].as_str().unwrap())
            .collect();
        assert_eq!(recent, ["r2", "r1", "r0"]);
        assert!(a["avg_ms"].is_number() && a["p95_ms"].is_number());

        let b = &ops[1];
        assert_eq!(b["runs"], 3);
        assert_eq!(b["failures"], 0);
        assert_eq!(b["last_error"], json!(null));
        assert_eq!(b["recent"][0]["status"], "skipped");
        assert_eq!(b["recent"][0]["ms"], json!(null));
        assert!(b["avg_ms"].is_number());
        assert_eq!(b["p95_ms"], json!(null));

        let Json(body) = stats(&st, Some(2)).await.unwrap();
        assert_eq!(body["ops"][0]["runs"], 2);
        assert_eq!(body["ops"][0]["failures"], 2);
        assert_eq!(body["ops"][1]["avg_ms"], json!(null));

        let Json(body) = stats(&st, Some(0)).await.unwrap();
        assert_eq!(body["ops"][0]["runs"], 1);
        assert_eq!(body["ops"][0]["last_error"], "timeout");

        let Json(body) = stats(&st, Some(9999)).await.unwrap();
        assert_eq!(body["ops"][0]["runs"], 3);
    }

    #[tokio::test]
    async fn op_stats_lists_ops_without_history() {
        let job = Job::builder("fresh")
            .op(Op::new("pull", |_| async { Ok(json!(null)) }))
            .op(Op::new("push", |_| async { Ok(json!(null)) }).after(["pull"]))
            .build()
            .unwrap();
        let st = state(vec![job]);

        let Json(body) = op_stats(
            State(st.clone()),
            Path("fresh".into()),
            Ok(Query(OpStatsQuery { runs: None })),
        )
        .await
        .unwrap();
        let ops = body["ops"].as_array().unwrap();
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0]["op"], "pull");
        assert_eq!(ops[1]["op"], "push");
        assert_eq!(ops[0]["runs"], 0);
        assert_eq!(ops[0]["failures"], 0);
        assert_eq!(ops[0]["avg_ms"], json!(null));
        assert_eq!(ops[0]["p95_ms"], json!(null));
        assert_eq!(ops[0]["last_error"], json!(null));
        assert!(ops[0]["recent"].as_array().unwrap().is_empty());

        let (status, Json(body)) = op_stats(
            State(st),
            Path("nope".into()),
            Ok(Query(OpStatsQuery { runs: None })),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown job: nope");
    }

    #[tokio::test]
    async fn job_summary_reports_pools_and_timeouts() {
        let job = Job::builder("pull")
            .op(Op::new("first", |_| async { Ok(json!(null)) })
                .pool("api")
                .timeout(std::time::Duration::from_secs(30)))
            .op(Op::new("second", |_| async { Ok(json!(null)) }).pool("api"))
            .op(Op::new("local", |_| async { Ok(json!(null)) }))
            .build()
            .unwrap();
        let runner = Runner::with_pools(
            [job],
            Store::open(":memory:").unwrap(),
            vec![],
            [("api".to_string(), 3)],
        )
        .unwrap();
        let st = AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: Arc::new(AssetRegistry::empty()),
            sensors: Arc::new(Vec::new()),
        };

        let Json(body) = get_job(State(st), Path("pull".into())).await.unwrap();
        let ops = body["ops"].as_array().unwrap();
        assert_eq!(ops[0]["pool"], "api");
        assert_eq!(ops[0]["timeout_secs"], json!(30.0));
        assert_eq!(ops[1]["pool"], "api");
        assert_eq!(ops[2]["pool"], json!(null));
        assert_eq!(ops[2]["timeout_secs"], json!(null));
        // one entry per pool the job draws from, with the process-wide limit
        assert_eq!(body["pools"], json!([{ "name": "api", "limit": 3 }]));
    }

    #[tokio::test]
    async fn validate_params_endpoint_answers_ok_bad_and_unknown() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Params {
            threshold: u32,
        }
        let job = Job::builder("gated")
            .op(Op::new("check", |_| async { Ok(json!(null)) }).params::<Params>())
            .build()
            .unwrap();
        let st = state(vec![job]);
        let check = |body: &'static str| {
            validate_params(State(st.clone()), Path("gated".into()), raw(body))
        };

        let Json(body) = check(r#"{"params": {"threshold": 3}}"#).await.unwrap();
        assert_eq!(body, json!({"ok": true}));

        let (status, Json(body)) = check(r#"{"params": {"threshold": "high"}}"#)
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .starts_with("invalid params for op check:"),
            "{body}"
        );

        // an empty body means {}, which this op also refuses
        let (status, _) = check("").await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, Json(body)) = check("{not json}").await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"].as_str().unwrap().starts_with("invalid body"),
            "{body}"
        );

        let (status, Json(body)) =
            validate_params(State(st), Path("nope".into()), raw(r#"{"params": {}}"#))
                .await
                .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown job: nope");
    }

    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct Window {
        days: u32,
    }

    // a job whose op refuses anything but {"days": <number>}
    fn windowed_job(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new("render", |ctx| async move { Ok(ctx.params().clone()) }).params::<Window>())
            .build()
            .unwrap()
    }

    // where an op runs, and what it may spend there, are facts about the job
    // that only the summary can carry
    #[tokio::test]
    async fn isolation_and_its_limits_reach_the_job_summary() {
        let job = Job::builder("risky")
            .op(Op::new("parse", |_| async { Ok(json!(null)) })
                .isolated()
                .memory_limit(512 * 1024 * 1024)
                .cpu_limit(Duration::seconds(30).to_std().unwrap()))
            .op(Op::new("here", |_| async { Ok(json!(null)) }).after(["parse"]))
            .build()
            .unwrap();
        let st = state(vec![job]);

        let Json(body) = get_job(State(st), Path("risky".into())).await.unwrap();
        assert_eq!(body["ops"][0]["isolated"], json!(true));
        assert_eq!(body["ops"][0]["memory_limit_bytes"], json!(536_870_912u64));
        assert_eq!(body["ops"][0]["cpu_limit_secs"], json!(30.0));
        // an ordinary op says so, with no limits to report
        assert_eq!(body["ops"][1]["isolated"], json!(false));
        assert_eq!(body["ops"][1]["memory_limit_bytes"], json!(null));
        assert_eq!(body["ops"][1]["cpu_limit_secs"], json!(null));
    }

    #[tokio::test]
    async fn a_declared_params_schema_reaches_the_api_merged_and_per_op() {
        let schema = json!({
            "type": "object",
            "properties": { "days": { "type": "integer", "description": "how far back" } },
            "required": ["days"]
        });
        let job = Job::builder("report")
            .op(
                Op::new("render", |ctx| async move { Ok(ctx.params().clone()) })
                    .params::<Window>()
                    .params_schema(schema.clone()),
            )
            .op(Op::new("notify", |_| async { Ok(json!(null)) }).after(["render"]))
            .build()
            .unwrap();
        let st = state(vec![job, echo_job("plain")]);

        let Json(body) = get_job(State(st.clone()), Path("report".into()))
            .await
            .unwrap();
        assert_eq!(body["params_schema"], schema);
        assert_eq!(body["ops"][0]["params_schema"], schema);
        // an op that declared nothing says so, next to its null params_type
        assert_eq!(body["ops"][1]["params_schema"], json!(null));
        assert_eq!(body["ops"][1]["params_type"], json!(null));

        // and a job nobody described behaves exactly as it did before schemas
        let Json(body) = get_job(State(st), Path("plain".into())).await.unwrap();
        assert_eq!(body["params_schema"], json!(null));
        assert_eq!(body["ops"][0]["params_schema"], json!(null));
    }

    // the schema describes the params; the declared type decides them. one
    // that disagrees is a bad legend, never a wider gate
    #[tokio::test]
    async fn a_schema_that_contradicts_the_type_cannot_admit_bad_params() {
        let lying = json!({
            "type": "object",
            "properties": { "days": { "type": "string" } },
            "required": ["days", "region"]
        });
        let job = Job::builder("report")
            .op(
                Op::new("render", |ctx| async move { Ok(ctx.params().clone()) })
                    .params::<Window>()
                    .params_schema(lying.clone()),
            )
            .build()
            .unwrap();
        let st = state(vec![job]);

        // reported as declared, wrong and all — the api does not correct it
        let Json(body) = get_job(State(st.clone()), Path("report".into()))
            .await
            .unwrap();
        assert_eq!(body["params_schema"], lying);

        // what the schema describes and the type refuses is still refused
        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("report".into()),
            raw(r#"{"params": {"days": "seven", "region": "eu"}}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .starts_with("invalid params for op render:"),
            "{body}"
        );
        let (status, _) = validate_params(
            State(st.clone()),
            Path("report".into()),
            raw(r#"{"params": {"days": "seven", "region": "eu"}}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        // a preset the schema would accept is refused before it is stored, too
        let (status, _) = put_preset(
            State(st.clone()),
            Path(("report".into(), "lying".into())),
            raw(r#"{"params": {"days": "seven", "region": "eu"}}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // and what the type accepts launches, whatever the schema claims to
        // require and whatever type it claims the field has
        let (status, _) = launch_run(
            State(st.clone()),
            Path("report".into()),
            raw(r#"{"params": {"days": 7}}"#),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let Json(body) = validate_params(
            State(st),
            Path("report".into()),
            raw(r#"{"params": {"days": 7}}"#),
        )
        .await
        .unwrap();
        assert_eq!(body, json!({"ok": true}));
    }

    #[tokio::test]
    async fn presets_are_written_listed_and_deleted() {
        let st = state(vec![windowed_job("report")]);
        let Json(body) = list_presets(State(st.clone()), Path("report".into()))
            .await
            .unwrap();
        assert_eq!(body["presets"], json!([]));

        let Json(body) = put_preset(
            State(st.clone()),
            Path(("report".into(), "nightly".into())),
            raw(r#"{"params": {"days": 1}}"#),
        )
        .await
        .unwrap();
        assert_eq!(body, json!({"ok": true}));

        let Json(body) = list_presets(State(st.clone()), Path("report".into()))
            .await
            .unwrap();
        let presets = body["presets"].as_array().unwrap();
        assert_eq!(presets.len(), 1);
        assert_eq!(presets[0]["name"], "nightly");
        assert_eq!(presets[0]["params"], json!({"days": 1}));
        assert!(presets[0]["created_at"].is_string());

        let Json(body) =
            delete_preset(State(st.clone()), Path(("report".into(), "nightly".into())))
                .await
                .unwrap();
        assert_eq!(body, json!({"deleted": true}));
        let (status, Json(body)) =
            delete_preset(State(st.clone()), Path(("report".into(), "nightly".into())))
                .await
                .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown preset: nightly on job report");

        // every one of the three refuses a job it does not have
        for status in [
            list_presets(State(st.clone()), Path("nope".into()))
                .await
                .unwrap_err()
                .0,
            put_preset(
                State(st.clone()),
                Path(("nope".into(), "p".into())),
                raw(r#"{"params": {}}"#),
            )
            .await
            .unwrap_err()
            .0,
            delete_preset(State(st), Path(("nope".into(), "p".into())))
                .await
                .unwrap_err()
                .0,
        ] {
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }

    // a preset that cannot launch is not worth storing
    #[tokio::test]
    async fn an_invalid_preset_is_refused_before_it_is_stored() {
        let st = state(vec![windowed_job("report")]);
        let (status, Json(body)) = put_preset(
            State(st.clone()),
            Path(("report".into(), "broken".into())),
            raw(r#"{"params": {"days": "many"}}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .starts_with("invalid params for op render:"),
            "{body}"
        );
        assert!(st.runner.store().presets("report").unwrap().is_empty());

        // an empty body is {}, which this op also refuses
        let (status, _) = put_preset(
            State(st.clone()),
            Path(("report".into(), "empty".into())),
            raw(""),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(st.runner.store().presets("report").unwrap().is_empty());
    }

    #[tokio::test]
    async fn launching_by_preset_name_uses_its_params() {
        let st = state(vec![windowed_job("report")]);
        st.runner
            .store()
            .put_preset("report", "nightly", &json!({"days": 7}))
            .unwrap();

        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("report".into()),
            raw(r#"{"preset": "nightly"}"#),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let id = body["run_id"].as_str().unwrap();
        let run = st.runner.store().run(id).unwrap().unwrap();
        assert_eq!(run.params, json!({"days": 7}));

        // a name nothing was stored under is a 404, and launches nothing
        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("report".into()),
            raw(r#"{"preset": "ghost"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown preset: ghost on job report");
        assert_eq!(
            st.runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .len(),
            1
        );

        let (status, _) = launch_run(
            State(st),
            Path("nope".into()),
            raw(r#"{"preset": "nightly"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_launch_carries_its_tags_and_the_runs_filter_finds_them() {
        let st = state(vec![echo_job("etl")]);
        let launch =
            |body: &'static str| launch_run(State(st.clone()), Path("etl".into()), raw(body));

        let (_, Json(body)) = launch(r#"{"tags": {"kind": "smoke", "who": "me"}}"#)
            .await
            .unwrap();
        let tagged = body["run_id"].as_str().unwrap().to_string();
        let (_, Json(body)) = launch(r#"{"params": {"n": 1}}"#).await.unwrap();
        let plain = body["run_id"].as_str().unwrap().to_string();

        let run = st.runner.store().run(&tagged).unwrap().unwrap();
        assert_eq!(run.tags["kind"], "smoke");
        assert_eq!(run.tags["who"], "me");
        assert!(
            st.runner
                .store()
                .run(&plain)
                .unwrap()
                .unwrap()
                .tags
                .is_empty()
        );

        let ids = |tag: Option<&str>| {
            let q = RunsQuery {
                job: None,
                since: None,
                before: None,
                before_id: None,
                tag: tag.map(String::from),
                limit: None,
            };
            list_runs(State(st.clone()), Ok(Query(q)))
        };
        let Json(body) = ids(Some("kind:smoke")).await.unwrap();
        let listed = body["runs"].as_array().unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0]["id"], tagged);
        assert_eq!(listed[0]["tags"], json!({"kind": "smoke", "who": "me"}));
        // a run with no tags reports an empty map rather than a null
        let Json(body) = ids(None).await.unwrap();
        assert_eq!(body["runs"].as_array().unwrap().len(), 2);
        assert_eq!(body["runs"][0]["tags"], json!({}));
        // a value the run does not carry matches nothing
        let Json(body) = ids(Some("kind:backfill")).await.unwrap();
        assert!(body["runs"].as_array().unwrap().is_empty());

        // a filter that is not a pair is a 400, not a silently ignored filter
        for bad in ["kind", "kind:", ":smoke", ":"] {
            let (status, Json(body)) = ids(Some(bad)).await.unwrap_err();
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
            assert!(
                body["error"].as_str().unwrap().starts_with("invalid tag:"),
                "{body}"
            );
        }
        // a value may hold a colon; only the first splits
        let (_, Json(body)) = launch(r#"{"tags": {"at": "12:30"}}"#).await.unwrap();
        let colon = body["run_id"].as_str().unwrap().to_string();
        let Json(body) = ids(Some("at:12:30")).await.unwrap();
        assert_eq!(body["runs"][0]["id"], colon);
    }

    // two independent branches, a -> b -> c and d -> e: enough shape to tell
    // "and downstream" from "and everything"
    fn branched_job(name: &str) -> Job {
        let op = |n: &str| Op::new(n.to_string(), |_| async { Ok(json!(null)) });
        Job::builder(name)
            .op(op("a"))
            .op(op("b").after(["a"]))
            .op(op("c").after(["b"]))
            .op(op("d"))
            .op(op("e").after(["d"]))
            .build()
            .unwrap()
    }

    // the ops a run wrote rows for, which is what it covered
    fn covered(st: &AppState, id: &str) -> Vec<String> {
        let mut ops: Vec<String> = st
            .runner
            .store()
            .op_runs(id)
            .unwrap()
            .into_iter()
            .map(|r| r.op)
            .collect();
        ops.sort();
        ops
    }

    #[tokio::test]
    async fn a_subset_launch_runs_its_ops_and_their_downstream() {
        let st = state(vec![branched_job("etl")]);
        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("etl".into()),
            raw(r#"{"ops": ["d"], "tags": {"kind": "partial"}}"#),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let id = body["run_id"].as_str().unwrap().to_string();

        // the run records which ops it covered: d and everything downstream of
        // it, and nothing else — no row at all for the other branch
        assert_eq!(covered(&st, &id), ["d", "e"]);
        wait_success(&st, &id).await;
        let run = st.runner.store().run(&id).unwrap().unwrap();
        assert_eq!(run.trigger, Trigger::Manual);
        assert_eq!(run.tags["kind"], "partial");

        // an op named with its own upstream runs from there down
        let (_, Json(body)) = launch_run(
            State(st.clone()),
            Path("etl".into()),
            raw(r#"{"ops": ["a", "b"]}"#),
        )
        .await
        .unwrap();
        assert_eq!(
            covered(&st, body["run_id"].as_str().unwrap()),
            ["a", "b", "c"]
        );

        // and a launch that names no ops at all is still the whole job
        let (_, Json(body)) = launch_run(State(st.clone()), Path("etl".into()), raw("{}"))
            .await
            .unwrap();
        assert_eq!(
            covered(&st, body["run_id"].as_str().unwrap()),
            ["a", "b", "c", "d", "e"]
        );
    }

    // seeding nothing means an upstream left out has nothing to stand in for
    // it — the same refusal an asset build or a resume would get, from the
    // same check, naming what is missing
    #[tokio::test]
    async fn an_unsatisfiable_subset_is_a_400_naming_the_missing_dep() {
        let st = state(vec![branched_job("etl")]);
        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("etl".into()),
            raw(r#"{"ops": ["c"]}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let msg = body["error"].as_str().unwrap();
        assert!(msg.contains("subset op c depends on b"), "{msg}");
        assert!(
            msg.contains("neither in the subset nor seeded"),
            "cannot see what is missing: {msg}"
        );
        assert!(
            st.runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty(),
            "a refused subset left a run behind"
        );

        // an op the job does not have, from the same check
        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("etl".into()),
            raw(r#"{"ops": ["ghost"]}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("subset op ghost is not an op of the job"),
            "{body}"
        );

        // an empty selection is a request that names nothing, not one that
        // names everything
        let (status, Json(body)) =
            launch_run(State(st.clone()), Path("etl".into()), raw(r#"{"ops": []}"#))
                .await
                .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "no ops named");

        let (status, _) = launch_run(State(st), Path("nope".into()), raw(r#"{"ops": ["a"]}"#))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cloning_a_run_hands_over_its_params_and_tags() {
        let st = state(vec![echo_job("etl")]);
        let (_, Json(body)) = launch_run(
            State(st.clone()),
            Path("etl".into()),
            raw(r#"{"params": {"n": 5}, "tags": {"kind": "smoke"}}"#),
        )
        .await
        .unwrap();
        let id = body["run_id"].as_str().unwrap().to_string();

        let Json(body) = clone_run(State(st.clone()), Path(id.clone()))
            .await
            .unwrap();
        assert_eq!(
            body,
            json!({"job": "etl", "params": {"n": 5}, "tags": {"kind": "smoke"}})
        );
        // an untagged run clones as an untagged one, not as a null
        let (_, Json(plain)) = launch_run(State(st.clone()), Path("etl".into()), raw("{}"))
            .await
            .unwrap();
        let Json(body) = clone_run(
            State(st.clone()),
            Path(plain["run_id"].as_str().unwrap().into()),
        )
        .await
        .unwrap();
        assert_eq!(body["tags"], json!({}));
        assert_eq!(body["params"], json!({}));

        // and nothing was launched by any of it
        assert_eq!(
            st.runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .len(),
            2
        );

        let (status, Json(body)) = clone_run(State(st), Path("nope".into())).await.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown run: nope");
    }

    // a run outliving its job is the one case where a prefilled launchpad
    // would be a lie: it says so instead, exactly as a retry of one does
    #[tokio::test]
    async fn cloning_a_run_whose_job_is_gone_says_so() {
        let st = state(vec![echo_job("etl")]);
        let (_, Json(body)) = launch_run(State(st.clone()), Path("etl".into()), raw("{}"))
            .await
            .unwrap();
        let id = body["run_id"].as_str().unwrap().to_string();
        // finished, so the retry below refuses for the job rather than the status
        wait_success(&st, &id).await;

        // the same store, a process that no longer defines the job
        let runner = Runner::new(Vec::<Job>::new(), st.runner.store().clone());
        let gone = AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: Arc::new(AssetRegistry::empty()),
            sensors: Arc::new(Vec::new()),
        };
        let (status, Json(body)) = clone_run(State(gone.clone()), Path(id.clone()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "job no longer defined: etl");
        // which is what a retry of that run says too, so the two agree
        let (retry_status, Json(retry_body)) = retry_run(State(gone), Path(id)).await.unwrap_err();
        assert_eq!(retry_status, status);
        assert_eq!(retry_body["error"], body["error"]);
    }

    #[tokio::test]
    async fn a_launch_naming_both_params_and_a_preset_is_a_400() {
        let st = state(vec![windowed_job("report")]);
        st.runner
            .store()
            .put_preset("report", "nightly", &json!({"days": 7}))
            .unwrap();

        let (status, Json(body)) = launch_run(
            State(st.clone()),
            Path("report".into()),
            raw(r#"{"preset": "nightly", "params": {"days": 1}}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["error"],
            "params and preset are alternatives; name one"
        );
        assert!(
            st.runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .is_empty()
        );
    }

    // names and types, never values: a resource is usually a client holding
    // credentials
    #[tokio::test]
    async fn resources_endpoint_lists_names_and_types_only() {
        let mut built = std::collections::HashMap::new();
        built.insert(
            "api".to_string(),
            crate::resource::Resource {
                type_name: "demo::ApiClient",
                value: Arc::new("s3cret".to_string()),
            },
        );
        let runner = Runner::with_resources(
            Vec::<Job>::new(),
            Store::open(":memory:").unwrap(),
            Vec::new(),
            Vec::new(),
            Arc::new(built),
            crate::io::Io::default(),
        )
        .unwrap();
        let st = AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: Arc::new(AssetRegistry::empty()),
            sensors: Arc::new(Vec::new()),
        };
        let Json(body) = list_resources(State(st)).await;
        assert_eq!(
            body,
            json!({ "resources": [ { "name": "api", "type": "demo::ApiClient" } ] })
        );
        assert!(!body.to_string().contains("s3cret"));
    }

    // the dag draws a muted marker from this, and the default rule is the
    // absence of one
    #[tokio::test]
    async fn job_summary_reports_each_ops_trigger_rule() {
        let job = Job::builder("nightly")
            .op(Op::new("load", |_| async { Ok(json!(null)) }))
            .op(Op::new("summary", |_| async { Ok(json!(null)) })
                .after(["load"])
                .when(crate::model::When::Always))
            .op(Op::new("alert", |_| async { Ok(json!(null)) })
                .after(["load"])
                .when(crate::model::When::AnyFailed))
            .build()
            .unwrap();
        let Json(body) = get_job(State(state(vec![job])), Path("nightly".into()))
            .await
            .unwrap();
        let ops = body["ops"].as_array().unwrap();
        assert_eq!(ops[0]["when"], "all_succeeded");
        assert_eq!(ops[0]["requires"], json!([]));
        assert_eq!(ops[1]["when"], "always");
        assert_eq!(ops[2]["when"], "any_failed");
    }

    // the dag needs to know which node fans out; the instances themselves come
    // back from the run detail as ordinary op runs
    #[tokio::test]
    async fn job_summary_names_the_dep_a_mapped_op_fans_out_over() {
        let job = Job::builder("fanout")
            .op(Op::new("pages", |_| async { Ok(json!([1, 2])) }))
            .op(Op::mapped("process", |_ctx: OpCtx, page: u32| async move {
                Ok(json!(page))
            })
            .over("pages"))
            .build()
            .unwrap();
        let st = state(vec![job]);

        let Json(body) = get_job(State(st.clone()), Path("fanout".into()))
            .await
            .unwrap();
        let ops = body["ops"].as_array().unwrap();
        assert_eq!(ops[0]["mapped_over"], json!(null));
        assert_eq!(ops[1]["mapped_over"], "pages");
        assert_eq!(ops[1]["deps"], json!(["pages"]));

        let run = st
            .runner
            .run("fanout", json!({}), Trigger::Manual)
            .await
            .unwrap();
        let Json(body) = get_run(State(st), Path(run.id)).await.unwrap();
        let names: Vec<&str> = body["ops"]
            .as_array()
            .unwrap()
            .iter()
            .map(|o| o["op"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["pages", "process[0]", "process[1]"]);
    }

    // a job whose op reports a row count taken from the run's params, so
    // successive runs report successive numbers
    fn counting_job() -> Job {
        Job::builder("counter")
            .op(Op::new("load", |ctx| async move {
                let rows = ctx.params()["rows"].as_i64().unwrap_or(0);
                ctx.meta("rows", Meta::count(rows as u64));
                ctx.meta("note", "unchanged");
                Ok(json!(rows))
            }))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn a_run_op_row_carries_what_moved_since_the_last_run_of_that_op() {
        let st = state(vec![counting_job()]);
        let deltas_of = |body: &Value| body["ops"][0]["deltas"].clone();

        // the first run of all has nothing to compare against, and says so by
        // reporting no delta rather than a zero
        let first = st
            .runner
            .run("counter", json!({"rows": 1_203}), Trigger::Manual)
            .await
            .unwrap();
        let Json(body) = get_run(State(st.clone()), Path(first.id)).await.unwrap();
        assert_eq!(body["ops"][0]["metadata"]["rows"], json!({"count": 1_203}));
        assert_eq!(deltas_of(&body), json!({}));

        let second = st
            .runner
            .run("counter", json!({"rows": 1_240}), Trigger::Manual)
            .await
            .unwrap();
        let Json(body) = get_run(State(st.clone()), Path(second.id.clone()))
            .await
            .unwrap();
        // the number that moved, and nothing about the text that did not
        assert_eq!(
            deltas_of(&body),
            json!({"rows": {"delta": 37, "delta_pct": 3.08}})
        );

        // the row is otherwise exactly what it was: deltas sit beside the
        // fields the ui already reads
        let row = &body["ops"][0];
        assert_eq!(row["op"], "load");
        assert_eq!(row["status"], "success");
        assert_eq!(row["output"], json!(1_240));
        assert_eq!(row["error"], json!(null));
        assert_eq!(row["pid"], json!(null));

        // asking again gets the same answer: a run's deltas are against the
        // run before it, not against whatever ran most recently
        let third = st
            .runner
            .run("counter", json!({"rows": 1_000}), Trigger::Manual)
            .await
            .unwrap();
        let Json(body) = get_run(State(st.clone()), Path(second.id)).await.unwrap();
        assert_eq!(
            deltas_of(&body),
            json!({"rows": {"delta": 37, "delta_pct": 3.08}})
        );
        let Json(body) = get_run(State(st), Path(third.id)).await.unwrap();
        assert_eq!(
            deltas_of(&body),
            json!({"rows": {"delta": -240, "delta_pct": -19.35}})
        );
    }

    #[tokio::test]
    async fn history_entries_carry_deltas_against_the_previous_build() {
        let st = asset_state();
        let store = st.runner.store().clone();
        let meta =
            |rows: i64, note: &str| json!({ "rows": {"count": rows}, "note": {"text": note} });
        for (rows, note) in [(100, "a"), (150, "b"), (150, "b")] {
            store
                .record_materialization(
                    "docs",
                    None,
                    "d1",
                    &json!({}),
                    None,
                    None,
                    Some(&meta(rows, note)),
                )
                .unwrap();
        }
        // and a build that reported the key as something else entirely
        store
            .record_materialization(
                "docs",
                None,
                "d1",
                &json!({}),
                None,
                None,
                Some(&json!({"rows": {"text": "lots"}})),
            )
            .unwrap();

        let Json(body) = asset_history(
            State(st),
            Path("docs".into()),
            Ok(Query(HistoryQuery {
                limit: None,
                partition: None,
            })),
        )
        .await
        .unwrap();
        let rows = body["materializations"].as_array().unwrap();
        let seen: Vec<&Value> = rows.iter().map(|r| &r["deltas"]).collect();
        assert_eq!(
            seen,
            [
                // a type change is not a delta of any size
                &json!({}),
                // built again, reporting the same number
                &json!({"rows": {"delta": 0, "delta_pct": 0}}),
                &json!({"rows": {"delta": 50, "delta_pct": 50}}),
                // the first build of all
                &json!({}),
            ]
        );
    }

    #[tokio::test]
    async fn schedules_report_their_params() {
        let st = state(vec![echo_job("etl")]);
        st.runner
            .store()
            .sync_schedules(&[Schedule::new("etl", "0 * * * *").params(json!({"region": "eu"}))])
            .unwrap();

        let Json(body) = list_schedules(State(st.clone())).await.unwrap();
        assert_eq!(body["schedules"][0]["params"], json!({"region": "eu"}));

        let Json(body) = get_job(State(st), Path("etl".into())).await.unwrap();
        assert_eq!(body["schedules"][0]["params"], json!({"region": "eu"}));
    }

    #[tokio::test]
    async fn job_state_endpoint_lists_and_404s() {
        let st = state(vec![echo_job("etl")]);
        let (status, Json(body)) = job_state(State(st.clone()), Path("nope".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown job: nope");

        let Json(body) = job_state(State(st.clone()), Path("etl".into()))
            .await
            .unwrap();
        assert!(body["states"].as_array().unwrap().is_empty());

        let store = st.runner.store();
        store
            .set_op_state("etl", "pull", &json!({"cursor": 9}))
            .unwrap();
        store.set_op_state("other", "pull", &json!(1)).unwrap();
        let Json(body) = job_state(State(st), Path("etl".into())).await.unwrap();
        let states = body["states"].as_array().unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(states[0]["op"], "pull");
        assert_eq!(states[0]["value"], json!({"cursor": 9}));
        assert!(states[0]["updated_at"].is_string());
    }

    #[tokio::test]
    async fn the_queue_endpoint_reports_depth_and_blockers_and_takes_a_bump() {
        let slow = Job::builder("slow")
            .op(Op::new("nap", |_| async {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                Ok(json!(null))
            }))
            .build()
            .unwrap();
        let runner = Runner::new([slow], Store::open(":memory:").unwrap())
            .with_limits(crate::executor::Limits::new().global(1), 0);
        let st = AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: Arc::new(AssetRegistry::empty()),
            sensors: Arc::new(Vec::new()),
        };

        st.runner
            .launch("slow", json!({}), Trigger::Manual)
            .unwrap();
        let first = st
            .runner
            .launch("slow", json!({}), Trigger::Manual)
            .unwrap();
        let second = st
            .runner
            .launch("slow", json!({}), Trigger::Manual)
            .unwrap();

        let Json(body) = list_queue(State(st.clone())).await.unwrap();
        assert_eq!(body["depth"], 2);
        assert_eq!(body["limits"]["global"], 1);
        let queued = body["queued"].as_array().unwrap();
        assert_eq!(queued[0]["run"]["id"], first.as_str());
        assert_eq!(queued[0]["position"], 1);
        assert_eq!(queued[0]["blocked_by"]["scope"], "global");
        assert!(
            queued[0]["blocked_by"]["reason"]
                .as_str()
                .unwrap()
                .contains("which is the limit"),
            "{}",
            queued[0]["blocked_by"]
        );
        // the run json carries the queue's own fields now
        assert_eq!(queued[0]["run"]["priority"], 0);
        assert_eq!(queued[0]["run"]["claimed_by"], Value::Null);

        // a bump moves it to the head, and a run already claimed refuses
        let bump = |id: String, n: i64| {
            set_run_priority(
                State(st.clone()),
                Path(id),
                Bytes::from(format!("{{\"priority\":{n}}}")),
            )
        };
        let _ = bump(second.clone(), 5).await.unwrap();
        let Json(body) = list_queue(State(st.clone())).await.unwrap();
        assert_eq!(body["queued"].as_array().unwrap()[0]["run"]["id"], second);

        let (status, _) = bump("nope".into(), 1).await.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        let running = st
            .runner
            .store()
            .runs(None, None, None, None, None, 10)
            .unwrap()
            .into_iter()
            .find(|r| r.claimed_by.is_some())
            .unwrap();
        let (status, _) = bump(running.id, 1).await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn cancel_endpoint_statuses() {
        let slow = Job::builder("slow")
            .op(Op::new("nap", |_| async {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                Ok(json!(null))
            }))
            .build()
            .unwrap();
        let st = state(vec![slow]);

        let (status, Json(body)) = cancel_run(State(st.clone()), Path("nope".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown run: nope");

        insert_run(&st, "done", "slow", RunStatus::Success, json!({}));
        let (status, Json(body)) = cancel_run(State(st.clone()), Path("done".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "run already finished: done");

        // a queued run nobody has claimed is cancellable whoever asks: it is a
        // row on the queue, and taking it off is the whole of stopping it
        insert_run(&st, "waiting", "slow", RunStatus::Queued, json!({}));
        let (status, _) = cancel_run(State(st.clone()), Path("waiting".into()))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(
            st.runner.store().run("waiting").unwrap().unwrap().status,
            RunStatus::Canceled
        );

        // a running row with no live executor, as a restart leaves behind: the
        // signal has nowhere to go, and saying so beats pretending
        insert_run(&st, "stale", "slow", RunStatus::Running, json!({}));
        let (status, _) = cancel_run(State(st.clone()), Path("stale".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);

        let id = st
            .runner
            .launch("slow", json!({}), Trigger::Manual)
            .unwrap();
        let (status, Json(body)) = cancel_run(State(st), Path(id)).await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, json!({"ok": true}));
    }

    #[tokio::test]
    async fn upcoming_excludes_paused_and_caps() {
        let st = state(vec![echo_job("etl"), echo_job("health")]);
        st.runner
            .store()
            .sync_schedules(&[
                Schedule::new("etl", "* * * * *"),
                Schedule::new("health", "0 * * * *"),
            ])
            .unwrap();
        st.runner
            .store()
            .set_schedule_paused("health", "0 * * * *", true)
            .unwrap();

        let Json(body) = upcoming_schedules(State(st), Ok(Query(UpcomingQuery { window: None })))
            .await
            .unwrap();
        let upcoming = body["upcoming"].as_array().unwrap();
        assert_eq!(upcoming.len(), 1);
        assert_eq!(upcoming[0]["job"], "etl");
        assert_eq!(upcoming[0]["expr"], "* * * * *");
        assert_eq!(upcoming[0]["times"].as_array().unwrap().len(), 100);
    }

    #[tokio::test]
    async fn retry_relaunches_with_original_params() {
        let st = state(vec![echo_job("etl")]);
        // awaited: retry only accepts finished runs
        let id = st
            .runner
            .run("etl", json!({"n": 5}), Trigger::Manual)
            .await
            .unwrap()
            .id;

        let (status, Json(body)) = retry_run(State(st.clone()), Path(id.clone()))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let new_id = body["run_id"].as_str().unwrap();
        assert_ne!(new_id, id);
        let run = st.runner.store().run(new_id).unwrap().unwrap();
        assert_eq!(run.trigger, Trigger::Retry);
        assert_eq!(run.params, json!({"n": 5}));
    }

    #[tokio::test]
    async fn retry_unknown_run_404() {
        let st = state(vec![]);
        let (status, Json(body)) = retry_run(State(st), Path("nope".into())).await.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown run: nope");
    }

    #[tokio::test]
    async fn retry_active_run_409() {
        let st = state(vec![echo_job("etl")]);
        insert_run(&st, "r1", "etl", RunStatus::Queued, json!({}));

        let (status, Json(body)) = retry_run(State(st.clone()), Path("r1".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "run still active: r1");

        st.runner.store().run_started("r1").unwrap();
        let (status, Json(body)) = retry_run(State(st.clone()), Path("r1".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "run still active: r1");

        insert_run(&st, "g1", "ghost", RunStatus::Running, json!({}));
        let (status, Json(body)) = retry_run(State(st.clone()), Path("g1".into()))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "run still active: g1");

        st.runner
            .store()
            .run_finished("r1", RunStatus::Failed, None)
            .unwrap();
        let (status, Json(body)) = retry_run(State(st.clone()), Path("r1".into()))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let rerun = st
            .runner
            .store()
            .run(body["run_id"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(rerun.trigger, Trigger::Retry);
    }

    #[tokio::test]
    async fn retry_undefined_job_409() {
        let st = state(vec![]);
        insert_run(&st, "r1", "ghost", RunStatus::Failed, json!({}));
        let (status, Json(body)) = retry_run(State(st), Path("r1".into())).await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "job no longer defined: ghost");
    }

    #[tokio::test]
    async fn retry_revalidates_params() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Params {
            threshold: u32,
        }
        let job = Job::builder("gated")
            .op(Op::new("check", |_| async { Ok(json!(null)) }).params::<Params>())
            .build()
            .unwrap();
        let st = state(vec![job]);
        // a run recorded before the job grew params validation
        insert_run(
            &st,
            "r1",
            "gated",
            RunStatus::Failed,
            json!({"threshold": "high"}),
        );
        let (status, Json(body)) = retry_run(State(st), Path("r1".into())).await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("invalid params"));
    }

    fn raw(s: &str) -> Bytes {
        Bytes::from(s.to_string())
    }

    // a -> b -> c with b failing: enough shape for a resume to have a subset
    fn brittle_job(name: &str) -> Job {
        Job::builder(name)
            .op(Op::new("a", |_| async { Ok(json!({"rows": 3})) }))
            .op(Op::new("b", |_| async { Err("boom".into()) }).after(["a"]))
            .op(Op::new("c", |_| async { Ok(json!(null)) }).after(["b"]))
            .build()
            .unwrap()
    }

    fn insert_run_with_ops(st: &AppState, id: &str, job: &str, status: RunStatus, ops: &[&str]) {
        let run = Run {
            id: id.into(),
            job: job.into(),
            status,
            trigger: Trigger::Manual,
            params: json!({}),
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            error: None,
            resumed_from: None,
            scheduled_for: None,
            tags: Default::default(),
            priority: 0,
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
        };
        let ops: Vec<String> = ops.iter().map(|o| o.to_string()).collect();
        st.runner.store().create_run(&run, &ops).unwrap();
    }

    #[tokio::test]
    async fn resume_endpoint_launches_the_unfinished_subset() {
        let st = state(vec![brittle_job("etl")]);
        let failed = st
            .runner
            .run("etl", json!({"n": 5}), Trigger::Manual)
            .await
            .unwrap();
        assert_eq!(failed.status, RunStatus::Failed);

        let (status, Json(body)) = resume_run(State(st.clone()), Path(failed.id.clone()), raw(""))
            .await
            .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let new_id = body["run_id"].as_str().unwrap();
        let run = st.runner.store().run(new_id).unwrap().unwrap();
        assert_eq!(run.trigger, Trigger::Resume);
        assert_eq!(run.params, json!({"n": 5}));
        assert_eq!(run.resumed_from.as_deref(), Some(failed.id.as_str()));
        // the rows exist from the launch on: only the failed op and downstream
        let ops: Vec<String> = st
            .runner
            .store()
            .op_runs(new_id)
            .unwrap()
            .into_iter()
            .map(|o| o.op)
            .collect();
        assert_eq!(ops, ["b", "c"]);

        // and from a chosen op: a succeeded, and still runs again
        let (status, Json(body)) = resume_run(
            State(st.clone()),
            Path(failed.id.clone()),
            raw(r#"{"from": ["a"]}"#),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let ops = st
            .runner
            .store()
            .op_runs(body["run_id"].as_str().unwrap())
            .unwrap();
        assert_eq!(ops.len(), 3);
    }

    #[tokio::test]
    async fn resume_endpoint_statuses() {
        let st = state(vec![echo_job("etl")]);
        let resume = |st: &AppState, id: &str, b: &'static str| {
            resume_run(State(st.clone()), Path(id.into()), raw(b))
        };

        let (status, Json(b)) = resume(&st, "nope", "").await.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(b["error"], "unknown run: nope");

        insert_run_with_ops(&st, "live", "etl", RunStatus::Queued, &["echo"]);
        let (status, Json(b)) = resume(&st, "live", "").await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(b["error"], "run still active: live");

        insert_run_with_ops(&st, "won", "etl", RunStatus::Success, &["echo"]);
        let (status, Json(b)) = resume(&st, "won", "").await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(b["error"], "run did not fail: won");

        insert_run_with_ops(&st, "orphan", "gone", RunStatus::Failed, &["echo"]);
        let (status, Json(b)) = resume(&st, "orphan", "").await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(b["error"], "job no longer defined: gone");

        // a run recorded against a graph the job no longer has
        insert_run_with_ops(&st, "wide", "etl", RunStatus::Failed, &["echo", "extra"]);
        let (status, Json(b)) = resume(&st, "wide", "").await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            b["error"]
                .as_str()
                .unwrap()
                .contains("only in the run: extra"),
            "{b}"
        );

        // failed as a whole, but every op of it succeeded
        insert_run_with_ops(&st, "clean", "etl", RunStatus::Failed, &["echo"]);
        st.runner
            .store()
            .op_finished(
                "clean",
                "echo",
                OpStatus::Success,
                Some(&json!(1)),
                None,
                None,
            )
            .unwrap();
        let (status, Json(b)) = resume(&st, "clean", "").await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            b["error"],
            "nothing to resume: every op of run clean already succeeded"
        );

        insert_run_with_ops(&st, "r1", "etl", RunStatus::Failed, &["echo"]);
        let (status, Json(b)) = resume(&st, "r1", r#"{"from": ["ghost"]}"#)
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(b["error"].as_str().unwrap().contains("ghost"), "{b}");

        let (status, Json(b)) = resume(&st, "r1", "{not json}").await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            b["error"].as_str().unwrap().starts_with("invalid body"),
            "{b}"
        );

        // an empty body and an empty selection both mean "from the failure"
        let (status, _) = resume(&st, "r1", r#"{"from": []}"#).await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn resume_preview_lists_reuse_and_rerun() {
        let st = state(vec![brittle_job("etl")]);
        let preview = |st: &AppState, id: &str, from: Option<&str>| {
            resume_preview(
                State(st.clone()),
                Path(id.into()),
                Ok(Query(ResumePreviewQuery {
                    from: from.map(String::from),
                })),
            )
        };
        let failed = st
            .runner
            .run("etl", json!({}), Trigger::Manual)
            .await
            .unwrap();

        let Json(b) = preview(&st, &failed.id, None).await.unwrap();
        assert_eq!(b, json!({"reuse": ["a"], "rerun": ["b", "c"]}));

        // from an op that succeeded: it and its downstream, nothing reused
        let Json(b) = preview(&st, &failed.id, Some("a")).await.unwrap();
        assert_eq!(b, json!({"reuse": [], "rerun": ["a", "b", "c"]}));

        // names are comma separated and trimmed
        let Json(b) = preview(&st, &failed.id, Some("b, ")).await.unwrap();
        assert_eq!(b, json!({"reuse": ["a"], "rerun": ["b", "c"]}));

        // c's input never got produced, so no plan can honour re-running it
        let (status, Json(b)) = preview(&st, &failed.id, Some("c")).await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            b["error"]
                .as_str()
                .unwrap()
                .contains("has no recorded output"),
            "{b}"
        );

        // the same refusals as the launch, so a preview never promises more
        let (status, Json(b)) = preview(&st, "nope", None).await.unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(b["error"], "unknown run: nope");

        let (status, _) = preview(&st, &failed.id, Some("ghost")).await.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        insert_run_with_ops(&st, "live", "etl", RunStatus::Running, &["a", "b", "c"]);
        let (status, Json(b)) = preview(&st, "live", None).await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(b["error"], "run still active: live");
    }

    #[tokio::test]
    async fn overdue_follows_missed_fires() {
        let st = state(vec![echo_job("etl")]);
        let job = &st.jobs["etl"];

        let s = job_summary(job, &st).unwrap();
        assert_eq!(s["interval_secs"], json!(null));
        assert_eq!(s["overdue"], json!(false));

        // two fires a minute apart on jan 1, so overdue is deterministic year-round
        st.runner
            .store()
            .sync_schedules(&[Schedule::new("etl", "0,1 0 1 1 *")])
            .unwrap();

        let s = job_summary(job, &st).unwrap();
        assert_eq!(s["interval_secs"], json!(60));
        assert_eq!(s["overdue"], json!(true));

        let stale = Run {
            id: "old".into(),
            job: "etl".into(),
            status: RunStatus::Success,
            trigger: Trigger::Manual,
            params: json!({}),
            created_at: Utc::now() - Duration::days(400),
            started_at: None,
            finished_at: Some(Utc::now() - Duration::days(400)),
            error: None,
            resumed_from: None,
            scheduled_for: None,
            tags: Default::default(),
            priority: 0,
            claimed_by: None,
            claimed_at: None,
            lease_until: None,
        };
        st.runner.store().create_run(&stale, &[]).unwrap();
        let s = job_summary(job, &st).unwrap();
        assert_eq!(s["overdue"], json!(true));

        st.runner
            .run("etl", json!({}), Trigger::Manual)
            .await
            .unwrap();
        let s = job_summary(job, &st).unwrap();
        assert_eq!(s["overdue"], json!(false));

        st.runner
            .store()
            .set_schedule_paused("etl", "0,1 0 1 1 *", true)
            .unwrap();
        let s = job_summary(job, &st).unwrap();
        assert_eq!(s["interval_secs"], json!(null));
        assert_eq!(s["overdue"], json!(false));
    }

    #[tokio::test]
    async fn a_declared_policy_replaces_the_cron_heuristic() {
        let hourly = Job::builder("etl")
            .fresh_within(std::time::Duration::from_secs(3600))
            .op(Op::new("echo", |ctx: OpCtx| async move {
                Ok(ctx.params().clone())
            }))
            .build()
            .unwrap();
        let st = state(vec![hourly, echo_job("plain")]);
        // the same schedule that makes `plain` overdue below
        let sched = |job: &str| Schedule::new(job, "0,1 0 1 1 *");
        st.runner
            .store()
            .sync_schedules(&[sched("etl"), sched("plain")])
            .unwrap();

        let plain = job_summary(&st.jobs["plain"], &st).unwrap();
        assert_eq!(plain["overdue"], json!(true));
        assert_eq!(plain["freshness"], json!(null));

        // the heuristic would say overdue too; the policy is asked instead, and
        // never having succeeded is not late
        let etl = job_summary(&st.jobs["etl"], &st).unwrap();
        assert_eq!(etl["overdue"], json!(false), "the policy is the answer now");
        assert_eq!(etl["freshness"]["status"], json!("never"));
        assert_eq!(etl["freshness"]["last_success"], json!(null));

        st.runner
            .run("etl", json!({}), Trigger::Manual)
            .await
            .unwrap();
        let etl = job_summary(&st.jobs["etl"], &st).unwrap();
        assert_eq!(etl["freshness"]["status"], json!("fresh"));
        assert_eq!(etl["freshness"]["late_by_secs"], json!(null));
        assert_eq!(etl["overdue"], json!(false));

        let (status, body, _) = request(router(st.clone()), Method::GET, "/api/late").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.unwrap()["late"], json!([]));

        // an hour and a half of nothing: the policy says late, and /api/late
        // says the same thing in the same words
        let id = st
            .runner
            .store()
            .runs(Some("etl"), None, None, None, None, 1)
            .unwrap()[0]
            .id
            .clone();
        st.runner
            .store()
            .backdate_run(&id, Utc::now() - Duration::minutes(90))
            .unwrap();
        let etl = job_summary(&st.jobs["etl"], &st).unwrap();
        assert_eq!(etl["freshness"]["status"], json!("late"));
        assert_eq!(etl["freshness"]["late_by_secs"], json!(1800));
        let (_, body, _) = request(router(st.clone()), Method::GET, "/api/late").await;
        let late = body.unwrap();
        assert_eq!(late["late"][0]["kind"], json!("job"));
        assert_eq!(late["late"][0]["name"], json!("etl"));
        assert_eq!(late["late"][0]["late_by_secs"], json!(1800));
    }

    #[test]
    fn overdue_anchors_on_previous_fire_not_interval() {
        use chrono::TimeZone;
        let row = ScheduleRow {
            job: "report".into(),
            expr: "0 9 * * 1-5".into(),
            tz: "UTC".into(),
            paused: false,
            params: json!({}),
            catchup: crate::model::Catchup::Skip,
            cursor: None,
        };
        // sunday noon: the weekday schedule last fired friday 09:00
        let sunday = Utc.with_ymd_and_hms(2026, 8, 9, 12, 0, 0).unwrap();
        let prev = prev_fire(&row, sunday).unwrap();
        assert_eq!(prev, Utc.with_ymd_and_hms(2026, 8, 7, 9, 0, 0).unwrap());

        // older than an interval-sized window but newer than the fire: planned gap
        let friday_success = Utc.with_ymd_and_hms(2026, 8, 7, 9, 5, 0).unwrap();
        assert!(!is_overdue(prev, 86400, Some(friday_success), sunday));

        let thursday = Utc.with_ymd_and_hms(2026, 8, 6, 10, 0, 0).unwrap();
        assert!(is_overdue(prev, 86400, Some(thursday), sunday));
        assert!(is_overdue(prev, 86400, None, sunday));

        let just_after = prev + Duration::minutes(10);
        assert!(!is_overdue(prev, 86400, None, just_after));
    }

    async fn request(
        app: Router,
        method: Method,
        path: &str,
    ) -> (StatusCode, Option<Value>, String) {
        use tower::util::ServiceExt;
        let req = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).ok(), content_type)
    }

    // source -> derived -> derived(auto): enough graph for every endpoint shape
    fn asset_state() -> AppState {
        let docs = crate::Asset::source("docs");
        let stats = crate::Asset::new("stats", |_| async { Ok(json!({"files": 2})) }).from(&docs);
        let totals = crate::Asset::new("totals", |ctx| async move {
            let files = ctx.input("stats").unwrap()["files"].as_u64().unwrap();
            Ok(json!({"total": files}))
        })
        .from(&stats)
        .auto();
        let registry = Arc::new(
            AssetRegistry::new(vec![docs, stats, totals], Vec::new(), Vec::new()).unwrap(),
        );
        let runner = Runner::new(
            [registry.lower_job().unwrap()],
            Store::open(":memory:").unwrap(),
        );
        AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: registry,
            sensors: Arc::new(Vec::new()),
        }
    }

    async fn wait_success(st: &AppState, run_id: &str) {
        for _ in 0..300 {
            let run = st.runner.store().run(run_id).unwrap().unwrap();
            match run.status {
                RunStatus::Success => return,
                RunStatus::Queued | RunStatus::Running => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                other => panic!("run {run_id} finished {other:?}"),
            }
        }
        panic!("run {run_id} never succeeded");
    }

    // a build of one named asset says which one; a build of everything stale
    // names nothing, because there is nothing to name
    #[tokio::test]
    async fn a_build_of_one_asset_is_tagged_with_it() {
        let st = asset_state();
        st.runner
            .store()
            .record_materialization("docs", None, "d1", &json!({}), None, None, None)
            .unwrap();

        let (_, Json(body)) =
            build_one_asset(State(st.clone()), Path("stats".into()), Bytes::new())
                .await
                .unwrap();
        let run_id = body["run_id"].as_str().unwrap().to_string();
        wait_success(&st, &run_id).await;
        let run = st.runner.store().run(&run_id).unwrap().unwrap();
        assert_eq!(run.trigger, Trigger::Build);
        assert_eq!(run.tags["asset"], "stats");

        let (_, Json(body)) = build_all_assets(State(st.clone())).await.unwrap();
        let all_id = body["run_ids"][0].as_str().unwrap().to_string();
        wait_success(&st, &all_id).await;
        assert!(
            st.runner
                .store()
                .run(&all_id)
                .unwrap()
                .unwrap()
                .tags
                .is_empty()
        );
    }

    #[tokio::test]
    async fn assets_endpoint_lists_topo_with_staleness() {
        let st = asset_state();
        let Json(body) = list_assets(State(st.clone())).await.unwrap();
        let assets = body["assets"].as_array().unwrap();
        let names: Vec<&str> = assets.iter().map(|a| a["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["docs", "stats", "totals"]);
        assert_eq!(assets[0]["kind"], "source");
        assert_eq!(assets[1]["kind"], "derived");
        assert_eq!(assets[1]["deps"], json!(["docs"]));
        assert_eq!(assets[2]["auto"], json!(true));
        for a in assets {
            assert_eq!(a["fingerprint"], json!(null));
            assert_eq!(a["built_at"], json!(null));
            assert_eq!(a["stale"], json!(true));
            assert_eq!(a["reasons"], json!([]));
        }

        st.runner
            .store()
            .record_materialization("docs", None, "d1", &json!({}), None, None, None)
            .unwrap();
        let (status, Json(body)) = build_all_assets(State(st.clone())).await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let run_id = body["run_ids"][0].as_str().unwrap();
        wait_success(&st, run_id).await;

        let Json(body) = list_assets(State(st.clone())).await.unwrap();
        let assets = body["assets"].as_array().unwrap();
        assert!(assets.iter().all(|a| a["stale"] == json!(false)));
        assert_eq!(assets[1]["run_id"].as_str().unwrap(), run_id);
        assert!(assets[1]["fingerprint"].is_string());
        assert!(assets[1]["built_at"].is_string());
        assert_eq!(assets[0]["run_id"], json!(null));

        st.runner
            .store()
            .record_materialization("docs", None, "d2", &json!({}), None, None, None)
            .unwrap();
        let Json(body) = list_assets(State(st)).await.unwrap();
        let stats = &body["assets"].as_array().unwrap()[1];
        assert_eq!(stats["stale"], json!(true));
        assert_eq!(stats["reasons"][0]["dep"], "docs");
        assert_eq!(stats["reasons"][0]["had"], "d1");
        assert_eq!(stats["reasons"][0]["now"], "d2");
    }

    #[tokio::test]
    async fn history_endpoint_lists_changes_newest_first() {
        let st = asset_state();
        let store = st.runner.store().clone();
        for fp in ["d1", "d1", "d2"] {
            store
                .record_materialization("docs", None, fp, &json!({}), None, None, None)
                .unwrap();
        }

        let Json(body) = asset_history(
            State(st.clone()),
            Path("docs".into()),
            Ok(Query(HistoryQuery {
                limit: None,
                partition: None,
            })),
        )
        .await
        .unwrap();
        let rows = body["materializations"].as_array().unwrap();
        assert_eq!(rows.len(), 3);
        let seen: Vec<(&str, bool)> = rows
            .iter()
            .map(|r| {
                (
                    r["fingerprint"].as_str().unwrap(),
                    r["changed"].as_bool().unwrap(),
                )
            })
            .collect();
        assert_eq!(seen, [("d2", true), ("d1", false), ("d1", true)]);
        assert!(rows[0]["built_at"].is_string());
        assert_eq!(rows[0]["run_id"], json!(null));
        assert!(
            rows[0]["value"].is_null(),
            "history carries facts, not payloads"
        );

        // out-of-range limits clamp rather than 400
        for (asked, want) in [(0u32, 1usize), (500, 3)] {
            let Json(body) = asset_history(
                State(st.clone()),
                Path("docs".into()),
                Ok(Query(HistoryQuery {
                    limit: Some(asked),
                    partition: None,
                })),
            )
            .await
            .unwrap();
            assert_eq!(body["materializations"].as_array().unwrap().len(), want);
        }

        let (status, Json(body)) = asset_history(
            State(st),
            Path("nope".into()),
            Ok(Query(HistoryQuery {
                limit: None,
                partition: None,
            })),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown asset: nope");
    }

    #[tokio::test]
    async fn checks_endpoint_and_asset_summary_counts() {
        // one asset with a passing and a failing check, one with none
        let docs = crate::Asset::source("docs");
        let stats = crate::Asset::new("stats", |_| async { Ok(json!({"files": 2})) }).from(&docs);
        let totals = crate::Asset::new("totals", |_| async { Ok(json!(null)) }).from(&stats);
        let checks = vec![
            crate::AssetCheck::new("has_files", "stats", |_, v: Value| async move {
                let n = v["files"].as_u64().unwrap_or(0);
                Ok(crate::CheckResult::pass().meta("files", n as i64))
            }),
            crate::AssetCheck::new("many_files", "stats", |_, _| async {
                Ok(crate::CheckResult::fail("only 2"))
            })
            .severity(crate::Severity::Warn),
        ];
        let registry =
            Arc::new(AssetRegistry::new(vec![docs, stats, totals], Vec::new(), checks).unwrap());
        let runner = Runner::new(
            [registry.lower_job().unwrap()],
            Store::open(":memory:").unwrap(),
        );
        let st = AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: registry,
            sensors: Arc::new(Vec::new()),
        };
        st.runner
            .store()
            .record_materialization("docs", None, "d1", &json!({}), None, None, None)
            .unwrap();

        let (status, Json(body)) = build_all_assets(State(st.clone())).await.unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        wait_success(&st, body["run_ids"][0].as_str().unwrap()).await;

        let Json(body) = list_assets(State(st.clone())).await.unwrap();
        let assets = body["assets"].as_array().unwrap();
        let stats = assets.iter().find(|a| a["name"] == "stats").unwrap();
        assert_eq!(stats["checks"]["passed"], json!(1));
        assert_eq!(stats["checks"]["failed"], json!(1));
        assert!(stats["checks"]["last_run_at"].is_string());
        // an asset nobody checks reads as zero and zero, with no timestamp
        let totals = assets.iter().find(|a| a["name"] == "totals").unwrap();
        assert_eq!(
            totals["checks"],
            json!({"passed": 0, "failed": 0, "last_run_at": null})
        );

        let Json(body) = asset_checks(
            State(st.clone()),
            Path("stats".into()),
            Ok(Query(HistoryQuery {
                limit: None,
                partition: None,
            })),
        )
        .await
        .unwrap();
        let rows = body["checks"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        let passed = rows.iter().find(|c| c["check"] == "has_files").unwrap();
        assert_eq!(passed["status"], "passed");
        assert_eq!(passed["severity"], "error");
        assert_eq!(passed["metadata"], json!({"files": {"int": 2}}));
        let failed = rows.iter().find(|c| c["check"] == "many_files").unwrap();
        assert_eq!(failed["status"], "failed");
        assert_eq!(failed["severity"], "warn");
        assert_eq!(failed["message"], "only 2");

        // same clamps as history, and the same 404
        let Json(body) = asset_checks(
            State(st.clone()),
            Path("stats".into()),
            Ok(Query(HistoryQuery {
                limit: Some(0),
                partition: None,
            })),
        )
        .await
        .unwrap();
        assert_eq!(body["checks"].as_array().unwrap().len(), 1);
        let (status, Json(body)) = asset_checks(
            State(st),
            Path("nope".into()),
            Ok(Query(HistoryQuery {
                limit: None,
                partition: None,
            })),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown asset: nope");
    }

    #[tokio::test]
    async fn build_endpoint_statuses() {
        let st = asset_state();
        let (status, Json(body)) =
            build_one_asset(State(st.clone()), Path("nope".into()), Bytes::new())
                .await
                .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown asset: nope");

        // the source must have probed once, or every descendant stays provably stale
        st.runner
            .store()
            .record_materialization("docs", None, "d1", &json!({}), None, None, None)
            .unwrap();

        let (status, Json(body)) =
            build_one_asset(State(st.clone()), Path("totals".into()), Bytes::new())
                .await
                .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        let run_id = body["run_id"].as_str().unwrap().to_string();
        wait_success(&st, &run_id).await;
        let run = st.runner.store().run(&run_id).unwrap().unwrap();
        assert_eq!(run.job, "assets");
        assert_eq!(run.trigger, Trigger::Build);

        let (status, Json(body)) =
            build_one_asset(State(st.clone()), Path("totals".into()), Bytes::new())
                .await
                .unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"up_to_date": true}));

        let (status, Json(body)) =
            build_one_asset(State(st.clone()), Path("docs".into()), Bytes::new())
                .await
                .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"], "sources are probed, never built");

        let (status, Json(body)) = build_all_assets(State(st)).await.unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"up_to_date": true}));
    }

    fn partitioned_state() -> AppState {
        let daily = crate::Asset::new("daily", |ctx: crate::OpCtx| async move {
            Ok(json!({ "key": ctx.partition() }))
        })
        .partitioned(crate::Partitions::keys(["k1", "k2", "k3"]));
        let registry = Arc::new(AssetRegistry::new(vec![daily], Vec::new(), Vec::new()).unwrap());
        let runner = Runner::new(
            [registry.lower_job().unwrap()],
            Store::open(":memory:").unwrap(),
        );
        AppState {
            jobs: Arc::new(runner.jobs().clone()),
            runner,
            assets: registry,
            sensors: Arc::new(Vec::new()),
        }
    }

    #[tokio::test]
    async fn a_partitioned_asset_reports_its_key_set_and_builds_one_key() {
        let st = partitioned_state();
        // nothing built: three keys, all missing
        let Json(body) = list_assets(State(st.clone())).await.unwrap();
        let asset = &body["assets"][0];
        assert_eq!(
            asset["fingerprint"],
            Value::Null,
            "a key set has no one fingerprint"
        );
        assert_eq!(
            asset["partitions"],
            json!({"total": 3, "materialized": 0, "stale": 0, "missing": 3})
        );

        // an unknown key is the request's fault, not the server's
        let (status, Json(body)) = build_one_asset(
            State(st.clone()),
            Path("daily".into()),
            Bytes::from(r#"{"partitions":["nope"]}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            body["error"].as_str().unwrap().contains("no partition"),
            "{body}"
        );
        // and so is a body that does not parse
        let (status, _) = build_one_asset(
            State(st.clone()),
            Path("daily".into()),
            Bytes::from(r#"{"partitions": 7}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, Json(body)) = build_one_asset(
            State(st.clone()),
            Path("daily".into()),
            Bytes::from(r#"{"partitions":["k2"]}"#),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        wait_success(&st, body["run_id"].as_str().unwrap()).await;

        let Json(body) = list_assets(State(st.clone())).await.unwrap();
        assert_eq!(
            body["assets"][0]["partitions"],
            json!({"total": 3, "materialized": 1, "stale": 0, "missing": 2})
        );

        // the grid's data: newest key first, with what each one holds
        let app = router(st.clone());
        let (status, body, _) =
            request(app.clone(), Method::GET, "/api/assets/daily/partitions").await;
        assert_eq!(status, StatusCode::OK);
        let body = body.unwrap();
        assert_eq!(body["total"], 3);
        let keys: Vec<&str> = body["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["key"].as_str().unwrap())
            .collect();
        assert_eq!(keys, ["k3", "k2", "k1"]);
        let built = &body["partitions"][1];
        assert_eq!(built["state"], "materialized");
        assert!(built["fingerprint"].is_string() && built["run_id"].is_string());
        assert_eq!(body["partitions"][0]["state"], "missing");

        // an unpartitioned asset has no grid to draw
        let (status, _, _) = request(
            router(asset_state()),
            Method::GET,
            "/api/assets/stats/partitions",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn the_backfill_endpoints_record_a_range_and_cancel_it() {
        let st = partitioned_state();
        let app = router(st.clone());

        let (status, Json(body)) = start_backfill(
            State(st.clone()),
            Path("daily".into()),
            Bytes::from(r#"{"from":"k1","to":"k3"}"#),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body["asset"], "daily");
        assert_eq!(body["partitions"], json!(["k1", "k2", "k3"]));
        assert_eq!(body["status"], "running");
        let id = body["id"].as_i64().unwrap();

        // a second one for the same asset waits its turn
        let (status, Json(body)) = start_backfill(
            State(st.clone()),
            Path("daily".into()),
            Bytes::from(r#"{"from":"k1","to":"k2"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("still running"));

        let (status, body, _) = request(app.clone(), Method::GET, "/api/backfills").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.unwrap()["backfills"][0]["id"], id);

        // the record with the runs it launched
        let (status, body, _) =
            request(app.clone(), Method::GET, &format!("/api/backfills/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        let body = body.unwrap();
        assert_eq!(body["backfill"]["total"], 3);
        assert_eq!(body["runs"].as_array().unwrap().len(), 1);
        assert_eq!(body["runs"][0]["job"], "assets");

        let Json(body) = cancel_backfill(State(st.clone()), Path(id)).await.unwrap();
        assert_eq!(body, json!({"canceled": true}));
        // cancelling twice says what happened rather than lying
        let (status, _) = cancel_backfill(State(st.clone()), Path(id))
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);

        let (status, _) = start_backfill(
            State(st.clone()),
            Path("daily".into()),
            Bytes::from(r#"{"from":"k1","to":"nope"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let (status, _) = start_backfill(
            State(st.clone()),
            Path("ghost".into()),
            Bytes::from(r#"{"from":"k1","to":"k2"}"#),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);

        let (status, _, _) = request(app, Method::GET, "/api/backfills/999").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn build_endpoints_409_while_build_active() {
        let st = asset_state();
        st.runner
            .store()
            .record_materialization("docs", None, "d1", &json!({}), None, None, None)
            .unwrap();
        // an assets run planted as live, without an executor behind it
        insert_run(&st, "b1", "assets", RunStatus::Running, json!({}));
        let (status, Json(body)) =
            build_one_asset(State(st.clone()), Path("totals".into()), Bytes::new())
                .await
                .unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "asset build already running");
        let (status, Json(body)) = build_all_assets(State(st.clone())).await.unwrap_err();
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(body["error"], "asset build already running");
        assert_eq!(
            st.runner
                .store()
                .runs(None, None, None, None, None, 10)
                .unwrap()
                .len(),
            1
        );
        // the more specific answer wins: a source is a 400 even while a build is live
        let (status, _) = build_one_asset(State(st.clone()), Path("docs".into()), Bytes::new())
            .await
            .unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);

        st.runner
            .store()
            .run_finished("b1", RunStatus::Success, None)
            .unwrap();
        let (status, _) = build_one_asset(State(st.clone()), Path("totals".into()), Bytes::new())
            .await
            .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn sensors_endpoints_list_pause_and_ticks() {
        let st = asset_state();
        let st = AppState {
            sensors: Arc::new(vec![
                SensorInfo {
                    name: "watch".into(),
                    every: std::time::Duration::from_secs(30),
                    filter: None,
                    state: SensorState::new(),
                },
                SensorInfo {
                    name: "probe:docs".into(),
                    every: std::time::Duration::from_secs(60),
                    filter: None,
                    state: SensorState::new(),
                },
                SensorInfo {
                    name: "run:chain".into(),
                    every: std::time::Duration::from_secs(15),
                    filter: Some(json!({"job": "etl", "statuses": ["success"]})),
                    state: SensorState::new(),
                },
            ]),
            ..st
        };
        let store = st.runner.store();
        store
            .sync_sensors(&["watch".into(), "probe:docs".into(), "run:chain".into()])
            .unwrap();

        let Json(body) = list_sensors(State(st.clone())).await.unwrap();
        let sensors = body["sensors"].as_array().unwrap();
        assert_eq!(sensors.len(), 3);
        assert_eq!(sensors[0]["name"], "watch");
        assert_eq!(sensors[0]["every_secs"], 30);
        assert_eq!(sensors[0]["paused"], json!(false));
        assert_eq!(sensors[0]["cursor"], json!(null));
        assert_eq!(sensors[0]["last_tick"], json!(null));
        assert_eq!(sensors[0]["filter"], json!(null));
        // nothing has failed, so the next evaluation is the plain interval away
        assert_eq!(sensors[0]["consecutive_failures"], 0);
        assert!(sensors[0]["next_eval"].as_str().is_some());
        assert_eq!(sensors[1]["name"], "probe:docs");
        // a run sensor is a row like any other, with what it watches shown
        assert_eq!(sensors[2]["name"], "run:chain");
        assert_eq!(sensors[2]["every_secs"], 15);
        assert_eq!(
            sensors[2]["filter"],
            json!({"job": "etl", "statuses": ["success"]})
        );

        let Json(ok) = set_sensor_state(
            State(st.clone()),
            Json(SensorStateBody {
                name: "watch".into(),
                paused: true,
            }),
        )
        .await
        .unwrap();
        assert_eq!(ok, json!({"ok": true}));
        let (status, Json(body)) = set_sensor_state(
            State(st.clone()),
            Json(SensorStateBody {
                name: "nope".into(),
                paused: true,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"], "unknown sensor: nope");

        store
            .set_sensor_cursor("watch", &json!({"mtime": 5}))
            .unwrap();
        store
            .record_sensor_tick("watch", crate::SensorOutcome::Fired, 2, 1, 12, None)
            .unwrap();
        store
            .record_sensor_tick(
                "probe:docs",
                crate::SensorOutcome::Error,
                0,
                0,
                3,
                Some("no such dir"),
            )
            .unwrap();
        let Json(body) = list_sensors(State(st.clone())).await.unwrap();
        let sensors = body["sensors"].as_array().unwrap();
        assert_eq!(sensors[0]["paused"], json!(true));
        assert_eq!(sensors[0]["cursor"], json!({"mtime": 5}));
        assert_eq!(sensors[0]["last_tick"]["outcome"], "fired");
        assert_eq!(sensors[0]["last_tick"]["launched"], 2);
        assert_eq!(sensors[1]["last_tick"]["outcome"], "error");

        let q = |sensor: Option<&str>, limit: Option<u32>| {
            Ok(Query(SensorTicksQuery {
                sensor: sensor.map(String::from),
                limit,
            }))
        };
        let Json(body) = sensor_ticks(State(st.clone()), q(Some("watch"), None))
            .await
            .unwrap();
        let ticks = body["ticks"].as_array().unwrap();
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0]["sensor"], "watch");
        let Json(body) = sensor_ticks(State(st.clone()), q(None, Some(0)))
            .await
            .unwrap();
        assert_eq!(body["ticks"].as_array().unwrap().len(), 1);
        let Json(body) = sensor_ticks(State(st), q(None, None)).await.unwrap();
        assert_eq!(body["ticks"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn bad_query_params_are_json_400() {
        let st = state(vec![echo_job("etl")]);
        for path in [
            "/api/runs?limit=abc",
            "/api/runs/r1/events?after=abc",
            "/api/jobs/etl/op_stats?runs=abc",
            "/api/schedules/ticks?limit=abc",
            "/api/schedules/upcoming?window=abc",
            "/api/sensors/ticks?limit=abc",
        ] {
            let (status, body, _) = request(router(st.clone()), Method::GET, path).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            let body = body.unwrap_or_else(|| panic!("{path}: body not json"));
            assert!(body["error"].is_string(), "{path}: {body}");
        }
    }

    #[tokio::test]
    async fn api_root_is_json_404_not_index() {
        let st = state(vec![]);
        let (status, body, _) = request(router(st), Method::GET, "/api").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body.unwrap()["error"], "no such endpoint: /api");
    }

    #[tokio::test]
    async fn non_get_fallback_is_405_not_index() {
        let st = state(vec![]);
        let (status, body, _) = request(router(st.clone()), Method::DELETE, "/runs").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(body.unwrap()["error"], "method not allowed");
        let (status, _, content_type) = request(router(st), Method::GET, "/runs").await;
        assert_eq!(status, StatusCode::OK);
        assert!(content_type.starts_with("text/html"), "{content_type}");
    }
}
