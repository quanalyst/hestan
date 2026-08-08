# changelog

## unreleased

- `OpCtx::is_cancelled()` and `OpCtx::cancelled()`: blocking work can poll (or an async op can `select!`) and stop on request, which is the only way blocking work ever stops — tokio cannot abort it
- honest cancellation: after aborting, a canceled run gives its ops three seconds to actually come back. ops that do are recorded as what really happened; ops that don't are `canceled` with `not observed to stop` and **no** `finished_at`, instead of a finish time for work that is still running
- named concurrency pools shared process-wide: `Hestan::pool(name, limit)` declares one, `Op::pool(name)` takes a permit for the length of an attempt. that is the limit an external api actually imposes, which per-job `max_parallel` cannot express once two jobs overlap. an undeclared pool is a build-time `Error::Graph`; `Runner::with_pools` is the direct-executor form
- `Op::timeout(d)`: a hung attempt fails with `timed out after 30s` and retries normally, instead of running forever, holding its slot, and blocking `Overlap::Skip`. expiry trips the same signal `is_cancelled()` reads
- retries now back off exponentially with full jitter by default (`1s * 2^n`, capped at 30s), so ops that fail together stop retrying in lockstep and re-tripping the same rate limit. `.retry_backoff(base, max)` tunes it; `.retry_delay(d)` still means a fixed pause. http sources jitter their backoff too, and still honor `Retry-After` exactly
- a failed run carries its own `error` — `op {name} failed: {message}` from the first terminal failure, the same pair `on_failure` receives — instead of null. `GET /api/runs` and `/api/runs/{id}` return it, and the run page shows it
- job summaries report each op's `pool` and `timeout_secs`, plus the job's `pools` and their limits; the op inspector shows them
- schema v6: `runs.error`

- resume a finished run instead of redoing it: every op that did not succeed runs again with its downstream, every op that did is seeded from its recorded output
- `Runner::resume`, `Runner::resume_from` (re-run from a chosen op and its downstream, on any terminal run), and `Runner::resume_plan` for the same answer without launching
- `POST /api/runs/{id}/resume` (optional `{"from": [...]}`) and `GET /api/runs/{id}/resume_preview`; resumed runs carry the `resume` trigger and a `resumed_from` link, and the ui offers resume beside re-run plus "re-run from here" on a dag node
- resuming a resume walks the `resumed_from` chain for outputs; a resume is refused when the job's ops no longer match what the chain recorded
- schema v5: `runs.resumed_from`

## 0.1.0-alpha.1

first public release. the api will change; pin an exact version.

- ops wired into job dags, with serde-typed io and params validated before a run is created
- cron schedules in iana timezones, with pause/resume, tick history, and per-job overlap policy
- assets: content fingerprints make staleness provable, and builds materialize only the stale subgraph
- sensors with persisted cursors; asset probes run on the same loop
- run cancellation, per-op watermark state, and failure hooks (webhook and slack helpers behind the `http` feature)
- http sources: one builder lowers a rest endpoint into fan-out ops with http-aware retries
- sqlite run log with versioned migrations, crash recovery on boot, and optional retention
- embedded web ui: runs timeline, per-op gantt, asset graph, command palette
