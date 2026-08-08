# changelog

## unreleased

- dynamic fan-out: `Op::mapped(name, f).over(dep)` runs one instance per element of a dep's json array, discovered at run time. each instance is named `{op}[{i}]`, gets its own `op_runs` row and its element as a typed second argument, and is an ordinary spawned task — `max_parallel`, pools, retries, timeouts and cancellation apply with no special cases
- a mapped op's output, seen downstream under its plain name, is the array of instance outputs **in element order**, not completion order, and exists only if every instance succeeded: one failure fails the mapped op, skips its downstream and fails the run naming the instance. an empty array is legal — no instances, output `[]`, downstream runs normally
- the mapped op itself gets no `op_runs` row; the instances are the record, and an `op_expanded` event carries the count. resume reuses a mapped op only when it fully succeeded, and otherwise re-expands it whole, since the array can differ on a re-run
- fan-out does not nest, and the build says so: mapping over a mapped op, a mapped op without `.over`, and `.over` on an op that isn't mapped are all `Error::Graph`
- job summaries report each op's `mapped_over`; the dag badges a mapped node with its instance count (`process ×3`), the gantt lists instances as their own rows, and selecting the node lists them with per-instance status

- params on schedules: `Hestan::schedule_with(job, expr, params)` and `schedule_tz_with(job, expr, tz, params)` give a cron entry the params every fire launches with, closing the hole where a job whose ops declare `.params::<P>()` could never fire from cron (scheduled fires used to launch with `{}`, always). `schedule`/`schedule_tz` keep their signatures and mean `{}`
- schedule params are validated **at build**: `serve`/`run_once` run each schedule's params through the same op validators a launch runs, so an impossible schedule is `Error::InvalidParams` at startup instead of a tick that fails every night at 3am. `Job::params_error` is that check store-free, and `POST /api/jobs/{name}/validate_params` exposes it — the ui's params editor calls it on blur and shows the server's message inline
- `/api/schedules` rows and the job summary's `schedules` carry `params`; the job page shows a schedule's params beside its expression. a deferred (queue-policy) fire keeps the params it was held with
- schema v7: `schedules.params`

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
