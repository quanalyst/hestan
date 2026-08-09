# changelog

## unreleased

- pluggable io managers: `IoManager::put` persists an op's output and returns the handle recorded in `op_runs.output`, `get` turns a handle back into the value. the default `Inline` makes an output its own handle, so it is byte-for-byte what hestan has always done; the whole existing suite is the proof
- bundled `FileIo::new(dir)` writes `{dir}/{run_id}/{op}.json` and records `{"$io": "file", "path": ".."}` — an object rather than a bare path so anything reading the run log can tell a reference from a value. nothing is ever cleaned up: retention prunes run rows, not files
- `Hestan::io(manager)` sets the default and `Hestan::io_named(name, manager)` plus `Op::io(name)` select one per op; naming an unregistered manager is a build error rather than a quiet fall back to the run log. `Runner::with_io` is the direct-executor form
- handles are resolved on every path an op reads: downstream inputs as each op is spawned, the array a mapped op expands over, a fan-out's collected instance outputs, resume seeds from an earlier run, and an asset build's memoized seeds. an input that cannot be fetched fails that op rather than reading as "produced nothing"
- `put` runs **before** the success is recorded, so a failed put fails the op: a row claiming success for a value that was never stored would strand the next resume
- `get` is required to be total — it returns anything it did not produce unchanged — because a run seeds source assets `null`, assembles fan-out arrays itself, and can mix managers op by op
- the run page shows the selected op's output on one line, an `$io` handle as the reference it is; job summaries report each op's `io`
- new page: [docs/io-managers.md](docs/io-managers.md)

- resources: `Hestan::resource(name, |ctx| async { .. })` builds a value once at startup and shares it with every op that asks, replacing "capture a client in a closure". `ctx.resource::<T>(name)` hands back the same `Arc<T>` everywhere, and the error distinguishes "no such resource" from "there is one, and it is something else"
- constructors are async and fallible and run **before the store opens**, so one that fails aborts startup with `Error::Resource { name, reason }` and leaves no database behind. they run in declaration order, each handed a `ResourceCtx` holding the ones before it, so a client can lean on the config it reads; declaring one name twice is an error
- `Op::requires(["api"])` declares the dependency, making a resource nobody registered a build error rather than a run that gets halfway. ops may also just ask without declaring
- resources live for the process: no per-run scoping and no teardown hooks in this phase
- `GET /api/resources` reports names and declared types, never values; job summaries carry each op's `requires` and the op inspector shows it
- new page: [docs/resources.md](docs/resources.md)

- reusable graphs: `Graph::builder(name).op(..).input(..).output(..).build()` bundles ops into a unit a job can instantiate more than once with `JobBuilder::graph("clean_a", &clean).after([..])`. purely a build-time transformation — `JobBuilder::build` flattens each instance into ordinary ops named `{instance}.{inner}`, so the executor, resume, fan-out, assets and the ui are untouched
- declared `input` ops additionally wait on the instance's own deps (the only way into a graph — an inner dep naming something outside is a build error), and anything depending on the instance name is rewired to the op it declared as its `output`. duplicate instance names, an instance colliding with an op, and an unknown or dot-containing `input`/`output` are all `Error::Graph`
- a graph may contain a graph, and `input`/`output` may name a nested instance, which resolves through it; names compound (`s.inner.pages`). self-inclusion is refused rather than flattened forever
- ops keep their own vocabulary through the rename: `dedupe` inside `clean` still reads `ctx.input("parse")`, and a job-level op reads `ctx.input("clean_a")` — the name it wrote in `.after` — rather than the inner op that supplied it
- `OpCtx::inputs()` lists every dep that produced output, name and value, sorted: a reusable graph's input op cannot know what the job called the dep it was handed
- the dag mutes an op's `{instance}.` prefix so a graph instance's ops read as a group

- trigger rules: `Op::when(When::Always | When::AnyFailed | When::AllSucceeded)` decides whether an op runs once its deps settle, so a summary, an alert or a cleanup after a failure is expressible at last — the thing you most want after a failure used to be exactly what got skipped. `AllSucceeded` is the default and is what every op has always meant
- readiness moved from "every dep produced output" to "every dep reached a terminal status"; the rule then decides run vs skip. an op a rule turns down is `skipped` with an `op_skipped` event naming the rule (`skipped by rule any_failed: every dep succeeded`, `data: {"when": ...}`), worded apart from the upstream-failure skip so the log says which happened
- `OpCtx::dep_status(dep)` reports what each declared dep did; `ctx.input(dep)` for a dep that produced nothing stays `None`. deps seeded from outside the run — a resume's reused output, a memoized asset value, a source asset — read as `success`
- skip propagation asks each candidate's rule instead of blanket-skipping: the walk stops at an op that would still run, and at whatever hangs off it, which waits on that op instead. everything reached through plain `all_succeeded` ops is still skipped as one group naming the original root
- a rule applies to a mapped op whole. one admitted when its array never arrived expands into zero instances — no bodies, no rows, `op_expanded` with `instances: 0`, and `[]` downstream
- the run outcome is unchanged: any op failure still fails the run, however many cleanup ops succeed afterwards. there is no "recovered" state
- job summaries report each op's `when`; the dag marks such nodes with a muted `always` / `if failed`, and the op inspector spells the rule out

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
