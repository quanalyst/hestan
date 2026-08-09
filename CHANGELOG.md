# changelog

## unreleased

- freshness policies: `Asset::new(..).fresh_within(Duration::from_secs(3600))` and `Job::builder(..).fresh_within(..)` declare how old the latest success may get. the verdict is `Freshness::{Fresh, Late { by }, Never}`, computed at read time from the latest successful materialization (assets) or run (jobs). `never` is deliberately not late — a policy caps how old a success may get, and something with no success has no age to measure
- on a partitioned asset the policy applies **per key**: the asset is late as soon as any one key is, measured from the oldest key's build. keys that were never built are skipped rather than counted late, for the same reason `never` is not late
- a declared policy **replaces** the cron-derived `overdue` heuristic rather than sitting beside it. both fields stay on `GET /api/jobs`, and `overdue` is always false once `freshness` is non-null: two answers to "is this job behind" is one answer too many
- **the alert is the point, not the badge.** `Hestan::on_late(hook)` fires a `LateEvent {kind, name, late_by, last_success}` when something crosses from fresh to late — once per crossing, not once per poll, so a job late for a week pages once. the last-notified state lives in `freshness_state` (v10), so a restart does not re-announce a crossing, and a recovery clears the row so the next relapse is news again
- hooks go out through the same blocking-safe dispatch `on_failure` uses, and `notify::webhook` / `notify::slack` now serve either event — the call sites are unchanged, and which event a helper is built for is inferred from the hook it is handed to
- a checker task runs beside the scheduler on a 60s tick, started only when something declares a policy. `serve` runs it; `run_once` does not
- `GET /api/late` lists everything currently late in exactly the shape `on_late` hands its hooks; `freshness: {status, late_by_secs, last_success}` lands on job and asset summaries. the ui tags late jobs and assets and counts them on the overview statline
- new page: [docs/freshness.md](docs/freshness.md) — policies, the three states, `on_late`, and how a policy relates to `overdue` and to staleness
- schema v10: the `freshness_state` table, plus `schedules.cursor`, `schedules.catchup` and `runs.scheduled_for` for the parts that follow

- backfills: `POST /api/assets/{name}/backfill {"from", "to", "only_missing"}` records a request to materialize a range of one asset's partitions and launches it. the range resolves against the key set at the moment it is made and is then fixed — a daily set grows, and a backfill should build what it was asked for. `only_missing` (default true) drops the keys that are already fresh
- it launches **in chunks of `Partitions::build_limit`**, one run at a time: the first goes out immediately and each next one starts as the previous finishes, so a 400-day range is not 400 instances at somebody's api at once. each chunk is an ordinary build run, so the run page, gantt, cancel and events all apply
- the `backfills` table (v9) records `asset, from_key, to_key, partitions, run_ids, total, launched, created_at, finished_at, status`. status derives from the runs: `running` between chunks, `complete` when the last succeeded, `failed` when one failed (chunking stops), `canceled` when one was canceled or the backfill was. a range that resolves to nothing is `complete` on arrival rather than a 400
- `GET /api/backfills?limit=`, `GET /api/backfills/{id}` (with its runs), `POST /api/backfills/{id}/cancel`. one backfill per asset at a time — a second is a 409 — and no cross-asset backfills. a chunk also waits on the existing one-build-at-a-time gate rather than overlapping an active assets run
- the assets page gains a backfills section: asset, range, launched/total, status glyph, a link to the chunk running now and a cancel action while it is

- partitioned assets: `Asset::partitioned(Partitions::daily("2026-01-01"))` materializes an asset once per key instead of once — `Partitions::daily`, `::hourly` and `::keys([..])`, with `ctx.partition()` handing the body its key. materializations, fingerprints, history, checks and metadata all key on `(asset, partition)`
- **a build is a fan-out.** the lowered `assets` job gains an external `partitions:{asset}`, the asset's op expands over it, and one instance per target key runs as `{asset}[{key}]` through the machinery mapped ops already use. no second expansion path, so `max_parallel`, pools, retries, cancellation and per-instance rows come along untouched. `Op` instances are now named by their element where an op asks for it, rather than always by index
- dependencies between partitioned assets are **identity mapping only**: the same key, read from the store at `(dep, key)` so it means one thing whether the upstream partition was rebuilt this run or was already fresh. partitioned on unpartitioned is fine; **unpartitioned on partitioned is refused at build** — that needs an aggregation this phase does not define — and two partitioned assets in a dep relationship must use the same kind of key set
- a build with no keys named targets the missing or stale ones, newest first, capped by `Partitions::build_limit` (default 31), so an unbounded daily range cannot start a thousand instances by accident. `POST /api/assets/{name}/build` takes an optional `{"partitions": [..]}` to name them outright; an unknown key is a 400
- a probe fingerprint change marks **all** partitions of a descendant stale. that is not a special rule — an unpartitioned dep is read whole, so every key's recorded input disagrees at once. crude but honest, and documented as the current one
- `GET /api/assets` reports a partitioned asset as `partitions: {total, materialized, stale, missing}` instead of a fingerprint; `GET /api/assets/{name}/partitions` is one row per key for the grid, and `history`/`checks` take `partition=`
- the asset detail panel gains the **partition grid**: one cell per key, newest first, solid materialized / hatched stale / hollow missing, hovering for key, fingerprint and build time, clicking to build that key

- multi-assets: `MultiAsset::new("split_orders", f).produces(["orders_clean", "orders_rejected"])`, registered with `Hestan::multi_assets`, is one op that materializes several assets — the query or pull whose result splits into two tables you do not want to fetch twice. the body returns a json object whose keys are exactly the produced names; a key it did not return, or one nothing declared, fails the op naming the discrepancy
- the asset registry is now asset -> op **N:1**. staleness stays per asset and the op is stale when any asset it produces is; a plan holds the op once however many of its outputs are stale, so building either output is one run of one computation and materializes both
- each produced asset gets its own materialization row, fingerprint (the content hash of that key's value) and history. `ctx.set_fingerprint_of(asset, fp)` and `ctx.meta_of(asset, name, value)` override per output; plain `ctx.meta` describes the computation and lands on the op run, and `ctx.set_fingerprint` covers every output that staged none of its own
- downstream assets depend on the produced *name* — `Asset::from_named("orders_clean")`, read as `ctx.input("orders_clean")` — so an asset moving into or out of a multi-asset does not change anything reading it. a memoized build seeds the whole object the op returns, so whichever key a consumer reads is there
- `GET /api/assets` gains `op`, the op that materializes each asset, and the detail panel shows it when it is not simply the asset's own name
- schema v9: `asset_materializations.partition` and `asset_checks.partition` (null = unpartitioned, which is every existing row), latest lookups and history re-keyed per `(asset, partition)`, and the `backfills` table. `Store::materialization`, `materializations`, `asset_checks` and `record_materialization` take the key; passing `None` is exactly today's behaviour

- materialization history: `asset_materializations` is append-only (schema v8), so every build leaves an entry instead of overwriting the last one. an asset's newest entry is its current state — staleness, memoized seeding and `GET /api/assets` all read exactly what they read before, and the existing suite is the proof. every row an older database holds carries across as that asset's first entry
- `GET /api/assets/{name}/history?limit=` (default 20, clamped 1..=200) returns those entries newest first, each with `changed`: true when its fingerprint differs from the entry before it in time. that flag is the point — a rebuild and a change are different facts, and the keyed table could not tell them apart. the oldest entry counts as changed, and a page's oldest entry is compared against the entry just off the page rather than reported as a change the window invented
- `Store::materializations(asset, limit)` is the history read; the old no-argument `Store::materializations()` is now `latest_materializations()`, which is what it always returned
- history is capped rather than left to grow: at startup each asset is trimmed to its newest 200 entries, `Hestan::asset_history(n)` sets the number, and the newest entry is never trimmed at any `n`. run retention still never touches materializations — a latest value outlives the run that built it, like op state
- clicking an asset row (or its dag node) opens a detail panel listing recent materializations: relative time, short fingerprint, a mark on the ones that changed, and a link to the run

- metadata: `ctx.meta("rows", 1_234)` attaches typed facts to what an op produced — `Meta::{Int, Float, Text, Url, Markdown, Json}`, with the obvious rust types converting on their own. `u64` and `usize` deliberately do not convert; narrowing them silently is a lie waiting to happen
- staged per attempt like `set_state`, so a failed attempt's metadata is discarded whole and what lands is what the attempt that worked reported. committed in the op's terminal write, so an op run never carries facts about work that did not finish
- stored as one json object per op run in `op_runs.metadata`, keyed by name with a tagged value (`{"rows": {"int": 1234}}`); an op that reported nothing stores null rather than `{}`. an asset op's map is written to its materialization as well, so history carries what each build reported and keeps it after retention deletes the run
- `Meta::Markdown` is stored and shown as source. there is no markdown parser in this crate and this does not add one — the variant says which strings are worth rendering elsewhere
- surfaced on `GET /api/runs/{id}` op rows and on `GET /api/assets/{name}/history` entries. the ui renders by type rather than as raw json: numbers right-aligned and tabular, urls as links, text inline, markdown and json in a muted preformatted block
- new page: [docs/metadata.md](docs/metadata.md) — metadata is an op feature that assets carry, not an asset feature, so it reads next to op state rather than inside assets.md

- asset checks: `AssetCheck::new(name, asset, |ctx, value| ..)` registered with `Hestan::check(..)` asserts something about the value an asset just materialized, returning `CheckResult::pass()` or `CheckResult::fail(msg)` with `.meta(..)` facts attached. the value arrives owned rather than borrowed — a closure returning `async move` cannot tie its future's lifetime to a `&Value` argument
- checks lower into ops of the existing internal `assets` job, named `check:{asset}:{check}` and depending on the asset's own op. that is the whole implementation: no parallel execution path, so retries, cancellation, the gantt, the event log and `max_parallel` apply because a check *is* an op
- `Severity::Error` (the default) fails the check's op and so the run; `Severity::Warn` records the failed result and lets the op — and the run — succeed. either way the result is recorded before the verdict is acted on, so a failing error check leaves its message and metadata behind
- a failing error check does **not** un-materialize the asset. the materialization was written inside the asset's op, which succeeded, and checks hang off that op rather than feeding it, so downstream assets still see the value. what it does is fail the run that produced it
- **a memoized asset is not re-checked**: a check is in a build plan exactly when the asset it checks is, which follows from checks being ops in the plan. an asset that was seeded rather than rebuilt produced no new value, and its last result still describes the value that is still current. the consequence is that a check added after an asset last built waits for that asset's next build; `POST /api/assets/{name}/build` always rebuilds its target
- naming an unknown asset, naming a source (a check runs on what a build produced), and two checks with one name on one asset are all build errors
- results land in the v8 `asset_checks` table, capped per check by the same `Hestan::asset_history(n)`. `GET /api/assets/{name}/checks?limit=` lists them newest first, and each asset in `GET /api/assets` gains `"checks": {"passed", "failed", "last_run_at"}` from the latest result per name
- the assets table gains a checks cell in the established shape vocabulary — solid glyph all passed, × any failed, nothing at all when no check has recorded anything — and the asset detail panel lists each check's latest status, severity, message and metadata

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
