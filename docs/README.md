# hestan docs

- [getting started](getting-started.md) — add the dependency, write a first job, see it in the ui.
- [concepts](concepts.md) — ops, jobs, runs, events, triggers, and exactly how a run executes.
- [typed io](typed-io.md) — `Op::typed`, `input_as`, `.params::<P>()`, and what a type-check failure does.
- [op state](state.md) — persisted watermarks: `ctx.state`/`set_state`, the at-least-once commit order, the state endpoint.
- [assets](assets.md) — fingerprints, provable staleness, memoized builds, serialized builds, probes, and `.auto()`.
- [sensors](sensors.md) — the sensor loop, cursor commit-on-success, `RunRequest`, probes-as-sensors, pausing and tick history.
- [scheduling](scheduling.md) — cron syntax, timezones, pause/resume, ticks, and the scheduler loop.
- [http sources](http-sources.md) — declarative REST pulls: the full `HttpSource` builder, fan-out, retry policy.
- [notifications](notifications.md) — failure hooks: `on_failure`, `RunFailure`, the webhook and slack helpers.
- [web ui](web-ui.md) — page-by-page tour of the embedded ui and how it draws status.
- [http api](http-api.md) — every endpoint, parameter, response shape, and error code.
- [storage](storage.md) — the sqlite schema, migrations, and crash recovery.
- [embedding](embedding.md) — `serve` vs `run_once` vs `Runner`, testing, consuming from another repo.
- [development](development.md) — repo layout, quality gates, the ui build loop, adding a migration.
