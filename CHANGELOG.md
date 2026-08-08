# changelog

## unreleased

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
