# Changes

## Presentation metadata

- Jobs, assets and individual multi-asset outputs support optional display names,
  explicit subgroups and generic labels without changing persistent identifiers.
- Jobs and Assets have collapsible hierarchies, separate execution and freshness
  summaries, searchable names, and URL-backed grouping and filter selections.
- API summaries add `display_name`, `subgroup`, `labels` and `execution`.
  Existing fields, origins, permissions and stored history are preserved.

See [presentation metadata](docs/presentation.md) for declarations and defaults.

## Display fixes

- `hestan doctor` checks collisions in the UI’s six shades.
- Timeline group bands have a visible minimum opacity.
- Asset views use the `shade` URL parameter and still accept existing `colour` links.

## Compatibility notes

For callers updating older integrations:

- Independent asset builds can overlap. Use `Hestan::max_concurrent_builds(1)`
  when builds must execute serially.
- The `asset:` job-name prefix is reserved; rename user jobs that use it.
- Runs containing secret parameters cannot be replayed, resumed or retried from
  stored values. Launch again with credentials or use a resource; see [secrets](docs/secrets.md).
- `Runner::new` and `Runner::with_failure_hooks` return `Result<Runner, Error>`.
- Custom `IoManager` implementations must implement `drop_run` to clean up
  retained outputs; see [IO managers](docs/io-managers.md).

| what moved | what to write instead |
| --- | --- |
| eleven enums are `#[non_exhaustive]`: `Error`, `Meta`, `InputError`, `Auth`, `Trigger`, `SubjectKind`, `EventKind`, `TickOutcome`, `When`, `Reclaim`, `Blocked` | a `_` arm on a `match` over one. no variant was added, renamed or removed, and `matches!`, `if let` and naming a variant to construct it are all untouched |
| `Meta` gained `Series` and `Saved`; `EventKind` gained `RunReleased` | the same `_` arm, which is what those two being open is for from here on |
| `Identity` gained a public `scope` field | `Identity::new(name, access)`, or `viewer`, `operator`, `admin`, which is what the docs have always shown |
| `RunEvent`, `RunFailure` and `LateEvent` gained an `owner` field | nothing, unless you built one: a hook is handed a payload rather than building one |
| `Run` gained a `build` field | the same. `Store::runs` hands them back |
| `Store::runs` gained a `build` parameter, before `limit` | `None`, which is what the call meant before |
| `Scope::may_touch_job` and `Scope::may_touch_asset` take the namespace as a second argument | `None`, likewise |


Store migrations run on open and do not support downgrades. Restore a backup
when returning to a binary that cannot read the migrated schema; see
[backup and recovery](docs/backup.md).

The [stability policy](docs/stability.md) describes API compatibility.
Detailed development history remains in Git.
