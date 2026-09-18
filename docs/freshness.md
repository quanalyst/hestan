# Freshness

`fresh_within(d)` limits the age of the latest successful job run or asset
materialization. Exceeding that age makes the item late. It does not trigger a
build; use a schedule or [automation policy](assets.md#automation-policies)
for that.

```rust
Hestan::new()
    .job(Job::builder("etl").fresh_within(Duration::from_secs(86_400)).op(pull).build()?)
    .assets([Asset::new("report", build).fresh_within(Duration::from_secs(3600))])
    .on_late(hestan::notify::slack(slack_url))
    .serve(([127, 0, 0, 1], 4000))
    .await
```

## Fresh, late, never

the verdict is computed at read time (nothing caches it) from the latest
success:

| status | when |
| --- | --- |
| `fresh` | a success inside the window |
| `late` | the window closed `late_by` ago |
| `never` | nothing has ever succeeded |

for a **job** the success is its most recent run that finished `success`,
whatever triggered it: a manual launch counts, because the data is as current
either way. for an **asset** it is the most recent materialization, which is
what a build records.

`never` is deliberately not late. a policy caps how old a success may get, and
something with no success has no age to measure. reporting "infinitely late"
would be a number nobody can act on. a job that has never run and should have
is exactly what the [cron-derived `overdue` heuristic](scheduling.md#overdue-and-interval_secs)
already covers.

### Partitioned assets

on a [partitioned asset](assets.md#partitioned-assets) the policy applies per
key: the asset is late as soon as **any one key** is, and `late_by` is the
worst key's. the deadline is therefore measured from the *oldest* key's build
time.

keys that have never been built are skipped rather than counted late: a key
with no build has no age either, and the `missing` count on the asset summary
already says so. an asset with no key built at all is `never`.

## Which wins: policy or overdue

A declared freshness policy replaces the cron-derived overdue heuristic.
The API retains both fields, but `overdue` is false when `freshness` is present.
Without a policy, scheduled jobs continue to use overdue status.

Freshness measures elapsed time. [Staleness](assets.md#provable-staleness)
measures changed dependencies. An asset can be fresh but stale, or late while
its upstream fingerprints remain unchanged.

## Alerting on it

`on_late` registers a hook for a transition into late status:

```rust
Hestan::new()
    .on_late(|e: LateEvent| eprintln!("{} {} is {:?} late", e.kind.as_str(), e.name, e.late_by))
```

| field | what it holds |
| --- | --- |
| `kind` | `job` or `asset` |
| `name` | the job or asset name |
| `owner` | who to wake, from `JobBuilder::owner` or `Asset::owner`; `None` for one nobody claimed |
| `late_by` | how far past the deadline, at the crossing |
| `last_success` | the success the deadline was measured from |

`owner` is what makes this an alert somebody can act on rather than a line
naming an asset: it is filled in from the declaration when the crossing is
recorded, so a hook reads it off the event. it is also **the only path an
asset's owner reaches a hook by**, since an asset build runs under the internal
`assets` job and a run event carries that job's owner. see
[namespaces and owners](namespaces.md#it-reaches-the-alert).

it fires **once per crossing**, not once per poll: a job late for a week pages
once, not every minute. the last-notified state lives in the database
(`freshness_state`), so a restart does not re-announce a crossing it already
announced. going fresh again is not an alert: it clears the row, so the next
relapse is news again.

hooks are dispatched exactly like [`on_failure`](notifications.md): each on
tokio's blocking pool, so a hook may block outright, and a panicking one is
caught and logged without touching the others. the `notify::webhook` and
`notify::slack` helpers work here unchanged.

## The checker

`serve` runs a checker task next to the scheduler, the sensor loop and the
backfill chunker. it evaluates every declared policy every **60 seconds** and
hands each crossing to the hooks. a process where nothing declares a policy
never starts it.

`run_once` does not run the checker: it is one run, not a live process, and
there is nobody to notify.

60s is deliberate. a policy is a claim about hours or days, so a minute of lag
on noticing one broke is noise, and polling harder would only cost database
reads.

## Where it surfaces

- `GET /api/jobs` and `GET /api/assets`: `freshness: {status, late_by_secs,
  last_success}`, `null` when nothing was declared.
- `GET /api/late`: everything currently late, in the same shape `on_late`
  hands its hooks (jobs first, then assets, each by name), each with the
  [`owner`](namespaces.md#an-owner) of whatever went late and `null` where
  nobody claimed it.
- the ui tags late jobs and late assets with `late` (beside `overdue`, which
  is a different claim), and the jobs overview statline counts them.
