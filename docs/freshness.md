# Freshness

a freshness policy is a claim you make about how current something should be,
and hestan checks it for you:

```rust
Hestan::new()
    .job(Job::builder("etl").fresh_within(Duration::from_secs(86_400)).op(pull).build()?)
    .assets([Asset::new("report", build).fresh_within(Duration::from_secs(3600))])
    .on_late(hestan::notify::slack(slack_url))
    .serve(([127, 0, 0, 1], 4000))
    .await
```

`fresh_within(d)` says: the latest success may be up to `d` old. past that,
this is **late**, and that is worth waking someone for.

## Fresh, late, never

the verdict is computed at read time — nothing caches it — from the latest
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
something with no success has no age to measure — reporting "infinitely late"
would be a number nobody can act on. a job that has never run and should have
is exactly what the [cron-derived `overdue` heuristic](scheduling.md#overdue-and-interval_secs)
already covers.

### Partitioned assets

on a [partitioned asset](assets.md#partitioned-assets) the policy applies per
key: the asset is late as soon as **any one key** is, and `late_by` is the
worst key's. the deadline is therefore measured from the *oldest* key's build
time.

keys that have never been built are skipped rather than counted late — a key
with no build has no age either, and the `missing` count on the asset summary
already says so. an asset with no key built at all is `never`.

## Which wins: policy or overdue

`overdue` is a heuristic: it guesses from the cron expression that a job which
hasn't succeeded since its last fire is behind. `fresh_within` states the same
thing outright, in the units you actually care about.

so a declared policy **replaces** the heuristic. `GET /api/jobs` keeps both
fields, and once `freshness` is non-null, `overdue` is always `false` — two
answers to "is this job behind" is one answer too many. jobs that declare no
policy keep the heuristic exactly as it was, and it needs a schedule to say
anything at all.

they also measure different things on purpose. `overdue` anchors on the
schedule ("it was due at 09:00 and nothing has succeeded since"); a policy
anchors on age ("nothing has succeeded for 24 hours"), which is meaningful for
an asset built by a sensor, a probe or a hand, with no cron anywhere.

freshness is also not [staleness](assets.md#staleness). stale means a dep
moved; late means time passed. an asset can be fresh and stale (a dep changed
a minute ago), or late and not stale (nothing upstream moved, and nothing
rebuilt it either).

## Alerting on it

**the alert is the point; the badge is a side effect.** `on_late` registers a
hook that fires when something crosses from fresh to late:

```rust
Hestan::new()
    .on_late(|e: LateEvent| eprintln!("{} {} is {:?} late", e.kind.as_str(), e.name, e.late_by))
```

| field | what it holds |
| --- | --- |
| `kind` | `job` or `asset` |
| `name` | the job or asset name |
| `late_by` | how far past the deadline, at the crossing |
| `last_success` | the success the deadline was measured from |

it fires **once per crossing**, not once per poll: a job late for a week pages
once, not every minute. the last-notified state lives in the database
(`freshness_state`), so a restart does not re-announce a crossing it already
announced. going fresh again is not an alert — it clears the row, so the next
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
  hands its hooks — jobs first, then assets, each by name.
- the ui tags late jobs and late assets with `late` (beside `overdue`, which
  is a different claim), and the jobs overview statline counts them.
