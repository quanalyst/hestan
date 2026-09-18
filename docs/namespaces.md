# Namespaces and owners

Namespaces define the sets that write scopes can target. Owners supply contact
metadata to [notification hooks](notifications.md) and the UI. Both are explicit
declarations, independent of names and presentation groups.

## The rule, in one sentence

Groups and subgroups organize presentation. Namespaces constrain registrations
and API mutations through scopes. Neither is derived from the other. These
rules apply equally to jobs and assets.

## A namespace

one flat name, declared on the thing it is about:

```rust
Job::builder("orders_etl").namespace("finance")
Asset::source("orders").namespace("finance")
MultiAsset::new("split", body).produces(["a", "b"]).namespace("finance")
Sensor::new("new_files", every, body).namespace("finance")
RunStatusSensor::new("chain", body).namespace("finance")
```

**a schedule declares nothing: its namespace is its job's.** a schedule is a
firing rule for exactly one job, named in its constructor, so there is nothing
for it to decide and no way for the two to disagree. a sensor is the other
shape and does declare one: it names no job until it fires, and may name
several. a probe sensor declares nothing either, for the same reason a schedule
does not: it belongs to the asset it probes and is in whatever namespace that
asset is.

so all four kinds are in a namespace, and two of the four are told rather than
asked.

### Declared, not parsed out of the name

Persistent names remain the keys used by runs, materializations, lineage,
schedules and API routes. Adding or changing a namespace preserves those keys
and their history.

Namespaces must be nonblank and have no leading or trailing whitespace.
They have no fallback and are never inferred from names or groups.

## A namespace is not a group

Groups are user-defined presentation values. Origins describe source ancestry.
Namespaces are explicit values used by scope checks and namespace filters.
Each can be selected independently; groups may span namespaces.

Assets retain their legacy group fallback from the prefix before `/`. That
fallback never assigns a namespace. [Grouping views](presentation.md) do not
change namespace enforcement or origin calculations.

### A job has one too

```rust
Job::builder("weather_pull").group("weather")
```

the same label, said the same way, and the [jobs
overview](web-ui.md#jobs-overview) makes a group a row of its own on the
timeline with it, with the member jobs a disclosure away.
a group is still a label and not a boundary: nothing reads it to decide who may
touch what, and the boundary is still the namespace beside it.

**a job group and an asset group of the same name are one label and are drawn
in one colour.** `hestan::hue` is a pure function of a name, so the timeline and
the graph agree without either being told about the other, and
[`Asset::hue(n)`](assets.md#hue) pins the angle for both. that is deliberate
rather than incidental: they are the same word on one screen, and two colours
for one word is a reader's problem rather than a designer's. `hestan doctor`
counts a job's group among the labels it checks for hues too close to tell
apart, for the same reason.

**a job's group has no fallback, and that is the one place it differs from an
asset's.** an asset's falls back to the part of its name before the first `/`,
because that is how the asset graph grouped before `Asset::group` existed and
the fallback is what keeps an existing graph looking the way it looked. no
timeline has ever grouped by anything, so the same rule produces the opposite
mechanics here: a job's group is declared or it is absent, a job called
`finance/etl` is in no group, and a deployment that declares none gets the flat
timeline it always had.

both are refused at build by the one function, so a name one of them may carry
is a name the other may carry: an empty or whitespace-only group, and a group
containing `/`, which reads as nesting that is not there.

## It composes with a scope

[Scopes](auth.md#the-scopes) narrow
what a token may change, and before namespaces the only way to say "this team's
work" was to list every job in it:

```rust
Identity::operator("finance-ci").scoped_to(Scope::namespaces(["finance"]))
```

that admits every job **and** every asset declared in `finance`, including the
one somebody adds next week, without the token being edited. `Scope::jobs`,
`Scope::assets` and `Scope::namespaces` compose, and the union is the whole of
what may be touched.

**it is the same enforcement point, not a second one.** `out_of_scope` in
`server.rs` is still the only place a scope is ruled on, still called from
`guard` before any handler, and still derives the subject from the matched
route rather than from a list of endpoints. what changed is that
`Scope::may_touch_job` and `may_touch_asset` now also take the namespace the
thing is declared in, resolved from the registry in the same function. so a
mutation added tomorrow that names a job in its path lands on the namespace
rule without anybody adding it to anything, exactly as it already landed on the
job rule.

two things that follow, and are asserted:

- **a thing in no namespace is in nobody's.** a namespace-scoped token is
  refused an unnamespaced job the same way it is refused another team's. `None`
  is the absence of a namespace, not a namespace called "none".
- **the deployment itself is still nobody's.** `POST /api/assets/build`,
  `POST /api/schedules/state` and anything else naming no job or asset in its
  path is refused for every scoped token, namespace or not.

**A scope is not a
confidentiality boundary.** reads are not narrowed by one, so a token scoped to
`finance` still reads the whole deployment. `docs/auth.md` has the reasoning.

## Filtering by one

`?namespace=finance` narrows the four list endpoints:

```
GET /api/jobs?namespace=finance
GET /api/assets?namespace=finance
GET /api/schedules?namespace=finance
GET /api/sensors?namespace=finance
```

an absent parameter is every row, which is what every request made before this
existed. an empty one is the same: nothing is ever in the namespace `""`, so a
form that posts a blank field asks for everything rather than for nothing. a
namespace nothing is declared in is an empty list rather than an unfiltered
one.

**this narrows a list and is not a permission.** it is the ui asking a smaller
question, not the api answering a narrower one, and a token that may read still
reads whatever it asks for.

the ui keeps it in the url like every other filter, so a team's view is a link:
`/jobs?namespace=finance` and `/assets?namespace=finance`. and on the command
line, `hestan assets --namespace finance` beside the `--group` filter that was
already there.

## An owner

who to wake, on a job or an asset:

```rust
Job::builder("orders_etl").owner(
    Owner::team("data-platform")
        .person("ada")
        .contact("#data-alerts")
        .escalates_to("ops@example.com"),
)

Asset::source("orders").owner(Owner::team("finance").contact("#fin-alerts"))
```

a team, a person, or both, plus how to reach them. that is the whole shape.
**hestan carries this and hands it to a hook. it is not a directory service**:
it never parses, validates, resolves or dials one of these strings, and they
mean whatever the thing on the other end of your hook makes of them.

every half is optional. an owner that named only a team is a team;
`Owner::to_string()` is `ada of data-platform (#data-alerts)`, `data-platform`,
or `ada`, and **something nobody claimed is an absence everywhere it appears**:
`null` in the api, no line on the page, `-` in the cli column. never an empty
string dressed as a name.

`Owner`'s fields are private and it has accessors, unlike the row types hestan
hands back. it is a struct callers build, so it has to be able to gain a field
without breaking every literal that exists. `docs/stability.md` says which
structs are which.

## It reaches the alert

this is the row, and without it the rest is decoration.

**a run's terminal event carries the owner of the job it was a run of.** the
executor reads it off the declaration at the one place a terminal event is
built, so nothing downstream is handed a recipient by whoever registered the
hook:

```rust
Hestan::new()
    .job(orders_etl)                       // owner declared on the job
    .on_failure(|f: RunFailure| {
        // nothing threaded this through: it is on the event
        let reach = f.owner.as_ref().and_then(Owner::contact_at).unwrap_or("#fallback");
        page(reach, f.error.as_deref().unwrap_or("no reason recorded"));
    });
```

- [`RunEvent::owner`](notifications.md) on every terminal status, and
  `RunFailure::owner` on the failure-only hook, which is the same dispatch
  filtered.
- [`LateEvent::owner`](freshness.md) on a freshness crossing, which is how an
  **asset's** owner reaches a hook.
- the built-in [notifiers](notifications.md): a slack line for something with
  an owner ends `, owned by data-platform (#data-alerts)`, and one for
  something nobody claimed ends where it always did. the webhook body gains an
  `owner` object, and omits the key entirely when there is none.

**durable delivery carries it too.** the owner is written into the notification
row with the rest of the event, so the process that finally delivers does not
need to be holding the registry the run was launched from. a row written by an
older hestan has no `owner` key and reads back as an event with no owner rather
than a payload that will not parse.

### The limit, plainly

**an asset build's run event carries the `assets` job's owner, not the asset's.**
asset builds run under one internal job, and one run can build several assets
with several owners; picking one of them would be a guess. an asset's own owner
reaches a hook through `on_late`, and reaches a person through
`GET /api/assets`, the asset page and `hestan owner <name>`.

## Where it shows up

- the **run page** says `owned by ada of data-platform (#data-alerts)` under
  the trigger line, read off the job the run is of.
- the **asset page** and the assets drawer have an `owner` line, and a
  `namespace` line beside it.
- `GET /api/jobs`, `GET /api/jobs/{name}`, `GET /api/assets` and `GET /api/late`
  carry `owner`, and the two job endpoints carry `group` and `group_hue` beside
  `namespace`, both `null` where nothing declared one.
- `hestan owner <name>` answers it from a terminal, for a job, an asset, or
  both where a name is used for one of each:

```
$ orders owner margin
WHAT   NAME    OWNER              CONTACT       ESCALATES TO
asset  margin  ada of finance     #fin-alerts   ops@example.com
```

  a name nothing is registered under is a usage error (exit 2). a thing that
  exists and that nobody claimed is a row with `-` in it, which is a different
  answer and reads like one. `--db` opens a run log, which holds no
  definitions, so this needs the binary the jobs are compiled into or a
  `--server` pointed at one.

## Escalation, and where the line is

`escalates_to` is a second contact string passed to the UI and hooks alongside
the primary contact. Hestan does not time escalation, track acknowledgements,
repeat pages or manage on-call rotations. Implement those policies in the hook
or the system it calls.

## Defaults and API metadata

Namespace and owner declarations are optional. API responses expose nullable
metadata on jobs and assets, namespaces on schedules and sensors, and owners
on late items. Declarations do not migrate stored history.

`Scope::may_touch_job` and `Scope::may_touch_asset` receive the declared
namespace as their second argument; pass `None` for unnamespaced registrations.
Hook payloads carry the owner where applicable.

The UI shows a namespace filter when namespaces are declared and an owner line
when an owner is supplied.
