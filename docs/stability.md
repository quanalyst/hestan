# Stability

hestan is `0.2.2`, which is a 0.x version and therefore one that says out loud
that it will move. this page is what it will not move, so that something can be
built on it in the meantime.

it is not a promise about 1.0. it is a description of how 0.x is run, and the
whole of it is one sentence: **the things a caller reads, and the traits a
caller implements, change only with a line at the top of the changelog naming
what moved and what to write instead.**

## The version

cargo reads `hestan = "0.2.2"` as `>=0.2.2, <0.3.0`, so under 0.x the **minor
number is the compatibility number**. a `0.2.3` is additions and fixes and
`cargo update` takes it; a break lands on `0.3.0`, which the same requirement
refuses until somebody edits the manifest.

`0.2.1` and `0.2.2` are what that looks like from the other side: fixes and a
redrawn page in the ui, nothing added or moved on any public type, and `cargo
update` takes them without being asked because there is nothing in either to be
asked about.

`0.2.0` was the first release to spend the minor number. it carried no source
break at all and two changes to what an unchanged deployment does, and either
one of those on its own is what the minor number is for: a deployment that met
them through a `cargo update` nobody ran deliberately would have met them at
3am. the size of a release is not the question the number answers.

that rule arrived with 0.1.0. up to `0.1.0-beta.3` the version carried a
pre-release tag, whose requirement has the same ceiling: `cargo update` moved a
deployment from `beta.3` to `beta.4`, and then onto `0.1.0` itself, without
anybody asking. so a break has to be the **first line** of its changelog entry
rather than a paragraph inside it, and it stays there: the first line is the
only part somebody reads before finding out the hard way, and a requirement
written `hestan = "0"` still takes a `0.3.0` on its own.

what may land in a `0.2.x` without breaking a build: a new method, a new
variant on one of the enums marked `#[non_exhaustive]` below, a new endpoint, a
new column behind a migration, a better sentence in an error.

what may not, and is therefore what a `0.3.0` is for: a variant on any other
enum, a **field on a public struct** (that is a source break for a struct
literal, and hestan counts it as one), a required method on a trait somebody
implements, a rename, a removal, a default that changes what an unchanged
deployment does.

## The surfaces

five, and they move together on the one version number.

| surface | written down in | what it holds still |
| --- | --- | --- |
| the rust api | rustdoc, and the rest of this page | the types, the traits, and which of them are closed |
| the http api | `http-api.md` | every documented endpoint, parameter and response shape |
| the command line | `cli.md` | the commands, the output contract under `--json` and `--quiet`, and the nine exit codes |
| the event payloads | `events.md`, and `EVENT_SCHEMA` | a documented key keeps its name, its type and its meaning for as long as the number does not move |
| the run log | `storage.md` | it is read through `Store`, and a build refuses a schema written by a newer hestan rather than guessing at it |

## The enums

two rules, and the type says which one it is under.

**an enum carrying `#[non_exhaustive]` will gain variants.** match it with a
`_` arm. eleven do: `Error`, `Meta`, `InputError`, `Auth`, `Trigger`,
`SubjectKind`, `EventKind`, `TickOutcome`, `When`, `Reclaim` and `Blocked`.
each says in its own rustdoc what is coming and what a caller gives up by
holding a fallback arm open, which in every one of those eleven is close to
nothing: a rendering it has not seen, a cause it can group as "something else",
a refusal it reports by its message.

**an enum without it is a closed set, and that is a promise.** seventeen are:
`Exit`, `RunStatus`, `OpStatus`, `BackfillStatus`, `DeliveryState`, `Access`,
`EventLevel`, `Severity`, `Overlap`, `Catchup`, `Role`, `Freshness`,
`CancelOutcome`, `SensorOutcome`, `LateKind`, `LogStream` and `CheckStatus`.
match them with no `_` arm, and a future hestan that wanted to add to one owes
you the compile error rather than a silent fall into a wildcard. three reasons
run through the seventeen: a **state machine**, where a sixth state changes
what the five mean; an **ordered scale**, where `role >= needed` and
`severity` comparisons are already written against the order; and a **question
whose answers are covered**, where the next thing to say would be a field on a
variant rather than a variant beside it.

`EventKind` and `SubjectKind` carry an `Unknown` arm as well, and it is a
different mechanism for a different problem: `#[non_exhaustive]` is about a
build against a newer hestan, and `Unknown` is about **this** build reading a
row a newer hestan wrote. neither replaces the other.

## The structs

thirty-one public structs have public fields, and a new field on any of them
breaks a struct literal. twenty-eight are things hestan hands you: `Run`,
`Event`, `OpRun`, `Materialization`, `Tick`, the hook payloads, the rest of the
store's rows, and `IoKey`, which an `IoManager` receives rather than builds.
**read them; do not build them.** they gain fields as hestan records more, and
that is announced rather than avoided.

the three a caller does build have a way in that a new field does not break:

```rust
EventQuery { level: Some(EventLevel::Error), ..Default::default() }
RunRequest::new("publish").params(json!({ "day": day })).key(day)
Identity::new("alice", Access::Operator)
```

none of the thirty-one is `#[non_exhaustive]`, on purpose. on a struct that
attribute blocks the literal outright, functional update syntax included, so
putting it on the row types would leave anybody with a good reason to build one
(a test fixture, a fake store) with no way to do it. that wants constructors
first.

[`Owner`](namespaces.md#an-owner) is the first one built that way, and it is
the pattern for the ones after it: **public struct, private fields,
constructors and accessors**. it is a thing callers build (`Owner::team("x")
.contact("#y")`) and a thing hestan hands back on a hook payload, and it is
expected to grow, so a literal of it was never offered. it is not counted in
the thirty-one, and adding a field to it will not break anybody.

`Launch`, `Restored` and `Resettled` are built the same way and for the same
reason. each answers a question that is expected to gain a second half:
[`Launch`](launching.md#launching-once) is what a keyed launch came to,
[`Restored`](backup.md) is what a run log says about having come out of a copy,
and [`Resettled`](backup.md#resettle) is what a resettle handed back. none of
them is a thing you build, so accessors cost a caller nothing and a new field
costs them nothing either.

[`Deployment`](deployment.md) is the same pattern on the other side of the
line: a thing callers *do* build, with private fields and a builder, for the
reason `Owner` has one. it names what an installation is and it is exactly the
sort of thing that grows a third half.

## The extension points

a trait somebody implements is a contract whether or not it was meant as one,
because adding a required method to it breaks every implementation that exists.

| extension point | contract | what that means |
| --- | --- | --- |
| [`IoManager`](io-managers.md) | yes | a new required method is a break, taken deliberately or not at all |
| [`Sensor`](sensors.md)'s closure, and the `RunRequest` it returns | yes | the closure signature and the request's shape hold still |
| the [notification hooks](notifications.md): `on_run_finished`, `on_op_finished`, `on_failure`, `on_late` | yes | the callback shapes hold; the payload structs gain fields (`owner` was one) |
| [`notify::Alert`](notifications.md) | yes | `Serialize` plus a one-line `summary`, and it stays that |
| `Auth::custom`, and the `Request` it is handed | yes | the accessors hold; `Auth` itself will gain variants |
| a [resource](resources.md), by its concrete type | yes | `ctx.resource::<T>()` finds what was declared under `T` and nothing else |
| `Meta` | the other direction | it grows, and your `_` arm is what absorbs it |

two things that look like extension points and are not. `auth::Token` and
`auth::Check` are public because they sit inside `Auth`'s variants: you hand
back what `Auth::bearer` or `Auth::custom` gave you and there is nothing to
implement. `IoResult` and `IoDropped` are aliases naming what the three methods
above return, so they move if and only if `IoManager` does.

`IoManager` is the one with history worth reading. `drop_run` was made a
**required** method rather than a defaulted one on purpose: a default returning
`Ok(())` would have compiled for every manager that already existed and gone on
leaking every file each of them had ever written, which is the bug the method
exists to fix. that is the rule for this trait. hestan adds a required method
when a defaulted one would hide a bug, it is a break, and it arrives with a
changelog line rather than quietly.

## Not a surface

- **module paths inside the crate.** everything but `auth`, `capture`, `cli`,
  `dbt`, `notify`, `otel`, `secret` and `prelude` is a private module, so a
  type's path is its re-export at the crate root. `hestan::model::Run` was never
  something you could write.
- **the wording of an error.** the `Error` variant is the contract and its
  `Display` string is for a person to read. same for a log line and an event's
  `message`; an event's `kind` and its payload keys are the contract, and
  `events.md` says which.
- **the ui.** its html, its css, its bundle names and its urls are a page, not
  an api. what it draws from is the http api, and that is the surface.
- **the sql.** `storage.md` names the tables so a reader can follow what
  happens, not so a query can be written against them.
- **`auth::Token` and `auth::Check`.** public because they appear inside
  `Auth`'s variants. you receive one from `Auth::bearer` or `Auth::custom` and
  hand it back; there is nothing else to do with one.
- **the numbers.** default timeouts, page sizes, poll intervals, the claim
  lease and its renewal, `stop_within`'s eight seconds. each is documented
  where it is used and each may be re-tuned, which is a behaviour change and
  gets its changelog line, but it is not an api break.
- **which two names collide.** `hue` gives the same angle for the same name
  forever, and that much is deliberate (sha-256 rather than a hasher out of
  `std`, which is free to change between releases). that two names land close
  enough together to be hard to tell apart is not something any pure function
  of one name can prevent, and `hestan doctor` reports the pairs instead.

## What is checked rather than claimed

some of what is above is a `cargo test` case rather than a claim, because a
stability claim nothing checks is a comment.

- **`tests/stability.rs`** matches all seventeen closed sets with no `_` arm.
  it is an integration test on purpose: `#[non_exhaustive]` does not restrict
  the crate that defines the type, so the same matches written inside `src/`
  would compile either way and prove nothing. it also reads `src/` back and
  fails if a public enum lands there that is neither marked nor listed, so the
  decision gets made rather than defaulted.
- **the nine exit codes** in `cli.md`'s table are read out of the markdown and
  asserted against the `Exit` discriminants, so the published table and the
  type cannot drift apart.
- **`Trigger`'s rustdoc** carries the exhaustive match that no longer builds
  as a `compile_fail` example, beside the one that does. both are run.
- **`tests/docs.rs`** asserts this page is in the index and that the index
  links nothing that has gone.
