# Stability

Public API changes are documented in [Changes](../CHANGELOG.md), with migration
instructions for breaking changes. Hestan follows Cargo's compatibility rules:
while the package is pre-stable, breaking changes require a minor release.

Additions may include methods, endpoints, migrated columns and variants on
`#[non_exhaustive]` enums. Renames, removals, fields on public structs, variants
on closed enums, required trait methods and changed defaults require explicit
compatibility review.

## The surfaces

These contracts cover the library and its interfaces.

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

Public structs with public fields can be constructed with literals; adding a
field breaks those literals. Prefer constructors or default-based updates where
available:

```rust
EventQuery { level: Some(EventLevel::Error), ..Default::default() }
RunRequest::new("publish").params(json!({ "day": day })).key(day)
Identity::new("alice", Access::Operator)
```

Store rows and hook payloads remain constructible for fixtures and custom
integrations. Adding fields requires a documented compatibility change.

Types intended to grow, including `Owner`, `Launch`, `Restored`, `Resettled` and
`Deployment`, use private fields with constructors and accessors. Prefer this
pattern for new public types.

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

`IoManager::drop_run` is required because a default that does nothing would
silently leak stored outputs. Required trait methods are breaking changes even
when they prevent a bug.

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
