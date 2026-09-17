# Display names and grouping

Jobs and assets support optional display names, subgroups and labels:

```rust
let job = Job::builder("refresh_a")
    .display_name("Refresh A")
    .group("alpha")
    .subgroup("shared")
    .label("collection", "one")
    .build()?;
```

Use the same methods on `Asset`. For a `MultiAsset`, name the output first:
`.display_name("output_id", "Readable output")`, `.group("output_id", "alpha")`,
`.subgroup("output_id", "shared")`, or `.label("output_id", "collection", "one")`.
The output must be declared with `.produces(...)`.

## Names and validation

The persistent `name` remains the identifier for API routes, schedules,
dependencies and history. The UI shows `display_name` when present, otherwise
`name`; search matches both. Identifiers remain visible in details and tooltips.

- Subgroups are explicit and limited to one level. A subgroup belongs to its
  parent group; the same subgroup name can appear under different parents.
- Jobs require a declared parent group. Assets use their declared group or the
  existing prefix before the first `/` in their persistent name. Slashes never
  imply subgroup membership.
- Blank display names, subgroups, label keys and label values are rejected.
  Subgroups containing `/` or lacking a parent group are also rejected.
- Labels are case-sensitive strings with no special meaning assigned to their
  keys. Repeated keys use the last value. Strings are not trimmed or normalized.
- Metadata defaults to absent display names/subgroups and an empty label map.
  Existing group validation still applies.

Changing display names, subgroups or labels preserves identity, history and
origins. This metadata does not affect execution, ownership, scheduling or
namespace permissions. Group marks continue to use the existing group colours.

## Grouping views

Jobs and Assets default to group then subgroup. Members without a subgroup
appear under “no subgroup” within a mixed parent; existing flat groups stay flat.
Choose two distinct dimensions—group, subgroup or a label—to change the view.
Subgroups retain their parent identity in every view.

Missing values appear in explicit buckets: “no group”, “no subgroup” and
“no KEY label”. A missing value remains distinct from a literal label with that
wording. Label filters support exact values or missing values.

Groups and subgroups are collapsible. Their summaries separate execution
failures and running work from late freshness, stale inputs and failed checks.
Search reveals matching members. Collapsed asset graphs retain dependencies
between groups and hide internal edges.

Grouping, filters, the timeline window and expansion state are stored in the
URL. Switching grouping dimensions preserves filters and resets expansion.
“Declared hierarchy” restores the default view. Views do not change declarations,
dependencies, origins or permissions.

See the [API reference](http-api.md#presentation-metadata) for response fields.
Run the [example](../examples/presentation.rs) with
`cargo run --example presentation`, then open `http://127.0.0.1:4000`.
