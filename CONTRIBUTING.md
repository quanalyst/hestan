# Contributing

hestan is `0.1.0`, a 0.x version that says out loud it will move. open an issue
before a large change: the shape may already be planned differently.
[docs/stability.md](docs/stability.md) says what does not move in the meantime.

## Gates

`just ci` runs the whole gate, which is all four jobs of
[.github/workflows/ci.yml](.github/workflows/ci.yml) in the order ci runs them:

- **the rust job**, `just check`: `cargo fmt --check`, then clippy and the test
  suite across ten feature configurations. nine of those are subsets of
  `--all-features`; `--no-default-features` is the odd one out, and it is the
  only configuration a consumer can reach that `--all-features` does not cover,
  which is why it is listed separately rather than assumed.
- **the msrv job**, `just msrv`: a check under the oldest rustc the crate
  promises. the version comes from `rust-version` in `Cargo.toml` rather than
  being written down again.
- **the docs job**, `just docs`: the page docs.rs will build, built the way
  docs.rs builds it. `deny(missing_docs)` is live, so a failure here is a
  release whose documentation page is an error message.
- **the ui job**, `just ui`: lint, test, build, and a check that the committed
  bundle is the one `ui/src` produces.

two of these have a way of passing without having run:

- **the postgres half of the store suite skips itself when `HESTAN_TEST_PG` is
  unset**, and a skip prints what a pass prints. `just ci` sets it; `just check`
  says out loud when it is missing. `docs/development.md` has the server.
- **`just ui` needs a case-sensitive filesystem.** on a case-insensitive one
  `tsc` refuses the tree with TS1149, because an import differing from a file
  name only in casing resolves there to the wrong file. this bites on a macos
  volume mounted into a linux vm.

a test asserts the justfile still runs every configuration ci does, because a
gate that has quietly stopped covering one reports the same green as a gate
that covers it.

## The ui

`just ui-dev` starts vite with `/api` proxied to `localhost:4000`; run
`just demo` alongside it for data to look at.

`ui/dist` is a committed build artifact. the binary embeds it, so a stale
bundle ships silently and no rust test can see it. rebuild with `just ui-build`
(which also touches `src/server.rs`, since cargo cannot see through
`include_dir!`) and commit the result in the same commit as the `ui/src`
change. ci rebuilds the bundle and fails on a diff.

the ui is monochrome on purpose: status is carried by shape, which leaves hue
free to mean provenance. `Swatch.tsx` is the only place a hue is emitted.

## Tests

new behaviour needs a test. unit tests sit at the bottom of the module they
exercise; end-to-end executor tests go in `tests/pipeline.rs` against
`Store::open(":memory:")`. anything behind `http` goes in a test target with
`required-features = ["http"]`.

a test that passes whether or not the change is there has not tested the
change. the check that it is worth having is to revert the fix and watch a
named test go red.

**match a closed enum exhaustively.** `RunStatus`, `OpStatus`, `BackfillStatus`
and friends document themselves as closed sets; a `_` arm over one turns the
next variant into a wrong answer at runtime instead of a compile error, and
that has already happened once here: the notification summary sent every op
status it did not name out as "succeeded", and stayed correct only for as long
as no op could reach it with any other. `ctx.skip` made one reachable and the
arm was wrong the same day.

schema changes need a migration and a fixture test that opens a database at
the old version, and migrations are forward-only. see
[adding a migration](docs/development.md#adding-a-migration).

## Layout

[docs/development.md](docs/development.md) has the module-by-module map.
