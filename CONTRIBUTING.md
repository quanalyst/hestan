# Contributing

the api is still moving (see the alpha note in the readme), so open an issue
before a large change — the shape may already be planned differently.

## Gates

`just check` runs exactly what ci runs:

```
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo clippy --all-targets --features http -- -D warnings
cargo test
cargo test --features http
```

the `http` feature compiles real extra code and gates two test targets, so
both configurations have to pass. msrv is 1.87 and ci checks it.

## The ui

`just ui-dev` starts vite with `/api` proxied to `localhost:4000`; run
`just demo` alongside it for data to look at.

`ui/dist` is a committed build artifact — the binary embeds it, so a stale
bundle ships silently. rebuild it with `just ui-build` (which also touches
`src/server.rs`, since cargo can't see through `include_dir!`) and commit the
result in the same commit as the `ui/src` change. ci rebuilds the bundle and
fails on a diff.

## Tests

new behaviour needs a test. unit tests sit at the bottom of the module they
exercise; end-to-end executor tests go in `tests/pipeline.rs` against
`Store::open(":memory:")`. anything behind `http` goes in a test target with
`required-features = ["http"]`.

schema changes need a migration and a fixture test that opens a database at
the old version — see [adding a migration](docs/development.md#adding-a-migration).

## Layout

[docs/development.md](docs/development.md) has the module-by-module map.
