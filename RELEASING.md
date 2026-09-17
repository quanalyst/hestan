# Releasing

Publish from a clean checkout using a personal crates.io token. The repository
does not store publishing credentials or automatically publish releases.

## Validation

Run the checks described in [development](docs/development.md):

```sh
export HESTAN_TEST_PG=postgres://hestan:hestan@127.0.0.1:5432/hestan_test
just check
just check-pg
just ui-test
```

Rebuild and commit `ui/dist` with `just ui-build` after UI changes. Run
`just checks` for changes to containers, the queue or process roles; it requires
Docker and the Compose plugin.

Test with the minimum Rust toolchain declared in `Cargo.toml` and check the
docs.rs configuration:

```sh
rustup run 1.88 cargo test --all-features
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --all-features --no-deps
```

## Package metadata

Update the package version in `Cargo.toml` and refresh `Cargo.lock`. Keep
[Changes](CHANGELOG.md) concise, with compatibility changes and migration
instructions before other notes. Follow the [stability policy](docs/stability.md).
Update dependency examples when their requirements change.

## Package contents

Review the manifest's package allowlist and verify the packaged crate builds
outside the checkout:

```sh
cargo package --list
cargo package
just release-check
cargo publish --dry-run
```

The package must contain the embedded UI and all files included by Rust sources.

## Publish

Commit the prepared changes and tag that commit using `v` followed by the
manifest's package version. Push the commit and tag, then publish from that
clean checkout:

```sh
cargo publish
```

Authenticate through `cargo login` or `CARGO_REGISTRY_TOKEN`; do not commit a
token. A published crate cannot be replaced. Yanking prevents new resolutions
but does not remove existing downloads or lockfile entries.

## Verify publication

Check the crates.io package metadata and the docs.rs build log.
