# Releasing

publishing hestan is a person with their own crates.io token running one
command. **this repository holds no token, no `.cargo/credentials.toml` and no
workflow that publishes**, and that is deliberate: a token a repository can
reach is a token that publishes when nobody meant it to.

the steps are in order, and each one is a thing that can fail before anything
is uploaded. only the last one is irreversible.

## 1. the gate

nine feature configurations, which is what ci runs:

```
export HESTAN_TEST_PG=postgres://hestan:hestan@127.0.0.1:5432/hestan_test
just check
just check-pg
just ui-test
```

`just check` leaves `HESTAN_TEST_PG` to the environment, so the postgres half
of the store suite runs only if it is exported; `just check-pg` is the one
configuration where that matters most. `just ui-test` is the ui's own suites,
and ci additionally rebuilds `ui/dist` and fails on a diff, so rebuild it with
`just ui-build` and commit the result if anything under `ui/src` moved.

`just checks` is the container half: the image, the compose stack, a partition
and a stop. about ten minutes, and it wants a docker daemon and the compose
plugin. nothing it exercises is inside the `.crate`, so it does not gate a
publish, but a release that touched the image, the queue or the roles wants it.

## 2. the version and the changelog

- `version` in `Cargo.toml`.
- the changelog's top section: the version and the date as its heading, and a
  break named in its **first lines** rather than in a paragraph inside it.
  [stability](docs/stability.md) is why that is where it goes.
- the version in the prose. the readme and several pages under `docs/` quote it
  in a `[dependencies]` block or in a `hestan doctor` transcript:

```
grep -rn '0\.1\.0' README.md docs/ Cargo.toml
```

## 3. what will actually ship

`Cargo.toml`'s `include` is an allowlist, and `tests/docs.rs` holds it to both
halves: nothing ships that is not one of the roots it names, and nothing `src/`
reaches for with `include_str!` or `include_dir!` is left out. read the list
anyway, since a test only knows the rule it was given:

```
cargo package --list
cargo package
```

then build the file that would be uploaded, **offline, in a directory that is
not this one**. a crate that compiles here and nowhere else compiles for
nobody, and the file list looks fine either way because what is missing is not
on it:

```
just release-check
```

## 4. the msrv

`rust-version` in the manifest is a promise to whoever is pinned to it. ci
type-checks it with `cargo check --all-targets --all-features` on that
toolchain. before a release, run the suite on it instead:

```
rustup toolchain install 1.88
rustup run 1.88 cargo test --all-features
```

## 5. the docs.rs build

docs.rs builds `--all-features` on nightly with `--cfg docsrs` and no network,
and `deny(missing_docs)` and `deny(rustdoc::broken_intra_doc_links)` are both
live, so a doc build that fails there is a release whose documentation page is
an error message. it is not part of `just check`, so run it here:

```
RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --all-features --no-deps
```

## 6. commit, tag, push

one commit for the version and the changelog, a tag named `v<version>` to match
the five that came before it, and both pushed. publish from a clean checkout at
that tag, so what is on crates.io and what is on the tag are the same tree.

```
git tag v0.1.0
git push && git push --tags
```

## 7. publish, with your own token

`cargo publish` reads a token from `~/.cargo/credentials.toml`, put there by
`cargo login`, or from `CARGO_REGISTRY_TOKEN` in the environment of that one
command. either is yours. neither belongs in this repository, in its ci, or in
any file a checkout contains.

```
cargo publish
```

`cargo publish --dry-run` does everything except the upload and is worth one
run first: it is the packaging and verification of step 3 plus the registry's
own checks on the metadata.

## 8. after

- docs.rs builds on its own within a few minutes. the badge in the readme goes
  green, or its build log says why it did not.
- the crates.io page is the description, the five keywords, the three
  categories, the readme and the links in `Cargo.toml`. read it as a stranger
  would, because for most people it is the first and only page.

**what cannot be undone.** a published version is permanent: it cannot be
replaced, re-uploaded, or deleted. `cargo yank` stops new dependants from
resolving to it and does nothing for anybody already on it. so the last thing
to be sure of before step 7 is that step 3 built.
