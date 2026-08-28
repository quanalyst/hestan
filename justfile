# `just ci` is the whole gate: all four jobs of .github/workflows/ci.yml, in the
# order ci runs them. run that before a push. everything below it is one job of
# the four, for when you want a faster loop.
#
# a test asserts this file still lists every cargo command ci runs, because a
# gate that has quietly stopped covering a configuration reports the same green
# as one that covers it.
ci url="postgres://hestan:hestan@localhost/hestan_test":
    #!/usr/bin/env bash
    set -euo pipefail
    export HESTAN_TEST_PG="{{url}}"
    just check
    just msrv
    just docs
    just ui

# the gates ci runs. `--no-default-features` is the tenth and the odd one out:
# it is the only configuration a consumer can reach that is not a subset of
# --all-features, and it is linted rather than tested because with `bundled`
# off the link wants a system sqlite, which is a package on the machine rather
# than anything this repository can promise
check:
    #!/usr/bin/env bash
    set -euo pipefail
    # the store suite's postgres half skips itself when this is unset, and a
    # skip and a pass print the same thing. say which one this run is
    if [ -z "${HESTAN_TEST_PG:-}" ]; then
        echo "note: HESTAN_TEST_PG is unset, so the postgres half of the store"
        echo "      suite will not run. \`just ci\` or \`just check-pg\` runs it."
    fi
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-targets --no-default-features -- -D warnings
    cargo clippy --all-targets --features http -- -D warnings
    cargo clippy --all-targets --features capture -- -D warnings
    cargo clippy --all-targets --features postgres -- -D warnings
    cargo clippy --all-targets --features otel -- -D warnings
    cargo clippy --all-targets --features cli -- -D warnings
    cargo clippy --all-targets --features parquet -- -D warnings
    cargo clippy --all-targets --features dbt -- -D warnings
    cargo clippy --all-targets --all-features -- -D warnings
    cargo test
    cargo test --features http
    cargo test --features capture
    cargo test --features postgres
    cargo test --features otel
    cargo test --features cli
    cargo test --features parquet
    cargo test --features dbt
    cargo test --all-features

# the same, with the postgres half of the store suite actually running
check-pg url="postgres://hestan:hestan@localhost/hestan_test":
    HESTAN_TEST_PG={{url}} cargo test --features postgres

# the page docs.rs will build: nightly, every feature, `--cfg docsrs` and
# warnings denied. docs.rs builds it exactly like this and with no network, and
# `deny(missing_docs)` is live, so a failure there is a release whose
# documentation page is an error message
docs:
    RUSTDOCFLAGS="--cfg docsrs -D warnings" cargo +nightly doc --all-features --no-deps

# the oldest rustc the crate promises. the version comes from `rust-version` in
# Cargo.toml rather than being written here as well, so the two cannot drift; a
# test asserts ci's own pin matches it too
msrv:
    #!/usr/bin/env bash
    set -euo pipefail
    v=$(sed -n 's/^rust-version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
    rustup toolchain install "$v" --profile minimal >/dev/null
    cargo "+$v" check --all-targets --all-features

# ci's ui job. the last line is the one that matters: the binary embeds
# `ui/dist` through include_dir, so a bundle left behind by an edit to `ui/src`
# ships silently and no rust test can see it.
#
# this needs a case-sensitive filesystem. on a case-insensitive one `tsc`
# refuses the tree with TS1149, because an import that differs from a file name
# only in casing resolves to the wrong file there
ui:
    cd ui && npm ci
    cd ui && npm run lint
    cd ui && npm test
    cd ui && npm run build
    git diff --exit-code ui/dist

# the container checks: the image builds and serves, the compose stack splits
# the roles, what happens to the deciding process when its network is taken
# away, and what a stop is worth next to a kill. wants a docker daemon, the
# compose plugin and psql; about ten minutes. see docs/containers.md
checks:
    bash deploy/checks/run.sh

# the packaging half of a release: what would ship, then a build of exactly
# that file, offline and outside this directory, because a crate missing
# something it compiles against builds here and for nobody else. see
# RELEASING.md
release-check:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo package --list
    cargo package
    version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
    crate=$PWD/target/package/hestan-$version.crate
    scratch=$(mktemp -d)
    tar xzf "$crate" -C "$scratch"
    cd "$scratch/hestan-$version"
    cargo build --offline --all-features --all-targets
    echo "built hestan-$version from $crate in $scratch"

demo:
    cargo run --example demo --features cli

assets:
    cargo run --example assets --features cli

http-source:
    cargo run --example http_source --features http

ui-test:
    cd ui && npm test

ui-dev:
    cd ui && npm run dev

# cargo can't see through include_dir, so force a recompile after rebuilding
ui-build:
    cd ui && npm run build
    touch src/server.rs

build:
    cargo build
