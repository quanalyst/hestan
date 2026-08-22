# the gates ci runs
check:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
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

# the container checks: the image builds and serves, the compose stack splits
# the roles, what happens to the deciding process when its network is taken
# away, and what a stop is worth next to a kill. wants a docker daemon, the
# compose plugin and psql; about ten minutes. see docs/containers.md
checks:
    bash deploy/checks/run.sh

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
