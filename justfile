# the gates ci runs
check:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-targets --features http -- -D warnings
    cargo clippy --all-targets --features capture -- -D warnings
    cargo clippy --all-targets --features postgres -- -D warnings
    cargo clippy --all-targets --features otel -- -D warnings
    cargo test
    cargo test --features http
    cargo test --features capture
    cargo test --features postgres
    cargo test --features otel

# the same, with the postgres half of the store suite actually running
check-pg url="postgres://hestan:hestan@localhost/hestan_test":
    HESTAN_TEST_PG={{url}} cargo test --features postgres

demo:
    cargo run --example demo

assets:
    cargo run --example assets

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
