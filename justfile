# the gates ci runs
check:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings
    cargo clippy --all-targets --features http -- -D warnings
    cargo test
    cargo test --features http

demo:
    cargo run --example demo

assets:
    cargo run --example assets

http-source:
    cargo run --example http_source --features http

ui-dev:
    cd ui && npm run dev

# cargo can't see through include_dir, so force a recompile after rebuilding
ui-build:
    cd ui && npm run build
    touch src/server.rs

build:
    cargo build
