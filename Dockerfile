# the demo, built once and started with a role.
#
# a scheduler and its workers are the same image: they must build the same
# registry, because a worker executes runs the scheduler wrote and both have to
# agree about what a job is. that is why there is one binary here and not two.

FROM rust:1.88-slim-bookworm AS build
WORKDIR /src
# the sqlite crate is vendored (`bundled`), so nothing here needs libsqlite3
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY ui/dist ./ui/dist
COPY examples ./examples
COPY tests ./tests
COPY README.md LICENSE ./
RUN cargo build --release --example demo

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --uid 10001 hestan
COPY --from=build /src/target/release/examples/demo /usr/local/bin/hestan-demo
# the run log lives on a volume, because it is the queue: a scheduler and its
# workers are only one deployment if they share this file
RUN mkdir -p /var/lib/hestan && chown hestan:hestan /var/lib/hestan
VOLUME ["/var/lib/hestan"]
USER hestan
WORKDIR /var/lib/hestan
ENV HESTAN_DB=/var/lib/hestan/hestan.db
EXPOSE 4000
CMD ["hestan-demo"]
