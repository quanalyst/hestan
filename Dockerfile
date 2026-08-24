# a hestan application in an image.
#
# hestan is a library, so there is no hestan server to publish an image of.
# what goes in an image is *your* binary: the one that builds a registry and
# calls `serve` or `work`. this file builds `examples/demo.rs` as the worked
# case, because a thing somebody can build and run beats a snippet of one, and
# the shape it shows is the shape yours wants.
#
# a scheduler and its workers are the same image. they must build the same
# registry, because a worker executes runs a scheduler wrote and the two have
# to agree about what a job is. that is why there is one binary here and not
# two, and why the only difference between the containers in
# `docker-compose.yml` is `HESTAN_ROLE`.
#
#     docker build -t hestan-demo .
#     docker compose up -d
#
# built and run on aarch64. there is no `--platform` here, so it builds for
# whatever the daemon is; nothing in it is architecture specific.

# --------------------------------------------------------------------- build

FROM rust:1.88-slim-bookworm AS build
WORKDIR /src

# 1.88 is the crate's `rust-version`, pinned rather than `latest` so the image
# is built by the oldest compiler the crate claims to support. nothing is
# apt-installed in this stage, which is a claim rather than an omission:
#
#   - `rusqlite/bundled` compiles sqlite from source, so the build needs a c
#     compiler. this image has one at /usr/bin/cc.
#   - tokio-postgres speaks the wire protocol in rust, so there is no libpq.
#   - reqwest is on rustls over ring with its roots compiled in, so there is
#     no openssl, and so no pkg-config either.
COPY Cargo.toml Cargo.lock README.md LICENSE ./
COPY src ./src
COPY examples ./examples
# cargo will not parse a manifest whose [[test]] targets are missing files,
# whatever it was asked to build, and this one declares nine of them
COPY tests ./tests
# the ui is embedded with `include_dir!`, so `ui/dist` has to exist before
# rustc runs. it is a **committed** directory rather than something this build
# produces: the crate has to compile on docs.rs and in anybody's `cargo
# install` with no node anywhere, so the built ui is checked in. this image
# therefore runs no npm and installs no node.
#
# the price is worth stating: the image carries whatever `just ui-build` last
# wrote. changing the ui means rebuilding it, committing it, and then building
# the image, in that order.
COPY ui/dist ./ui/dist

# `cli` because `examples/demo.rs` declares it as a required feature: with no
# role set the demo is a command line over its own registry. `postgres`
# because the compose stack beside this file shares one database between a
# scheduler and three workers, and a database several processes can reach is
# what that needs.
#
# not `--all-features`: parquet pulls in arrow, which is tens of megabytes of
# build for a deployment whose op outputs are `{"loaded": 4210}`.
#
# one `RUN`, and no `--mount=type=cache` for the registry and the target
# directory. those need BuildKit, and a Dockerfile that fails outright on the
# plain builder is a worse trade than a dependency build that is not
# incremental: this compiles from scratch whenever anything under `src/`
# changes, and takes minutes rather than seconds when it does.
#
# `mkdir -p` before cargo, and the target directory outside `/src`, are both
# deliberate. cargo creates a target directory by making a temporary one
# beside it and renaming it into place, and overlayfs refuses to rename a
# directory it has not copied up: the build fails with `EXDEV` before it
# compiles anything. a directory that already exists is one cargo does not
# create. keeping it out of the image's own tree also means this layer holds
# one binary rather than a release build's worth of intermediates.
RUN mkdir -p /tmp/target \
    && cargo build --release --locked --target-dir /tmp/target \
      --example demo --features cli,postgres \
    && install -m 0755 /tmp/target/release/examples/demo /usr/local/bin/hestan-demo \
    && rm -rf /tmp/target

# ------------------------------------------------------------------- runtime

FROM debian:bookworm-slim

# what is in this layer, and nothing else is: debian's base, ca-certificates,
# one user, and one binary.
#
# the certificates are here for *your* ops rather than for hestan. hestan's
# own http client compiles its roots in and would work without them; an op
# that calls somebody's api through a client that reads the system store finds
# an empty store without this, and finds it at the worst possible moment.
#
# there is no curl, no wget and no psql. the container runs one process and
# has nothing to probe itself with, so the health checks in
# `docker-compose.yml` and the probes in `deploy/k8s` are made from outside
# it, which is where a health check belongs anyway.
#
# no package manager runs in this layer either. the certificates are copied out
# of the build stage, which has them because the rust image does. that is the
# smaller thing to do, and on an overlay filesystem that refuses dpkg's
# directory renames with `EXDEV` it is the only one that works: an `apt-get
# install ca-certificates` here fails while unpacking libssl3. do not
# helpfully put it back.
COPY --from=build /usr/share/ca-certificates /usr/share/ca-certificates
COPY --from=build /etc/ssl/certs /etc/ssl/certs
RUN useradd --system --create-home --uid 10001 hestan

COPY --from=build /usr/local/bin/hestan-demo /usr/local/bin/hestan-demo

# a writable directory this container owns. on postgres the run log is in the
# database and this holds only what this process wrote for itself (the demo's
# own warehouse file); on sqlite it is the run log, and then sharing it is
# what makes several containers one deployment rather than several.
RUN install -d -o hestan -g hestan /var/lib/hestan
VOLUME ["/var/lib/hestan"]
USER hestan
WORKDIR /var/lib/hestan
ENV HESTAN_DB=/var/lib/hestan/hestan.db

# which build of the application this image is, baked in at build time.
#
#     docker build --build-arg HESTAN_BUILD=$(git rev-parse --short HEAD) .
#
# hestan is a library compiled into the binary above and cannot see the
# repository it came from, so somebody has to tell it, and the moment somebody
# knows is this one: the sha is a fact about the build, and an image is the
# thing a build produces. the demo reads the variable and passes it to
# `Deployment::build`, and from there every run it launches records it.
#
# the default is deliberately not a plausible-looking string. an unset build
# argument becomes an empty value, hestan reads an empty value as an absence,
# and a run log that says "no build declared" is a better answer than one that
# says every run came from `unknown`.
#
# **last, and after the layers that do work.** this argument changes on every
# commit, and a build layer under it would be rebuilt every time.
ARG HESTAN_BUILD=""
ENV HESTAN_BUILD=$HESTAN_BUILD
EXPOSE 4000
CMD ["hestan-demo"]
