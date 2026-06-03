# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.96
ARG PACKAGE

FROM rust:${RUST_VERSION}-bookworm AS healthcheck-builder
WORKDIR /workspace

COPY tools/grpc-healthcheck/Cargo.toml tools/grpc-healthcheck/Cargo.lock ./
COPY tools/grpc-healthcheck/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/workspace/tools/grpc-healthcheck/target,sharing=locked \
    cargo build --release && \
    cp target/release/grpc-healthcheck /tmp/grpc-healthcheck

FROM rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /workspace

COPY . .
ARG PACKAGE
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=shared \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=shared \
    --mount=type=cache,target=/workspace/target,sharing=locked \
    cargo build --release -p "${PACKAGE}" && \
    cp "target/release/${PACKAGE}" /tmp/raccoon-app

FROM debian:bookworm-slim AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /srv/raccoon
COPY --from=healthcheck-builder /tmp/grpc-healthcheck /usr/local/bin/grpc-healthcheck
COPY --from=builder /tmp/raccoon-app /usr/local/bin/raccoon-app

ENTRYPOINT ["/usr/local/bin/raccoon-app"]
