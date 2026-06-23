# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.96
ARG DCMTK_VERSION=3.7.0
ARG DCMTK_SHA256=5bb3ec8317dc465788bed2ca789e76d03ae5848c9381cce3b14c1a3f8b6aca56
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

FROM debian:bookworm-slim AS dcmtk-builder
ARG DCMTK_VERSION
ARG DCMTK_SHA256
WORKDIR /tmp/dcmtk

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        libjpeg62-turbo-dev \
        libpng-dev \
        libtiff-dev \
        libxml2-dev \
        ninja-build \
        zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://github.com/DCMTK/dcmtk/archive/refs/tags/DCMTK-${DCMTK_VERSION}.tar.gz" -o /tmp/dcmtk.tar.gz && \
    echo "${DCMTK_SHA256}  /tmp/dcmtk.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/dcmtk.tar.gz --strip-components=1 -C /tmp/dcmtk

RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/opt/dcmtk \
        -DBUILD_SHARED_LIBS=OFF \
        -DDCMTK_BUILD_APPS=ON \
        -DDCMTK_BUILD_TESTING=OFF \
        -DDCMTK_WITH_OPENSSL=OFF \
        -DDCMTK_WITH_PNG=ON \
        -DDCMTK_WITH_TIFF=ON \
        -DDCMTK_WITH_XML=ON \
        -DDCMTK_WITH_ZLIB=ON && \
    cmake --build build --target install

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /srv/raccoon
COPY --from=healthcheck-builder /tmp/grpc-healthcheck /usr/local/bin/grpc-healthcheck

FROM runtime-base AS runtime
COPY --from=builder /tmp/raccoon-app /usr/local/bin/raccoon-app

ENTRYPOINT ["/usr/local/bin/raccoon-app"]

FROM runtime AS runtime-dcmtk
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        libjpeg62-turbo \
        libpng16-16 \
        libtiff6 \
        libxml2 \
        zlib1g && \
    rm -rf /var/lib/apt/lists/*
COPY --from=dcmtk-builder /opt/dcmtk /opt/dcmtk
