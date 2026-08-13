FROM rust:1.88 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    pkg-config \
    libssl-dev \
    libjq-dev \
    libonig-dev \
    libprotobuf-dev \
    protobuf-compiler \
    cmake \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src
COPY e2e-test-framework/Cargo.toml e2e-test-framework/Cargo.lock ./
RUN sed -i '/  "proxy",/d; /  "reactivator",/d' Cargo.toml
COPY e2e-test-framework/infrastructure ./infrastructure
COPY e2e-test-framework/test-data-store ./test-data-store
COPY e2e-test-framework/data-collector ./data-collector
COPY e2e-test-framework/test-run-host ./test-run-host
COPY e2e-test-framework/test-service ./test-service

WORKDIR /usr/src/test-service
RUN JQ_LIB_DIR=$(dirname $(find /usr/lib -name 'libjq.so' | head -1)) \
    cargo install --force --path .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    jq \
    libssl3 \
    libjq1 \
    libonig5 \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV RUST_BACKTRACE=1

COPY --from=builder /usr/local/cargo/bin/test-service /usr/local/bin/test-service
COPY aspire/Drasi.E2E.Runner/run-building-comfort-grpc.sh /usr/local/bin/run-building-comfort-grpc
COPY e2e-test-framework/examples/building_comfort/dynamic/config.json /app/scenario/config.json
COPY e2e-test-framework/examples/building_comfort/dynamic/components/server /app/scenario/components/server

RUN chmod +x /usr/local/bin/run-building-comfort-grpc

ENTRYPOINT ["run-building-comfort-grpc"]
