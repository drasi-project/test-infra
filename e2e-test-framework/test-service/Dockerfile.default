# Copyright 2025 The Drasi Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM rust:1.88 AS builder

# test-run-host enables drasi-core's "middleware-jq" feature, which pulls in
# jq-sys. jq-sys's build script links against the system libjq and
# liboniguruma, so we install libjq-dev/libonig-dev plus the usual
# build-essential / pkg-config / clang toolchain. Mirrors the approach used
# in drasi-project/drasi-server/Dockerfile.
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
COPY ./infrastructure ./infrastructure
COPY ./test-data-store ./test-data-store
COPY ./data-collector ./data-collector
COPY ./test-run-host ./test-run-host

RUN cargo new test-service
WORKDIR /usr/src/test-service

COPY ./test-service/Cargo.toml .
# Set JQ_LIB_DIR dynamically for multiarch support — Debian's libjq-dev does
# not ship a pkg-config file, so jq-sys can't autodetect the install path.
RUN JQ_LIB_DIR=$(dirname $(find /usr/lib -name 'libjq.so' | head -1)) \
    cargo fetch
COPY ./test-service .
RUN JQ_LIB_DIR=$(dirname $(find /usr/lib -name 'libjq.so' | head -1)) \
    cargo install --force --path .


FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    gdb curl wget vim net-tools iputils-ping \
    ca-certificates libssl3 libjq1 libonig5 \
    && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
ENV RUST_BACKTRACE=1
COPY --from=builder /usr/local/cargo/bin/test-service /usr/local/bin/test-service
CMD ["test-service"]