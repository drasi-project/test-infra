FROM rust@sha256:8fae3b1a63a4dcfb6cf277a49fb5967ccbf479b9e9cee4588a077a9cb216e6d4 AS builder
# rust:1.81-bullseye
RUN apt-get update && apt-get install -y protobuf-compiler libcurl4 && apt-get clean

WORKDIR /usr/src
COPY ./infrastructure ./infrastructure
COPY ./test-data-store ./test-data-store
COPY ./data-collector ./data-collector
COPY ./test-run-host ./test-run-host

RUN cargo new test-service
WORKDIR /usr/src/test-service

COPY ./test-service/Cargo.toml . 
RUN cargo fetch
COPY ./test-service .
RUN cargo install --force --path .

    
FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y \
    gdb curl wget vim net-tools iputils-ping \
    && apt-get clean
ENV RUST_BACKTRACE=1
COPY --from=builder /usr/local/cargo/bin/test-service /usr/local/bin/test-service
# RUN apt-get update && apt install -y openssl
CMD ["test-service"]