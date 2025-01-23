FROM rust@sha256:8fae3b1a63a4dcfb6cf277a49fb5967ccbf479b9e9cee4588a077a9cb216e6d4 AS builder
# rust:1.81-bullseye
RUN apt-get update && apt-get install -y protobuf-compiler libcurl4 && apt-get clean

WORKDIR /usr/src
COPY ./proxy ./proxy

WORKDIR /usr/src/proxy
RUN cargo install --force --path .

FROM debian:bullseye-slim
RUN apt-get update && apt-get install -y \
    gdb curl wget vim net-tools iputils-ping \
    && apt-get clean
ENV RUST_BACKTRACE=1
COPY --from=builder /usr/local/cargo/bin/proxy /usr/local/bin/proxy
CMD ["proxy"]