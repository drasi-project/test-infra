FROM rust@sha256:fd45a543ed41160eae2ce9e749e5b3c972625b0778104e8962e9bfb113535301 as builder
# rust:1.74
RUN apt-get update && apt-get install -y protobuf-compiler libcurl4 && apt-get clean

WORKDIR /usr/src
COPY ./infrastructure ./infrastructure

RUN cargo new test-runner
WORKDIR /usr/src/test-runner

COPY ./test-runner/Cargo.toml . 
RUN cargo fetch
COPY ./test-runner .
RUN cargo install --force --path .

    
FROM  gcr.io/distroless/cc
ENV RUST_BACKTRACE=1
COPY --from=builder /usr/local/cargo/bin/test-runner /usr/local/bin/test-runner
# RUN apt-get update && apt install -y openssl
CMD ["test-runner"]